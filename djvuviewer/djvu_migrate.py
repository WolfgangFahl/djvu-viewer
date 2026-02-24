"""
Created on 2026-02-20

@author: wf
"""

import argparse
from argparse import ArgumentParser, Namespace
from typing import List, Optional, Tuple

import logging

from basemkit.base_cmd import BaseCmd
from ngwidgets.progress import TqdmProgressbar

logger = logging.getLogger(__name__)

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_wikimages import DjVuImagesCache
from djvuviewer.mw_server import Server, ServerConfig, ServerProfile
from lodstorage.multilang_querymanager import MultiLanguageQueryManager
from djvuviewer.version import Version


class DjVuMigration(BaseCmd):
    """
    Command-line tool for DjVu migration to MediaWiki 1.39.

    All three sources are extracted into a single in-memory federation db
    (djvu_migrate endpoint) as tables named after their domain prefix:
      djvu       — extracted from DjVu SQLite
      mw_images  — extracted from MediaWiki API cache
      wiki       — extracted from MariaDB (genwiki39)

    Stats queries (djvu_stats, mw_images_stats, wiki_stats) then run
    against that single db via MultiLanguageQueryManager.

    See https://djvu-wiki.genealogy.net/DjVu-Viewer_Integration
    """

    def __init__(self, args: argparse.Namespace):
        """
        Initialize DjVu migration tool.

        Args:
            args: Parsed command-line arguments
        """
        super().__init__(Version())
        self.args = args
        self.config = DjVuConfig.get_instance()

    def add_arguments(self, parser: ArgumentParser) -> ArgumentParser:
        """
        Add migration-specific arguments.

        Args:
            parser: ArgumentParser to add arguments to

        Returns:
            The modified ArgumentParser
        """
        super().add_arguments(parser)
        parser.add_argument(
            "--info",
            action="store_true",
            help="Show migration statistics from all sources",
        )
        parser.add_argument(
            "--format",
            default="simple",
            metavar="FMT",
            help=(
                "tabulate table format for --info output "
                "(e.g. simple, grid, pipe, github, latex, mediawiki). "
                "Default: simple"
            ),
        )
        parser.add_argument(
            "--test",
            action="store_true",
            help="Check test files on all servers: bundled state, size, djvudump timing",
        )
        parser.add_argument(
            "--write",
            action="store_true",
            help="With --test: write updated migrated/djvudumpMs back to server_config.yaml",
        )
        parser.add_argument(
            "--migrate",
            metavar="PATTERN",
            default=None,
            help=(
                "Check migration eligibility for DjVu files matching PATTERN. "
                "PATTERN is matched as a substring of the file path, e.g. '8', "
                "'8/8d', or '8/8d/AB-Koeln-1929-1.djvu'. "
                "Applies all migration rules: must exist in djvu DB, mw_images "
                "cache, timestamps must match within 60s, file must be bundled."
            ),
        )
        return parser

    def handle_args(self, args: Namespace) -> bool:
        """
        Handle parsed arguments.

        Args:
            args: Parsed command-line arguments

        Returns:
            True if handled
        """
        handled = super().handle_args(args)
        if args.info:
            self.show_info()
            handled = True
        if args.test:
            server_config = ServerConfig.of_yaml()
            profile = ServerProfile(server_config, debug=args.debug)
            profile.run()
            profile.show(args.format)
            if getattr(args, "write", False):
                profile.write_back()
            handled = True
        if args.migrate:
            self.migrate(args.migrate)
            handled = True
        return handled

    def extract_djvu(self) -> Tuple[str, Optional[List[dict]]]:
        """
        Extract all DjVu records from the DjVu SQLite database.

        Returns:
            Tuple of table name 'djvu' and list of dicts, or None on error.
        """
        lod = None
        try:
            lod = DjVuManager(self.config).query("all_djvu", {"limit": 100000})
        except Exception as ex:
            logger.warning("djvu extract failed: %s", ex)
        return "djvu", lod

    def extract_mw_images(self) -> Tuple[str, Optional[List[dict]]]:
        """
        Extract all MediaWiki images from the API cache.

        Returns:
            Tuple of table name 'mw_images' and list of dicts, or None on error.
        """
        lod = None
        try:
            cache = DjVuImagesCache.from_cache(
                config=self.config,
                url=self.config.base_url,
                name="wiki",
                limit=10000,
                progressbar=TqdmProgressbar(
                    total=10000, desc="Fetching MediaWiki images", unit="files"
                ),
            )
            if cache.images:
                lod = cache.to_lod()
        except Exception as ex:
            logger.warning("mw_images extract failed: %s", ex)
        return "mw_images", lod

    def extract_wiki(self) -> Tuple[str, Optional[List[dict]]]:
        """
        Extract wiki DjVu stats from the MariaDB (genwiki39).

        Returns:
            Tuple of table name 'wiki' and list of dicts, or None on error.
        """
        lod = None
        if not self.config.wiki_queries_path:
            return "wiki", lod
        try:
            mlqm = MultiLanguageQueryManager(
                yaml_path=self.config.wiki_queries_path,
                endpoint_name=self.config.wiki_endpoint,
                endpoints_path=None,
                languages=["sql"],
            )
            lod = mlqm.query("wiki_djvu_stats")
        except Exception as ex:
            logger.warning("wiki extract failed: %s", ex)
        return "wiki", lod

    def wiki_image_links(self, filename: str) -> Optional[List[dict]]:
        """
        Find all MediaWiki pages that embed the given DjVu image file.

        Queries the imagelinks table in the wiki MariaDB (genwiki39) for all
        pages that link to *filename*.

        Args:
            filename: Bare filename without path and without 'File:' prefix,
                      e.g. 'AB-Koeln-1929-1.djvu'.

        Returns:
            List of dicts with keys page_title, page_namespace,
            il_from_namespace, or None on error.
        """
        lod = None
        if not self.config.wiki_queries_path:
            return lod
        try:
            mlqm = MultiLanguageQueryManager(
                yaml_path=self.config.wiki_queries_path,
                endpoint_name=self.config.wiki_endpoint,
                endpoints_path=None,
                languages=["sql"],
            )
            lod = mlqm.query("wiki_image_links", {"filename": filename})
        except Exception as ex:
            logger.warning("wiki_image_links failed for %s: %s", filename, ex)
        return lod

    def prepare(self) -> MultiLanguageQueryManager:
        """
        Extract all sources and load them into the single djvu_migrate
        in-memory federation database.

        Returns:
            MultiLanguageQueryManager connected to the federation db,
            ready for named queries.
        """
        mlqm = MultiLanguageQueryManager(
            yaml_path=self.config.migrate_queries_path,
            endpoint_name="djvu_migrate",
            endpoints_path=self.config.endpoints_path,
            languages=["sql"],
        )
        for table, lod in [
            self.extract_djvu(),
            self.extract_mw_images(),
            self.extract_wiki(),
        ]:
            if lod:
                mlqm.store_lod(lod, table, primary_key=None)
        return mlqm

    def get(
        self,
        mlqm: MultiLanguageQueryManager,
        query_name: str,
        params: Optional[dict] = None,
    ) -> Optional[List[dict]]:
        """
        Run a named query against the federation database.

        Args:
            mlqm: Federation MultiLanguageQueryManager from prepare().
            query_name: Named query defined in djvu_queries.yaml.
            params: Optional parameter dict passed to the query.

        Returns:
            List of result dicts, or None on error.
        """
        lod = None
        try:
            lod = mlqm.query(query_name, params or {})
        except Exception as ex:
            logger.warning("%s failed: %s", query_name, ex)
        return lod

    def show_section(
        self,
        mlqm: MultiLanguageQueryManager,
        query_name: str,
        fmt: str,
        params: Optional[dict] = None,
    ) -> str:
        """
        Render one info section via Query.documentQueryResult.

        Args:
            mlqm: Federation MultiLanguageQueryManager from prepare().
            query_name: Named query to run and render.
            fmt: tabulate format string.
            params: Optional parameter dict passed to the query.

        Returns:
            Rendered string for the section.
        """
        lod = self.get(mlqm, query_name, params)
        if lod is None:
            result = f"{query_name}: (nicht verfügbar)"
        else:
            q = mlqm.query4Name(query_name)
            result = q.documentQueryResult(lod, tablefmt=fmt, withSourceCode=False)
        return result

    def show_info(self) -> None:
        """
        Prepare the federation db and display all migration statistics.
        """
        fmt = getattr(self.args, "format", "simple")
        mlqm = self.prepare()
        for query_name in ["wiki_stats", "djvu_stats", "mw_images_stats"]:
            print(self.show_section(mlqm, query_name, fmt))

    def migrate(self, pattern: str) -> Optional[List[dict]]:
        """
        Check migration eligibility for DjVu files whose path contains *pattern*.

        Loads the federation DB (djvu + mw_images + wiki tables) and runs the
        named query ``djvu_migrate_candidates`` which enforces all migration rules:

        - exists in djvu DB
        - exists in mw_images API cache
        - timestamps agree within 60 seconds
        - file is bundled

        Args:
            pattern: Substring matched against djvu.path, e.g. ``'8'``,
                     ``'8/8d'``, or ``'8/8d/AB-Koeln-1929-1.djvu'``.

        Returns:
            List of candidate dicts, or None on error.
        """
        fmt = getattr(self.args, "format", "simple")
        mlqm = self.prepare()
        lod = self.get(mlqm, "djvu_migrate_candidates", {"pattern": pattern})
        if lod is not None:
            print(
                self.show_section(
                    mlqm, "djvu_migrate_candidates", fmt, {"pattern": pattern}
                )
            )
        return lod


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point for the DjVu migration tool.

    Args:
        argv: Command-line arguments (defaults to sys.argv if None)

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    return DjVuMigration.main(argv)


if __name__ == "__main__":
    main()
