"""
Created on 2026-02-20

@author: wf
"""

import argparse
import datetime
import logging
from argparse import ArgumentParser, Namespace
from typing import List, Optional, Tuple

from basemkit.base_cmd import BaseCmd
from ngwidgets.progress import TqdmProgressbar

logger = logging.getLogger(__name__)

from lodstorage.multilang_querymanager import MultiLanguageQueryManager

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_wikimages import DjVuImagesCache
from djvuviewer.mw_server import Server, ServerConfig, ServerProfile
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
            self.migrate(args.migrate, timestamp_precision_secs=60)
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

    def migrate(
        self,
        pattern: str,
        timestamp_precision_secs: int = 60,
    ) -> List[dict]:
        """
        Check migration eligibility for DjVu files matching *pattern* from the
        source server image folder.

        Applies the 6 migration rules from
        https://media.bitplan.com/index.php/GenWiki/2026-02-24#DjVu_file_migration_rules
        per file, using the already-loaded federation DB:

          1. file exists in MediaWiki database (mw_images table)
          2. file exists in MediaWiki API image cache (mw_images table)
          3. file exists in DjVu Images database (djvu table)
          4. file does NOT yet exist in target folder (checked via server_config target)
          5. timestamp (almost) the same on all sources (within timestamp_precision_secs)
          6. file is bundled (djvu.bundled = True)

        Args:
            pattern: Path substring to filter the source filelist, e.g. ``'8'``,
                     ``'8/8d'``, or ``'8/8d/AB-Koeln-1929-1.djvu'``.
            timestamp_precision_secs: Maximum allowed timestamp difference in seconds
                                      between djvu DB and mw_images (default: 60).

        Returns:
            List of candidate dicts for files passing all 6 rules.
        """
        server_config = ServerConfig.of_yaml()
        source_location = server_config.folders.get("source")
        target_location = server_config.folders.get("target")
        source_server = server_config.servers.get(source_location.server)
        target_server = server_config.servers.get(target_location.server)

        source_filelist = source_server.find_djvu_images(source_location.folder)
        target_filelist = set(target_server.find_djvu_images(target_location.folder))

        mlqm = self.prepare()

        candidates = []
        for source_path in source_filelist:
            if pattern not in source_path:
                continue
            # derive the normalised relpath (/x/xx/filename.djvu) and djvu path
            # source_path is absolute on the remote server e.g.
            # /hd/luxio/genwiki/images/0/00/Ev-g1816.djvu
            # we need the last 3 path components: /hex/hex2/filename.djvu
            parts = source_path.split("/")
            relpath = "/" + "/".join(parts[-3:])
            djvu_path = "/images" + relpath

            # Rule 3: exists in djvu DB
            djvu_rows = self.get(mlqm, "djvu_for_relpath", {"path": djvu_path})
            if not djvu_rows:
                continue
            djvu_row = djvu_rows[0]

            # Rule 1+2: exists in mw_images (API cache covers both wiki DB and API)
            mw_rows = self.get(mlqm, "mw_images_for_relpath", {"relpath": relpath})
            if not mw_rows:
                continue
            mw_row = mw_rows[0]

            # Rule 4: must NOT yet exist on target
            if any(relpath in t for t in target_filelist):
                continue

            # Rule 5: timestamp (almost) the same
            djvu_date = djvu_row.get("iso_date") or ""
            mw_date = mw_row.get("timestamp") or ""
            if djvu_date and mw_date:
                try:
                    dt_djvu = datetime.datetime.fromisoformat(djvu_date)
                    dt_mw = datetime.datetime.fromisoformat(
                        mw_date.replace("Z", "+00:00")
                    )
                    diff_secs = abs((dt_djvu - dt_mw).total_seconds())
                    if diff_secs > timestamp_precision_secs:
                        continue
                except (ValueError, TypeError, AttributeError):
                    continue
            else:
                continue

            # Rule 6: file is bundled
            if not djvu_row.get("bundled"):
                continue

            candidates.append(
                {
                    "path": djvu_path,
                    "djvu_date": djvu_date,
                    "mw_date": mw_date,
                    "filesize": djvu_row.get("filesize"),
                    "page_count": djvu_row.get("page_count"),
                }
            )

        return candidates


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
