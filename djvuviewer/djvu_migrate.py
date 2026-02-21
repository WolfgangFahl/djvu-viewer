"""
Created on 2026-02-20

@author: wf
"""

import argparse
from argparse import ArgumentParser, Namespace
from typing import List, Optional

from basemkit.base_cmd import BaseCmd
from ngwidgets.progress import TqdmProgressbar

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_wikimages import DjVuImagesCache
from djvuviewer.multilang_querymanager import MultiLanguageQueryManager
from djvuviewer.version import Version


class DjVuMigration(BaseCmd):
    """
    Command-line tool for DjVu migration to MediaWiki 1.39.

    Queries three sources via named parameterized queries
    (MultiLanguageQueryManager - sql, sparql, ask):
    1. Wiki database (MariaDB)        — endpoint: genwiki39
    2. DjVu-Viewer database (SQLite)  — endpoint: djvu
    3. MediaWiki images               — endpoint: mw_images (in-memory SQLite)

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
            help="Show statistics from all three sources (wiki DB, SQLite, mw_images)",
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
        return handled

    def query_wiki_db(self) -> Optional[List[dict]]:
        """
        Query wiki MariaDB via named query 'wiki_djvu_stats'.

        Returns:
            List of dicts or None if endpoint not configured
        """
        queries_path = self.config.wiki_queries_path
        if not queries_path:
            return None
        lod = None
        try:
            mlqm = MultiLanguageQueryManager(
                yaml_path=queries_path,
                endpoint_name=self.config.wiki_endpoint,
                endpoints_path=self.config.endpoints_path,
                languages=["sql"],
            )
            lod = mlqm.query("wiki_djvu_stats")
        except Exception as ex:
            print(f"  Wiki DB query failed: {ex}")
        return lod

    def query_djvu(self) -> Optional[List[dict]]:
        """
        Query DjVu SQLite database via named query 'djvu_stats'.

        Returns:
            List of dicts or None on error
        """
        lod = None
        try:
            manager = DjVuManager(self.config)
            lod = manager.query("djvu_stats")
        except Exception as ex:
            print(f"  SQLite query failed: {ex}")
        return lod

    def query_mw_images(self) -> Optional[List[dict]]:
        """
        Fetch MediaWiki images, store into the mw_images in-memory endpoint,
        then query 'mw_images_stats' by name.

        Returns:
            List of dicts from mw_images_stats query, or None on error
        """
        lod = None
        try:
            url = self.config.base_url
            progressbar = TqdmProgressbar(
                total=10000, desc="Fetching MediaWiki images", unit="files"
            )
            cache = DjVuImagesCache.from_cache(
                config=self.config,
                url=url,
                name="wiki",
                limit=10000,
                progressbar=progressbar,
            )
            if cache.images:
                mlqm = MultiLanguageQueryManager(
                    yaml_path=self.config.queries_path,
                    endpoint_name="mw_images",
                    endpoints_path=self.config.endpoints_path,
                    languages=["sql"],
                )
                mlqm.store_lod(cache.to_lod(), "MediaWikiImage", primary_key="url")
                lod = mlqm.query("mw_images_stats")
        except Exception as ex:
            print(f"  mw_images query failed: {ex}")
        return lod

    def show_info(self) -> None:
        """
        Display statistics from all three sources using named queries and documentQueryResult.
        """
        fmt = getattr(self.args, "format", "simple")

        print("DjVu Migration Info")
        print("=" * 60)

        # --- 1. Wiki DB ---
        print("\n1. Wiki-Datenbank (MariaDB)")
        wiki_lod = self.query_wiki_db()
        if wiki_lod is None:
            if not self.config.wiki_queries_path:
                print(
                    "  (nicht verfügbar — endpoint 'genwiki39' noch nicht konfiguriert)"
                )
            else:
                print("  (nicht verfügbar)")
        else:
            mlqm = MultiLanguageQueryManager(
                yaml_path=self.config.wiki_queries_path, languages=["sql"]
            )
            q = mlqm.query4Name("wiki_djvu_stats")
            print(q.documentQueryResult(wiki_lod, tablefmt=fmt, withSourceCode=False))

        # --- 2. DjVu SQLite ---
        print("\n2. DjVu-Viewer Datenbank (SQLite)")
        manager = DjVuManager(self.config)
        djvu_lod = self.query_djvu()
        if djvu_lod is None:
            print("  (nicht verfügbar)")
        else:
            q = manager.mlqm.query4Name("djvu_stats")
            print(q.documentQueryResult(djvu_lod, tablefmt=fmt, withSourceCode=False))

        # --- 3. MediaWiki images ---
        print("\n3. MediaWiki Images")
        mw_lod = self.query_mw_images()
        if mw_lod is None:
            print("  (nicht verfügbar)")
        else:
            mlqm = MultiLanguageQueryManager(
                yaml_path=self.config.queries_path, languages=["sql"]
            )
            q = mlqm.query4Name("mw_images_stats")
            print(q.documentQueryResult(mw_lod, tablefmt=fmt, withSourceCode=False))


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
