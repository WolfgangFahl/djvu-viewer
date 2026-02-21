"""
Created on 2026-02-20

@author: wf
"""

import argparse
from argparse import ArgumentParser, Namespace
from typing import List, Optional

from basemkit.base_cmd import BaseCmd
from ngwidgets.progress import TqdmProgressbar
from tabulate import tabulate

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
    1. Wiki database (MariaDB)
    2. DjVu-Viewer database (SQLite)
    3. MediaWiki API cache

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
            help="Show statistics from all three sources (wiki DB, SQLite, API cache)",
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
        Query wiki MariaDB via MultiLanguageQueryManager with endpoint_name.

        Returns:
            List of dicts with wiki DB stats or None if endpoint not configured
        """
        queries_path = self.config.wiki_queries_path
        if not queries_path:
            return None
        try:
            mlqm = MultiLanguageQueryManager(
                yaml_path=queries_path,
                endpoint_name=self.config.wiki_endpoint,
                languages=["sql"],
            )
            lod = mlqm.query("wiki_djvu_stats")
            return lod
        except Exception as ex:
            print(f"  Wiki DB query failed: {ex}")
        return None

    def query_sqlite(self) -> Optional[List[dict]]:
        """
        Query DjVu SQLite database via DjVuManager for file statistics.

        Returns:
            List of dicts with SQLite stats or None on error
        """
        try:
            manager = DjVuManager(self.config)
            lod = manager.query("djvu_stats")
            return lod
        except Exception as ex:
            print(f"  SQLite query failed: {ex}")
        return None

    def query_api_cache(self) -> Optional[dict]:
        """
        Query MediaWiki API cache for DjVu file count and date range.

        Returns:
            Dict with API stats or None on error
        """
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
            images = cache.images
            if images:
                timestamps = sorted(img.timestamp for img in images if img.timestamp)
                result = {
                    "files": len(images),
                    "oldest": timestamps[0] if timestamps else None,
                    "newest": timestamps[-1] if timestamps else None,
                }
                return result
        except Exception as ex:
            print(f"  API cache query failed: {ex}")
        return None

    def show_info(self) -> None:
        """
        Display statistics from all three sources.

        Named queries return a list-of-dicts; each is rendered via
        Query.documentQueryResult so --format maps directly to tablefmt.
        The API-cache result has no named query and is rendered with tabulate directly.
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

        # --- 2. SQLite ---
        print("\n2. DjVu-Viewer Datenbank (SQLite)")
        manager = DjVuManager(self.config)
        sqlite_lod = self.query_sqlite()
        if sqlite_lod is None:
            print("  (nicht verfügbar)")
        else:
            q = manager.mlqm.query4Name("djvu_stats")
            print(q.documentQueryResult(sqlite_lod, tablefmt=fmt, withSourceCode=False))

        # --- 3. API cache ---
        print("\n3. MediaWiki API Cache")
        api_result = self.query_api_cache()
        if api_result is None:
            print("  (nicht verfügbar)")
        else:
            print(tabulate([api_result], headers="keys", tablefmt=fmt))


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
