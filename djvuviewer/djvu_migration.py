"""
Created on 2026-02-20

@author: wf
"""

import argparse
import os
from argparse import ArgumentParser, Namespace
from typing import List, Optional

from basemkit.base_cmd import BaseCmd
from lodstorage.mysql import MySqlQuery
from lodstorage.query import EndpointManager, QueryManager

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.multilang_querymanager import MultiLanguageQueryManager
from djvuviewer.version import Version
from djvuviewer.wiki_images import MediaWikiImages


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
            "--wiki-url",
            default="https://wiki.genealogy.net/",
            help="MediaWiki base URL for API queries (default: %(default)s)",
        )
        parser.add_argument(
            "--db-path",
            default=self.config.db_path,
            help="Path to DjVu SQLite database (default: %(default)s)",
        )
        parser.add_argument(
            "--wiki-queries-path",
            default=self.config.wiki_queries_path,
            help="Path to YAML file with wiki MariaDB queries (default: %(default)s)",
        )
        parser.add_argument(
            "--gov-endpoint",
            default="https://gov.genealogy.net/sparql",
            help="GOV SPARQL endpoint URL (default: %(default)s)",
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
            self.config.db_path = args.db_path
            self.show_info()
            handled = True
        return handled

    def query_wiki_db(self) -> Optional[dict]:
        """
        Query wiki MariaDB via EndpointManager + QueryManager + MySqlQuery.
        Uses endpoint 'genwiki39' from ~/.pylodstorage/endpoints.yaml.

        Returns:
            Dict with wiki DB stats or None if endpoint not configured
        """
        queries_path = self.args.wiki_queries_path
        if not queries_path:
            return None
        try:
            ep_path = os.path.expanduser("~/.pylodstorage/endpoints.yaml")
            em = EndpointManager.load_from_yaml_file(ep_path)
            ep = em.endpoints.get("genwiki39")
            if ep is None:
                return None
            qm = QueryManager(lang="sql", queriesPath=queries_path, with_default=False)
            q = qm.queriesByName.get("wiki_djvu_stats")
            if q is None:
                return None
            sql = q.params.apply_parameters_with_check({})
            rows = MySqlQuery(ep).execute_sql_query(sql)
            return rows[0] if rows else None
        except Exception as ex:
            print(f"  Wiki DB query failed: {ex}")
        return None

    def query_sqlite(self) -> Optional[dict]:
        """
        Query DjVu SQLite database via DjVuManager for file statistics.

        Returns:
            Dict with SQLite stats or None on error
        """
        try:
            manager = DjVuManager(self.config)
            rows = manager.query("djvu_stats")
            return rows[0] if rows else None
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
            api_url = self.args.wiki_url.rstrip("/") + "/api.php"
            client = MediaWikiImages(
                api_url=api_url,
                mime_types=("image/vnd.djvu", "image/x-djvu"),
                aiprop=("url", "mime", "size", "timestamp"),
                timeout=30,
            )
            images = client.fetch_allimages(limit=10000, as_objects=True)
            if images:
                timestamps = sorted(
                    img.timestamp for img in images if img.timestamp
                )
                return {
                    "files": len(images),
                    "oldest": timestamps[0] if timestamps else None,
                    "newest": timestamps[-1] if timestamps else None,
                }
        except Exception as ex:
            print(f"  API cache query failed: {ex}")
        return None

    def show_info(self) -> None:
        """
        Display statistics from all three sources.
        """
        print("DjVu Migration Info")
        print("=" * 60)

        print("\n1. Wiki-Datenbank (MariaDB)")
        wiki_stats = self.query_wiki_db()
        if wiki_stats:
            print(f"   Dateien:  {wiki_stats.get('files', '?')}")
            print(f"   Älteste:  {wiki_stats.get('oldest', '?')}")
            print(f"   Neueste:  {wiki_stats.get('newest', '?')}")
        else:
            print("   (nicht verfügbar — endpoint 'genwiki39' noch nicht konfiguriert)")

        print("\n2. DjVu-Viewer Datenbank (SQLite)")
        sqlite_stats = self.query_sqlite()
        if sqlite_stats:
            max_fs = sqlite_stats.get("max_filesize") or 0
            avg_fs = sqlite_stats.get("avg_filesize") or 0
            print(f"   Dateien:     {sqlite_stats.get('files', '?')}")
            print(f"   Älteste:     {sqlite_stats.get('oldest', '?')}")
            print(f"   Neueste:     {sqlite_stats.get('newest', '?')}")
            print(f"   Max. Seiten: {sqlite_stats.get('max_pages', '?')}")
            print(f"   Ø Seiten:    {sqlite_stats.get('avg_pages', '?')}")
            print(f"   Max. Größe:  {max_fs / 1024 / 1024:.1f} MB")
            print(f"   Ø Größe:     {avg_fs / 1024 / 1024:.1f} MB")
        else:
            print("   (nicht verfügbar)")

        print("\n3. MediaWiki API Cache")
        api_stats = self.query_api_cache()
        if api_stats:
            print(f"   Dateien:  {api_stats.get('files', '?')}")
            print(f"   Älteste:  {api_stats.get('oldest', '?')}")
            print(f"   Neueste:  {api_stats.get('newest', '?')}")
        else:
            print("   (nicht verfügbar)")

        print()


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
