"""
Created on 2026-02-20

@author: wf
"""

from basemkit.basetest import Basetest

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_migration import DjVuMigration


class TestDjVuMigration(Basetest):
    """
    Test DjVu migration tool
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        self.config = DjVuConfig(is_example=True)

    def test_info_sqlite(self):
        """
        Test --info: SQLite query returns expected stats structure
        """
        from djvuviewer.djvu_manager import DjVuManager

        manager = DjVuManager(self.config)
        rows = manager.query("djvu_stats")
        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 1)
        stats = rows[0]
        if self.debug:
            print(f"SQLite stats: {stats}")
        for key in ["files", "oldest", "newest", "max_pages", "avg_pages", "max_filesize", "avg_filesize"]:
            self.assertIn(key, stats, f"Missing key: {key}")

    def test_info_output(self):
        """
        Test --info runs without error using example config
        """
        migration = DjVuMigration.__new__(DjVuMigration)
        migration.config = self.config
        import argparse
        migration.args = argparse.Namespace(
            info=True,
            wiki_url="https://wiki.genealogy.net/",
            db_path=self.config.db_path,
            wiki_endpoint="genwiki39",
            queries_path=self.config.queries_path,
        )
        migration.show_info()
