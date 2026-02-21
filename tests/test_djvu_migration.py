"""
Created on 2026-02-20

@author: wf
"""

import argparse
from unittest.mock import MagicMock, patch

from basemkit.basetest import Basetest
from ngwidgets.progress import Progressbar

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_migrate import DjVuMigration
from djvuviewer.djvu_wikimages import DjVuImagesCache
from djvuviewer.wiki_images import MediaWikiImage


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
        for key in [
            "files",
            "oldest",
            "newest",
            "max_pages",
            "avg_pages",
            "max_filesize",
            "avg_filesize",
        ]:
            self.assertIn(key, stats, f"Missing key: {key}")

    def test_info_output(self):
        """
        Test --info runs without error using example config
        """
        migration = DjVuMigration.__new__(DjVuMigration)
        migration.config = self.config
        migration.args = argparse.Namespace(
            info=True,
            wiki_url="https://wiki.genealogy.net/",
            db_path=self.config.db_path,
            wiki_endpoint="genwiki39",
            queries_path=self.config.queries_path,
        )
        migration.show_info()

    def test_query_mw_images_uses_cache_with_progressbar(self):
        """
        Test that query_mw_images uses DjVuImagesCache.from_cache and passes a progressbar.

        Verifies:
        1. DjVuImagesCache.from_cache is called (cache is used, not bypassed)
        2. A Progressbar instance is passed as the progressbar argument
        3. Result is a list of dicts with a 'files' key
        """
        fake_image = MagicMock(spec=MediaWikiImage)
        fake_image.timestamp = "2020-01-01T00:00:00Z"
        fake_image.url = "https://wiki.example.org/images/a/ab/Test.djvu"
        fake_cache = MagicMock(spec=DjVuImagesCache)
        fake_cache.images = [fake_image]
        fake_cache.to_lod.return_value = [
            {
                "url": fake_image.url,
                "timestamp": fake_image.timestamp,
                "mime": "image/vnd.djvu",
                "size": 1234,
            }
        ]

        migration = DjVuMigration.__new__(DjVuMigration)
        migration.config = self.config

        with patch(
            "djvuviewer.djvu_migrate.DjVuImagesCache.from_cache",
            return_value=fake_cache,
        ) as mock_from_cache:
            result = migration.query_mw_images()

        # Cache must have been used
        mock_from_cache.assert_called_once()
        call_kwargs = mock_from_cache.call_args.kwargs

        # A progressbar must have been passed
        progressbar = call_kwargs.get("progressbar")
        self.assertIsNotNone(progressbar, "progressbar must be passed to from_cache")
        self.assertIsInstance(
            progressbar, Progressbar, "progressbar must be a Progressbar instance"
        )

        # Result must be a list of dicts with 'files'
        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        stats = result[0]
        self.assertEqual(stats["files"], 1)
        if self.debug:
            print(f"query_mw_images result: {result}")
