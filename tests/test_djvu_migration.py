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

    def test_extract_djvu(self):
        """
        Test extract_djvu returns table name 'djvu' and expected stats keys
        """
        migration = DjVuMigration.__new__(DjVuMigration)
        migration.config = self.config
        table, lod = migration.extract_djvu()
        self.assertEqual(table, "djvu")
        self.assertIsNotNone(lod)
        self.assertGreater(len(lod), 0)
        for key in ["iso_date", "page_count", "filesize"]:
            self.assertIn(key, lod[0], f"Missing key: {key}")
        if self.debug:
            print(f"extract_djvu: {len(lod)} rows, first: {lod[0]}")

    def test_extract_mw_images_uses_cache_with_progressbar(self):
        """
        Test that extract_mw_images uses DjVuImagesCache.from_cache and passes a progressbar.
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
            table, lod = migration.extract_mw_images()

        mock_from_cache.assert_called_once()
        call_kwargs = mock_from_cache.call_args.kwargs
        progressbar = call_kwargs.get("progressbar")
        self.assertIsNotNone(progressbar, "progressbar must be passed to from_cache")
        self.assertIsInstance(progressbar, Progressbar)

        self.assertEqual(table, "mw_images")
        self.assertIsNotNone(lod)
        self.assertEqual(len(lod), 1)
        self.assertEqual(lod[0]["url"], fake_image.url)
        if self.debug:
            print(f"extract_mw_images result: {lod}")

    def test_federated_diff(self):
        """
        Federated query: find files present in mw_images but not in djvu and vice versa.
        Reveals the discrepancy between the MediaWiki API image list and the DjVu SQLite index.
        """
        migration = DjVuMigration.__new__(DjVuMigration)
        migration.config = self.config
        mlqm = migration.prepare()

        only_in_mw = mlqm.query("mw_images_not_in_djvu")
        only_in_djvu = mlqm.query("djvu_not_in_mw_images")

        self.assertIsInstance(only_in_mw, list)
        self.assertIsInstance(only_in_djvu, list)
        if self.debug:
            for query_name in ["mw_images_not_in_djvu", "djvu_not_in_mw_images"]:
                print(migration.show_section(mlqm, query_name, "simple"))
            print(f"in mw_images not in djvu: {len(only_in_mw)}")
            print(f"in djvu not in mw_images: {len(only_in_djvu)}")

    def test_show_info(self):
        """
        Test show_info runs without error using example config
        """
        migration = DjVuMigration.__new__(DjVuMigration)
        migration.config = self.config
        migration.args = argparse.Namespace(info=True, format="simple")
        migration.show_info()
