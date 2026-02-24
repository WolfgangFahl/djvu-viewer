"""
Created on 2026-02-20

@author: wf
"""

import argparse
import unittest

from basemkit.basetest import Basetest
from djvuviewer.djvu_migrate import DjVuMigration


class TestDjVuMigrate(Basetest):
    """
    Test DjVu migration tool
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        args = argparse.Namespace()
        self.migration = DjVuMigration(args)

    def test_extract_djvu(self):
        """
        Test extract_djvu returns table name 'djvu' and expected stats keys
        """
        table, lod = self.migration.extract_djvu()
        self.assertEqual(table, "djvu")
        self.assertIsNotNone(lod)
        self.assertGreater(len(lod), 0)
        for key in ["iso_date", "page_count", "filesize"]:
            self.assertIn(key, lod[0], f"Missing key: {key}")
        if self.debug:
            print(f"extract_djvu: {len(lod)} rows, first: {lod[0]}")

    def test_federated_diff(self):
        """
        Federated query: find files present in mw_images but not in djvu and vice versa.
        Reveals the discrepancy between the MediaWiki API image list and the DjVu SQLite index.
        """
        mlqm = self.migration.prepare()

        only_in_mw = mlqm.query("mw_images_not_in_djvu")
        only_in_djvu = mlqm.query("djvu_not_in_mw_images")

        self.assertIsInstance(only_in_mw, list)
        self.assertIsInstance(only_in_djvu, list)
        if self.debug:
            for query_name in ["mw_images_not_in_djvu", "djvu_not_in_mw_images"]:
                print(self.migration.show_section(mlqm, query_name, "simple"))
            print(f"in mw_images not in djvu: {len(only_in_mw)}")
            print(f"in djvu not in mw_images: {len(only_in_djvu)}")

    def test_show_info(self):
        """
        Test show_info runs without error using example config
        """
        self.migration.args = argparse.Namespace(info=True, format="simple")
        self.migration.show_info()

    @unittest.skipIf(Basetest.inPublicCI(), "wiki DB not available in CI")
    def test_migrate(self):
        """
        Test migrate applies all 6 migration rules per file from the source
        server filelist and returns only eligible candidates.
        """
        pattern = "0/00"
        candidates = self.migration.migrate(pattern, timestamp_precision_secs=86400)
        self.assertIsInstance(candidates, list)
        for row in candidates:
            for key in ["path", "djvu_date", "mw_date", "filesize", "page_count"]:
                self.assertIn(key, row)
        if self.debug:
            print(f"migrate('{pattern}'): {len(candidates)} candidate(s)")
            for row in candidates:
                print(f"  {row['path']}")

    @unittest.skipIf(Basetest.inPublicCI(), "wiki DB not available in CI")
    def test_wiki_image_links(self):
        """
        Test wiki_image_links returns pages that embed a known DjVu file.
        Uses '02_Amt_Loewenburg.djvu' which is confirmed present in imagelinks.
        """
        filename = "02_Amt_Loewenburg.djvu"
        lod = self.migration.wiki_image_links(filename)
        self.assertIsNotNone(lod, "wiki_image_links must return a list, not None")
        self.assertIsInstance(lod, list)
        self.assertGreater(
            len(lod), 0, f"Expected at least one page linking to {filename}"
        )
        for row in lod:
            self.assertIn("page_title", row, "Each row must have page_title")
            self.assertIn("page_namespace", row, "Each row must have page_namespace")
        if self.debug:
            print(f"wiki_image_links('{filename}'): {len(lod)} page(s)")
            for row in lod:
                print(f"  ns={row['page_namespace']} title={row['page_title']}")
