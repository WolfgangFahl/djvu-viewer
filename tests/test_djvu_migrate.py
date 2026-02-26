"""
Created on 2026-02-20

@author: wf
"""

import argparse
import unittest

from basemkit.basetest import Basetest

from djvuviewer.djvu_migrate import DjVuMigration
from djvuviewer.mw_server import ImageFolder, Server


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
        self.migration.configure_profile(debug=debug)

    def test_update_profile(self):
        """
        Test updating the profile
        """
        write = False
        self.migration.update_profile(tablefmt="mediawiki", write=write)

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
        Test migrate for the given pattern
        """
        pattern = "0/00"
        limit=1
        self.migration.migrate(pattern,limit)

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

    def test_check_bundled_by_size(self):
        """
        Test check_bundled_by_size with bundled and stub examples.
        """
        server = Server(hostname="test", os="Linux", latencyMs=0.0)

        # Bundled case: 300KB filesize, 1.8MB min
        is_bundled = server.check_bundled_by_size(300694, 1868800)
        self.assertTrue(is_bundled)

        # Stub case: 118 bytes filesize, 25MB min
        is_bundled = server.check_bundled_by_size(118, 25000000)
        self.assertFalse(is_bundled)

        if self.debug:
            print("Bundled check passed for both cases")

    def test_get_folder_server(self):
        """
        Test get_folder_server returns correct Server and ImageFolder.
        """
        self.migration.configure_profile(debug=self.debug)
        source_server, source_folder = self.migration.profile.get_folder_server(
            "source"
        )
        self.assertIsNotNone(source_server)
        self.assertIsNotNone(source_folder)
        if self.debug:
            print(f"source: {source_server.hostname}, {source_folder.path}")

    def test_generate_scp_command(self):
        """
        Test generate_scp_command produces correct format.
        """

        source_server = Server(hostname="source.example.com", os="Linux", latencyMs=0.0)
        source_folder = ImageFolder(path="/source/images", fs="HD")
        target_server = Server(hostname="target.example.com", os="Linux", latencyMs=0.0)
        target_folder = ImageFolder(path="/target/images", fs="SSD")
        relpath = "/0/00/Test.djvu"

        self.migration.configure_profile(debug=self.debug)
        scp_command = self.migration.profile.generate_scp_command(
            source_server, source_folder, target_server, target_folder, relpath
        )

        expected = "scp source.example.com:/source/images/0/00/Test.djvu target.example.com:/target/images/0/00/Test.djvu"
        self.assertEqual(scp_command, expected)
        if self.debug:
            print(f"scp_command: {scp_command}")
