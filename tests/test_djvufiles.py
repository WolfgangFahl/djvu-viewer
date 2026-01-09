"""
Created on 2026-01-05

@author: wf
"""

import json
from dataclasses import asdict

from basemkit.basetest import Basetest

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_files import DjVuFiles


class TestDjVuFiles(Basetest):
    """
    Test djvu files handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        self.config = DjVuConfig.get_instance()
        self.djvu_files = DjVuFiles(config=self.config)
        self.limit = 10

    def get_images(self):
        self.wiki_images = self.djvu_files.fetch_images(
            self.config.base_url, "wiki", limit=self.limit
        )
        self.new_images = self.djvu_files.fetch_images(
            self.config.new_url, "new", limit=self.limit
        )

    def show_images(self, images):
        for image in images:
            print(json.dumps(asdict(image), indent=2))

    def test_diff(self):
        """
        test diff between wiki and migration
        """
        if not self.config.new_url:
            return
        self.get_images()
        diff_images = self.djvu_files.get_diff("wiki", "new")
        if self.debug:
            print(
                f"wiki:{len(self.wiki_images)} new: {len(self.new_images)} diff:{len(diff_images)} "
            )
            self.show_images(diff_images)

    def test_djvu_files(self):
        """
        Test fetching djvu image files from index database
        """
        if not self.inPublicCI():
            file_limit = 6 # 12 per second
            djvu_files_by_path = self.djvu_files.get_djvu_files_by_path(
                file_limit=file_limit, page_limit=100
            )
            if self.debug:
                for djvu_file in djvu_files_by_path.values():
                    print(djvu_file.to_yaml())
                    self.assertTrue(len(djvu_file.pages) > 0)
            self.assertGreaterEqual(len(djvu_files_by_path), file_limit)

    def test_fetch_by_titles(self):
        """
        combine the mediawiki image and djvu file retrieval
        """
        titles = ["AB1953-Gohr.djvu"]
        paths = []
        images = self.djvu_files.fetch_images(self.config.base_url, "wiki", titles)
        for image in images:
            if self.debug:
                print(image.to_yaml())
            paths.append(f"/images{image.relpath}")
        djvu_files = self.djvu_files.get_djvu_files_by_path(paths)
        for djvu_file in djvu_files.values():
            if self.debug:
                print(djvu_file.to_yaml())
            for page in djvu_file.pages:
                if self.debug:
                    print(asdict(page))

    def test_wikimedia_commons(self):
        """
        Test fetching images from Wikimedia Commons
        """
        url = "https://commons.wikimedia.org/w"
        name = "commons"
        try:
            images = self.djvu_files.fetch_images(url, name, limit=self.limit)
            self.fail("commons will not work in Miser mode")
            self.show_images(images)
        except RuntimeError as error:
            self.assertTrue("Miser" in str(error))
