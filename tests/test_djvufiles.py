"""
Created on 2026-01-05

@author: wf
"""

import json
import re
from collections import Counter
from dataclasses import asdict

from basemkit.basetest import Basetest

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_files import DjVuFiles
from djvuviewer.wiki_images import MediaWikiImages


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
        """
        get the images
        """
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
            file_limit = 6  # 12 per second
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

    def test_wikimedia_commons_cirrus(self):
        """
        Test fetching DjVu files from Wikimedia Commons using CirrusSearch
        """

        api_url = "https://commons.wikimedia.org/w/api.php"
        mime_types = ("image/vnd.djvu", "image/x-djvu")

        for mime_type in mime_types:
            if self.debug:
                print(f"\n{mime_type}:")
            client = MediaWikiImages(api_url=api_url, mime_types=[mime_type])
            search_query = f"filemime:{mime_type}"
            images = client.fetch_by_cirrus_search(
                search_query, limit=50, max_size_kb=10
            )

            # Create pagecount histogram
            pagecount_histogram = Counter()
            for image in images:
                if image.pagecount is not None:
                    pagecount_histogram[image.pagecount] += 1
            if self.debug:
                print(pagecount_histogram.most_common(10))
            for image in images:
                if image.pagecount is not None and image.pagecount > 1:
                    if self.debug:
                        print(json.dumps(asdict(image), indent=2))

    def get_color(self, name: str, view_record: dict) -> str:
        """Extract color value from the style attribute of the named link in view_record"""
        style = view_record.get(name, "")
        match = re.search(r"color:\s*([^;]+)", style)
        return match.group(1).strip() if match else ""

    def test_add_links(self):
        """
        Test that add_links creates blue links for cached images
        """
        if not self.inPublicCI():
            self.get_images()
            filenames = ["Net-G1819 071.djvu"]
            for filename in filenames:
                view_record = {}
                self.djvu_files.add_links(view_record, filename)

                wiki_color = self.get_color("wiki", view_record)
                new_color = self.get_color("new", view_record)

                if self.debug:
                    print(f"\nColors for {filename}:")
                    print(f"  wiki: {wiki_color}")
                    print(f"  new: {new_color}")

                self.assertEqual("blue", wiki_color, "wiki")
                self.assertEqual("blue", new_color, "new")
