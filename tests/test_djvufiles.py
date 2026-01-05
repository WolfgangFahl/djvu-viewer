"""
Created on 2026-01-05

@author: wf
"""
from basemkit.basetest import Basetest
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_files import DjvuFiles
from dataclasses import asdict
import json

class TestDjVuFiles(Basetest):
    """
    Test djvu files handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        self.config=DjVuConfig.get_instance()
        self.djvu_files=DjvuFiles(config=self.config)
        self.limit=10

    def show_images(self,images):
        for image in images:
            print(json.dumps(asdict(image),indent=2))

    def test_diff(self):
        """
        test diff between wiki and migration
        """
        if not self.config.new_url:
            return
        wiki_images=self.djvu_files.fetch_images(self.config.base_url,"wiki",limit=self.limit)
        new_images=self.djvu_files.fetch_images(self.config.new_url,"new",limit=self.limit)
        diff_images=self.djvu_files.get_diff("wiki", "new")
        if self.debug:
            print(f"wiki:{len(wiki_images)} new: {len(new_images)} diff:{len(diff_images)} ")
            self.show_images(diff_images)

    def test_wikimedia_commons(self):
        """
        Test fetching images from Wikimedia Commons
        """
        url="https://commons.wikimedia.org/w"
        name="commons"
        try:
            images=self.djvu_files.fetch_images(url, name, self.limit)
            self.fail("commons will not work in Miser mode")
            self.show_images(images)
        except RuntimeError as error:
            self.assertTrue("Miser" in str(error))
