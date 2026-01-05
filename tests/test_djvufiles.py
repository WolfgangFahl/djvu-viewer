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
        self.limit=5000

    def testDiff(self):
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
            for image in diff_images:
                print(json.dumps(asdict(image),indent=2))
