"""
Created on 2026-01-02

@author: wf
"""
from basemkit.basetest import Basetest
from djvuviewer.wiki_images import MediaWikiImages

class TestMediaWikiImages(Basetest):
    """
    Test MediaWiki Images handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)

        self.mwi = MediaWikiImages(
            api_url="https://genwiki.genealogy.net/api.php",
            mime_types=("image/vnd.djvu", "image/x-djvu"),
            aiprop=("url", "mime", "size", "timestamp", "user"),
            timeout=20,
        )

    def testFetchAllImages(self):
        """
        test fetching all images
        """
        images = self.mwi.fetch_allimages(limit=3)
        for img in images:
            if self.debug:
                print(img)