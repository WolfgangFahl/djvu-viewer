"""
Created on 2026-01-02

@author: wf
"""

import datetime
from dataclasses import asdict

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

    def testFetchSingleImage(self):
        """
        test fetching a single image as a dataclass object
        """
        title = "Datei:AB1938_Heessen-Geschi.djvu"
        image = self.mwi.fetch_image(title)
        image_dict = asdict(image)
        if self.debug:
            print(image)
            print(image_dict)
        expected = {
            "name": "Datei:AB1938 Heessen-Geschi.djvu",
            "url": "https://wiki.genealogy.net/images//0/0c/AB1938_Heessen-Geschi.djvu",
            "mime": "image/vnd.djvu",
            "size": 161771,
            "user": "KlausErdmann",
            "timestamp": datetime.datetime(2008, 5, 17, 10, 0, 3),
            "description_url": "https://wiki.genealogy.net/Datei:AB1938_Heessen-Geschi.djvu",
            "height": 2689,
            "width": 2095,
        }
        self.assertEqual(image_dict, expected)

    def testFetchAllImages(self):
        """
        test fetching all images
        """
        limit = 3
        images = self.mwi.fetch_allimages(limit=limit)
        for img in images:
            if self.debug:
                print(img)
        self.assertEqual(len(images), limit)
