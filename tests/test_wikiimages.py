"""
Created on 2026-01-02

@author: wf
"""
import json
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
            print(json.dumps(image_dict,indent=2))
        expected = {
          "url": "https://wiki.genealogy.net/images//0/0c/AB1938_Heessen-Geschi.djvu",
          "mime": "image/vnd.djvu",
          "size": 161771,
          "user": "KlausErdmann",
          "timestamp": "2008-05-17T10:00:03Z",
          "description_url": None,
          "height": 2689,
          "width": 2095,
          "pagecount": 3,
          "descriptionurl": "https://wiki.genealogy.net/Datei:AB1938_Heessen-Geschi.djvu",
          "descriptionshorturl": "https://wiki.genealogy.net/index.php?curid=499473",
          "ns": None,
          "title": "Datei:AB1938 Heessen-Geschi.djvu",
          "relpath": "/0/0c/AB1938_Heessen-Geschi.djvu",
          "filename": "AB1938 Heessen-Geschi.djvu"
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
