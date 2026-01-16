"""
Created on 2026-01-02

@author: wf
"""

import json
from dataclasses import asdict
from typing import Dict

from basemkit.basetest import Basetest

from djvuviewer.wiki_images import MediaWikiImage, MediaWikiImages


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
            print(json.dumps(image_dict, indent=2))
        expected = {
            "url": "https://wiki.genealogy.net/images/0/0c/AB1938_Heessen-Geschi.djvu",
            "mime": "image/vnd.djvu",
            "size": 161771,
            "user": "KlausErdmann",
            "timestamp": "2008-05-17T10:00:03Z",
            "description_url": None,
            "page_id": 499473,
            "height": 2689,
            "width": 2095,
            "pagecount": 3,
            "descriptionurl": "https://wiki.genealogy.net/Datei:AB1938_Heessen-Geschi.djvu",
            "descriptionshorturl": "https://wiki.genealogy.net/index.php?curid=499473",
            "ns": None,
            "title": "Datei:AB1938 Heessen-Geschi.djvu",
            "relpath": "/0/0c/AB1938_Heessen-Geschi.djvu",
            "filename": "AB1938 Heessen-Geschi.djvu",
        }
        self.assertEqual(image_dict, expected)

    def test_api_to_image(self):
        """Test parsing MediaWiki API response to MediaWikiImage object.

        Verifies that parse_image_response correctly extracts image metadata
        from API responses including URL, MIME type, dimensions, and file info.
        """
        test_cases = [
            (
                {
                    "batchcomplete": "",
                    "query": {
                        "normalized": [
                            {
                                "from": "File:Hemelingen-AB-1903.djvu",
                                "to": "Datei:Hemelingen-AB-1903.djvu",
                            }
                        ],
                        "pages": {
                            "-1": {
                                "ns": 6,
                                "title": "Datei:Hemelingen-AB-1903.djvu",
                                "missing": "",
                                "known": "",
                                "imagerepository": "local",
                                "imageinfo": [
                                    {
                                        "timestamp": "2017-08-17T15:42:07Z",
                                        "user": "HReinhardt",
                                        "size": 550,
                                        "width": 1642,
                                        "height": 2423,
                                        "pagecount": 100,
                                        "url": "https://wiki.genealogy.net/images//2/2b/Hemelingen-AB-1903.djvu",
                                        "descriptionurl": "https://wiki.genealogy.net/Datei:Hemelingen-AB-1903.djvu",
                                        "mime": "image/vnd.djvu",
                                    }
                                ],
                            }
                        },
                    },
                },
                "File:Hemelingen-AB-1903.djvu",
                "Hemelingen-AB-1903.djvu",
                -1,
                "image/vnd.djvu",
                550,
            ),
        ]

        for (
            data,
            title,
            expected_filename,
            expected_page_id,
            expected_mime,
            expected_size,
        ) in test_cases:
            with self.subTest(title=title):
                img = self.mwi.parse_image_response(data, title)
                self.assertIsNotNone(img)
                self.assertEqual(img.filename, expected_filename)
                self.assertEqual(img.page_id, expected_page_id)
                self.assertEqual(img.mime, expected_mime)
                self.assertEqual(img.size, expected_size)

    def testFetchAllImages(self):
        """
        test fetching all images
        """
        limit = 3
        for as_objects in [False, True]:
            with self.subTest(as_object=as_objects):
                images = self.mwi.fetch_allimages(limit=limit, as_objects=as_objects)
                for img in images:
                    if self.debug:
                        print(img)
                    if as_objects:
                        self.assertTrue(isinstance(img, MediaWikiImage))
                    else:
                        self.assertTrue(isinstance(img, Dict))
                self.assertEqual(len(images), limit)
