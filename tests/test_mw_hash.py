"""
Created on 2026-02-26

@author: wf
"""

from basemkit.basetest import Basetest

from djvuviewer.mw_hash import MediaWikiHash


class TestMediaWikiHash(Basetest):
    """
    Test MediaWiki hash encoding
    """

    def setUp(self, debug=True, profile=True):
        Basetest.setUp(self, debug=debug, profile=profile)

    def test_of_filename(self):
        """
        Test MediaWikiHash.of_filename with real MediaWiki examples
        """
        test_cases = [
            ("AB1938_Kreis-Beckum_Inhaltsverz.djvu", "c/c7", "c7", 199),
            ("Auenheim-Frauweiler_Dokument-1693-03-09.djvu", "b/b8", "b8", 184),
        ]

        for filename, path, hash_value, value in test_cases:
            with self.subTest(filename=filename, hash=hash, value=value):
                mw_hash = MediaWikiHash.of_filename(filename)
                self.assertEqual(mw_hash.path, path)
                self.assertEqual(mw_hash.hash_value, hash_value)
                self.assertEqual(mw_hash.value, value)

    def test_of(self):
        """
        Test MediaWikiHash.of_value and of_hash for all 256 possible values
        """
        for value in range(256):
            hash_value = format(value, "02x")
            path = f"{hash_value[0]}/{hash_value}"
            with self.subTest(value=value):
                mw_hash = MediaWikiHash.of_value(value)
                self.assertEqual(mw_hash.hash_value, hash_value)
                self.assertEqual(mw_hash.value, value)
                self.assertEqual(mw_hash.path, path)
            with self.subTest(hash_value=hash_value):
                mw_hash = MediaWikiHash(hash_value)
                self.assertEqual(mw_hash.hash_value, hash_value)
                self.assertEqual(mw_hash.value, value)
                self.assertEqual(mw_hash.path, path)
