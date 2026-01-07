"""
Created on 2026-01-07

@author: wf
"""
from basemkit.basetest import Basetest
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_wikimages import DjVuImagesCache


class TestDjVuMediaWikiMages(Basetest):
    """
    Test wiki handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)

    def test_caching(self):
        """
        test caching
        """
        config = DjVuConfig.get_instance(test=True)
        cache = DjVuImagesCache.from_cache(config)
        self.assertGreaterEqual(len(cache.images), 3000)
        self.assertLessEqual(len(cache.images), 5000)