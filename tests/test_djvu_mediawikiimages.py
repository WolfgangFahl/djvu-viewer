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
        local = not self.inPublicCI()
        config = DjVuConfig.get_instance(test=not local)
        limit = 5000 if local else 40
        expected = 4000 if local else limit
        for name, url, limit in [
            ("wiki", config.base_url, limit),
            ("new", config.new_url, limit),
        ]:
            if url:
                cache = DjVuImagesCache.from_cache(
                    config=config, url=url, name=name, limit=limit
                )
                if self.debug:
                    print(f"{name}:{url} -> {len(cache.images)} with limit {limit}")
                self.assertGreaterEqual(len(cache.images), expected)
