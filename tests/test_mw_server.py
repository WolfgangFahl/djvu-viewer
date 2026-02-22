"""
Created on 2026-02-21

@author: wf
"""

from basemkit.basetest import Basetest
import unittest
from djvuviewer.mw_server import ServerConfig, ServerProfile


class TestMwServer(Basetest):
    """
    Test MediaWiki server handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        self.server_config = ServerConfig.of_yaml()
        self.profile = ServerProfile()

    @unittest.skipIf(Basetest.inPublicCI(), "server config not available in CI")
    def test_server_config(self):
        """Test server config loading"""
        print(self.server_config)
        self.profile.run()
        self.profile.save()
        pass
