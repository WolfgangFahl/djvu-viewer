"""
Created on 2026-02-21

@author: wf
"""

import unittest

from basemkit.basetest import Basetest

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

    @unittest.skipIf(Basetest.inPublicCI(), "server config not available in CI")
    def test_find_djvu_images(self):
        """
        Test find_djvu_images runs against the source server/folder from
        server_config.yaml  and that subsequent calls use the
        cache instead of going remote again.

        First call: hits the remote server via Remote.run and writes the cachefile

        Second call (within expiry window): reads from cache without any
        remote call.
        """
        source_location = self.server_config.folders.get("source")
        self.assertIsNotNone(
            source_location, "server_config.yaml must have a 'source' folder entry"
        )

        server_name = source_location.server
        folder_name = source_location.folder
        server = self.server_config.servers.get(server_name)
        self.assertIsNotNone(
            server, f"server '{server_name}' not found in server_config.yaml"
        )
        imagefolder = server.imagefolders.get(folder_name)
        self.assertIsNotNone(
            imagefolder,
            f"imagefolder '{folder_name}' not found on server '{server_name}'",
        )

        # --- first call: must go remote ---
        djvu_paths = server.find_djvu_images(folder_name)
