"""
Created on 2026-07-01

@author: wf
"""

from pathlib import Path

from basemkit.basetest import Basetest

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile


class TestDjVuCore(Basetest):
    """
    Test djvu core handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        self.config = DjVuConfig.get_instance()

    def test_from_path(self):
        """
        test retrieving a DjVuFile from the given package path
        """
        for package_mode in ["zip", "tar"]:
            for filename in ["AB1932-Ramrath", "AB1938_Kreis-Beckum_Inhaltsverz"]:
                package_filename = f"{Path(filename).stem}.{package_mode}"
                package_path = Path(self.config.package_path) / package_filename
                djvu_file = DjVuFile.from_package(package_path)
                self.assertIsNotNone(djvu_file, package_path)
                if self.debug:
                    print(djvu_file.to_yaml())
