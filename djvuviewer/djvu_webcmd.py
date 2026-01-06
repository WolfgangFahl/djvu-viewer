"""
Created on 2024-08-15

@author: wf
Refactored for DjVuViewer
"""

import sys

from ngwidgets.cmd import WebserverCmd

# Assuming the package name has changed from genwiki to djvuviewer
from djvuviewer.webserver import DjVuViewerWebServer


class DjVuViewerCmd(WebserverCmd):
    """
    command line handling for DjVu Viewer and package converter
    """

    def __init__(self):
        """
        constructor
        """
        config = DjVuViewerWebServer.get_config()
        WebserverCmd.__init__(self, config, DjVuViewerWebServer, DEBUG)


def main(argv: list = None):
    """
    main call
    """
    cmd = DjVuViewerCmd()
    exit_code = cmd.cmd_main(argv)
    return exit_code


DEBUG = 0
if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-d")
    sys.exit(main())
