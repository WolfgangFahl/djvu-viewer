"""
Created on 2026-01-01

@author: wf
"""

import os
import pathlib
import re
import urllib.parse
from enum import Enum
from typing import Optional

from basemkit.yamlable import lod_storable


class PngMode(Enum):
    """PNG generation mode"""

    CLI = "cli"  # Use ddjvu command-line tool
    PIL = "pil"  # Use PIL with rendered buffer


@lod_storable
class DjVuConfig:
    """
    configuration for DjVu Viewer and Converter
    """

    # singleton
    _instance: Optional["DjVuConfig"] = None

    tarball_path: Optional[str] = None
    images_path: Optional[str] = None
    db_path: Optional[str] = None
    queries_path: Optional[str] = None
    base_url: Optional[str] = "https://wiki.genealogy.net/"
    new_url: Optional[str] = None
    url_prefix: Optional[str] = (
        ""  # URL prefix for proxied deployments (e.g., "/djvu-viewer")
    )

    def __post_init__(self):
        """
        make sure we set defaults
        """
        examples_path = DjVuConfig.get_examples_path()
        if self.queries_path is None:
            self.queries_path = os.path.join(examples_path, "djvu_queries.yaml")
        if self.tarball_path is None:
            self.tarball_path = os.path.join(examples_path, "djvu_images")
        if self.images_path is None:
            self.images_path = os.path.join(examples_path, "images")
        if self.db_path is None:
            self.db_path = os.path.join(examples_path, "djvu_data.db")

    def djvu_relpath(self, path: str) -> str:
        """Convert path to wiki image-relative format by removing './' and '/images/'."""

        # Look for 'images/' anywhere in the path and extract everything after it
        match = re.search(r"images/(.*)", path)

        if match:
            # Extract the part after 'images/' and prepend '/'
            cleaned_path = "/" + match.group(1)
        else:
            # No 'images/' found - just handle './' prefix
            if path.startswith("./"):
                cleaned_path = "/" + path[2:]
            else:
                cleaned_path = path

        # Remove duplicate slashes
        cleaned_path = re.sub(r"/+", "/", cleaned_path)

        return cleaned_path

    def wiki_fileurl(
        self, filename: str, new: bool = False, quoted: bool = False
    ) -> str:
        """get the wiki file url for the given filename"""
        url = self.new_url if new else self.base_url
        # wiki_url = f"{self.base_url}/File:{filename}"
        wiki_url = urllib.parse.urljoin(url, f"index.php?title=File:{filename}")
        if quoted:
            wiki_url = urllib.parse.quote(wiki_url)
        return wiki_url

    def djvu_abspath(self, path: str) -> str:
        """Get absolute DjVu path by prepending images_path to relative path."""
        djvu_path = self.images_path + self.djvu_relpath(path)
        return djvu_path

    def extract_and_clean_path(self, url: str) -> str:
        """
        URL decode, extract path from /images, and remove duplicate slashes.

        Args:
            url (str): The URL to process

        Returns:
            str: The cleaned path starting from /images
        """
        # URL decode
        decoded_url = urllib.parse.unquote(url)
        relpath = self.djvu_relpath(decoded_url)
        return relpath

    @classmethod
    def get_config_file_path(cls) -> str:
        """
        Returns the standard location for the config file: $HOME/.djvuviewer/config.yaml
        """
        home = pathlib.Path.home()
        config_dir = home / ".djvuviewer"
        config_dir.mkdir(parents=True, exist_ok=True)
        return str(config_dir / "config.yaml")

    @classmethod
    def get_instance(cls, test: bool = False) -> "DjVuConfig":
        """
        get my instance
        """
        if cls._instance is None:
            config_path = cls.get_config_file_path()
            if os.path.exists(config_path) and not test:
                # load_from_yaml_file is provided by the @lod_storable decorator
                instance = cls.load_from_yaml_file(config_path)
            else:
                # Return default instance if no config file found
                instance = cls()
            cls._instance = instance
        return cls._instance

    @classmethod
    def get_examples_path(cls) -> str:
        # the root directory (default: examples)
        path = os.path.join(os.path.dirname(__file__), "../djvuviewer_examples")
        path = os.path.abspath(path)
        return path
