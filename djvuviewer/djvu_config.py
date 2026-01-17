"""
Created on 2026-01-01

@author: wf
"""

import os
import pathlib
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
    is_example: bool = False
    package_path: Optional[str] = (
        None  # package path for viewer files in tar or zip format
    )
    images_path: Optional[str] = None  # MediaWiki images directory
    db_path: Optional[str] = None  # full Path for the djvu index database
    queries_path: Optional[str] = (
        None  # Path for YAML files with named parameterized queries
    )
    backup_path: Optional[str] = None  # Path for bundle backups
    log_path: Optional[str] = None  # Path for log files
    script_path: Optional[str] = None  # Path for bundle scripts
    cache_path: Optional[str] = None  # Path for cached date e.g. mediawiki images
    container_name: Optional[str] = None  # MediaWiki container name for maintenance
    base_url: Optional[str] = "https://wiki.genealogy.net/"
    new_url: Optional[str] = None
    url_prefix: Optional[str] = (
        ""  # URL prefix for proxied deployments (e.g., "/djvu-viewer")
    )
    # package display mode
    package_mode: Optional[str] = "tar"
    use_sudo: bool = False
    timeout: float = (
        60  # maximum number of secs to wait for a background task to complete
    )

    def __post_init__(self):
        """
        make sure we set defaults
        """
        examples_path = DjVuConfig.get_examples_path()
        if self.queries_path is None:
            self.queries_path = os.path.join(examples_path, "djvu_queries.yaml")
        if self.is_example:
            self.package_path = os.path.join(examples_path, "djvu_images")
            self.images_path = os.path.join(examples_path, "images")
            self.db_path = os.path.join(examples_path, "djvu_data.db")
            self.backup_path = os.path.join(examples_path, "backup")
            self.backup_path = os.path.join(examples_path, "scripts")
            # Create log directory for example configuration
            self.log_path = "/tmp/djvu-viewer/log"
            os.makedirs(self.log_path, exist_ok=True)
        else:
            # List of required fields to check
            required_fields = [
                "package_path",
                "images_path",
                "script_path",
                "db_path",
                "backup_path",
                "log_path",
                "package_mode",
            ]

            # Check which required fields are missing
            missing_fields = [
                field for field in required_fields if not getattr(self, field, None)
            ]

            if missing_fields:
                raise ValueError(
                    f"Incomplete DjVuConfig. Missing required fields: {', '.join(missing_fields)}.\n"
                    f"For configuration help, see: https://github.com/WolfgangFahl/djvu-viewer/wiki/Help#Configuration"
                )
            if self.container_name is None:
                self.container_name = "genwiki39-mw"

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

    def full_path(self, relpath: str) -> str:
        """Get full DjVu path by prepending images_path to relative path.

        Args:
            path: Relative path to DjVu file

        Returns:
            Absolute filesytem path to DjVu file
        """
        if relpath.startswith("/images"):
            relpath = relpath.replace("/images", "")
        full_path = f"{self.images_path}{relpath}"
        return full_path

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
                instance = cls(is_example=True)
            cls._instance = instance
        return cls._instance

    @classmethod
    def get_examples_path(cls) -> str:
        # the root directory (default: examples)
        path = os.path.join(os.path.dirname(__file__), "../djvuviewer_examples")
        path = os.path.abspath(path)
        return path
