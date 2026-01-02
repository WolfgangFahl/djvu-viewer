"""
Created on 2026-01-01

@author: wf
"""

import os
import pathlib
from typing import Optional

from basemkit.yamlable import lod_storable


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
            self.images_path = os.path.join(examples_path,"images")
        if self.db_path is None:
            self.db_path = os.path.join(examples_path, "djvu_data.db")

    def djvu_abspath(self,path:str)->str:
        """
        get the absolute djvu path for the given relative path
        """
        djvu_path=path.replace("./", "/")
        djvu_path=djvu_path.replace("/images/","/")
        djvu_path=self.images_path+djvu_path
        return djvu_path

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
    def get_instance(cls,test:bool=False) -> "DjVuConfig":
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
