"""
Created on 2026-02-21

MediaWiki Server handling

@author: wf
"""

import time
from dataclasses import field
from typing import Dict, List, Optional

from basemkit.profiler import Profiler
from basemkit.yamlable import lod_storable
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVu
from mwstools_backend.remote import Remote
from djvuviewer.lod_show import LodShow


@lod_storable
class ImageFolder:
    """An mediawiki Wiki image folder on a server."""

    path: str
    fs: str
    total: Optional[int] = None
    migrated: Optional[int] = None
    statMs: Optional[float] = None
    readMs: Optional[float] = None
    speedMBs: Optional[float] = None
    djvudumpMs: Optional[float] = None


@lod_storable
class Server:
    """A server with one or more image stores."""

    hostname: str
    os: str
    latencyMs: float
    imagefolders: Dict[str, ImageFolder] = field(default_factory=dict)
    is_local: Optional[bool] = False

    def find_djvu_images(
        self,
        imagefolder_name: str,
        expiry_secs: int = 86400,
    ) -> List[str]:
        """
        Find all .djvu files in the hex subdirectories (0-f) of an image folder
        on a remote server.  Results are cached in
        ~/.djvuviewer/{imagefolder_name}/djvu_filelist.txt and reused until
        the cache is older than *expiry_secs* seconds.

        Args:
            server: The server that hosts the image folder.
            imagefolder_name: Short name used as the local cache directory.
            expiry_secs: Cache lifetime in seconds (default: 86400 = 24 h).

        Returns:
            Sorted list of relative .djvu file paths found under the hex dirs.
        """
        imagefolder = self.imagefolders.get(imagefolder_name)
        if not imagefolder:
            raise Exception(f"invalid image folder name {imagefolder_name}")
        cache_dir = DjVuConfig.get_config_dir() / imagefolder_name
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / "djvu_filelist.txt"

        cache_valid = (
            cache_file.exists()
            and cache_file.stat().st_size > 0
            and (time.time() - cache_file.stat().st_mtime) < expiry_secs
        )
        if cache_valid:
            djvu_paths = cache_file.read_text().splitlines()
            return djvu_paths

        cmd = f"find {imagefolder.path} -type f -path '*/[0-9a-f]/*.djvu' 2>/dev/null"
        remote = Remote(host=self.hostname)
        result = remote.run(cmd)
        raw_output = result.stdout or ""
        djvu_paths = sorted(
            line.strip() for line in raw_output.splitlines() if line.strip()
        )

        cache_file.write_text("\n".join(djvu_paths))
        return djvu_paths


@lod_storable
class TestFile(DjVu):
    """
    a test file
    """

    bundled_size: Optional[int] = None
    unbundled_stub_size: Optional[int] = None


@lod_storable
class SetupLocation:
    """
    A server/folder location for migration.
    """

    server: Optional[str] = None
    folder: Optional[str] = None


@lod_storable
class ServerConfig:
    """
    Server configuration loaded from ~/.djvuviewer/server_config.yaml.
    Only image stores marked target: true are writable for migration.
    """

    folders: Dict[str, SetupLocation] = field(default_factory=dict)

    test_files: List[TestFile] = field(default_factory=list)
    servers: Dict[str, Server] = field(default_factory=dict)

    @classmethod
    def get_config_path(cls) -> str:
        """
        Returns the standard location: ~/.djvuviewer/server_config.yaml.
        """
        config_dir = DjVuConfig.get_config_dir()
        config_path = str(config_dir / "server_config.yaml")
        return config_path

    @classmethod
    def of_yaml(cls, yaml_path: str = None) -> "ServerConfig":
        """
        Load and return ServerConfig from the standard config path.
        """
        if yaml_path is None:
            yaml_path = cls.get_config_path()
        server_config = cls.load_from_yaml_file(yaml_path)
        return server_config


class ServerProfile:
    """
    Profile server by checking test files.
    """

    def __init__(
        self,
        config: ServerConfig = None,
        yaml_path: str = None,
        debug: bool = False,
        verbose: bool = False,
    ):
        if config is None:
            self.config = ServerConfig.of_yaml(yaml_path)
        else:
            self.config = config
        self.djvu_config = DjVuConfig.get_instance()
        self.debug = debug
        self.verbose = verbose

    def check_djvu(
        self, server: Server, imagefolder: ImageFolder, djvu: TestFile
    ) -> Optional[float]:
        """
        Check a single djvu file in the given imagefolder on the given server.
        Sets bundled on djvu and returns djvudumpMs.

        Args:
            server: The server to check.
            imagefolder: The image folder to check.
            djvu: The djvu test file to check.

        Returns:
            djvudumpMs or None if check failed.
        """
        remote = Remote(host=server.hostname)
        relpath = self.djvu_config.normalize_relpath(djvu.path)
        filepath = f"{imagefolder.path}{relpath}"
        cmd = f"djvudump {filepath}"
        profiler = Profiler(f"{cmd}", profile=self.debug or self.verbose)
        result = remote.run(cmd)
        output = result.stdout
        elapsed_sec = profiler.time()
        djvudump_ms = round(elapsed_sec * 1000, 1)

        if output is None:
            return None

        if "bundled" in output:
            djvu.bundled = True
        elif "indirect" in output:
            djvu.bundled = False
        return djvudump_ms

    def imagefolder_gen(self):
        """
        generate a loop over all servers and imagefolders
        """
        for _server_name, server in self.config.servers.items():
            for _image_name, imagefolder in server.imagefolders.items():
                yield server, imagefolder

    def show(self, tablefmt: str):
        slod = []
        ilod = []
        for server, imagefolder in self.imagefolder_gen():
            ilod.append(imagefolder.to_dict())
        LodShow.show(ilod, tablefmt)

    def run(self):
        """
        Check all test files on all servers/image stores.
        """
        for server, imagefolder in self.imagefolder_gen():
            for tf in self.config.test_files:
                if self.debug:
                    print(f"checking sever {server.hostname} ...")
                djvudump_ms = self.check_djvu(server, imagefolder, tf)
                if djvudump_ms:
                    imagefolder.djvudumpMs = djvudump_ms

    def save(self) -> None:
        """
        save back to server_config.yaml.
        """
        self.config.save_to_yaml_file(ServerConfig.get_config_path())
