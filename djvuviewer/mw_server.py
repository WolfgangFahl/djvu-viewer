"""
Created on 2026-02-21

MediaWiki Server handling

@author: wf
"""

from dataclasses import field
import pathlib
import subprocess
import time
from typing import Dict, List, Optional

from basemkit.yamlable import lod_storable
from djvuviewer.djvu_core import DjVu


@lod_storable
class ImageFolder:
    """An mediawiki Wiki image folder on a server."""

    path: str
    fs: str
    total: Optional[int] = None
    migrated: Optional[int] = None
    target: bool = False
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


@lod_storable
class TestFile(DjVu):
    """
    a test file
    """

    bundled_size: Optional[int] = None
    unbundled_stub_size: Optional[int] = None

    @property
    def hash_path(self) -> Optional[str]:
        """Extract hash path from path (e.g., 'c/c7' from '/images/c/c7/file.djvu')."""
        if self.path:
            parts = self.path.rsplit("/", 2)
            if len(parts) >= 2:
                return parts[-2]
        return None

    @property
    def name(self) -> Optional[str]:
        """Extract filename from path (e.g., 'file.djvu')."""
        if self.path:
            return self.path.rsplit("/", 1)[-1]
        return None


@lod_storable
class ServerConfig:
    """
    Server configuration loaded from ~/.djvuviewer/server_config.yaml.
    Only image stores marked target: true are writable for migration.
    """

    test_files: List[TestFile] = field(default_factory=list)
    servers: Dict[str, Server] = field(default_factory=dict)

    @classmethod
    def get_config_path(cls) -> str:
        """
        Returns the standard location: ~/.djvuviewer/server_config.yaml.
        """
        return str(pathlib.Path.home() / ".djvuviewer" / "server_config.yaml")

    @classmethod
    def of_yaml(cls, yaml_path: str = None) -> "ServerConfig":
        """
        Load and return ServerConfig from the standard config path.
        """
        if yaml_path is None:
            yaml_path = cls.get_config_path()
        server_config = cls.load_from_yaml_file(cls.get_config_path())
        return server_config


class ServerTester:
    """
    Runs DjVu file checks across all servers and image stores in ServerConfig.
    """

    def __init__(self, server_config: ServerConfig):
        """
        Initialize with a ServerConfig instance.

        Args:
            server_config: Loaded server configuration.
        """
        self.server_config = server_config

    def check_djvu(
        self, server: Server, imagefolder: ImageFolder, djvu: TestFile
    ) -> Optional[float]:
        """
        Check a single djvu file in the given imagefolder on the given server.
        Sets bundled on djvu and returns djvudumpMs.

        Args:
            server: The server to check on.
            imagefolder: The image folder to check.
            djvu: The djvu test file to check.

        Returns:
            djvudumpMs or None if check failed.
        """
        filepath = f"{imagefolder.path}/{djvu.hash_path}/{djvu.name}"
        is_local = server.is_local
        try:
            if is_local:
                djvu.set_fileinfo(filepath)
                stat_cmd = None
            else:
                stat_cmd = ["ssh", server.hostname, f"stat -c '%s' {filepath}"]
            t0 = time.time()
            if stat_cmd:
                stat_proc = subprocess.run(stat_cmd, capture_output=True, text=True)
                if stat_proc.returncode != 0:
                    return None
            cmd = (
                ["djvudump", filepath]
                if is_local
                else ["ssh", server.hostname, f"djvudump {filepath}"]
            )
            proc = subprocess.run(cmd, capture_output=True, text=True)
            djvudump_ms = round((time.time() - t0) * 1000, 1)

            output = proc.stdout
            if "bundled" in output:
                djvu.bundled = True
            elif "indirect" in output:
                djvu.bundled = False
            return djvudump_ms
        except Exception:
            return None

    def run(self):
        """
        Check all test files on all servers/image stores.
        """
        for _server_name, server in self.server_config.servers.items():
            for _image_name, imagefolder in server.imagefolders.items():
                for tf in self.server_config.test_files:
                    djvudump_ms = self.check_djvu(server, imagefolder, tf)
                    if djvudump_ms:
                        imagefolder.djvudumpMs = djvudump_ms

    def write_back(self) -> None:
        """
        Run checks and save back to server_config.yaml.
        """
        self.run()
        self.server_config.save_to_yaml_file(ServerConfig.get_config_path())
