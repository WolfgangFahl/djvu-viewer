"""
Created on 2026-02-21

MediaWiki Server handling

@author: wf
"""

from dataclasses import field
import time
from typing import Dict, List, Optional, Tuple

from basemkit.profiler import Profiler
from basemkit.yamlable import lod_storable
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVu
from djvuviewer.djvu_manager import DjVuManager
from mwstools_backend.remote import Remote

from djvuviewer.lod_show import LodShow
from dataclasses import dataclass

@dataclass
class MediaWikiHash:
    """
    support MediaWiki file hash encoding
    """
    hash:str

    @property
    def path(self)->str:
        return path

    @property
    def value(self)->int:
        return value

    @classmethod
    def of_value(cls,value:int):

    @classmethod
    def of_filename(cls,str):



@lod_storable
class DjVuToBeMigrated(DjVu):
    """
    DjVu File ready for migration
    """

    @classmethod
    def hash_to_pattern(cls,hash:str):
        """
        convert a mediawiki hash value to a pattern
        """



@lod_storable
class ImageFolder:
    """An mediawiki Wiki image folder on a server."""

    path: str
    fs: str
    total: Optional[int] = None
    migrated: Optional[int] = None
    cache_expiration: int = 86400
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
    ) -> List[str]:
        """
        Find all .djvu files in the hex subdirectories (0-f) of an image folder
        on a remote server.  Results are cached in
        ~/.djvuviewer/{imagefolder_name}/djvu_filelist.txt and reused until
        the cache is older than *expiry_secs* seconds.

        Args:
            server: The server that hosts the image folder.
            imagefolder_name: Short name used as the local cache directory.

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
            and (time.time() - cache_file.stat().st_mtime)
            < imagefolder.cache_expiration
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
        imagefolder.total = len(djvu_paths)
        return djvu_paths

    def check_bundled_by_size(self, filesize: int, min_uncompressed: int) -> bool:
        """
        Check if file is bundled based on size heuristic.

        Test data from 0/00 bucket:
        - Bundled: 1,868,800 / 300,694 = 6.2 (ratio < 20)
        - Bundled: 98,442,273 / 7,536,104 = 13.1 (ratio < 20)
        - Stub: 25,726,780 / 118 = 218,024 (ratio > 200,000)

        Threshold: compression_ratio > 100 indicates stub file (safe margin between 20 and 200,000).

        Args:
            filesize: Actual file size in bytes
            min_uncompressed: Minimum uncompressed size (from calculate_min_uncompressed_size)

        Returns:
            True if bundled, False if stub
        """
        # Avoid division by zero
        if filesize == 0 or min_uncompressed == 0:
            is_bundled = False
            return is_bundled

        compression_ratio = min_uncompressed / filesize

        # Stub detection: impossibly high compression ratio (> 1000:1) or tiny filesize (< 1000 bytes)
        is_stub = compression_ratio > 1000 or filesize < 1000
        is_bundled = not is_stub
        return is_bundled


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
        self.djvu_manager = DjVuManager(self.djvu_config)
        self.debug = debug
        self.verbose = verbose
        self.filelist_of_folder = {}

    def imagefolder_gen(self):
        """
        generate a loop over all servers and imagefolders
        """
        for server_name, server in self.config.servers.items():
            server._name = server_name
            for imagefolder_name, imagefolder in server.imagefolders.items():
                imagefolder._name = imagefolder_name
                imagefolder._server = server
                yield imagefolder

    def cache_filelists(self):
        """
        cache filelists for all folders
        """
        for imagefolder in self.imagefolder_gen():
            server = imagefolder._server
            if self.debug:
                print(
                    f"fetching djvu files for {server.hostname} {imagefolder.path} ..."
                )
            filelist = server.find_djvu_images(imagefolder._name)
            self.filelist_of_folder[imagefolder._name] = filelist

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

    def show(self, tablefmt: str) -> None:
        """
        Show server and imagefolder information in tables.

        Args:
            tablefmt: tabulate format string (e.g. simple, grid, github).
        """
        slod = []
        server_set = set()
        ilod = []
        for imagefolder in self.imagefolder_gen():
            server = imagefolder._server
            if server.hostname not in server_set:
                server_set.add(server.hostname)
                server_dict = server.to_dict()
                server_dict["imagefolder"] = imagefolder._name
                slod.append(server_dict)
            imagefolder_dict = imagefolder.to_dict()
            imagefolder_dict["server"] = server.hostname
            ilod.append(imagefolder_dict)
        print("=== Servers ===")
        LodShow.show(slod, tablefmt)
        print("=== Image Folders ===")
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

    def get_folder_server(self, folder_role: str) -> Tuple[Server, ImageFolder]:
        """
        Get server and imagefolder for a given role.

        Args:
            folder_role: Role from config.folders (e.g. "source", "test", "target")

        Returns:
            Tuple of (Server, ImageFolder) for the specified role
        """
        setup_location = self.config.folders.get(folder_role)
        if not setup_location:
            raise ValueError(f"No folder configuration for role: {folder_role}")

        server_name = setup_location.server
        folder_name = setup_location.folder

        server = self.config.servers.get(server_name)
        if not server:
            raise ValueError(f"Server not found: {server_name}")

        imagefolder = server.imagefolders.get(folder_name)
        if not imagefolder:
            raise ValueError(
                f"ImageFolder not found: {folder_name} on server {server_name}"
            )

        result = (server, imagefolder)
        return result

    def generate_scp_command(
        self,
        source_server: Server,
        source_folder: ImageFolder,
        target_server: Server,
        target_folder: ImageFolder,
        relpath: str,
    ) -> str:
        """
        Generate scp command for file migration.

        Args:
            source_server: Source server
            source_folder: Source imagefolder
            target_server: Target server
            target_folder: Target imagefolder
            relpath: Relative path of file (e.g. "/0/00/File.djvu")

        Returns:
            SCP command string
        """
        normalized_relpath = self.djvu_config.normalize_relpath(relpath)
        source_path = (
            f"{source_server.hostname}:{source_folder.path}{normalized_relpath}"
        )
        target_path = (
            f"{target_server.hostname}:{target_folder.path}{normalized_relpath}"
        )
        scp_command = f"scp {source_path} {target_path}"
        return scp_command


    def files_tomigrate(self, pattern: str, limit: Optional[int] = None) -> List[DjVuToBeMigrated]:
        """
        Filter files matching pattern for DjVu files needing to be migrated

        Args:
            pattern: Path pattern to match (e.g. "0/00")
            limit: Optional limit on number of files to check

        Returns:
            List of DjVu files to migrate
        """
        # Query djvu database for files matching pattern
        query_limit = limit if limit else 10000
        djvu_records = self.djvu_manager.query(
            "djvu_by_path_pattern", {"pattern": pattern, "limit": query_limit}
        )

        # Check eligibility for each
        djvu_files = []
        for djvu_record in djvu_records:
            djvu_files.append(self.create_file_tomigrate(djvu_record))

        return djvu_files

    def create_file_tomigrate(self,djvu_record:Dict[str,object])->DjVuToBeMigrated:
        """

        """
        file_tomigrate=DjVuToBeMigrated()
        djvu_path=djvu_record.get("path")
        page_records = self.djvu_manager.query(
            "all_pages_for_path", {"djvu_path": djvu_path, "limit": 10000}
        )
        return file_tomigrate



    def show_migration_plan(self, eligible_files: List[dict], execute: bool):
        """
        Display migration plan with check results and scp commands.

        Args:
            eligible_files: List of eligibility check results
            execute: If True, execute scp; if False, show dry-run
        """
        source_server, source_folder = self.get_folder_server("source")
        target_server, target_folder = self.get_folder_server("target")

        for file_result in eligible_files:
            path = file_result["path"]
            eligible = file_result["eligible"]
            reason = file_result["reason"]
            checks = file_result.get("checks", {})

            print(f"\n{'=' * 80}")
            print(f"File: {path}")
            print(f"  Page count: {checks.get('page_count', 'N/A')}")
            print(f"  Filesize: {checks.get('filesize', 0):,} bytes")
            print(f"  Min uncompressed: {checks.get('min_uncompressed', 0):,} bytes")
            print(f"  Bundled (DB): {checks.get('bundled_db', False)}")
            print(f"  Bundled (size): {checks.get('bundled_size', False)}")
            print(f"  Eligible: {eligible}")
            print(f"  Reason: {reason}")

            if eligible:
                scp_command = self.generate_scp_command(
                    source_server, source_folder, target_server, target_folder, path
                )
                if execute:
                    print(f"  EXECUTING: {scp_command}")
                    remote = Remote(host=source_server.hostname)
                    remote.run(scp_command)
                else:
                    print(f"  DRY-RUN: would execute: {scp_command}")
