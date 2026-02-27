"""
Created on 2026-02-21

MediaWiki Server handling

@author: wf
"""

import os
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
from djvuviewer.mw_hash import MediaWikiHash
from djvuviewer import mw_hash


@lod_storable
class DjVuToBeMigrated(DjVu):
    """
    DjVu File ready for migration
    """

    ready: bool = False


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
        mw_hash: MediaWikiHash,
    ) -> List[str]:
        """
        Find all .djvu files in a single hash bucket subfolder of an image
        folder on a remote server.  Results are cached in
        ~/.djvuviewer/{imagefolder_name}/{hash_value}/filelist.txt and reused
        until the cache is older than imagefolder.cache_expiration seconds.

        The remote find command uses -printf to emit pipe-separated metadata:
            path|size|mtime|ctime|user|group|mode

        Args:
            imagefolder_name: Short name used as the local cache directory.
            mw_hash: The hash bucket (e.g. "c/c7") to search within.

        Returns:
            List of pipe-separated file records found in that bucket.
        """
        filelist = []
        imagefolder = self.imagefolders.get(imagefolder_name)
        if not imagefolder:
            raise Exception(f"invalid image folder name {imagefolder_name}")
        cache_dir = DjVuConfig.get_config_dir() / "djvu_filelists" / imagefolder_name
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"{mw_hash.hash_value}.txt"

        cache_valid = (
            cache_file.exists()
            and cache_file.stat().st_size > 0
            and (time.time() - cache_file.stat().st_mtime)
            < imagefolder.cache_expiration
        )
        if cache_valid:
            filelist = cache_file.read_text().splitlines()
            return filelist

        bucket_path = f"{imagefolder.path}/{mw_hash.path}"
        cmd = (
            f"find {bucket_path} -type f -name '*.djvu'"
            f" -printf '%p|%s|%T+|%C+|%u|%g|%m\\n' | iconv -f iso-8859-1 -t utf-8//IGNORE 2>/dev/null"
        )
        try:
            remote = Remote(host=self.hostname)
            result = remote.run(cmd)
            raw_output = result.stdout or ""
            filelist = sorted(
                line.strip() for line in raw_output.splitlines() if line.strip()
            )
            cache_file.write_text("\n".join(filelist))
        except Exception as ex:
            print(f"caching {bucket_path} failed with {str(ex)}")

        return filelist


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

    # singleton
    _instance: Optional["ServerConfig"] = None

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
        Preserves any leading header comments for round-trip fidelity.
        """
        if yaml_path is None:
            yaml_path = cls.get_config_path()
        server_config = cls.load_from_yaml_file(yaml_path, with_header_comment=True)
        return server_config

    @classmethod
    def of_example(cls) -> "ServerConfig":
        """
        Load and return ServerConfig from the bundled examples directory.
        Parallel to DjVuConfig.get_instance(test=True).
        Preserves any leading header comments for round-trip fidelity.
        """
        examples_path = DjVuConfig.get_examples_path()
        yaml_path = os.path.join(examples_path, "server_config.yaml")
        server_config = cls.load_from_yaml_file(yaml_path, with_header_comment=True)
        return server_config

    @classmethod
    def get_instance(cls, test: bool = False) -> "ServerConfig":
        """
        Get the ServerConfig singleton instance.

        If test=True or no config file exists, load from the bundled examples.
        Otherwise load from ~/.djvuviewer/server_config.yaml.

        Args:
            test: If True, always use the example config (skips user config file).

        Returns:
            ServerConfig instance.
        """
        if cls._instance is None:
            config_path = cls.get_config_path()
            if os.path.exists(config_path) and not test:
                server_config = cls.of_yaml(config_path)
            else:
                server_config = cls.of_example()
            cls._instance = server_config
        return cls._instance


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

    def cache_filelists(self, limit: int = 256, progress_bar=None):
        """
        Cache filelists for all image folders, bucketed by all 256 MediaWiki
        hash values.  Each bucket's file records are fetched via find -printf
        and stored in a per-bucket cache file.

        Args:
            progress_bar: Optional progress bar instance (e.g. a
                NiceGUI or tqdm progress bar) that will be advanced once per
                bucket via .update(1).  Pass None to skip.
        """
        for imagefolder in self.imagefolder_gen():
            server = imagefolder._server
            if self.debug:
                print(
                    f"fetching djvu files for {server.hostname} {imagefolder.path} ..."
                )
            if progress_bar is not None:
                progress_bar.total = limit
            for value in range(limit):
                mw_hash = MediaWikiHash.of_value(value)
                filelist = server.find_djvu_images(imagefolder._name, mw_hash)
                if progress_bar is not None:
                    progress_bar.update(1)

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
        save back to server_config.yaml, preserving the header comment.
        """
        self.config.save_to_yaml_file(
            ServerConfig.get_config_path(), with_header_comment=True
        )

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

    def files_tomigrate(
        self, pattern: str, limit: Optional[int] = None
    ) -> List[DjVuToBeMigrated]:
        """
        Filter files matching pattern for DjVu files needing to be migrated

        Args:
            pattern: Path pattern to match (e.g. "0/00")
            limit: Optional limit on number of files to check
            progress_bar: Optional progress_bar

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

    def create_file_tomigrate(self, djvu_record: Dict[str, object]) -> DjVuToBeMigrated:
        """
        Decide whether the given djvu_record is ready for migration.

        Args:
            djvu_record: Dictionary containing DjVu file metadata fields
                (path, page_count, bundled, filesize, package_filesize,
                iso_date, package_iso_date).

        Returns:
            DjVuToBeMigrated: Migration candidate with ready flag set accordingly.
        """
        file_tomigrate = DjVuToBeMigrated(
            path=djvu_record.get("path"),
            page_count=djvu_record.get("page_count"),
            bundled=djvu_record.get("bundled"),
            filesize=djvu_record.get("filesize"),
            package_filesize=djvu_record.get("package_filesize"),
            iso_date=djvu_record.get("iso_date"),
            package_iso_date=djvu_record.get("package_iso_date"),
        )
        size_records = self.djvu_manager.query(
            "min_uncompressed_for_path", {"djvu_path": file_tomigrate.path}
        )
        min_uncompressed = (
            size_records[0].get("min_uncompressed", 0) if size_records else 0
        )
        # check the readiness conditions
        # Check if file is bundled based on size heuristic.
        # Test data from 0/00 bucket:
        # - Bundled: 1,868,800 / 300,694 = 6.2 (ratio < 20)
        # - Bundled: 98,442,273 / 7,536,104 = 13.1 (ratio < 20)
        # - Stub: 25,726,780 / 118 = 218,024 (ratio > 200,000)
        if not file_tomigrate.filesize:
            file_tomigrate.ready = False
            file_tomigrate.compression_ratio = -1
            pass
        else:
            file_tomigrate.compression_ratio = (
                min_uncompressed / file_tomigrate.filesize
            )

        return file_tomigrate

    def show_migration_plan(
        self, files_tomigrate: List[DjVuToBeMigrated], execute: bool
    ):
        """
        Display migration plan with check results and scp commands.

        Args:
            file_tomigrate: List of files to migrate
            execute: If True, execute scp; if False, show dry-run
        """
        source_server, source_folder = self.get_folder_server("source")
        target_server, target_folder = self.get_folder_server("target")

        for df in files_tomigrate:
            if not df.ready:
                print(f"❌ {df.compression_ratio:.1f}")
            else:
                scp = self.generate_scp_command(
                    source_server, source_folder, target_server, target_folder, df.path
                )
                print(f"✅ {df.path}: {scp}")
                # print(f"  Page count: {checks.get('page_count', 'N/A')}")
                # print(f"  Filesize: {checks.get('filesize', 0):,} bytes")
                # print(f"  Min uncompressed: {checks.get('min_uncompressed', 0):,} bytes")
                # print(f"  Bundled (DB): {checks.get('bundled_db', False)}")
                # print(f"  Bundled (size): {checks.get('bundled_size', False)}")

                if execute:
                    print(f"{scp}")
                    remote = Remote(host=source_server.hostname)
                    remote.run(scp)
