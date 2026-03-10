"""
Created on 2026-02-21

MediaWiki Server handling

@author: wf
"""

from copy import copy
from dataclasses import dataclass, field
import os
from pathlib import Path
import subprocess
import time
from typing import Dict, List, Optional, Tuple

from basemkit.profiler import Profiler
from basemkit.yamlable import lod_storable
from djvuviewer.content_indexer import ContentIndexer
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import BaseFile, DjVu
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.lod_show import LodShow
from djvuviewer.mw_hash import MediaWikiHash
from mwstools_backend.remote import Remote, RunConfig
from ngwidgets.progress import TqdmProgressbar


@dataclass
class Bucket(MediaWikiHash):
    """
    A MediaWiki hash bucket with its server image path and local cache path.
    """

    image_path: str = ""
    cache_path: str = ""

    @property
    def bucket_path(self) -> str:
        """
        Full path of the bucket directory on the server.

        Returns:
            Absolute path combining image_path and the hash path.
        """
        bucket_path = f"{self.image_path}/{self.path}"
        return bucket_path

    @property
    def cache_file(self):
        """
        Local cache file path for this bucket.

        Returns:
            Path object for the cache file.
        """
        cache_file = Path(self.cache_path) / f"{self.hash_value}.txt"
        return cache_file

    @classmethod
    def of_index(cls, bucket_index: int, image_path: str, cache_path: str) -> "Bucket":
        """
        Create a Bucket from a bucket index and paths.

        Args:
            bucket_index: Integer 0-255 identifying the MediaWiki hash bucket.
            image_path: Server-side image folder path.
            cache_path: Local cache directory path.

        Returns:
            Bucket instance.
        """
        mw_hash = MediaWikiHash.of_value(bucket_index)
        bucket = cls(
            hash_value=mw_hash.hash_value, image_path=image_path, cache_path=cache_path
        )
        return bucket


@lod_storable
class DjVuToBeMigrated(DjVu):
    """
    DjVu File ready for migration
    """

    ready: bool = False
    min_uncompressed: float = 0

    @property
    def bundled_marker(self) -> str:
        """
        Returns a Unicode icon indicating if the item is bundled (📦) or not bundled (🔗)
        """
        marker = "📦" if self.bundled else "🔗"
        return marker

    def check_readiness(self):
        # check the readiness conditions
        # Check if file is bundled based on size heuristic.
        # Test data from 0/00 bucket:
        # - Bundled: 1,868,800 / 300,694 = 6.2 (ratio < 20)
        # - Bundled: 98,442,273 / 7,536,104 = 13.1 (ratio < 20)
        # - Stub: 25,726,780 / 118 = 218,024 (ratio > 200,000)
        if not self.filesize:
            self.ready = False
            self.compression_ratio = -1
            pass
        else:
            cr = self.min_uncompressed / self.filesize
            self.compression_ratio = cr
            self.ready = cr < 1000  # heuristic
            if self.ready:
                self.bundled = True


@lod_storable
class ImageFolder:
    """An mediawiki Wiki image folder on a server."""

    path: str
    fs: str
    total: Optional[int] = None
    migrated: Optional[int] = None
    cache: Optional[bool] = False
    cache_expiration: int = 86400
    statMs: Optional[float] = None
    readMs: Optional[float] = None
    speedMBs: Optional[float] = None
    djvudumpMs: Optional[float] = None

    def get_cache_dir(self):
        """
        get my cache directory
        """
        cache_dir = DjVuConfig.get_config_dir() / "djvu_filelists" / self._name
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def expiration_of_bucket(self, bucket_index: int) -> float:
        """
        Return the seconds until the cache file for the given bucket expires.

        Args:
            bucket_index: Integer 0-255 identifying the MediaWiki hash bucket.

        Returns:
            0       if the cache file does not exist or is empty
            > 0     seconds remaining until expiry
            < 0     seconds past expiry (already expired)
        """
        bucket = Bucket.of_index(bucket_index, self.path, str(self.get_cache_dir()))
        cache_file = bucket.cache_file
        if not cache_file.exists() or cache_file.stat().st_size == 0:
            return 0
        age = time.time() - cache_file.stat().st_mtime
        expiration = self.cache_expiration - age
        return expiration


@lod_storable
class Server:
    """A server with one or more image stores."""

    hostname: str
    os: str
    latencyMs: float
    imagefolders: Dict[str, ImageFolder] = field(default_factory=dict)
    is_local: Optional[bool] = False

    def run_remote(self, cmd: str, debug: bool = False) -> subprocess.CompletedProcess:
        """
        Execute a command on the remote server.

        Args:
            cmd: Command to execute on the remote server
            debug: If True, show command output during execution

        Returns:
            CompletedProcess

        Raises:
            Exception: If the remote command execution fails
        """
        try:
            remote = Remote(host=self.hostname)
            run_config = RunConfig(tee=debug, do_log=debug)
            result = remote.run(cmd, run_config=run_config)
        except Exception as ex:
            raise Exception(f"remote command on {self.hostname} failed: {str(ex)}")
        return result

    def set_remote_fileinfo(
        self, base_file: BaseFile, file_path: str, debug: bool = False
    ) -> None:
        """
        Set fileinfo fields on a BaseFile instance from remote server metadata.

        Args:
            base_file: BaseFile instance to update (filesize, iso_date, filename)
            file_path: Absolute path to the file on the remote server
            debug: If True, show command output during execution
        """
        try:
            remote = Remote(host=self.hostname)
            stats = remote.get_file_stats(file_path)
            if stats is None:
                base_file.filesize = -1
            else:
                base_file.filename = stats.basename
                base_file.filesize = stats.size
                base_file.iso_date = stats.modified_iso
        except Exception as ex:
            if debug:
                print(f"set_remote_fileinfo for {file_path} failed: {str(ex)}")

    def find_djvu_images(
        self,
        imagefolder_name: str,
        bucket_index: int,
        debug: bool = False,
    ) -> List[str]:
        """
        Find all .djvu files in a single hash bucket subfolder of an image
        folder on a remote server.  Results are cached in a per-bucket file
        under the config cache directory and reused
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
        bucket = Bucket.of_index(
            bucket_index, imagefolder.path, str(imagefolder.get_cache_dir())
        )
        if imagefolder.expiration_of_bucket(bucket_index) > 0:
            filelist = bucket.cache_file.read_text().splitlines()
            return filelist

        cmd = (
            f"find {bucket.bucket_path} -type f -name '*.djvu'"
            f" -printf '%p|%s|%T+|%C+|%u|%g|%m\\n' | iconv -f iso-8859-1 -t utf-8//IGNORE 2>/dev/null"
        )
        try:
            remote_result = self.run_remote(cmd, debug=debug)
            raw_output = remote_result.stdout
            filelist = sorted(
                line.strip() for line in raw_output.splitlines() if line.strip()
            )
            bucket.cache_file.write_text("\n".join(filelist))
        except Exception as ex:
            print(f"caching {bucket.bucket_path} failed with {str(ex)}")

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
    Server configuration loaded from the config directory.
    Only image stores marked target: true are writable for migration.
    """

    # singleton
    _instance: Optional["ServerConfig"] = None

    folders: Dict[str, SetupLocation] = field(default_factory=dict)
    files_db_path: Optional[str] = None
    test_files: List[TestFile] = field(default_factory=list)
    servers: Dict[str, Server] = field(default_factory=dict)
    migration_script: Optional[str] = None

    def __post_init__(self):
        """
        Set files_db_path default relative to the config directory if not provided.
        """
        if not self.files_db_path:
            self.files_db_path = str(DjVuConfig.get_config_dir() / "files_cache.db")

    @classmethod
    def get_config_path(cls) -> str:
        """
        Returns the standard location for server_config.yaml.
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
        Otherwise load from the standard config path.

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

    def cache_filelists(self, limit: int = 256, progress_bar: Optional = None):
        """
        Cache filelists for all image folders, bucketed by all 256 MediaWiki
        hash values.  Each bucket's file records are fetched via find -printf
        and stored in a per-bucket cache file.

        Only caches imagefolders where imagefolder.cache is True.

        Args:
            progress_bar: Optional progress bar instance (e.g. a
                NiceGUI or tqdm progress bar) that will be advanced once per
                bucket via .update(1).  Pass None to skip.
        """
        imagefolders = [imgf for imgf in self.imagefolder_gen() if imgf.cache]
        if progress_bar is not None:
            progress_bar.total = limit * len(imagefolders)
        for imagefolder in imagefolders:
            server = imagefolder._server
            if self.debug:
                print(
                    f"fetching djvu files for {server.hostname} {imagefolder.path} ..."
                )
            if progress_bar is not None:
                progress_bar.set_description(f"{server.hostname}:{imagefolder._name}")
            for bucket_index in range(limit):
                filelist = server.find_djvu_images(
                    imagefolder._name, bucket_index, debug=self.debug or self.verbose
                )
                if progress_bar is not None:
                    progress_bar.update(1)
                yield imagefolder, filelist

    def index_filelists(
        self, limit: Optional[int] = 256, with_progress: bool = False
    ) -> int:
        """
        Cache and import all bucket filelist files into a single SQLite database
        via ContentIndexer. Consumes cache_filelists generator internally.

        Args:
            limit: Number of hash buckets to process (0-255, default 256).
            with_progress: Whether to show a progress bar.

        Returns:
            Total number of records imported.
        """
        files_db_path = self.config.files_db_path
        if not files_db_path:
            raise ValueError("files_db_path not set in server_config")
        indexer = ContentIndexer(db_path=files_db_path)
        total_imported = 0
        progress_bar = None
        if with_progress:
            imagefolders_count = len(
                [imgf for imgf in self.imagefolder_gen() if imgf.cache]
            )
            total = limit * imagefolders_count
            progress_bar = TqdmProgressbar(
                total=total,
                desc="indexing filelists",
                unit="buckets",
            )
        for imagefolder, filelist in self.cache_filelists(
            limit=limit, progress_bar=progress_bar
        ):
            if filelist:
                imported = indexer.import_lines(
                    lines=filelist, directory=imagefolder._name
                )
                total_imported += imported
        return total_imported

    def cache_expiration(self) -> float:
        """
        Return the number of seconds until the next cache refresh is due,
        based on the oldest cache file across all imagefolders and their
        cache_expiration settings.

        Returns:
            0     if any expected cache file is missing (cache incomplete)
            > 0   seconds remaining until the oldest cache file expires
            < 0   seconds past expiry of the oldest cache file (already expired)
        """
        oldest_remaining = None
        for imagefolder in self.imagefolder_gen():
            for bucket_index in range(256):
                remaining = imagefolder.expiration_of_bucket(bucket_index)
                if remaining == 0:
                    return 0
                if oldest_remaining is None or remaining < oldest_remaining:
                    oldest_remaining = remaining
        if oldest_remaining is None:
            return 0
        return oldest_remaining

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
        relpath = self.djvu_config.normalize_relpath(djvu.path)
        filepath = f"{imagefolder.path}{relpath}"
        cmd = f"djvudump {filepath}"
        profiler = Profiler(f"{cmd}", profile=self.debug or self.verbose)
        remote_result = server.run_remote(cmd, debug=self.debug or self.verbose)
        output = remote_result.stdout
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
        file_tomigrate.min_uncompressed = (
            size_records[0].get("min_uncompressed", 0) if size_records else 0
        )
        return file_tomigrate

    def print_status(self, message: str):
        """
        Print status message to stdout and optionally to logfile.

        Args:
            message: Status message to print
        """
        print(message)
        if self.logfile:
            with open(self.logfile, "a", encoding="utf-8") as log_file:
                log_file.write(message + "\n")

    def show_migration_plan(
        self,
        files_tomigrate: List[DjVuToBeMigrated],
        execute: bool,
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
            normalized_relpath = self.djvu_config.normalize_relpath(df.path)
            source_path = (
                f"{source_server.hostname}:{source_folder.path}{normalized_relpath}"
            )
            target_path = (
                f"{target_server.hostname}:{target_folder.path}{normalized_relpath}"
            )
            df.check_readiness()
            original_iso_date = df.iso_date
            if not df.ready:
                # according to the database record the file is small let's check the real situation
                source_file_path = f"{source_folder.path}{normalized_relpath}"
                source_server.set_remote_fileinfo(
                    df, source_file_path, debug=self.debug or self.verbose
                )
                # reassess
                df.check_readiness()
            if not df.ready:
                self.print_status(
                    f"❌ {df.bundled_marker} {df.path}:cr={df.compression_ratio:.1f} "
                )
            else:
                scp_command = f"scp -p {source_path} {target_path}"
                relpath = self.djvu_config.normalize_relpath(df.path)
                # Touch back the file if the copied file is newer than original
                touch_date = (
                    original_iso_date
                    if (
                        df.iso_date
                        and original_iso_date
                        and df.iso_date > original_iso_date
                    )
                    else ""
                )
                script = (
                    f"{self.config.migration_script} {relpath} {touch_date}"
                    if self.config.migration_script
                    else ""
                )

                # Check if file already exists on target
                target_file_check = copy(df)
                target_file_path = f"{target_folder.path}{normalized_relpath}"
                target_server.set_remote_fileinfo(
                    target_file_check, target_file_path, debug=False
                )

                file_exists_on_target = (
                    target_file_check.filesize and target_file_check.filesize > 0
                )

                if file_exists_on_target:
                    self.print_status(
                        f"ℹ️  {df.bundled_marker}: {df.path}: file already exists on target (size: {target_file_check.filesize:,} bytes) - skipping"
                    )
                else:
                    self.print_status(
                        f"✅ {df.bundled_marker}: {df.path}: {scp_command} {script}"
                    )
                    pass
                    # print(f"  Page count: {checks.get('page_count', 'N/A')}")
                    # print(f"  Filesize: {checks.get('filesize', 0):,} bytes")
                    # print(f"  Min uncompressed: {checks.get('min_uncompressed', 0):,} bytes")
                    # print(f"  Bundled (DB): {checks.get('bundled_db', False)}")
                    # print(f"  Bundled (size): {checks.get('bundled_size', False)}")

                    if execute:
                        print(f"{scp_command}")
                        source_server.run_remote(
                            scp_command, debug=self.debug or self.verbose
                        )
                        if script:
                            print(f"{script}")
                            target_server.run_remote(
                                script, debug=self.debug or self.verbose
                            )
                        pass
