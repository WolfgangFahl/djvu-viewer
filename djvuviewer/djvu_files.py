"""
Created on 2026-01-05

@author: wf
"""

from dataclasses import asdict
from typing import Any, Dict, List, Optional

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVu, DjVuFile, DjVuPage
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_wikimages import DjVuMediaWikiImages
from djvuviewer.packager import Packager
from djvuviewer.wiki_images import MediaWikiImage


class DjVuFiles:
    """
    Handler for a list of DjVu Files from various MediaWiki sources.
    """

    def __init__(self, config: DjVuConfig):
        """
        Initialize the DjvuFiles handler.

        Args:
            config: Configuration object containing cache paths and default settings.
        """
        self.config = config
        # Cache for djvu_files_by path
        self.djvu_files_by_path = {}

        # Cache for image lists: {name_or_url: [image_dict, ...]}
        self.images: Dict[str, List[MediaWikiImage]] = {}
        # cache for images by relative key
        self.images_by_relpath: Dict[str, Dict[str, MediaWikiImage]] = {}

        # Client instances: {name_or_url: DjVuMediaWikiImages}
        self.mw_clients: Dict[str, DjVuMediaWikiImages] = {}

        # silently track errors
        self.errors = []

        self.lod = None
        # SQL db based
        if self.config.db_path:
            self.dvm = DjVuManager(config=self.config)
            self.dvm.migrate_to_package_fields()

    def get_client(self, url: str, name: Optional[str] = None) -> DjVuMediaWikiImages:
        """
        Get or create a MediaWiki client. If a 'name' is provided, the client
        is registered under that alias for future easy access.

        Args:
            url: The MediaWiki base URL.
            name: An optional short alias (e.g., 'prod', 'new').

        Returns:
            DjVuMediaWikiImages: The initialized client.
        """
        key = name if name else url

        if key not in self.mw_clients:
            self.mw_clients[key] = DjVuMediaWikiImages.get_mediawiki_images_client(url)

        return self.mw_clients[key]

    def get_djvu_files_by_path(
        self,
        paths: Optional[List[str]] = None,
        file_limit: int = None,
        page_limit: int = None,
    ) -> Dict[str, DjVuFile]:
        """
        Retrieve all DjVu file and page records from the database
        using the all_djvu and all_pages queries and reassemble
        the DjVuFile objects

        Args:
            paths: Optional list of specific paths to fetch. If None, fetches all files.
            file_limit: Maximum number of files to fetch (when paths is None)
            page_limit: Maximum number of pages per file

        Returns:
            Dict mapping paths to DjVuFile objects

            e.g. :
            "/images/1/1e/AB1953-Gohr.djvu":
            DjVuFile(
                path="/images/1/1e/AB1953-Gohr.djvu",
                page_count=2,
                dir_pages=1,
                iso_date="2007-09-09T08:33:15+00:00",  # Using iso_date parameter name
                filesize=27733,
                package_iso_date="2025-02-28T04:59:07+00:00",  # Using package_iso_date parameter name
                package_filesize=409600,
                bundled=False,
            )
        """
        if file_limit is None:
            file_limit = 10000
        if paths is None:
            djvu_file_records = self.dvm.query(
                "all_djvu", param_dict={"limit": file_limit}
            )
        else:
            djvu_file_records = []
            for path in paths:
                single_djvu_file_records = self.dvm.query(
                    "djvu_for_path", param_dict={"path": path}
                )
                for record in single_djvu_file_records:
                    djvu_file_records.append(record)

        if page_limit is None and file_limit is None:
            djvu_page_records = self.dvm.query(
                "all_pages", param_dict={"limit": 10000000}
            )
        for djvu_file_record in djvu_file_records:
            djvu_file = DjVuFile.from_dict(djvu_file_record)  # @UndefinedVariable
            self.djvu_files_by_path[djvu_file.path] = djvu_file
            if file_limit is not None:  # query pages per file mode
                if page_limit is None:
                    page_limit = 10000
                djvu_page_records = self.dvm.query(
                    "all_pages_for_path",
                    param_dict={"djvu_path": djvu_file.path, "limit": page_limit},
                )
                for djvu_page_record in djvu_page_records:
                    djvu_page = DjVuPage.from_dict(djvu_page_record)  # @UndefinedVariable
                    djvu_file.pages.append(djvu_page)
        if file_limit is None:  # all mode
            for djvu_page_record in djvu_page_records:
                djvu_page = DjVuPage.from_dict(djvu_page_record)  # @UndefinedVariable
                djvu_file = self.djvu_files_by_path.get(djvu_page.djvu_path, None)
                if djvu_file is None:
                    self.errors.append(
                        f"djvu_file {djvu_page.djvu_path} missing for page {djvu_page.page_index}"
                    )
                else:
                    djvu_file.pages.append(djvu_page)
        return self.djvu_files_by_path

    def add_to_cache(self, key: str, images: List[MediaWikiImage]):
        if key not in self.images:
            self.images[key] = []
        self.images[key].extend(images)

        # cache lookup map
        if key not in self.images_by_relpath:
            self.images_by_relpath[key] = {}

        self.images_by_relpath[key] = {
            img.relpath: img for img in images if img.relpath
        }

    def fetch_images(
        self,
        url: str,
        name: Optional[str] = None,
        titles: Optional[List[str]] = None,
        limit: int = 50000,
        refresh: bool = False,
    ) -> List[MediaWikiImage]:
        """
        Fetch images for a specific wiki. Can be called with just the name
        if the client was already initialized, or a fresh URL.

        Args:
            url: The MediaWiki base URL.
            name: Short alias for this wiki instance.
            titles: Optional list of specific image titles to fetch.
                If provided, only these images are fetched instead of all images.

            limit: Max images to fetch.
            refresh: Force API call even if cached.

        Returns:
            List[MediaWikImage]: The list of MediaWiki image metadata objects.
        """
        key = name if name else url

        # Ensure client exists
        client = self.get_client(url, name)

        if not refresh and key in self.images:
            return self.images[key]

        if titles:
            # Fetch specific images by title
            current_images = []
            for title in titles:
                img = client.fetch_image(title)
                if img:
                    current_images.append(img)
        else:
            # Fetch all images
            current_images = client.fetch_allimages(limit=limit, as_objects=True)

        self.add_to_cache(key, current_images)
        return current_images

    def lookup_djvu_file_by_path(self, path: str) -> Dict[str, DjVuFile]:
        """
        Look up DjVu files by path across all sources.

        Args:
            path: The path to the DjVu file (e.g., "/images/1/1e/AB1953-Gohr.djvu")

        Returns:
            Dictionary mapping source names to DjVuFile objects for all sources
            that have a file at this path. Empty dict if not found in any source.
        """
        results = {}
        for source, images_dict in self.images_by_relpath.items():
            if path in images_dict:
                img = images_dict[path]
                if isinstance(img, DjVuFile):
                    results[source] = img
        return results

    def load_djvufile_from_package(
        self,
        filename: str,
    ) -> DjVuFile:
        """
        Load DjVu file data from a package archive.

        Args:
            filename: filename of  the DjVu file

        Returns:
            Complete DjVuFile object with all page data loaded from package

        Raises:
            FileNotFoundError: If package file does not exist
        """
        package_file = self.config.package_abspath(filename)
        yaml_file = str(package_file).replace(".djvu", ".yaml")

        if not package_file.exists():
            raise FileNotFoundError(f"Package file not found: {package_file}")

        yaml_data = Packager.read_from_package(package_file, yaml_file).decode("utf-8")
        djvu_file = DjVuFile.from_yaml(yaml_data)  # @UndefinedVariable

        return djvu_file

    def load_djvufile_from_djvu(
        self,
        relpath: str,
    ) -> DjVuFile:
        """
        Load DjVu file data directly from a DjVu file.

        Args:
            relpath: Relative path to the DjVu file

        Returns:
            Complete DjVuFile object with all page data loaded from file

        Raises:
            FileNotFoundError: If DjVu file does not exist
        """
        djvu_path = self.config.djvu_abspath(relpath)

        if not djvu_path.exists():
            raise FileNotFoundError(f"DjVu file not found: {djvu_path}")

        return self.dproc.get_djvu_file(djvu_path=djvu_path)

    def store(
        self,
        djvu_files: List[DjVuFile],
        sample_record_count: int = 1,
    ) -> None:
        """
        Store DjVu files and their pages in the database.

        Args:
            djvu_files: List of DjVuFile objects to store
            sample_record_count: Number of sample records for schema inference
        """
        djvu_lod, page_lod = self.get_db_records(djvu_files)
        self._store_lods(djvu_lod, page_lod, sample_record_count)

    def _store_lods(
        self,
        djvu_lod: List[Dict[str, Any]],
        page_lod: List[Dict[str, Any]],
        sample_record_count: int = 1,
    ) -> None:
        """
        Store DjVu and page records in the database.

        Args:
            djvu_lod: List of DjVu file records
            page_lod: List of page records
            sample_record_count: Number of sample records for schema inference
        """
        self.dvm.store(
            lod=page_lod,
            entity_name="Page",
            primary_key="page_key",
            with_drop=True,
            sampleRecordCount=sample_record_count,
        )
        self.dvm.store(
            lod=djvu_lod,
            entity_name="DjVu",
            primary_key="path",
            with_drop=True,
            sampleRecordCount=sample_record_count,
        )

    def get_db_records(
        self,
        djvu_files: List[DjVuFile],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Convert DjVuFile objects to database records.

        Args:
            djvu_files: List of DjVuFile objects to convert

        Returns:
            Tuple of (djvu_lod, page_lod):
                - djvu_lod: List of DjVu file records (without pages)
                - page_lod: List of page records from all files
        """
        djvu_lod = []
        page_lod = []

        for djvu_file in djvu_files:
            # Convert DjVuFile to dict
            djvu_record = asdict(djvu_file)

            # Remove pages from djvu record (they're stored separately)
            djvu_record.pop("pages", None)
            djvu_lod.append(djvu_record)

            # Extract all page records
            for page in djvu_file.pages:
                page_record = asdict(page)
                page_lod.append(page_record)

        return djvu_lod, page_lod

    def init_database(self) -> None:
        """
        Initialize the database with sample records.

        Creates the database schema using sample DjVu and page records.
        """
        djvu_record = asdict(DjVu.get_sample())
        djvu_lod = [djvu_record]
        page_record = asdict(DjVuPage.get_sample())
        page_lod = [page_record]
        self.store(djvu_lod, page_lod, sample_record_count=1)

    def get_diff(self, name_a: str, name_b: str) -> List[MediaWikiImage]:
        """
        get symmetric diff
        """
        map_a = self.images_by_relpath[name_a]
        map_b = self.images_by_relpath[name_b]

        # Use ^ instead of - to get ALL differences
        diff_keys = map_a.keys() ^ map_b.keys()

        diff_objs = []
        for k in diff_keys:
            # Grab the object from whichever list has it
            obj = map_a[k] if k in map_a else map_b[k]
            diff_objs.append(obj)

        return sorted(diff_objs, key=lambda x: x.relpath)
