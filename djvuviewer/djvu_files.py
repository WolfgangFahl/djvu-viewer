"""
Created on 2026-01-05

@author: wf
"""

import os
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from lodstorage.lod import LOD
from ngwidgets.progress import Progressbar
from ngwidgets.widgets import Link

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVu, DjVuFile, DjVuPage
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_wikimages import DjVuImagesCache, DjVuMediaWikiImages
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

        # cache for images by filename
        self.images_by_filename: Dict[str, Dict[str, MediaWikiImage]] = {}

        # Client instances: {name_or_url: DjVuMediaWikiImages}
        self.mw_clients: Dict[str, DjVuMediaWikiImages] = {}

        # Cache instances: {name_or_url: DjVuImagesCache}
        self.caches: Dict[str, DjVuImagesCache] = {}

        # silently track errors
        self.errors = []

        self.lod = None
        # SQL db based
        if self.config.db_path:
            empty = False
            # Remove broken 0-byte database file if it exists
            if os.path.exists(self.config.db_path):
                if os.path.getsize(self.config.db_path) == 0:
                    os.remove(self.config.db_path)
                    empty = True
            self.dvm = DjVuManager(config=self.config)
            if not empty:
                self.dvm.migrate_to_package_fields()

    def in_cache(self, filename: str, name: str = "wiki") -> Tuple[bool, str]:
        """
        Check if a file exists in the cache.

        Args:
            filename: Name of the file to check
            name: Which cache to check - "wiki" or "new"

        Returns:
            Tuple of (is_in_cache, normalized_filename)
        """
        images_by_filename = self.images_by_filename.get(name)

        if not images_by_filename:
            return False, filename

        if filename in images_by_filename:
            return True, filename

        # Try with underscores replaced by spaces
        if "_" in filename:
            spaced_filename = filename.replace("_", " ")
            if spaced_filename in images_by_filename:
                return True, spaced_filename

        return False, filename


    def add_link(self, view_record: Dict[str, Any], filename: str, new: bool = False) -> None:
        """Add a wiki link to the view record."""
        name = "new" if new else "wiki"
        url = self.config.wiki_fileurl(filename, new=new)

        is_cached, normalized_filename = self.in_cache(filename, name)
        link_style = Link.blue if is_cached else Link.red

        link = Link.create(url=url, text=normalized_filename, style=link_style)
        view_record[name] = link

    def add_links(self, view_record: Dict[str, any], filename: str):
        """
        Add the DjVu links.
        """
        config = self.config
        if filename:
            self.add_link(view_record, filename, new=False)

            if config.new_url:
                self.add_link(view_record, filename, new=True)
                backlink = self.config.wiki_fileurl(filename, new=True, quoted=True)
            backparam = f"?backlink={backlink}" if backlink else ""
            local_url = f"{config.url_prefix}/djvu/{filename}{backparam}"
            archive_name = filename.replace(".djvu", "." + self.config.package_mode)
            view_record["Package"] = Link.create(url=local_url, text=archive_name)

            debug_url = f"{config.url_prefix}/djvu/debug/{filename}"
            view_record["debug"] = Link.create(url=debug_url, text="🔍")

    def get_djvu_files_by_path(
        self,
        paths: Optional[List[str]] = None,
        file_limit: int = None,
        page_limit: int = None,
        progressbar: Progressbar = None,
    ) -> Dict[str, DjVuFile]:
        """
        Retrieve all DjVu file and page records from the database
        using the all_djvu and all_pages queries and reassemble
        the DjVuFile objects

        Args:
            paths: Optional list of specific paths to fetch. If None, fetches all files.
            file_limit: Maximum number of files to fetch (when paths is None)
            page_limit: Maximum number of pages per file
            progressbar: Optional Progressbar instance

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
                if page_limit > 0:
                    djvu_page_records = self.dvm.query(
                        "all_pages_for_path",
                        param_dict={"djvu_path": djvu_file.path, "limit": page_limit},
                    )
                    for djvu_page_record in djvu_page_records:
                        djvu_page = DjVuPage.from_dict( # @UndefinedVariable
                            djvu_page_record
                        )
                        djvu_file.pages.append(djvu_page)
            if progressbar:
                progressbar.update(1)

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

    def add_to_cache(
        self, key: str, images: List[MediaWikiImage], replace: bool = False
    ):
        """
        Add images to the cache, with optional replacement of existing entries.
        Automatically updates relpath and filename lookups using LOD.

        Args:
            key: Cache key (usually wiki name or URL)
            images: List of MediaWikiImage objects to add
            replace: If True, replace existing images with same relpath.
                    If False, append new images.
        """
        # Ensure list exists
        if key not in self.images:
            self.images[key] = []

        if replace and self.images[key]:
            # Merge strategy implies we need a temporary map to handle overwrites
            # 1. Map existing images by relpath
            existing_map = {
                img.relpath: img
                for img in self.images[key]
                if hasattr(img, "relpath") and img.relpath
            }

            # 2. Update map with new images (this performs the replacement)
            for img in images:
                if hasattr(img, "relpath") and img.relpath:
                    existing_map[img.relpath] = img
                else:
                    # If image has no relpath, we can't key it, so just append
                    self.images[key].append(img)

            # 3. Write back values
            self.images[key] = list(existing_map.values())
        else:
            # Append mode
            self.images[key].extend(images)

        # Rebuild all lookups (relpath and filename) securely
        self.refresh_lookups(key)

    def refresh_lookups(self, key: str):
        """
        Rebuild auxiliary indices (relpath, filename) for a specific cache key
        using the generic LOD lookup generator.
        """
        if key not in self.images:
            return

        img_list = self.images[key]

        # 1. Primary Lookup: Relpath
        # generic getLookup returns (lookup_dict, list_of_duplicates). We only need the dict.
        self.images_by_relpath[key], _ = LOD.getLookup(img_list, "relpath")

        # 2. Secondary Lookup: Filename (New requirement)
        self.images_by_filename[key], _ = LOD.getLookup(img_list, "filename")

    def fetch_images(
        self,
        url: str,
        name: Optional[str] = None,
        titles: Optional[List[str]] = None,
        limit: int = 50000,
        refresh: bool = False,
        progressbar=None,
    ) -> List[MediaWikiImage]:
        """
        Fetch images for a specific wiki. Can be called with just the name
        if the client was already initialized, or a fresh URL.

        Args:
            url: The MediaWiki base URL.
            name: Short alias for this wiki instance.
            titles: Optional list of specific image titles to fetch.
                If provided, fetches detailed metadata for these images,
                replacing any existing cache entries with the same title.
            limit: Max images to fetch when fetching all images.
            refresh: Force API call even if cached.
            progressbar: Optional progress bar for cache operations.

        Returns:
            List[MediaWikiImage]: The list of MediaWiki image metadata objects.
                If titles is specified, returns only those newly fetched images.
                Otherwise returns all images from the cache.
        """
        # Use name as cache key if provided, otherwise use URL
        key = name if name else url

        # Determine cache freshness: 0 days = force refresh, 1 day = use cache if fresh
        freshness_days = 0 if refresh else 1

        # Get or create cache for this wiki
        cache = DjVuImagesCache.from_cache(
            config=self.config,
            url=url,
            name=name or key,
            limit=limit,
            freshness_days=freshness_days,
            progressbar=progressbar,
        )

        # Store cache for future access
        self.caches[key] = cache

        # Handle specific titles if requested
        if titles:
            current_images = []

            # Create title-indexed mapping for O(1) replacement instead of O(n) list searches
            images_dict = {img.title: img for img in cache.images}

            # Fetch detailed metadata for each requested title
            for title in titles:
                img = cache.mw_client.fetch_image(title)
                if img:
                    current_images.append(img)
                    # Per-title fetches typically include more detailed metadata than bulk operations
                    images_dict[title] = img

            # Update cache with merged image data
            cache.images = list(images_dict.values())

            # Update internal cache with replacement semantics
            self.add_to_cache(key, current_images, replace=True)

            # Return only the newly fetched images
            return current_images

        # No specific titles requested: add all cache images to internal cache
        self.add_to_cache(key, cache.images, replace=False)

        # Return all images from cache
        return cache.images

    def get_client(self, url: str, name: Optional[str] = None) -> DjVuMediaWikiImages:
        """
        Get or create a MediaWiki client for the given URL/name.

        Args:
            url: The MediaWiki base URL
            name: Optional short alias for this wiki instance

        Returns:
            DjVuMediaWikiImages: The client instance
        """
        key = name if name else url

        if key not in self.mw_clients:
            self.mw_clients[key] = DjVuMediaWikiImages(
                config=self.config, url=url, name=name
            )

        return self.mw_clients[key]

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
        self.store_lods(djvu_lod, page_lod, sample_record_count)

    def store_lods(
        self,
        djvu_lod: List[Dict[str, Any]],
        page_lod: Optional[List[Dict[str, Any]]] = None,
        sample_record_count: int = 1,
        with_drop: bool = False,
    ) -> None:
        """
        Store DjVu and page records in the database.

        Args:
            djvu_lod: List of DjVu file records
            page_lod: List of page records - if None do not store
            sample_record_count: Number of sample records for schema inference
            with_drop: If True, drop existing tables before creating new ones
        """
        if page_lod:
            self.dvm.store(
                lod=page_lod,
                entity_name="Page",
                primary_key="page_key",
                with_drop=with_drop,
                sampleRecordCount=sample_record_count,
            )
        self.dvm.store(
            lod=djvu_lod,
            entity_name="DjVu",
            primary_key="path",
            with_drop=with_drop,
            sampleRecordCount=sample_record_count,
        )
        pass

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
        self.store_lods(djvu_lod, page_lod, sample_record_count=1, with_drop=True)

    def get_diff(self, name_a: str, name_b: str) -> List[MediaWikiImage]:
        """
        Get symmetric difference of images between two wikis.

        Computes the set of images that exist in one wiki but not the other,
        effectively finding images unique to each source.

        Args:
            name_a: Name/key of first wiki
            name_b: Name/key of second wiki

        Returns:
            List of MediaWikiImage objects that exist in exactly one of the two wikis,
            sorted by relpath
        """
        map_a = self.images_by_relpath[name_a]
        map_b = self.images_by_relpath[name_b]

        # Symmetric difference: elements in either set but not in both
        diff_keys = map_a.keys() ^ map_b.keys()

        # Collect image objects from whichever source contains them
        diff_objs = []
        for k in diff_keys:
            obj = map_a[k] if k in map_a else map_b[k]
            diff_objs.append(obj)

        return sorted(diff_objs, key=lambda x: x.relpath)
