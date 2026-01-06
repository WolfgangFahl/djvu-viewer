"""
Created on 2026-01-05

@author: wf
"""

from typing import Dict, List, Optional

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile, DjVuPage
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_wikimages import DjVuMediaWikiImages
from djvuviewer.wiki_images import MediaWikiImage
from lodstorage.lod import LOD


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
        # Cache for image lists: {name_or_url: [image_dict, ...]}
        self.images: Dict[str, List[MediaWikiImage]] = {}
        # cache for images by relative key
        self.images_by_relpath: Dict[str, Dict[str, MediaWikiImage]] = {}

        # Client instances: {name_or_url: DjVuMediaWikiImages}
        self.mw_clients: Dict[str, DjVuMediaWikiImages] = {}

        # silently track errors
        self.errors=[]

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

    def get_djvu_files_by_path(self,file_limit:int=None,page_limit:int=None) -> Dict[str, DjVuFile]:
        """
        Retrieve all DjVu file and page records from the database
        using the all_djvu and all_pages queries and reassemble
        ad DjVuFile objects

        Returns:
        List of DjVuFiles reassembled from the records
            e.g. dict:
            {
                'path':
                '/images/b/b3/AB1951-Suenninghausen.djvu',
                'page_count': 4,
                'bundled': False,
                'iso_date': '2009-06-02',
                'filesize': 85,
                'package_filesize': 0,
                'package_iso_date':
                '2026-01-02',
                'dir_pages': 5
            }
        """
        djvu_files_by_path={}
        if file_limit is None:
            file_limit=10000
        djvu_file_records = self.dvm.query("all_djvu",  param_dict = {"limit":file_limit})
        if page_limit is None and file_limit is None:
            djvu_page_records = self.dvm.query("all_pages", param_dict = {"limit":10000000})
        for djvu_file_record in djvu_file_records:
            djvu_file = DjVuFile.from_dict(djvu_file_record) # @UndefinedVariable
            djvu_files_by_path[djvu_file.path]=djvu_file
            if file_limit is not None: # query pages per file mode
                if page_limit is None:
                    page_limit=10000
                djvu_page_records = self.dvm.query("all_pages_for_path", param_dict = {"djvu_path": djvu_file.path,"limit":page_limit})
                for djvu_page_record in djvu_page_records:
                    djvu_page = DjVuPage.from_dict(djvu_page_record) # @UndefinedVariable
                    djvu_file.pages.append(djvu_page)
        if file_limit is None: # all mode
            for djvu_page_record in djvu_page_records:
                djvu_page = DjVuPage.from_dict(djvu_page_record) # @UndefinedVariable
                djvu_file=djvu_files_by_path.get(djvu_page.djvu_path,None)
                if djvu_file is None:
                    self.errors.append(f"djvu_file {djvu_page.djvu_path} missing for page {djvu_page.page_index}")
                else:
                    djvu_file.pages.append(djvu_page)
        return djvu_files_by_path

    def add_to_cache(self, key: str, images: List[MediaWikiImage]):
        # Update cache
        self.images[key] = images

        # cache lookup map
        self.images_by_relpath[key] = {
            img.relpath: img for img in images if img.relpath
        }

    def fetch_images(
        self,
        url: str,
        name: Optional[str] = None,
        limit: int = 50000,
        refresh: bool = False,
    ) -> List[MediaWikiImage]:
        """
        Fetch images for a specific wiki. Can be called with just the name
        if the client was already initialized, or a fresh URL.

        Args:
            url: The MediaWiki base URL.
            name: Short alias for this wiki instance.
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

        # Fetch actual data
        current_images = client.fetch_allimages(limit=limit, as_objects=True)

        self.add_to_cache(key, current_images)
        return current_images

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
