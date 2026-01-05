"""
Created on 2026-01-05

@author: wf
"""
from typing import Dict, List, Optional
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_wikimages import DjVuMediaWikiImages
from djvuviewer.wiki_images import MediaWikiImage

class DjvuFiles:
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

    def fetch_images(self, url: str, name: Optional[str] = None,
                     limit: int = 50000, refresh: bool = False) -> List[dict]:
        """
        Fetch images for a specific wiki. Can be called with just the name
        if the client was already initialized, or a fresh URL.

        Args:
            url: The MediaWiki base URL.
            name: Short alias for this wiki instance.
            limit: Max images to fetch.
            refresh: Force API call even if cached.

        Returns:
            List[dict]: The list of image metadata.
        """
        key = name if name else url

        # Ensure client exists
        client = self.get_client(url, name)

        if not refresh and key in self.images:
            return self.images[key]

        # Fetch actual data
        current_images = client.fetch_allimages(limit=limit,as_objects=True)

        # Update cache
        self.images[key] = current_images

        # cach lookup map
        self.images_by_relpath[key] = {
            img.relpath: img
            for img in current_images
            if img.relpath
        }


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