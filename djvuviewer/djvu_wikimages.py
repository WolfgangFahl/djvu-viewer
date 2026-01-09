"""
Created on 2026-01-02

@author: wf
"""
from ngwidgets.progress import Progressbar
from dataclasses import field
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from typing import List, Optional

from basemkit.yamlable import lod_storable
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.wiki_images import MediaWikiImages, MediaWikiImage


class DjVuMediaWikiImages:
    """
    MediaWiki images handler
    """

    @classmethod
    def get_mediawiki_images_client(self, url: str) -> MediaWikiImages:
        """
        get the images client for the given url
        """
        mw_client = None
        if url:
            api_epp = "api.php"
            base = url if url.endswith("/") else f"{url}/"
            mw_client = MediaWikiImages(
                api_url=f"{base}{api_epp}",
                mime_types=("image/vnd.djvu", "image/x-djvu"),
                timeout=10,
            )
        return mw_client

@lod_storable
class DjVuImagesCache:
    """
    a cache for MediaWiki images from a given url
    """
    name:str
    url: str
    images: List[MediaWikiImage] = field(default_factory=list)
    last_fetch: Optional[datetime] = None
    mw_client: Optional[MediaWikiImages] = field(default=None, init=False, repr=False, compare=False, metadata={'exclude': True})

    def __post_init__(self):
        """Restore mw_client after deserialization from cache if needed"""
        if self.mw_client is None and self.url:
            self.mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(self.url)

    @classmethod
    def get_cache_file(cls, config: DjVuConfig, name:str="wiki",ext:str="json") -> str:
        base_dir = Path(config.cache_path) if getattr(config, 'cache_path', None) else Path.home() / ".djvuviewer" / "cache"
        base_dir.mkdir(parents=True, exist_ok=True)
        cache_file=str(base_dir / f"djvu_images_{name}.{ext}")
        return cache_file

    @classmethod
    def from_cache(cls, config:DjVuConfig,url:str,name:str,
        limit:int=10000,freshness_days: int = 1,
        progressbar:Progressbar=None) -> "DjVuImagesCache":
        cache_file, cache = cls.get_cache_file(config,name), None
        if os.path.exists(cache_file):
            cache = cls.load_from_json_file(cache_file)
            if (datetime.now(timezone.utc) - cache.last_fetch.astimezone(timezone.utc)) < timedelta(days=freshness_days):
                return cache

        if progressbar:
            progressbar.desc = f"Fetching djvu {name} images to be cached from ... {url}"

        mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(url)
        images=mw_client.fetch_allimages(limit=limit, as_objects=True,progressbar=progressbar)
        cache = cls(images=images,url=url,name=name, last_fetch=datetime.now())
        cache.mw_client=mw_client
        cache.save_to_json_file(cache_file)
        return cache
