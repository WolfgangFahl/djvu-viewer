"""
Created on 2026-01-02

@author: wf
"""

import os
from dataclasses import field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from basemkit.yamlable import lod_storable
from ngwidgets.progress import Progressbar

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.wiki_images import MediaWikiImage, MediaWikiImages


class DjVuMediaWikiImages:
    """
    MediaWiki images handler
    """

    @classmethod
    def get_mediawiki_images_client(cls, url: str) -> MediaWikiImages:
        """
        Get the images client for the given url.

        Args:
            url: MediaWiki base URL

        Returns:
            MediaWikiImages client instance or None
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
    Cache for MediaWiki images from a given url.
    """

    name: str
    url: str
    images: List[MediaWikiImage] = field(default_factory=list)
    last_fetch: Optional[datetime] = None

    def __post_init__(self):
        """Initialize transient (non-serializable) attributes."""
        self._mw_client = None

    def is_fresh(self, freshness_days: int) -> bool:
        """
        Check if cache is still fresh and has data.

        Args:
            freshness_days: Number of days before cache is considered stale

        Returns:
            True if cache is fresh, False otherwise
        """
        if self.last_fetch is None:
            return False

        fresh = (
            datetime.now(timezone.utc) - self.last_fetch.astimezone(timezone.utc)
        ) < timedelta(days=freshness_days)
        return fresh

    @property
    def mw_client(self) -> MediaWikiImages:
        """Lazy initialization of MediaWiki client."""
        if self._mw_client is None and self.url:
            self._mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(self.url)
        return self._mw_client

    @classmethod
    def get_cache_file(
        cls, config: DjVuConfig, name: str = "wiki", ext: str = "json"
    ) -> str:
        """
        Get the cache file path for the given config and name.

        Args:
            config: DjVu configuration
            name: Cache identifier
            ext: File extension

        Returns:
            Path to cache file
        """
        base_dir = (
            Path(config.cache_path)
            if getattr(config, "cache_path", None)
            else Path.home() / ".djvuviewer" / "cache"
        )
        base_dir.mkdir(parents=True, exist_ok=True)
        cache_file = str(base_dir / f"djvu_images_{name}.{ext}")
        return cache_file

    @classmethod
    def from_cache(
        cls,
        config: DjVuConfig,
        url: str,
        name: str,
        limit: int = 10000,
        freshness_days: int = 1,
        progressbar: Progressbar = None,
    ) -> "DjVuImagesCache":
        """
        Load cache from file if fresh, otherwise fetch new data.

        Args:
            config: DjVu configuration
            url: MediaWiki base URL
            name: Cache identifier
            limit: Maximum number of images to fetch
            freshness_days: Days before cache is considered stale
            progressbar: Optional progress bar

        Returns:
            DjVuImagesCache instance
        """
        cache_file = cls.get_cache_file(config, name)

        # Try to load from cache
        if os.path.exists(cache_file):
            cache = cls.load_from_json_file(cache_file)
            if cache.is_fresh(freshness_days):
                return cache

        # Cache missing or stale - fetch fresh data
        if progressbar:
            progressbar.desc = (
                f"Fetching djvu {name} images to be cached from ... {url}"
            )

        mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(url)
        images = mw_client.fetch_allimages(
            limit=limit, as_objects=True, progressbar=progressbar
        )

        cache = cls(
            images=images, url=url, name=name, last_fetch=datetime.now(timezone.utc)
        )
        cache._mw_client = mw_client
        cache.save_to_json_file(cache_file)
        return cache
