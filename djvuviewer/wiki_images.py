"""
Created on 2026-01-02

@author: wf
"""

import logging
import re
from dataclasses import field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union
from urllib.parse import unquote

import requests
from basemkit.yamlable import lod_storable
from ngwidgets.progress import Progressbar

from djvuviewer.version import Version

logger = logging.getLogger(__name__)


@lod_storable
class MediaWikiImage:
    """
    Represents a single image resource from MediaWiki.
    """

    url: str
    mime: str
    size: int
    page_id: int = (
        -1
    )  # if negative the page might be deleted or redirected of fetched via allimages
    user: Optional[str] = None
    timestamp: Optional[str] = None
    description_url: Optional[str] = None
    height: Optional[int] = None
    width: Optional[int] = None
    pagecount: Optional[int] = None
    descriptionurl: Optional[str] = None
    descriptionshorturl: Optional[str] = None
    ns: Optional[int] = None
    title: Optional[str] = None
    # key
    relpath: Optional[str] = field(init=False, default=None)
    filename: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        if self.url:
            self.relpath = MediaWikiImage.relpath_of_url(self.url)
        if self.title:
            # Split on colon and take everything after the last colon
            # This handles cases like "Project:File:Example.jpg" or "File talk:Example.jpg"
            self.filename = self.title.split(":")[-1].strip()

    @property
    def timestamp_datetime(self):
        # Format: 2013-04-10T20:03:08Z
        # replacing Z with +00:00 for timezone awareness if running Python 3.7+
        ts_str = self.timestamp.replace("Z", "+00:00")
        ts_dt = datetime.fromisoformat(ts_str)
        return ts_dt

    @classmethod
    def relpath_of_url(cls, url: str) -> str:
        """retrieve wiki image-relative path from url by removing './' and '/images/'."""
        path = unquote(url)
        # Look for 'images/' anywhere in the path and extract everything after it
        match = re.search(r"images/(.*)", path)

        if match:
            # Extract the part after 'images/' and prepend '/'
            cleaned_path = "/" + match.group(1)
        else:
            # No 'images/' found - just handle './' prefix
            if path.startswith("./"):
                cleaned_path = "/" + path[2:]
            else:
                cleaned_path = path

        # Remove duplicate slashes
        cleaned_path = re.sub(r"/+", "/", cleaned_path)

        return cleaned_path


class MediaWikiImages:
    """
    Fetch images from a MediaWiki API via the 'allimages' list or by title.

    Example:
        client = MediaWikiImages(
            api_url="https://genwiki.genealogy.net/w/api.php",
            mime_types=("image/vnd.djvu", "image/x-djvu"),
            aiprop=("url", "mime", "size", "timestamp", "user"),
            timeout=20,
        )
        # Fetch list of images
        images = client.fetch_allimages(limit=120)

        # Fetch single image details
        single_img = client.fetch_image("File:Example.djvu")
    """

    def __init__(
        self,
        api_url: str,
        mime_types: Optional[Iterable[str]] = None,
        aiprop: Optional[Iterable[str]] = None,
        timeout: int = 30,
        session: Optional[requests.Session] = None,
    ):
        """
        Args:
            api_url: Full API endpoint, e.g. 'https://genwiki.genealogy.net/w/api.php'
            mime_types: MIME types to filter by (joined with '|'). e.g. ("image/jpeg",).
            aiprop: Properties to request. Defaults to url, mime, size, timestamp, user, dimensions.
            timeout: Per-request timeout in seconds.
            session: Optional requests.Session to reuse connections.
        """
        self.api_url = api_url
        self.timeout = timeout
        self.session = session or requests.Session()

        # Default filters
        self.mime_types = tuple(mime_types) if mime_types else ()
        # User agent
        self.user_agent = f"{Version.name}/{Version.version}"
        # Default properties if none provided
        if aiprop is None:
            self.aiprop = ("url", "mime", "size", "timestamp", "user", "dimensions")
        else:
            self.aiprop = tuple(aiprop)

    def parse_image_response(self, data: Dict, title: str) -> MediaWikiImage:
        """Parse API response dict to MediaWikiImage object."""
        pages = data.get("query", {}).get("pages", {})
        logger.info(f"API Response for title '{title}':")
        logger.info(f"Full data: {data}")
        mw_image = None
        for page_id, page_data in pages.items():
            imageinfo = page_data.get("imageinfo", [])
            if imageinfo:
                # Merge page title into the image info dict to match 'allimages' structure
                info_dict = imageinfo[0]
                info_dict["title"] = page_data.get("title")
                info_dict["page_id"] = page_id
                mw_image = MediaWikiImage.from_dict(info_dict)
        return mw_image

    def fetch_image(self, title: str) -> Optional[MediaWikiImage]:
        """
        Retrieve a single image by its file title (e.g., 'File:Example.jpg').

        Args:
            title: The exact page title of the file.

        Returns:
            MediaWikiImage object if found, None otherwise.
        """
        if not ":" in title:
            title = "File:" + title
        params = {
            "action": "query",
            "prop": "imageinfo",
            "titles": title,
            "format": "json",
            "iiprop": "|".join(self.aiprop),
        }

        data = self._make_request(params)
        mw_image = self.parse_image_response(data, title)
        return mw_image

    def fetch_allimages(
        self,
        limit: int,
        per_request: int = 50,
        extra_params: Optional[Dict[str, str]] = None,
        as_objects: bool = False,
        progressbar: Optional[Progressbar] = None,
    ) -> Union[List[MediaWikiImage], List[Dict]]:
        """
        Retrieve up to 'limit' images.

        Args:
            limit: Maximum number of image records to return.
            per_request: Page size for each API call (max 50 normally, 500 for bots).
            extra_params: Extra query params to merge into the request.
            as_objects: If True, returns List[MediaWikiImage]. If False, returns List[Dict].

        Returns:
            List of MediaWikiImage objects or dictionaries.
        """
        if progressbar:
            progressbar.total = limit or 0  # Set total if known

        results = []
        remaining = max(0, int(limit))
        if remaining == 0:
            return results

        base_params = {
            "action": "query",
            "list": "allimages",
            "format": "json",
        }

        # Apply Configuration filters
        if self.mime_types:
            base_params["aimime"] = "|".join(self.mime_types)
        if self.aiprop:
            base_params["aiprop"] = "|".join(self.aiprop)

        if extra_params:
            base_params.update(extra_params)

        continue_params: Dict[str, str] = {}

        while remaining > 0:
            params = dict(base_params)
            params.update(continue_params)
            params["ailimit"] = min(per_request, remaining)

            data = self._make_request(params)

            # Extract list
            images_raw = data.get("query", {}).get("allimages", [])
            if not images_raw:
                break

            # Append up to 'remaining'
            take = min(remaining, len(images_raw))
            batch = images_raw[:take]

            if as_objects:
                results.extend([MediaWikiImage.from_dict(img) for img in batch])
            else:
                results.extend(batch)
            # Update progress bar
            if progressbar:
                progressbar.update(len(batch))

            remaining -= take

            # Handle pagination
            cont = data.get("continue")
            if remaining > 0 and cont:
                continue_params = cont
            else:
                break

        return results

    def build_size_filter(
        self, min_size: Optional[int] = None, max_size: Optional[int] = None
    ) -> str:
        """
        Build a CirrusSearch filesize filter string.

        Args:
            min_size: Minimum file size in kilo bytes
            max_size: Maximum file size in kilo bytes

        Returns:
            CirrusSearch filter string (e.g., " filesize:1000..5000")
        """
        if min_size and max_size:
            return f" filesize:{min_size}..{max_size}"
        elif min_size:
            return f" filesize:>{min_size}"
        elif max_size:
            return f" filesize:<{max_size}"
        return ""

    def fetch_titles_by_cirrus(
        self, search_query: str, limit: int = 50, per_request: int = 50
    ) -> List[str]:
        """
        Get file titles using CirrusSearch.

        Args:
            search_query: CirrusSearch query (e.g., "filemime:image/vnd.djvu")
            limit: Maximum number of titles to return
            per_request: Results per API call

        Returns:
            List of file titles
        """
        titles = []
        remaining = limit
        continue_params = {}

        while remaining > 0:
            params = {
                "action": "query",
                "list": "search",
                "srsearch": search_query,
                "srnamespace": 6,  # File namespace
                "srlimit": min(per_request, remaining),
                "format": "json",
            }
            params.update(continue_params)

            data = self._make_request(params)

            results = data.get("query", {}).get("search", [])
            if not results:
                break

            titles.extend([r["title"] for r in results])
            remaining -= len(results)

            if remaining > 0 and data.get("continue"):
                continue_params = data["continue"]
            else:
                break

        return titles

    def fetch_images_by_titles(
        self, titles: List[str], progressbar: Optional[Progressbar] = None
    ) -> List[MediaWikiImage]:
        """
        Fetch full image details for a list of titles.

        Args:
            titles: List of file titles (e.g., ["File:Example.jpg"])
            progressbar: Optional progress bar

        Returns:
            List of MediaWikiImage objects with full details
        """
        images = []

        # Fetch in batches (API limit: 50 titles per request)
        for i in range(0, len(titles), 50):
            batch = titles[i : i + 50]
            params = {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "imageinfo",
                "iiprop": "|".join(self.aiprop),
                "format": "json",
            }

            data = self._make_request(params)
            pages = data.get("query", {}).get("pages", {})

            for page_id, page_data in pages.items():
                imageinfo = page_data.get("imageinfo", [])
                if imageinfo:
                    # Merge page title into the image info dict to match 'allimages' structure
                    info_dict = imageinfo[0].copy()
                    info_dict["title"] = page_data.get("title")
                    info_dict["page_id"] = page_id
                    images.append(MediaWikiImage.from_dict(info_dict))

            if progressbar:
                progressbar.update(len(batch))

        return images

    def fetch_by_cirrus_search(
        self,
        search_query: str,
        limit: int = 50,
        per_request: int = 50,
        min_size_kb: Optional[int] = None,
        max_size_kb: Optional[int] = None,
        progressbar: Optional[Progressbar] = None,
    ) -> List[MediaWikiImage]:
        """
        Search for files using CirrusSearch, then fetch full image details.

        Args:
            search_query: CirrusSearch query (e.g., "filemime:image/vnd.djvu")
            limit: Maximum number of results
            per_request: Results per API call
            min_size_kb: Minimum file size in kilo bytes (optional)
            max_size_kb: Maximum file size in kilo bytes (optional)
            progressbar: Optional progress bar

        Returns:
            List of MediaWikiImage objects with full details
        """
        # Build size filter and append to query
        size_filter = self.build_size_filter(min_size_kb, max_size_kb)
        full_query = search_query + size_filter

        # Step 1: Get titles via CirrusSearch
        titles = self.fetch_titles_by_cirrus(
            search_query=full_query, limit=limit, per_request=per_request
        )

        # Step 2: Fetch full image details
        images = self.fetch_images_by_titles(titles=titles, progressbar=progressbar)

        return images[:limit]

    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Helper to execute the request and handle basic errors.
        """
        headers = {"User-Agent": f"{self.user_agent} (via {self.__class__.__name__})"}
        resp = self.session.get(
            self.api_url,
            params=params,
            timeout=self.timeout,
            headers=headers,
            allow_redirects=True,
        )
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            code = data["error"].get("code", "unknown")
            info = data["error"].get("info", "")
            raise RuntimeError(f"MediaWiki API error: {code} - {info}")

        return data
