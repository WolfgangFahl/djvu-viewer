"""
Created on 2026-01-02

@author: wf
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from ngwidgets.progress import Progressbar


@dataclass
class MediaWikiImage:
    """
    Represents a single image resource from MediaWiki.
    """

    name: str
    url: str
    mime: str
    size: int
    user: Optional[str] = None
    timestamp: Optional[datetime] = None
    description_url: Optional[str] = None
    height: Optional[int] = None
    width: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MediaWikiImage":
        """
        Factory method to create an instance from a MediaWiki API result dict.
        """
        # Parse timestamp if available, usually ISO 8601 format like "2023-01-01T12:00:00Z"
        ts_str = data.get("timestamp")
        ts = None
        if ts_str:
            try:
                # Basic ISO parsing
                ts = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass

        mw_image = cls(
            name=data.get("name") or data.get("title", "?"),
            url=data.get("url", ""),
            mime=data.get("mime", "application/octet-stream"),
            size=data.get("size", 0),
            user=data.get("user", None),
            timestamp=ts,
            description_url=data.get("descriptionurl"),
            height=data.get("height"),
            width=data.get("width"),
        )
        return mw_image


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

        # Default properties if none provided
        if aiprop is None:
            self.aiprop = ("url", "mime", "size", "timestamp", "user", "dimensions")
        else:
            self.aiprop = tuple(aiprop)

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
        pages = data.get("query", {}).get("pages", {})
        mw_image = None
        for page_id, page_data in pages.items():
            # If page_id is negative (e.g., "-1"), the page does not exist
            if int(page_id) < 0:
                continue

            imageinfo = page_data.get("imageinfo", [])
            if imageinfo:
                # Merge page title into the image info dict to match 'allimages' structure
                info_dict = imageinfo[0]
                info_dict["title"] = page_data.get("title")
                mw_image = MediaWikiImage.from_dict(info_dict)

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

    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Helper to execute the request and handle basic errors.
        """
        resp = self.session.get(
            self.api_url, params=params, timeout=self.timeout, allow_redirects=True
        )
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            code = data["error"].get("code", "unknown")
            info = data["error"].get("info", "")
            raise RuntimeError(f"MediaWiki API error: {code} - {info}")

        return data
