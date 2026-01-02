import requests
from typing import List, Dict, Iterable, Optional

class MediaWikiImages:
    """
    Fetch images from a MediaWiki API via the 'allimages' list.

    Example:
        client = MediaWikiImages(
            api_url="https://genwiki.genealogy.net/w/api.php",
            mime_types=("image/vnd.djvu", "image/x-djvu"),
            aiprop=("url", "mime", "size", "timestamp", "user"),
            timeout=20,
        )
        images = mwi.fetch_allimages(limit=120)  # returns a list of dicts
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
                     You can also use 'https://genwiki.genealogy.net/api.php' (redirects followed).
            mime_types: MIME types to filter by (joined with '|'). If None, no filter is applied.
            aiprop: Properties to request for each image. If None, defaults to common fields.
            timeout: Per-request timeout in seconds.
            session: Optional requests.Session to reuse connections.
        """
        self.api_url = api_url
        self.timeout = timeout
        self.session = session or requests.Session()

        # Default filters
        self.mime_types = tuple(mime_types) if mime_types else ()
        if aiprop is None:
            self.aiprop = ("url", "mime", "size", "timestamp", "user")
        else:
            self.aiprop = tuple(aiprop)

    def fetch_allimages(
        self,
        limit: int,
        per_request: int = 50,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> List[Dict]:
        """
        Retrieve up to 'limit' images with details as a list of dicts.

        Args:
            limit: Maximum number of image records to return.
            per_request: Page size for each API call (MediaWiki typical max is 50 for non-bot).
            extra_params: Extra query params to merge into the request.

        Returns:
            List of image dicts (as returned under 'query' -> 'allimages').
        """
        results: List[Dict] = []
        remaining = max(0, int(limit))
        if remaining == 0:
            return results

        base_params = {
            "action": "query",
            "list": "allimages",
            "format": "json",
            # 'continue' behavior: MW returns a dict with tokens; we pass them back verbatim.
        }

        # Filters
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

            resp = self.session.get(self.api_url, params=params, timeout=self.timeout, allow_redirects=True)
            resp.raise_for_status()
            data = resp.json()

            if "error" in data:
                # Raise a helpful exception
                code = data["error"].get("code", "unknown")
                info = data["error"].get("info", "")
                raise RuntimeError(f"MediaWiki API error: {code} - {info}")

            images = data.get("query", {}).get("allimages", [])
            if not images:
                break

            # Append up to 'remaining'
            take = min(remaining, len(images))
            results.extend(images[:take])
            remaining -= take

            # If we still need more, continue only if API gave us a continue token
            cont = data.get("continue")
            if remaining > 0 and cont:
                # Pass all keys from 'continue' back (not just 'aicontinue')
                continue_params = cont
            else:
                break

        return results