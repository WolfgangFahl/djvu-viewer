"""
Created on 2026-01-02

@author: wf
"""
import urllib.parse
import re
from djvuviewer.wiki_images import MediaWikiImages

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

    @classmethod
    def extract_and_clean_path(cls,url:str)->str:
        """
        URL decode, extract path from /images, and remove duplicate slashes.

        Args:
            url (str): The URL to process

        Returns:
            str: The cleaned path starting from /images
        """
        cleaned_path=None
        # URL decode
        decoded_url = urllib.parse.unquote(url)

        # Extract path from /images using regex
        match = re.search(r'/images/.*', decoded_url)

        if match:
            path = match.group(0)

            # Remove duplicate slashes
            cleaned_path = re.sub(r'/+', '/', path)

        return cleaned_path
