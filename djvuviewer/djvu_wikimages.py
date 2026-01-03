"""
Created on 2026-01-02

@author: wf
"""

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
