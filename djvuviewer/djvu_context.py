"""
Created on 2026-01-04

@author: wf
"""

from argparse import Namespace

from ngwidgets.progress import Progressbar

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_files import DjVuFiles
from djvuviewer.djvu_processor import DjVuProcessor
from djvuviewer.packager import PackageMode


class DjVuContext:
    """
    a Context for working with DjVu files and actions
    """

    def __init__(self, config: DjVuConfig, args: Namespace):
        self.config = config
        self.args = args
        # Initialize manager and processor
        self.djvu_files = DjVuFiles(config=self.config)
        self.package_mode = PackageMode.from_name(self.config.package_mode)
        self.dproc = DjVuProcessor(
            debug=self.args.debug,
            verbose=self.args.verbose,
            package_mode=self.package_mode,
            batch_size=self.args.batch_size,
            limit_gb=self.args.limit_gb,
            max_workers=self.args.max_workers,
            pngmode=self.args.pngmode,
        )

    def warmup_image_cache(self, pbar: Progressbar):
        """
        Pre-fetch caches for both wikis with progressbar
        """
        self.djvu_files.fetch_images(
            url=self.config.base_url, name="wiki", limit=10000, progressbar=pbar
        )

        if self.config.new_url:
            self.djvu_files.fetch_images(
                url=self.config.new_url, name="new", limit=10000, progressbar=pbar
            )

    def load_djvu_file(self, page_title: str, progressbar=None) -> DjVuBundle:
        """
        Load DjVu file metadata via DjVuFiles interface.

        Args:
            page_title: The page title to load
            progressbar: Optional progressbar for feedback

        Returns:
            DjVuBundle: Bundle with loaded images and DjVu file data

        Raises:
            ValueError: If image not found in any wiki
            Exception: For other loading errors
        """
        # Fetch image metadata from wikis using DjVuFiles
        mw_images = {}

        wiki_images = self.djvu_files.fetch_images(
            url=self.config.base_url, name="wiki", titles=[page_title]
        )
        if wiki_images:
            mw_images["wiki"] = wiki_images[0]

        if self.config.new_url:
            new_images = self.djvu_files.fetch_images(
                url=self.config.new_url, name="new", titles=[page_title]
            )
            if new_images:
                mw_images["new"] = new_images[0]

        if not mw_images:
            raise ValueError(f"Image not found in any wiki: {page_title}")

        # Use first available image to determine path
        active_image = next(iter(mw_images.values()))
        djvu_file = self.dproc.get_djvu_file(
            url=active_image.url, config=self.config, progressbar=progressbar
        )

        # Create bundle with MediaWiki metadata
        djvu_bundle = DjVuBundle(
            djvu_file, config=self.config, debug=self.args.debug, mw_images=mw_images
        )

        return djvu_bundle
