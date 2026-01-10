"""
Created on 2026-01-04

@author: wf
"""

from argparse import Namespace

from ngwidgets.progress import Progressbar

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
