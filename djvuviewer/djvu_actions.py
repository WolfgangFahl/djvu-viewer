"""
Created on 2026-01-02

@author: wf
"""

import logging
import os
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from basemkit.profiler import Profiler
from ngwidgets.progress import TqdmProgressbar
from tqdm import tqdm

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_context import DjVuContext
from djvuviewer.djvu_core import DjVu, DjVuFile, DjVuPage
from djvuviewer.djvu_processor import ImageJob
from djvuviewer.djvu_wikimages import DjVuMediaWikiImages
from djvuviewer.wiki_images import MediaWikiImage


class DjVuActions:
    """
    DjVu file processing operations.

    Core functionality for cataloging, converting,
    and managing DjVu files and their metadata in a database.
    """

    def __init__(self, context=DjVuContext):
        """
        Initialize DjVuActions with required components.

        Args:
            context=DjVuContext
        """
        self.context = context

        # Direct references from context
        self.config = context.config
        self.args = context.args
        self.djvu_files = context.djvu_files
        self.dvm = context.djvu_files.dvm
        self.dproc = context.dproc
        self.package_mode = context.package_mode

        # Convenience attributes derived from args
        self.images_path = self.args.images_path
        self.output_path = self.args.output_path
        self.debug = self.args.debug
        self.verbose = self.args.verbose
        self.force = self.args.force

        self.errors: List[Exception] = []

        # Configure processor output path
        if self.output_path:
            self.dproc.output_path = self.output_path
        self.setup_logging()

    def setup_logging(self):
        """Configure file-based logging if log_path is set."""
        self.logger = logging.getLogger("djvu_actions")
        self.logger.setLevel(logging.INFO)
        # Ensure we don't duplicate handlers
        if not self.logger.handlers:
            log_name = f"djvu_{datetime.now():%Y%m%d_%H%M%S}.log"
            log_path = os.path.join(self.config.log_path, log_name)
            fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def get_djvu_files(
        self, url: Optional[str] = None, page_limit: int = None
    ) -> Dict[str, DjVuFile]:
        """
        get DjVu files.

        Args:
            url: Optional single path for processing e.g.
            /images/1/1e/AB1953-Gohr.djvu

        Returns:
            Dict of DjVu file paths to process

        """
        if url is None:
            total_files = self.args.limit if self.args.limit else 10000
            progressbar = TqdmProgressbar(
                total=total_files, desc="Loading DjVu files", unit="files"
            )
            djvu_files_by_path = self.djvu_files.get_djvu_files_by_path(
                file_limit=self.args.limit,
                page_limit=page_limit,
                progressbar=progressbar,
            )
        else:
            djvu_file = self.dproc.get_djvu_file(url=url, config=self.config)
            djvu_files_by_path = {djvu_file.path: djvu_file}
            pass
        return djvu_files_by_path

    def init_database(self):
        """
        initialize the database
        """
        # delegate
        self.djvu_files.init_database()

    def catalog_djvu(self, limit: int = 10000000) -> List[DjVuFile]:
        """
        Catalog DjVu files by scanning and extracting metadata

        This is the first pass operation that reads DjVu files from
        the filesystem and creates database records containing file and page information.

        Args:
            limit: Maximum number of pages to process before stopping

        Returns:
            List of DjVuFile objects with their pages populated
        """
        total = 0
        start_time = time.time()
        djvu_files = []

        images = self.djvu_files.fetch_images(
            self.config.base_url, name="wiki", limit=limit
        )

        for index, image in enumerate(images, start=1):
            full_path = self.config.full_path(image.relpath)

            if not full_path or not os.path.exists(full_path):
                self.errors.append(Exception(f"missing {full_path}"))
                continue

            pages = []
            page_count = 0
            bundled = False

            # Process each page in the document
            for document, page in self.dproc.yield_pages(full_path):
                page_count = len(document.pages)
                page_index = len(pages) + 1

                # Create DjVuPage directly
                try:
                    filename = page.file.name
                    # Check for restricted content marker
                    if "gesperrtes" in filename:
                        filename = "?"
                        valid = False
                    else:
                        valid = True
                except Exception:
                    filename = "?"
                    valid = False

                dpage = DjVuPage(
                    path=filename,
                    page_index=page_index,
                    valid=valid,
                    djvu_path=image.relpath,
                )
                pagefile_path = os.path.join(os.path.dirname(full_path), filename)
                dpage.set_fileinfo(pagefile_path)
                pages.append(dpage)
                bundled = document.type == 2

            iso_date, filesize = ImageJob.get_fileinfo(full_path)
            djvu_file = DjVuFile(
                path=image.relpath,
                page_count=page_count,
                bundled=bundled,
                iso_date=iso_date,
                filesize=filesize,
                pages=pages,  # Attach pages directly
            )
            djvu_files.append(djvu_file)
            total += len(pages)

            if total > limit:
                break

            # Progress reporting
            elapsed = time.time() - start_time
            pages_per_sec = total / elapsed if elapsed > 0 else 0
            print(
                f"{index:4d} {page_count:4d} {total:7d} {pages_per_sec:7.0f} pages/s: {image.relpath}"
            )

        return djvu_files

    def show_fileinfo(self, path: str) -> int:
        """
        show info for a file
        """
        iso_date, filesize = ImageJob.get_fileinfo(path)
        if self.debug:
            print(f"{path} ({filesize}) {iso_date}")
        return filesize

    def bundle_single_file(
        self,
        url: str,
        generate_script: bool = False,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """Bundle a single DjVu file from indirect to bundled format."""
        try:
            # Resolve URL to DjVuBundle
            if not "image/" in url:
                mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(
                    self.config.new_url
                )
                image = mw_client.fetch_image(f"File:{url}")
                url = image.url

            relpath = MediaWikiImage.relpath_of_url(url)
            full_path = self.config.full_path(relpath)

            if not os.path.exists(full_path):
                raise FileNotFoundError(f"File not found: {full_path}")

            djvu_file = self.dproc.get_djvu_file(url=url, config=self.config)
            djvu_bundle = DjVuBundle(djvu_file, config=self.config, debug=self.debug)

            # Generate script mode
            if generate_script:
                return True, None, djvu_bundle.generate_bundling_script()

            # Progress/error callbacks for CLI
            def on_progress(msg: str):
                if self.verbose:
                    print(msg)

            def on_error(msg: str):
                if self.verbose:
                    print(f"❌ {msg}")
                self.errors.append(Exception(msg))

            # Execute bundling
            success = djvu_bundle.bundle(
                create_backup=True,
                update_wiki=self.args.get("update_wiki", True),
                update_index_db=self.args.get("update_index_db", False),
                on_progress=on_progress,
                on_error=on_error,
            )

            bundled_path = djvu_bundle.bundled_file if success else None
            msg = f"✅ Successfully bundled {url}" if success else f"❌ Bundling failed"

            return success, bundled_path, msg

        except Exception as e:
            self.errors.append(e)
            return False, None, f"Error: {e}"

    def bundle_djvu_files(self) -> None:
        """
        Convert indirect/multi-file DjVu files to bundled format.

        Note:
            - Creates backup ZIPs before bundling
            - Only processes indirect (multi-file) DjVu files
            - Uses args.url, args.cleanup, args.sleep, args.script from self.args
            - Displays file sizes and compression ratio

        This is a wrapper around bundle_single_file() for CLI compatibility.
        """
        url = self.args.url
        generate_script = getattr(self.args, "script", False)

        if not url:
            raise ValueError("bundle is currently only implemented for single files")

        success, bundled_path, message = self.bundle_single_file(
            url=url, generate_script=generate_script
        )

        # Print the message (script, success message, or error)
        print(message)

    def convert_djvu(
        self,
        djvu_files: List[str],
        serial: bool = False,
    ) -> None:
        """
        Convert DjVu files to PNG format and create archive in tar or zip format.

        This is the second pass operation that processes DjVu files,
        extracts images, and stores them along with metadata.

        Args:
            djvu_files: List of DjVu file paths to process
            serial: If True, use serial processing; otherwise use parallel
        """
        if not self.output_path:
            raise ValueError("output_path is not set")
        # Select processing function based on serial flag
        process_func = self.dproc.process if serial else self.dproc.process_parallel

        with tqdm(total=len(djvu_files), desc="DjVu", unit="file") as pbar:
            page_count = 0
            for path in djvu_files:
                try:
                    relpath = MediaWikiImage.relpath_of_url(path)
                    full_path = self.config.full_path(relpath)
                    djvu_file = None
                    prefix = ImageJob.get_prefix(path)
                    package_file = os.path.join(
                        self.output_path, prefix + self.package_mode.ext
                    )

                    # Skip if package already exists and not forcing reprocessing
                    if os.path.isfile(package_file) and not self.force:
                        continue

                    # Process all pages in the document
                    for image_job in process_func(
                        full_path,
                        relurl=path,
                        save_png=True,
                        output_path=self.output_path,
                    ):
                        # Collect upstream errors
                        if hasattr(image_job, "error") and image_job.error:
                            self.errors.append(image_job.error)
                            continue

                        if djvu_file is None:
                            page_count = len(image_job.document.pages)
                            djvu_file = DjVuFile(path=path, page_count=page_count)

                        image = image_job.image
                        if image is None:
                            raise ValueError(f"image creation failed for {path}")
                        djvu_page = DjVuPage(
                            path=image.path,
                            page_index=image.page_index,
                            valid=image.valid,
                            width=image.width,
                            height=image.height,
                            dpi=image.dpi,
                            djvu_path=image.djvu_path,
                        )
                        djvu_file.pages.append(djvu_page)
                        prefix = image_job.prefix

                    yaml_file = os.path.join(self.dproc.output_path, prefix + ".yaml")
                    djvu_file.save_to_yaml_file(yaml_file)

                    # Create package after YAML is saved
                    if self.dproc.do_package:
                        self.dproc.wrap_as_package(full_path)

                except BaseException as e:
                    self.errors.append(e)
                finally:
                    error_count = len(self.errors)
                    status_msg = "✅" if error_count == 0 else f"❌ {error_count}"
                    _, mem_usage = self.dproc.check_memory_usage()
                    pbar.set_postfix_str(
                        f"{mem_usage:.2f} GB {page_count} pages {status_msg}"
                    )
                    pbar.update(1)

    def update_database(
        self,
        djvu_files_by_path: Dict[str, DjVuFile],
        max_errors: float = 1.0,
        force_reload: bool = False,
    ) -> None:
        """
        Update the DjVu database with complete metadata from DjVu files.

        For each DjVu file, ensures complete page data is available (loading
        from packages or directly from files if needed) and updates the database.

        Args:
            djvu_files_by_path: Dictionary mapping relative paths to DjVuFile objects
            max_errors: Maximum allowed error percentage (0-100) before aborting update
            force_reload: If True, reload all files even if they have page data

        Note:
            The database update is skipped if the error percentage exceeds
            the max_errors threshold to prevent corrupting the database with
            incomplete or erroneous data.
        """
        error_count = 0
        updated_files = []

        with tqdm(
            total=len(djvu_files_by_path),
            desc="Processing DjVu files for database update",
            unit="file",
        ) as pbar:
            for relpath, djvu_file in djvu_files_by_path.items():
                try:
                    # get the actual file path
                    # but consider https://github.com/WolfgangFahl/djvu-viewer/issues/33
                    # where there might be Datei:AB1938 Kreis-Beckum Inhaltsverz.djvu
                    # with a blank and relpath /c/c7/AB1938_Kreis-Beckum_Inhaltsverz.djvu
                    full_path = self.config.full_path(relpath)
                    djvu_file.set_fileinfo(full_path)
                    updated_files.append(djvu_file)

                except BaseException as e:
                    self.errors.append(e)
                    error_count += 1
                finally:
                    status_msg = "✅" if error_count == 0 else f"❌ {error_count}"
                    pbar.set_postfix_str(status_msg)
                    pbar.update(1)

        # Calculate error percentage and decide whether to update database
        total_files = len(djvu_files_by_path)
        err_percent = (error_count / total_files * 100) if total_files > 0 else 0

        if err_percent > max_errors:
            print(
                f"❌ Error rate {err_percent:.1f}% exceeds {max_errors:.1f}% limit "
                f"- skipping database update ({error_count}/{total_files} failed)"
            )
        else:
            print(
                f"✅ Error rate {err_percent:.1f}% ≤ {max_errors:.1f}% limit "
                f"- updating database with {len(updated_files)}/{total_files} files"
            )
            # Use the new store() method with DjVuFile objects
            if updated_files:
                self.djvu_files.store(
                    updated_files, sample_record_count=min(10, len(updated_files))
                )

    def report_errors(self, profiler: Profiler = None) -> None:
        """
        Report errors collected during processing.

        Args:
            profiler: Optional profiler for timing/profiling output

        Note:
            Displays check mark if no errors, cross mark with count if errors occurred.
            When debug is enabled, lists all errors. When verbose is enabled,
            includes full stack traces.
        """
        if not self.errors:
            msg = " ✅ Ok"
        else:
            msg = f" ❌ {len(self.errors)} errors"

        if profiler:
            profiler.time(msg)
        else:
            print(msg)

        if self.debug:
            for i, error in enumerate(self.errors, 1):
                print(f"📝 {i}. {error}")
                if self.verbose:
                    tb = "".join(
                        traceback.format_exception(
                            type(error), error, error.__traceback__
                        )
                    )
                    print("📜", tb)

    def catalog_and_store(self, limit: int, sample_record_count: int = 1) -> None:
        """
        Execute catalog operation and store results in database.

        Args:
            limit: Maximum number of pages to process
            sample_record_count: Number of sample records for schema inference
        """
        djvu_files = self.catalog_djvu(limit=limit)
        self.djvu_files.store(djvu_files, sample_record_count=sample_record_count)

    def convert_from_database(
        self, serial: bool = False, url: Optional[str] = None
    ) -> None:
        """
        Convert DjVu files to PNG format using database records.

        Args:
            serial: If True, use serial processing; otherwise use parallel
            url: Optional single file URL for targeted conversion
        """
        # Work with DjVuFile objects
        if not url:
            djvu_files_by_path = self.get_djvu_files()
            djvu_paths = list(djvu_files_by_path.keys())
        else:
            djvu_paths = [url]
        self.convert_djvu(djvu_paths, serial=serial)

    def update_from_database(
        self, max_errors: float = 1.0, url: Optional[str] = None
    ) -> None:
        """
        Update database with metadata from processed files.

        Args:
            max_errors: Maximum allowed error percentage before skipping update
            url: Optional single file path for targeted update
        """
        djvu_files = self.get_djvu_files(url=url, page_limit=0)
        self.update_database(djvu_files, max_errors=max_errors)
