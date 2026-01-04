"""
Created on 2026-01-02

@author: wf
"""

import os
import shlex
import time
import traceback
from argparse import Namespace
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from lodstorage.lod import LOD
from tqdm import tqdm

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVu, DjVuFile, DjVuPage
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_processor import DjVuProcessor, ImageJob
from djvuviewer.djvu_wikimages import DjVuMediaWikiImages
from djvuviewer.tarball import Tarball


class DjVuActions:
    """
    DjVu file processing operations.

    Core functionality for cataloging, converting,
    and managing DjVu files and their metadata in a database.
    """

    def __init__(
        self,
        config: DjVuConfig,
        args: Namespace,
        dvm: DjVuManager,
        dproc: DjVuProcessor,
        images_path: str,
        output_path: Optional[str] = None,
        debug: bool = False,
        verbose: bool = False,
        force: bool = False,
    ):
        """
        Initialize DjVuActions with required components.

        Args:
            config: DjVu configuration object
            args: command line arguments
            dvm: DjVu manager for database operations
            dproc: DjVu processor for file operations
            images_path: Base path for DjVu files
            output_path: Path for output files (PNG, tar, etc.)
            debug: Enable debug mode
            verbose: Enable verbose output
            force: Force reprocessing of existing files
        """
        self.config = config
        self.args = args
        self.dvm = dvm
        self.dproc = dproc
        self.images_path = images_path
        self.output_path = output_path
        self.debug = debug
        self.verbose = verbose
        self.force = force
        self.errors: List[Exception] = []

        # Configure processor output path
        if self.output_path:
            self.dproc.output_path = self.output_path

    def add_page(
        self,
        page_lod: List[Dict[str, Any]],
        path: str,
        page_index: int,
        page: Any,
    ) -> DjVuPage:
        """
        Create a DjVuPage record and append it to the page list.

        Args:
            page_lod: List to which the page dictionary is appended
            path: Path to the DjVu file
            page_index: Index of the page (1-based)
            page: Page object containing metadata from the DjVu library

        Returns:
            The created DjVuPage instance

        Note:
            Files containing "gesperrtes" in their name are marked as invalid
            as they typically represent locked/restricted content.
        """
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
            djvu_path=path,
        )
        row = asdict(dpage)
        page_lod.append(row)
        return dpage

    def get_djvu_lod(self) -> List[Dict[str, Any]]:
        """
        Retrieve all DjVu file records from the database.

        Returns:
            List of dictionaries containing DjVu file records
        """
        lod = self.dvm.query("all_djvu")
        return lod

    def get_djvu_files(
        self, djvu_lod: List[Dict[str, Any]], url: Optional[str] = None
    ) -> List[str]:
        """
        Extract DjVu file paths from the record list.

        Args:
            djvu_lod: List of DjVu file records
            url: Optional single file URL for processing

        Returns:
            List of DjVu file paths to process

        Note:
            When url is provided, returns a single-item list for targeted processing.
            Otherwise, extracts all paths from the database records.
        """
        if url:
            # Single-file mode for targeted processing
            return [url]

        # Batch mode - process all files from database
        djvu_files = [r.get("path").replace("./", "/") for r in djvu_lod]
        return djvu_files

    def catalog_djvu(self, limit: int = 10000000) -> Tuple[List[Dict], List[Dict]]:
        """
        Catalog DjVu files by scanning and extracting metadata.

        This is the first pass operation that reads DjVu files from the filesystem
        and creates database records containing file and page information.

        Args:
            limit: Maximum number of pages to process before stopping

        Returns:
            A tuple of (djvu_lod, page_lod) containing the list of DjVu records
            and page records respectively
        """
        # @FIXME bootstrap differently e.g. directly from wiki images
        # bootstrap_dvm = DjVuManager(config=self.config)
        # lod = bootstrap_dvm.query("all_djvu")
        mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(
            self.config.base_url
        )
        total = 0
        start_time = time.time()
        djvu_lod = []
        page_lod = []
        images = mw_client.fetch_allimages(limit)

        for index, r in enumerate(images, start=1):
            url = r.get("url")
            path = self.config.extract_and_clean_path(url)
            djvu_path = self.config.djvu_abspath(path)

            if not djvu_path or not os.path.exists(djvu_path):
                self.errors.append(Exception(f"missing {djvu_path}"))
                continue

            page_index = 0
            page_count = 0
            bundled = False

            # Process each page in the document
            for document, page in self.dproc.yield_pages(djvu_path):
                page_count = len(document.pages)
                page_index += 1
                self.add_page(page_lod, path, page_index, page)
                bundled = document.type == 2

            iso_date, filesize = ImageJob.get_fileinfo(djvu_path)
            djvu = DjVu(
                path=path,
                page_count=page_count,
                bundled=bundled,
                iso_date=iso_date,
                filesize=filesize,
            )
            djvu_row = asdict(djvu)
            djvu_lod.append(djvu_row)
            total += page_index

            if total > limit:
                break

            # Progress reporting
            elapsed = time.time() - start_time
            pages_per_sec = total / elapsed if elapsed > 0 else 0
            print(
                f"{index:4d} {page_count:4d} {total:7d} {pages_per_sec:7.0f} pages/s: {path}"
            )

        return djvu_lod, page_lod

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
        sleep: float = 0.0,
        generate_script: bool = False,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Bundle a single DjVu file from indirect to bundled format.

        Args:
            url: URL or filename of the DjVu file to bundle
            sleep: Sleep time in seconds before finalization
            generate_script: If True, only generate and return the bundling script

        Returns:
            Tuple of (success, bundled_path, message)
            - success: True if bundling was successful
            - bundled_path: Path to the bundled file (or None if failed/script mode)
            - message: Script command if generate_script=True, error message if failed,
                       or success message if successful
        """
        try:
            # Resolve URL to actual file path
            if not "image/" in url:
                mw_client = DjVuMediaWikiImages.get_mediawiki_images_client(
                    self.config.new_url
                )
                image = mw_client.fetch_image(f"File:{url}")
                url = image.url
                url = self.config.djvu_relpath(url)

            djvu_path = self.config.djvu_abspath(url)
            relpath = self.config.djvu_relpath(djvu_path)

            if not os.path.exists(djvu_path):
                raise FileNotFoundError(f"File not found: {djvu_path}")

            # Show original file info
            original_size = self.show_fileinfo(djvu_path)

            djvu_file = self.dproc.get_djvu_file(djvu_path, config=self.config)
            djvu_bundle = DjVuBundle(djvu_file, config=self.config, debug=self.debug)

            # If only generating script, return it
            if generate_script:
                script_cmd = djvu_bundle.generate_bundling_script()
                return True, None, script_cmd

            if self.verbose:
                print(
                    f"Creating backup for {url}... {djvu_file.page_count} pages {djvu_file.iso_date}"
                )

            zip_path = djvu_bundle.create_backup_zip()
            zip_size = self.show_fileinfo(zip_path)

            if self.verbose:
                print(f"Converting to bundled format...")
            bundled_path = djvu_bundle.convert_to_bundled()

            # Show bundled file info
            bundled_size = self.show_fileinfo(bundled_path)

            if self.verbose:
                print(f"Finalizing bundling...")
            djvu_bundle.finalize_bundling(zip_path, bundled_path, sleep=sleep)

            if not djvu_bundle.is_valid():
                self.errors.extend(djvu_bundle.errors)
                error_msg = f"Bundling failed with {len(djvu_bundle.errors)} errors"
                if self.verbose:
                    print(f"❌ {error_msg}")
                return False, None, error_msg

            # Success message
            success_msg = f"✅ Successfully bundled {url}"
            if self.verbose:
                print(success_msg)

            # Generate MediaWiki maintenance command if applicable
            if hasattr(self.config, "container_name") and self.config.container_name:
                filename = os.path.basename(djvu_path)
                docker_cmd = f"docker exec {self.config.container_name} php maintenance/refreshImageMetadata.php --force --mime=image/vnd.djvu --start={filename} --end={filename}"
                success_msg += f"\n\nTo update the wiki run:\n{docker_cmd}"
                if self.verbose:
                    print(f"To update the wiki run:\n{docker_cmd}")

            return True, bundled_path, success_msg

        except Exception as e:
            self.errors.append(e)
            error_msg = f"Error bundling {url}: {e}"
            if self.verbose:
                print(f"❌ {error_msg}")
            return False, None, error_msg


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
        sleep = getattr(self.args, 'sleep', 2.0)
        generate_script = getattr(self.args, 'script', False)

        if not url:
            raise ValueError("bundle is currently only implemented for single files")

        success, bundled_path, message = self.bundle_single_file(
            url=url,
            sleep=sleep,
            generate_script=generate_script
        )

        # Print the message (script, success message, or error)
        print(message)

    def convert_djvu(
        self,
        djvu_files: List[str],
        serial: bool = False,
    ) -> None:
        """
        Convert DjVu files to PNG format and create tarball archives.

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
                    djvu_path = self.config.djvu_abspath(path)
                    djvu_file = None
                    prefix = ImageJob.get_prefix(path)
                    tar_file = os.path.join(self.output_path, prefix + ".tar")

                    # Skip if tarball already exists and not forcing reprocessing
                    if os.path.isfile(tar_file) and not self.force:
                        continue

                    # Process all pages in the document
                    for image_job in process_func(
                        djvu_path,
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

                    # Create tarball after YAML is saved
                    if self.dproc.tar:
                        self.dproc.wrap_as_tarball(djvu_path)

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

    def get_db_records(
        self,
        tarball_file: str,
        yaml_file: str,
    ) -> List[Dict[str, Any]]:
        """
        Extract database records from a tarball's YAML metadata.

        Args:
            tarball_file: Path to the tarball file
            yaml_file: Name of the YAML file within the tarball

        Returns:
            List of dictionaries containing page records
        """
        page_lod = []
        yaml_data = Tarball.read_from_tar(tarball_file, yaml_file).decode("utf-8")
        djvu_file = DjVuFile.from_yaml(yaml_data)  # @UndefinedVariable

        for page in djvu_file.pages:
            page_record = asdict(page)
            page_lod.append(page_record)

        return page_lod

    def store(
        self,
        djvu_lod: List[Dict[str, Any]],
        page_lod: List[Dict[str, Any]],
        sample_record_count: int = 1,
    ) -> None:
        """
        Store DjVu and page records in the database.

        Args:
            djvu_lod: List of DjVu file records
            page_lod: List of page records
            sample_record_count: Number of sample records for schema inference
        """
        self.dvm.store(
            lod=page_lod,
            entity_name="Page",
            primary_key="page_key",
            with_drop=True,
            sampleRecordCount=sample_record_count,
        )
        self.dvm.store(
            lod=djvu_lod,
            entity_name="DjVu",
            primary_key="path",
            with_drop=True,
            sampleRecordCount=sample_record_count,
        )

    def init_database(self) -> None:
        """
        Initialize the database with sample records.

        Creates the database schema using sample DjVu and page records.
        """
        djvu_record = asdict(DjVu.get_sample())
        djvu_lod = [djvu_record]
        page_record = asdict(DjVuPage.get_sample())
        page_lod = [page_record]
        self.store(djvu_lod, page_lod, sample_record_count=1)

    def update_database(
        self,
        djvu_files: List[str],
        djvu_by_path: Dict[str, Dict[str, Any]],
        max_errors: float = 1.0,
    ) -> None:
        """
        Update the DjVu database with metadata from processed files.

        Reads metadata from tarball archives and updates the database records
        with processing information and extracted page data.

        Args:
            djvu_files: List of DjVu file paths to update
            djvu_by_path: Dictionary mapping paths to DjVu records
            max_errors: Maximum allowed error percentage before aborting update

        Note:
            The database update is skipped if the error percentage exceeds
            the max_errors threshold to prevent corrupting the database with
            incomplete or erroneous data.
        """
        error_count = 0
        page_lod = []
        djvu_lod = list(djvu_by_path.values())

        with tqdm(
            total=len(djvu_files),
            desc="Updating the DjVu meta data database",
            unit="file",
        ) as pbar:
            for path in djvu_files:
                try:
                    djvu_record = djvu_by_path.get(path)
                    prefix = ImageJob.get_prefix(path)
                    tar_file = os.path.join(self.output_path, prefix + ".tar")

                    if not os.path.isfile(tar_file):
                        raise Exception(f"tar file for {path} missing")

                    tar_iso_date, tar_filesize = ImageJob.get_fileinfo(tar_file)
                    if djvu_record:
                        djvu_record["tar_iso_date"] = tar_iso_date
                        djvu_record["tar_filesize"] = tar_filesize

                    tar_lod = self.get_db_records(tar_file, prefix + ".yaml")
                    page_lod.extend(tar_lod)

                except BaseException as e:
                    self.errors.append(e)
                finally:
                    error_count = len(self.errors)
                    status_msg = "✅" if error_count == 0 else f"❌ {error_count}"
                    pbar.set_postfix_str(status_msg)
                    pbar.update(1)

        # Calculate error percentage and decide whether to update database
        err_percent = error_count / len(djvu_files) * 100 if djvu_files else 0

        if err_percent > round(max_errors, 1):
            print(
                f"{err_percent:.1f}% errors ❌ > {max_errors:.1f}% limit - skipping database update"
            )
        else:
            print(f"{err_percent:.1f}% errors ✅ < {max_errors:.1f}% limit")
            self.store(djvu_lod, page_lod)

    def report_errors(self, profiler_time_func=None) -> None:
        """
        Report errors collected during processing.

        Args:
            profiler_time_func: Optional function for timing/profiling output

        Note:
            Displays check mark if no errors, cross mark with count if errors occurred.
            When debug is enabled, lists all errors. When verbose is enabled,
            includes full stack traces.
        """
        if not self.errors:
            msg = " ✅ Ok"
        else:
            msg = f" ❌ {len(self.errors)} errors"

        if profiler_time_func:
            profiler_time_func(msg)
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
        djvu_lod, page_lod = self.catalog_djvu(limit=limit)
        self.store(djvu_lod, page_lod, sample_record_count=sample_record_count)

    def convert_from_database(
        self, serial: bool = False, url: Optional[str] = None
    ) -> None:
        """
        Convert DjVu files to PNG format using database records.

        Args:
            serial: If True, use serial processing; otherwise use parallel
            url: Optional single file URL for targeted conversion
        """
        djvu_lod = self.get_djvu_lod()
        djvu_files = self.get_djvu_files(djvu_lod, url=url)
        self.convert_djvu(djvu_files, serial=serial)

    def update_from_database(
        self, max_errors: float = 1.0, url: Optional[str] = None
    ) -> None:
        """
        Update database with metadata from processed files.

        Args:
            max_errors: Maximum allowed error percentage before skipping update
            url: Optional single file URL for targeted update
        """
        djvu_lod = self.get_djvu_lod()
        djvu_by_path, duplicates = LOD.getLookup(djvu_lod, "path")

        if len(duplicates) > 0:
            print(f"Warning: {len(duplicates)} duplicate path entries in DjVu table")

        djvu_files = self.get_djvu_files(djvu_lod, url=url)
        self.update_database(djvu_files, djvu_by_path, max_errors=max_errors)
