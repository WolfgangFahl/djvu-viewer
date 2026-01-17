"""
Created on 2025-02-25

@author: wf
"""

import gc
import logging
import os
import shutil
import sys
import tempfile
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Generator, List, Optional

import djvu.decode
import numpy
from basemkit.shell import Shell
from ngwidgets.profiler import Profiler
from PIL import Image

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_config import DjVuConfig, PngMode
from djvuviewer.djvu_core import DjVuFile, DjVuImage, DjVuPage
from djvuviewer.djvu_image_job import ImageJob
from djvuviewer.packager import PackageMode, Packager
from djvuviewer.wiki_images import MediaWikiImage

if sys.platform != "win32":
    import resource


class DjVuDecodeContext(djvu.decode.Context):
    """
    A lightweight wrapper around djvu.decode.Context to handle messages.
    """

    def __init__(self):
        super().__init__()
        self.message_handler = None

    def handle_message(self, message):
        """
        Handles messages from the DjVu decoding context.
        """
        if self.message_handler:
            self.message_handler(message)


class DjVuProcessor:
    """
    Processes DjVu files and converts pages to image buffers.

    see https://raw.githubusercontent.com/jwilk-archive/python-djvulibre/refs/heads/master/examples/djvu2png
    with Copyright © 2010-2021 Jakub Wilk <jwilk@jwilk.net> and GNU General Public License version 2
    """

    def __init__(
        self,
        package_mode: Optional[PackageMode] = None,
        verbose: bool = False,
        debug: bool = False,
        batch_size: int = 100,
        limit_gb: int = 16,
        max_workers: int = None,
        pngmode: str = "pil",
        clean_temp: bool = True,
    ):
        """
        Initializes the DjVuProcessor.

        Args:
            package_mode (Optional[PackageMode]): Package format (TAR/ZIP) or None to disable packaging
            verbose (bool, optional): Enable verbose output (default: False).
            debug (bool, optional): Enable debug logging (default: False).
            batch_size (int, optional): Number of pages to process in each batch (default: 100).
            limit_gb(int): maximum amount of memory to be used in GB
            max_workers (int, optional): Maximum number of worker threads (default: min(CPU count, 8)).
            pngmode(str): PNG generation mode - "cli" or "pil" (default: "pil")
            clean_temp(bool): if True remove files from temp directories when tar done
        """
        self.package_mode = package_mode
        self.do_package = package_mode is not None
        self.verbose = verbose
        self.debug = debug
        self.batch_size = batch_size
        self.limit_gb = limit_gb
        self.pngmode = PngMode.CLI if pngmode == "cli" else PngMode.PIL

        # Set a reasonable default for max_workers if not specified
        if max_workers is None:
            self.max_workers = os.cpu_count() * 4
        else:
            self.max_workers = max_workers
        self.clean_temp = clean_temp
        self.context = DjVuDecodeContext()  # delegate context instance
        self.context.message_handler = self.handle_message
        self.djvu_pixel_format = djvu.decode.PixelFormatRgbMask(
            0xFF0000, 0xFF00, 0xFF, bpp=32
        )
        self.djvu_pixel_format.rows_top_to_bottom = 1
        self.djvu_pixel_format.y_top_to_bottom = 0
        self.shell = Shell()

    def handle_message(self, message):
        if isinstance(message, djvu.decode.ErrorMessage):
            raise Exception(message)

    def check_memory_usage(self):
        """Check if memory usage exceeds the given limit in GB"""
        if sys.platform == "win32":
            # On Windows, we'll just do a GC and return False (no check)
            gc.collect()
            return False, 0
        else:
            # Get current memory usage in bytes
            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # Convert to GB (note: on some systems this is KB, others it's bytes)
            if sys.platform == "darwin":  # macOS reports in bytes
                usage_gb = usage / (1024 * 1024 * 1024)
            else:  # Linux reports in KB
                usage_gb = usage / (1024 * 1024)

            return usage_gb >= self.limit_gb, usage_gb

    def save_image_to_png(
        self, image_job: ImageJob, output_path: str, free_buffer: bool = True
    ) -> None:
        """
        Save a decoded DjVu image to PNG using PIL.

        This method converts the ARGB32 buffer stored in the ImageJob to RGB format
        and saves it as a PNG file. The buffer is expected to be in Cairo's ARGB32 format
        (0xAARRGGBB as uint32), which is produced by the render_page method.

        Args:
            image_job (ImageJob): The image job containing the decoded image buffer.
                The buffer should be stored in image_job.image._buffer as a tuple
                containing a numpy array with dtype uint32 in ARGB32 format.
            output_path (str): The file path where the PNG image should be saved.
            free_buffer (bool, optional): If True, frees the image buffer after saving
                to reduce memory usage. Defaults to True.

        Raises:
            ValueError: If the image or buffer is not available in the ImageJob.

        Note:
            The buffer format is Cairo ARGB32, where each pixel is a 32-bit integer
            with the layout 0xAARRGGBB (alpha, red, green, blue from MSB to LSB).
            This is converted to RGB24 for PNG output. The buffer may have stride
            padding for alignment, which is handled automatically.

        See Also:
            https://github.com/Piolie/DjvuRleImagePlugin
        """
        if not image_job.image or image_job.image._buffer is None:
            raise ValueError("Image buffer not available in ImageJob")

        try:
            image = image_job.image
            width = image.width
            height = image.height

            # Extract buffer from tuple
            buffer_data = image_job.image._buffer
            if isinstance(buffer_data, tuple):
                buffer_data = buffer_data[0]

            # Convert from uint32 ARGB to RGB bytes
            # The buffer is in ARGB32 format (0xAARRGGBB as uint32)
            # Buffer shape is (height, stride_width) where stride_width >= width due to alignment
            rgb_array = numpy.zeros((height, width, 3), dtype=numpy.uint8)

            # Extract RGB channels from ARGB32, handling possible stride padding
            # Red channel: bits 16-23
            rgb_array[:, :, 0] = (buffer_data[:, :width] >> 16) & 0xFF
            # Green channel: bits 8-15
            rgb_array[:, :, 1] = (buffer_data[:, :width] >> 8) & 0xFF
            # Blue channel: bits 0-7
            rgb_array[:, :, 2] = buffer_data[:, :width] & 0xFF

            # Create PIL Image from RGB array
            img = Image.fromarray(rgb_array, mode="RGB")

            # Save as PNG
            img.save(output_path, "PNG")

        finally:
            # Free buffer if requested
            if free_buffer and image_job.image:
                image_job.image._buffer = None
                # Also explicitly delete the image object to help with memory cleanup
                del image_job.image
                image_job.image = None

    def save_as_png(
        self, image_job: ImageJob, output_dir: str, free_buffer: bool
    ) -> str:
        """
        Save an image job as PNG in the specified directory using the configured PNG mode

        Args:
            image_job: The image job to save
            output_dir: Directory to save to
            free_buffer(bool): if True free the buffer

        Returns:
            Path to the saved PNG file
        """
        output_path = os.path.join(
            output_dir, f"{image_job.prefix}_page_{image_job.page_index:04d}.png"
        )
        image_job.log(f"save png ({self.pngmode}) start")
        if self.pngmode == PngMode.CLI:
            # Use ddjvu command-line tool with DPI from the DjVu page
            dpi = image_job.image.dpi if image_job.image else 300

            # get CLI size arg from the image dimensions
            if image_job.image:
                width = image_job.image.width
                height = image_job.image.height
            else:
                # Fallback to pagejob size if image not available
                width, height = image_job.get_size()

            # Calculate size string in format "widthxheight"
            size = f"{width}x{height}"

            DjVuBundle.render_djvu_page_cli(
                image_job.djvu_path,
                image_job.page_index - 1,  # Convert to 0-based for ddjvu
                output_path,
                size=size,
                shell=self.shell,
                debug=self.debug,
            )
            # Update job's image path to point to the PNG
            if image_job.image:
                image_job.image.path = output_path
        else:
            # Save PNG
            # Use PIL with rendered buffer
            self.save_image_to_png(image_job, output_path, free_buffer)
        image_job.log(f"save png ({self.pngmode}) to {output_path} done")
        return output_path

    def render_pagejob_to_buffer(self, image_job: ImageJob, mode: int) -> numpy.ndarray:
        """
        Renders a DjVu page job to a color buffer.

        Args:
            image_job (ImageJob): The job containing the pagejob to render
            mode (int): Rendering mode.

        Returns:
            numpy.ndarray: The rendered color buffer.
        """
        if not image_job.pagejob:
            raise ValueError("PageJob not available")

        width, height = image_job.get_size()
        rect = (0, 0, width, height)

        # Calculate stride for ARGB32 format (4 bytes per pixel)
        # Ensure 4-byte alignment (though width * 4 is already aligned for uint32)
        bytes_per_line = width * 4

        assert bytes_per_line % 4 == 0

        color_buffer = numpy.zeros((height, bytes_per_line // 4), dtype=numpy.uint32)
        image_job.pagejob.render(
            mode,
            rect,
            rect,
            self.djvu_pixel_format,
            row_alignment=bytes_per_line,
            buffer=color_buffer,
        )

        if mode == djvu.decode.RENDER_FOREGROUND:
            mask_buffer = numpy.zeros_like(color_buffer)
            image_job.pagejob.render(
                djvu.decode.RENDER_MASK_ONLY,
                rect,
                rect,
                self.djvu_pixel_format,
                row_alignment=bytes_per_line,
                buffer=mask_buffer,
            )
            color_buffer |= mask_buffer << 24

        color_buffer ^= 0xFF000000  # Apply transparency
        return color_buffer

    def ensure_file_exists(self, path: str):
        """
        check that the given file exists
        """
        if not os.path.isfile(path):
            msg = f"file {path} not found"
            raise ValueError(msg)

    def get_djvu_file(
        self,
        url: str,
        config: DjVuConfig,
        progressbar: Optional["Progressbar"] = None,
    ) -> DjVuFile:
        """
        Efficiently retrieves DjVu file metadata and page structure.

        This method parses the document structure and page headers to construct
        a DjVuFile object with DjVuPage children without fully decoding
        the pixel data of the images.

        Args:
            url (str): The url or file system path to the .djvu file.
            config(DjVuConfig): the config to use for path handling
            progressbar (Optional[Progressbar]): Optional progress bar to track processing

        Returns:
            DjVuFile: The structured representation of the DjVu file.
        """
        # get the relative path
        relpath = MediaWikiImage.relpath_of_url(url)
        full_path = config.full_path(relpath)

        # 1. Get container file metadata
        self.ensure_file_exists(full_path)
        iso_date, filesize = ImageJob.get_fileinfo(full_path)

        pages: List[DjVuPage] = []
        is_bundled = False
        dir_pages = 0
        doc_info_captured = False

        # Get document to determine total pages for progress bar
        document = self.context.new_document(djvu.decode.FileURI(full_path))
        document.decoding_job.wait()

        # Set up progress bar if provided
        if progressbar:
            total_pages = len(document.pages)
            progressbar.total = total_pages
            progressbar.reset()
            progressbar.set_description(f"Loading {os.path.basename(full_path)}")

        # 2. Iterate pages using existing generator
        # document is the same reference in every iteration
        for document, page in self.yield_pages(full_path):

            # Capture document-level flags on first pass
            if not doc_info_captured:
                # document.type: 0=Single, 1=Indirect, 2=Bundled
                is_bundled = document.type == 2
                # document.files contains the directory of included files
                dir_pages = len(document.files)
                doc_info_captured = True

            page_index = len(pages) + 1

            # 3. Retrieve Page Metadata (Lightweight)
            # get_info waits for IFF headers (dimensions, dpi) but not pixel decoding
            try:
                page.get_info(wait=True)
                valid_page = True
                width = page.width
                height = page.height
                dpi = page.dpi
                error_msg = None
            except Exception as e:
                valid_page = False
                width = 0
                height = 0
                dpi = 0
                error_msg = str(e)
                if self.debug:
                    logging.warning(
                        f"Failed to get info for page {page_index} in {full_path}: {e}"
                    )

            # 4. Determine Filename and File-specific stats
            try:
                # Handle bytes vs str filename encoding issues typical in djvulibre bindings
                page_filename = page.file.name
                if isinstance(page_filename, bytes):
                    page_filename = page_filename.decode("utf-8", errors="replace")
            except Exception:
                page_filename = f"page_{page_index:04d}.djvu"

            # Default to container stats
            page_iso = iso_date
            page_size = filesize

            # If not bundled (Indirect), the page is likely a separate file on disk
            if not is_bundled:
                dirname = os.path.dirname(full_path)
                component_path = os.path.join(dirname, page_filename)
                # Only override if the component file physically exists
                if os.path.exists(component_path):
                    page_iso, page_size = ImageJob.get_fileinfo(component_path)

            # 5. Construct Page Object
            djvu_page = DjVuPage(
                path=page_filename,
                page_index=page_index,
                valid=valid_page,
                iso_date=page_iso,
                filesize=page_size,
                width=width,
                height=height,
                dpi=dpi,
                djvu_path=relpath,
                error_msg=error_msg,
            )
            pages.append(djvu_page)
            # Update progress bar
            if progressbar:
                progressbar.update(1)

        # 6. Construct and return File Object
        djvu_file = DjVuFile(
            path=relpath,
            page_count=len(pages),
            bundled=is_bundled,
            iso_date=iso_date,
            filesize=filesize,
            # package_filesize/date are not calculated by the processor, typically 0 or None initially
            package_filesize=0,
            dir_pages=dir_pages,
            pages=pages,
        )

        return djvu_file

    def yield_pages(self, djvu_path: str):
        """
        yield the pages for the given djvu_path
        """
        self.ensure_file_exists(djvu_path)
        document = self.context.new_document(djvu.decode.FileURI(djvu_path))
        document.decoding_job.wait()
        for page in document.pages:
            yield document, page

    def create_image_jobs(self, djvu_path: str, relurl: str) -> List[ImageJob]:
        """
        Create initial image jobs for all pages in the document

        Args:
            djvu_path (str): Path to the DjVu file
            relurl (str): Relative URL

        Returns:
            List[ImageJob]: List of initialized image jobs
        """
        image_jobs = []
        page_index = 0
        for document, page in self.yield_pages(djvu_path):
            page_index += 1
            job = ImageJob(
                djvu_path=djvu_path,
                document=document,
                page=page,
                page_index=page_index,
                relurl=relurl,
                debug=self.debug,
                verbose=self.verbose,
            )
            image_jobs.append(job)

        return image_jobs

    def decode_page(self, image_job: ImageJob, wait: bool = True) -> ImageJob:
        """
        Decodes a single page and updates the ImageJob

        Args:
            image_job (ImageJob): The job to process
            wait (bool): Whether to wait for decoding to complete

        Returns:
            ImageJob: Updated image job with pagejob
        """
        try:
            file_size_msg = ""
            filepath = image_job.filepath
            # check whether the document is bundled or not
            if image_job.document.type != 2:
                # we need to check the file is external
                self.ensure_file_exists(filepath)
                image_job.set_fileinfo(filepath)
                file_size_msg = f"{filepath}:{image_job.filesize} bytes "
            else:
                # For bundled files, get the container metadata
                container_path = image_job.djvu_path
                if os.path.exists(container_path):
                    image_job.set_fileinfo(container_path)
            image_job.log(f" page.decode {file_size_msg} start")
            pagejob = image_job.page.decode(wait=wait)
            image_job.log(" page.decode done")
            # Update the image job with the decoded page job
            image_job.pagejob = pagejob
        except Exception as e:
            # Store exception but don't raise
            image_job.error = e
        finally:
            gc.collect()
        return image_job

    def render_page(
        self, image_job: ImageJob, mode: int = djvu.decode.RENDER_COLOR
    ) -> ImageJob:
        """
        Renders a page and updates the ImageJob with the rendered image

        Args:
            image_job (ImageJob): The job to process
            mode (int): Rendering mode

        Returns:
            ImageJob: Updated image job with rendered image
        """
        try:
            if image_job.pagejob:
                image_job.log(" render start")

                width, height = image_job.get_size()

                image = DjVuImage(
                    width=width,
                    height=height,
                    dpi=image_job.pagejob.dpi,
                    iso_date=image_job.iso_date,
                    filesize=image_job.filesize,
                    page_index=image_job.page_index,
                    djvu_path=image_job.relurl,
                    path=image_job.decoded_filename,
                )
                # Determine if we need to render to a buffer
                if not self.pngmode == PngMode.CLI:
                    color_buffer = self.render_pagejob_to_buffer(image_job, mode)
                    image._buffer = (color_buffer,)

                # Update the image job with the rendered image
                image_job.image = image
                image_job.log(" render done")
        except Exception as e:
            # Store exception but don't raise
            image_job.error = e
        return image_job

    def prepare(self, output_path: str, relurl: str):
        """
        Prepares the output directory and sets up temporary storage if package creation is enabled.

        Args:
            output_path (str): The final destination path for output files.
            relurl(str): the relative url to process

        Attributes:
            final_output_path (str): The actual output path where the final files will be stored.
            temp_dir (Optional[str]): A temporary directory for intermediate storage if package creation is enabled.
            output_path (str): The working output path (either temporary or final).
            profiler (Profiler): Profiler instance for tracking processing time.
        """
        self.final_output_path = output_path
        self.profiler = Profiler(
            f"processing {relurl}", profile=self.verbose or self.debug
        )
        # process without any output
        if output_path is None:
            return
        if self.do_package:
            # Use a temporary directory for intermediate PNG storage
            self.temp_dir = tempfile.mkdtemp()
            self.output_path = self.temp_dir
        else:
            self.output_path = output_path
        # Prepare output directory if needed
        os.makedirs(self.final_output_path, exist_ok=True)

    def wrap_as_package(self, djvu_path: str):
        """
        Wraps processed output files into a package

        Args:
            djvu_path (str): The path to the original DjVu file.

        """
        if not self.package_mode:
            raise ValueError("Cannot wrap package: package_mode not configured")

        package_mode = self.package_mode
        package_path = os.path.join(
            self.final_output_path, f"{Path(djvu_path).stem}.{package_mode.ext}"
        )
        Packager.create_package(self.output_path, package_path, mode=package_mode)
        if self.clean_temp:
            shutil.rmtree(self.temp_dir)

    def process(
        self,
        djvu_path: str,
        relurl: str,
        mode: int = djvu.decode.RENDER_COLOR,
        wait: bool = True,
        save_png: bool = False,
        free_buffer: bool = True,
        output_path: str = None,
    ) -> Generator[ImageJob, None, None]:
        """
        Converts a DjVu URL to image buffers with sequential decoding and rendering.
        """
        self.prepare(output_path=output_path, relurl=relurl)
        # Step 1: Create image jobs for all pages
        image_jobs = self.create_image_jobs(djvu_path, relurl)
        self.profiler.time(f" create image jobs")

        # Process each page sequentially
        for job in image_jobs:
            # Step 2: Decode the page
            decoded_job = self.decode_page(job, wait)

            # Step 3: Render the page
            rendered_job = self.render_page(decoded_job, mode)

            self.profiler.time(f" process page {rendered_job.page_index:4d}")

            # Step 4: Optionally save to PNG
            if save_png:
                self.save_as_png(rendered_job, self.output_path, free_buffer)

            yield rendered_job

    def process_batch(
        self,
        image_jobs: List[ImageJob],
        mode: int = djvu.decode.RENDER_COLOR,
        wait: bool = True,
        save_png: bool = False,
        free_buffer: bool = True,
    ) -> Generator[ImageJob, None, None]:
        """
        Process a batch of image jobs with parallel execution.
        To be called from within process_parallel.
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Step 2: Decode all pages in parallel
            decode_futures = [
                executor.submit(self.decode_page, job, wait) for job in image_jobs
            ]

            # Step 3 & 4: Render and save all pages in parallel
            render_futures: List[Future] = []
            save_futures: List[Future] = []

            # Submit rendering jobs as decoding completes
            for future in decode_futures:
                decoded_job = future.result()
                render_futures.append(
                    executor.submit(self.render_page, decoded_job, mode)
                )

            # Process rendered jobs as they become available
            for future in render_futures:
                # check memory before processing
                exceeds_limit, usage = self.check_memory_usage()
                if exceeds_limit:
                    msg = f"Memory usage {usage} GB exceeds {self.limit_gb} GB limit"
                    raise Exception(msg)
                rendered_job = future.result()
                self.profiler.time(f" process page {rendered_job.page_index:4d}")

                # Optionally save to PNG in parallel
                if save_png:
                    save_future = executor.submit(
                        self.save_as_png, rendered_job, self.output_path, free_buffer
                    )
                    save_futures.append(save_future)

                yield rendered_job
            # Wait for all PNG saves
            for future in save_futures:
                future.result()

        # Clean up memory after processing the batch
        gc.collect()
        self.profiler.time(" memory cleaned after batch")

    def process_parallel(
        self,
        djvu_path: str,
        relurl: str,
        mode: int = djvu.decode.RENDER_COLOR,
        wait: bool = True,
        save_png: bool = False,
        output_path: str = None,
    ) -> Generator[ImageJob, None, None]:
        """
        Converts a DjVu URL to image buffers with fully parallel decoding and rendering.

        Args:
            djvu_path (str): Path to the DjVu file.
            relurl (str): Relative URL for referencing the file.
            mode (int, optional): Rendering mode (default: djvu.decode.RENDER_COLOR).
            wait (bool, optional): Whether to wait for processing to complete (default: True).
            save_png (bool, optional): Whether to save output as PNG files (default: False).
            output_path (str, optional): Directory path to save PNG files (default: None).

        Yields:
            Generator[ImageJob, None, None]: A generator yielding image jobs.
        """
        self.prepare(output_path=output_path, relurl=relurl)

        # Step 1: Create image jobs for all pages
        image_jobs = self.create_image_jobs(djvu_path, relurl)
        total_pages = len(image_jobs)
        self.profiler.time(f" create {total_pages} image jobs")

        # Process Steps 2 to 5 in batches
        for batch_start in range(0, total_pages, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total_pages)
            batch = image_jobs[batch_start:batch_end]

            self.profiler.time(
                f" processing batch {batch_start//self.batch_size + 1}: pages {batch_start+1}-{batch_end}"
            )

            # Process this batch
            for rendered_job in self.process_batch(
                batch, mode, wait, save_png=save_png
            ):
                yield rendered_job
