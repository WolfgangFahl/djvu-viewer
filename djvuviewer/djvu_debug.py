"""
DjVu debug/info page.

Created on 2026-01-02

@author: wf
"""
import os
from datetime import datetime
from pathlib import Path

import djvu.decode
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile
from djvuviewer.djvu_processor import DjVuProcessor
from ngwidgets.lod_grid import ListOfDictsGrid
from ngwidgets.widgets import Link
from nicegui import background_tasks, run, ui

from djvuviewer.djvu_wikimages import DjVuMediaWikiImages


class DjVuDebug:
    """
    UI for displaying debug/info page for a DjVu document.
    """

    def __init__(
        self,
        solution,
        config: DjVuConfig,
        page_title: str,
    ):
        """
        Initialize the DjVu debug view.

        Args:
            solution: The solution instance
            config: Configuration object
            page_title: pagetitle of the DjVu file
        """
        self.solution = solution
        self.config = config
        self.webserver = self.solution.webserver
        self.page_title=page_title
        self.mw_image=None
        self.mw_image_new=None
        self.view_lod = []
        self.lod_grid = None
        self.load_task = None
        self.timeout = 30.0  # Longer timeout for DjVu processing
        self.ui_container = None

    def get_djvu_file(self,path:str):
        """
        get the djvu file for the given path
        """
        djvu_path = self.config.djvu_abspath(path)
        if not os.path.exists(djvu_path):
            raise FileNotFoundError(f"DjVu file not found: {djvu_path}")

        # Get file-level metadata
        djvu_image_file=Path(djvu_path)
        stat_info = djvu_image_file.stat()
        filesize = stat_info.st_size
        iso_date = datetime.fromtimestamp(stat_info.st_mtime).isoformat()

        # Create processor and load document
        dproc = DjVuProcessor(
            verbose=self.solution.debug, debug=self.solution.debug
        )
        url = djvu.decode.FileURI(djvu_path)
        document = dproc.context.new_document(url)
        document.decoding_job.wait()

        # Get document-level info
        page_count = len(document.files)
        bundled = document.type == 2

        # Process pages to get detailed metadata
        pages = []
        for image_job in dproc.process(
            djvu_path, path, save_png=False, output_path=None
        ):
            image = image_job.image
            pages.append(image)

        # Create DjVuFile object
        djvu_file = DjVuFile(
            path=str(path),
            page_count=page_count,
            bundled=bundled,
            iso_date=iso_date,
            filesize=filesize,
            pages=pages,
        )
        return djvu_file

    def load_djvu_file(self) -> bool:
        """
        Load DjVu file metadata via DjVuProcessor.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.mw_image=self.solution.webserver.mw_client_base.fetch_image(title=self.page_title)
            if self.solution.webserver.mw_client_new:
                self.mw_image_new=self.solution.webserver.mw_client_new.fetch_image(title=self.page_title)
                self.mw_image_new.djvu_path=DjVuMediaWikiImages.extract_and_clean_path(self.mw_image_new.url)
                self.mw_image_new.djvu_file=self.get_djvu_file(self.mw_image_new.djvu_path)
            self.mw_image.djvu_path=DjVuMediaWikiImages.extract_and_clean_path(self.mw_image.url)
            self.mw_image.djvu_file=self.get_djvu_file(self.mw_image.djvu_path)
            return True

        except Exception as ex:
            self.solution.handle_exception(ex)
            raise

    def get_view_lod(self) -> list:
        """Convert page records to view format with row numbers and links."""
        view_lod = []
        filename = Path(self.path).name

        for idx, page in enumerate(self.djvu_file.pages, 1):
            view_record = {
                "#": idx,
                "Page": page.page_index,
                "Filename": page.path if page.path else "—",
                "Dimensions": (
                    f"{page.width}×{page.height}" if page.width and page.height else "—"
                ),
                "DPI": page.dpi if page.dpi else "—",
                "Size": f"{page.filesize:,}" if page.filesize else "—",
            }

            # Add view link
            page_url = (
                f"{self.config.url_prefix}/djvu/{filename}?page={page.page_index}"
            )
            view_record["view"] = Link.create(url=page_url, text="view")

            # Add download link using png_file property
            png_file = page.png_file
            png_url = f"{self.config.url_prefix}/djvu/content/{Path(filename).stem}/{png_file}"
            view_record["png"] = Link.create(url=png_url, text="png")

            view_lod.append(view_record)

        return view_lod

    def get_header_html(self) -> str:
        """Generate document info as HTML."""
        if not self.djvu_file:
            return ""

        filename = Path(self.path).name
        total_pages = self.djvu_file.page_count
        format_type = "Bundled" if self.djvu_file.bundled else "Non-bundled"
        total_page_size = (
            sum(page.filesize or 0 for page in self.djvu_file.pages)
            if not self.djvu_file.bundled
            else None
        )

        # Get dimensions from first page
        first_page = self.djvu_file.pages[0] if self.djvu_file.pages else None
        dimensions = (
            f"{first_page.width}×{first_page.height}"
            if first_page and first_page.width and first_page.height
            else "N/A"
        )
        dpi = first_page.dpi if first_page and first_page.dpi else "N/A"

        # Build links
        info_record = {}
        self.solution.add_links(info_record, filename)

        # Build HTML
        html_parts = [
            f"<h5>DjVu Debug: {filename}</h5>",
            "<div style='display: grid; grid-template-columns: auto 1fr; gap: 8px; margin: 16px 0;'>",
            f"<strong>Format:</strong><span>{format_type}</span>",
            f"<strong>Total Pages:</strong><span>{total_pages}</span>",
            f"<strong>Dimensions:</strong><span>{dimensions}</span>",
            f"<strong>DPI:</strong><span>{dpi}</span>",
            f"<strong>File Size:</strong><span>{self.djvu_file.filesize:,} bytes</span>",
        ]

        if total_page_size is not None:
            html_parts.append(
                f"<strong>Total Size (pages):</strong><span>{total_page_size:,} bytes</span>"
            )

        # Add links
        if "wiki" in info_record:
            html_parts.append(f"<strong>Wiki:</strong>{info_record['wiki']}")
        if "new" in info_record:
            html_parts.append(f"<strong>New Wiki:</strong>{info_record['new']}")
        if "tarball" in info_record:
            html_parts.append(f"<strong>Tarball:</strong>{info_record['tarball']}")
        if "debug" in info_record:
            html_parts.append(f"<strong>Debug:</strong>{info_record['debug']}")

        html_parts.append("</div>")

        return "\n".join(html_parts)

    async def load_debug_info(self):
        """Load DjVu file metadata and display it."""
        try:
            # Load file metadata (blocking IO)
            await run.io_bound(self.load_djvu_file)

            # Generate header HTML
            header_html = self.get_header_html()

            # Convert pages to view format
            self.view_lod = self.get_view_lod()

            # Clear and update UI
            self.content_row.clear()
            with self.content_row:
                # Header
                ui.html(header_html)

                # Pages section
                ui.label(f"Pages ({len(self.djvu_file.pages)} total)").classes(
                    "text-h6 mt-4"
                )

                # Grid
                self.lod_grid = ListOfDictsGrid()
                self.lod_grid.load_lod(self.view_lod)

            if self.lod_grid:
                self.lod_grid.sizeColumnsToFit()

            with self.solution.container:
                self.content_row.update()

        except Exception as ex:
            self.solution.handle_exception(ex)
            self.content_row.clear()
            with self.content_row:
                ui.notify(f"Error loading DjVu file: {str(ex)}", type="negative")
                ui.label(f"Failed to load: {self.path}").classes("text-negative")

    def reload_debug_info(self):
        """Create background task to reload debug info."""
        self.load_task = background_tasks.create(self.load_debug_info())

    def on_refresh(self):
        """Handle refresh button click."""

        def cancel_running():
            if self.load_task:
                self.load_task.cancel()

        # Show loading spinner
        self.content_row.clear()
        with self.content_row:
            ui.spinner()
        self.content_row.update()

        # Cancel any running task
        cancel_running()

        # Set timeout
        ui.timer(self.timeout, lambda: cancel_running(), once=True)

        # Reload
        self.reload_debug_info()

    def setup_ui(self):
        """Set up the user interface components for the DjVu debug page."""
        self.ui_container = self.solution.container

        # Header with refresh button
        with ui.row() as self.header_row:
            ui.label("DjVu Debug").classes("text-h6")
            self.refresh_button = ui.button(
                icon="refresh",
                on_click=self.on_refresh,
            ).tooltip("Refresh debug info")

        # Content row for all content
        self.content_row = ui.row()

        # Initial load
        self.reload_debug_info()
