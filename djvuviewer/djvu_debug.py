"""
DjVu debug/info page.

Created on 2026-01-02

@author: wf
"""

import urllib.parse
from pathlib import Path

from ngwidgets.lod_grid import ListOfDictsGrid
from ngwidgets.widgets import Link
from nicegui import background_tasks, run, ui

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile, DjVuPage
from djvuviewer.djvu_processor import DjVuProcessor


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
        self.page_title = page_title
        self.mw_image = None
        self.mw_image_new = None
        self.total_pages = 0
        self.view_lod = []
        self.lod_grid = None
        self.load_task = None
        self.timeout = 30.0  # Longer timeout for DjVu processing
        self.ui_container = None
        self.dproc = DjVuProcessor(
            verbose=self.solution.debug, debug=self.solution.debug
        )

    def load_djvu_file(self) -> bool:
        """
        Load DjVu file metadata via DjVuProcessor.

        Returns:
            bool: True if successful, False otherwise
        """
        success=False
        try:
            self.mw_image = self.solution.webserver.mw_client_base.fetch_image(
                title=self.page_title
            )
            if self.solution.webserver.mw_client_new:
                self.mw_image_new = self.solution.webserver.mw_client_new.fetch_image(
                    title=self.page_title
                )
            if self.mw_image:
                relpath = self.config.extract_and_clean_path(self.mw_image.url)
                abspath = self.config.djvu_abspath(f"/images/{relpath}")
                self.mw_image.djvu_file = self.dproc.get_djvu_file(
                    abspath, config=self.config
                )
                success=True

        except Exception as ex:
            self.solution.handle_exception(ex)
            raise
        return success

    def _get_sources(self):
        """
        Yields tuples of (Source Label, WikiImage Instance) for generic iteration.
        This makes adding a 3rd or 4th source trivial in the future.
        """
        sources = [("Current Wiki", self.mw_image), ("New Wiki", self.mw_image_new)]

        for label, image_obj in sources:
            if image_obj and hasattr(image_obj, "djvu_file") and image_obj.djvu_file:
                yield label, image_obj.djvu_file

    def _get_single_header_html(self, title: str, djvu_file: DjVuFile) -> str:
        """Helper to generate HTML summary for a single DjVuFile instance."""
        format_type = "Bundled" if djvu_file.bundled else "Indirect/Indexed"

        # Safe aggregations
        total_page_size = sum((p.filesize or 0) for p in (djvu_file.pages or []))

        # Safe first page access
        first_page = djvu_file.pages[0] if djvu_file.pages else None

        dims = (
            f"{first_page.width}×{first_page.height}"
            if (first_page and first_page.width)
            else "—"
        )
        dpi = first_page.dpi if (first_page and first_page.dpi) else "—"

        tar_info = ""
        if djvu_file.tar_filesize:
            tar_info = f"<strong>Tarball:</strong><span>{djvu_file.tar_filesize:,} bytes ({djvu_file.tar_iso_date})</span>"

        # Build HTML
        html_parts = [
            f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 4px; min-width: 300px;'>",
            f"<h6 style='margin: 0 0 10px 0; color: #1976D2;'>{title}</h6>",
            "<div style='display: grid; grid-template-columns: auto 1fr; gap: 4px 12px; font-size: 0.9em;'>",
            f"<strong>Path:</strong><span style='word-break: break-all;'>{djvu_file.path}</span>",
            f"<strong>Format:</strong><span>{format_type}</span>",
            f"<strong>Pages (Doc):</strong><span>{djvu_file.page_count}</span>",
            f"<strong>Pages (Dir):</strong><span>{djvu_file.dir_pages or '—'}</span>",
            f"<strong>Dimensions:</strong><span>{dims}</span>",
            f"<strong>DPI:</strong><span>{dpi}</span>",
            f"<strong>File Date:</strong><span>{djvu_file.iso_date or '—'}</span>",
            (
                f"<strong>Main Size:</strong><span>{djvu_file.filesize:,} bytes</span>"
                if djvu_file.filesize
                else ""
            ),
            f"<strong>Pages Size:</strong><span>{total_page_size:,} bytes</span>",
            tar_info,
            "</div></div>",
        ]
        return "".join(html_parts)

    def get_header_html(self) -> str:
        """Generate document info as HTML using a loop over available sources."""
        html_blocks = [
            self._get_single_header_html(label, djvu_file)
            for label, djvu_file in self._get_sources()
        ]

        if not html_blocks:
            return "<div>No DjVu file information loaded.</div>"

        return f"<div style='display: flex; flex-wrap: wrap; gap: 16px;'>{''.join(html_blocks)}</div>"

    def _create_page_record(
        self, source_name: str, djvu_path: str, page: DjVuPage
    ) -> dict:
        """Helper to create a single dictionary record for the LOD."""
        filename_stem = Path(djvu_path).name

        record = {
            "Source": source_name,
            "#": page.page_index,
            "Page": page.page_index,
            "Filename": page.path or "—",
            "Valid": "✅" if page.valid else "❌",
            "Dimensions": (
                f"{page.width}×{page.height}" if (page.width and page.height) else "—"
            ),
            "DPI": page.dpi or "—",
            "Size": f"{page.filesize:,}" if page.filesize else "—",
            "Error": page.error_msg or "",
        }

        # Add Links if config exists
        if hasattr(self, "config") and hasattr(self.config, "url_prefix"):
            base_url = f"{self.config.url_prefix}/djvu"

            # View Link
            if self.mw_image_new.description_url:
                backlink = (
                    f"&backlink={urllib.parse.quote(self.mw_image_new.description_url)}"
                )
            view_url = f"{base_url}/{filename_stem}?page={page.page_index}{backlink}"
            record["view"] = Link.create(url=view_url, text="view")

            # PNG Download Link
            # Logic assumes content is served under content/{stem}/{png_file}
            stem_only = Path(filename_stem).stem
            png_url = f"{base_url}/content/{stem_only}/{page.png_file}"
            record["png"] = Link.create(url=png_url, text="png")

        return record

    def get_view_lod(self) -> list:
        """
        Convert page records into a List of Dicts by iterating over abstract sources.
        """
        view_lod = []

        for source_name, djvu_file in self._get_sources():
            if not djvu_file:
                continue

            for page in djvu_file.pages:
                record = self._create_page_record(source_name, djvu_file.path, page)
                view_lod.append(record)
                self.total_pages += 1

        return view_lod

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
                ui.label(f"Pages ({self.total_pages} total)").classes("text-h6 mt-4")

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
                ui.label(f"Failed to load: {self.page_title}").classes("text-negative")

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
