"""
DjVu debug/info page.

Created on 2026-01-02

@author: wf
"""
import os
from pathlib import Path
import urllib.parse

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_core import DjVuPage
from djvuviewer.djvu_processor import DjVuProcessor
from ngwidgets.lod_grid import ListOfDictsGrid
from ngwidgets.progress import NiceguiProgressbar
from ngwidgets.widgets import Link
from nicegui import background_tasks, run, ui

from djvuviewer.djvu_context import DjVuContext


class DjVuDebug:
    """
    UI for displaying debug/info page for a DjVu document.
    """

    def __init__(
        self,
        solution,
        context:DjVuContext,
        page_title: str,
    ):
        """
        Initialize the DjVu debug view.

        Args:
            solution: The solution instance
            context: context with proc and actions
            page_title: pagetitle of the DjVu file
        """
        self.solution = solution
        self.context=context
        self.config = context.config
        self.webserver = self.solution.webserver
        self.progressbar = None
        self.page_title = page_title
        self.mw_image = None
        self.mw_image_new = None
        self.djvu_file = None
        self.djvu_bundle=None
        self.total_pages = 0
        self.view_lod = []
        self.lod_grid = None
        self.load_task = None
        self.timeout = 30.0  # Longer timeout for DjVu processing
        self.ui_container = None
        self.bundle_state_container=None
        self.dproc = DjVuProcessor(
            verbose=self.solution.debug, debug=self.solution.debug
        )

    def authenticated(self) -> bool:
        """
        check authentication
        """
        allow = self.solution.webserver.authenticated()
        return allow

    def load_djvu_file(self) -> bool:
        """
        Load DjVu file metadata via DjVuProcessor.

        Returns:
            bool: True if successful, False otherwise
        """
        success = False
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
                self.djvu_file = self.dproc.get_djvu_file(
                    abspath, config=self.config, progressbar=self.progressbar
                )
                self.djvu_bundle = DjVuBundle(self.djvu_file, config=self.config, debug=self.context.args.debug)
                success = True

        except Exception as ex:
            self.solution.handle_exception(ex)
            raise
        return success

    def get_header_html(self) -> str:
        """Helper to generate HTML summary our DjVuFile instance."""
        djvu_file=self.djvu_file
        if not djvu_file:
            markup="<div>No DjVu file information loaded.</div>"
            return markup

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
        markup=f"<div style='display: flex; flex-wrap: wrap; gap: 16px;'>{''.join(html_parts)}</div>"
        return markup

    def setup_djvu_info(self):
        # Generate header HTML
        header_html = self.get_header_html()

        # Header
        ui.html(header_html)

    def update_bundle_state(self):
        if not hasattr(self, 'djvu_bundle') or self.djvu_bundle is None:
            self.bundle_state_container.clear()
            with self.bundle_state_container:
                ui.label("No bundle information available")
            return

        self.bundle_state_container.clear()
        with self.bundle_state_container:
            ui.label("Bundling State").classes("text-subtitle1 mb-2")

            # Bundled status - just a disabled checkbox
            ui.checkbox("Bundled", value=self.djvu_file.bundled).props('disable')

            # Backup file - just a disabled checkbox and download link
            backup_exists = os.path.exists(self.djvu_bundle.backup_file)
            with ui.row().classes("gap-4 items-center"):
                ui.checkbox("Backup exists", value=backup_exists).props('disable')

                if backup_exists:
                    backup_rel_path = os.path.relpath(
                        self.djvu_bundle.backup_file,
                        self.config.backup_path
                    )
                    download_url = f"{self.config.url_prefix}/backups/{backup_rel_path}"
                    ui.link(f"⬇️{backup_rel_path}", download_url).classes("text-primary")

            with ui.expansion('Bundling script', icon='code'):
                # Script
                script = ("# File is already bundled\n# No script needed"
                          if self.djvu_file.bundled
                          else self.djvu_bundle.generate_bundling_script())
                ui.code(script, language='bash').classes('w-full text-xs')


    def create_page_record(
        self, djvu_path: str, page: DjVuPage
    ) -> dict:
        """Helper to create a single dictionary record for the LOD."""
        filename_stem = Path(djvu_path).name

        record = {
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
        if not self.djvu_file:
            return

        for page in self.djvu_file.pages:
            record = self.create_page_record(self.djvu_file.path, page)
            view_lod.append(record)
            self.total_pages += 1

        return view_lod


    async def load_debug_info(self):
        """Load DjVu file metadata and display it."""
        try:
            # Load file metadata (blocking IO)
            await run.io_bound(self.load_djvu_file)
            # Convert pages to view format
            self.view_lod = self.get_view_lod()

            # Clear and update UI
            self.content_row.clear()
            # side by side
            with self.card_row:
                with ui.splitter() as splitter:
                    with splitter.before:
                        self.setup_djvu_info()
                    with splitter.after:
                        with ui.element("div").classes("w-full") as self.bundle_state_container:
                            self.update_bundle_state()

            with self.content_row:
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

    async def bundle(self):
        """
        run the bundle activities in background
        """
        try:
            if os.path.exists(self.djvu_bundle.backup_file):
                with self.content_row:
                    ui.notify(f"{self.djvu_bundle.backup_file} already exists")
            else:
                self.djvu_bundle.create_backup_zip()
            self.update_bundling_state()

        except Exception as ex:
            self.solution.handle_exception(ex)


    def on_bundle(self):
        """
        handle bundle click
        """
        with self.content_row:
            self.bundle_task=background_tasks.create(self.bundle())

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
            self.bundle_button = ui.button(
                icon="archive",
                on_click=self.on_bundle,
            ).tooltip("bundle the shown DjVu file")
            self.bundle_button.enabled = self.authenticated()
            self.progressbar = NiceguiProgressbar(
                total=1,  # Will be updated by get_djvu_file
                desc="Loading DjVu",
                unit="pages",
            )
        # side by side cards for bundle infos left: djvu right: state
        self.card_row=ui.row().classes("w-full")
        # Content row for all content
        self.content_row = ui.row()

        # Initial load
        self.reload_debug_info()
