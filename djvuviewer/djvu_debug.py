"""
DjVu debug/info page.

Created on 2026-01-02

@author: wf
"""

import os
import urllib.parse
from pathlib import Path

from ngwidgets.lod_grid import ListOfDictsGrid
from ngwidgets.progress import NiceguiProgressbar
from ngwidgets.task_runner import TaskRunner
from ngwidgets.widgets import Link
from nicegui import run, ui

from djvuviewer.djvu_context import DjVuContext
from djvuviewer.djvu_core import DjVuPage
from djvuviewer.djvu_image_job import ImageJob


class DjVuDebug:
    """
    UI for displaying debug/info page for a DjVu document.
    """

    def __init__(
        self,
        solution,
        context: DjVuContext,
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
        self.context = context
        self.config = context.config
        self.webserver = self.solution.webserver
        # Get DjVuFiles from context
        self.djvu_files = context.djvu_files

        self.progressbar = None
        self.page_title = page_title
        self.djvu_file = None
        self.djvu_bundle = None
        self.total_pages = 0
        self.view_lod = []
        self.lod_grid = None
        self.task_runner = TaskRunner(timeout=self.config.timeout)
        self.zip_size = 0
        self.bundled_size = 0

        # options
        self.update_index_db = True
        self.update_wiki = True
        self.create_package = False
        self.use_sudo = self.config.use_sudo
        self.package_type = self.config.package_mode
        self.bundling_enabled = False

        self.ui_container = None
        self.bundle_state_container = None

    def authenticated(self) -> bool:
        """
        check authentication
        """
        allow = self.solution.webserver.authenticated()
        return allow

    def get_header_html(self) -> str:
        """Helper to generate HTML summary our DjVuFile instance."""

        def label_value(label: str, value, span_style: str = "") -> str:
            """Helper to create a label-value HTML row."""
            if not value and value != 0:  # Skip if empty/None but allow 0
                return ""
            style_attr = f" style='{span_style}'" if span_style else ""
            return f"<strong>{label}:</strong><span{style_attr}>{value}</span>"

        def link_list():
            """
            get the available image links
            """
            links = []
            # Fixed: Access through djvu_bundle with null check
            if self.djvu_bundle and self.djvu_bundle.image_wiki:
                links.append(label_value("Wiki", view_record.get("wiki", "")))
            if self.djvu_bundle and self.djvu_bundle.image_new:
                links.append(label_value("New", view_record.get("new", "")))
            links.append(label_value("Package", view_record.get("package", "")))
            return links

        djvu_file = self.djvu_file
        view_record = {}
        filename = self.page_title
        self.djvu_files.add_links(view_record, filename)

        if not djvu_file:
            links_html = "".join(link_list())
            wiki_url = None
            if self.djvu_bundle and self.djvu_bundle.image_wiki:
                wiki_url = self.djvu_bundle.descriptionurl_wiki
            elif self.djvu_bundle and self.djvu_bundle.image_new:
                wiki_url = self.djvu_bundle.descriptionurl_new

            if not wiki_url:
                wiki_url = f"{self.config.base_url}/File:{self.page_title}"
            error_html = f"<div>No DjVu file information loaded for <a href='{wiki_url}'>{self.page_title}</a></div>"
            markup = f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 4px; min-width: 300px;'><div style='display: grid; grid-template-columns: auto 1fr; gap: 4px 12px; font-size: 0.9em;'>{links_html}</div>{error_html}</div>"
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

        package_info = (
            f"{djvu_file.package_filesize:,} bytes ({djvu_file.package_iso_date})"
            if djvu_file.package_filesize
            else None
        )

        main_size = f"{djvu_file.filesize:,} bytes" if djvu_file.filesize else None

        # Build HTML
        html_parts = [
            "<div style='border: 1px solid #ddd; padding: 10px; border-radius: 4px; min-width: 300px;'>",
            "<div style='display: grid; grid-template-columns: auto 1fr; gap: 4px 12px; font-size: 0.9em;'>",
            *link_list(),
            label_value("Path", djvu_file.path, "word-break: break-all;"),
            label_value("Format", format_type),
            label_value("Pages (Doc)", djvu_file.page_count),
            label_value("Pages (Dir)", djvu_file.dir_pages or "—"),
            label_value("Dimensions", dims),
            label_value("DPI", dpi),
            label_value("File Date", djvu_file.iso_date or "—"),
            label_value("Main Size", main_size),
            label_value("Pages Size", f"{total_page_size:,} bytes"),
            label_value("Package", package_info),
            "</div></div>",
        ]

        markup = f"<div style='display: flex; flex-wrap: wrap; gap: 16px;'>{''.join(html_parts)}</div>"
        return markup

    def setup_djvu_info(self):
        # Generate header HTML
        header_html = self.get_header_html()

        # Header
        ui.html(header_html)

    def update_bundle_state(self):
        """
        update bundle state
        """
        if not hasattr(self, "djvu_bundle") or self.djvu_bundle is None:
            self.bundle_state_container.clear()
            with self.bundle_state_container:
                ui.label("No bundle information available")
            return
        self.bundling_enabled = not self.djvu_file.bundled
        self.bundle_state_container.clear()
        with self.bundle_state_container:
            ui.label("Bundling State").classes("text-subtitle1 mb-2")
            # Bundled Three-state display: ❌ Not bundled | ⚠️ Incomplete | ✅ Bundled
            if self.djvu_bundle.has_incomplete_bundling:
                ui.label("⚠️ Incomplete - retry bundling").classes("text-warning")
                self.bundling_enabled = True  # Allow retry
            elif self.djvu_file.bundled:
                ui.label("✅ Bundled").classes("text-positive")
            else:
                ui.label("❌ Not bundled").classes("text-grey-7")
            if self.bundled_size and self.bundled_size > 0:
                ui.label(f"Size: {self.bundled_size:,} bytes").classes(
                    "text-caption text-grey-7"
                )

            # Backup file - just a disabled checkbox and download link
            backup_exists = os.path.exists(self.djvu_bundle.backup_file)
            self.create_package = not backup_exists
            with ui.row().classes("gap-4 items-center"):
                ui.checkbox("Backup exists", value=backup_exists).props("disable")

                if backup_exists:
                    backup_rel_path = os.path.relpath(
                        self.djvu_bundle.backup_file, self.config.backup_path
                    )
                    download_url = f"{self.config.url_prefix}/backups/{backup_rel_path}"
                    ui.link(f"⬇️{backup_rel_path}", download_url).classes("text-primary")
                    # Add size labels when available
                    if self.zip_size and self.zip_size > 0:
                        ui.label(f"{self.zip_size:,} bytes").classes(
                            "text-caption text-grey-7"
                        )

            with ui.expansion("Bundling script", icon="code"):
                # Script
                script = self.djvu_bundle.generate_bundling_script(
                    update_index_db=self.update_index_db
                )
                ui.code(script, language="bash").classes("w-full text-xs")

    def create_page_record(self, djvu_path: str, page: DjVuPage) -> dict:
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
            backlink = ""
            # View Link
            if self.djvu_bundle and self.djvu_bundle.description_url_new:
                image_url = self.djvu_bundle.description_url_new
                backlink = f"&backlink={urllib.parse.quote(image_url)}"
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
            return []

        for page in self.djvu_file.pages:
            record = self.create_page_record(self.djvu_file.path, page)
            view_lod.append(record)
            self.total_pages += 1

        return view_lod

    async def load_debug_info(self):
        """Load DjVu file metadata and display it."""
        try:
            self.bundle_button.enabled = False
            if self.progressbar:
                self.progressbar.reset()
                self.progressbar.set_description("Loading DjVu file")

            self.progress_row.visible = True
            # Load file metadata (blocking IO)
            try:
                self.djvu_bundle = await run.io_bound(
                    self.context.load_djvu_file, self.page_title, self.progressbar
                )

                # Extract djvu_file for convenience
                self.djvu_file = self.djvu_bundle.djvu_file
            except Exception as e:
                error_msg = str(e)
                self.content_row.clear()
                with self.content_row:
                    ui.notify(error_msg, type="negative")
                    ui.label(error_msg).classes("text-negative")
                return
            self.progress_row.visible = False
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
                        with ui.element("div").classes(
                            "w-full"
                        ) as self.bundle_state_container:
                            self.update_bundle_state()

            with self.content_row:
                if self.view_lod:
                    # Grid
                    self.lod_grid = ListOfDictsGrid()
                    self.lod_grid.load_lod(self.view_lod)
                else:
                    ui.notify("No pages")

            if self.lod_grid:
                self.lod_grid.sizeColumnsToFit()

            with self.solution.container:
                self.content_row.update()
            self.bundle_button.enabled = self.authenticated()

        except Exception as ex:
            self.solution.handle_exception(ex)
            self.content_row.clear()
            with self.content_row:
                ui.notify(f"Error loading DjVu file: {str(ex)}", type="negative")
                ui.label(f"Failed to load: {self.page_title}").classes("text-negative")
        finally:
            # Always hide progress when done
            with self.solution.container:
                if self.progress_row:
                    self.progress_row.visible = False

    def reload_debug_info(self):
        """Create background task to reload debug info."""
        self.task_runner.run(self.load_debug_info)

    def show_fileinfo(self, path: str) -> int:
        """
        show info for a file
        """
        iso_date, filesize = ImageJob.get_fileinfo(path)
        with self.content_row:
            ui.notify(f"{path} ({filesize}) {iso_date}")
        return filesize

    def show_bundling_errors(self, title: str) -> bool:
        """
        show bundling errors
        Returns:
            bool: true if there are errors
        """
        error_count = self.djvu_bundle.error_count
        has_errors = error_count > 0
        if has_errors:
            with self.content_row:
                ui.label(f"❌ {title}: {error_count} error(s) found").classes(
                    "text-h6 text-negative"
                )

                # Show errors in an expansion panel
                with ui.expansion(
                    f"Error Details ({error_count})", icon="error"
                ).classes("w-full"):
                    for i, error in enumerate(self.djvu_bundle.errors, 1):
                        with ui.card().classes("w-full bg-red-50"):
                            ui.label(f"{i}. {error}").classes("text-negative")

        return has_errors

    async def bundle(self):
        """Run bundling activities in background."""
        try:
            self.djvu_bundle.use_sudo = self.use_sudo

            # Use TaskRunner for progress/errors
            def on_progress(msg: str):
                with self.content_row:
                    ui.notify(msg)

            def on_error(msg: str):
                with self.content_row:
                    with ui.card().classes("w-full bg-red-50"):
                        ui.label(msg).classes("text-negative")

            success = self.djvu_bundle.bundle(
                create_backup=self.create_package,
                update_wiki=self.update_wiki,
                update_index_db=self.update_index_db,
                on_progress=on_progress,
                on_error=on_error,
            )
            msg = "✅ Bundling done" if success else "❌ Bundling failed"
            on_progress(msg)

            self.update_bundle_state()

        except Exception as ex:
            self.solution.handle_exception(ex)

    def on_bundle(self):
        """
        handle bundle click
        """
        with self.content_row:
            self.task_runner.run(self.bundle)

    def on_refresh(self):
        """Handle refresh button click."""
        # Cancel any running task using TaskRunner
        self.task_runner.cancel_running()

        # Show loading spinner
        self.show_spinner()

        # Run reload task asynchronously
        self.task_runner.run(self.reload_debug_info)

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
            self.bundle_button.enabled = False
            ui.checkbox("Create archive package").bind_value(
                self, "create_package"
            ).bind_enabled_from(self, "bundling_enabled")
            ui.radio(["zip", "tar"]).props("inline").bind_value(
                self, "package_type"
            ).bind_enabled_from(self, "bundling_enabled")
            # use sudo
            ui.checkbox("sudo").bind_value(self, "use_sudo")
            ui.checkbox("update wiki").bind_value(
                self, "update_wiki"
            ).bind_enabled_from(self, "bundling_enabled")
            # bundling options
            ui.checkbox("Update Index DB").bind_value(
                self, "update_index_db"
            ).bind_enabled_from(self, "bundling_enabled")

        with ui.row() as self.progress_row:
            self.progressbar = NiceguiProgressbar(
                total=1,  # Will be updated by get_djvu_file
                desc="Loading DjVu",
                unit="pages",
            )
            # attach to the task_runner
            self.task_runner.progress = self.progressbar
            self.progress_row.visible = False
        # side by side cards for bundle infos left: djvu right: state
        self.card_row = ui.row().classes("w-full")
        # Content row for all content
        self.content_row = ui.row()

        # Initial load
        self.reload_debug_info()
