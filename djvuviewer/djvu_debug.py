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
from ngwidgets.widgets import Link
from nicegui import background_tasks, run, ui

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_context import DjVuContext
from djvuviewer.djvu_core import DjVuPage
from djvuviewer.djvu_image_job import ImageJob
from djvuviewer.djvu_processor import DjVuProcessor


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
        self.mw_image = None
        self.mw_image_new = None
        self.djvu_file = None
        self.djvu_bundle = None
        self.total_pages = 0
        self.view_lod = []
        self.lod_grid = None
        self.load_task = None
        self.zip_size = 0
        self.bundled_size = 0

        # options
        self.update_index_db = True
        self.update_wiki = True
        self.create_package = False
        self.package_type = self.config.package_mode
        self.bundling_enabled = False

        self.timeout = 30.0  # Longer timeout for DjVu processing
        self.ui_container = None
        self.bundle_state_container = None
        self.dproc = DjVuProcessor(
            verbose=self.solution.debug, debug=self.solution.debug
        )

    def authenticated(self) -> bool:
        """
        check authentication
        """
        allow = self.solution.webserver.authenticated()
        return allow

    def load_djvu_file(self) -> tuple[bool, str]:
        """
        Load DjVu file metadata via DjVuFiles interface.

        Returns:
            tuple[bool, str]: (success, error_message)
        """
        try:
            # Fetch image metadata from both wikis using DjVuFiles
            wiki_images = self.djvu_files.fetch_images(
                url=self.config.base_url, name="wiki", titles=[self.page_title]
            )
            self.mw_image_wiki = wiki_images[0] if wiki_images else None

            if self.config.new_url:
                new_images = self.djvu_files.fetch_images(
                    url=self.config.new_url, name="new", titles=[self.page_title]
                )
                self.mw_image_new = new_images[0] if new_images else None

            if not (self.mw_image_wiki or self.mw_image_new):
                return False, f"Image not found in any wiki: {self.page_title}"

            # Use available image to determine path
            active_image = self.mw_image_wiki or self.mw_image_new
            relpath = active_image.relpath  # Already cleaned by MediaWikiImage
            abspath = self.config.djvu_abspath(f"/images/{relpath}")

            self.djvu_file = self.dproc.get_djvu_file(
                abspath, progressbar=self.progressbar
            )
            self.djvu_bundle = DjVuBundle(
                self.djvu_file, config=self.config, debug=self.context.args.debug
            )
            return True, ""

        except Exception as ex:
            error_msg = f"Error loading DjVu file: {str(ex)}"
            self.solution.handle_exception(ex)
            return False, error_msg

        except Exception as ex:
            error_msg = f"Error loading DjVu file: {str(ex)}"
            self.solution.handle_exception(ex)
            return False, error_msg

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
            if self.mw_image_wiki:
                links.append(label_value("Wiki", view_record.get("wiki", "")))
            if self.mw_image_new:
                links.append(label_value("New", view_record.get("new", "")))
            links.append(label_value("Package", view_record.get("package", "")))
            return links

        djvu_file = self.djvu_file
        view_record = {}
        filename = self.page_title
        self.djvu_files.add_links(view_record, filename)

        if not djvu_file:
            links_html = "".join(link_list())
            wiki_url = (
                self.mw_image_wiki.descriptionurl
                if self.mw_image_wiki
                else (
                    self.mw_image_new.descriptionurl
                    if self.mw_image_new
                    else f"{self.config.base_url}/File:{self.page_title}"
                )
            )
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
            # Bundled status - just a disabled checkbox
            ui.checkbox("Bundled", value=self.djvu_file.bundled).props("disable")
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
            return []

        for page in self.djvu_file.pages:
            record = self.create_page_record(self.djvu_file.path, page)
            view_lod.append(record)
            self.total_pages += 1

        return view_lod

    async def load_debug_info(self):
        """Load DjVu file metadata and display it."""
        try:
            if self.progressbar:
                self.progressbar.reset()
                self.progressbar.set_description("Loading DjVu file")

            self.progress_row.visible = True
            # Load file metadata (blocking IO)
            success, error_msg = await run.io_bound(self.load_djvu_file)

            if not success:
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
        self.load_task = background_tasks.create(self.load_debug_info())

    def show_fileinfo(self, path: str) -> int:
        """
        show info for a file
        """
        iso_date, filesize = ImageJob.get_fileinfo(path)
        with self.content_row:
            ui.notify(f"{path} ({filesize}) {iso_date}")
        return filesize

    async def bundle(self):
        """
        run the bundle activities in background
        """
        try:
            zip_path = self.djvu_bundle.backup_file
            self.zip_size = self.show_fileinfo(zip_path)
            if self.create_package:
                if os.path.exists(self.djvu_bundle.backup_file):
                    with self.content_row:
                        ui.notify(f"{self.djvu_bundle.backup_file} already exists")
                else:
                    zip_path = self.djvu_bundle.create_backup_zip()

            bundled_path = self.djvu_bundle.convert_to_bundled()
            self.bundled_size = self.show_fileinfo(bundled_path)

            self.djvu_bundle.finalize_bundling(zip_path, bundled_path, sleep=True)
            docker_cmd = self.djvu_bundle.get_docker_cmd()
            if docker_cmd and self.update_wiki:
                result = self.djvu_bundle.shell.run(docker_cmd)
                if result.returncode != 0:
                    with self.content_row:
                        ui.notify("docker command failed")
            if self.update_index_db:
                success, msg = self.djvu_bundle.update_index_database()
                with self.content_row:
                    if success:
                        ui.notify(msg, type="positive")
                    else:
                        ui.notify(msg, type="warning")
            self.update_bundle_state()

        except Exception as ex:
            self.solution.handle_exception(ex)

    def on_bundle(self):
        """
        handle bundle click
        """
        with self.content_row:
            self.bundle_task = background_tasks.create(self.bundle())

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
            ui.checkbox("Create archive package").bind_value(
                self, "create_package"
            ).bind_enabled_from(self, "bundling_enabled")
            ui.radio(["zip", "tar"]).props("inline").bind_value(
                self, "package_type"
            ).bind_enabled_from(self, "bundling_enabled")
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
            self.progress_row.visible = False
        # side by side cards for bundle infos left: djvu right: state
        self.card_row = ui.row().classes("w-full")
        # Content row for all content
        self.content_row = ui.row()

        # Initial load
        self.reload_debug_info()
