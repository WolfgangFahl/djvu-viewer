"""
Created on 2024-08-26

2026-02-02: Refactored to support dual-mode browsing (Database vs MediaWiki API).
2026-02-02: Added paging and page size selection.
@author: wf
"""
from dataclasses import asdict
from djvuviewer.djvu_config import DjVuConfig
from ngwidgets.lod_grid import ListOfDictsGrid, GridConfig
from ngwidgets.progress import NiceguiProgressbar
from nicegui import background_tasks, run, ui
from typing import Any, Dict, List

class BaseCatalog:
    """
    UI for browsing a catalog of images
    """
    def __init__(
        self,
        solution,
        config: DjVuConfig,
        title: str,
        limit: int=5000
    ):
        """
        Initialize the DjVu catalog view.

        Args:
            solution: The solution instance
            config: Configuration object containing connection and mode details
        """
        self.solution = solution
        self.progress_row = None
        self.progressbar = None
        self.config = config
        self.title=title
        self.limit=limit

        self.webserver = self.solution.webserver
        self.djvu_files = self.webserver.context.djvu_files
        self.ui_container = None
        self.show_todo=False

        self.lod = []
        self.multiselect = False
        self.view_lod = []
        self.lod_grid = None
        self.load_task = None
        self.header_row = None
        self.grid_row = None
        self.timeout = 10.0
        self.limit_options = [15, 30, 50, 100, 500, 1500, 5000]
        self.images_url = self.config.base_url

    def authenticated(self) -> bool:
        """
        check authentication
        """
        allow = self.solution.webserver.authenticated()
        return allow

    def setup_refresh_button(self):
        self.refresh_button = ui.button(
            icon="refresh",
            on_click=self.on_refresh,
        ).tooltip("Refresh catalog")

    def get_view_lod(self):
        """Convert records to view format with row numbers and links."""
        try:
            view_lod = []
            for i, record in enumerate(self.lod):
                index = i + 1
                view_record = self.get_view_record(record, index)
                view_lod.append(view_record)
            self.view_lod = view_lod
        except Exception as ex:
            self.solution.handle_exception(ex)

    def get_grid_config(self)->GridConfig:
        """
        get the ListOfDicts Grid Configuration
        """
        self.multiselect=self.authenticated()
        grid_config = GridConfig(
            key_col="#",
            multiselect=self.multiselect,
            with_buttons=self.multiselect
        )
        return grid_config

    def setup_lod_grid(self, view_lod: List[Dict[str, Any]], source_hint: str)->ListOfDictsGrid:
        """
        setup the catalog grid
        """
        grid_config=self.get_grid_config()
        with self.grid_row:
            self.lod_grid = ListOfDictsGrid(lod=view_lod, config=grid_config)
            self.lod_grid.ag_grid.style("height: 85%; width: 100%;")
        self.post_lod_grid_setup(source_hint)

        self.lod_grid.ag_grid.options["pagination"] = True
        self.lod_grid.ag_grid.options["paginationPageSize"] = 15
        self.lod_grid.ag_grid.options["paginationPageSizeSelector"] = (
            self.limit_options
        )
        self.lod_grid.load_lod(self.view_lod)
        if self.multiselect:
            self.lod_grid.set_checkbox_selection("#")
        self.lod_grid.sizeColumnsToFit()

    def setup_ui(self):
        """
        Set up the user interface components for the DjVu catalog.
        """
        self.ui_container = self.solution.container
        with self.ui_container:
            self.setup_header_row()
            with ui.row() as self.progress_row:
                self.progressbar = NiceguiProgressbar(
                    total=1,  # Will be updated by get_djvu_file
                    desc="Loading DjVu Images",
                    unit="pages",
                )

            # Grid row for displaying results
            self.grid_row = ui.column().classes("w-full").style(
                    "height: calc(100vh - 80px);"  # full height - progress bar
            )

        # Initial load
        self.reload_catalog()

    def reload_catalog(self):
        self.load_task = background_tasks.create(self.load_catalog())

    def on_refresh(self):
        """
        Handle refresh button click.
        """

        def cancel_running():
            if self.load_task:
                self.load_task.cancel()

        # Show loading spinner
        if self.grid_row:
            self.grid_row.clear()
            with self.grid_row:
                ui.spinner()
            self.grid_row.update()

        # Cancel any running task
        cancel_running()

        ui.timer(self.timeout, lambda: cancel_running(), once=True)

        self.reload_catalog()

    async def load_catalog(self):
        """
        Load the catalog data and display it in the grid.
        """
        try:
            # Fetch data
            await run.io_bound(self.get_query_lod)

            if not self.lod:
                with self.solution.container:
                    ui.notify("No DjVu files found")
                return

            # Convert to view format with links
            await run.io_bound(self.get_view_lod)

            if self.grid_row and self.view_lod:
                # Clear and update grid and header
                self.grid_row.clear()
                record_count = len(self.view_lod)
                source_hint=f"{record_count} records from {self.title}"
                self.setup_lod_grid(view_lod=self.view_lod,source_hint=source_hint)

            with self.solution.container:
                self.grid_row.update()

        except Exception as ex:
            self.solution.handle_exception(ex)
        finally:
            with self.solution.container:
                if self.progress_row:
                    self.progress_row.visible = False

    def setup_header_row(self):
        """Setup header row components. Override in subclasses."""
        pass

    def post_lod_grid_setup(self, source_hint: str):
        """Post-processing after grid setup. Override in subclasses."""
        pass

class DjVuCatalog(BaseCatalog):
    """
    UI for browsing and querying the DjVu document catalog.
    Supports fetching records from a local SQLite DB or a remote MediaWiki API.
    """
    def __init__(
        self,
        solution,
        config: DjVuConfig,
    ):
        """
        Initialize the DjVu catalog view.

        Args:
            solution: The solution instance
            config: Configuration object containing connection and mode details
        """
        super().__init__(solution=solution,config=config,title="DjVu Index Database")
        self.multiselect=False

    def setup_header_row(self):
        # we will use the load grid button row
        pass

    def post_lod_grid_setup(self,source_hint:str):
        with self.lod_grid.button_row:
            # Header row with title and refresh button
            ui.label(f"{source_hint}").classes("text-caption")
            self.setup_refresh_button()
            show_todo_checkbox = ui.checkbox("show todo").bind_value(self, "show_todo")
            show_todo_checkbox.on('click', lambda: self.on_refresh())


    def get_view_record(self, record: dict, index: int) -> dict:
        """
        Handle Database format records (DjVu dataclass).

        Expected fields:
            - path: str (filename)
            - page_count: int
            - filesize: int
            - iso_date: str
            - bundled: bool
            - package_filesize, package_iso_date, dir_pages (optional)
        """
        view_record = {"#": index}

        filename = None
        if "path" in record:
            val = record["path"]
            if isinstance(val, str) and "/" in val:
                filename = val.split("/")[-1]
            else:
                filename = val
        self.djvu_files.add_links(view_record, filename)
        view_record["filesize"] = record.get("filesize")
        view_record["pages"] = record.get("page_count")
        view_record["date"] = record.get("iso_date")
        view_record["bundled"] = "✓" if record.get("bundled") else "X"

        # Generic package fields (works with tar, zip, or any package format)
        if record.get("package_filesize"):
            view_record["package_size"] = record.get("package_filesize")
        if record.get("package_iso_date"):
            view_record["package_date"] = record.get("package_iso_date")
        if record.get("dir_pages"):
            view_record["dir_pages"] = record.get("dir_pages")

        return view_record

    def get_query_lod(self):
        """
        Fetch DjVu catalog data from index db
        sets self.lod: List of dictionaries containing DjVu file records
        """
        lod = []
        try:
            # Fetch from SQLite Database via DjVuFiles
            djvu_files_by_path = self.djvu_files.get_djvu_files_by_path(
                file_limit=self.limit,
                page_limit=0,  # no pages needed for catalog
            )
            # Convert DjVuFile objects to dicts
            for df in djvu_files_by_path.values():
                do_add=True
                if self.show_todo:
                    do_add=not df.bundled and df.filesize is not None
                    pass
                if do_add:
                    lod.append(asdict(df))

        except Exception as ex:
            self.solution.handle_exception(ex)
        self.lod=lod

class WikiImageBrowser(BaseCatalog):
    """
    browser for wiki images
    """
    def __init__(
        self,
        solution,
        config: DjVuConfig,
    ):
        """
        Initialize the DjVu catalog view.

        Args:
            solution: The solution instance
            config: Configuration object containing connection and mode details
        """
        super().__init__(solution=solution,config=config,title="MediaWiki API")

    def setup_header_row(self):
        """
        setup the given header row
        """
        with ui.row() as self.header_row:
            self.setup_refresh_button()
            self.setup_url_selector( )

    def post_lod_grid_setup(self,source_hint:str):
        pass

    def setup_url_selector(self):
        # Url Selector
        url_options = [self.config.base_url]
        if self.config.new_url:
            url_options.append(self.config.new_url)
        ui.select(
            options=url_options, label="wiki", on_change=self.on_refresh
        ).classes("w-64").bind_value(self, "images_url")
        # Limit Selector
        ui.select(
            options=self.limit_options,
            value=self.limit,
            label="Limit",
            on_change=lambda e: self.update_limit(e.value),
        ).classes("w-16")

    def get_view_record(self, record: dict, index: int) -> dict:
        """
        Handle MediaWiki API format records.

        Expected fields:
            - name/title: "Datei:02_Amt_Loewenburg.djvu"
            - size: 838675
            - timestamp: "2011-12-11T11:15:20Z"
            - pagecount: 1
            - user: "MLCarl3"
            - width: 4175
            - height: 5014
            - url: "https://wiki.genealogy.net/images//3/3b/..."
            - descriptionurl, mime, ns
        """
        view_record = {"#": index}

        raw_name = record.get("title", "")
        filename = raw_name.replace("File:", "").replace("Datei:", "")
        self.djvu_files.add_links(view_record, filename)
        view_record["size"] = record.get("size")
        view_record["pages"] = record.get("pagecount")
        view_record["timestamp"] = record.get("timestamp")
        view_record["user"] = record.get("user")
        if record.get("width") and record.get("height"):
            view_record["dimensions"] = f"{record['width']}×{record['height']}"

        return view_record

    def get_query_lod(self):
        """
        Fetch DjVu catalog data based via API

        sets self.lod: List of dictionaries containing DjVu file records
        """
        lod = []
        try:
            # Determine which wiki to fetch from
            wiki_name = "wiki" if self.images_url == self.config.base_url else "new"

            # Setup progress bar for API fetch
            if self.progress_row:
                self.progress_row.visible = True
            if self.progressbar:
                self.progressbar.total = self.limit
                self.progressbar.reset()
                self.progressbar.set_description(f"Fetching from {wiki_name}")

            # Fetch via DjVuFiles with caching
            images = self.djvu_files.fetch_images(
                url=self.images_url,
                name=wiki_name,
                limit=self.limit,
                refresh=False,  # Use cache if available
                progressbar=self.progressbar,
                )

            # Convert MediaWikiImage objects to dicts for compatibility
            lod = []
            for img in images:
                lod.append(asdict(img))
        except Exception as ex:
            self.solution.handle_exception(ex)

        self.lod=lod

    def update_limit(self, new_limit: int):
        """Update the fetch limit and refresh catalog."""
        self.limit = new_limit
        self.on_refresh()