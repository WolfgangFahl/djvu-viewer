"""
Created on 2024-08-26

2026-01-02: Refactored to support dual-mode browsing (Database vs MediaWiki API).
2026-01-02: Added paging and page size selection.
2026-01-15: use GridView base class

@author: wf
"""
from dataclasses import asdict
from djvuviewer.djvu_config import DjVuConfig
from nicegui import ui

from djvuviewer.grid_view import GridView


class BaseCatalog(GridView):
    """
    UI for browsing a catalog of images
    """
    def __init__(
        self,
        solution,
        config: DjVuConfig,
        title: str,
        limit: int = 5000
    ):
        """
        Initialize the catalog view.

        Args:
            solution: The solution instance
            config: Configuration object
            title: Catalog title
            limit: Maximum records to load
        """
        # Initialize GridView with configuration
        super().__init__(
            solution=solution,
            search_cols=None,  # Auto-detect from first record
        )

        self.config = config
        self.title = title
        self.limit = limit
        self.djvu_files = self.webserver.context.djvu_files
        self.show_todo = False
        self.images_url = self.config.base_url

        # Setup UI (inherited method)
        self.setup_ui()

        # Initial load using background task runner
        self.run_background_task(self.load_lod)

    def to_view_lod(self):
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

    def load_lod(self):
        """
        Load catalog data from source.
        This is called by run_background_task() automatically.
        Override in subclasses to implement specific data loading.
        """
        raise NotImplementedError("load_lod must be implemented by subclass")

    def get_view_record(self, record: dict, index: int) -> dict:
        """
        Convert a data record to view format.
        Must be implemented by subclasses.

        Args:
            record: Source data record
            index: Row number

        Returns:
            Formatted view record
        """
        raise NotImplementedError("get_view_record must be implemented by subclass")


class DjVuCatalog(BaseCatalog):
    """
    UI for browsing and querying the DjVu document catalog.
    Supports fetching records from a local SQLite DB.
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
        super().__init__(solution=solution, config=config, title="DjVu Index Database")

    def setup_custom_header_items(self):
        """Add DjVu-specific header items (todo checkbox)."""
        show_todo_checkbox = ui.checkbox("show todo").bind_value(self, "show_todo")
        show_todo_checkbox.on('click', lambda: self.on_refresh())

    def get_source_hint(self) -> str:
        """Provide source hint for DjVu catalog."""
        return f"{len(self.view_lod)} records from {self.title}"

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

    def load_lod(self):
        """
        Fetch DjVu catalog data from index db.
        Sets self.lod: List of dictionaries containing DjVu file records
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
                do_add = True
                if self.show_todo:
                    do_add = not df.bundled and df.filesize is not None
                if do_add:
                    lod.append(asdict(df))

        except Exception as ex:
            self.solution.handle_exception(ex)
        self.lod = lod


class WikiImageBrowser(BaseCatalog):
    """
    Browser for wiki images via MediaWiki API.
    """
    def __init__(
        self,
        solution,
        config: DjVuConfig,
    ):
        """
        Initialize the wiki image browser view.

        Args:
            solution: The solution instance
            config: Configuration object containing connection and mode details
        """
        super().__init__(solution=solution, config=config, title="MediaWiki API")

    def setup_custom_header_items(self):
        """Add WikiImage-specific header items (url and limit selectors)."""
        # URL Selector
        url_options = [self.config.base_url]
        if self.config.new_url:
            url_options.append(self.config.new_url)
        ui.select(
            options=url_options,
            label="wiki",
            on_change=lambda: self.on_refresh()
        ).classes("w-64").bind_value(self, "images_url")

        # Limit Selector
        ui.select(
            options=self.limit_options,
            value=self.limit,
            label="Limit",
            on_change=lambda e: self.update_limit(e.value),
        ).classes("w-32")

    def get_source_hint(self) -> str:
        """Provide source hint for wiki images."""
        wiki_name = "wiki" if self.images_url == self.config.base_url else "new"
        return f"{len(self.view_lod)} records from {wiki_name}"

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
            - url: full image URL
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

    def load_lod(self):
        """
        Fetch DjVu catalog data via MediaWiki API.
        Sets self.lod: List of dictionaries containing DjVu file records
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
            for img in images:
                lod.append(asdict(img))

        except Exception as ex:
            self.solution.handle_exception(ex)

        self.lod = lod

    def update_limit(self, new_limit: int):
        """Update the fetch limit and refresh catalog."""
        self.limit = new_limit
        self.on_refresh()