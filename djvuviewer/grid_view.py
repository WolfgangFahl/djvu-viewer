"""
Created on 2026-01-15

@author: wf
"""

import inspect
from collections import OrderedDict
from typing import List, Optional

from _collections_abc import Callable
from ngwidgets.input_webserver import InputWebSolution
from ngwidgets.lod_grid import GridConfig, ListOfDictsGrid
from ngwidgets.progress import NiceguiProgressbar
from ngwidgets.task_runner import TaskRunner
from nicegui import run, ui


class View:
    """
    Base class for views with common functions
    """

    def __init__(self, solution: InputWebSolution):
        self.solution = solution
        self.webserver = solution.webserver
        self.debug = self.solution.debug
        self.exception: Optional[Exception] = None  # Store background errors here

    def label_value(self, label: str, value, default="", compact: bool = False):
        """
        Helper function to display a label-value pair

        Args:
            label: The label to display
            value: The value to display
            default: Default value if value is None
            compact: Whether to use compact display
        """
        value = value if value is not None else default
        if compact:
            ui.label("•").classes("text-gray-500")
            ui.label(value).tooltip(label)
        else:
            with ui.row().classes("items-center gap-2"):
                ui.label(f"{label}:").classes("font-bold")
                ui.label(f"{value}")

    def authenticated(self) -> bool:
        """
        Check if user is authenticated

        Returns:
            bool: True if user is authenticated
        """
        if hasattr(self.solution, "webserver"):
            return self.solution.webserver.authenticated()
        return False


class GridView(View):
    """
    Base class for grid-based views using ListOfDictsGrid
    """

    def __init__(
        self,
        solution: InputWebSolution,
        search_cols: List[str] = None,
        limit_options: List[int] = None,
    ):
        """
        Initialize the GridView with a UI solution.

        Args:
            solution: the web solution providing the content_div for rendering
            search_cols: list of column names to be searched; defaults to all columns
            limit_options: list of page size options for pagination
        """
        super().__init__(solution=solution)
        self.grid = None
        self.grid_config = None
        self.reset_lod()
        self.search_cols = search_cols
        self.search_text = ""

        # Background task management
        self.task_runner: Optional[TaskRunner] = None

        # Progress bar
        self.progress_row = None
        self.progressbar: Optional[NiceguiProgressbar] = None

        # UI containers
        self.ui_container = None
        self.header_row = None
        self.header_setup = False
        self.grid_row = None
        self.source_info_label = None

        if not limit_options:
            limit_options = [15, 30, 50, 100, 500, 1500, 5000]
        self.limit_options = limit_options

    def setup_ui(self):
        """
        Setup the full UI layout including progress bar, header, and grid container.
        Subclasses should call this from their construction phase or __init__.
        """
        if not self.progress_row:
            self.setup_progress_bar()

        # Create header row for controls
        if not self.header_row:
            self.header_row = ui.row().classes("w-full items-center gap-2")

        # Create grid row for the actual grid
        if not self.grid_row:
            self.grid_row = ui.row().classes("w-full fit")

    def setup_header(self, source_hint: str = ""):
        """
        Setup the standard header with search, refresh, and optional source info.
        Calls setup_custom_header_items() for subclass-specific additions.

        Args:
            source_hint: Description of data source (e.g., "100 records from Database")
        """
        if self.header_setup:
            if source_hint:
                self.source_info_label.set_text(source_hint)

            return
        if not self.header_row:
            return

        self.header_row.clear()

        with self.header_row:
            # Standard items: search and refresh
            self.setup_search()
            self.setup_refresh_button()

            # Custom items from subclass
            self.setup_custom_header_items()

            # Spacer to push source info to the right
            ui.space()

            # Source info label
            if source_hint:
                self.source_info_label = ui.label(source_hint).classes(
                    "text-caption text-gray-600"
                )
            self.header_setup = True

    def setup_custom_header_items(self):
        """
        Hook for subclasses to add custom header items.
        Override this method to add subclass-specific controls.
        """
        pass

    def setup_refresh_button(self):
        """Create refresh button."""
        self.refresh_button = ui.button(
            icon="refresh",
            on_click=self.on_refresh,
        ).tooltip("Refresh")

    def setup_search(self):
        """Setup search controls."""
        ui.input(
            label="Search",
            placeholder="search ...",
            on_change=lambda: None,  # Just update binding
        ).bind_value(self, "search_text").classes("w-48")
        ui.button("Search", icon="search", on_click=self.on_search_click)

    async def on_search_click(self):
        """Handle search button click."""
        try:
            if not self.grid or not self.search_text.strip():
                return
            search_lower = self.search_text.strip().lower()
            matched_keys = []
            columns = (
                # all first record columns if no search_cold are specified
                self.search_cols or list(self.lod[0].keys())
                if self.lod
                else []
            )
            for row in self.lod:
                for col in columns:
                    val = row.get(col)
                    if isinstance(val, str) and search_lower in val.lower():
                        key_value = row.get(self.grid_config.key_col)
                        matched_keys.append(key_value)
                        break  # Move to next row after first match
            msg = f"search {self.search_text} → {len(matched_keys)} matches"
            ui.notify(msg)
            self.grid.select_rows_by_keys(matched_keys)
        except Exception as ex:
            self.solution.handle_exception(ex)

    def reset_lod(self):
        """Reset the logical and view layer data and summary state."""
        self.lod: List[dict] = []
        self.view_lod: List[dict] = []

    def to_view_lod(self):
        """
        Create view layer data with key_col first and sorted by key_col.
        Override in subclasses if different transformation is needed.
        """
        self.view_lod = []
        for ri, record in enumerate(self.lod):
            view_record = OrderedDict(record)
            if hasattr(self, "key_col") and self.key_col in view_record:
                view_record.move_to_end(self.key_col, last=False)
            view_record["#"] = ri + 1
            view_record.move_to_end("#", last=False)
            self.view_lod.append(view_record)
        if hasattr(self, "key_col") and self.key_col:
            self.view_lod.sort(key=lambda r: r.get(self.key_col, ""))

    def get_grid_config(self) -> GridConfig:
        """
        Get the grid configuration.

        Returns:
            GridConfig: Configuration for the ListOfDictsGrid
        """
        multiselect = self.authenticated()
        grid_config = GridConfig(
            key_col="#",
            editable=False,
            multiselect=multiselect,
            with_buttons=False,  # We handle buttons in header_row
            debug=self.debug,
        )
        return grid_config

    async def render_grid(self):
        """
        Render the view_lod into a ListOfDictsGrid.
        """

        if not self.grid_row:
            return
        try:
            grid_config = self.get_grid_config()
            self.grid_config = grid_config

            with self.grid_row:
                self.grid = ListOfDictsGrid(lod=self.view_lod, config=grid_config)
                self.grid.ag_grid.options["pagination"] = True
                self.grid.ag_grid.options["paginationPageSize"] = 15
                self.grid.ag_grid.options["paginationPageSizeSelector"] = (
                    self.limit_options
                )
                if grid_config.multiselect:
                    self.grid.set_checkbox_selection(grid_config.key_col)
        except Exception as ex:
            self.solution.handle_exception(ex)

    def load_lod(self):
        """
        Abstract data loading - needs to be overridden by subclasses.
        """
        raise NotImplementedError("load_lod must be implemented by subclass")

    def get_source_hint(self) -> str:
        """
        Get the source hint text for the header.
        Override in subclasses to provide custom source information.

        Returns:
            str: Source hint text (e.g., "100 records from Database")
        """
        return f"{len(self.view_lod)} records"

    def clear_grid_row(self):
        """
        clear the grid row
        """
        self.grid_row.clear()
        self.grid = None

    def show_spinner(self):
        # Show loading spinner
        if self.grid_row:
            self.clear_grid_row()
            with self.grid_row:
                ui.spinner("dots", size="lg")
            self.grid_row.update()

    def on_refresh(self) -> None:
        """
        Handle refresh button click.
        Initiates a background reload and updates the view upon completion.
        """
        self.show_spinner()
        # Run background task
        self.run_background_task(self.load_lod)

    def setup_progress_bar(
        self, total: int = 1, desc: str = "Loading", unit: str = "items"
    ):
        """
        Setup progress bar UI.

        Args:
            total: Total number of items
            desc: Description text
            unit: Unit name for progress
        """
        if not self.progress_row:
            self.progress_row = ui.row()

        with self.progress_row:
            self.progressbar = NiceguiProgressbar(
                total=total,
                desc=desc,
                unit=unit,
            )

    def hide_progress_bar(self):
        """Hide the progress bar."""
        if self.progress_row:
            self.progress_row.visible = False

    def show_progress_bar(self):
        """Show the progress bar."""
        if self.progress_row:
            self.progress_row.visible = True

    def configure_task_runner(self) -> TaskRunner:
        """
        Initialize the TaskRunner linked to the view's progress bar.

        Returns:
            TaskRunner: Configured task runner instance
        """
        if self.progressbar:
            task_runner = TaskRunner(
                timeout=self.config.timeout, progress=self.progressbar
            )
        else:
            task_runner = TaskRunner(timeout=self.config.timeout)
        return task_runner

    async def update_view(self) -> None:
        """
        Synchronize the UI grid with the current logical data state.

        1. Converts logical data (lod) to view data (view_lod).
        2. Sets up the header with source info.
        3. Re-renders or updates the grid component.
        """
        try:
            # Transform data
            self.to_view_lod()

            # Setup header with source info
            source_hint = self.get_source_hint()
            self.setup_header(source_hint)
            # Clear grid row - removing potential progress spinner
            # and render grid
            self.clear_grid_row()
            # First time render
            await self.render_grid()

            # Update container if exists
            if self.ui_container:
                self.ui_container.update()

        except Exception as ex:
            self.solution.handle_exception(ex)
        finally:
            self.hide_progress_bar()

    def run_background_task(self, task_func: Callable, *args, **kwargs) -> None:
        """
        Execute a function in the background via TaskRunner and update view on completion.

        Args:
            task_func: The function (async or sync) to execute.
            *args: Positional arguments for task_func.
            **kwargs: Keyword arguments for task_func.
        """
        self.task_runner = self.configure_task_runner()

        async def wrapped_execution():
            try:
                if inspect.iscoroutinefunction(task_func):
                    await task_func(*args, **kwargs)
                else:
                    await run.io_bound(task_func, *args, **kwargs)
                await self.update_view()
            except Exception as ex:
                self.exception = ex
                self.solution.handle_exception(ex)
            finally:
                # Ensure UI is updated even on failure
                if self.grid_row:
                    self.grid_row.update()

        self.task_runner.run_async(wrapped_execution)
