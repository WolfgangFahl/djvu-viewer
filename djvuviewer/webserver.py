"""
Created on 2024-08-15

@author: wf
Refactored to Focus on DjVu functionality
"""

from argparse import Namespace
from typing import Any, Dict

from ngwidgets.input_webserver import InputWebserver, InputWebSolution
from ngwidgets.login import Login
from ngwidgets.progress import TqdmProgressbar
from ngwidgets.sso_users_solution import SsoSolution
from ngwidgets.webserver import WebserverConfig
from ngwidgets.widgets import Link
from nicegui import Client, app, ui
from starlette.responses import FileResponse, HTMLResponse
from wikibot3rd.sso_users import Sso_Users

from djvuviewer.djvu_catalog import DjVuCatalog, WikiImageBrowser
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_context import DjVuContext
from djvuviewer.djvu_debug import DjVuDebug
from djvuviewer.djvu_viewer import DjVuViewer
from djvuviewer.version import Version


class DjVuViewerWebServer(InputWebserver):
    """WebServer class that manages the server and handles DjVu operations."""

    @classmethod
    def get_config(cls) -> WebserverConfig:
        copy_right = "(c)2024-2026 Wolfgang Fahl"
        config = WebserverConfig(
            copy_right=copy_right,
            # Ideally, the Version class should also belong to a package named 'djvu_viewer' or similar
            version=Version(),
            default_port=9840,
            short_name="djvuviewer",
        )
        server_config = WebserverConfig.get(config)
        server_config.solution_class = DjVuSolution
        return server_config

    def __init__(self):
        """Constructs all the necessary attributes for the WebServer object."""
        InputWebserver.__init__(self, config=DjVuViewerWebServer.get_config())
        self.users = Sso_Users(self.config.short_name)
        self.login = Login(self, self.users)
        self.djvu_config = DjVuConfig.get_instance()

        @ui.page("/")
        async def home(client: Client):
            # Default to catalog as it is the primary remaining feature
            return await self.page(client, DjVuSolution.djvu_catalog)

        @ui.page("/djvu/catalog")
        async def djvu_catalog(client: Client):
            return await self.page(client, DjVuSolution.djvu_catalog)

        @ui.page("/djvu/browse")
        async def djvu_browse(client: Client):
            return await self.page(client, DjVuSolution.djvu_browse)

        @ui.page("/djvu/debug/{page_title:str}")
        async def djvu_debug_route(client: Client, page_title: str) -> HTMLResponse:
            """Route for DjVu debug page"""
            return await self.page(client, DjVuSolution.djvu_debug, page_title)

        @ui.page("/login")
        async def login(client: Client) -> None:
            return await self.page(client, DjVuSolution.show_login)

        # Add the static files route for serving the backup files
        app.add_static_files("/backups", self.djvu_config.backup_path)

        @app.get("/djvu/content/{file:path}")
        def get_content(file: str) -> FileResponse:
            """
            Serves content from a wrapped DjVu file.

            Args:
                file (str): The full path  <DjVu name>/<file name>.

            Returns:
                FileResponse: The requested content file (PNG, JPG, YAML, etc.).
            """
            file_response = self.djvu_viewer.get_content(file)
            return file_response

        @app.get("/djvu/download/{path:path}")
        def download_package(path: str) -> FileResponse:
            """
            Serves the complete package for download.

            Args:
                path (str): The path to the DjVu document.
            """
            response = self.djvu_viewer.get_package_response(path)
            return response

        @app.get("/djvu/{path:path}/page/{scale:float}/{pageno:int}.{ext:str}")
        def get_djvu_page_with_scale(
            path: str,
            pageno: int,
            scale: float = 1.0,
            ext: str = "png",
            quality: int = 85,
        ) -> FileResponse:
            """
            Fetches and displays a specific PNG page of a DjVu file.

            Args:
                path (str): The path to the DjVu document.
                pageno (int): The page number within the DjVu document.
                scale(float,optional): the scale of the jpg impage
                ext (str): The desired file extension for the page ("png" or "jpg").
                quality (int, optional): The desired jpg quality - default:85
            """
            file_response = self.djvu_viewer.get_page4path(
                path, pageno, ext=ext, scale=scale, quality=quality
            )
            return file_response

        @app.get("/djvu/{path:path}/page/{pageno:int}.{ext:str}")
        def get_djvu_page(
            path: str,
            pageno: int,
            scale: float = 1.0,
            ext: str = "png",
            quality: int = 85,
        ) -> FileResponse:
            """
            Fetches and displays a specific PNG page of a DjVu file.

            Args:
                path (str): The path to the DjVu document.
                pageno (int): The page number within the DjVu document.
                scale(float,optional): the scale of the jpg impage
                ext (str): The desired file extension for the page ("png" or "jpg").
                quality (int, optional): The desired jpg quality - default:85
            """
            file_response = self.djvu_viewer.get_page4path(
                path, pageno, ext=ext, scale=scale, quality=quality
            )
            return file_response

        @app.get("/djvu/{path:path}")
        def display_djvu(
            path: str, page: int = 1, backlink: str = None
        ) -> HTMLResponse:
            """
            Fetches and displays a specific PNG page of a DjVu file.

            Args:
                path: Path to the DjVu file
                page: Page number to display (default: 1)
                backlink: Optional URL to return to (default: None)
            """
            html_response = self.djvu_viewer.get_page(
                path=path, page_index=page, backlink=backlink
            )
            return html_response

    def authenticated(self) -> bool:
        """
        check authentication
        """
        allow = self.login.authenticated()
        return allow

    def configure_run(self):
        """
        configure me
        """
        super().configure_run()
        djvu_cmd_args = Namespace(
            # From BaseCmd
            debug=self.args.debug,
            verbose=self.args.verbose,
            force=self.args.force,
            quiet=self.args.quiet,
            # From DjVu config
            images_path=self.djvu_config.images_path,
            db_path=self.djvu_config.db_path,
            backup_path=self.djvu_config.backup_path,
            container_name=self.djvu_config.container_name,
            # DjVu defaults
            batch_size=100,
            command="bundle",
            limit=10000000,
            limit_gb=16,
            max_errors=1.0,
            sleep=2.0,
            max_workers=None,
            output_path=None,
            pngmode="pil",
            serial=False,
            sort="asc",
            url=None,
            cleanup=False,
            script=False,
            dry_run=False,
        )
        self.context = DjVuContext(self.djvu_config, djvu_cmd_args)
        pbar = TqdmProgressbar(total=100, desc="Initializing Cache", unit="batches")
        # warm up the mediawiki images cache
        self.context.warmup_image_cache(pbar)
        # make helper classes available
        self.djvu_viewer = DjVuViewer(app=app, config=self.djvu_config)


class DjVuSolution(InputWebSolution):
    """
    the DjVuViewer solution
    """

    def __init__(self, webserver: DjVuViewerWebServer, client: Client):
        """
        Initialize the solution

        Args:
            webserver (DjVuViewerWebServer): The webserver instance associated with this solution.
            client (Client): The client instance this context is associated with.
        """
        super().__init__(webserver, client)
        self.djvu_config = webserver.djvu_config

    def configure_menu(self):
        """
        setup the menu
        """
        InputWebSolution.configure_menu(self)
        self.login = self.webserver.login
        self.sso_solution = SsoSolution(webserver=self.webserver)
        self.sso_solution.configure_menu()
        # icons from https://fonts.google.com/icons
        # if self.webserver.authenticated():
        #    self.link_button(name="wikis", icon_name="menu_book", target="/wikis")

        with self.header:
            self.link_button("DjVu Archives", "/djvu/catalog", "library_books")
            self.link_button("DjVu Wiki Images", "/djvu/browse", "image")

    async def show_login(self):
        """Show login page"""
        await self.login.login(self)

    async def djvu_debug(self, page_title: str):
        """Show the DjVu Debug page"""

        def show():
            debug_view = DjVuDebug(
                self,
                context=self.webserver.context,
                page_title=page_title,
            )
            debug_view.setup_ui()

        await self.setup_content_div(show)

    async def djvu_catalog(self):
        def show():
            self.djvu_catalog_view = DjVuCatalog(
                self, config=self.webserver.djvu_config
            )
            self.djvu_catalog_view.setup_ui()

        await self.setup_content_div(show)

    async def djvu_browse(self):
        def show():
            self.djvu_catalog_view = WikiImageBrowser(
                self, config=self.webserver.djvu_config
            )
            self.djvu_catalog_view.setup_ui()

        await self.setup_content_div(show)
