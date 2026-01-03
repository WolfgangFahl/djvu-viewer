"""
Created on 2024-08-15

@author: wf
Refactored to Focus on DjVu functionality
"""

from typing import Dict

from ngwidgets.input_webserver import InputWebserver, InputWebSolution
from ngwidgets.webserver import WebserverConfig
from ngwidgets.widgets import Link
from nicegui import Client, app, ui
from starlette.responses import FileResponse, HTMLResponse

from djvuviewer.djvu_catalog import DjVuCatalog
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_debug import DjVuDebug
from djvuviewer.djvu_viewer import DjVuViewer
from djvuviewer.djvu_wikimages import DjVuMediaWikiImages
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
        def download_tarball(path: str) -> FileResponse:
            """
            Serves the complete tarball for download.

            Args:
                path (str): The path to the DjVu document.
            """
            response = self.djvu_viewer.get_tarball_response(path)
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

    def configure_run(self):
        """
        configure me
        """
        super().configure_run()
        self.djvu_config = DjVuConfig.get_instance()
        # make helper classes available
        self.djvu_viewer = DjVuViewer(app=app, config=self.djvu_config)
        # Initialize MediaWiki clients if using API mode
        self.mw_client_base = DjVuMediaWikiImages.get_mediawiki_images_client(
            self.djvu_config.base_url
        )
        self.mw_client_new = DjVuMediaWikiImages.get_mediawiki_images_client(
            self.djvu_config.new_url
        )


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

    def add_links(self, view_record: Dict[str, any], filename: str):
        """
        Add the DjVu links.
        """
        config = self.djvu_config
        if filename:
            wiki_url = self.djvu_config.wiki_fileurl(filename)
            view_record["wiki"] = Link.create(url=wiki_url, text=filename)

            if config.new_url:
                new_url = self.djvu_config.wiki_fileurl(filename, new=True)
                view_record["new"] = Link.create(url=new_url, text=filename)
                backlink = self.djvu_config.wiki_fileurl(
                    filename, new=True, quoted=True
                )
            backparam = f"?backlink={backlink}" if backlink else ""
            local_url = f"{config.url_prefix}/djvu/{filename}{backparam}"
            view_record["tarball"] = Link.create(url=local_url, text=filename)

            debug_url = f"{config.url_prefix}/djvu/debug/{filename}"
            view_record["debug"] = Link.create(url=debug_url, text="🔍")

    def setup_menu(self, detailed: bool = True):
        """
        setup the menu
        """
        super().setup_menu(detailed=detailed)
        with self.header:
            self.link_button("DjVu Tarballs", "/djvu/catalog", "library_books")
            self.link_button("DjVu Wiki Images", "/djvu/browse", "image")

    async def djvu_debug(self, page_title: str):
        """Show the DjVu Debug page"""

        def show():
            debug_view = DjVuDebug(
                self,
                config=self.webserver.djvu_config,
                page_title=page_title,
            )
            debug_view.setup_ui()

        await self.setup_content_div(show)

    async def djvu_modal_catalog(self, browse_wiki: bool = False):
        """Show the DjVu Catalog page"""

        def show():
            self.djvu_catalog_view = DjVuCatalog(
                self,
                config=self.webserver.djvu_config,
                browse_wiki=browse_wiki,
            )
            self.djvu_catalog_view.setup_ui()

        await self.setup_content_div(show)

    async def djvu_catalog(self):
        await self.djvu_modal_catalog(browse_wiki=False)

    async def djvu_browse(self):
        await self.djvu_modal_catalog(browse_wiki=True)
