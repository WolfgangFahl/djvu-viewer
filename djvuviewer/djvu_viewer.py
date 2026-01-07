"""
Created on 2025-02-25


@author: wf
"""

import logging
import mimetypes
import traceback
from pathlib import Path
from typing import Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile, DjVuViewPage
from djvuviewer.image_convert import ImageConverter
from djvuviewer.packager import PackageMode, Packager


class DjVuViewer:
    """
    Handles loading DjVu metadata, serving static content, and browsing via DB or API.
    """

    _static_mounted = False

    def __init__(self, app: FastAPI, config: DjVuConfig):
        """
        Initialize the DjVu viewer.

        Args:
            app: FastAPI application instance
            config:DjVu configuration
        """
        self.config = config
        self.url_prefix = self.config.url_prefix.rstrip("/")
        self.app = app
        self.package_mode = PackageMode.from_name(self.config.package_mode)

        if not DjVuViewer._static_mounted:
            app.mount(
                "/static/djvu",
                StaticFiles(directory=self.config.images_path),
                name="djvu_images",
            )
            DjVuViewer._static_mounted = True

    def handle_exception(self, e: BaseException, trace: Optional[bool] = None):
        """Handles an exception by creating an error message.

        Args:
            e (BaseException): The exception to handle.
            trace (bool, optional): Whether to include the traceback in the error message. Default is False.
        """
        if trace:
            self.error_msg = str(e) + "\n" + traceback.format_exc()
        else:
            self.error_msg = str(e)
        logging.error(self.error_msg)

    def sanitize_path(self, path: str) -> str:
        """
        fix mediawiki path quirks e.g. with blanks
        """
        path = path.replace(" ", "_")
        return path

    def get_file_content(self, file: str) -> Tuple[str, bytes]:
        """
        Retrieves a content file (PNG, JPG, YAML, etc.) from the package

        Args:
            file (str): The full path in the format <DjVu name>/<file name>.
        """
        djvu_name, filename = file.split("/", 1)
        package_path = (
            Path(self.config.package_path) / f"{djvu_name}.{self.package_mode.ext}"
        )
        file_content = Packager.read_from_package(package_path, filename)
        return filename, file_content

    def create_content_response(self, filename: str, file_content: bytes) -> Response:
        # Detect MIME type based on file extension
        media_type, _ = mimetypes.guess_type(filename)
        if media_type is None:
            media_type = "application/octet-stream"  # Default for unknown types

        content_response = Response(content=file_content, media_type=media_type)

        return content_response

    def get_package_response(self, path: str) -> FileResponse:
        """Serves the complete package for download for my package mode.

        Args:
            path: The path to the package resource.

        Returns:
            FileResponse configured with the appropriate package file and media type.

        Raises:
            HTTPException: If the package file does not exist (404).
        """
        path = self.sanitize_path(path)
        package_filename = f"{Path(path).stem}{self.package_mode.ext}"
        package_file = Path(self.config.package_path) / package_filename

        if not package_file.exists():
            raise HTTPException(
                status_code=404, detail=f"Package {package_filename} not found"
            )

        response = FileResponse(
            path=package_file,
            media_type=self.package_mode.media_type,
            filename=package_filename,
        )
        return response

    def get_content(self, file: str) -> Response:
        """
        Retrieves a content file (PNG, JPG, YAML, etc.) from the package and serves it as a response.

        Args:
            file (str): The full path in the format <DjVu name>/<file name>.

        Returns:
            Response: The requested content file with the correct media type.
        """
        try:
            filename, file_content = self.get_file_content(file)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid file path format. Expected <DjVu name>/<file name>.",
            )
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="DjVu  not found in archive")
        except KeyError:
            djvu_name, filename = file.split("/", 1)
            raise HTTPException(
                status_code=404,
                detail=f"File {filename} of DjVu {djvu_name} not found in archive",
            )
        content_response = self.create_content_response(filename, file_content)
        return content_response

    def get_djvu_view_page(self, path: str, page_index: int) -> DjVuViewPage:
        """
        Helper function to fetch DjVu page data.

        Args:
            path (str): Path to the DjVu file (without page notation).
            page_index (int): Page number to display (1-based).

        Returns:
            DjVuViewPage: dataclass instance with file,page and image_url
        """
        path = self.sanitize_path(path)
        package_filename = f"{Path(path).stem}.{self.package_mode.ext}"
        package_path = Path(self.config.package_path) / package_filename
        djvu_file = DjVuFile.from_package(package_path)

        if not djvu_file:
            raise HTTPException(
                status_code=404,
                detail=f"Package {package_filename} for {path} not available",
            )

        page_count = len(djvu_file.pages)
        if page_index < 1 or page_index > page_count:
            raise HTTPException(status_code=404, detail=f"Page {page_index} not found")

        djvu_page = djvu_file.pages[page_index - 1]
        djvu_view_page = DjVuViewPage(file=djvu_file, page=djvu_page, base_path=path)
        return djvu_view_page

    def get_page4path(
        self, path: str, pageno: int, ext: str, scale: float = 1.0, quality: int = 85
    ) -> Response:
        """
        Fetches and displays a specific page of a DjVu file in the desired format.

        Args:
            path (str): The path to the DjVu document.
            pageno (int): The page number within the DjVu document.
            ext (str): The desired file extension for the page (e.g., "png", "jpg").
            scale (float, optional): The scale factor to apply to the image (0.0-1.0). Defaults to 1.0.
            quality (int, optional): The JPEG quality (1-100). Defaults to 85.

        Returns:
            Response: Response with the page content in the requested format.

        Raises:
            HTTPException: With status code 501 if the specified file extension is unsupported.
            HTTPException: With status code 500 if an error occurs while retrieving the page content.
        """
        # Check if the file extension is supported
        exts = ["png", "jpg"]
        if ext not in exts:
            msg = (
                f"Unsupported file extension: {ext}. Must be one of {', '.join(exts)}."
            )
            raise HTTPException(status_code=501, detail=msg)

        try:
            # Get the DjVu view page
            djvu_view_page = self.get_djvu_view_page(path, pageno)
            content_path = djvu_view_page.content_path

            # Get the original file content (PNG format)
            filename, file_content = self.get_file_content(content_path)

            # If JPG is requested, convert the PNG data to JPG
            if ext == "jpg":
                # Extract the DPI from the page metadata (assuming it's available)
                # If not available, you might need to add a parameter or use a default value
                dpi = getattr(
                    djvu_view_page.page, "dpi", 300
                )  # Default to 300 if not specified

                # Use ImageConverter to convert PNG to JPG
                converter = ImageConverter(file_content, dpi)
                file_content = converter.convert_to_jpg(scale=scale, quality=quality)

                # Update filename to reflect the JPG extension
                filename = filename.replace(".png", ".jpg")

            # Create and return the response with the appropriate content
            file_response = self.create_content_response(filename, file_content)
            return file_response
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error retrieving page content: {str(e)}"
            )

    def create_page_dropdown(self, path, current_page, total_pages):
        """
        Create an HTML select dropdown for page navigation

        Args:
            path: Path of the DjVu file
            current_page: Currently displayed page number
            total_pages: Total number of pages in the document

        Returns:
            HTML select element with page options
        """
        options = []
        for page_num in range(1, total_pages + 1):
            selected = " selected" if page_num == current_page else ""
            options.append(f'<option value="{page_num}"{selected}>{page_num}</option>')

        options_html = "\n".join(options)

        select_html = f"""<select onchange="window.location.href='{self.url_prefix}/djvu/{path}?page='+this.value">
        {options_html}
    </select>"""

        return select_html

    def get_page(
        self, path: str, page_index: int, backlink: str = None
    ) -> HTMLResponse:
        """
        Fetches and renders an HTML page displaying the PNG image of the given DjVu file page from a package.
        """
        djvu_view_page = self.get_djvu_view_page(path, page_index)
        djvu_file = djvu_view_page.file
        image_url = djvu_view_page.image_url
        html_response = HTMLResponse(
            content=self.get_markup(
                path, page_index, len(djvu_file.pages), image_url, backlink
            )
        )
        return html_response

    def get_markup(
        self,
        path: str,
        page_index: int,
        total_pages: int,
        image_url: str,
        backlink: str = None,
    ) -> str:
        """
        Returns the HTML markup for displaying the DjVu page with navigation.

        Args:
            path (str): DjVu file path.
            page_index (int): Current page index.
            total_pages (int): Total number of pages in the DjVu document.
            image_url (str): URL to the PNG file.
            backlink (str, optional): URL to navigate back to. Defaults to None.

        Returns:
            str: HTML markup.
        """
        first_page = 1  # Fix: Pages start from 1
        last_page = total_pages  # Fix: Last page is total_pages, not total_pages - 1
        prev_page = max(first_page, page_index - 1)
        next_page = min(last_page, page_index + 1)
        fast_backward = max(first_page, page_index - 10)
        fast_forward = min(last_page, page_index + 10)
        select_markup = self.create_page_dropdown(path, page_index, total_pages)
        download_page_url = f"{self.url_prefix}{image_url}"
        download_package_url = f"{self.url_prefix}/djvu/download/{path}"
        help_url = "https://github.com/WolfgangFahl/djvu-viewer/wiki/Help"
        # Add back button if backlink is provided
        back_button = ""
        if backlink:
            back_button = f'<a href="{backlink}" title="Back">⬅️</a> | '

        markup = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>DjVu Viewer</title>
            <style>
                body {{ font-family: Arial, sans-serif; text-align: center; }}
                img {{ max-width: 100%; height: auto; }}
                .nav {{ margin-top: 20px; }}
                .nav a {{ margin: 0 10px; text-decoration: none; font-weight: bold; font-size: 24px; }}
            </style>
        </head>
        <body>
            <div class="nav">
                {back_button}
                <a href="{self.url_prefix}/djvu/{path}?page={first_page}" title="First Page (1/{total_pages})">⏮</a>
                <a href="{self.url_prefix}/djvu/{path}?page={fast_backward}" title="Fast Backward (Jump -10 Pages)">⏪</a>
                <a href="{self.url_prefix}/djvu/{path}?page={prev_page}" title="Previous Page">⏴</a>
                <span>{select_markup} / {total_pages}</span>
                <a href="{self.url_prefix}/djvu/{path}?page={next_page}" title="Next Page">⏵</a>
                <a href="{self.url_prefix}/djvu/{path}?page={fast_forward}" title="Fast Forward (Jump +10 Pages)">⏩</a>
                <a href="{self.url_prefix}/djvu/{path}?page={last_page}" title="Last Page ({total_pages}/{total_pages})">⏭</a>
                | <a href="{download_page_url}" title="Page" download>⬇️</a>
                <a href="{download_package_url}"  title="Package" download>📦</a>
                <a href="{help_url}" title="Help" target="_blank">❓</a>
            </div>
            <img src="{self.url_prefix}{image_url}" alt="DjVu Page {page_index}">
        </body>
        </html>
        """
        return markup
