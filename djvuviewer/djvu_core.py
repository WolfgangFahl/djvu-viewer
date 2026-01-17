"""
Created on 2025-02-25

@author: wf
"""

import datetime
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import numpy
from basemkit.yamlable import lod_storable

from djvuviewer.packager import Packager


@dataclass
class BaseFile:
    filename: Optional[str] = field(default=None, kw_only=True)
    iso_date: Optional[str] = field(default=None, kw_only=True)
    filesize: Optional[int] = field(default=None, kw_only=True)

    @property
    def exists(self) -> bool:
        """Check if file metadata is populated"""
        avail = self.filesize is not None and self.filesize > 0
        return avail

    @staticmethod
    def get_fileinfo(filepath: str):
        filesize = None
        iso_date = None
        if os.path.exists(filepath):
            # Set file size in bytes
            filesize = os.path.getsize(filepath)

            # Get file modification time and convert to UTC ISO format with second precision
            mtime = os.path.getmtime(filepath)
            datetime_obj = datetime.datetime.fromtimestamp(
                mtime, tz=datetime.timezone.utc
            )
            iso_date = datetime_obj.isoformat(timespec="seconds")
        return iso_date, filesize

    def set_fileinfo(self, filepath: str):
        """
        Set base filename and filesize and ISO date with sec prec for
        the given filepath

        Args:
            filepath (str): Path to the file
        """
        self.filename = os.path.basename(filepath)
        self.iso_date, self.filesize = self.get_fileinfo(filepath)


@lod_storable
class DjVuPage(BaseFile):
    """Represents a single djvu page"""

    path: str
    page_index: int
    valid: bool = False
    width: Optional[int] = None
    height: Optional[int] = None
    dpi: Optional[int] = None
    djvu_path: Optional[str] = None
    page_key: Optional[str] = None
    error_msg: Optional[str] = None

    def __post_init__(self):
        """Post-initialization logic for DjVuPage."""
        if self.page_key is None:
            # we expect no more than 9999 pages per document in the genwiki context that is proven
            anchor = f"#{self.page_index:04d}" if self.page_index is not None else ""
            self.page_key = f"{self.djvu_path}{anchor}"

    @property
    def png_file(self) -> str:
        """
        Returns the PNG file name derived from the DjVu file path and page index.
        """
        prefix = os.path.splitext(os.path.basename(self.djvu_path))[0]
        png_file = f"{prefix}_page_{self.page_index:04d}.png"
        return png_file

    @classmethod
    def get_sample(cls):
        """Returns a sample DjVuPage instance for testing."""
        sample_page = cls(
            path="gohr_s108_0001.djvu",
            page_index=1,
            valid=False,
            iso_date="2007-09-09T08:33:15+00:00",
            filesize=15000,
            width=1689,
            height=284,
            dpi=300,
            djvu_path="/images/1/1e/AB1953-Gohr.djvu",
            page_key="/images/1/1e/AB1953-Gohr.djvu#0001",
            error_msg="-sample error message-",
        )
        return sample_page


@dataclass
class DjVu(BaseFile):
    """Represents a DjVu main file e.g. bundled or indexed"""

    path: str
    page_count: int
    bundled: bool = False
    package_filesize: Optional[int] = None
    package_iso_date: Optional[str] = None
    dir_pages: Optional[int] = None

    @classmethod
    def get_sample(cls):
        """Returns a sample DjVu instance for testing."""
        sample_djvu = cls(
            path="/images/1/1e/AB1953-Gohr.djvu",
            page_count=2,
            dir_pages=1,
            iso_date="2007-09-09T08:33:15+00:00",
            filesize=27733,
            package_iso_date="2025-02-28T04:59:07+00:00",
            package_filesize=409600,
            bundled=False,
        )
        return sample_djvu


@lod_storable
class DjVuFile(DjVu):
    """Represents a DjVu main file e.g. bundled or indexed"""

    pages: List[DjVuPage] = field(default_factory=list)

    def get_page_by_page_index(self, page_index: int) -> Optional[DjVuPage]:
        """
        Retrieve a page by its page index.

        Args:
            page_index (int): The index of the page to retrieve.

        Returns:
            Optional[DjVuPage]: The requested DjVuPage if found, otherwise None.
        """
        for page in self.pages:
            if page.page_index == page_index:
                return page
        return None

    @classmethod
    def from_package(cls, package_path: Path) -> "DjVuFile":
        """
        (re)-instantiate me from a YAML serialization in a package

        Args:
            package_path (Path): Path to the package
        """
        djvu_file = None
        if package_path.exists():
            yaml_filename = f"{package_path.stem}.yaml"

            yaml_data = Packager.read_from_package(package_path, yaml_filename).decode(
                "utf-8"
            )
            djvu_file = cls.from_yaml(yaml_data)
            djvu_file.set_fileinfo(package_path)
        return djvu_file


@dataclass
class DjVuViewPage:
    file: DjVuFile
    page: DjVuPage
    base_path: str

    @property
    def content_path(self) -> str:
        """Path for content retrieval"""
        return f"{Path(self.base_path).stem}/{self.page.png_file}"

    @property
    def image_url(self) -> str:
        """URL path for HTML display"""
        return f"/djvu/content/{self.content_path}"


@dataclass
class DjVuImage(DjVuPage):
    _buffer: Optional[numpy.ndarray] = None
