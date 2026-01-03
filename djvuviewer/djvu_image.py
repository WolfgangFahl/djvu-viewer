"""
Created on 2026-01-03

@author: wf
"""

import datetime
import logging
import os
from dataclasses import dataclass, field
from typing import Optional, Tuple

import djvu
from basemkit.profiler import Profiler

from djvuviewer.djvu_core import DjVuImage


@dataclass
class ImageJob:
    """
    Represents a processed DjVu page,
    optionally including document, page, page job, and image data.
    """

    djvu_path: str  # fully qualifying path of the container DjVu document
    document: djvu.decode.Document
    page: djvu.decode.Page
    page_index: int  # page_index to track position
    relurl: str  # relurl for context
    pagejob: Optional[djvu.decode.PageJob] = field(default=None)
    image: Optional[DjVuImage] = field(default=None)
    # flags
    verbose: bool = False
    debug: bool = False
    # fields for database records
    iso_date: Optional[str] = field(default=None)
    filesize: Optional[int] = field(default=None)
    error: Optional[Exception] = field(default=None)

    def __post_init__(self):
        """Initialize profiler if not provided"""
        self.profiler = Profiler(
            f"Image Job {self.relurl}#{self.page_index:04d}",
            profile=self.verbose or self.debug,
        )
        self.profiler.start()

    def log(self, msg):
        if self.verbose or self.debug:
            self.profiler.time(" " + msg)

    def get_size(self) -> Tuple[int, int]:
        """Get the width and height of the page if pagejob is available"""
        if self.pagejob:
            return self.pagejob.size
        return (0, 0)

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
        Set filesize and ISO date with sec prec for
        the given filepath

        Args:
            filepath (str): Path to the file
        """
        self.iso_date, self.filesize = self.get_fileinfo(filepath)

    @staticmethod
    def get_prefix(relurl: str):
        prefix = os.path.splitext(os.path.basename(relurl))[0]
        return prefix

    @staticmethod
    def get_relative_image_path(relurl: str):
        image_rel_dir = os.path.dirname(relurl)
        # Ensure image_rel_dir is treated as a relative path
        if image_rel_dir.startswith(
            os.sep
        ):  # os.sep is '/' on Unix and '\\' on Windows
            image_rel_dir = image_rel_dir.lstrip(os.sep)
        return image_rel_dir

    @property
    def prefix(self) -> str:
        prefix = ImageJob.get_prefix(relurl=self.relurl)
        return prefix

    @property
    def filename(self) -> str:
        try:
            # Attempt to safely decode the file name
            filename = self.page.file.name.encode("utf-8", errors="replace").decode(
                "utf-8"
            )
        except Exception as e:
            if self.debug:
                logging.warn(
                    f"Failed to decode filename for page {self.page_index}: {e}"
                )
            filename = f"page_{self.page_index:04d}.djvu"
        return filename

    @property
    def dirname(self) -> str:
        dirname = os.path.dirname(self.djvu_path)
        return dirname

    @property
    def filepath(self) -> str:
        filepath = os.path.join(self.dirname, self.filename)
        return filepath
