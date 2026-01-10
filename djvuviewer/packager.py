"""
Created on 2025-02-26

@author: wf
"""

import logging
import os
import tarfile
import zipfile
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

logger = logging.getLogger(__name__)


class PackageMode(Enum):
    """
    Enum for supported package modes including their standard media types.
    """

    TAR = ("tar", "application/x-tar")
    ZIP = ("zip", "application/zip")
    NONE = ("none", None)

    def __init__(self, pkg_name, mime_type):
        self.pkg_name = pkg_name  # Use a different attribute name
        self.ext = f"{pkg_name}"
        self.media_type = mime_type

    @classmethod
    def from_name(cls, name: str):
        if name is None or name.lower() == "none":
            return None
        name_lower = name.lower()
        for mode in cls:
            if mode.pkg_name == name_lower:
                return mode
        raise ValueError(f"invalid PackageMode {name}")


class Packager:
    """
    Support for packaging files into archives (tarball and zip).
    """

    @classmethod
    def get_package_mode(cls, package_path: Union[str, Path]) -> PackageMode:
        """
        Determine the package mode from file extension.

        Args:
            package_path: Path to the package file

        Returns:
            PackageMode enum value

        Raises:
            ValueError: If extension is not supported
        """
        package_path = Path(package_path)
        ext = "".join(package_path.suffixes).lower()

        if ext.endswith(".zip"):
            return PackageMode.ZIP
        elif ".tar" in ext:
            return PackageMode.TAR
        else:
            raise ValueError(f"Unknown package extension: {ext}")

    @classmethod
    def create_package(
        cls,
        source_dir: Union[str, Path],
        output_path: Union[str, Path],
        include_ext: Optional[List[str]] = None,
        mode: PackageMode = PackageMode.TAR,
    ):
        """
        Creates a package (tar or zip) from the given source directory,
        including only specific file types.

        Args:
            source_dir (Union[str, Path]): Directory containing files to package.
            output_path (Union[str, Path]): Path to the output package file.
            include_ext (Optional[List[str]]): List of file extensions to include.
                Defaults to ["yaml", "png", "jpg"].
            mode (PackageMode): The type of package to create (TAR or ZIP).
        """
        source_dir = Path(source_dir)
        output_path = str(output_path)

        if include_ext is None:
            # yaml metadata, png lossless original, jpg thumbnails
            include_ext = ["yaml", "png", "jpg"]

        # Collect files to package
        files_to_package = []
        if source_dir.exists() and source_dir.is_dir():
            for file in os.listdir(source_dir):
                if any(file.lower().endswith(ext) for ext in include_ext):
                    full_path = source_dir / file
                    files_to_package.append((full_path, file))

        if mode == PackageMode.TAR:
            cls._create_tar(output_path, files_to_package)
        elif mode == PackageMode.ZIP:
            cls._create_zip(output_path, files_to_package)
        else:
            raise ValueError(f"Unsupported package mode: {mode}")
        try:
            archive_size = Path(output_path).stat().st_size
            msg = (
                f"Archive created successfully: {output_path} ({archive_size:,} bytes)"
            )
            logger.info(msg)
        except Exception:
            pass

    @staticmethod
    def get_indexfile(package_file: Union[str, Path]) -> str:
        """
        get the index file for the given packag_file
        """
        basename = os.path.basename(package_file)
        stem = os.path.splitext(basename)[0]
        basename = Path(package_file).name
        indexfile = f"{stem}.yaml"
        return indexfile

    @classmethod
    def archive_exists(cls, package_path: Union[str, Path]) -> bool:
        """
        Check if archive file exists and is readable.

        Args:
            package_path: Path to the package file

        Returns:
            True if archive exists and is readable
        """
        exists = False
        package_path = Path(package_path)
        if package_path.is_file():

            try:
                mode = cls.get_package_mode(package_path)
                if mode == PackageMode.TAR:
                    with tarfile.open(package_path, "r") as _tar:
                        exists = True
                elif mode == PackageMode.ZIP:
                    with zipfile.ZipFile(package_path, "r") as _zipf:
                        exists = True
            except (tarfile.TarError, zipfile.BadZipFile):
                pass
        return exists

    @staticmethod
    def _create_tar(output_path: str, files: List[tuple]):
        """Internal helper to create tarball."""
        with tarfile.open(output_path, "w") as tar:
            for full_path, arcname in files:
                tar.add(full_path, arcname=arcname)

    @staticmethod
    def _create_zip(output_path: str, files: List[tuple]):
        """Internal helper to create zip."""
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for full_path, arcname in files:
                zipf.write(full_path, arcname=arcname)

    @classmethod
    def read_from_package(cls, package_path: Union[str, Path], filename: str) -> bytes:
        """
        Reads a file directly from a package determining mode by extension (.zip vs .tar).

        Args:
            package_path (Union[str, Path]): Path to the package.
            filename (str): Name of the file inside the package to read.

        Returns:
            bytes: The file contents.

        Raises:
            FileNotFoundError: If the file is not found inside the package.
            ValueError: If the extension is unsupported.
        """
        package_path = Path(package_path)
        ext = "".join(package_path.suffixes).lower()

        if ext.endswith(".zip"):
            return cls._read_from_zip(package_path, filename)
        elif ".tar" in ext:
            return cls._read_from_tar(package_path, filename)
        else:
            raise ValueError(f"Unknown package extension: {ext}")

    @staticmethod
    def _read_from_tar(tarball_path: Path, filename: str) -> bytes:
        with tarfile.open(tarball_path, "r") as tar:
            try:
                member = tar.getmember(filename)
            except KeyError:
                raise FileNotFoundError(
                    f"File '{filename}' not found in tar package {tarball_path}"
                )

            f = tar.extractfile(member)
            if f is None:
                raise ValueError(
                    f"Could not extract file '{filename}' from tar package"
                )
            with f:
                return f.read()

    @staticmethod
    def _read_from_zip(zip_path: Path, filename: str) -> bytes:
        with zipfile.ZipFile(zip_path, "r") as zipf:
            try:
                return zipf.read(filename)
            except KeyError:
                raise FileNotFoundError(
                    f"File '{filename}' not found in zip package {zip_path}"
                )

    @classmethod
    def list_archive_members(cls, package_path: Union[str, Path]) -> List[str]:
        """
        List all member filenames in the archive.

        Args:
            package_path: Path to the package file

        Returns:
            List of member filenames
        """
        mode = cls.get_package_mode(package_path)

        if mode == PackageMode.TAR:
            return cls._list_tar_members(package_path)
        elif mode == PackageMode.ZIP:
            return cls._list_zip_members(package_path)
        else:
            raise ValueError(f"Unsupported package mode: {mode}")

    @staticmethod
    def _list_tar_members(tar_path: Path) -> List[str]:
        """List all members in a tar archive."""
        with tarfile.open(tar_path, "r") as tar:
            return [member.name for member in tar.getmembers()]

    @staticmethod
    def _list_zip_members(zip_path: Path) -> List[str]:
        """List all members in a zip archive."""
        with zipfile.ZipFile(zip_path, "r") as zipf:
            return zipf.namelist()
