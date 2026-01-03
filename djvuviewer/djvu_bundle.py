"""
Created on 2026-01-03

@author: wf
"""
import io
import os
from pathlib import Path
import re
import shlex
import shutil
import tarfile
import tempfile
from typing import List, Optional
import zipfile

from PIL import Image
from basemkit.shell import Shell
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile
from djvuviewer.tarball import Tarball


class DjVuBundle:
    """
    DjVu bundle handling with validation and error collection.
    """

    def __init__(self, djvu_file: DjVuFile,config:DjVuConfig=None,debug:bool=False):
        """
        Initialize DjVuBundle with a DjVuFile instance.

        Args:
            djvu_file: The DjVuFile metadata
            config: configuration
            debug: if True
        """
        self.djvu_file = djvu_file
        if config is None:
            config=DjVuConfig.get_instance()
        self.config=config
        self.debug=debug
        self.errors: List[str] = []
        self.shell=Shell()
        self.djvu_dump_log=None

    @classmethod
    def from_tarball(cls, tar_file: str, with_check: bool = True) -> "DjVuBundle":
        """
        Create a DjVuBundle from a tarball file.

        Args:
            tar_file: Path to the tar archive
            with_check: If True, automatically run validation checks

        Returns:
            DjVuBundle: Instance with loaded metadata and optional validation

        Example:
            bundle = DjVuBundle.from_tarball("document.tar")
            if not bundle.is_valid():
                print(bundle.get_error_summary())
        """
        tarball_path = Path(tar_file)

        # Find YAML file in tarball
        with tarfile.open(tar_file) as tar:
            yaml_files = [m.name for m in tar.getmembers() if m.name.endswith(".yaml")]

        if not yaml_files:
            # Create a minimal bundle with an error
            bundle = cls(DjVuFile(pages=[]))
            bundle.errors.append(f"No YAML metadata file found in tarball: {tar_file}")
            return bundle

        if len(yaml_files) > 1:
            # Use first one but warn
            bundle = cls(DjVuFile.from_tarball(tarball_path, yaml_files[0]))
            bundle.errors.append(
                f"Found {len(yaml_files)} YAML files, expected 1. Using {yaml_files[0]}"
            )
        else:
            bundle = cls(DjVuFile.from_tarball(tarball_path, yaml_files[0]))

        if with_check:
            bundle.check_tarball(tar_file)

        return bundle

    def _add_error(self, message: str):
        """Add an error message to the error list."""
        self.errors.append(message)

    def check_tarball(self, tar_file: str, relurl: Optional[str] = None):
        """
        Verify that a tar file exists and contains expected contents with correct dimensions.
        Collects errors instead of raising exceptions.

        Args:
            tar_file: Path to the tar file to validate
            relurl: Optional relative URL for error context
        """
        context = f" for {relurl}" if relurl else ""

        # Check file exists
        if not os.path.isfile(tar_file):
            self._add_error(f"Expected tar file '{tar_file}' was not created{context}")
            return

        try:
            with tarfile.open(tar_file) as tar:
                members = tar.getmembers()

                # Check tar is not empty
                if len(members) == 0:
                    self._add_error(f"Tar file '{tar_file}' is empty{context}")
                    return

                # Find PNG files
                png_files = [m.name for m in members if m.name.endswith(".png")]
                if len(png_files) == 0:
                    self._add_error(f"No PNG files found in tar{context}")

                # Check for YAML file
                yaml_files = [m.name for m in members if m.name.endswith(".yaml")]
                if len(yaml_files) == 0:
                    self._add_error(f"No YAML file found in tar{context}")
                    return
                elif len(yaml_files) > 1:
                    self._add_error(
                        f"Expected 1 YAML file in tar, found {len(yaml_files)}{context}"
                    )

                # Create dimension mapping from metadata
                page_dimensions = {
                    i + 1: (page.width, page.height)
                    for i, page in enumerate(self.djvu_file.pages)
                }

                # Verify PNG dimensions match metadata
                tarball_path = Path(tar_file)
                for png_file in sorted(png_files):
                    basename = os.path.basename(png_file)
                    match = re.search(r"(\d+)\.png$", basename)

                    if not match:
                        self._add_error(
                            f"Could not extract page number from PNG: {png_file}{context}"
                        )
                        continue

                    page_num = int(match.group(1))
                    if page_num not in page_dimensions:
                        self._add_error(
                            f"PNG {png_file} references page {page_num} not in YAML{context}"
                        )
                        continue

                    expected_width, expected_height = page_dimensions[page_num]

                    try:
                        png_data = Tarball.read_from_tar(tarball_path, png_file)
                        with Image.open(io.BytesIO(png_data)) as img:
                            actual_width, actual_height = img.size

                        if actual_width != expected_width:
                            self._add_error(
                                f"PNG {png_file} width mismatch: expected {expected_width}, "
                                f"got {actual_width}{context}"
                            )

                        if actual_height != expected_height:
                            self._add_error(
                                f"PNG {png_file} height mismatch: expected {expected_height}, "
                                f"got {actual_height}{context}"
                            )
                    except Exception as e:
                        self._add_error(
                            f"Failed to read/validate PNG {png_file}: {e}{context}"
                        )

        except tarfile.TarError as e:
            self._add_error(f"Failed to open tar file '{tar_file}': {e}{context}")
        except Exception as e:
            self._add_error(
                f"Unexpected error checking tar file '{tar_file}': {e}{context}"
            )

    def get_part_filenames(self)->List[str]:
        """
        get a list of my part file names
        """
        if not self.djvu_dump_log:
            self.djvu_dump()
        part_files = []
        for line in self.djvu_dump_log.split('\n'):
            match = re.search(r'^\s+(.+\.(?:djvu|djbz))\s+->', line)
            if match:
                part_files.append(match.group(1))
        return part_files


    def djvu_dump(self) -> str:
        """
        Run djvudump on self.djvu_file.djvu_path and return output.
        Adds error to self.errors on failure.

        Returns:
            djvudump output string (empty on error)
        """
        djvu_path = self.djvu_file.path

        if not os.path.exists(djvu_path):
            self._add_error(f"File not found: {djvu_path}")
            return ""

        cmd = f"djvudump {shlex.quote(djvu_path)}"
        result = self.shell.run(cmd, text=True, debug=self.debug)

        if result.returncode == 0:
            self.djvu_dump_log=result.stdout
        else:
            self._add_error(f"djvudump failed: {result.stderr}")
        return self.djvu_dump_log

    def finalize_bundling(self, zip_path: str, bundled_path: str):
        """
        Finalize bundling by removing the original main file and
        all zipped component files then move the bundled_path file to the original.

        Args:
            zip_path: Path to the backup ZIP file (for verification)
            bundled_path: Path to the new bundled DjVu file
        """
        djvu_path = self.djvu_file.path

        # Verify backup ZIP exists before proceeding
        if not os.path.exists(zip_path):
            self._add_error(f"Backup ZIP not found: {zip_path}")
            return

        # Verify bundled file exists
        if not os.path.exists(bundled_path):
            self._add_error(f"Bundled file not found: {bundled_path}")
            return

        # Get directory of original file
        djvu_dir = os.path.dirname(djvu_path)

        # Get list of component files to remove
        part_files = self.get_part_filenames()

        try:
            # Remove component parts
            for part_file in part_files:
                part_path = os.path.join(djvu_dir, part_file)
                if os.path.exists(part_path):
                    os.remove(part_path)
                    if self.debug:
                        print(f"Removed component: {part_path}")

            # Remove original main file
            if os.path.exists(djvu_path):
                os.remove(djvu_path)
                if self.debug:
                    print(f"Removed original: {djvu_path}")

            # Move bundled file to original location
            shutil.move(bundled_path, djvu_path)
            if self.debug:
                print(f"Moved {bundled_path} to {djvu_path}")

            # Update the DjVuFile object to reflect it's now bundled
            self.djvu_file.bundled = True

        except Exception as e:
            self._add_error(f"Error during finalization: {e}")

    def create_backup_zip(self) -> str:
        """
        Create a ZIP backup of all unbundled DjVu files.

        Returns:
            Path to created backup ZIP file
        """
        if self.djvu_file.bundled:
            raise ValueError(f"File {self.djvu_file.path} is already bundled")

        djvu_path = self.djvu_file.path

        # Get base filename without extension
        basename = os.path.basename(djvu_path)
        stem = os.path.splitext(basename)[0]

        # Create backup ZIP path
        backup_file = os.path.join(self.config.backup_path, f"{stem}.zip")

        # Get list of page files
        part_files = self.get_part_filenames()
        djvu_dir = os.path.dirname(djvu_path)

        # Create ZIP archive
        with zipfile.ZipFile(backup_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add main index file
            zipf.write(djvu_path, basename)

            # Add each page file
            for part_file in part_files:
                part_path = os.path.join(djvu_dir, part_file)
                if os.path.exists(part_path):
                    zipf.write(part_path, part_file)
                else:
                    self.errors.append(Exception(f"missing {part_path}"))

        return backup_file

    def convert_to_bundled(self, output_path: str = None) -> str:
        """
        Convert self.djvu_file to bundled format using djvmcvt.

        Returns:
            Path to bundled file
        """
        djvu_path = self.djvu_file.path

        if output_path is None:
            dirname = os.path.dirname(djvu_path)
            basename = os.path.basename(djvu_path)
            stem = os.path.splitext(basename)[0]
            output_path = os.path.join(dirname, f"{stem}_bundled.djvu")

        cmd = f"djvmcvt -b {shlex.quote(djvu_path)} {shlex.quote(output_path)}"
        result = self.shell.run(cmd, text=True, debug=self.debug)

        if result.returncode != 0:
            raise RuntimeError(f"djvmcvt failed: {result.stderr}")

        if not os.path.exists(output_path):
            raise RuntimeError(f"Bundled file not created: {output_path}")

        return output_path


    @classmethod
    def convert_djvu_to_ppm(
        cls,
        djvu_path: str,
        page_num: int,
        output_path: str,
        size: str = None,  # e.g., "2480x3508" for A4 @ 300dpi
        shell: Shell=None,
        debug: bool = False,
    ) -> None:
        """Convert DJVU page to PPM using ddjvu CLI."""
        # Build command parts
        cmd_parts = [
            "ddjvu",
            "-format=ppm",
            f"-page={page_num + 1}",
        ]

        if size:
            cmd_parts.append(f"-size={size}")

        cmd_parts.extend([
            shlex.quote(djvu_path),
            shlex.quote(output_path),
        ])

        cmd = " ".join(cmd_parts)
        result = shell.run(cmd, text=True, debug=debug)

        if result.returncode != 0:
            raise RuntimeError(
                f"ddjvu failed (rc={result.returncode}):\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

    @classmethod
    def convert_ppm_to_png(cls,ppm_path: str, png_path: str) -> None:
        """Convert PPM to PNG using PIL."""
        img = Image.open(ppm_path)
        img.save(png_path, "PNG")

    @classmethod
    def render_djvu_page_cli(
        cls,
        djvu_path: str,
        page_num: int,
        output_path: str,
        size: str, # e.g., "2480x3508" for A4 @ 300dpi
        debug: bool = False,
        shell: Shell = None,
    ) -> str:
        """Render a DJVU page to PNG using ddjvu CLI."""
        if shell is None:
            shell = Shell()

        # Create temporary PPM file
        with tempfile.NamedTemporaryFile(suffix=".ppm", delete=False) as tmp_file:
            tmp_ppm_path = tmp_file.name

        try:
            # Step 1: DJVU → PPM
            cls.convert_djvu_to_ppm(
                djvu_path=djvu_path,
                page_num=page_num,
                output_path=tmp_ppm_path,
                size=size,
                shell=shell,
                debug=debug,
            )

            # Step 2: PPM → PNG
            cls.convert_ppm_to_png(tmp_ppm_path, output_path)

            return output_path

        finally:
            # Clean up
            if os.path.exists(tmp_ppm_path):
                os.remove(tmp_ppm_path)

    def is_valid(self) -> bool:
        """Check if the bundle has no errors."""
        return len(self.errors) == 0

    def get_error_summary(self) -> str:
        """Get a formatted summary of all errors."""
        if not self.errors:
            return "No errors found"

        return f"Found {len(self.errors)} error(s):\n" + "\n".join(
            f"  - {error}" for error in self.errors
        )
