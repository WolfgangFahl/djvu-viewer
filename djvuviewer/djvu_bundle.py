"""
Created on 2026-01-03

@author: wf
"""

import logging
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import tempfile
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from basemkit.shell import Shell

from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile
from djvuviewer.image_convert import ImageConverter
from djvuviewer.packager import Packager

logger = logging.getLogger(__name__)

class DjVuBundle:
    """
    DjVu bundle handling with validation and error collection.
    """

    def __init__(
        self,
        djvu_file: DjVuFile,
        config: DjVuConfig = None,
        debug: bool = False,
        mw_images: Optional[Dict[str, 'MediaWikiImage']] = None,
    ):
        """
        Initialize DjVuBundle with a DjVuFile instance.

        Args:
            djvu_file: The DjVuFile metadata
            config: configuration
            debug: if True
            mw_images: Optional dict of MediaWiki images keyed by wiki name
        """
        self.djvu_file = djvu_file
        if config is None:
            config = DjVuConfig.get_instance()
        self.full_path=f"{config.images_path}/images/{djvu_file.path}"
        self.djvu_dir = os.path.dirname(self.full_path)
        self.basename = os.path.basename(djvu_file.path)
        self.stem = os.path.splitext(self.basename)[0]
        self.config = config
        self.debug = debug
        self.mw_images: Dict[str, 'MediaWikiImage'] = mw_images or {}
        self.errors: List[str] = []
        self.shell = Shell()
        self.djvu_dump_log = None

    @property
    def error_count(self) -> int:
        """Check if the bundle has errors."""
        error_count=len(self.errors)
        return error_count

    @property
    def bundled_file_path(self) -> str:
        bundled_file_path=os.path.join(self.djvu_dir, f"{self.stem}_bundled.djvu")
        return bundled_file_path

    @property
    def backup_file(self) -> str:
        # Create backup ZIP path
        backup_file = os.path.join(self.config.backup_path, f"{self.stem}.zip")
        return backup_file

    @property
    def has_incomplete_bundling(self) -> bool:
        """Check if bundling was interrupted (both files exist)."""
        incomplete=(os.path.exists(self.full_path) and
                os.path.exists(self.bundled_file_path))
        return incomplete

    @property
    def image_wiki(self) -> Optional['MediaWikiImage']:
        """Get image from main wiki."""
        image_wiki=self.mw_images.get('wiki')
        return image_wiki

    @property
    def image_new(self) -> Optional['MediaWikiImage']:
        """Get image from new wiki."""
        image_new=self.mw_images.get('new')
        return image_new

    @property
    def description_url_wiki(self) -> Optional[str]:
        """Get description URL from main wiki image."""
        file_url = self.image_wiki.descriptionurl if self.image_wiki else None
        return file_url

    @property
    def description_url_new(self) -> Optional[str]:
        """Get description URL from new wiki image."""
        file_url = self.image_new.descriptionurl if self.image_new else None
        return file_url

    @property
    def description_url(self) -> Optional[str]:
        """Get the first available description URL (wiki or new)."""
        file_url = self.description_url_wiki or self.description_url_new
        return file_url

    @classmethod
    def from_package(cls, package_file: str, with_check: bool = True) -> "DjVuBundle":
        """
        Create a DjVuBundle from a package file.

        Args:
            package_file: Path to the package archive
            with_check: If True, automatically run validation checks

        Returns:
            DjVuBundle: Instance with loaded metadata and optional validation

        Example:
            bundle = DjVuBundle.from_package("document.zip")
            if bundle.error_count>0:
                print(bundle.get_error_summary())
        """
        package_path = Path(package_file)
        djvu_file = DjVuFile.from_package(package_path)
        bundle = cls(djvu_file)

        if with_check:
            bundle.check_package(package_file)

        return bundle

    def _add_error(self, message: str):
        """Add an error message to the error list."""
        self.errors.append(message)

    def check_package(self, package_file: str, relurl: Optional[str] = None):
        """
        Verify that a package file exists and contains expected contents with correct dimensions.
        Collects errors instead of raising exceptions.

        Args:
            package_file: Path to the package file to validate
            relurl: Optional relative URL for error context
        """
        context = f" for {relurl}" if relurl else ""
        package_path = Path(package_file)
        yaml_indexfile = Packager.get_indexfile(package_path)

        # Check file exists
        if not package_path.is_file():
            self._add_error(
                f"Expected package file '{package_file}' was not created{context}"
            )
            return

        # Check if archive is readable
        if not Packager.archive_exists(package_path):
            self._add_error(
                f"Package file '{package_file}' is not a valid archive{context}"
            )
            return

        try:
            # Get list of members using abstracted interface
            members = Packager.list_archive_members(package_path)

            # Check archive is not empty
            if len(members) == 0:
                self._add_error(f"Package file '{package_file}' is empty{context}")
                return

            # Find PNG files
            png_files = [m for m in members if m.endswith(".png")]
            if len(png_files) == 0:
                self._add_error(f"No PNG files found in package{context}")

            # Check for YAML index file
            if not yaml_indexfile in members:
                self._add_error(f"Expected  {yaml_indexfile} file in package")

            # Create dimension mapping from metadata
            page_dimensions = {
                i + 1: (page.width, page.height)
                for i, page in enumerate(self.djvu_file.pages)
            }

            # Verify PNG dimensions match metadata
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
                    png_data = Packager.read_from_package(package_path, png_file)
                    image_conv = ImageConverter(png_data)
                    actual_width, actual_height = image_conv.size

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

        except Exception as e:
            self._add_error(
                f"Unexpected error checking package file '{package_file}': {e}{context}"
            )
        # done
        pass

    def get_part_filenames_from_dump(self) -> List[str]:
        """
        get a list of my part file names
        """
        if not self.djvu_dump_log:
            self.djvu_dump()
        part_files = []
        for line in self.djvu_dump_log.split("\n"):
            match = re.search(r"^\s+(.+\.(?:djvu|djbz))\s+->", line)
            if match:
                part_files.append(match.group(1))
        return part_files

    def get_part_filenames(self) -> List[str]:
        """
        get a list of my part file names
        """
        # Get list of component files to remove
        if self.has_incomplete_bundling:
            part_files=[]
        else:
            part_files = self.get_part_filenames_from_dump()
        return part_files

    def djvu_dump(self) -> str:
        """
        Run djvudump on self.djvu_file.djvu_path and return output.
        Adds error to self.errors on failure.

        Returns:
            djvudump output string (empty on error)
        """
        output=""
        if not os.path.exists(self.full_path):
            self._add_error(f"File not found: {self.full_path}")
        else:
            cmd = f"djvudump {shlex.quote(self.full_path)}"
            result = self.run_cmd(cmd, "djvudump failed")
            if result.returncode == 0:
                output = result.stdout
        self.djvu_dump_log=output
        return output

    def finalize_bundling(self, zip_path: str, bundled_path: str, sleep: float = 1.0):
        """
        Finalize bundling by removing the original main file and
        all zipped component files then move the bundled_path file to the original.

        Args:
            zip_path: Path to the backup ZIP file (for verification)
            bundled_path: Path to the new bundled DjVu file
        """
        # Verify backup ZIP exists before proceeding
        if not os.path.exists(zip_path):
            self._add_error(f"Backup ZIP not found: {zip_path}")
            return

        # Verify bundled file exists
        if not os.path.exists(bundled_path):
            self._add_error(f"Bundled file not found: {bundled_path}")
            return

        # Get list of component files to remove
        part_files = self.get_part_filenames()

        try:
            original_stat = os.stat(self.full_path)
            original_atime = original_stat.st_atime
            original_mtime = original_stat.st_mtime
            if self.debug:
                print(
                    f"Preserving timestamps - atime: {original_atime}, mtime: {original_mtime}"
                )

            # Remove component parts
            # only if original state is still available
            for part_file in part_files:
                part_path = os.path.join(self.djvu_dir, part_file)
                if os.path.exists(part_path):
                    os.remove(part_path)
                    if self.debug:
                        print(f"Removed component: {part_path}")

            if not os.access(self.djvu_dir, os.W_OK):
                self._add_error(
                    f"No write permission in directory: {self.djvu_dir}\n"
                    f"Try: sudo chmod g+w {self.djvu_dir}"
                )
                return

            if self.debug:
                print(f"trying to\nmv {bundled_path} {self.full_path}")
            if self.move_file(bundled_path, self.full_path):
                # Restore original timestamps
                os.sync()
                print(f"Sleeping {sleep} secs")
                time.sleep(sleep)
                os.utime(self.full_path, (original_atime, original_mtime))
                if self.debug:
                    print(f"Restored timestamps to {self.full_path}")

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

        backup_file = self.backup_file

        # Get list of page files
        part_files = self.get_part_filenames()

        # Create ZIP archive
        with zipfile.ZipFile(backup_file, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add main index file
            zipf.write(self.full_path, self.basename)

            # Add each page file
            for part_file in part_files:
                part_path = os.path.join(self.djvu_dir, part_file)
                if os.path.exists(part_path):
                    zipf.write(part_path, part_file)
                else:
                    self.errors.append(Exception(f"missing {part_path}"))

        return backup_file

    def run_cmd(self, cmd: str, error_msg: str = None) -> subprocess.CompletedProcess:
        """Run shell command with error handling."""
        result = self.shell.run(cmd, text=True, debug=self.debug)

        if result.returncode != 0:
            msg = error_msg or f"Command failed: {cmd}"
            if self.debug:
                print(f"{result.stdout}")
            self._add_error(f"{msg}\n{result.stderr}")

        return result

    def move_file(self, src: str, dst: str) -> bool:
        """Move file using copy+delete pattern for better reliability"""
        try:
            # First copy the file
            shutil.copy2(src, dst)  # copy2 preserves metadata
            if self.debug:
                print(f"Copied: {src} → {dst}")

            # Then remove the source
            os.remove(src)
            if self.debug:
                print(f"Removed source: {src}")

            return True

        except PermissionError as e:
            if self.debug:
                print(f"Permission error moving {src} → {dst}: {e}")
            self._add_error(f"Permission error: {e}")
            return False

        except FileNotFoundError as e:
            if self.debug:
                print(f"File not found {src} → {dst}: {e}")
            self._add_error(f"File not found: {src}")
            return False

        except Exception as e:
            if self.debug:
                print(f"Failed to move {src} → {dst}: {e}")
            self._add_error(f"Move failed: {e}")
        return False

    def get_docker_cmd(self) -> str:
        """
        get the docker exec command to update the mediawiki
        """
        djvu_path = self.djvu_file.path
        # MediaWiki maintenance call if container is configured
        if hasattr(self.config, "container_name") and self.config.container_name:
            filename = os.path.basename(djvu_path)
            docker_cmd = f"docker exec {self.config.container_name} php maintenance/refreshImageMetadata.php --force --mime=image/vnd.djvu --start={filename} --end={filename}"
        return docker_cmd

    def update_index_database(self) -> tuple[bool, str]:
        """
        Update SQLite database after bundling.
        Sets bundled=1 and updates filesize to actual file size.

        Returns:
            tuple[bool, str]: (success, message)
        """
        if not hasattr(self.config, "db_path") or not self.config.db_path:
            msg = "No database path configured"
            self._add_error(msg)
            return False, msg

        if not os.path.exists(self.full_path):
            msg = f"File not found for DB update: {self.full_path}"
            self._add_error(msg)
            return False, msg

        try:
            actual_size = os.path.getsize(self.full_path)
            djvu_path=self.djvu_file.path
            with sqlite3.connect(self.config.db_path, timeout=10.0) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE DjVu SET bundled = 1, filesize = ? WHERE path = ?",
                    (actual_size, djvu_path),
                )
                conn.commit()

                if cursor.rowcount == 0:
                    msg = f"No database record found for path: {djvu_path}"
                    self._add_error(msg)
                    return False, msg

            msg = f"Database updated: bundled=1, filesize={actual_size} for {djvu_path}"
            return True, msg

        except sqlite3.Error as e:
            msg = f"Database update failed: {e}"
            self._add_error(msg)
            return False, msg

    def generate_bundling_script(self, update_index_db: bool = False) -> str:
        """
        Generate an idempotent bash script for bundling.
        Each step is a function that can be safely retried.
        """
        part_files = self.get_part_filenames()
        backup_file = self.backup_file
        bundled_file =self.bundled_file_path
        djvu_path=self.djvu_file.path

        # Build part files for zip command
        part_files_zip = " \\\n        ".join(shlex.quote(pf) for pf in part_files)

        # Build part removal commands
        part_removals = "\n    ".join(
            f'rm -f {shlex.quote(os.path.join(self.djvu_dir, pf))}'
            for pf in part_files
        )

        docker_cmd = self.get_docker_cmd()
        docker_step = f'    refresh_mediawiki\n' if docker_cmd else ''

        # Database update function
        db_func = ""
        db_call = ""
        if update_index_db:
            db_func = f"""
    update_database() {{
        [ -f "$FULL_PATH" ] || error "Missing file for DB update"
        log "Updating database..."
        FILESIZE=$(stat -f%z "$FULL_PATH" 2>/dev/null || stat -c%s "$FULL_PATH")
        sqlite3 "{self.config.db_path}" "UPDATE DjVu SET bundled=1, filesize=$FILESIZE WHERE path='$DJVU_PATH';"
        log "DB updated: bundled=1, size=$FILESIZE"
    }}
    """
            db_call = "    update_database\n"

        mediawiki_func = ""
        if docker_cmd:
            mediawiki_func = f"""
    refresh_mediawiki() {{
        log "Refreshing MediaWiki..."
        {docker_cmd}
        log "✓ MediaWiki refreshed"
    }}
    """

        script = f"""#!/bin/bash
# DjVu Bundling Script - {djvu_path}
# Generated: {datetime.now().isoformat()}
# IDEMPOTENT: Safe to re-run if any step fails

set -e

# ============================================================================
# CONFIGURATION
# ============================================================================

DJVU_PATH={shlex.quote(djvu_path)}
FULL_PATH={shlex.quote(self.full_path)}
DJVU_DIR={shlex.quote(self.djvu_dir)}
BACKUP_FILE={shlex.quote(backup_file)}
BUNDLED_FILE={shlex.quote(bundled_file)}
TIMESTAMP_FILE="$DJVU_DIR/.{self.stem}_timestamps"

# ============================================================================
# UTILITIES
# ============================================================================

log() {{ echo "[$(date '+%H:%M:%S')] $1"; }}
error() {{ echo "[ERROR] $1" >&2; exit 1; }}

# ============================================================================
# STEPS (Each is idempotent)
# ============================================================================

backup_original() {{
    [ -f "$BACKUP_FILE" ] && {{ log "Backup exists, skipping"; return 0; }}

    log "Creating backup..."
    [ -f "$FULL_PATH" ] || error "Source file missing"

    cd "$DJVU_DIR"
    zip -j "$BACKUP_FILE" {shlex.quote(self.basename)} \\
        {part_files_zip}

    [ -f "$BACKUP_FILE" ] || error "Backup creation failed"
    log "✓ Backup: $BACKUP_FILE"
}}

save_timestamps() {{
    [ -f "$TIMESTAMP_FILE" ] && {{ log "Timestamps saved, skipping"; return 0; }}

    log "Saving timestamps..."
    stat -c "%X %Y" "$FULL_PATH" > "$TIMESTAMP_FILE" 2>/dev/null || \\
        stat -f "%a %m" "$FULL_PATH" > "$TIMESTAMP_FILE"
    log "✓ Timestamps saved"
}}

bundle_djvu() {{
    [ -f "$BUNDLED_FILE" ] && {{ log "Already bundled, skipping"; return 0; }}

    log "Converting to bundled format..."
    [ -f "$FULL_PATH" ] || error "Source file missing"

    djvmcvt -b "$FULL_PATH" "$BUNDLED_FILE"
    [ -f "$BUNDLED_FILE" ] || error "Bundling failed"
    log "✓ Created: $BUNDLED_FILE"
}}

cleanup_originals() {{
    [ ! -f "$FULL_PATH" ] && {{ log "Originals removed, skipping"; return 0; }}

    log "Removing originals..."
    [ -f "$BACKUP_FILE" ] || error "No backup, cannot remove originals"
    [ -f "$BUNDLED_FILE" ] || error "No bundled file, cannot remove originals"

    rm -f "$FULL_PATH"
    {part_removals}
    log "✓ Originals removed"
}}

finalize_bundled() {{
    [ -f "$FULL_PATH" ] && [ ! -f "$BUNDLED_FILE" ] && {{
        log "Already in place, skipping"
        return 0
    }}

    log "Moving bundled file..."
    [ -f "$BUNDLED_FILE" ] || error "Bundled file missing"

    mv "$BUNDLED_FILE" "$FULL_PATH"
    sync; sleep 1
    log "✓ Moved to: $FULL_PATH"
}}

restore_timestamps() {{
    [ ! -f "$TIMESTAMP_FILE" ] && {{ log "No timestamps to restore"; return 0; }}

    log "Restoring timestamps..."
    read ATIME MTIME < "$TIMESTAMP_FILE"
    touch -a -d "@$ATIME" "$FULL_PATH"
    touch -m -d "@$MTIME" "$FULL_PATH"
    rm -f "$TIMESTAMP_FILE"
    log "✓ Timestamps restored"
}}

{mediawiki_func}{db_func}

# ============================================================================
# MAIN
# ============================================================================

main() {{
    log "Starting bundling: $DJVU_PATH"

    backup_original
    save_timestamps
    bundle_djvu
    cleanup_originals
    finalize_bundled
    restore_timestamps
{docker_step}{db_call}
    log "✅ COMPLETE: $DJVU_PATH"
}}

main "$@"
"""
        return script

    def convert_to_bundled(self, output_path: str = None) -> str:
        """
        Convert self.djvu_file to bundled format using djvmcvt.

        Returns:
            Path to bundled file
        """
        if output_path is None:
            output_path = self.bundled_file_path

        cmd = f"djvmcvt -b {shlex.quote(self.full_path)} {shlex.quote(output_path)}"
        self.run_cmd(cmd, "Failed to bundle DjVu file")

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
        shell: Shell = None,
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

        cmd_parts.extend(
            [
                shlex.quote(djvu_path),
                shlex.quote(output_path),
            ]
        )

        cmd = " ".join(cmd_parts)
        result = shell.run(cmd, text=True, debug=debug)

        if result.returncode != 0:
            raise RuntimeError(
                f"ddjvu failed (rc={result.returncode}):\n"
                f"stdout: {result.stdout}\n"
                f"stderr: {result.stderr}"
            )

    @classmethod
    def render_djvu_page_cli(
        cls,
        djvu_path: str,
        page_num: int,
        output_path: str,
        size: str,  # e.g., "2480x3508" for A4 @ 300dpi
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
            ImageConverter.convert_ppm_to_png(tmp_ppm_path, output_path)

            return output_path

        finally:
            # Clean up
            if os.path.exists(tmp_ppm_path):
                os.remove(tmp_ppm_path)

    def get_error_summary(self) -> str:
        """Get a formatted summary of all errors."""
        if not self.errors:
            return "No errors found"

        return f"Found {len(self.errors)} error(s):\n" + "\n".join(
            f"  - {error}" for error in self.errors
        )
