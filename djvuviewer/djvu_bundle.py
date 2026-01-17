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
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

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
        mw_images: Optional[Dict[str, "MediaWikiImage"]] = None,
    ):
        """
        Initialize DjVuBundle with a DjVuFile instance.

        Args:
            djvu_file: The DjVuFile metadata
            config: configuration
            use_sudo: if True, use sudo for file operations
            debug: if True output debug info where appropriate
            mw_images: Optional dict of MediaWiki images keyed by wiki name
        """
        self.djvu_file = djvu_file
        if config is None:
            config = DjVuConfig.get_instance()
        self.full_path = config.full_path(djvu_file.path)
        self.djvu_dir = os.path.dirname(self.full_path)
        self.basename = os.path.basename(djvu_file.path)
        self.stem = os.path.splitext(self.basename)[0]
        self.config = config
        self.use_sudo = config.use_sudo
        self.debug = debug
        self.mw_images: Dict[str, "MediaWikiImage"] = mw_images or {}
        self.errors: List[str] = []
        self.shell = Shell()
        self.djvu_dump_log = None

    @property
    def error_count(self) -> int:
        """Check if the bundle has errors."""
        error_count = len(self.errors)
        return error_count

    @property
    def bundled_file_path(self) -> str:
        bundled_file_path = os.path.join(self.djvu_dir, f"{self.stem}_bundled.djvu")
        return bundled_file_path

    @property
    def backup_file(self) -> str:
        # Create backup ZIP path
        backup_file = os.path.join(self.config.backup_path, f"{self.stem}.zip")
        return backup_file

    @property
    def has_incomplete_bundling(self) -> bool:
        """Check if bundling was interrupted (both files exist)."""
        incomplete = os.path.exists(self.full_path) and os.path.exists(
            self.bundled_file_path
        )
        return incomplete

    @property
    def image_wiki(self) -> Optional["MediaWikiImage"]:
        """Get image from main wiki."""
        image_wiki = self.mw_images.get("wiki")
        return image_wiki

    @property
    def image_new(self) -> Optional["MediaWikiImage"]:
        """Get image from new wiki."""
        image_new = self.mw_images.get("new")
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

    def get_part_filenames_from_dump(self, djvu_path: str) -> List[str]:
        """
        Get a list of part file names extracted from a DjVu dump.

        Args:
            djvu_path (str): Path to the DjVu file or the directory containing the dump.

        Returns:
            List[str]: List of part file names.
        """
        if not self.djvu_dump_log:
            self.djvu_dump(djvu_path)
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
            # we already bundled so the part_file list is in the bundled_file_path
            part_files = self.get_part_filenames_from_dump(self.bundled_file_path)
        else:
            part_files = self.get_part_filenames_from_dump(self.full_path)
        return part_files

    def djvu_dump(self, djvu_path: str = None) -> str:
        """
        Run djvudump on self.djvu_file.djvu_path and return output.
        Adds error to self.errors on failure.

        Args:
            djvu_path(str): full_path to the djvu file (bundled or indexed) to be dumped
        Returns:
            djvudump output string (empty on error)
        """
        output = ""
        if djvu_path is None:
            djvu_path = self.full_path
        if not os.path.exists(self.full_path):
            self._add_error(f"File not found: {djvu_path}")
        else:
            cmd = f"djvudump {shlex.quote(djvu_path)}"
            result = self.run_cmd(cmd, "djvudump failed")
            if result.returncode == 0:
                output = result.stdout
        self.djvu_dump_log = output
        return output

    def finalize_bundling(self):
        """
        Finalize bundling: remove originals, move bundled file to final location.

        """
        zip_path = self.backup_file
        bundled_path = self.bundled_file_path
        # Validate prerequisites
        if not Path(zip_path).exists():
            self._add_error(f"Backup ZIP not found: {zip_path}")
            return

        if not Path(bundled_path).exists():
            self._add_error(f"Bundled file not found: {bundled_path}")
            return

        # Get component files to remove
        part_files = self.get_part_filenames()

        try:
            # Remove component parts (only safe because backup exists)
            for part_file in part_files:
                part_path = Path(self.djvu_dir) / part_file
                if part_path.exists():
                    part_path.unlink()
                    if self.debug:
                        print(f"Removed: {part_file}")

            # Move bundled file to final location
            # handles ALL permission and timestamp logic
            if self.safe_move(bundled_path, self.full_path):
                self.djvu_file.bundled = True
                if self.debug:
                    print(f"✓ Finalized: {self.full_path}")

        except Exception as e:
            self._add_error(f"Finalization error: {e}")

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

    def set_timestamps(self, path: str, timestamps: tuple) -> None:
        """
        Set file timestamps with cross-platform sudo fallback.

        Uses os.utime() first, falls back to platform-specific touch commands.
        """
        atime, mtime = timestamps
        dest_path = Path(path)

        try:
            os.utime(dest_path, (atime, mtime))
            return
        except (OSError, PermissionError) as e:
            if not self.use_sudo:
                if self.debug:
                    print(f"Warning: Could not restore timestamps: {e}")
                return

            # Sudo fallback - use platform-agnostic format (no fractional seconds)
            atime_dt = datetime.fromtimestamp(atime)
            mtime_dt = datetime.fromtimestamp(mtime)

            # Format: YYMMDDhhmm (works on both GNU and BSD touch)
            atime_fmt = atime_dt.strftime("%Y%m%d%H%M")
            mtime_fmt = mtime_dt.strftime("%Y%m%d%H%M")

            # Set times separately (more reliable than combined -t)
            self.run_cmd(
                f"sudo touch -a -t {atime_fmt} {shlex.quote(path)}",
                "Warning: Failed to restore access time",
            )
            self.run_cmd(
                f"sudo touch -m -t {mtime_fmt} {shlex.quote(path)}",
                "Warning: Failed to restore modification time",
            )

    def move_file(self, src: str, dst: str) -> bool:
        """
        Move file using copy+delete pattern for better reliability in
        CIFS envs
        """
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

    def safe_move(self, source: str, dest: str, preserve_times: bool = True) -> bool:
        """
        Move file with timestamp preservation and permission handling.

        Handles:
        - Standard filesystems and CIFS mounts
        - Permission issues (sudo when configured)
        - Atomic timestamp preservation

        Args:
            source: Source file path
            dest: Destination file path
            preserve_times: Preserve timestamps (default: True)

        Returns:
            True on success, False on failure (errors logged)
        """
        source_path = Path(source)
        dest_path = Path(dest)

        if not source_path.exists():
            self._add_error(f"Source not found: {source}")
            return False

        try:
            # Capture timestamps before move
            timestamps = None
            if preserve_times:
                stat_info = source_path.stat()
                timestamps = (stat_info.st_atime, stat_info.st_mtime)

            # Ensure write permission on destination directory
            dest_dir = dest_path.parent
            if not os.access(dest_dir, os.W_OK):
                if self.use_sudo:
                    result = self.run_cmd(
                        f"sudo chmod g+w {shlex.quote(str(dest_dir))}",
                        f"Failed to set write permission on {dest_dir}",
                    )
                    if result.returncode != 0:
                        return False
                else:
                    self._add_error(f"No write permission: {dest_dir}")
                    return False

            # If destination exists, ensure we can overwrite it
            if dest_path.exists() and not os.access(dest_path, os.W_OK):
                if self.use_sudo:
                    result = self.run_cmd(
                        f"sudo chmod g+w {shlex.quote(dest)}",
                        f"Failed to set write permission on {dest}",
                    )
                    if result.returncode != 0:
                        return False
                else:
                    self._add_error(f"No write permission: {dest}")
                    return False

            # Perform the move
            if self.use_sudo:
                result = self.run_cmd(
                    f"sudo mv {shlex.quote(source)} {shlex.quote(dest)}",
                    f"Move failed: {source} → {dest}",
                )
                if result.returncode != 0:
                    return False
                # Restore timestamps
                if timestamps:
                    self.set_timestamps(dest, timestamps)
            else:
                # move with timestamp preservation
                self.move_file(source, dest)
                # Critical for CIFS: ensure data is written before timestamp operations
                os.sync()

            if self.debug:
                print(f"✓ Moved: {Path(source).name} → {Path(dest).name}")

            return True

        except Exception as e:
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
            djvu_path = f"/images{self.djvu_file.path}"
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
        bundled_file = self.bundled_file_path
        djvu_path = self.djvu_file.path

        # Build part files for zip command
        part_files_zip = " \\\n        ".join(shlex.quote(pf) for pf in part_files)

        # Build part removal commands
        part_removals = "\n    ".join(
            f"rm -f {shlex.quote(os.path.join(self.djvu_dir, pf))}" for pf in part_files
        )

        docker_cmd = self.get_docker_cmd()
        docker_step = f"    refresh_mediawiki\n" if docker_cmd else ""

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

    def convert_to_bundled(self):
        """
        Convert self.djvu_file to bundled format using djvmcvt.

        """
        output_path = self.bundled_file_path

        cmd = f"djvmcvt -b {shlex.quote(self.full_path)} {shlex.quote(output_path)}"
        result = self.run_cmd(cmd, "Failed to bundle DjVu file")

        if not os.path.exists(output_path) or result.returncode != 0:
            raise RuntimeError(
                f"Bundled file not created: {output_path} return code {result.returncode}"
            )

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

    def bundle(
        self,
        create_backup: bool = True,
        update_wiki: bool = True,
        update_index_db: bool = True,
        on_progress: Optional[Callable[[str], None]] = None,
        on_error: Optional[Callable[[str], None]] = None,
    ) -> bool:
        """
        Execute complete bundling workflow.

        Args:
            create_backup: Create backup ZIP before bundling
            update_wiki: Run MediaWiki maintenance command after bundling
            update_index_db: Update the index database after bundling
            on_progress: Callback for progress messages
            on_error: Callback for error messages

        Returns:
            bool: True if successful, False if errors occurred
        """

        def progress(msg: str):
            if on_progress:
                on_progress(msg)

        def error(msg: str):
            if on_error:
                on_error(msg)

        try:
            # Step 1: Create backup (if needed)
            zip_path = self.backup_file
            if create_backup and not os.path.exists(zip_path):
                progress(f"Creating backup ZIP...")
                zip_path = self.create_backup_zip()
                if self.error_count > 0:
                    error(f"Backup failed with {self.error_count} errors")
                    return False
                progress(f"Backup created: {zip_path}")

            # Step 2: Convert to bundled format
            if not os.path.exists(self.bundled_file_path):
                progress("Converting to bundled format...")
                self.convert_to_bundled()
                if self.error_count > 0:
                    error(f"Bundling failed with {self.error_count} errors")
                    return False
                progress(f"Bundled file created: {self.bundled_file_path}")

            # Step 3: Finalize (replace original)
            progress("Finalizing bundling...")
            self.finalize_bundling()
            if self.error_count > 0:
                error(f"Finalization failed with {self.error_count} errors")
                return False
            progress("Bundling finalized")

            # Step 4: Update MediaWiki (optional)
            if update_wiki:
                docker_cmd = self.get_docker_cmd()
                if docker_cmd:
                    progress(f"Running MediaWiki update...")
                    result = self.shell.run(docker_cmd)
                    if result.returncode != 0:
                        error(f"MediaWiki update failed: {result.stderr}")
                        return False
                    progress("MediaWiki updated")

            # Step 5: Update index database (optional)
            if update_index_db:
                progress("Updating index database...")
                success, msg = self.update_index_database()
                if success:
                    progress(msg)
                else:
                    error(msg)
                    return False

            progress(f"✅ Successfully bundled {self.djvu_file.path}")
            return True

        except Exception as e:
            self.errors.append(e)
            error(f"Bundling failed: {e}")
            return False
