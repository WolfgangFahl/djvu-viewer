"""
Created on 2025-02-25

@author: wf
"""

import argparse
import logging
from argparse import ArgumentParser, Namespace
from typing import List, Optional

from basemkit.base_cmd import BaseCmd
from basemkit.profiler import Profiler

from djvuviewer.djvu_actions import DjVuActions
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_context import DjVuContext
from djvuviewer.version import Version


class DjVuCmd(BaseCmd):
    """
    Command-line interface for DjVu file processing and conversion.

    Provides commands for:
    - catalog: Scan and catalog DjVu files
    - convert: Convert DjVu files to PNG format
    - dbupdate: Update database with processed file metadata
    - initdb: Initialize the database schema
    """

    def __init__(self, args: argparse.Namespace):
        """
        Initialize the DjVu command-line interface.

        Args:
            args: Parsed command-line arguments
        """
        super().__init__(Version())
        self.args = args
        self.config = DjVuConfig.get_instance()
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    def add_arguments(self, parser: ArgumentParser) -> ArgumentParser:
        """
        Add command-specific arguments to the argument parser.

        Args:
            parser: ArgumentParser to add arguments to

        Returns:
            The modified ArgumentParser
        """
        super().add_arguments(parser)

        parser.add_argument(
            "--images-path",
            default=self.config.images_path,
            help="Base path for DjVu files e.g. images directory of a wiki",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of pages to process in each batch (default: %(default)s)",
        )
        parser.add_argument(
            "--command",
            choices=["bundle", "catalog", "convert", "dbupdate", "initdb"],
            required=True,
            help="Command to execute",
        )
        parser.add_argument(
            "--db-path",
            default=self.config.db_path,
            help="Path to the database (default: %(default)s)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=5000,
            help="Maximum number of files to process (default: %(default)s)",
        )
        parser.add_argument(
            "--limit_gb",
            type=int,
            default=16,
            help="Memory limit in GB (default: %(default)s)",
        )
        parser.add_argument(
            "--max-errors",
            type=float,
            default=1.0,
            help="Maximum allowed error percentage before skipping database update (default: %(default)s)",
        )
        parser.add_argument(
            "--sleep",
            type=float,
            default=2.0,
            help="number of seconds to sleep before trying to move to avoid actimeo issues on CIFS mounts (default: %(default)s)",
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=None,
            help="Maximum number of worker threads (default: CPU count * 4)",
        )
        parser.add_argument(
            "--output-path",
            help="Path for PNG files",
        )
        parser.add_argument(
            "--pngmode",
            choices=["cli", "pil"],
            default="pil",
            help="PNG generation mode: cli (ddjvu command) or pil (Python Imaging Library) (default: %(default)s)",
        )
        parser.add_argument(
            "--packagemode",
            choices=["tar", "zip", "none"],
            default="zip",
            help="package generation mode: tar or zip (default: %(default)s)",
        )
        parser.add_argument(
            "--serial",
            action="store_true",
            help="Use serial processing instead of parallel",
        )
        parser.add_argument(
            "--sort",
            choices=["asc", "desc"],
            default="asc",
            help="Sort by page count (asc=smallest first, default: %(default)s)",
        )
        parser.add_argument(
            "--url",
            help="Process a single DjVu file (only valid in convert mode)",
        )
        # Bundle-specific arguments
        parser.add_argument(
            "--backup-path",
            default=self.config.backup_path,
            help="Path for backup ZIP files default: %(default)s)",
        )
        parser.add_argument(
            "--cleanup",
            action="store_true",
            help="Remove thumbnails during bundling",
        )
        parser.add_argument(
            "--script",
            action="store_true",
            help="Generate bash script instead of executing",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show operations without executing (implies --script)",
        )
        parser.add_argument(
            "--container-name",
            default=self.config.container_name,
            help="MediaWiki container for maintenance call default: %(default)s)",
        )

        return parser

    def handle_args(self, args: Namespace) -> bool:
        """
        Handle the command-line arguments and execute the requested command.

        Sets up the configuration and dispatches to the appropriate command handler.

        Args:
            args: Parsed command-line arguments

        Returns:
            True if handled (no further processing needed)
        """
        handled = super().handle_args(args)
        # Configure paths
        self.config.db_path = self.args.db_path
        self.config.images_path = self.args.images_path
        self.config.backup_path = self.args.backup_path
        self.config.container_name = self.args.container_name
        self.context = DjVuContext(self.config, self.args)
        self.actions = DjVuActions(context=self.context)

        # Dispatch to command handler
        command_handlers = {
            "bundle": self.bundle,
            "catalog": self.catalog,
            "convert": self.convert,
            "dbupdate": self.dbupdate,
            "initdb": self.initdb,
        }

        handler = command_handlers.get(self.args.command)
        if handler:
            self.profiler = Profiler(self.args.command)
            handler()
            handled = True
        else:
            print(f"unknown command {self.args.command}")
        return handled

    def bundle(self):
        """
        Execute the bundle command.

        Converts indirect/multi-file DjVu files to bundled format.
        """
        self.actions.bundle_djvu_files()
        self.actions.report_errors(profiler=self.profiler)

    def catalog(self) -> None:
        """
        Execute the catalog command.

        Scans DjVu files and stores their metadata in the database.
        """
        self.actions.catalog_and_store(limit=self.args.limit, sample_record_count=1)
        self.actions.report_errors(profiler=self.profiler)

    def convert(self) -> None:
        """
        Execute the convert command.

        Converts DjVu files to PNG format using database records.
        """
        self.actions.convert_from_database(serial=self.args.serial, url=self.args.url)
        self.actions.report_errors(profiler=self.profiler)

    def dbupdate(self) -> None:
        """
        Execute the dbupdate command.

        Updates the DjVu database with metadata from processed files.
        """
        self.actions.update_from_database(
            max_errors=self.args.max_errors, url=self.args.url
        )
        self.actions.report_errors(profiler=self.profiler)

    def initdb(self) -> None:
        """
        Execute the initdb command.

        Initializes the database schema with sample records.
        """
        self.actions.init_database()
        self.actions.report_errors(profiler=self.profiler)


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point for the DjVu command-line tool.

    Args:
        argv: Command-line arguments (defaults to sys.argv if None)

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    exit_code = DjVuCmd.main(argv)
    return exit_code


if __name__ == "__main__":
    main()
