"""
Created on 2026-02-25

Content indexer for MediaWiki image folders

@author: wf
"""

import logging
import os
from pathlib import Path
from typing import List, Optional

from lodstorage.sql import SQLDB, EntityInfo
from ngwidgets.progress import TqdmProgressbar

logger = logging.getLogger(__name__)


class ContentIndexer:
    """
    Handle content index import to SQLite database.

    Imports content files created by scripts/createcontent into a SQLite database
    for efficient querying of MediaWiki image folder contents.
    """

    def __init__(self, db_path: str):
        """
        Initialize database connection and create table if needed.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.sqldb = SQLDB(dbname=db_path, debug=False)
        self.create_table()

    def create_table(self):
        """
        Create files table if it doesn't exist.
        """
        create_table_sql = """CREATE TABLE IF NOT EXISTS files (
            directory TEXT,
            path TEXT,
            filename TEXT,
            ext TEXT,
            size INTEGER,
            mtime TEXT,
            ctime TEXT,
            encoding_issue INTEGER,
            PRIMARY KEY (directory, path, filename)
        )"""
        self.sqldb.execute(create_table_sql)

    def check_encoding_issue(self, text: str) -> int:
        """
        Check if text has potential encoding issues.

        Args:
            text: Text to check for encoding problems

        Returns:
            1 if encoding issue detected, 0 otherwise
        """
        encoding_issue = 0
        try:
            # Check for replacement characters or suspicious bytes
            if "\ufffd" in text:
                encoding_issue = 1
            else:
                # Check for common ISO-8859-1 artifacts in UTF-8
                suspicious = ["Ã", "Ã¤", "Ã¶", "Ã¼", "ÃŸ", "Ã©", "Ã¨"]
                for pattern in suspicious:
                    if pattern in text:
                        encoding_issue = 1
                        break
        except Exception:
            encoding_issue = 1
        return encoding_issue

    def import_lines(
        self,
        lines: List[str],
        directory: str,
    ) -> int:
        """
        Import lines directly from memory into database.

        Args:
            lines: List of pipe-separated file records
            directory: Directory name for the content

        Returns:
            Number of records imported
        """
        records = []
        for line in lines:
            line = line.strip()
            if line.startswith("#") or not line:
                continue

            parts = line.split("|")
            if len(parts) >= 4:
                filepath = parts[0]
                path = os.path.dirname(filepath)
                filename = os.path.basename(filepath)
                name_part, ext = os.path.splitext(filename)
                ext = ext.lstrip(".").lower() if ext else ""
                encoding_issue = self.check_encoding_issue(filepath)

                record = {
                    "directory": directory,
                    "path": path,
                    "filename": filename,
                    "ext": ext,
                    "size": parts[1],
                    "mtime": parts[2],
                    "ctime": parts[3],
                    "encoding_issue": encoding_issue,
                }
                records.append(record)

        if records:
            entity_info = EntityInfo(records, "files", primaryKey=None, quiet=True)
            self.sqldb.store(records, entity_info, replace=True)

        imported_count = len(records)
        return imported_count

    def import_file(
        self,
        content_file: str,
        directory: str,
        progressbar: Optional[TqdmProgressbar] = None,
    ) -> int:
        """
        Import content file into database, skip comment lines.

        Args:
            content_file: Path to content index file
            directory: Directory name for the content
            progressbar: Optional progress bar

        Returns:
            Number of records imported
        """
        content_path = Path(content_file)
        if not content_path.exists():
            logger.warning(f"Content file not found: {content_file}")
            return 0

        with open(content_file, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        imported_count = self.import_lines(lines, directory)
        return imported_count
