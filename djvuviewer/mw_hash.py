"""
Created on 2026-02-26

@author: wf
"""

import hashlib
from dataclasses import dataclass


@dataclass
class MediaWikiHash:
    """
    support MediaWiki file hash encoding
    """

    hash_value: str  # e.g. c7

    @property
    def path(self) -> str:
        """
        Get the MediaWiki hash-based path (e.g. "c/c7").

        Returns:
            Hash-based path used in MediaWiki file structure
        """
        path = f"{self.hash_value[0]}/{self.hash_value}"
        return path

    @property
    def value(self) -> int:
        """
        Get the integer value of the first byte of the hash.

        Returns:
            Integer representation of the first hash value (0–255)
        """
        value = int(self.hash_value, 16)
        return value

    @classmethod
    def of_value(cls, value: int) -> "MediaWikiHash":
        """
        Create MediaWikiHash from an integer value (0–255).

        Args:
            value: Integer value of the first hash byte

        Returns:
            MediaWikiHash instance
        """
        if value < 0 or value > 255:
            raise ValueError(f"value {value} out of range 0-255")
        hash_value = format(value, "02x")
        mw_hash = cls(hash_value=hash_value)
        return mw_hash

    @classmethod
    def of_filename(cls, filename: str) -> "MediaWikiHash":
        """
        Create MediaWikiHash from a filename using MD5 hash

        Args:
            filename: Filename to hash (e.g. "File.djvu")

        Returns:
            MediaWikiHash instance
        """
        md5_hex = hashlib.md5(filename.encode("utf-8")).hexdigest()
        mw_hash = cls(hash_value=md5_hex[0:2])
        return mw_hash
