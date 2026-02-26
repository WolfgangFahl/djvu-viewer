"""
Created on 2026-02-26

@author: wf
"""
from dataclasses import dataclass
import hashlib


@dataclass
class MediaWikiHash:
    """
    support MediaWiki file hash encoding
    """

    hash: str

    @property
    def path(self) -> str:
        """
        Get the MediaWiki hash-based path (e.g. "0/00" from hash "00...")

        Returns:
            Hash-based path used in MediaWiki file structure
        """
        path = f"{self.hash[0]}/{self.hash[0:2]}"
        return path

    @property
    def value(self) -> int:
        """
        Get the integer value of the hash

        Returns:
            Integer representation of the hash
        """
        value = int(self.hash, 16)
        return value

    @classmethod
    def of_value(cls, value: int) -> "MediaWikiHash":
        """
        Create MediaWikiHash from an integer value

        Args:
            value: Integer value to convert to hash

        Returns:
            MediaWikiHash instance
        """
        if value<0 or value>256:
            raise
        hash_str = format(value, "x")
        mw_hash = cls(hash=hash_str)
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
        md5_hash = hashlib.md5(filename.encode("utf-8")).hexdigest()
        mw_hash = cls(hash=md5_hash)
        return mw_hash

