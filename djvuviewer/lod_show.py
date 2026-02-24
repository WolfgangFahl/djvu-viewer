"""
Created on 2026-02-22

@author: wf
"""

from typing import Any, Dict, List

from tabulate import tabulate


class LodShow:
    @classmethod
    def show(cls, rows: List[Dict[str, Any]], tablefmt: str = "simple") -> None:
        """
        Print List of Dicts as a tabulate table.

        Args:
             rows: Result rows from run().
             tablefmt: tabulate format string (e.g. simple, grid, github).
        """
        print(tabulate(rows, headers="keys", tablefmt=tablefmt))
