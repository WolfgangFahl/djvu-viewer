"""
Created on 2026-02-22

@author: wf
"""
from tabulate import tabulate
from typing import Any,List,Dict


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