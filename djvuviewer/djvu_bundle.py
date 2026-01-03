"""
Created on 2026-01-03

@author: wf
"""
import subprocess

from djvuviewer.djvu_core import DjVuFile

class DjVuBundle:
    """

    """

    def __init__(self,djvu_file:DjVuFile):
        """
        """
        self.djvu_file=djvu_file


    def render_djvu_page_cli(self,djvu_path, page_num, output_path, dpi=300):
        """
        Use ddjvu command-line tool .

        Install: sudo apt install djvulibre-bin (Linux)
                 brew install djvulibre (Mac)
        """
        cmd = [
            "ddjvu",
            f"-format=png",
            f"-page={page_num + 1}",  # ddjvu uses 1-based indexing
            f"-dpi={dpi}",
            djvu_path,
            output_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"ddjvu failed: {result.stderr}")

        return output_path