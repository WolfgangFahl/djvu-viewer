"""
Created on 04.01.2026

@author: wf
"""
from argparse import Namespace

from basemkit.profiler import Profiler
from djvuviewer.djvu_actions import DjVuActions
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_processor import DjVuProcessor


class DjVuContext:
    """
    a Context for working with DjVu files and actions
    """
    def __init__(self,config:DjVuConfig,args:Namespace):
        self.config=config
        self.args=args
        # Initialize manager and processor
        self.dvm = DjVuManager(config=self.config)
        self.dproc = DjVuProcessor(
            debug=self.args.debug,
            verbose=self.args.verbose,
            batch_size=self.args.batch_size,
            limit_gb=self.args.limit_gb,
            max_workers=self.args.max_workers,
            pngmode=self.args.pngmode,
        )

        # Initialize actions handler
        self.actions = DjVuActions(
            config=self.config,
            args=self.args,
            dvm=self.dvm,
            dproc=self.dproc,
            images_path=self.args.images_path,
            output_path=self.args.output_path,
            debug=self.args.debug,
            verbose=self.args.verbose,
            force=self.args.force,
        )

        self.profiler = Profiler(self.args.command)
