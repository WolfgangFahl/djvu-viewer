"""
Created on 2025-02-24

@author: wf
"""

import argparse
import glob
import json
import os

from basemkit.basetest import Basetest
import djvu.decode
from djvuviewer.djvu_cmd import DjVuCmd
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuImage
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_processor import DjVuProcessor
from djvuviewer.download import Download


class TestDjVu(Basetest):
    """
    Test djvu handling
    """

    def setUp(self, debug=True, profile=True):
        """
        setUp test environment
        """
        Basetest.setUp(self, debug=debug, profile=profile)
        # Define base directory
        base_dir = os.path.expanduser("/tmp/djvu")

        # Set up subdirectories
        self.output_dir = os.path.join(base_dir, "test_pngs")
        self.db_path = os.path.join(base_dir, "test_db", "genwiki_images.db")

        # Create all necessary directories
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.local =os.path.exists(DjVuConfig.get_config_file_path())
        self.config=DjVuConfig.get_instance()
        self.limit=50 if not self.local else 10000000;
        self.test_tuples= [
            ("c/ce/Plauen-AB-1938.djvu",2,True),
            ("b/b3/AB1951-Suenninghausen.djvu",5,False)
        ]
        self.test_tuples_2024= [
            ("2/2f/Sorau-AB-1913.djvu", 255,False),
            ("9/96/vz1890-neuenhausen-zb04.djvu", 3,True),
            ("0/08/Deutsches-Kirchliches-AB-1927.djvu", 1188,False),
            ("a/a1/Treuen-Vogtland-AB-1905.djvu",38,False)
        ]

    def get_args(self,command:str)->argparse.Namespace:
        """
        get CLI arguments for testing
        """
        args = argparse.Namespace(
            command=command,
            db_path=self.db_path,
            images_path=self.config.images_path,
            limit=self.limit,
            url=None,
            sort="asc",
            force=False,
            output_path=self.output_dir,
            parallel=False,
            batch_size=100,
            limit_gb=16,
            max_errors=1,
            max_workers=None,
            debug=self.debug,
            verbose=False,
            serial=False,
        )
        return args

    def check_command(self,command:str,expected_errors:int=0):
        """
        check the given command
        """
        args =self.get_args(
            command=command,
        )
        djvu_cmd = DjVuCmd(args=args)
        djvu_cmd.handle_args()
        error_count=len(djvu_cmd.actions.errors)
        if self.debug and error_count>expected_errors:
            print(djvu_cmd.actions.errors)
        self.assertTrue(error_count <= expected_errors)

    def test_config(self):
        """
        test the configuration
        """
        if self.debug:
            print(self.config)
        self.assertEqual(self.local, not self.inPublicCI())

    def test_001_init_db(self):
        """
        check init database command first
        """
        self.check_command("initdb")


    def get_djvu(self, relurl, with_download:bool=False):
        """
        get the djvu file for the relative url
        """
        djvu_path = f"{self.config.images_path}/{relurl}"
        url = self.config.base_url + relurl
        if not self.local and with_download:
            try:
                Download.download(url, djvu_path)
            except Exception as _ex:
                print(f"invalid {djvu_path}")
                return None
        self.assertTrue(os.path.isfile(djvu_path), djvu_path)
        return djvu_path

    def test_djvu_processor(self):
        """
        test the DjVu processor
        """
        for relurl, elen,expected_bundled in self.test_tuples:
            djvu_path = self.get_djvu(relurl)
            url = djvu.decode.FileURI(djvu_path)
            # url=f"{baseurl}/{relurl}"
            dproc = DjVuProcessor()
            if self.debug:
                print(f"processing {url}")
            document = dproc.context.new_document(url)
            document.decoding_job.wait()
            if self.debug:
                page_count = len(document.files)
                print(f"{page_count} pages")
            self.assertEqual(elen, page_count)
            bundled=document.type==2
            self.assertEqual(expected_bundled,bundled,url)
        pass

    def test_djvu_images(self):
        """
        test the DjVu image generation
        """
        for relurl, elen, expected_bundled in self.test_tuples:
            djvu_path = self.get_djvu(relurl)
            dproc = DjVuProcessor(verbose=self.debug, debug=self.debug)

            if self.debug:
                print(f"processing images for {relurl}")

            count = 0
            # iterate over the generator
            for image_job in dproc.process(
                djvu_path, relurl, save_png=False,output_path=self.output_dir
            ):
                count += 1
                image=image_job.image

                # Check ImageJob integrity
                self.assertIsNotNone(image, f"Image should be present for page {count}")
                self.assertIsInstance(image, DjVuImage)

                # Check DjVuImage properties
                self.assertIsNotNone(image._buffer, "Image buffer should not be None")
                self.assertGreater(image.width, 0, "Width should be positive")
                self.assertGreater(image.height, 0, "Height should be positive")
                self.assertGreaterEqual(image.page_index, count, "Page index mismatch")
                if self.debug:
                    print(image.to_yaml())
            self.assertGreaterEqual(elen, count, f"Expected {elen} images but got {count}")


    def test_queries(self):
        """
        test all queries
        """
        query_params = {
            "all_pages": {"limit": 50},
            "pages_of_djvu": {"djvu_path": "/images/a/a1/Treuen-Vogtland-AB-1905.djvu"},
        }
        djvm = DjVuManager(config=self.config)
        djvm.sql_db.debug = self.debug
        # Get all available queries from the MultiLanguageQueryManager
        for query_name in djvm.mlqm.query_names:
            if self.debug:
                print(query_name)
            param_dict = query_params.get(query_name, {})
            if param_dict:
                pass
            lod = djvm.query(query_name, param_dict=param_dict)
            if self.debug:
                print(f"{len(lod)} records")

    def test_update_database(self):
        """
        test updating the database
        """
        self.check_command("dbupdate")

    def test_all_djvu(self):
        """
        test all djvu pages
        """
        expected_errors = 0 if self.local else 2
        self.check_command("catalog",expected_errors)

    def test_convert(self):
        """
        test the conversion
        """
        args = argparse.Namespace(
            command="convert",
            db_path=self.config.db_path,
            images_path=self.config.images_path,
            limit=50,
            force=True,
            sort="asc",
            output_path=self.output_dir,
            parallel=True,
            # url="/images/2/2f/Sorau-AB-1913.djvu",
            url="/9/96/vz1890-neuenhausen-zb04.djvu",
            debug=True,
            serial=False,
            batch_size=100,
            limit_gb=16,
            max_workers=None,
            verbose=True,
        )
        djvu_cmd = DjVuCmd(args=args)
        djvu_cmd.handle_args()

    def test_issue49(self):
        """
        Test loading DjVu file with python-djvu and storing relevant metadata.
        """
        for url, page_count in [
            ("9/96/vz1890-neuenhausen-zb04.djvu", 3),
            # ("f/fc/Siegkreis-AB-1905-06_Honnef.djvu", 35),
            # ("./images/9/96/Elberfeld-AB-1896-97-Stadtplan.djvu", 1),
            # ("./images/0/08/Deutsches-Kirchliches-AB-1927.djvu", 1188),
        ]:
            with self.subTest(url=url, expected_pages=page_count):
                if not self.local and page_count > 1:
                    return
                relurl=url
                djvu_path = self.get_djvu(relurl)
                dproc = DjVuProcessor(tar=False)
                if self.debug:
                    print(f"processing {relurl}")
                # for document, page in dproc.yield_pages(djvu_path):
                #    pass
                count = 0
                for _image_job in dproc.process_parallel(
                    djvu_path, relurl=relurl, save_png=True, output_path=self.output_dir
                ):
                    count += 1

                if self.debug:
                    print(f"Processed {count} pages in {self.output_dir}")
                    base_name = os.path.splitext(os.path.basename(relurl))[0]
                pattern = os.path.join(self.output_dir, f"{base_name}_page_*.png")
                png_files = glob.glob(pattern)

                self.assertEqual(
                    len(png_files),
                    page_count,
                    f"Expected {page_count} PNG files matching pattern '{pattern}', but found {len(png_files)}"
                )


    def testDjVuManager(self):
        """
        test the DjVu Manager
        """
        dvm = DjVuManager(config=self.config)
        lod = dvm.query("total")
        if self.debug:
            print(json.dumps(lod, indent=2))
        self.assertEqual(lod, [{"files": 1, "pages": 4}])
        #self.assertEqual(lod, [{"files": 4288, "pages": 1028225}])
