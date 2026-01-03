"""
Created on 2025-02-24

@author: wf
"""

import argparse
import glob
import json
import os
import tarfile
from argparse import Namespace

from basemkit.basetest import Basetest

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
        self.local = os.path.exists(DjVuConfig.get_config_file_path())
        # set to True to emulate CI mode
        force_test = False
        if force_test:
            self.local = False
        self.config = DjVuConfig.get_instance(test=force_test)
        self.limit = 50 if not self.local else 50  # 10000000
        self.test_tuples = [
            ("/images/c/c7/AB1938_Kreis-Beckum_Inhaltsverz.djvu", 3, False),
            ("/images/c/ce/Plauen-AB-1938.djvu", 2, True),
            ("/images/f/ff/AB1932-Ramrath.djvu", 2, True),
        ]
        self.test_tuples_2024 = [
            ("/images/2/2f/Sorau-AB-1913.djvu", 255, False),
            ("/images/9/96/vz1890-neuenhausen-zb04.djvu", 3, True),
            ("/images/0/08/Deutsches-Kirchliches-AB-1927.djvu", 1188, False),
            ("/images/a/a1/Treuen-Vogtland-AB-1905.djvu", 38, False),
        ]

    def get_args(self, command: str) -> argparse.Namespace:
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
            quiet=False,
            about=False,
            serial=False,
        )
        return args

    def check_command(
        self, command: str, expected_errors: int = 0, args: Namespace = None
    ):
        """
        check the given command
        """
        if args is None:
            args = self.get_args(
                command=command,
            )
        djvu_cmd = DjVuCmd(args=args)
        djvu_cmd.handle_args(args)
        error_count = len(djvu_cmd.actions.errors)
        if self.debug and error_count > expected_errors:
            print(djvu_cmd.actions.errors)
        self.assertTrue(error_count <= expected_errors)

    def test_config(self):
        """
        test the configuration
        """
        if self.debug:
            print(self.config)
        self.assertEqual(self.local, not self.inPublicCI())

    def test_relpath(self):
        """
        test relpath
        """
        # Test cases: (input, expected_output, description)
        test_cases = [
            # Relative paths that should be transformed
            (
                "./images/c/ce/Plauen-AB-1938.djvu",
                "/c/ce/Plauen-AB-1938.djvu",
                "Relative path with ./images/",
            ),
            (
                "./c/ce/Plauen-AB-1938.djvu",
                "/c/ce/Plauen-AB-1938.djvu",
                "Relative path with ./",
            ),
            # Absolute paths get everything before and including /images/ removed
            (
                "/Users/wf/hd/genwiki_gruff/images/c/ce/Plauen-AB-1938.djvu",
                "/c/ce/Plauen-AB-1938.djvu",
                "Absolute path with /images/ in middle",
            ),
            # Path starting with /images/ should have it removed
            (
                "/images/c/ce/Plauen-AB-1938.djvu",
                "/c/ce/Plauen-AB-1938.djvu",
                "Path starting with /images/",
            ),
            # corner case images/ prefix needs to be handled
            (
                "images/c/ce/Plauen-AB-1938.djvu",
                "/c/ce/Plauen-AB-1938.djvu",
                "Path starting with images/ (no dot or slash)",
            ),
            # duplicate slashes
            (
                "/images//f/ff/AB1932-Ramrath.djvu",
                "/f/ff/AB1932-Ramrath.djvu",
                "duplicate slashes",
            ),
        ]

        for input_path, expected, description in test_cases:
            result = self.config.djvu_relpath(input_path)
            if self.debug:
                print(f"\n{description}")
                print(f"  Input:    {input_path}")
                print(f"  Expected: {expected}")
                print(f"  Got:      {result}")
                print(f"  Match:    {'✓' if result == expected else '✗'}")

            self.assertEqual(
                expected, result, f"Failed for {description}: input='{input_path}'"
            )

    def test_001_init_db(self):
        """
        check init database command first
        """
        self.check_command("initdb")

    def get_djvu(self, relurl, with_download: bool = False):
        """
        get the djvu file for the relative url
        """
        djvu_path = self.config.djvu_abspath(relurl)
        url = self.config.base_url + relurl
        if not self.local and with_download:
            try:
                Download.download(url, djvu_path)
            except Exception as _ex:
                print(f"invalid {djvu_path}")
                return None
        self.assertTrue(os.path.isfile(djvu_path), djvu_path)
        return djvu_path

    def test_djvu_files(self):
        """
        test the djvu file operations
        """
        dproc = DjVuProcessor()
        for relurl, elen, expected_bundled in self.test_tuples:
            djvu_path = self.get_djvu(relurl)
            rel_path = self.config.djvu_relpath(djvu_path)
            if self.debug:
                print(f"getting DjVuFile for {rel_path}")
            djvu_file = dproc.get_djvu_file(djvu_path, config=self.config)
            if self.debug:
                print(djvu_file)
                print(djvu_file.to_yaml())
            self.assertEqual(expected_bundled, djvu_file.bundled)
            self.assertEqual(elen, djvu_file.page_count)

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
                djvu_path, relurl, save_png=False, output_path=self.output_dir
            ):
                count += 1
                self.assertIsNone(image_job.error)
                image = image_job.image

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
            self.assertGreaterEqual(
                elen, count, f"Expected {elen} images but got {count}"
            )

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
        for relurl, _elen, _expected_bundled in self.test_tuples:
            args = self.get_args("dbupdate")
            args.url = relurl
            self.check_command("dbupdate", args=args)

    def test_all_djvu(self):
        """
        test all djvu pages
        """
        expected_errors = 0 if self.local else 49
        self.check_command("catalog", expected_errors)

    def test_convert(self):
        """
        test the conversion
        """
        for relurl, _elen, _expected_bundled in self.test_tuples:
            args = self.get_args("convert")
            args.url = relurl
            args.force = True
            self.check_command("convert", args=args)

            # Verify tar file was created
            base_name = os.path.splitext(os.path.basename(relurl))[0]
            tar_file = os.path.join(self.output_dir, f"{base_name}.tar")

            self.assertTrue(
                os.path.isfile(tar_file),
                f"Expected tar file '{tar_file}' was not created",
            )

            # Verify tar contains PNG files
            with tarfile.open(tar_file) as tar:
                members = tar.getmembers()
                self.assertGreater(len(members), 0, f"Tar file '{tar_file}' is empty")
                png_files = [
                    m.name for m in tar.getmembers() if m.name.endswith(".png")
                ]
                self.assertGreater(len(png_files), 0, "No PNG files found in tar")

                # Check for YAML file
                yaml_files = [m for m in members if m.name.endswith(".yaml")]
                self.assertEqual(
                    len(yaml_files),
                    1,
                    f"Expected 1 YAML file in tar, found {len(yaml_files)}",
                )

    def test_issue49(self):
        """
        Test loading DjVu file with python-djvu and storing relevant metadata.
        """
        for url, page_count in [
            ("/images/9/96/vz1890-neuenhausen-zb04.djvu", 3),
            # ("f/fc/Siegkreis-AB-1905-06_Honnef.djvu", 35),
            # ("./images/9/96/Elberfeld-AB-1896-97-Stadtplan.djvu", 1),
            # ("./images/0/08/Deutsches-Kirchliches-AB-1927.djvu", 1188),
        ]:
            with self.subTest(url=url, expected_pages=page_count):
                if not self.local and page_count > 1:
                    return
                relurl = url
                djvu_path = self.get_djvu(relurl)
                dproc = DjVuProcessor(tar=False, debug=self.debug, verbose=self.debug)
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
                    f"Expected {page_count} PNG files matching pattern '{pattern}', but found {len(png_files)}",
                )

    def testDjVuManager(self):
        """
        test the DjVu Manager
        """
        dvm = DjVuManager(config=self.config)
        lod = dvm.query("total")
        if self.debug:
            print(json.dumps(lod, indent=2))
        if self.local:
            self.assertEqual(lod, [{"files": 4288, "pages": 1006670}])
        else:
            self.assertEqual(lod, [{"files": 1, "pages": 4}])
