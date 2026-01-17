"""
Created on 2025-02-24

@author: wf
"""

import argparse
import glob
import json
import os
import shutil
from argparse import Namespace
from typing import List, Optional

from basemkit.basetest import Basetest

from djvuviewer.djvu_bundle import DjVuBundle
from djvuviewer.djvu_cmd import DjVuCmd
from djvuviewer.djvu_config import DjVuConfig
from djvuviewer.djvu_core import DjVuFile, DjVuImage
from djvuviewer.djvu_image_job import ImageJob
from djvuviewer.djvu_manager import DjVuManager
from djvuviewer.djvu_processor import DjVuProcessor
from djvuviewer.download import Download
from djvuviewer.packager import PackageMode
from djvuviewer.wiki_images import MediaWikiImage


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
        self.output_dir = os.path.join(base_dir, "test_archive")
        self.db_path = os.path.join(base_dir, "test_db", "genwiki_images.db")
        self.backup_path = os.path.join(base_dir, "backup")
        # Create all necessary directories
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(self.backup_path, exist_ok=True)
        self.local = os.path.exists(DjVuConfig.get_config_file_path())
        # set to True to emulate CI mode to create a fresh djvu_data.db
        force_test = False
        if force_test:
            self.local = False
        self.config = DjVuConfig.get_instance(test=force_test)
        self.config.backup_path = self.backup_path
        self.limit = 50 if not self.local else 50  # 10000000
        self.test_tuples = [
            ("/images/c/c7/AB1938_Kreis-Beckum_Inhaltsverz.djvu", 3, False),
            ("/images/c/ce/Plauen-AB-1938.djvu", 2, True),
            ("/images/f/ff/AB1932-Ramrath.djvu", 2, True),
            ("/images/1/1e/AB1953-Gohr.djvu", 2, True),
        ]
        self.test_bundles = None
        self.test_tuples_2024 = [
            ("/images/2/2f/Sorau-AB-1913.djvu", 255, False),
            ("/images/9/96/vz1890-neuenhausen-zb04.djvu", 3, True),
            ("/images/0/08/Deutsches-Kirchliches-AB-1927.djvu", 1188, False),
            ("/images/a/a1/Treuen-Vogtland-AB-1905.djvu", 38, False),
        ]
        self.dproc = DjVuProcessor()

    def get_djvu_test_bundles(self) -> List[DjVuFile]:
        """
        get the djvu test bundles by derived from unbundled/indexed DjVu test tuples
        """
        if self.test_bundles is None:
            self.test_bundles = []

            for relurl, _elen, expected_bundled in self.test_tuples:
                if not expected_bundled:
                    full_path = self.get_djvu(relurl)
                    rel_path = MediaWikiImage.relpath_of_url(full_path)
                    if self.debug:
                        print(f"getting DjVuFile for {rel_path}")
                    djvu_file = self.dproc.get_djvu_file(
                        url=full_path, config=self.config
                    )
                    djvu_bundle = DjVuBundle(djvu_file, config=self.config)
                    self.test_bundles.append(djvu_bundle)
        return self.test_bundles

    def get_args(self, command: str) -> argparse.Namespace:
        """
        get CLI arguments for testing
        """
        args = argparse.Namespace(
            command=command,
            db_path=self.db_path,
            images_path=self.config.images_path,
            backup_path=self.backup_path,
            container_name="genwiki39-mw",
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
            pngmode="pil",
            package_mode="zip",
            verbose=self.debug,
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
        self.assertLessEqual(error_count, expected_errors)

    def test_djvu_dump(self):
        """
        test djvu_dump
        """
        for djvu_bundle in self.get_djvu_test_bundles():
            if self.debug:
                print(djvu_bundle.djvu_file.to_yaml())
            log = djvu_bundle.djvu_dump()
            if self.debug:
                print(log)
            part_filenames = djvu_bundle.get_part_filenames()
            if self.debug:
                for i, filename in enumerate(part_filenames):
                    print(f"{i}:{filename}")

    def show_fileinfo(self, path: str) -> int:
        """
        show info for a file
        """
        iso_date, filesize = ImageJob.get_fileinfo(path)
        if self.debug:
            print(f"{path} ({filesize}) {iso_date}")
        return filesize

    def test_bundle(self):
        """
        Test zipping and bundling with size ratio validation.
        """
        for djvu_bundle in self.get_djvu_test_bundles():
            if os.path.exists(djvu_bundle.bundled_file_path):
                os.remove(djvu_bundle.bundled_file_path)
            # Create backup zip
            zip_path = djvu_bundle.create_backup_zip()
            zip_filesize = self.show_fileinfo(zip_path)

            # Convert to bundled format
            djvu_bundle.convert_to_bundled()
            bundled_path = djvu_bundle.bundled_file_path
            bundle_filesize = self.show_fileinfo(bundled_path)

            # Calculate and validate size ratio
            if zip_filesize > 0:
                size_ratio = zip_filesize / bundle_filesize

                # Assert bundled file is slightly smaller than zip
                assert 1.0 < size_ratio < 1.2, (
                    f"Unexpected size ratio: {size_ratio:.2f}. "
                    f"Bundle: {bundle_filesize} bytes, "
                    f"Zip: {zip_filesize} bytes. "
                    f"Files: {zip_path}, {bundled_path}"
                )
            else:
                self.fail(f"Zero-size zip file: {zip_path}")

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
            result = MediaWikiImage.relpath_of_url(input_path)
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
        full_path = self.config.full_path(relurl)
        url = self.config.base_url + relurl
        if not self.local and with_download:
            try:
                Download.download(url, full_path)
            except Exception as _ex:
                print(f"invalid {full_path}")
                return None
        self.assertTrue(os.path.isfile(full_path), full_path)
        return full_path

    def test_djvu_files(self):
        """
        test the djvu file operations
        """

        for relurl, elen, expected_bundled in self.test_tuples:
            djvu_path = self.get_djvu(relurl)
            rel_path = MediaWikiImage.relpath_of_url(djvu_path)
            if self.debug:
                print(f"getting DjVuFile for {rel_path}")
            djvu_file = self.dproc.get_djvu_file(url=djvu_path, config=self.config)
            if self.debug:
                print(djvu_file)
                print(djvu_file.to_yaml())
            self.assertEqual(expected_bundled, djvu_file.bundled)
            self.assertEqual(elen, djvu_file.page_count)

    def test_djvu_images(self):
        """
        test the DjVu image generation
        """
        for relurl, elen, _expected_bundled in self.test_tuples:
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
            "all_pages_for_path": {
                "djvu_path": "/images/1/1e/AB1953-Gohr.djvu",
                "limit": 50,
            },
            "all_djvu": {"limit": 50},
            "djvu_for_path": {"path": "/images/1/1e/AB1953-Gohr.djvu"},
            "pages_of_djvu": {"djvu_path": "/images/f/ff/AB1932-Ramrath.djvu "},
        }
        djvm = DjVuManager(config=self.config)
        djvm.sql_db.debug = self.debug
        # Get all available queries from the MultiLanguageQueryManager
        for query_name in djvm.mlqm.query_names:
            if self.debug:
                print(query_name)
            param_dict = query_params.get(query_name, {})
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
        expected_errors = 1 if self.local else 49
        self.check_command("catalog", expected_errors)

    def check_package(self, package_file: str, relurl: Optional[str] = None):
        """
        Test helper: Verify package and assert validity.

        Args:
            package_file: Path to the tar file to validate
            relurl: Optional relative URL for error context
        """
        bundle = DjVuBundle.from_package(package_file, with_check=False)

        # If relurl provided, re-check with context (bundle.check_package is idempotent)
        if relurl:
            bundle.check_package(package_file, relurl=relurl)

        self.assertTrue(
            bundle.error_count == 0,
            f"package validation failed for {relurl or package_file}:\n{bundle.get_error_summary()}",
        )

    def test_convert(self):
        """
        Test the conversion with different PNG and package modes.
        """
        png_modes = ["pil", "cli"]
        package_modes = ["tar", "zip"]

        for relurl, _elen, _expected_bundled in self.test_tuples:
            base_name = os.path.splitext(os.path.basename(relurl))[0]
            for package_mode in package_modes:
                for pngmode in png_modes:
                    with self.subTest(
                        relurl=relurl, pngmode=pngmode, package_mode=package_mode
                    ):
                        args = self.get_args("convert")
                        args.url = relurl
                        args.force = True
                        args.pngmode = pngmode
                        args.package_mode = package_mode
                        self.config.package_mode = package_mode
                        self.check_command("convert", args=args)

                        # Verify tar file was created and contains expected content
                        package_file = os.path.join(
                            self.output_dir, f"{base_name}.{package_mode}"
                        )
                        self.check_package(package_file, relurl)

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
                dproc = DjVuProcessor(
                    package_mode=PackageMode.ZIP,
                    debug=self.debug,
                    verbose=self.debug,
                    clean_temp=False,
                )
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
                pattern = os.path.join(dproc.output_path, f"{base_name}_page_*.png")
                png_files = glob.glob(pattern)

                self.assertEqual(
                    len(png_files),
                    page_count,
                    f"Expected {page_count} PNG files matching pattern '{pattern}', but found {len(png_files)}",
                )
                shutil.rmtree(dproc.temp_dir)

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
            self.assertEqual(lod, [{"files": 5, "pages": 11}])
