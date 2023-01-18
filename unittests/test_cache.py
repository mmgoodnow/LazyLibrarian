#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in cache.py
#   TODO: Test more functions; for now, it is just clean_cache that is tested.

import unittest
from typing import List
import itertools
import logging
import os
import random
import time
from lazylibrarian import cache
from lazylibrarian.database import DBConnection
from lazylibrarian.dbupgrade import db_upgrade, upgrade_needed
from lazylibrarian.filesystem import DIRS, remove_dir, remove_file
from unittests.unittesthelpers import LLTestCaseWithConfigandDIRS


class TestCache(LLTestCaseWithConfigandDIRS):
    def setUp(self):
        # Create a test directory and test database
        super().setUp()
        self.logger.setLevel(logging.ERROR)
        self.testdir = DIRS.get_cachedir('test')
        remove_dir(self.testdir, remove_contents=True)
        DIRS.ensure_dir_is_writeable(self.testdir)

        # Create a new, empty database for testing
        DIRS.DBFILENAME = "test-db.db"
        try:  # Make sure the file doesn't exist
            remove_file(DIRS.get_dbfile())
        except FileNotFoundError:
            pass  # Ignore
        curr_ver = upgrade_needed()
        db_upgrade(curr_ver, restartjobs=False)
        self.logger.setLevel(logging.INFO)

    def tearDown(self) -> None:
        """ Delete the test directory and database after each test """
        super().tearDown()
        remove_dir(self.testdir, remove_contents=True)
        remove_file(DIRS.get_dbfile())

    @unittest.SkipTest
    def test_fetch_url(self):
        assert False

    @unittest.SkipTest
    def test_cache_img(self):
        assert False

    @unittest.SkipTest
    def test_gr_xml_request(self):
        assert False

    @unittest.SkipTest
    def test_json_request(self):
        assert False

    @unittest.SkipTest
    def test_html_request(self):
        assert False

    @unittest.SkipTest
    def test_get_cached_request(self):
        assert False

    def test_clean_cache(self):
        results = cache.clean_cache()
        self.assertEqual(12, len(results), 'Expected 12 cleaning results')
        # No need to test with actual data as the detailed unit tests below cover those cases

    def test_cache_cleaner(self):
        # CacheCleaner is an abstract class
        with self.assertRaises(TypeError):
            _ = cache.CacheCleaner()

    def test_file_cleaner(self):
        with self.assertRaises(TypeError):
            _ = cache.FileCleaner()

    def create_hex_dirs(self):
        subdirs = itertools.product("0123456789abcdef", repeat=2)
        for i, j in subdirs:
            dirname = os.path.join(self.testdir, i, j)
            DIRS.ensure_dir_is_writeable(dirname)

    def create_test_files(self, basename: str, num: int, age: int = 0, hexdirs: bool = False) -> List[str]:
        files = []
        hexstr = "0123456789abcdef"
        for i in range(num):
            if hexdirs:
                part1 = hexstr[random.randint(0, len(hexstr)-1)]
                part2 = hexstr[random.randint(0, len(hexstr)-1)]
                filename = os.path.join(part1, part2, basename % i)
            else:
                filename = basename % i
            files.append(filename)
            fullname = os.path.join(self.testdir, filename)
            with open(fullname, 'w') as f:
                f.write('Hello')
                f.close()
            if age:
                # Now set its modified/access time for testing
                timestamp = time.time() - age
                os.utime(fullname, (timestamp, timestamp))
        return files

    def test_file_expirer(self):
        # Empty a directory that doesn't exist
        fe = cache.FileExpirer('12345', False, 3600)
        msg = fe.clean()
        self.assertEqual(f'Cleaned 0 expired files from 12345, kept 0', msg)

        # Empty a directory with nothing in it
        fe = cache.FileExpirer('test', False, 3600)
        msg = fe.clean()
        self.assertEqual(f'Cleaned 0 expired files from test, kept 0', msg)

        # Create 5 files to delete, and 3 to keep
        self.create_test_files('keepers_%s.jpg', 3, 100, False)
        self.create_test_files('oldies_%s.jpg', 5, 3600, False)
        fe = cache.FileExpirer('test', False, 1000)
        msg = fe.clean()
        self.assertEqual(f'Cleaned 5 expired files from test, kept 3', msg)

        # Delete the keepers too, by decreasing the age threshold
        fe = cache.FileExpirer('test', False, 1)
        msg = fe.clean()
        self.assertEqual(f'Cleaned 3 expired files from test, kept 0', msg)

        # Create some files in a complex set of nested '0..f' directories
        self.create_hex_dirs()
        self.create_test_files('keepers_%s.jpg', 30, 10, True)
        self.create_test_files('oldies_%s.jpg', 50, 3600, True)
        # Deleting them with a normal exirer does nothing - files are in subdirs
        fe = cache.FileExpirer('test', False, 1000)
        msg = fe.clean()
        self.assertEqual(f'Cleaned 0 expired files from test, kept 0', msg)
        # Deleting them with a hexdir will work:
        fe = cache.FileExpirer('test', True, 1000)
        msg = fe.clean()
        self.assertEqual(f'Cleaned 50 expired files from test, kept 30', msg)

    def test_extension_cleaner(self):
        # Empty a directory with nothing in it
        fe = cache.ExtensionCleaner('test', 'doc')
        msg = fe.clean()
        self.assertEqual(f'Cleaned 0 superfluous files from test, kept 0', msg)

        # Create some files to keep, and some to delete
        self.create_test_files('keepers_%s.png', 3)
        self.create_test_files('goners_%s.doc', 5)
        fe = cache.ExtensionCleaner('test', 'doc')
        msg = fe.clean()
        self.assertEqual(f'Cleaned 5 superfluous files from test, kept 3', msg)

    def test_orphan_cleaner(self):
        # Empty database and no files: Expect nothing needs cleaning
        db = DBConnection()
        oc = cache.OrphanCleaner('test', False, db, 'BookID', 'books', '%s', True)
        msg = oc.clean()
        self.assertEqual(f'Cleaned 0 orphan files from test, kept 0', msg)

        # Create some files; with an empty DB, they will all be deleted as orphans
        self.create_test_files('MyID%s.png', 3)
        oc = cache.OrphanCleaner('test', False, db, 'BookID', 'books', '%s', True)
        msg = oc.clean()
        self.assertEqual(f'Cleaned 3 orphan files from test, kept 0', msg)

        # Create some files and mention 2 of them in the DB
        self.create_test_files('MyID%s.png', 3)
        db.action('INSERT into books (BookID) VALUES (?)', ('MyID1',))
        db.action('INSERT into books (BookID) VALUES (?)', ('MyID2',))
        oc = cache.OrphanCleaner('test', False, db, 'BookID', 'books', '%s', True)
        msg = oc.clean()
        self.assertEqual(f'Cleaned 1 orphan file from test, kept 2', msg)

        # Create some files in hexdirs and mention all but one of them in the DB
        self.create_hex_dirs()
        files = self.create_test_files('%s_book.jpg', 10, hexdirs=True)
        for inx, file in enumerate(files):
            if inx != 5:
                db.action('INSERT into books (BookID,BookImg) VALUES (?,?)', (f'{inx}.jpg', file,))
        oc = cache.OrphanCleaner('test', True, db, 'BookID', 'books', '%s', False)
        msg = oc.clean()
        self.assertEqual(f'Cleaned 1 orphan file from test, kept 9', msg)
        db.close()

    def test_unreferenced_cleaner(self):
        db = DBConnection()
        uc = cache.UnreferencedCleaner('test', 'testing', db, 'BookImg', 11, 'books where BookImg like "test/%"')
        msg = uc.clean()
        self.assertEqual(f'Cleaned 0 orphan files from testing, kept 0', msg)

        # Create some files, they'll all be deleted as they are unreferenced
        self.create_test_files('test%s.jpg', 10)
        uc = cache.UnreferencedCleaner('test', 'testing', db, 'BookImg', 11, 'books where BookImg like "test/%"')
        msg = uc.clean()
        self.assertEqual(f'Cleaned 10 orphan files from testing, kept 0', msg)

        # Create some files, reference all but one, then clean
        files = self.create_test_files('test%s.jpg', 15)
        for inx, file in enumerate(files):
            if inx != 5:
                db.action('INSERT into books (BookID,BookImg) VALUES (?,?)', (f'{inx}.jpg', f'test/{file}',))
        uc = cache.UnreferencedCleaner('test', 'testing', db, 'BookImg', 5, 'books where BookImg like "test/%"')
        msg = uc.clean()
        self.assertEqual(f'Cleaned 1 orphan file from testing, kept 14', msg)
        db.close()

    def test_dbcleaner(self):
        # First try with an empty database; nothing will be changed
        db = DBConnection()
        try:
            dc = cache.DBCleaner("book", "Cover", db, "books", "BookImg", "BookName", "BookID", "images/nocover.png")
            msg = dc.clean()
            self.assertEqual(f'Cleaned 0 missing Covers, kept 0', msg)

            # Now add some items to the database, there the covers don't exist
            db.action('INSERT into books (BookID) VALUES (?)', ('MyID1',))
            db.action('INSERT into books (BookID) VALUES (?)', ('MyID2',))
            db.action('INSERT into books (BookID,BookImg) VALUES (?,?)', (f'12345', f'test/somefile.jpg',))
            # And some that are ok
            files = self.create_test_files('covers%s.jpg', 5)
            for inx, file in enumerate(files):
                db.action('INSERT into books (BookID,BookImg) VALUES (?,?)', (f'ok{inx}', f'test/{file}',))
            dc = cache.DBCleaner("test", "Cover", db, "books", "BookImg", "BookName", "BookID", "images/nocover.png")
            msg = dc.clean()
            self.assertEqual(f'Cleaned 3 missing Covers, kept 5', msg)

            # Check that the database was updated correctly
            res = db.select('SELECT BookImg,BookID from books')
            for item in res:
                self.assertIsNotNone(item['BookImg'], 'Expect all items to have a BookImg field')
                bookid = item['BookID']
                img = item['BookImg']
                if bookid.startswith('ok'):
                    self.assertNotEqual('images/nocover.png', img)
                else:
                    self.assertEqual('images/nocover.png', img)
        finally:
            db.close()

