#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in cache.py
#   TODO: Test more functions; for now, it is just clean_cache that is tested.

import unittest
from typing import List
import itertools
import logging
import mock
import os
import random
import requests
import time

from lazylibrarian import cache
from lazylibrarian.blockhandler import BLOCKHANDLER
from lazylibrarian.cache import ImageType
from lazylibrarian.config2 import CONFIG
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

    def test_fetch_url_no_mock(self):
        """ Test fetch_url without mocking the actual request call """
        # Run all-blank parameters
        msg, res = cache.fetch_url('', {}, False, False)
        self.assertFalse(res, 'Expect blank URL to fail')

        # Block GoogleAPIs, then test
        BLOCKHANDLER.add_provider_entry('googleapis', 100, 'testing')
        msg, res = cache.fetch_url('googleapis', {}, False, False)
        self.assertFalse(res, 'Expect blank URL to fail')
        self.assertEqual(msg, 'Blocked')

    # This method will be used by the mock to replace requests.get
    def mocked_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, content, status_code):
                self.content = content
                self.text = 'Text: ' + content
                self.status_code = status_code

            def json(self):
                if self.content == 'Limit Error':
                    return {
                        'content': self.content,
                        'error': {'message': 'Limit Exceeded'}}
                else:
                    return {
                        'content': self.content}

        if args[0] == 'https://someurl.com/test1':
            return MockResponse("Good stuff", 200)
        elif args[0] == 'http://someourl.com/test403-1':
            return MockResponse("Error in request", 403)
        elif args[0] == 'http://someourl.com/test403-2':
            return MockResponse("Limit Error", 403)
        elif args[0] == 'http://someourl.com/test99999':
            return MockResponse("Invalid error", 99999)
        elif args[0] == 'http://someourl.com/test-redirects':
            raise requests.exceptions.TooManyRedirects('Test redirect error')
        elif args[0] == 'http://someourl.com/test-timeout':
            raise requests.exceptions.Timeout('Test timeout error')
        elif str(args[0]).startswith('http://someourl.com/torznab/'):
            return MockResponse(f"torznab {kwargs['params']['timeout']}", 200)

        return MockResponse('', 404)

    @mock.patch.object(requests, 'get', side_effect=mocked_requests_get)
    def test_fetch_url_with_mock(self, mock_get):
        """ Test fetch_url, mocking requests.get """
        # Set up test conditions
        BLOCKHANDLER.clear_all()
        timeout = 30
        ext_timeout = 100
        agent = {'User-Agent': 'test'}
        CONFIG.set_int('HTTP_EXT_TIMEOUT', ext_timeout)
        CONFIG.set_int('HTTP_TIMEOUT', timeout)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Test "good" return value with SSL_VERIFY and no raw
        CONFIG.set_bool('SSL_VERIFY', True)
        url = 'https://someurl.com/test1'
        msg, res = cache.fetch_url(url, agent, False, raw=False)
        self.assertEqual(msg, 'Text: Good stuff')
        self.assertTrue(res, 'Expected success')

        # Same, but ask for raw data
        msg, res = cache.fetch_url(url, agent, False, raw=True)
        self.assertEqual(msg, 'Good stuff')
        self.assertTrue(res, 'Expected success')

        # Test error code 403, normal, but blocks googleapis
        logger.setLevel(logging.DEBUG)
        url = 'http://someourl.com/test403-1'
        with self.assertLogs(logger, logging.DEBUG) as logmsg:
            msg, res = cache.fetch_url(url, agent, False, raw=True)
        self.assertFalse(res, 'Expected failure 403')
        self.assertEqual(msg, 'Response status 403: Forbidden')
        self.assertTrue(BLOCKHANDLER.is_blocked('googleapis'), 'Expected blockage')
        self.assertListEqual(logmsg.output, [
            'DEBUG:lazylibrarian.cache:Request denied, blocking googleapis for 3600 seconds: Error 403: see debug log'
        ])
        BLOCKHANDLER.clear_all()

        # Test error code 403, 'Limit Exceeded'
        url = 'http://someourl.com/test403-2'
        with self.assertLogs(logger, logging.DEBUG) as logmsg:
            msg, res = cache.fetch_url(url, agent, False, raw=True)
        self.assertFalse(res, 'Expected failure 403')
        self.assertEqual(msg, 'Response status 403: Forbidden')
        self.assertTrue(BLOCKHANDLER.is_blocked('googleapis'), 'Expected blockage')
        BLOCKHANDLER.clear_all()
        logger.setLevel(logging.INFO)

        # Test invalid error code 99999
        url = 'http://someourl.com/test99999'
        msg, res = cache.fetch_url(url, agent, False, raw=False)
        self.assertFalse(res, 'Expected failure')
        self.assertEqual(msg, 'Response status 99999: Text: Invalid error')
        self.assertFalse(BLOCKHANDLER.is_blocked('googleapis'), 'Unknown error should not cause blockage')

        # Test redirect error
        url = 'http://someourl.com/test-redirects'
        with self.assertLogs(logger, logging.ERROR):
            msg, res = cache.fetch_url(url, agent, False, raw=False)
        self.assertFalse(res, 'Expected failure')
        self.assertEqual(msg, 'TooManyRedirects Test redirect error')

        # Test redirect error with retry. Curously, no error in log
        url = 'http://someourl.com/test-redirects'
        msg, res = cache.fetch_url(url, agent, True, raw=False)
        self.assertFalse(res, 'Expected failure')
        self.assertEqual(msg, 'Exception TooManyRedirects: Test redirect error')

        # Test timeout error. No error in log
        url = 'http://someourl.com/test-timeout'
        with self.assertLogs(logger, logging.ERROR):
            msg, res = cache.fetch_url(url, agent, False, raw=False)
        self.assertFalse(res, 'Expected failure')
        self.assertEqual(msg, 'Timeout Test timeout error')

        # Test timeout error with retry
        url = 'http://someourl.com/test-timeout'
        msg, res = cache.fetch_url(url, agent, True, raw=False)
        self.assertFalse(res, 'Expected failure')
        self.assertEqual(msg, 'Exception Timeout: Test timeout error')

        # Test extended timeout
        url = 'http://someourl.com/torznab/all/test'
        msg, res = cache.fetch_url(url, agent, True, raw=True)
        self.assertTrue(res, 'Should have worked')
        self.assertEqual(msg, f'torznab {ext_timeout}')

        # Test normal timeout
        url = 'http://someourl.com/torznab/something'
        msg, res = cache.fetch_url(url, agent, True, raw=True)
        self.assertTrue(res, 'Should have worked')
        self.assertEqual(msg, f'torznab {timeout}')

        # Test 404 error
        url = 'http://someourl.com/unknown'
        msg, res = cache.fetch_url(url, agent, True, raw=False)
        self.assertFalse(res, 'Should have been a 404')
        self.assertEqual(msg, 'Response status 404: Not Found')

    def test_cache_img_filelink(self):
        """ Test cache_img where the link parameter is a filename """
        # Test with an invalid cache name that doesn't exist
        link = 'not_really_a_link'
        msg, success, was_cached = cache.cache_img(ImageType.TEST, '123', link, refresh=False)
        self.assertFalse(success, 'Should not succeed with bad file')
        self.assertFalse(was_cached, 'Bad file should not be in cache')

        # Test with a file that exists and will get cached
        link = DIRS.get_testdatafile('lazylibrarian.png')
        msg, success, was_cached = cache.cache_img(ImageType.TEST, '123', link, refresh=False)
        self.assertTrue(success, 'Expected the file to be cached')
        self.assertFalse(was_cached, 'The file was not yet be cached')
        self.assertEqual('cache/test/123.jpg', msg, 'The link is not as expected')

        # Now check the file is actually cached
        msg, success, was_cached = cache.cache_img(ImageType.TEST, '123', 'can_be_anything', refresh=False)
        self.assertTrue(success, 'Expected the file to be cached')
        self.assertTrue(was_cached, 'The file should now be cached')
        self.assertEqual('cache/test/123.jpg', msg, 'The link is not as expected')

        # Doing it again with refresh=True just copies the file again
        msg, success, was_cached = cache.cache_img(ImageType.TEST, '123', link, refresh=True)
        self.assertTrue(success, 'Expected the file to be cached')
        self.assertTrue(was_cached, 'And it was already cached')
        self.assertEqual('cache/test/123.jpg', msg, 'The link is not as expected')

        # But if refresh=True and the file does not exist
        msg, success, was_cached = cache.cache_img(ImageType.TEST, '123', 'cannot_be_anything', refresh=True)
        self.assertFalse(success, 'Expected failure')
        self.assertFalse(was_cached, 'Expected not cached with refresh True')

        # Try to cache it to a folder that doesn't exist (during testing)
        with self.assertLogs(self.logger, logging.ERROR):
            # Expect to see an error message in the log
            msg, success, was_cached = cache.cache_img(ImageType.MAG, '123', link, refresh=False)
        self.assertFalse(success, 'Cannot cache file into dir that does not exist')
        self.assertFalse(was_cached, 'Should not be cached')

        # Check for the (non-existing) jpg file instead of the png, and watch magic happen: It
        # checks for, and stores the .png file instead, though it's still called .jpg
        link = DIRS.get_testdatafile('lazylibrarian.jpg')
        msg, success, was_cached = cache.cache_img(ImageType.TEST, '456', link, refresh=False)
        self.assertTrue(success, 'Expected the file to be cached')
        self.assertFalse(was_cached, 'The file was not yet be cached')
        self.assertEqual('cache/test/456.jpg', msg, 'The link is not as expected')

    @mock.patch.object(cache, 'fetch_url')
    def test_cache_img_httplink(self, mock_fetch_url):
        """ Test cache_img where the link parameter is an http link """
        # Mock loading a URL and caching it
        link = "https://lazylibrarian.gitlab.io/logo.svg"
        mock_fetch_url.return_value = (b'Looks like a teddy', True)
        msg, success, was_cached = cache.cache_img(ImageType.TEST, 'abc', link, refresh=False)
        self.assertTrue(success, 'Expected the file to be retrieved and cached')
        self.assertFalse(was_cached, 'The file was not yet be cached')
        self.assertEqual('cache/test/abc.jpg', msg, 'The link is not as expected')

        # Once it's cached, the URL doesn't matter if refresh is false
        mock_fetch_url.return_value = (b'Looks like an error', False)
        msg, success, was_cached = cache.cache_img(ImageType.TEST, 'abc', 'https://invalid', refresh=False)
        self.assertTrue(success, 'Expected to get the cache')
        self.assertTrue(was_cached, 'Expected it was cached')
        self.assertEqual('cache/test/abc.jpg', msg, 'The link is not as expected')

        # If Refresh is True, we don't retrieve a cached file with an invalid URL
        mock_fetch_url.return_value = (b'Looks like an error', False)
        msg, success, was_cached = cache.cache_img(ImageType.TEST, 'abc', 'https://invalid', refresh=True)
        self.assertFalse(success, 'Expected the file to be retrieved and cached')
        self.assertFalse(was_cached, 'The file was not yet be cached')

        # Mock with a URL that fails to work
        mock_fetch_url.return_value = (b'Looks like an error', False)
        msg, success, was_cached = cache.cache_img(ImageType.TEST, 'def', link, refresh=False)
        self.assertFalse(success, 'Expected that caching did not work for wrong URL')
        self.assertFalse(was_cached, 'The file was not yet be cached')

        # Mock with a valid URL but the destination is not writeable (in test)
        mock_fetch_url.return_value = (b'Looks like a teddy', True)
        with self.assertLogs(self.logger, logging.ERROR):
            msg, success, was_cached = cache.cache_img(ImageType.COMIC, 'abc', link, refresh=False)
        self.assertFalse(success, 'Expected an error')
        self.assertFalse(was_cached, 'The file was not yet be cached')

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
            _ = cache.CacheCleaner('')

    def test_file_cleaner(self):
        with self.assertRaises(TypeError):
            _ = cache.FileCleaner('', False)

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
