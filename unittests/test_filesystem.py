# This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the functions in filesystem.py

import os
import sys
import mock
import unittest
import logging

from unittests.unittesthelpers import LLTestCase
from lazylibrarian import filesystem
from lazylibrarian.config2 import CONFIG
from lazylibrarian.filesystem import DIRS, get_directory, syspath, remove_dir


class FilesystemTest(LLTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(False)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        filesystem.remove_dir(DIRS.TMPDIR, remove_contents=True)
        return super().tearDownClass()

    def setUp(self):
        # Save this, as some tests change it
        self.datadir = DIRS.DATADIR

        return super().setUp()

    def tearDown(self):
        # Save this, as some tests change it
        DIRS.set_datadir(self.datadir)
        return super().tearDown()

    def test_syspath(self):
        """ Test that syspath returns a proper path in both Linux and Windows"""
        paths_input_windows = [
            ['', '\\\\?\\'],
            ['simple', '\\\\?\\simple'],
            ['./relative/path', './relative/path'],
            ['/absolute/path', '\\\\?\\/absolute/path'],
            [b'not a string', '\\\\?\\not a string'],
            ['D:\\', 'D:\\'],
            ['\\\\SERVER\\SHARE\\dir', '\\\\?\\UNC\\SERVER\\SHARE\\dir'],
            ['\\\\SERVER\\SHARE/dir', '\\\\?\\UNC\\SERVER\\SHARE/dir'],
        ]
        with mock.patch('os.path.__name__', 'posixpath'):
            for path1, _ in paths_input_windows:
                self.assertEqual(filesystem.syspath(path1), path1)

        with mock.patch('os.path.__name__', 'ntpath'):
            for path1, path2 in paths_input_windows:
                self.assertEqual(filesystem.syspath(path1), path2)

    # Tests for set_datadir

    @mock.patch.object(filesystem, 'path_isdir')
    @mock.patch.object(os, 'access')
    def test_set_datadir_exists_writeable(self, mock_os_access, mock_path_isdir):
        """ Test set_datadir, which will raise a SystemExit exception on error """
        testdir = 'Pretend-it-exists'
        mock_path_isdir.return_value = True
        mock_os_access.return_value = True
        DIRS.set_datadir(testdir)
        self.assertEqual(DIRS.DATADIR, testdir)
        self.assertEqual(DIRS.CACHEDIR, os.path.join(DIRS.DATADIR, 'cache'))
        self.assertEqual(DIRS.TMPDIR, os.path.join(DIRS.DATADIR, 'tmp'))

    @mock.patch.object(filesystem, 'path_isdir')
    @mock.patch.object(os, 'makedirs')
    @mock.patch.object(os, 'access')
    def test_set_datadir_exists_not_writeable(self, mock_os_access, mock_os_makedirs, mock_path_isdir):
        testdir = 'Pretend-it-exists'
        mock_path_isdir.return_value = True
        mock_os_access.return_value = False
        with self.assertRaises(expected_exception=SystemExit):
            DIRS.set_datadir(testdir)
        mock_path_isdir.assert_called_once_with(testdir)
        mock_os_makedirs.assert_not_called()
        mock_os_access.assert_called_once_with(testdir, os.W_OK)

    @mock.patch.object(filesystem, 'path_isdir')
    @mock.patch.object(os, 'makedirs')
    @mock.patch.object(os, 'access')
    def test_set_datadir_doesnot_exist_created(self, mock_os_access, mock_os_makedirs, mock_path_isdir):
        testdir = 'Pretend-it-exists'
        mock_path_isdir.return_value = False
        mock_os_makedirs.return_value = True
        mock_os_access.return_value = True
        DIRS.set_datadir(testdir)
        self.assertEqual(DIRS.DATADIR, testdir)
        self.assertEqual(DIRS.CACHEDIR, os.path.join(DIRS.DATADIR, 'cache'))
        self.assertEqual(DIRS.TMPDIR, os.path.join(DIRS.DATADIR, 'tmp'))

    @mock.patch.object(filesystem, 'path_isdir')
    @mock.patch.object(os, 'makedirs')
    @mock.patch.object(os, 'access')
    def test_set_datadir_doesnot_exist_cannotcreate(self, mock_os_access, mock_os_makedirs, mock_path_isdir):
        testdir = 'Invalid?*path'
        mock_path_isdir.return_value = False
        mock_os_makedirs.side_effect = OSError
        with self.assertRaises(expected_exception=SystemExit):
            DIRS.set_datadir(testdir)
        mock_path_isdir.assert_called_once_with(testdir)
        mock_os_makedirs.assert_called_once_with(testdir)
        mock_os_access.assert_not_called()

    def test_get_tmpfilename(self):
        # Create lots of temp filenames, make sure they are unique
        tmpnames = {}
        for i in range(10000):
            tmpname = DIRS.get_tmpfilename()
            self.assertFalse(tmpname in tmpnames, f'Temp file name not unique: {tmpname} duplicated!')
            tmpnames[tmpname] = 1

    def test_remove_file(self):
        tmpname = DIRS.get_tmpfilename()
        with open(tmpname, 'x') as f:
            f.write('test')
        self.assertTrue(filesystem.path_isfile(tmpname), f'Should be a file: {tmpname}')
        ok = filesystem.remove_file(tmpname)
        self.assertTrue(ok, f'Could not remove temp file {tmpname}')
        self.assertFalse(filesystem.path_isfile(tmpname), f'Should have been removed: {tmpname}')

    def test_remove_dir(self):
        tmpname = DIRS.get_tmpfilename()
        DIRS.ensure_dir_is_writeable(tmpname)
        self.assertTrue(filesystem.path_isdir(tmpname), f'Should be a dir: {tmpname}')
        ok = filesystem.remove_dir(tmpname)
        self.assertTrue(ok, f'Could not remove temp file {tmpname}')
        self.assertFalse(filesystem.path_isdir(tmpname), f'Should have been removed: {tmpname}')

    def test_get_directory_ok(self):
        # Test the get_directory() function
        # The directories should all have values from unittest/testdata/config-defaults.ini, and differ from the default
        bookdir = get_directory("eBook")
        self.assertNotEqual(bookdir, DIRS.DATADIR, "BookDir and Datadir cannot be the same")
        self.assertEndsWith(bookdir, "eBooks")

        audiobookdir = get_directory("AudioBook")
        audiodir = get_directory("Audio")
        self.assertEqual(audiobookdir, audiodir)
        self.assertNotEqual(audiobookdir, DIRS.DATADIR)
        self.assertEndsWith(audiobookdir, "Audiobooks")

        downloaddir = get_directory("Download")
        self.assertNotEqual(downloaddir, DIRS.DATADIR)
        self.assertEndsWith(downloaddir, "Downloads")

        altdir = get_directory("Alternate")
        self.assertNotEqual(altdir, DIRS.DATADIR)
        self.assertEndsWith(altdir, "Alternative")

        testdir = get_directory("Testdata")
        self.assertNotEqual(testdir, DIRS.DATADIR)
        self.assertEndsWith(testdir, "testdata")

        faultydir = get_directory("This is invalid")
        self.assertEqual(faultydir, "")

    @unittest.skipUnless(sys.platform == 'win32', 'This test is only for Windows')
    def test_get_directory_cannot_be_valid(self):
        # Test the get_directory() function for invalid paths
        self.set_loglevel(logging.DEBUG)
        save = get_directory("Testdata")
        try:
            DIRS.config['TESTDATA_DIR'] = 'Cannot*Be*?V"al/id&Nope'
            with self.assertLogs('root', level='WARN'):
                testdir = get_directory("Testdata")
            self.assertEqual(testdir, DIRS.DATADIR)
        finally:
            DIRS.config['TESTDATA_DIR'] = save

    def test_get_directory_create_ok(self):
        self.set_loglevel(logging.DEBUG)
        save = get_directory("Testdata")
        try:
            newdir = DIRS.get_tmpfilename('newdirtocreate')
            DIRS.config['TESTDATA_DIR'] = newdir
            try:
                with self.assertLogs('root', level='INFO'):
                    testdir = get_directory("Testdata")
                self.assertEqual(syspath(testdir), newdir)
            finally:
                self.assertTrue(remove_dir(testdir))
        finally:
            DIRS.config['TESTDATA_DIR'] = save

    def test_setperm(self):
        self.set_loglevel(logging.DEBUG)
        afile = DIRS.get_tmpfilename()
        self.assertFalse(filesystem.setperm(afile), 'setperm should not work on file that does not exist')

        with open(afile, 'x') as f:
            f.write('test')
        perm1 = os.stat(syspath(afile))
        ok = filesystem.setperm(afile)
        perm2 = os.stat(syspath(afile))
        self.assertTrue(ok, f'setperm should work on a new file, {afile}: {perm1}, {perm2}')

        self.assertTrue(filesystem.setperm(filesystem.get_directory("Testdata")), 'setperm should work on a dir')

    def test_make_dirs(self):
        self.set_loglevel(logging.INFO)
        basedir = DIRS.TMPDIR
        deepdir = os.path.join(basedir, 'testmake', 'level', 'three', 'deepest')
        self.assertTrue(filesystem.make_dirs(deepdir), 'Cannot create deep directory tree')
        self.assertTrue(filesystem.path_isdir(deepdir), 'Expected directory to be a dir')

        self.assertTrue(filesystem.make_dirs(deepdir, new=True), 'Cannot re-create deep directory tree')

    def test_safe_move_and_copy(self):
        self.set_loglevel(logging.INFO)
        startfile = filesystem.any_file(get_directory("Testdata"), 'ini')
        file1 = DIRS.get_tmpfilename('tst-move')
        self.assertFalse(filesystem.path_isfile(file1), 'File1 must not exist to start')

        filesystem.safe_copy(startfile, file1)
        self.assertTrue(filesystem.path_isfile(file1), 'File1 must exist after copy')

        file2 = DIRS.get_logfile('tstmove2')
        filesystem.safe_move(file1, file2)
        self.assertFalse(filesystem.path_isfile(file1), 'File1 must not exist after moving to file2')
        self.assertTrue(filesystem.path_isfile(file2), 'File2 must exist after move')

    def test_any_file(self):
        anyfile = filesystem.any_file(get_directory("Testdata"), 'ini')
        self.assertNotEqual(anyfile, '', 'Expected to find an ini file!')

        anyfile = filesystem.any_file(get_directory("Testdata"), 'fred')
        self.assertEqual(anyfile, '', 'Expected to not find any .fred files')

        anyfile = filesystem.any_file(get_directory("Testdata"), '')
        self.assertNotEqual(anyfile, '', 'Expected to not a file without an extension')

        newdir = os.path.join(get_directory("Testdata"), 'a_new_dir')
        anyfile = filesystem.any_file(newdir, '')
        self.assertEqual(anyfile, '', 'Expected to not find any files in a dir that does not exist')

    def test_opf_file(self):
        opf = filesystem.opf_file(get_directory("Testdata"))
        self.assertEndsWith(opf, 'metadata.opf')

        opf = filesystem.opf_file(DIRS.DATADIR)
        self.assertEqual(opf, '')

    def test_book_file(self):
        book = filesystem.book_file(get_directory("Testdata"), '', config=CONFIG)
        self.assertEqual(book, '', 'Searching for type None should not find a book')

        book = filesystem.book_file(get_directory("Testdata"), 'book', config=CONFIG, recurse=False)
        self.assertTrue(book != '', 'Expected to find a book file in the testdata dir')

        book = filesystem.book_file(DIRS.DATADIR, 'book', config=CONFIG, recurse=False)
        self.assertEqual(book, '', 'Did not expect to find a book file in the DATADIR')

        book = filesystem.book_file(DIRS.DATADIR, 'book', config=CONFIG, recurse=True)
        self.assertTrue(book != '', 'Expected to find a book file below the DATADIR')
