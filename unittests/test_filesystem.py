#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the functions in filesystem.py

import os
import mock

from unittesthelpers import LLTestCase
from lazylibrarian import filesystem
from lazylibrarian.filesystem import DIRS

class FilesystemTest(LLTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(False)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        filesystem.remove_dir(DIRS.TMPDIR)
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
            [ b'not a string', '\\\\?\\not a string'],
            ['D:\\','D:\\'],
            [ '\\\\SERVER\\SHARE\\dir', '\\\\?\\UNC\\SERVER\\SHARE\\dir'],
            [ '\\\\SERVER\\SHARE/dir', '\\\\?\\UNC\\SERVER\\SHARE/dir'],
            # If CACHEDIR is part of it, / is replaced with \\ in Windows
            [f'{DIRS.CACHEDIR}/test', f'\\\\?\\{DIRS.CACHEDIR}\\test'],
        ]
        with mock.patch('os.path.__name__', 'posixpath'):
            for path1, _ in paths_input_windows:
                self.assertEqual(filesystem.syspath(path1), path1)

        with mock.patch('os.path.__name__', 'ntpath'):
            for path1, path2 in paths_input_windows:
                self.assertEqual(filesystem.syspath(path1), path2)

    ### Tests for set_datadir

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
