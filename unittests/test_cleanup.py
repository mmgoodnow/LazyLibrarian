#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the library file cleanup

import os
import shutil
import subprocess
from unittest import TestCase

import mock

from lazylibrarian.cleanup import ll_dependencies, get_library_locations, calc_libraries_to_delete, \
    install_missing_libraries, delete_libraries


class CleanupTest(TestCase):

    def test_get_library_locations(self):
        # Validate that we know this library isn't bundled
        lib = [('apprise', '', '')]
        bundled, distro = get_library_locations(basedir='/test', dependencies=lib)
        self.assertTrue(lib[0][0] not in bundled)
        self.assertLessEqual(len(distro), 1, 'Expect to find at most 1 item in distro')
        if distro:
            self.assertTrue(lib[0][0] in distro, 'Expect to find the key for the lib we looked for')

        # Would like to test more, but it's hard to mock out finder in a good way. Parked.

    @mock.patch.object(subprocess, 'run')
    def test_install_missing_libraries(self, mock_subprocess_run):
        # Most basic case: Nothing bundled, nothing in distro
        distro = {}
        bundled = {}
        newdistro = install_missing_libraries(bundled, distro)
        self.assertEqual({}, newdistro, 'Expect empty list of distro')

        # Install two fake libraries successfully:
        distro = {}
        bundled = {'abc': 'somewhere', 'def': 'somewhereelse'}
        mock_subprocess_run.stdout = 'Success'
        newdistro = install_missing_libraries(bundled, distro)
        self.assertEqual({'abc': 'new install', 'def': 'new install'}, newdistro, 'Libs did not install as expected')

        # Fake failing at installing a library
        distro = {}
        bundled = {'abc': 'somewhere'}
        mock_subprocess_run.stdout = 'Error installing abc...'
        mock_subprocess_run.side_effect = mock.Mock(side_effect=subprocess.CalledProcessError(17, 'pip', 'Error'))
        newdistro = install_missing_libraries(bundled, distro)
        self.assertEqual({}, newdistro, 'Expected empty distro list after error installing')

    def test_calc_libraries_to_delete(self):
        # Test that we calculate the list of libraries to delete correctly
        distro = {}
        todel = calc_libraries_to_delete(ll_dependencies, distro)
        self.assertListEqual([], todel, 'Expect no deletions when distro list is empty')

        # Test 3 libraries to use pipname and aka:
        distro = {'urllib3': 'somepath',
                  'PIL': 'xyz',
                  'cherrypy_cors': 'abc'}
        todel = calc_libraries_to_delete(ll_dependencies, distro)
        self.assertEqual(len(distro), len(todel), 'Expect to see delete list same len as distro')
        self.assertListEqual(['urllib3', 'cherrypy_cors.py', 'Pillow'], todel, 'Did not get expected list of deletions')

        # Test for library that doesn't exist
        distro = {'randomlib': '', }
        todel = calc_libraries_to_delete(ll_dependencies, distro)
        self.assertEqual(0, len(todel), 'Expect to see no deletions')

    @mock.patch.object(os, 'remove')
    @mock.patch.object(os.path, 'isdir')
    @mock.patch.object(os.path, 'isfile')
    @mock.patch.object(shutil, 'rmtree')
    def test_delete_libraries(self, mock_shutil_rmtree, mock_os_path_isfile, mock_os_path_isdir, mock_os_remove):
        cwd = os.getcwd()
        libraries = []
        removed = delete_libraries('', libraries)
        self.assertEqual([], removed, 'We removed libraries when none were supplied')

        # Test removing a file
        mock_os_path_isdir.return_value = False
        mock_os_path_isfile.return_value = True
        libraries = ['somefile']
        removed = delete_libraries('/test', libraries)
        self.assertEqual(['somefile'], removed, 'Expected to remove the file supplied')
        expect = os.path.join('/test', 'somefile')
        mock_os_path_isdir.assert_called_with(expect)
        mock_os_remove.assert_called_with(expect)

        # Test removing a dir
        mock_os_path_isdir.return_value = True
        mock_os_path_isfile.return_value = False
        libraries = ['somedir']
        removed = delete_libraries('/test', libraries)
        self.assertEqual(['somedir'], removed, 'Expected to remove the dir supplied')
        expect = os.path.join('/test', 'somedir')
        mock_os_path_isdir.assert_called_with(expect)
        mock_shutil_rmtree.assert_called_with(expect)
