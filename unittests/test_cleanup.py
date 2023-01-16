#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the library file cleanup

from unittest import TestCase
import mock
from lazylibrarian.cleanup import unbundle_libraries, get_library_locations


class CleanupTest(TestCase):

    @mock.patch.object(shutil, 'rmtree')
    def testUnbundle(self):
        # Validate that we don't remove a library that isn't bundled
        lib = [('apprise', None, '')]
        mock_shutil_rmtree.return_value = True
        res = unbundle_libraries(lib)
        self.assertEqual([], res)
        # validate that we correctly remove a bundled library that can be system version
        lib = [('cherrypy_cors', 'cherrypy_cors.py', '')]
        mock_shutil_rmtree.return_value = True
        res = unbundle_libraries(lib)
        self.assertEqual(['cherrypy_cors.py'], res)

    def testLocations(self):
        # Validate that we know this library isn't bundled
        lib = [('apprise', None, '')]
        bundled, distro = get_library_locations(lib)
        self.assertTrue(lib[0] not in bundled)
        