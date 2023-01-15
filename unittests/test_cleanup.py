#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the library file cleanup

from unittest import TestCase
from lazylibrarian.cleanup import unbundle_libraries


class CleanupTest(TestCase):

    def testCleanup(self):
        # Validate that we don't remove a library that isn't bundled
        lib = [('apprise', None, '')]
        res = unbundle_libraries(lib, testing=True)
        self.assertEqual([], res)
        # validate that we correctly remove a bundled library that can be system version
        lib = [('cherrypy_cors', 'cherrypy_cors.py', '')]
        res = unbundle_libraries(lib, testing=True)
        self.assertEqual(['cherrypy_cors.py'], res)
