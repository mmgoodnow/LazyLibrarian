#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the library file cleanup

from lazylibrarian.cleanup import unbundle_libraries


class CleanupTest():

    def testCleanup(self):
        # Validate that we don't remove a library that isn't bundled
        lib = [('apprise', None, '')]
        res = unbundle_libraries(lib, testing=True)
        self.assertEqual([], res)
        # validate that we correctly remove a bundled library that can be system version
        lib = [('cherrypy_cors', 'cherrypy_cors.py', '')]
        res = unbundle_libraries(lib, testing=True)
        self.assertEqual(['cherrypy_cors.py'], res)
