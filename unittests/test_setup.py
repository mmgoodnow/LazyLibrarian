#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the startup sequence

import unittest
import unittesthelpers

import lazylibrarian
from lazylibrarian import startup


class SetupTest(unittest.TestCase):
 
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        # Run startup code without command line arguments and no forced sleep
        options = startup.startup_parsecommandline(__file__, args = [''], seconds_to_sleep = 0)
        startup.init_logs()
        startup.init_config()
        startup.init_caches()
        startup.init_database()
        startup.init_build_debug_header(online = False)
        startup.init_build_lists()
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        startup.shutdown(restart=False, update=False, exit=False)
        unittesthelpers.removetestDB()
        unittesthelpers.removetestCache()
        return super().tearDownClass()

    def testConfig(self):
        # Validate that basic global objects and configs have run
        self.assertEqual(lazylibrarian.LOGLEVEL, 1)
        self.assertIsNotNone(lazylibrarian.CONFIG)
        self.assertIsInstance(lazylibrarian.CONFIG['LOGLIMIT'], int)

    def testApprise(self):
        # Validate that APPRISE is defined properly; it's set up uniquely
        self.assertIsNotNone(lazylibrarian.APPRISE)