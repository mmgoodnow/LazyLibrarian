#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the startup sequence

import lazylibrarian
from lazylibrarian.logger import lazylibrarian_log
from unittests.unittesthelpers import LLTestCase

class SetupTest(LLTestCase):

    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(True)
        return super().setUpClass()

    def testConfig(self):
        # Validate that basic global objects and configs have run
        self.assertEqual(lazylibrarian_log.LOGLEVEL, 0)  # From config.ini
        self.assertIsNotNone(lazylibrarian.CONFIG)
        self.assertIsInstance(lazylibrarian.CONFIG.get_int('LOGLIMIT'), int)

    def testApprise(self):
        # Validate that APPRISE is defined properly; it's set up uniquely
        self.assertIsNotNone(lazylibrarian.APPRISE)
