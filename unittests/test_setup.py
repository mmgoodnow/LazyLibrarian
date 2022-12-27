#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the startup sequence

import lazylibrarian
from lazylibrarian.config2 import CONFIG
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
        self.assertEqual(lazylibrarian_log.LOGLEVEL, 1)  # From config.ini
        self.assertIsNotNone(CONFIG)
        self.assertIsInstance(CONFIG.get_int('LOGLIMIT'), int)

    def testApprise(self):
        # Validate that APPRISE is defined properly; it's set up uniquely
        self.assertIsNotNone(lazylibrarian.APPRISE)
