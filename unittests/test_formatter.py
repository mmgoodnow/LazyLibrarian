#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in formatter.py

import unittest
import unittesthelpers

from lazylibrarian import startup, formatter


class FormatterTest(unittest.TestCase):
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        # Run startup code without command line arguments and no forced sleep
        options = startup.startup_parsecommandline(__file__, args = [''], seconds_to_sleep = 0)
        startup.init_logs()
        startup.init_config()
        # startup.init_caches()
        # startup.init_database()
        # startup.init_build_debug_header(online = False)
        # startup.init_build_lists()
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        startup.shutdown(restart=False, update=False, exit=False)
        # unittesthelpers.removetestDB()
        # unittesthelpers.removetestCache()
        # unittesthelpers.clearGlobals()
        unittesthelpers.clearGlobals()
        return super().tearDownClass()

    def test_format_author_name(self):
        testnames = [
            ("Allan Mertner", "Allan Mertner"),
            ("Allan & Mamta Mertner", "Allan"),
            ("Allan Mertner, Jr.", "Allan Mertner Jr."),
            ("Mertner, Allan", "Allan Mertner"),
            ("MERTNER, Allan", "Allan MERTNER"),
            ("ALLAN MERTNER", "Allan Mertner"),
            ("allan mertner", "Allan Mertner"),
            ("aLLaN mErtNer", "aLLaN mErtNer"),
            ("A Mertner", "A. Mertner"),
            ("A. Mertner", "A. Mertner")
        ]
        for names in testnames:
            self.assertEqual(formatter.format_author_name(names[0]), names[1])