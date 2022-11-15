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
        startup.shutdown(restart=False, update=False, exit=False, testing=True)
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

    def test_book_series(self):
        testseries =[
            # Single-series
            ("My Book (Toot, #40)", "Toot", '40'), 
            ("Some series (Book 3)", "Book", '3'),
            ("Mrs Bradshaws Handbook (Discworld, #40.5)", "Discworld", '40.5'),
            ("Test book (The Series: Book 6)", "The Series", "6"),
            ("Test book (The Series, Book 6)", "The Series", "6"),
            ("Test book (The Series, 6)", "The Series", "6"),
            # Not sure this should work:
            ("Failure Two (The best, volume 3)", "The best volume", '3'),
            # Multi-volume
            ("My test (The testers 2-5)", "The testers", "2-5"),
            # Multi-series
            ("The Shepherds Crown (Discworld, #41; Tiffany Aching, #5)", "Discworld", '41'),
            ("Another one (TheFirst, book 8; Second, part 3)", "TheFirst", '8'),
            ("A second one (MyFirst, novel 2; Second, part 3)", "MyFirst", '2'),
            ("A third one (Check, part 3; Second, book)", "Check", '3'),
            # Not recognized as series
            ("Book 12, Some series", "", ''),
            ("A book title, not a series", "", ''),
            ("Testing 8: Hello World", "", ""),
            ("Book 3: Some series", "", ""),
            ("Another one (First, part; Second, book 2)", "", ''),
            # Special words cause series to be ignored
            ("Failure One (first of 12 )", "", ''),
            ("Failure Two (volume 3)", "", ''),
            ("Failure Three (unabridged book 3)", "", ''),
            ("Failure Four (phrase 3)", "", ''),
            ("Failure Five (from 3)", "", ''),
            ("Failure Six (chapters 3-7)", "", ''),
            ("Failure Seven (season 3)", "", ''),
            ("Failure Eight (the first 3)", "", ''),
            ("Failure Nine (includes 3)", "", ''),
            ("Failure Ten (paperback no 3)", "", ''),
            ("Failure Eleven (books 3-4)", "", ''),
            ("Failure Twelve (large print 3)", "", ''),
            ("Failure Thirteen (of 3)", "", ''),
            ("Failure Fourteen (rrp 3)", "", ''),
            ("Failure Fifteen (2 in 3)", "", ''),
            ("Failure Sixteen (& 3)", "", ''),
            ("Failure Seventeen (v. 3)", "", ''),
        ]
        for book in testseries:
            seriesname, num = formatter.book_series(book[0])
            self.assertEqual(seriesname, book[1])
            self.assertEqual(num, book[2])

    def test_checkint(self):
        values = [
            ('17', 0, True, 17),
            ('abc', 0, True, 0),
            ('-48', 0, True, 0),
            ('-48', 0, False, -48),
            ('18.2', 0, True, 0),
            ('18.bob', 0, True, 0),
            ('', 0, True, 0),
            (["11"], 0, True, 0),
            (31.2, 0, True, 31),
            (3.8, 3, True, 3),
            (None, 4, True, 4),
        ]
        for value in values:
            self.assertEqual(formatter.check_int(value[0], value[1], value[2]), value[3])
        # Also test with named parameters
        for value in values:
            self.assertEqual(formatter.check_int(default=value[1], positive=value[2], var=value[0]), value[3])

    def test_checkfloat(self):
        values = [
            ('17', 0, 17),
            ('abc', 0, 0),
            ('-48', 0, -48),
            ('-48.7', 0, -48.7),
            ('18.2', 0, 18.2),
            ('18.bob', 0, 0),
            ('', 0, 0),
            (["11"], 0, 0),
            (31.2, 0, 31.2),
            (3.8, 3, 3.8),
            (None, 4, 4),
        ]
        for value in values:
            self.assertEqual(formatter.check_float(value[0], value[1]), value[2])
        # Also test with named parameters
        for value in values:
            self.assertEqual(formatter.check_float(default=value[1], var=value[0]), value[2])

    def test_plural(self):
        values = [
            (1, "hour", "hour"),
            (2, "minute", "minutes"),
            (0, "second", "seconds"),
            (1, "copy", "copy"),
            (-3, "copy", "copies"),
            (1, "entry", "entry"),
            (4, "entry", "entries"),
            (1, "shelf", "shelf"),
            (4, "shelf", "shelves"),
            (1, "series", "series"),
            (4, "series", "series"),
            (1, "is", "is"),
            (4, "is", "are"),
        ]
        for value in values:
            self.assertEqual(formatter.plural(value[0], value[1]), value[2])
