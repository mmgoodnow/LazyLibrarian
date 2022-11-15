#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in formatter.py

import unittest
import unittesthelpers

import lazylibrarian
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
        startup.init_build_lists()
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        startup.shutdown(restart=False, update=False, exit=False, testing=True)
        # unittesthelpers.removetestDB()
        # unittesthelpers.removetestCache()
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

    def test_datecompare(self):
        datepairs = [ # Note all dates must be yyyy-mm-dd or yy-mm-dd
            # Valid datepairs
            ("2000-01-02", "2000-01-01", 1),
            ("2000-1-3", "2000-1-1", 2),
            ("99-01-04", "99-01-01", 3),
            ("1999-01-05", "99-01-01", 4),
            ("99-01-01", "1999-01-05", -4),
            ("2000-01-01", "2022-01-01", -8036),
            ("2003-3-1", "2003-2-28", 1),
            ("2004-3-1", "2004-2-28", 2),
            # If one date is invalid, returns 0
            ("Bob is", "your uncle", 0),
            ("2003-2-29", "2003-3-1", 0),
        ]
        for dates in datepairs:
            self.assertEqual(formatter.datecompare(dates[0], dates[1]), dates[2])

    def test_age(self):
        dates = [
            ("2000-01-02"),
            ("2000-1-3"),
            ("99-01-04"),
            ("99-01-01"),
        ]
        for date in dates:
            self.assertEqual(formatter.age(date), formatter.datecompare(formatter.today(), date))

    def test_month2num(self):
        mnum = 0
        for m in lazylibrarian.MONTHNAMES:
            # Try both the short and the long versions
            self.assertEqual(formatter.month2num(m[0]), mnum)
            self.assertEqual(formatter.month2num(m[1]), mnum)
            mnum += 1

        specialmonths = [
            ("winter", 1),
            ("spring", 4),
            ("summer", 7),
            ("fall", 10),
            ("autumn", 10),
            ("christmas", 12),
            ("Not A Month", 0)
        ]
        for special in specialmonths:
            self.assertEqual(formatter.month2num(special[0]), special[1])

    def test_date_format(self):
        dates = [
            ("Tue, 23 Aug 2016 17:33:26 +0100", "2016-08-23"),  # Newznab/Torznab 
            ("13 Nov 2014 05:01:18 +0200", "2014-11-13"),       # LimeTorrent 
            ("04-25 23:46", formatter.now()[:4] + "-04-25"),    # torrent_tpb - use current year
            ("2018-04-25", "2018-04-25"), 
            ("May 1995", "1995-05-01"),                         # openlibrary 
            ("June 20, 2008", "2008-06-20"),
            ("28Dec2008", "2008-12-28"),                        # Compressed into one string
            ("XYZ is not a date", "XYZ-00-not a:date:00"),      # Error, but seen as a date
            ("XYZ", "XYZ"),                                     # Error, just a string
        ]
        for d in dates:
            self.assertEqual(formatter.date_format(d[0]), d[1])