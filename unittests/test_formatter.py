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

    def test_split_title(self):
        testdata = [
            # "Normal" books
            ("Author Name", "Author Name: The Book Title", ("The Book Title", "", "")),
            ("Author Name", "The Book Title", ("The Book Title", "", "")),
            # Titles with a "subtitle"
            ("Author", "Book: An explanation", ("Book", "An explanation", "")),
            ("Author", "Author: Book: An explanation", ("Book", "An explanation", "")),
            # Title with a "series" but no subtitle
            ("Author", "My Book (Toot, #40)", ("My Book", "", "Toot, #40)")), 
            ("Author", "Author: Some series (Book 3)", ("Some series", "", "Book 3)")),
            ("Author", "Test book (The Series: Book 6)", ("Test book", "", "The Series: Book 6)")),
            ("Author", "Author: Test book (The Series, 6)", ("Test book", "", "The Series, 6)")),
            ("Author Name", "Author Name: Book (Series: Subseries 1)", ("Book", "", "Series: Subseries 1)")),
            # Titles with "commentary" in the title
            ("Author Name", "Author Name: Book (Unabridged)", ("Book", "(Unabridged)", "")),
            ("Author Name", "Author Name: Book (Unabridged volume)", ("Book", "(Unabridged volume)", "")),
            # Books with a subtitle in a series
            ("Abraham Lincoln", "Vampire Hunter: A horrifying tale (Vampires #2)", ("Vampire Hunter", "A horrifying tale", "Vampires #2)")),
            ("Abraham Lincoln", "Abraham Lincoln: Vampire Hunter: A horrifying tale (Vampires #2)", ("Vampire Hunter", "A horrifying tale", "Vampires #2)")),
        ]
        lazylibrarian.CONFIG['IMP_NOSPLIT'] = ''
        for data in testdata:
            name, sub, series = formatter.split_title(data[0], data[1])
            self.assertEqual((name, sub, series), data[2], f"No split: {data}")
        # TODO/AM: Fix this test once splitlist functionality works
        #lazylibrarian.CONFIG['IMP_NOSPLIT'] = "unabridged","tm","annotated"
        #for data in testdata:
        #    name, sub, series = formatter.split_title(data[0], data[1])
        #    self.assertEqual((name, sub, series), data[2], f"Split: {data}")
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
            ("", ""), 
        ]
        for d in dates:
            self.assertEqual(formatter.date_format(d[0]), d[1])

    def test_versiontuple(self):
        versions = [
            ("1.2", (1,2,0)),
            ("2.3.4", (2,3,4)),
            ("1.2.3-beta4", (1,2,3)),
            ("gibberish", (0,0,0)),
            ("8.x.y", (8,0,0)),
        ]
        for v in versions:
            self.assertEqual(formatter.versiontuple(v[0]), v[1])

    def test_human_size(self):
        sizes = [
            (100, "100.00B"),
            (2000, "1.95KiB"),
            (32*1024**2+8, "32.00MiB"),
            (12*1024**3, "12.00GiB"),
            (3*1024**4, "3.00TiB"),
            (81*1024**5.2+10*1024**4, "324.01PiB"),
            ("bob", "0.00B"),
        ]
        for s in sizes:
            self.assertEqual(formatter.human_size(s[0]), s[1])

    def test_size_in_bytes(self):
        sizes = [
            (100, "100"),
            (1996, "1.95KiB"),
            (33554432, "32.00MiB"),
            (12*1024**3, "12.00GiB"),
            (0, "bob"),
        ]
        for s in sizes:
            self.assertEqual(formatter.size_in_bytes(s[1]), s[0])

    def test_md5_utf8(self):
        strings = [
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("This is a test", "ce114e4501d2f4e2dcea3e17b546f339"),
            ("Using ÆØÅ, æøå and ½é", "93addf1c05adc126200c25b512a3cdbd"),
        ]
        for str in strings:
            self.assertEqual(formatter.md5_utf8(str[0]), str[1])

    def test_make_utf8bytes(self):
        strings = [
            ("", b'', ""),
            ("This is a test", b'This is a test', ""),
            ("ÆØÅ, æøå and ½é", b'\xc3\x83\xc2\x86\xc3\x83\xc2\x98\xc3\x83\xc2\x85, \xc3\x83\xc5\xa0\xc3\x83\xc5\xbe\xc3\x83\xc2\xa5 and \xc3\x82\xc5\x93\xc3\x83\xc2\xa9', "ISO-8859-15"),
        ]
        for str in strings:
            encoded, name = formatter.make_utf8bytes(str[0])
            self.assertEqual((encoded, name), (str[1], str[2]))

    # def test_make_unicode(self):
    #     strings = [
    #         (b'', b''),
    #         (b'\xc3\x83\xc2\x86\xc3\x83\xc2\x98\xc3\x83\xc2\x85, \xc3\x83\xc5\xa0\xc3\x83\xc5\xbe\xc3\x83\xc2\xa5'),
    #     ]
    #     for str in strings:
    #         uni = formatter.make_unicode(str[0])
    #         print(uni, uni==str[1])
            #self.assertEqual((encoded, name), (str[1], str[2]))

    
    def test_is_valid_isbn(self):
        isbns = [
            ("0123456789", True),
            ("0123456789123", True),
            ("012345678X123", False),
            ("0136091814", True),
            ("013609181X", False),
            ("1616550416", False),
            ("155404295X", True),
            ("", False),
            (None, False),
        ]
        for isbn in isbns:
            self.assertEqual(formatter.is_valid_isbn(isbn[0]), isbn[1], isbn[0])

    def test_is_valid_type(self):
        filenames = [
            ("book.opf", True),      # Book metadata
            ("cover.jpg", True),     # Cover images
            ("A volume.pdf", True),  # Magazines and ebooks
            ("Audio.mp3", True),     # Audiobook 
            ("Adio.m4b", True),      # Modern audiobook
            ("TEST.EPUB", True),     # eBook
            ("Book 2.mobi", True),   # eBook
            ("Marvel.Cbr", True),    # Comic
            ("DC.cbZ", True),        # Comic
            # Not valid extensions:
            ("", False),
            ("Hello", False),
            ("jpg", False),
            (".mobi", False),        # eBook without a name
            ("Allan.test", False),
         ]
        for name in filenames:
            self.assertEqual(formatter.is_valid_type(name[0]), name[1], name[0])

    def test_is_valid_booktype(self):
        types = ["book", "mag", "audio", "comic"]
        filenames_ok = [
            # Books: 'epub, mobi, pdf'
            ("A volume.pdf", ("book", "mag")),  
            ("TEST.EPUB", ("book")),     
            ("Book 2.mobi", ("book")),   
            # Audiobooks: mp3, m4b
            ("Audio.mp3", ("audio")),
            ("Adio.m4b", ("audio")),     
            # Comics: cbr, cbz
            ("Marvel.Cbr", ("comic")),    
            ("DC.cbZ", ("comic")),       
            # Magazines: .pdf
            ("My mag.pdf", ("mag", "book"))
        ]
        for name in filenames_ok:
            fn = name[0]
            valid_types = name[1]
            for t in types:
                self.assertEqual(formatter.is_valid_booktype(fn, t), t in valid_types, f"{fn} ({valid_types}, {t})")

    def test_get_list(self):
        lists = [
            # Standard separations
            ("A few items, and some more", None, ["A", "few", "items", "and", "some", "more"]),
            ("C:\Program Files\Test\Some File.jpg,D:\Another file.jpg",None, ['C:\\Program', 'Files\\Test\\Some', 'File.jpg', 'D:\\Another', 'file.jpg']),
            # Separate just on comma
            ("C:\Program Files\Test\Some File.jpg,D:\Another file.jpg",',', ['C:\\Program Files\\Test\\Some File.jpg', 'D:\\Another file.jpg']),
            # Tricky: Tell it to separate on comma and space, and it separates on the default
            ("C:\Program Files\Test\Some File.jpg,D:\Another file.jpg",',;', ['C:\\Program', 'Files\\Test\\Some', 'File.jpg', 'D:\\Another', 'file.jpg']),
            # The empy list
            ("", ' ', []),
            (" ,   ", '', []),
        ]
        for string in lists:
            self.assertEqual(formatter.get_list(string[0], string[1]), string[2])

    def test_sort_definite(self):
        strings = [
            ("The Test Case", "Test Case, The"),
            ("A case of testing", "case of testing, A"),
            ("This is just a book", "This is just a book"),
            ("", ""),
            ("A", "A"),
        ]
        for s in strings:
            self.assertEqual(formatter.sort_definite(s[0]), s[1])

    def test_surname_first(self):
        testnames = [
            # Passing through case
            ("Allan Mertner", "Mertner, Allan"),
            ("Allan & Mamta Mertner", "Mertner, Allan & Mamta"),
            ("ALLAN MERTNER", "MERTNER, ALLAN"),
            ("allan mertner", "mertner, allan"),
            ("aLLaN mErtNer", "mErtNer, aLLaN"),
            # Testing with initials, with or without .
            ("A Mertner", "Mertner, A"),
            ("A. Mertner", "Mertner, A."),
            # Testing with middle names
            ("Allan Douglas Mertner", "Mertner, Allan Douglas"),
            # It doesn't reverse strings already in order
            ("Mertner, Allan", "Allan, Mertner"),
            ("MERTNER, Allan", "Allan, MERTNER"),
            # Test with postfixes
            ("Allan Mertner, Jr.", "Mertner Jr., Allan"),
            ("Allan Mertner JNR", "Mertner JNR, Allan"),
            ("Allan Mertner, PhD", "Mertner PhD, Allan"),
            ("Allan Testing Mertner Snr", "Mertner Snr, Allan Testing"),
        ]
        for name in testnames:
            self.assertEqual(formatter.surname_first(name[0]), name[1])

    def test_format_author_name(self):
        testnames = [
            ("Allan Mertner", "Allan Mertner"),
            ("Allan & Mamta Mertner", "Allan"),
            ("Mertner, Allan", "Allan Mertner"),
            ("MERTNER, Allan", "Allan MERTNER"),
            ("ALLAN MERTNER", "Allan Mertner"),
            ("allan mertner", "Allan Mertner"),
            ("aLLaN mErtNer", "aLLaN mErtNer"),
            ("A Mertner", "A. Mertner"),
            ("A. Mertner", "A. Mertner"),
            # With suffix
            ("Allan Mertner, Jr.", "Allan Mertner Jr."),
            ("Allan Mertner PhD", "Allan Mertner PhD"),
            ("Allan Mertner, General", "General Allan Mertner"),
        ]
        for names in testnames:
            self.assertEqual(formatter.format_author_name(names[0]), names[1])
