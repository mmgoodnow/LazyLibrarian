#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in formatter.py

import lazylibrarian
from lazylibrarian.config2 import CONFIG
from lazylibrarian import formatter
from unittests.unittesthelpers import LLTestCase

class FormatterTest(LLTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(False)
        return super().setUpClass()

    def test_sanitize(self):
        import unicodedata
        strings = [
            ("", ""),
            ("C:\\My eBooks\\book.epub", 'C\\My eBooks\\book.epub'),
            ("My oddly named ÆØÅ ebook...", 'My oddly named AOÅ ebook'),
            ("Stuff here "+chr(2)+">< |&!?-\\$|+`~=*", 'Stuff here  &!-\\s+~='),
            ("Not C:\\\\// usable [as a] file name.jpg", 'Not C\\/ usable [as a] file name.jpg'),
            (u'\2160'+u'\0049', '\x8e09'),
            ('Hello Über', 'Hello Über'), # Unicode-string in NFKD->NFC format
            ("\\\\Server\\Test An odd one:2131", '\\Server\\Test An odd one2131'),
        ]
        for s in strings:
            sn = str(formatter.sanitize(s[0]))
            self.assertEqual(sn, s[1])
            try:
                self.assertTrue(unicodedata.is_normalized("NFC", sn))
            except AttributeError:
                pass # P37: unicodedata.is_normalized is not valid in Python 3.7

    def test_url_fix(self):
        urls = [
            ("http://www.random.com/query?test=123", 'http://www.random.com/query?test=123'),
            ("https://10.11.12.13:1234/query?test=I am :a pup/py:&x y", 'https://10.11.12.13:1234/query?test=I+am+:a+pup%2Fpy:&x+y'),
            ("I am not an Über URL '+chr(8)", 'I%20am%20not%20an%20U%CC%88ber%20URL%20%27%2Bchr%288%29'),
        ]
        for url in urls:
            self.assertEqual(formatter.url_fix(url[0]), url[1])

    def test_make_bytestr(self):
        data = [
            ("This is a string", b'This is a string'),
            ("This is an Über Über string", b'This is an U\xcc\x88ber \xc3\x9cber string'),
            (b'123', b'123'),
            (None, None),
            (231, b'231'),
            (["abc", 123], b"['abc', 123]")
        ]
        for d in data:
            self.assertEqual(formatter.make_bytestr(d[0]), d[1])

    def test_safe_unicode(self):
        strings = [
            ("", ""),
            ("Stuff here "+chr(2)+">< |&!?-\\$|+`~=*", "Stuff here "+chr(2)+">< |&!?-\\$|+`~=*"),
            (u'\2160'+u'\0049', u'\2160'+u'\0049'),
            (u'\x8e09', u'\x8e09'),
            ('Hello Über', 'Hello Über'),
            (b'\xc3\x28', "b'\\xc3('" ), # Invalid 2-byte sequence
            (b"\xf0\x28\x8c\xbc", "b'\\xf0(\\x8c\\xbc'"), # Invalid 4-byte sequence
        ]
        for s in strings:
            self.assertEqual(formatter.safe_unicode(s[0]), s[1])

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
            ("Author", "My Book (Toot, #40)", ("My Book", "", "Toot, #40")),
            ("Author", "Author: Some series (Book 3)", ("Some series", "", "Book 3")),
            ("Author", "Test book (The Series: Book 6)", ("Test book", "", "The Series: Book 6")),
            ("Author", "Author: Test book (The Series, 6)", ("Test book", "", "The Series, 6")),
            ("Author Name", "Author Name: Book (Series: Subseries 1)", ("Book", "", "Series: Subseries 1")),
            # Titles with "commentary" in the title
            ("Author Name", "Author Name: Book (Unabridged)", ("Book", "(Unabridged)", "")),
            ("Author Name", "Author Name: Book (Unabridged volume)", ("Book", "(Unabridged volume)", "")),
            ("Author Name", "Author Name: Book (TM)", ("Book", "(TM)", "")),
            # Books with a subtitle in a series
            ("Abraham Lincoln", "Vampire Hunter: A horrifying tale (Vampires #2)", ("Vampire Hunter", "A horrifying tale", "Vampires #2")),
            ("Abraham Lincoln", "Abraham Lincoln: Vampire Hunter: A horrifying tale (Vampires #2)", ("Vampire Hunter", "A horrifying tale", "Vampires #2")),
        ]
        testcommentarydata = [
            # Titles with "commentary" in the title
            ("Author Name", "Author Name: Book (Unabridged)", ("Book", "", "")),
            ("Author Name", "Author Name: Book (Unabridged volume)", ("Book", "(Unabridged volume)", "")),
            ("Author Name", "Author Name: Book (TM)", ("Book", "", "")),
        ]
        CONFIG.set_str('IMP_NOSPLIT', '')
        for data in testdata:
            name, sub, series = formatter.split_title(data[0], data[1])
            self.assertEqual((name, sub, series), data[2], f"Testdata: {data}")

        CONFIG.set_csv('IMP_NOSPLIT', "unabridged,tm,annotated")
        for data in testcommentarydata:
            name, sub, series = formatter.split_title(data[0], data[1])
            self.assertEqual((name, sub, series), data[2], f"Testcommentarydata: {data}")

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
            "2000-01-02",
            "2000-1-3",
            "99-01-04",
            "99-01-01",
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

    def test_nzbdate2format(self):
        nzbdates = [
            ("mon 22 oct 1998", "1998-10-22"),
            ("Sun 23 Jan 2001", "2001-01-23"),
            ("Whatever 2 nov 1994", "1994-11-2"),
            ("Not a date", "1970-01-01"),
            ("2000-01-01", "1970-01-01"),
        ]
        for nzbdate in nzbdates:
            self.assertEqual(formatter.nzbdate2format(nzbdate[0]), nzbdate[1])

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
        for teststr in strings:
            self.assertEqual(formatter.md5_utf8(teststr[0]), teststr[1])

    def test_make_utf8bytes(self):
        strings = [
            ("", b'', ""),
            ("This is a test", b'This is a test', ""),
            ("ÆØÅ, æøå and ½é", b'\xc3\x83\xc2\x86\xc3\x83\xc2\x98\xc3\x83\xc2\x85, \xc3\x83\xc5\xa0\xc3\x83\xc5\xbe\xc3\x83\xc2\xa5 and \xc3\x82\xc5\x93\xc3\x83\xc2\xa9', "ISO-8859-15"),
        ]
        for teststr in strings:
            encoded, name = formatter.make_utf8bytes(teststr[0])
            self.assertEqual((encoded, name), (teststr[1], teststr[2]))

    def test_make_unicode(self):
        strings = [
            (None, None),
            (b'', ''),
            (b'\xc3\x83\xc2\x86\xc3\x83\xc2\x98\xc3\x83\xc2\x85', 'Ã\x86Ã\x98Ã\x85'),
            ('Hello Über', 'Hello Über'),
            (123, "123"),
            ([False, None, "Allan"], "[False, None, 'Allan']"),
            (b'\xc3\x28', 'Ã(' ), # Invalid 2-byte sequence

        ]
        for teststr in strings:
            self.assertEqual(formatter.make_unicode(teststr[0]), teststr[1])

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
        validfilenames = [
            "book.opf",      # Book metadata
            "cover.jpg",     # Cover images
            "A volume.pdf",  # Magazines and ebooks
            "Audio.mp3",     # Audiobook
            "Adio.m4b",      # Modern audiobook
            "TEST.EPUB",     # eBook
            "Book 2.mobi",   # eBook
            "Marvel.Cbr",    # Comic
            "DC.cbZ",        # Comic
        ]
        invalidfilenames = [
            "",
            "Hello",
            "jpg",
            ".mobi",        # eBook without a name
            "Allan.test",
        ]
        allowlist = CONFIG.get_all_types_list()
        for name in validfilenames:
            self.assertTrue(formatter.is_valid_type(name, extensions=allowlist))
        for name in invalidfilenames:
            self.assertFalse(formatter.is_valid_type(name, extensions=allowlist))

    def test_is_valid_booktype(self):
        types = ["book", "mag", "audio", "comic"]
        filenames_ok = [
            # Books: 'epub, mobi, pdf'
            ("A volume.pdf", ("book", "mag")),
            ("TEST.EPUB", "book"),
            ("Book 2.mobi", "book"),
            # Audiobooks: mp3, m4b
            ("Audio.mp3", "audio"),
            ("Adio.m4b", "audio"),
            # Comics: cbr, cbz
            ("Marvel.Cbr", "comic"),
            ("DC.cbZ", "comic"),
            # Magazines: .pdf
            ("My mag.pdf", ("mag", "book"))
        ]
        for name in filenames_ok:
            fn = name[0]
            valid_types = name[1]
            for t in types:
                self.assertEqual(CONFIG.is_valid_booktype(fn, t), t in valid_types, f"{fn} ({valid_types}, {t})")

    def test_get_list(self):
        lists = [
            # Standard separations
            ("A few items, and some more", None, ["A", "few", "items", "and", "some", "more"]),
            ("C:\\Program Files\\Test\\Some File.jpg,D:\\Another file.jpg",None, ['C:\\Program', 'Files\\Test\\Some', 'File.jpg', 'D:\\Another', 'file.jpg']),
            # Separate just on comma
            ("C:\\Program Files\\Test\\Some File.jpg,D:\\Another file.jpg",',', ['C:\\Program Files\\Test\\Some File.jpg', 'D:\\Another file.jpg']),
            # Tricky: Tell it to separate on comma and space, and it separates on the default
            ("C:\\Program Files\\Test\\Some File.jpg,D:\\Another file.jpg",',;', ['C:\\Program', 'Files\\Test\\Some', 'File.jpg', 'D:\\Another', 'file.jpg']),
            # CSV content
            ('snr, jnr, jr, sr, phd', None, ['snr', 'jnr', 'jr', 'sr', 'phd']),
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
            ("Allan Pedersen", "Pedersen, Allan"),
            ("Allan & Mamta Pedersen", "Pedersen, Allan & Mamta"),
            ("ALLAN SMITH", "SMITH, ALLAN"),
            ("allan bmythe-banks", "bmythe-banks, allan"),
            ("aLLaN apPlEBy", "apPlEBy, aLLaN"),
            # Testing with initials, with or without .
            ("A Pedersen", "Pedersen, A"),
            ("A. Pedersen", "Pedersen, A."),
            # Testing with middle names
            ("Allan Douglas Pedersen", "Pedersen, Allan Douglas"),
            # It doesn't reverse strings already in order
            ("Pedersen, Allan", "Allan, Pedersen"),
            ("SMITH, Allan", "Allan, SMITH"),
            # Test with postfixes
            ("Allan Pedersen, Jr.", "Pedersen Jr., Allan"),
            ("Allan Pedersen JNR", "Pedersen JNR, Allan"),
            ("Allan Pedersen, PhD", "Pedersen PhD, Allan"),
            ("Allan Testing Pedersen Snr", "Pedersen Snr, Allan Testing"),
        ]
        for name in testnames:
            authorname = formatter.surname_first(name[0], postfixes=CONFIG.get_list('NAME_POSTFIX'))
            self.assertEqual(authorname, name[1], f"{name[0]} -> {authorname} instead of {name[1]}")

    def test_format_author_name(self):
        testnames_plain = [
            ("Allan Pedersen", "Allan Pedersen"),
            ("Allan & Mamta Pedersen", "Allan"),
            ("Pedersen, Allan", "Allan Pedersen"),
            ("SMITH, Allan", "Allan SMITH"),
            ("ALLAN SMITH", "Allan Smith"),
            ("allan smythe-banks", "Allan Smythe-Banks"),
            ("aLLaN apPlEBy", "aLLaN apPlEBy"),
            ("A Pedersen", "A. Pedersen"),
            ("A. Pedersen", "A. Pedersen"),
        ]
        testnames_withsuffix = [
            ("Allan Pedersen, Jr.", "Allan Pedersen Jr."),
            ("Allan Pedersen PhD", "Allan Pedersen PhD"),
            ("Allan Pedersen, General", "General Allan Pedersen"),
        ]
        # Test with a variety of postfixes on names without one
        for postfix in [[], [''], ['phd', 'mr', 'jr']]:
            for name in testnames_plain:
                with self.subTest(msg=f'Testing "{name}" with "{postfix}"'):
                    authorname = formatter.format_author_name(name[0], postfix=postfix)
                    self.assertEqual(authorname, name[1])
        # Test with a valid suffix list on names with suffixes
        for name in testnames_withsuffix:
            with self.subTest(msg=f'Testing suffixed "{name}" with "{postfix}"'):
                authorname = formatter.format_author_name(name[0], postfix=['phd', 'mr', 'jr'])
                self.assertEqual(authorname, name[1])

        # Test with the config from ini file
        postfix=CONFIG.get_list('NAME_POSTFIX')
        for name in testnames_plain + testnames_withsuffix:
            with self.subTest(msg=f'Testing "{name}" with ini file postfix "{postfix}"'):
                authorname = formatter.format_author_name(name[0], postfix=postfix)
                self.assertEqual(authorname, name[1], f"{name[0]} -> {authorname} instead of {name[1]}")

    def test_no_umlauts(self):
        teststrings = [
            ('Test ' + u'\xe4', 'Test ae'),
            ('Test ' + u'\xf6', 'Test oe'),
            ('Test ' + u'\xfc', 'Test ue'),
            ('Test ' + u'\xc4', 'Test Ae'),
            ('Test ' + u'\xd6', 'Test Oe'),
            ('Test ' + u'\xdc', 'Test Ue'),
            ('Test ' + u'\xdf', 'Test ss'),
        ]
        # no_umlauts only does something if German is a language used
        lang = CONFIG['IMP_PREFLANG']
        CONFIG.set_str('IMP_PREFLANG', 'eng')
        # First test that nothing changes without German
        for s in teststrings:
            self.assertEqual(formatter.no_umlauts(s[0]), s[0])
        CONFIG.set_str('IMP_PREFLANG', 'de')
        for s in teststrings:
            self.assertEqual(formatter.no_umlauts(s[0]), s[1])
        CONFIG.set_str('IMP_PREFLANG', lang)

    def test_disp_name(self):
        # Add some dummy data to test on
        rss = CONFIG.providers('RSS')
        self.assertIsNotNone(rss)
        if rss:
            rss[0].set_str('HOST', 'test-host')
            rss[0].set_str('DISPNAME', 'short-rss-name')
        irc = CONFIG.providers('IRC')
        self.assertIsNotNone(irc)
        if irc:
            irc[0].set_str('SERVER', 'irc-host')
            irc[0].set_str('DISPNAME', '123456789012345/67890Thisistoolong')

        providers = [
            ('test', 'test'),
            ('quite/short/item', 'quite/short/item'),
            ('test/hello/world/veryvery/long/item', 'veryvery long item'),
            ('', 'Apprise'),
            ('test-host', 'short-rss-name'),
            ('irc-host', '67890Thisistoolong'),
        ]
        for p in providers:
            with self.subTest(f"Transforming {p[0]}"):
                self.assertEqual(formatter.disp_name(p[0]), p[1])

    def test_replace_quotes_with(self):
        allchars = ''
        for ch in range(32, 255):
            allchars += chr(ch)
        allchars += u'\uff02' # Add a single non-ascii quote to the test
        self.assertEqual(len(allchars), 255-32+1)

        newstr = formatter.replace_quotes_with(allchars, 'x')
        self.assertEqual(newstr.count('x'), 7)
        newstr = formatter.replace_quotes_with(allchars, '')
        self.assertEqual(len(newstr), 218)

