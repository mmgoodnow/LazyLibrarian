#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing functionality in importer.py

import unittest
import unittesthelpers
import warnings
import lazylibrarian
from lazylibrarian import startup
from lazylibrarian.importer import is_valid_authorid, get_preferred_author_name,  \
  add_author_name_to_db, add_author_to_db

class LibrarySyncTest(unittest.TestCase):
    bookapi = None
 
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        # Run startup code without command line arguments and no forced sleep
        warnings.simplefilter("ignore", ResourceWarning)
        options = startup.startup_parsecommandline(__file__, args = [''], seconds_to_sleep = 0)
        unittesthelpers.disableHTTPSWarnings()
        startup.init_logs()
        startup.init_config()
        startup.init_caches()
        startup.init_database()
        unittesthelpers.prepareTestDB()
        startup.init_build_debug_header(online = False)
        startup.init_build_lists()
        cls.bookapi = lazylibrarian.CONFIG['BOOK_API']
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        lazylibrarian.CONFIG['BOOK_API'] = cls.bookapi
        startup.shutdown(restart=False, update=False, exit=False, testing=True)
        unittesthelpers.removetestDB()
        unittesthelpers.removetestCache()
        unittesthelpers.clearGlobals()
        return super().tearDownClass()

    def test_is_valid_authorid_InvalidIDs(self):
        # Test blank/empty/non-string IDs
        self.assertEqual(is_valid_authorid(None), False)
        self.assertEqual(is_valid_authorid(0), False)
        self.assertEqual(is_valid_authorid(''), False)
        self.assertEqual(is_valid_authorid(10), False)

    def test_is_valid_authorid_GoogleBooks(self):
        # Test potentially valid Google Books IDs
        lazylibrarian.CONFIG['BOOK_API'] = 'GoogleBooks'
        self.assertEqual(is_valid_authorid('123'), True)
        self.assertEqual(is_valid_authorid('OLrandomA'), True)

    def test_is_valid_authorid_Goodreads(self):
        # Test potentially valid Goodreads Books IDs
        lazylibrarian.CONFIG['BOOK_API'] = 'GoodReads'
        self.assertEqual(is_valid_authorid('123'), True)
        self.assertEqual(is_valid_authorid('OLrandomA'), False)

    def test_is_valid_authorid_OpenLibrary(self):
        # Test potentially valid Goodreads Books IDs
        lazylibrarian.CONFIG['BOOK_API'] = 'OpenLibrary'
        self.assertEqual(is_valid_authorid('123'), False)
        self.assertEqual(is_valid_authorid('OLrandomA'), True)


    def test_get_preferred_author_name_NotInDB(self):
        testname = 'Allan Mertner'
        name, found = get_preferred_author_name(testname)
        self.assertEqual(name, testname)
        self.assertEqual(found, False)

        longertestname = testname + ' & Someone Else'
        name, found = get_preferred_author_name(longertestname)
        self.assertEqual(name, testname)
        self.assertEqual(found, False)


    def test_add_author_name_to_db_UnknownPerson(self):
        testname = 'Mr Allan Mertner The Tester'
        authorname, authorid, new = add_author_name_to_db(author=testname, refresh=False, addbooks=False, reason='Testing', title=False)
        self.assertEqual(new, False)
        self.assertEqual(authorname, '')

    def test_add_author_name_to_db_KnownAuthor_OL(self):
        lazylibrarian.CONFIG['BOOK_API'] = 'OpenLibrary'
        testname = 'Douglas Adams'
        authorname, authorid, new = add_author_name_to_db(author=testname, refresh=False, addbooks=False, reason='Testing', title=False)
        self.assertEqual(new, True)
        self.assertEqual(authorname, testname)
        self.assertEqual(authorid, 'OL272947A')

        # Try re-adding, and see that it's no longer new
        authorname, authorid, new = add_author_name_to_db(author=testname, refresh=False, addbooks=False, reason='Testing', title=False)
        self.assertEqual(new, False)
        self.assertEqual(authorname, testname)
        self.assertEqual(authorid, 'OL272947A')


    def test_add_author_to_db_JustByID(self):
        testid = 'OL2219179A' # Maud D. Davies
        lazylibrarian.CONFIG['BOOK_API'] = 'OpenLibrary'
        id = add_author_to_db(authorname=None, refresh=False, addbooks=False, reason='Testing', authorid=testid)
        self.assertEqual(id, testid)





