#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing functionality in importer.py

import unittest
import unittesthelpers
import warnings
import lazylibrarian
from lazylibrarian import startup, importer


class ImporterTest(unittest.TestCase):
    bookapi = None
 
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        unittesthelpers.testSetUp(all=True)
        cls.bookapi = lazylibrarian.CONFIG['BOOK_API']
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        lazylibrarian.CONFIG['BOOK_API'] = cls.bookapi
        unittesthelpers.testTearDown()
        return super().tearDownClass()

    def test_is_valid_authorid_InvalidIDs(self):
        # Test blank/empty/non-string IDs
        self.assertEqual(importer.is_valid_authorid(None), False)
        self.assertEqual(importer.is_valid_authorid(0), False)
        self.assertEqual(importer.is_valid_authorid(''), False)
        self.assertEqual(importer.is_valid_authorid(10), False)

    def test_is_valid_authorid_GoogleBooks(self):
        # Test potentially valid Google Books IDs
        lazylibrarian.CONFIG['BOOK_API'] = 'GoogleBooks'
        self.assertEqual(importer.is_valid_authorid('123'), True)
        self.assertEqual(importer.is_valid_authorid('OLrandomA'), True)

    def test_is_valid_authorid_Goodreads(self):
        # Test potentially valid Goodreads Books IDs
        lazylibrarian.CONFIG['BOOK_API'] = 'GoodReads'
        self.assertEqual(importer.is_valid_authorid('123'), True)
        self.assertEqual(importer.is_valid_authorid('OLrandomA'), False)

    def test_is_valid_authorid_OpenLibrary(self):
        # Test potentially valid Goodreads Books IDs
        lazylibrarian.CONFIG['BOOK_API'] = 'OpenLibrary'
        self.assertEqual(importer.is_valid_authorid('123'), False)
        self.assertEqual(importer.is_valid_authorid('OLrandomA'), True)


    def test_get_preferred_author_name_NotInDB(self):
        testname = 'Allan Mertner'
        name, found = importer.get_preferred_author_name(testname)
        self.assertEqual(name, testname)
        self.assertEqual(found, False)

        longertestname = testname + ' & Someone Else'
        name, found = importer.get_preferred_author_name(longertestname)
        self.assertEqual(name, testname)
        self.assertEqual(found, False)


    def test_add_author_name_to_db_UnknownPerson(self):
        testname = 'Mr Allan Mertner The Tester'
        authorname, authorid, new = importer.add_author_name_to_db(
            author=testname, refresh=False, addbooks=False, reason='Testing', title=False)
        self.assertEqual(new, False)
        self.assertEqual(authorname, '')

    def test_add_author_name_to_db_KnownAuthor_OL(self):
        lazylibrarian.CONFIG['BOOK_API'] = 'OpenLibrary'
        testname = 'Douglas Adams'
        authorname, authorid, new = importer.add_author_name_to_db(
            author=testname, refresh=False, addbooks=False, reason='Testing', title=False)
        self.assertEqual(new, True)
        self.assertEqual(authorname, testname)
        self.assertEqual(authorid, 'OL272947A')

        # Try re-adding, and see that it's no longer new
        authorname, authorid, new = importer.add_author_name_to_db(
            author=testname, refresh=False, addbooks=False, reason='Testing', title=False)
        self.assertEqual(new, False)
        self.assertEqual(authorname, testname)
        self.assertEqual(authorid, 'OL272947A')


    def test_add_author_to_db_JustByID(self):
        testid = 'OL2219179A' # Maud D. Davies
        lazylibrarian.CONFIG['BOOK_API'] = 'OpenLibrary'
        id = importer.add_author_to_db(
            authorname=None, refresh=False, addbooks=False, reason='Testing', authorid=testid)
        self.assertEqual(id, testid)

    def test_search_for(self):
        # Need to find a good way to test this
        #s = importer.search_for("Douglas Adams")
        #print(s)
        pass




