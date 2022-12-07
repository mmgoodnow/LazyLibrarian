#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing functionality in librarysync.py

import unittesthelpers
import os
from lazylibrarian import directory, librarysync

class LibrarySyncTest(unittesthelpers.LLTestCase):
 
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(True)
        return super().setUpClass()

    def testGetBookInfo_NoExtension(self):
        # Test reading metadata from book files
        self.assertEqual(librarysync.get_book_info("BookWithNoExtension"), {})

    def getTestBook(self, name):
        return directory("Testdata") + os.path.sep + name

    def testGetBookInfo_Epub(self):
        expected_meta = {'type': 'epub', 'title': 'Test Title', 'creator': 'Bob Builder', 'publisher': 'Testing, Inc', 
            'language': 'en', 'isbn': '9782123456803', 'gb_id': '9876542'}
        bookfile = self.getTestBook('Test Title - Bob Builder.epub')
        self.assertEqual(librarysync.get_book_info(bookfile), expected_meta)

    def testGetBookInfo_Mobi(self):
        bookfile = self.getTestBook('Test Title - Bob Builder.mobi')
        expected_meta = {'type': 'mobi', 'title': 'Test Title', 'creator': 'Bob Builder',  
            'language': 'en', 'isbn': '9782123456803' } 
        self.assertEqual(librarysync.get_book_info(bookfile), expected_meta)

    def testGetBookInfo_Azw3(self):
        bookfile = self.getTestBook('Test Title - Bob Builder.azw3')
        expected_meta = {'type': 'azw3', 'title': 'Test Title', 'creator': 'Bob Builder', 
            'language': 'en', 'isbn': '9782123456803' } 
        self.assertEqual(librarysync.get_book_info(bookfile), expected_meta)

    def testGetBookInfo_Opf(self):
        bookfile = self.getTestBook('metadata.opf')
        expected_meta = {'type': 'opf', 'title': 'Test Title', 'creator': 'Bob Builder', 'publisher': 'Testing, Inc', 
            'language': 'eng', 'isbn': '9782123456803', 'gb_id': '9876542'}
        self.assertEqual(librarysync.get_book_info(bookfile), expected_meta)

    def testLibraryScan(self):
        # Test library_scan, first with an invalid dir:
        self.assertEqual(librarysync.library_scan("Invalid Start Dir"), 0)



