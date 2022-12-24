#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the startup sequence

import unittesthelpers
import lazylibrarian
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.filesystem import DIRS

class SetupTest(unittesthelpers.LLTestCase):

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

    def assertEndsWith(self, teststr, end):
        self.assertEqual(teststr[-len(end):],end)

    # Test global functions declared in __init__.py
    # They should probably move somewhere else at some point.
    def test_directory(self):
        # Test the directory() function
        # The directories should all have values from unittest/testdata/config-defaults.ini, and differ from the default
        bookdir = lazylibrarian.directory("eBook")
        self.assertNotEqual(bookdir, DIRS.DATADIR, "BookDir and Datadir cannot be the same")
        self.assertEndsWith(bookdir, "eBooks")

        audiobookdir = lazylibrarian.directory("AudioBook")
        audiodir = lazylibrarian.directory("Audio")
        self.assertEqual(audiobookdir, audiodir)
        self.assertNotEqual(audiobookdir, DIRS.DATADIR)
        self.assertEndsWith(audiobookdir, "Audiobooks")

        downloaddir = lazylibrarian.directory("Download")
        self.assertNotEqual(downloaddir, DIRS.DATADIR)
        self.assertEndsWith(downloaddir, "Downloads")

        altdir = lazylibrarian.directory("Alternate")
        self.assertNotEqual(altdir, DIRS.DATADIR)
        self.assertEndsWith(altdir, "Alternative")

        faultydir = lazylibrarian.directory("This is invalid")
        self.assertEqual(faultydir, "")

    def test_wishlist_type(self):
        providers = [
            ('https://www.goodreads.com/review/list_rss/userid','goodreads'),
            ('https://www.goodreads.com/list/show/143500.Best_Books_of_the_Decade_2020_s', 'listopia'),
            ('https://www.goodreads.com/book/show/title', 'listopia'),
            ('https://www.amazon.co.uk/charts', 'amazon'),
            ('https://www.nytimes.com/books/best-sellers/', 'ny_times'),
            ('https://best-books.publishersweekly.com/pw/best-books/2022/top-10', 'publishersweekly'),
            ('https://apps.npr.org/best-books/#year=2022', 'apps.npr.org'),
            ('https://www.penguinrandomhouse.com/books/all-best-sellers', 'penguinrandomhouse'),
            ('https://www.barnesandnoble.com/b/books/_/N-1fZ29Z8q8', 'barnesandnoble'),
            ('https://somewhere-else.com/', '')
        ]
        for p in providers:
            self.assertEqual(lazylibrarian.wishlist_type(p[0]), p[1])

    def test_use_rss(self):
        self.assertFalse(lazylibrarian.use_rss())

    def test_use_wishlist(self):
        self.assertFalse(lazylibrarian.use_wishlist())

    def test_use_irc(self):
        self.assertFalse(lazylibrarian.use_irc())

    def test_use_nzb(self):
        self.assertFalse(lazylibrarian.use_nzb())

    def test_use_tor(self):
        self.assertFalse(lazylibrarian.use_tor())

    def test_use_direct(self):
        self.assertFalse(lazylibrarian.use_direct())

