#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the images library.
# Note:
#   All of the functions here are really large, and unit testing is not possible.
#   What we have here is just some tests.

import logging
import os
from typing import List

from lazylibrarian.filesystem import DIRS
from lazylibrarian.images import get_book_cover, crawl_image
from unittests.unittesthelpers import LLTestCaseWithStartup


class TestImages(LLTestCaseWithStartup):

    def test_get_book_cover(self):
        self.assertEqual((None, None), get_book_cover(), 'No parameters, expect empty result')

        # sources = ['current', 'cover', 'goodreads', 'librarything', 'openlibrary',
        #            'googleisbn', 'bing', 'googleimage']
        # for source in sources:
        #     covers = get_book_cover('', source)

    def test_crawl_image(self):
        """ Test function only used in get_book_cover """
        crawler_names = ['bing', 'google']
        for crawler in crawler_names:
            img, src = crawl_image(crawler_name=crawler, src=crawler, cachedir=DIRS.TMPDIR, bookid='123',
                                   safeparams='Someone+Is+Trouble')
            self.assertIsNotNone(img, f'Expected an image from {crawler}')
    #
    # def test_createthumb(self):
    #     assert False
    #
    # def test_coverswap(self):
    #     assert False
    #
    # def test_get_author_images(self):
    #     assert False
    #
    # def test_get_book_covers(self):
    #     assert False
    #
    # def test_use_img(self):
    #     assert False
    #
    # def test_get_author_image(self):
    #     assert False
    #
    # def test_create_mag_covers(self):
    #     assert False
    #
    # def test_find_gs(self):
    #     assert False
    #
    # def test_shrink_mag(self):
    #     assert False
    #
    # def test_create_mag_cover(self):
    #     assert False
