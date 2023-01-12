# Basic use test for iCrawler, based on
# https://github.com/hellock/icrawler/blob/master/tests/test_todo.py

import logging
import os
import os.path as osp
import shutil
import tempfile
import unittest

from lib.icrawler.builtin import (BaiduImageCrawler, BingImageCrawler,
                              GoogleImageCrawler, GreedyImageCrawler,
                              UrlListCrawler)


class TestICrawler(unittest.TestCase):
    logger = logging.getLogger()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.test_dir = tempfile.mkdtemp()
        cls.logger.setLevel(logging.DEBUG)

    def test_google(self):
        img_dir = osp.join(self.test_dir, 'google')
        google_crawler = GoogleImageCrawler(
            downloader_threads=2,
            storage={'root_dir': img_dir},
            log_level=logging.INFO)
        search_filters = dict(
            size='large')
        google_crawler.crawl('cat', filters=search_filters, max_num=5)
        self.assertTrue(os.path.exists(self.test_dir), f'Expect {self.test_dir} to exist')
        self.assertTrue(os.path.exists(img_dir), f'Expect {img_dir} to exist')
        res = len(os.listdir(img_dir))
        self.assertGreater(res, 0, f'Expected Google to have a cat result!')
        shutil.rmtree(img_dir)

    def test_bing(self):
        img_dir = osp.join(self.test_dir, 'bing')
        bing_crawler = BingImageCrawler(
            downloader_threads=2,
            storage={'root_dir': img_dir},
            log_level=logging.INFO)
        search_filters = dict(
            type='photo',
            size='large')
        bing_crawler.crawl('cat', max_num=5, filters=search_filters)
        self.assertTrue(os.path.exists(self.test_dir), f'Expect {self.test_dir} to exist')
        self.assertTrue(os.path.exists(img_dir), f'Expect {img_dir} to exist')
        res = len(os.listdir(img_dir))
        self.assertGreater(res, 0, f'Expected Bing to have a cat result!')
        shutil.rmtree(img_dir)

    def test_greedy(self):
        img_dir = osp.join(self.test_dir, 'greedy')
        greedy_crawler = GreedyImageCrawler(
            parser_threads=2, storage={'root_dir': img_dir})
        greedy_crawler.crawl(
            'https://www.bbc.com/news', max_num=5, min_size=(100, 100))
        self.assertTrue(os.path.exists(self.test_dir), f'Expect {self.test_dir} to exist')
        self.assertTrue(os.path.exists(img_dir), f'Expect {img_dir} to exist')
        res = len(os.listdir(img_dir))
        self.assertGreater(res, 0, f'Expected Greedy to have a cat result!')
        shutil.rmtree(img_dir)
