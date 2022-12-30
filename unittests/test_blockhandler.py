#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in blockhandler.py

import time

from unittests.unittesthelpers import LLTestCase
from lazylibrarian import logger
from lazylibrarian.config2 import CONFIG, LLConfigHandler
from lazylibrarian.configarray import ArrayConfig
from lazylibrarian.blockhandler import BLOCKHANDLER, BlockHandler


class BlockhandlerTest(LLTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logger.RotatingLogger.SHOW_LINE_NO = False  # type: ignore # Hack used to make tests more robust
        super().setDoAll(False)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        BLOCKHANDLER.clear_all()
        return super().tearDownClass()

    def setUp(self):
        BLOCKHANDLER.clear_all()  # Clear before each test

    def test_set_config(self):
        # Create a new, emppty handler for this
        handler = BlockHandler()
        self.assertIsNone(handler._config)
        self.assertIsNone(handler._newznab)
        self.assertIsNone(handler._torznab)
        handler.set_config(CONFIG, CONFIG.providers("NEWZNAB"), CONFIG.providers("TORZNAB"))
        self.assertIsInstance(handler._config, LLConfigHandler)
        self.assertIsInstance(handler._newznab, ArrayConfig)
        self.assertIsInstance(handler._torznab, ArrayConfig)
        self.assertNotEqual(handler._torznab, handler._newznab)

    def test_gb_call(self):
        logger.lazylibrarian_log.update_loglevel(1)
        msg, ok = BLOCKHANDLER.add_gb_call()
        self.assertTrue(ok, 'Nothing should be blocked')
        self.assertEqual(msg, 'Ok')

        calls = BLOCKHANDLER.get_gb_calls()
        self.assertEqual(calls, 1, 'Count of gb calls not correct')

        BLOCKHANDLER.block_provider('googleapis', 'bored with testing', delay=2)  # 2 seconds!
        msg, ok = BLOCKHANDLER.add_gb_call()
        self.assertFalse(ok, 'This API should be blocked')
        self.assertEqual(msg, 'Blocked')

        time.sleep(2.1)
        msg, ok = BLOCKHANDLER.add_gb_call()
        self.assertTrue(ok, 'Nothing should be blocked')
        self.assertEqual(msg, 'Ok')
        calls = BLOCKHANDLER.get_gb_calls()
        self.assertEqual(calls, 0, 'Count of gb calls should have been reset')

    def test_remove_provider_entry(self):
        name = 'test'
        with self.assertLogs('lazylibrarian.logger', 'DEBUG'):
            _ = BLOCKHANDLER.block_provider(name, 'blocked', delay=120)
        self.assertTrue(BLOCKHANDLER.is_blocked(name), 'Expected this to be blocked')
        self.assertEqual(BLOCKHANDLER.number_blocked(), 1)

        BLOCKHANDLER.remove_provider_entry(name)
        self.assertFalse(BLOCKHANDLER.is_blocked(name))
        self.assertEqual(BLOCKHANDLER.number_blocked(), 0)

    def test_block_provider(self):
        logger.lazylibrarian_log.update_loglevel(2)
        name = 'someone'
        with self.assertLogs('lazylibrarian.logger', 'DEBUG') as cm:
            delay = BLOCKHANDLER.block_provider(name, 'just because', delay=120)  # Block for 2 minutes
        self.assertListEqual(cm.output, [
            'INFO:lazylibrarian.logger:MainThread : blockhandler.py:block_provider : Blocking provider someone for 2 minutes because just because',
            'DEBUG:lazylibrarian.logger:MainThread : blockhandler.py:block_provider : Provider Blocklist contains 1 entry'
        ])
        self.assertTrue(BLOCKHANDLER.is_blocked(name), 'Expected this to be blocked')
        self.assertEqual(delay, 120, 'Timeout value is unexpected')

        # Repeat the same block with a shorter time
        newdelay = 2
        with self.assertLogs('lazylibrarian.logger', 'DEBUG'):
            delay = BLOCKHANDLER.block_provider('someone', 'just because', delay=newdelay)
        self.assertListEqual(cm.output, [
            'INFO:lazylibrarian.logger:MainThread : blockhandler.py:block_provider : Blocking provider someone for 2 minutes because just because',
            'DEBUG:lazylibrarian.logger:MainThread : blockhandler.py:block_provider : Provider Blocklist contains 1 entry'
        ])
        self.assertTrue(BLOCKHANDLER.is_blocked(name), 'Expected this to be blocked')
        self.assertEqual(delay, newdelay, 'Timeout value is unexpected')
        self.assertEqual(BLOCKHANDLER.number_blocked(), 1, 'Should be exactly one blocked')

        # Let the block be expired
        time.sleep(newdelay + 0.1)
        self.assertFalse(BLOCKHANDLER.is_blocked(name), 'Expected this to be blocked')
        self.assertEqual(BLOCKHANDLER.number_blocked(), 0, 'There should be no more blocks')

        logger.lazylibrarian_log.update_loglevel(2)
        # Add a 0-time block, which will be ignored
        delay = BLOCKHANDLER.block_provider('someone', 'just because', delay=0)
        self.assertEqual(delay, 0, 'Timeout value is unexpected')
        self.assertEqual(BLOCKHANDLER.number_blocked(), 0, 'Should not have added a block')

        # Add a block where the reason will be truncated to 80 chars
        _ = BLOCKHANDLER.block_provider('short', 'X' * 200)
        lines = BLOCKHANDLER.get_text_list_of_blocks().splitlines()
        self.assertListEqual(lines, [
            f"short blocked for 1 hours: {'X' * 80}",
        ])

    def test_number_blocked(self):
        self.assertEqual(BLOCKHANDLER.number_blocked(), 0, 'There should be no blocks by default')

    def test_clear_all(self):
        self.assertEqual(BLOCKHANDLER.number_blocked(), 0)
        BLOCKHANDLER.block_provider('test', 'no reason', )
        self.assertEqual(BLOCKHANDLER.number_blocked(), 1)
        BLOCKHANDLER.clear_all()
        self.assertEqual(BLOCKHANDLER.number_blocked(), 0)

    def test_check_day(self):
        """ Testing check_day requires simulating a day change """
        change = BLOCKHANDLER.check_day(pretend_day='12345678')
        self.assertTrue(change, 'Expected day to change')

        # Make up some pretend API calls
        for provider in CONFIG.providers('NEWZNAB'):
            provider.set_int('APICOUNT', 10)
        for provider in CONFIG.providers('TORZNAB'):
            provider.set_int('APICOUNT', 100)
        BLOCKHANDLER.check_day(pretend_day='12345678')
        for provider in CONFIG.providers('NEWZNAB'):
            self.assertEqual(provider.get_int('APICOUNT'), 10)
        for provider in CONFIG.providers('TORZNAB'):
            self.assertEqual(provider.get_int('APICOUNT'), 100)

        BLOCKHANDLER.check_day(pretend_day='12345700')
        for provider in CONFIG.providers('NEWZNAB'):
            self.assertEqual(provider.get_int('APICOUNT'), 0)
        for provider in CONFIG.providers('TORZNAB'):
            self.assertEqual(provider.get_int('APICOUNT'), 0)

    def test_get_text_list_of_blocks(self):
        txt = BLOCKHANDLER.get_text_list_of_blocks()
        self.assertEqual(txt, 'No blocked providers')

        BLOCKHANDLER.block_provider('first', 'abc', 100)
        BLOCKHANDLER.block_provider('second', 'hello', 10000)
        BLOCKHANDLER.block_provider('googleapi', 'xyz')
        txt = BLOCKHANDLER.get_text_list_of_blocks()
        lines = txt.splitlines()
        self.assertListEqual(lines, [
            'first blocked for 1 minutes, 40 seconds: abc',
            'second blocked for 2 hours, 46 minutes, 40 seconds: hello',
            'googleapi blocked for 1 hours: xyz',
        ])
