#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the logconfig module.
# Constraints:
#   Do not use the standard unittesthelper; this needs to run standalone

import logging
import unittest
from typing import List

from lazylibrarian.logconfig import LOGCONFIG


class TestLogConfig(unittest.TestCase):
    """ Test the logconfig.py class """

    def setUp(self) -> None:
        # For each test, clear the old config and read a fresh one
        LOGCONFIG.read_log_config('./unittests/testdata/loggingtest.yaml')
        LOGCONFIG.clear_ui_log()
        root = logging.getLogger('root')
        root.disabled = False  # Sometimes, logging sets it to disabled after loading. Hmm.

    def test_read_log_config(self):
        """ Very basic test: Just load the config and validate it loaded """

        def exercise_logger(logname: str, loglevels: List[int], num: int):
            """ Look up a logger, emit some messages, see if they appear as expected """
            logger = logging.getLogger(logname)
            self.assertIsNotNone(logger)
            with self.assertLogs(logger, logger.level) as logrecs:
                for level in loglevels:
                    logger.log(level, f'Test level {level} for logger {logname}')
            self.assertEqual(num, len(logrecs.output), f'Unexpected result for logger {logname}')

        exercise_logger('root', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 3)
        exercise_logger('unittest', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 3)
        exercise_logger('special', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 4)
        # All of the special loggers default to INFO
        exercise_logger('special.configread', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 3)
        exercise_logger('special.configwrite', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 3)
        exercise_logger('special.dbcomms.dbtiming', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 3)

    def test_setting_log_level(self):
        def test_enabled(logname: str, yes: List[int], no: List[int]) -> logging.Logger:
            """ Look up a logger, emit some messages, see if they appear as expected """
            testlogger = logging.getLogger(logname)
            try:
                self.assertIsNotNone(testlogger)
                for level in yes:
                    self.assertTrue(testlogger.isEnabledFor(level))
                for level in no:
                    self.assertFalse(testlogger.isEnabledFor(level))
            finally:
                return testlogger

        # Test that only DEBUG messages are filtered by default
        logger = test_enabled('root', [logging.INFO, logging.WARNING, logging.ERROR], [logging.DEBUG])
        # Test that setting ERRORs+ filter works
        logger.setLevel(logging.ERROR)
        test_enabled('root', [logging.ERROR], [logging.DEBUG, logging.INFO, logging.WARNING])
        logger.setLevel(logging.INFO)

        # Test that the special.configread logger logs everything but DEBUG by default
        logger = test_enabled('special.configread', [logging.INFO, logging.WARNING, logging.ERROR], [logging.DEBUG])
        # Test that the special.configread logger log level can be changed, and that it didn't affect the root logger
        logger.setLevel(logging.ERROR)
        test_enabled('special.configread', [logging.ERROR], [logging.DEBUG, logging.INFO, logging.WARNING])
        logger.setLevel(logging.INFO)  # Restore to default
        test_enabled('root', [logging.INFO, logging.WARNING, logging.ERROR], [logging.DEBUG])

    def test_disabling_loggers(self):
        """ Test disabling and enabling loggers """
        logger = LOGCONFIG.enable_logger('special.configread', True)
        try:
            self.assertTrue(logger.isEnabledFor(logging.INFO))
            logger = LOGCONFIG.enable_logger('special.configread', False)
            self.assertFalse(logger.isEnabledFor(logging.INFO))
        finally:
            LOGCONFIG.enable_logger('special.configread', True)

        # Get some random logger, which defaults to the root logger
        logger = LOGCONFIG.enable_logger('some_module', True)
        self.assertTrue(logger.isEnabledFor(logging.ERROR))

        # Try disabling the root logger
        logger = LOGCONFIG.enable_logger('root', False)
        self.assertFalse(logger.isEnabledFor(logging.ERROR))

    def test_get_loglevel(self):
        self.assertEqual(logging.INFO, LOGCONFIG.get_loglevel('root'))
        self.assertEqual(logging.DEBUG, LOGCONFIG.get_loglevel('special'))
        self.assertEqual(logging.INFO, LOGCONFIG.get_loglevel('special.fuzz'))
        self.assertEqual(logging.ERROR, LOGCONFIG.get_loglevel('cherrypy'))
        self.assertEqual(logging.INFO, LOGCONFIG.get_loglevel(''))

    def test_get_loglevel_name(self):
        self.assertEqual('INFO', LOGCONFIG.get_loglevel_name('root'))
        self.assertEqual('DEBUG', LOGCONFIG.get_loglevel_name('special'))
        self.assertEqual('INFO', LOGCONFIG.get_loglevel_name('special.fuzz'))
        self.assertEqual('ERROR', LOGCONFIG.get_loglevel_name('cherrypy'))
        # Undefined logger gets root level:
        self.assertEqual(logging.INFO, LOGCONFIG.get_loglevel('somelogger-notdefinedyet'))

    def test_set_loglevel(self):
        self.assertEqual(logging.WARNING, LOGCONFIG.set_loglevel(logging.WARNING, 'root'))
        self.assertEqual(logging.DEBUG, LOGCONFIG.set_loglevel(logging.DEBUG, 'special.fuzz'))
        self.assertEqual(logging.WARNING, LOGCONFIG.get_loglevel('root'))
        self.assertEqual(logging.DEBUG, LOGCONFIG.get_loglevel('special.fuzz'))
        # Setting to NOTSET gets the parent level:
        self.assertEqual(logging.WARNING, LOGCONFIG.set_loglevel(logging.NOTSET, 'someotherlogger'))
        self.assertEqual(logging.WARNING, LOGCONFIG.get_loglevel('someotherlogger'))

    def test_is_logger_enabled_for(self):
        self.assertTrue(LOGCONFIG.is_logger_enabled_for('root', logging.INFO))
        self.assertFalse(LOGCONFIG.is_logger_enabled_for('root', logging.DEBUG))

    def test_get_special_logger_list(self):
        loggers = LOGCONFIG.get_special_logger_list()
        self.assertGreater(len(loggers), 10, 'Expect more than 10 special loggers')

    def test_is_special_logger_enabled(self):
        # Test with special.fuzz
        self.assertFalse(LOGCONFIG.is_special_logger_enabled('fuzz'))
        LOGCONFIG.enable_special_logger('fuzz', True)
        self.assertTrue(LOGCONFIG.is_special_logger_enabled('fuzz'))
        LOGCONFIG.enable_special_logger('fuzz', False)
        self.assertFalse(LOGCONFIG.is_special_logger_enabled('fuzz'))

    def test_all_special_loggers(self):
        # The parent special logger is on and at debug level
        self.assertEqual(logging.DEBUG, LOGCONFIG.get_loglevel('special'))
        self.assertTrue(logging.getLogger('special').isEnabledFor(logging.DEBUG))

        loggers = LOGCONFIG.get_special_logger_list()
        for logger in loggers:
            shortname = LOGCONFIG.get_short_special_logger_name(logger.name)
            self.assertFalse(LOGCONFIG.is_special_logger_enabled(shortname),
                             'All special loggers should be disabled by default')
            self.assertFalse(logger.isEnabledFor(logging.DEBUG),
                             'Results should be consistent with is_special_logger_enabled')

            with self.assertLogs(logger.name) as lm:
                self.assertFalse(logger.isEnabledFor(logging.DEBUG))
                self.assertTrue(logger.isEnabledFor(logging.INFO))
                logger.debug('Debug')
                logger.info('Info')
            self.assertEqual(1, len(lm.output), 'Expected 1 Info message')

            # This should now result in messages in the log:
            LOGCONFIG.enable_special_logger(shortname, True)
            self.assertTrue(logger.isEnabledFor(logging.DEBUG))
            with self.assertLogs(logger.name, logging.DEBUG) as lm:
                self.assertTrue(logger.isEnabledFor(logging.DEBUG))
                self.assertTrue(logger.isEnabledFor(logging.INFO))
                logger.debug('Debug')
                logger.info('Info')
            self.assertEqual(2, len(lm.output), 'Expected both debug and info message')

    def test_ensure_memoryhandler_for_ui(self):
        oldhandler = LOGCONFIG.get_ui_loghandler()
        self.assertIsNotNone(oldhandler)
        LOGCONFIG.ensure_memoryhandler_for_ui(capacity_lines=10)
        newhandler = LOGCONFIG.get_ui_loghandler()
        self.assertIsNotNone(newhandler)
        self.assertEqual(newhandler, oldhandler)

    def test_get_ui_logrows(self):
        logger = logging.getLogger()
        self.assertEqual(logging.INFO, logger.getEffectiveLevel())
        logger.debug('Testing debug')
        logger.info('Testing info')
        logger.info('Testing warning')

        rows, total = LOGCONFIG.get_ui_logrows(None)
        self.assertEqual(total, len(rows), 'Expect to return all rows')
        self.assertEqual(2, len(rows), 'Expected 2 rows')

        rows, _ = LOGCONFIG.get_ui_logrows('warn')
        self.assertEqual(1, len(rows), 'Expected 1 filtered row')

        # Make it overrun
        test_capacity = 5
        LOGCONFIG.ensure_memoryhandler_for_ui(capacity_lines=test_capacity)
        for i in range(10):
            logger.info(f"Log {i}")
        rows, _ = LOGCONFIG.get_ui_logrows('')
        lastrow = rows[-1]
        self.assertEqual(lastrow[6], 'Log 9', 'The message is not as expected')
        self.assertEqual(len(rows), test_capacity)

        # Test a redactlist
        redactedrows, _ = LOGCONFIG.get_ui_logrows('', ['9', 'INFO'])
        lastrow = redactedrows[-1]
        self.assertEqual(lastrow[6], 'Log [redacted]', 'The message is not redacted properly')
        self.assertEqual(len(redactedrows), test_capacity)

    def test_special_loggers_to_ui(self):
        logger = LOGCONFIG.enable_special_logger('fuzz', True)
        self.assertEqual(logging.DEBUG, logger.getEffectiveLevel())
        logger.debug('Testing debug')
        logger.debug('Testing info')

        rows, total = LOGCONFIG.get_ui_logrows(None)
        self.assertEqual(total, len(rows), 'Expect to return all rows')
        self.assertEqual(2, len(rows), 'Expected 2 rows')

