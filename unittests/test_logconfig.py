#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the logconfig module.
# Constraints:
#   Do not use the standard unittesthelper; this needs to run standalone

import unittest
from typing import List

from lazylibrarian import logconfig

import logging


class TestLogConfig(unittest.TestCase):
    """ Test the logconfig.py class """

    def setUp(self) -> None:
        # Make sure the default root logger is enabled and at INFO level
        root = logconfig.enable_logger('root', True)
        root.setLevel(logging.INFO)

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

        logconfig.read_log_config('./unittests/testdata/loggingtest.yaml', fixfilenames=False)
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

        logconfig.read_log_config('./unittests/testdata/loggingtest.yaml', fixfilenames=False)
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
        logconfig.read_log_config('unittests/testdata/loggingtest.yaml', fixfilenames=False)

        logger = logconfig.enable_logger('special.configread', True)
        try:
            self.assertTrue(logger.isEnabledFor(logging.INFO))
            logger = logconfig.enable_logger('special.configread', False)
            self.assertFalse(logger.isEnabledFor(logging.INFO))
        finally:
            logconfig.enable_logger('special.configread', True)

        # Get some random logger, which defaults to the root logger
        logger = logconfig.enable_logger('some_module', True)
        self.assertTrue(logger.isEnabledFor(logging.ERROR))

        # Try disabling the root logger
        logger = logconfig.enable_logger('root', False)
        self.assertFalse(logger.isEnabledFor(logging.ERROR))

    def test_saving_log_config(self):
        logcfg = logconfig.read_log_config('unittests/testdata/loggingtest.yaml', fixfilenames=False)
