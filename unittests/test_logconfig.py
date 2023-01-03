#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the logconfig module.
# Constraints:
#   Do not use the standard unittesthelper; this needs to run standalone

import unittest
from typing import List
import yaml

from lazylibrarian import logconfig

import logging
import logging.handlers


class CaptureLogs:
    def __init__(self, uselogger: logging.Logger, level: int):
        self.logger = uselogger
        self.level = level

    def __enter__(self):
        self.original_handlers = self.logger.handlers
        self.logger.handlers = []

        # Create a MemoryHandler and add it to the logger
        self.memory_handler = logging.handlers.MemoryHandler(1024*1024)
        self.memory_handler.setLevel(self.level)
        self.logger.addHandler(self.memory_handler)

        return self.memory_handler.buffer

    def __exit__(self, *exc_info):
        self.logger.handlers = self.original_handlers
        self.memory_handler.close()


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

        logconfig.read_log_config('unittests/testdata/loggingtest.yaml', fixfilenames=False)
        exercise_logger('root', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 3)
        exercise_logger('unittest', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 4)
        exercise_logger('special', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 4)
        exercise_logger('special.configread', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 4)
        exercise_logger('special.configwrite', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 4)
        exercise_logger('special.dbcomms.dbtiming', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], 4)

    def test_setting_log_level(self):
        def test_enabled(logname: str, yes: List[int], no: List[int]) -> logging.Logger:
            """ Look up a logger, emit some messages, see if they appear as expected """
            testlogger = logging.getLogger(logname)
            self.assertIsNotNone(testlogger)
            for level in yes:
                self.assertTrue(testlogger.isEnabledFor(level))
            for level in no:
                self.assertFalse(testlogger.isEnabledFor(level))
            return testlogger

        logconfig.read_log_config('unittests/testdata/loggingtest.yaml', fixfilenames=False)
        # Test that only DEBUG messages are filtered by default
        logger = test_enabled('root', [logging.INFO, logging.WARNING, logging.ERROR], [logging.DEBUG])
        # Test that setting ERRORs+ filter works
        logger.setLevel(logging.ERROR)
        test_enabled('root', [logging.ERROR], [logging.DEBUG, logging.INFO, logging.WARNING])
        logger.setLevel(logging.INFO)

        # Test that the special.configread logger logs everything by default
        logger = test_enabled('special.configread', [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], [])
        # Test that the special.configread logger log level can be changed, and that it didn't affect the root logger
        logger.setLevel(logging.ERROR)
        test_enabled('special.configread', [logging.ERROR], [logging.DEBUG, logging.INFO, logging.WARNING])
        test_enabled('root', [logging.INFO, logging.WARNING, logging.ERROR], [logging.DEBUG])

    def test_disabling_loggers(self):
        """ Test disabling and enabling loggers """
        logconfig.read_log_config('unittests/testdata/loggingtest.yaml', fixfilenames=False)

        logger = logconfig.enable_logger('special.configread', True)
        self.assertTrue(logger.isEnabledFor(logging.DEBUG))
        logger = logconfig.enable_logger('special.configread', False)
        self.assertFalse(logger.isEnabledFor(logging.DEBUG))

        # Get some random logger, which defaults to the root logger
        logger = logconfig.enable_logger('some_module', True)
        self.assertTrue(logger.isEnabledFor(logging.ERROR))

        # Try disabling the root logger
        logger = logconfig.enable_logger('root', False)
        self.assertFalse(logger.isEnabledFor(logging.ERROR))

    def test_saving_log_config(self):
        logcfg = logconfig.read_log_config('unittests/testdata/loggingtest.yaml', fixfilenames=False)
