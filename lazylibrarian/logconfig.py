#  This file is part of Lazylibrarian.
#
# Purpose:
#    Handles logging configuration, stored in logging.yaml but with some
#    overrides from config.ini

import logging
import logging.config
import logging.handlers
from typing import Dict, List

import yaml


class RecentMemoryHandler(logging.handlers.MemoryHandler):
    """ Memory handler that only empties if it's asked to; flushing simply
    makes sure it doesn't go beyond capacity """

    def __init__(self, capacity):
        super().__init__(capacity)
        self.name = 'LazyLibrarian In-Memory Log'

    def flush(self):
        self.acquire()
        try:
            while len(self.buffer) > self.capacity:
                del self.buffer[0]  # Just delete the last entry
        finally:
            self.release()

    def clear(self):
        self.acquire()
        try:
            self.buffer.clear()
        finally:
            self.release()


class LogConfig:
    """ Central log configuration for LazyLibrarian. Mainly to hold the in-memory
    log handler necessary to display the log in the UI. """

    def __init__(self):
        self._memorybuffer = None
        self.ensure_memoryhandler_for_ui(100)

    def read_log_config(self, yamlname='logging.yaml') -> Dict:
        with open(yamlname, "r") as stream:
            try:
                logsettings = yaml.safe_load(stream)
                logging.config.dictConfig(logsettings)
                self.ensure_memoryhandler_for_ui()
                return logsettings
            except yaml.YAMLError as exc:
                print(f"YAML error reading logging config: {str(exc)}")
            except Exception as e:
                raise RuntimeError(f"Error reading logging config, exiting: {str(e)}")

    def ensure_memoryhandler_for_ui(self, capacity_lines: int = 100):
        """ Ensure there is a memory handler for displaying the log in the UI """
        logger = logging.getLogger('root')
        if self._memorybuffer:
            self._memorybuffer.capacity = capacity_lines
        else:
            self._memorybuffer = RecentMemoryHandler(capacity=capacity_lines)

        # Make sure it's part of the root logger
        logger.addHandler(self._memorybuffer)

    def get_ui_loghandler(self) -> RecentMemoryHandler:
        return self._memorybuffer

    def clear_ui_log(self):
        self._memorybuffer.clear()

    def get_ui_logrows(self, filterstr: str = '', redactlist=None) -> (List, int):
        """ Return log rows to show in the UI, filtered by lowercase(filter), as
         well as the total number of items that could be displayed """
        if redactlist is None:
            redactlist = []
        filterstr = filterstr.lower() if filterstr else ''

        rows = []
        handler = self.get_ui_loghandler()
        for logrec in handler.buffer:
            if hasattr(logrec, 'message'):  # If not, it's a sign it was added to the log without a formatter
                redacted = logrec.message
                if redactlist:
                    for item in redactlist:
                        if item in redacted:
                            redacted = redacted.replace(item, '[redacted]')
                # Timestamp, level, thread, file, method, line, message
                line = [logrec.asctime, logrec.levelname, logrec.threadName, logrec.filename, logrec.funcName,
                        logrec.lineno, redacted]
                if not filterstr or filterstr in str(line).lower():
                    rows.append(line)
        return rows, len(handler.buffer)

    @staticmethod
    def change_memory_limit(limitstr: str):
        """ Method used as onchange event for LOGLIMIT """
        LOGCONFIG.ensure_memoryhandler_for_ui(capacity_lines=int(limitstr))

    @staticmethod
    def change_root_loglevel(value: str):
        """ Onchange event for LOGLEVEL """
        logger = logging.getLogger('root')
        logger.setLevel(int(value))

    @staticmethod
    def enable_logger(logname: str, enabled: bool = True) -> logging.Logger:
        """ Enable/disable the logger named logname, return the logger """
        logger = logging.getLogger(logname)
        # Use the disabled property of the logger, though the logic is reversed
        logger.disabled = not enabled
        return logger

    @staticmethod
    def get_loglevel(logname: str = 'root') -> int:
        return logging.getLogger(logname).getEffectiveLevel()

    @staticmethod
    def get_loglevel_name(logname: str = 'root') -> str:
        return logging.getLevelName(LogConfig.get_loglevel(logname))

    @staticmethod
    def set_loglevel(level: int = logging.INFO, logname: str = 'root') -> int:
        """ Set the log level for the logger, return the effective log level """
        logger = logging.getLogger(logname)
        logger.setLevel(level)
        return logger.getEffectiveLevel()

    @staticmethod
    def get_special_logger_list() -> List:
        """ Get the list of special loggers and their current state """
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith('special.')]
        return loggers

    @staticmethod
    def is_logger_enabled_for(name: str, level: int = logging.DEBUG) -> bool:
        logger = logging.getLogger(name)
        return logger.isEnabledFor(level)

    @staticmethod
    def is_special_logger_enabled(shortname: str) -> bool:
        return LogConfig.is_logger_enabled_for(f'special.{shortname}', logging.DEBUG)

    @staticmethod
    def enable_special_logger(shortname: str, enabled: bool) -> logging.Logger:
        logger = LogConfig.enable_logger(f'special.{shortname}', enabled)
        if enabled:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        return logger

    @staticmethod
    def get_short_special_logger_name(fullname: str) -> str:
        """ Return the name of the logger without 'special.' at the beginning """
        return fullname[8:]


LOGCONFIG = LogConfig()
