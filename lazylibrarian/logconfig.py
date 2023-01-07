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

from lazylibrarian.configenums import OnChangeReason
from lazylibrarian.filesystem import DIRS


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

    DefaultConfig = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s %(levelname)s: %(message)s [%(module)s.py:%(lineno)s (%(threadName)s)]"
            },
            "detail": {
                "format": "%(asctime)s %(levelname)s  %(filename)s.%(funcName)s(): %(message)s (%(threadName)s)"
            },
            "timing": {
                "format": "%(asctime)s %(threadName)s %(levelname)s, %(filename)s.%(funcName)s() (line %(lineno)s): %(message)s"
            },
            "special": {
                "format": "%(asctime)s %(levelname)s: %(message)s [%(module)s.py:%(lineno)s (%(threadName)s/%(name)s)]"
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
            "console_special": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "special",
                "stream": "ext://sys.stdout",
            },
            "info_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "simple",
                "filename": "lazylibrarian-info.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf8",
            },
            "debug_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "detail",
                "filename": "lazylibrarian-debug.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf",
            },
            "debug_timing_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "timing",
                "filename": "lazylibrarian-debug.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf",
            },
            "debug_special_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "special",
                "filename": "lazylibrarian-debug.log",
                "maxBytes": 10485760,
                "backupCount": 5,
                "encoding": "utf",
            },
        },
        "loggers": {
            "special": {
                "level": "DEBUG",
                "handlers": ["console_special", "debug_special_file"],
            },
            "special.admin": {"level": "INFO"},
            "special.cache": {"level": "INFO"},
            "special.configread": {"level": "INFO"},
            "special.configwrite": {"level": "INFO"},
            "special.dbcomms": {"level": "INFO"},
            "special.dlcomms": {"level": "INFO"},
            "special.fileperms": {"level": "INFO"},
            "special.fuzz": {"level": "INFO"},
            "special.grsync": {"level": "INFO"},
            "special.iterateproviders": {"level": "INFO"},
            "special.libsync": {"level": "INFO"},
            "special.matching": {"level": "INFO"},
            "special.postprocess": {"level": "INFO"},
            "special.requests": {"level": "INFO"},
            "special.searching": {"level": "INFO"},
            "special.serverside": {"level": "INFO"},
            "cherrypy": {"level": "ERROR", "propagate": False},
            "unittest": {"level": "INFO", "handlers": ["console"]},
        },
        "root": {"handlers": ["console", "info_file", "debug_file"]},
    }

    StartupLoggerConfig = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s %(levelname)s: %(message)s [%(module)s.py:%(lineno)s (%(threadName)s)]"
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "special": {
                "level": "INFO",
            },
        },
        "root": {"handlers": ["console"]},
    }

    def __init__(self):
        self._memorybuffer = None
        self.ensure_memoryhandler_for_ui()

    def get_default_logconfig(self, console_only: bool):
        return self.StartupLoggerConfig if console_only else self.DefaultConfig

    def get_log_config(self, console_only: bool, usedefault: bool = True, yamlname: str = '') -> Dict:
        """ Return a log configuration, either the default or from a file """
        custom = False
        if not usedefault and yamlname != '':
            with open(yamlname, "r") as stream:
                try:
                    logsettings = yaml.safe_load(stream)
                    custom = True
                except yaml.YAMLError as exc:
                    print(f"YAML error reading logging configfrom {yamlname}, using defaults ({str(exc)})")
                    logsettings = self.DefaultConfig
                except Exception as e:
                    print(f"Error reading logging config from {yamlname}, using defaults ({str(e)})")
                    logsettings = self.DefaultConfig
        if custom:
            return logsettings
        else:
            return self.get_default_logconfig(console_only)

    def initialize_console_only_log(self) -> Dict:
        """ Initialize the console-only log used for startup """
        settings = self.get_log_config(console_only=True)
        logging.config.dictConfig(settings)
        self.ensure_memoryhandler_for_ui(capacity_lines=-1)
        return settings

    def initialize_log_config(self, max_size: int, max_number: int) -> Dict:
        """ Read a new config from yaml file and apply """
        settings = self.get_log_config(console_only=False)
        for name, handler in settings['handlers'].items():
            if 'filename' in handler:
                handler['filename'] = DIRS.get_logfile(handler['filename'])
                handler['maxBytes'] = max_size
                handler['backupCount'] = max_number
        logging.config.dictConfig(settings)
        self.ensure_memoryhandler_for_ui(capacity_lines=-1)
        return settings

    # Methods for dealing with in-memory log for UI display

    def ensure_memoryhandler_for_ui(self, capacity_lines: int = 400):
        """ Ensure there is a memory handler for displaying the log in the UI """
        logger = logging.getLogger('root')
        if self._memorybuffer:
            if capacity_lines > 0:
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
    def change_memory_limit(limitstr: str, reason: OnChangeReason = OnChangeReason.SETTING):
        """ Method used as onchange event for LOGLIMIT """
        if reason != OnChangeReason.COPYING:
            LOGCONFIG.ensure_memoryhandler_for_ui(capacity_lines=int(limitstr))

    # Methods for dealing with normal loggers

    @staticmethod
    def change_root_loglevel(value: str, reason: OnChangeReason = OnChangeReason.SETTING):
        """ Onchange event for LOGLEVEL """
        if reason != OnChangeReason.COPYING:
            logger = logging.getLogger('root')
            levelmap = logging.getLevelNamesMapping()
            if value.upper() in levelmap:
                level = levelmap[value.upper()]
            else:
                level = int(value)
            logger.setLevel(level)

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
    def is_logger_enabled_for(name: str, level: int = logging.DEBUG) -> bool:
        logger = logging.getLogger(name)
        return logger.isEnabledFor(level)

    @staticmethod
    def remove_console_handlers_from_logger(name: str) -> int:
        """ Remove all handlers named console* from the logger named name.
        Returns number of handlers removed. Not reversible without reloading config. """
        logger = logging.getLogger(name)
        count = 0
        for handler in logger.handlers:
            if handler.get_name().startswith('console'):
                logger.removeHandler(handler)
                count += 1
        return count

    @staticmethod
    def remove_console_handlers() -> int:
        """ Called on --quiet, to make sure all LL handlers named console* are removed """
        # The only predefined ones are in root and special:
        removed = LogConfig.remove_console_handlers_from_logger('root') + \
            LogConfig.remove_console_handlers_from_logger('special')
        return removed

    # Methods for dealing with special loggers

    @staticmethod
    def get_special_logger_list() -> List:
        """ Get the list of special loggers and their current state """
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict if name.startswith('special.')]
        return loggers

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

    # Other methods for log management

    def delete_log_files(self) -> str:
        """ Delete on-disc log files, return status string """
        return 'Not yet implemented'

        # * Look up all File-based loggers
        # * For each, close the logger and delete the file
        # * Then restart the loggers
        error = False

        # TODO P1: clear_log() needs some thought
        logger = logging.getLogger(__name__)
        for f in glob.glob(CONFIG['LOGDIR'] + "/*.log*"):
            try:
                os.remove(syspath(f))
            except OSError as err:
                error = err.strerror
                logger.debug("Failed to remove %s : %s" % (f, error))

        if error:
            return 'Failed to clear logfiles: %s' % error
        else:
            return f"{deleted} log files deleted from {CONFIG['LOGDIR']}"


LOGCONFIG = LogConfig()
