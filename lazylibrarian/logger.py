#  This file is part of Lazylibrarian.
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

import inspect
import logging
import os
from typing import Optional

from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.processcontrol import get_info_on_caller

# Simple rotating log handler that uses RotatingFileHandler
class RotatingLogger(object):
    # Class variables
    __LOGGER_INITIALIZED__ = False
    SHOW_LINE_NO = True
    DEBUG_LOG_LIMIT = 100
    LOGTYPE = ''
    LOGLEVEL_OVERRIDE = False # Remember if LOGLEVEL was overridden so it's not saved
    LOGLEVEL = 1 # LOGLEVEL can differ from CONFIG['LOGLEVEL'] on command line override
    LOGLIST = [] # Used for debug logging

    @classmethod
    def is_initialized(cls):
        return cls.__LOGGER_INITIALIZED__

    def __init__(self, filename: str, config: Optional[ConfigDict]=None):
        self.filename = filename
        self.filehandler = None
        self.consolehandler = None
        self.config = config

    def stop_logger(self):
        lg = logging.getLogger(__name__)
        if self.filehandler:
            lg.removeHandler(self.filehandler)
        if self.consolehandler:
            lg.removeHandler(self.consolehandler)
        self.filehandler = None
        self.consolehandler = None
        RotatingLogger.__LOGGER_INITIALIZED__ = False

    def init_logger(self, config: ConfigDict):
        self.config = config

        lg = logging.getLogger(__name__)
        lg.setLevel(logging.DEBUG)
        self.update_loglevel()
        self.LOGLIST = []

        # Don't import filesystem.DIRS to avoid circular reference
        self.filename = os.path.join(self.config['LOGDIR'], self.filename)
        RotatingLogger.DEBUG_LOG_LIMIT = self.config.get_int('LOGLIMIT') # We must not access the config when logging, so init it here

        if RotatingLogger.__LOGGER_INITIALIZED__:
            return # Do not set handlers again

        # concurrentLogHandler/0.8.7 (to deal with windows locks)
        # since this only happens on windows boxes, if it's nix/mac use the default logger.
        if os.name == 'nt':
            try:
                from lib.concurrent_log_handler import ConcurrentRotatingFileHandler as RotatingFileHandler
                self.LOGTYPE = 'Concurrent'
            except ImportError as e:
                from logging.handlers import RotatingFileHandler
                self.LOGTYPE = 'Rotating (%s)' % e
        else:
            from logging.handlers import RotatingFileHandler
            self.LOGTYPE = 'Rotating'

        filehandler = RotatingFileHandler(
            self.filename,
            maxBytes = self.config.get_int('LOGSIZE'),
            backupCount = self.config.get_int('LOGFILES'))

        filehandler.setLevel(logging.DEBUG)

        fileformatter = logging.Formatter('%(asctime)s - %(levelname)-7s :: %(message)s', '%d-%b-%Y %H:%M:%S')

        filehandler.setFormatter(fileformatter)
        lg.addHandler(filehandler)
        self.filehandler = filehandler

        consolehandler = logging.StreamHandler()
        if self.LOGLEVEL == 1:
            consolehandler.setLevel(logging.INFO)
        if self.LOGLEVEL >= 2:
            consolehandler.setLevel(logging.DEBUG)
        consoleformatter = logging.Formatter('%(asctime)s - %(levelname)s :: %(message)s', '%d-%b-%Y %H:%M:%S')
        consolehandler.setFormatter(consoleformatter)
        lg.addHandler(consolehandler)
        self.consolehandler = consolehandler

        RotatingLogger.__LOGGER_INITIALIZED__ = True

    def update_loglevel(self, override: Optional[int]=None) -> int:
        """ Update the LOGLEVEL. Call this when the config changes during testing.
        Once override has been specified, it no longer follows the CONFIG setting """
        if override:
            self.LOGLEVEL_OVERRIDE = True
            self.LOGLEVEL = override
        elif self.config:
            if not self.LOGLEVEL_OVERRIDE:
                self.LOGLEVEL = self.config.get_int('LOGLEVEL')
        else:
            if not self.LOGLEVEL_OVERRIDE:
                self.LOGLEVEL = 1  # A reasonable default
        return self.LOGLEVEL

    def set_new_loglevel_from_ui(self, loglevel):
        """ Update the LOGLEVEL, *and* update the configuration. Should be used
        when the UI is used to set a new loglevel """
        self.config.set_int('LOGLEVEL', loglevel)
        self.LOGLEVEL_OVERRIDE = False # Even if overridden on command line, update it now
        self.LOGLEVEL = loglevel

    @staticmethod
    def log(message, level):
        from lazylibrarian.formatter import thread_name, unaccented, now

        logger = logging.getLogger(__name__)
        threadname = thread_name()

        # Get the frame data of the method that made the original logger call
        program, method, lineno = get_info_on_caller(depth=2, filenamewithoutext=False)
        if os.name == 'nt':  # windows cp1252 can't handle some accents
            message = unaccented(message)
        else:
            message = message.replace('\x98', '')  # invalid utf-8 eg La mosai\x98que Parsifal

        if lazylibrarian_log:
            if level != 'DEBUG' or lazylibrarian_log.LOGLEVEL >= 2:
                # Limit the size of the "in-memory" log, as gets slow if too long
                lazylibrarian_log.LOGLIST.insert(0, (now(), level, threadname, program, method, lineno, message))
                if len(lazylibrarian_log.LOGLIST) > RotatingLogger.DEBUG_LOG_LIMIT:
                    del lazylibrarian_log.LOGLIST[-1]

        if RotatingLogger.SHOW_LINE_NO:
            message = "%s : %s:%s:%s : %s" % (threadname, program, method, lineno, message)
        else:
            message = "%s : %s:%s : %s" % (threadname, program, method, message)

        if level == 'DEBUG':
            logger.debug(message)
        elif level == 'INFO':
            logger.info(message)
        elif level == 'WARNING':
            logger.warning(message)
        else:
            logger.error(message)

    def debug(self, message):
       self.log(message, level='DEBUG')

    def info(self, message):
        if self.LOGLEVEL > 0:
            self.log(message, level='INFO')

    def warn(self, message):
        self.log(message, level='WARNING')

    def error(self, message):
        self.log(message, level='ERROR')


lazylibrarian_log = RotatingLogger('lazylibrarian.log', config=None)


def debug(message):
    if lazylibrarian_log.LOGLEVEL > 1:
        lazylibrarian_log.log(message, level='DEBUG')


def info(message):
    if lazylibrarian_log.LOGLEVEL > 0:
        lazylibrarian_log.log(message, level='INFO')


def warn(message):
    lazylibrarian_log.log(message, level='WARNING')


def error(message):
    lazylibrarian_log.log(message, level='ERROR')


def logmessage(message, level):
    if level == "DEBUG" and lazylibrarian_log.LOGLEVEL <= 1:
        return

    if level == "INFO" and lazylibrarian_log.LOGLEVEL <= 0:
        return

    lazylibrarian_log.log(message, level)


# extended loglevels
log_matching = 1 << 2  # 4 magazine/comic date/name matching
log_searching = 1 << 3  # 8 extra search logging
log_dlcomms = 1 << 4  # 16 detailed downloader communication
log_dbcomms = 1 << 5  # 32 database comms
log_postprocess = 1 << 6  # 64 detailed postprocessing
log_fuzz = 1 << 7  # 128 fuzzy logic
log_serverside = 1 << 8  # 256 serverside processing
log_fileperms = 1 << 9  # 512 changes to file permissions
log_grsync = 1 << 10  # 1024 detailed goodreads sync
log_cache = 1 << 11  # 2048 cache results
log_libsync = 1 << 12  # 4096 librarysync details
log_admin = 1 << 13  # 8192 admin logging
log_cherrypy = 1 << 14  # 16384 cherrypy logging
log_requests = 1 << 15  # 32768 requests httpclient logging
log_configread = 1 << 16 # log all config2 read requests
log_configwrite = 1 << 17 # log all config2 read requests
log_iterateproviders = 1 << 18 # Log iterating over providers in detail
