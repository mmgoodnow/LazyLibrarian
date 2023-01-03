#  This file is part of Lazylibrarian.
#
# Purpose:
#   Hold helper functions only needed for testing

import unittest
from collections import Counter
from os import remove
from shutil import rmtree
from typing import List
import logging

import lazylibrarian
from lazylibrarian import dbupgrade, startup, config2
from lazylibrarian.configtypes import Access
from lazylibrarian.filesystem import DIRS, path_isdir


# noinspection PyBroadException
class LLTestCase(unittest.TestCase):
    ALLSETUP = None
    CONFIGFILE = './unittests/testdata/testconfig-defaults.ini'
    starter = startup.StartupLazyLibrarian()
    logger = logging.getLogger('unittest')  # For logging unittest

    @classmethod
    def setUpClass(cls) -> None:
        options, configfile = cls.starter.startup_parsecommandline(__file__, args=[''], testing=True)
        cls.starter.load_config(cls.CONFIGFILE, options)
        cls.starter.init_logs()
        # Only log errors during the rest of startup
        logging.getLogger('root').setLevel(logging.ERROR)
        cls.starter.init_misc(config2.CONFIG)
        if cls.ALLSETUP:
            LLTestCase.disableHTTPSWarnings()
            cls.starter.init_caches(config2.CONFIG)
            cls.starter.init_database(config2.CONFIG)
            cls.prepareTestDB()
        cls.starter.init_build_lists(config2.CONFIG)
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        config2.CONFIG.create_access_summary()

        cls.starter.shutdown(restart=False, update=False, exit=False, testing=True)
        if cls.ALLSETUP:
            cls.removetestDB()
            cls.removetestCache()
            cls.ALLSETUP = None
        cls.delete_test_logs()
        cls.clearGlobals()
        cls.CONFIGFILE = './unittests/testdata/testconfig-defaults.ini'
        return super().tearDownClass()

    @classmethod
    def setDoAll(cls, doall=None):
        cls.ALLSETUP = doall

    @classmethod
    def setConfigFile(cls, configfile):
        cls.CONFIGFILE = configfile

    @classmethod
    def removetestDB(cls):
        # Delete the database that was created for unit testing
        if len(DIRS.get_dbfile()):
            cls.logger.debug("Deleting unit test database")
            try:
                remove(DIRS.get_dbfile())
                remove(DIRS.get_dbfile() + "-shm")
                remove(DIRS.get_dbfile() + "-wal")
            except Exception:
                pass

    @classmethod
    def removetestCache(cls):
        # Delete the database that was created for unit testing
        if len(DIRS.CACHEDIR):
            cls.logger.debug("Deleting unit test cache directory")
            try:
                rmtree(DIRS.CACHEDIR)
            except Exception:
                pass

    @classmethod
    def delete_test_logs(cls):
        if path_isdir(config2.CONFIG['LOGDIR']) and len(config2.CONFIG['LOGDIR']) > 3:
            try:  # Do not delete if there is a risk that it's the root of somewhere important
                rmtree(config2.CONFIG['LOGDIR'], ignore_errors=False)
            except Exception as e:
                print(str(e))

    def set_loglevel(self, level):
        """ Set the root log level per request; the calling test function depends on it to test log messages """
        logging.getLogger('root').setLevel(level)
        self.logger.setLevel(level)

    @classmethod
    def clearGlobals(cls):
        # Clear configuration variables to ahve a clean slate for any further test runs
        lazylibrarian.DAEMON = False
        lazylibrarian.SIGNAL = None
        lazylibrarian.SYS_ENCODING = ''
        lazylibrarian.LOGINUSER = None
        lazylibrarian.COMMIT_LIST = None
        lazylibrarian.STOPTHREADS = False

    @classmethod
    def disableHTTPSWarnings(cls):
        import urllib3
        urllib3.disable_warnings()

    @classmethod
    def prepareTestDB(cls):
        curr_ver = dbupgrade.upgrade_needed()
        if curr_ver:
            dbupgrade.db_upgrade(curr_ver)

    def assertEndsWith(self, teststr, end):
        self.assertEqual(teststr[-len(end):], end)

    def single_access_compare(self, got: Counter, expected: Counter, exclude: List[Access], error: str = ''):
        """ Helper function, validates that two access counters are the same """
        for access in got:
            if access not in exclude:
                self.assertTrue(access in expected, f'Excected {access}')
                vgot = got[access]
                vexp = expected[access]
                self.assertEqual(vgot, vexp, f'{access}:{vgot}!={vexp}: {error}')


def false_method() -> bool:
    """ A method that returns False. used for testing. """
    return False
