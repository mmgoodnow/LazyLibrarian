#  This file is part of Lazylibrarian.
#
# Purpose:
#   Hold helper functions only needed for testing

import unittest
from os import remove
from shutil import rmtree

import lazylibrarian
from lazylibrarian.common import logger
from lazylibrarian.filesystem import DIRS, path_isdir
from lazylibrarian import dbupgrade, startup, config2, configdefs

class LLTestCase(unittest.TestCase):
    ALLSETUP = None
    CONFIGFILE = './unittests/testdata/testconfig-defaults.ini'

    @classmethod
    def setUpClass(cls) -> None:
        # Run startup code without command line arguments and no forced sleep
        startup.startup_parsecommandline(__file__, args = [''],
            seconds_to_sleep = 0, config_override=cls.CONFIGFILE)
        startup.init_logs()
        startup.init_config()
        if cls.ALLSETUP:
            LLTestCase.disableHTTPSWarnings()
            startup.init_caches()
            startup.init_database()
            cls.prepareTestDB()
            startup.init_build_debug_header(online = False)
        startup.init_build_lists()
        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        config2.CONFIG.create_access_summary()

        startup.shutdown(restart=False, update=False, exit=False, testing=True)
        if cls.ALLSETUP:
            cls.removetestDB()
            cls.removetestCache()
            cls.ALLSETUP = None
        logger.lazylibrarian_log.stop_logger()
        cls.delete_test_logs()
        cls.clearGlobals()
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
            logger.debug("Deleting unit test database")
            try:
                remove(DIRS.get_dbfile())
                remove(DIRS.get_dbfile() + "-shm")
                remove(DIRS.get_dbfile() + "-wal")
            except:
                pass

    @classmethod
    def removetestCache(cls):
        # Delete the database that was created for unit testing
        if len(DIRS.CACHEDIR):
            logger.debug("Deleting unit test cache directory")
            try:
                rmtree(DIRS.CACHEDIR)
            except:
                pass

    @classmethod
    def delete_test_logs(cls):
        if path_isdir(config2.CONFIG['LOGDIR']) and len(config2.CONFIG['LOGDIR']) > 3:
            try: # Do not delete if there is a risk that it's the root of somewhere important
#                logger.debug("Deleting Logs")
                rmtree(config2.CONFIG['LOGDIR'], ignore_errors=False)
            except Exception as e:
                print(str(e))

    @classmethod
    def set_loglevel(cls, level: int):
        logger.lazylibrarian_log.update_loglevel(override=level)

    @classmethod
    def clearGlobals(cls):
        # Clear configuration variables to ahve a clean slate for any further test runs
        lazylibrarian.DAEMON = False
        lazylibrarian.SIGNAL = None
        lazylibrarian.PIDFILE = ''
        lazylibrarian.SYS_ENCODING = ''
        logger.lazylibrarian_log.update_loglevel(1)
        logger.lazylibrarian_log.LOGLEVEL_OVERRIDE = False
        config2.CONFIG = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS)
        lazylibrarian.LOGINUSER = None
        lazylibrarian.COMMIT_LIST = None
        lazylibrarian.SHOWLOGOUT = 1
        lazylibrarian.CHERRYPYLOG = 0
        lazylibrarian.REQUESTSLOG = 0
        lazylibrarian.DOCKER = False
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
        self.assertEqual(teststr[-len(end):],end)

def false_method() -> bool:
    """ A method that returns False. used for testing. """
    return False

