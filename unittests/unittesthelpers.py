#  This file is part of Lazylibrarian.
#
# Purpose:
#   Hold helper functions only needed for testing

import unittest
import lazylibrarian
from os import remove
from shutil import rmtree
from lazylibrarian.common import logger
from lazylibrarian import dbupgrade, startup

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
        startup.shutdown(restart=False, update=False, exit=False, testing=True)
        if cls.ALLSETUP:
            cls.removetestDB()
            cls.removetestCache()
            cls.ALLSETUP = None
        cls.clearGlobals()
        return super().tearDownClass()

    @classmethod
    def setDoAll(cls, all=None):
        cls.ALLSETUP = all

    @classmethod
    def setConfigFile(cls, configfile):
        cls.CONFIGFILE = configfile

    @classmethod
    def removetestDB(cls):
        # Delete the database that was created for unit testing
        if len(lazylibrarian.DBFILE):
            logger.debug("Deleting unit test database")
            try:
                remove(lazylibrarian.DBFILE)
                remove(lazylibrarian.DBFILE + "-shm")
                remove(lazylibrarian.DBFILE + "-wal")
            except:
                pass

    @classmethod
    def removetestCache(cls):
        # Delete the database that was created for unit testing
        if len(lazylibrarian.CACHEDIR):
            logger.debug("Deleting unit test cache directory")
            try:
                rmtree(lazylibrarian.CACHEDIR)
            except:
                pass

    @classmethod
    def clearGlobals(cls):
        # Clear configuration variables to ahve a clean slate for any further test runs
        lazylibrarian.FULL_PATH = None
        lazylibrarian.PROG_DIR = ''
        lazylibrarian.ARGS = None
        lazylibrarian.DAEMON = False
        lazylibrarian.SIGNAL = None
        lazylibrarian.PIDFILE = ''
        lazylibrarian.DATADIR = ''
        lazylibrarian.CONFIGFILE = ''
        lazylibrarian.SYS_ENCODING = ''
        lazylibrarian.LOGLEVEL = 1
        lazylibrarian.LOGINUSER = None
        lazylibrarian.CONFIG = {}
        lazylibrarian.CFG = None
        lazylibrarian.DBFILE = None
        lazylibrarian.COMMIT_LIST = None
        lazylibrarian.SHOWLOGOUT = 1
        lazylibrarian.CHERRYPYLOG = 0
        lazylibrarian.REQUESTSLOG = 0
        lazylibrarian.DOCKER = False
        lazylibrarian.STOPTHREADS = False

        lazylibrarian.NEWZNAB_PROV = []
        lazylibrarian.TORZNAB_PROV = []
        lazylibrarian.RSS_PROV = []
        lazylibrarian.IRC_PROV = []
        lazylibrarian.GEN_PROV = []
        lazylibrarian.APPRISE_PROV = []

    @classmethod
    def disableHTTPSWarnings(cls):
        import urllib3
        urllib3.disable_warnings()

    @classmethod
    def prepareTestDB(cls):
        curr_ver = dbupgrade.upgrade_needed()
        if curr_ver:
            dbupgrade.dbupgrade(curr_ver)
