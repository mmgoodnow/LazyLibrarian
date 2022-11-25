#  This file is part of Lazylibrarian.
#
# Purpose:
#   Hold helper functions only needed for testing

import lazylibrarian
from os import remove
from shutil import rmtree
from lazylibrarian.common import logger
from lazylibrarian import dbupgrade

def removetestDB():
    # Delete the database that was created for unit testing
    if len(lazylibrarian.DBFILE):
        logger.debug("Deleting unit test database")
        try:
            remove(lazylibrarian.DBFILE)
            remove(lazylibrarian.DBFILE + "-shm")
            remove(lazylibrarian.DBFILE + "-wal")
        except:
            pass

def removetestCache():
    # Delete the database that was created for unit testing
    if len(lazylibrarian.CACHEDIR):
        logger.debug("Deleting unit test cache directory")
        try:
            rmtree(lazylibrarian.CACHEDIR)
        except:
            pass

def clearGlobals():
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

def disableHTTPSWarnings():
    import urllib3
    urllib3.disable_warnings()

def prepareTestDB():
    curr_ver = dbupgrade.upgrade_needed()
    if curr_ver:
        dbupgrade.db_upgrade(curr_ver)
