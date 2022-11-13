#  This file is part of Lazylibrarian.
#
# Purpose:
#   Hold helper functions only needed for testing

import lazylibrarian
from os import remove
from shutil import rmtree
from lazylibrarian.common import logger

def removetestDB():
    # Delete the database that was created for unit testing
    if len(lazylibrarian.DBFILE):
        logger.debug("Deleting unit test database")
        remove(lazylibrarian.DBFILE)
        remove(lazylibrarian.DBFILE + "-shm")
        remove(lazylibrarian.DBFILE + "-wal")

def removetestCache():
    # Delete the database that was created for unit testing
    if len(lazylibrarian.CACHEDIR):
        logger.debug("Deleting unit test cache directory")
        rmtree(lazylibrarian.CACHEDIR)

