#  This file is part of Lazylibrarian.
#
# Purpose:
#    Contain core functions that access the filesystem, as well as
#    core global variables containing references to files
# Constraint:
#    Should not depend on any other LazyLibrarian files, except ConfigDict

import os
import sys
from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.logger import lazylibrarian_log, log_fileperms

class DirectoryHolder:
    DATADIR: str                    # Where LL stores its data files
    CACHEDIR: str = ''              # Where LL stores its cache
    TMPDIR: str                     # Where LL will store temporary files

    def __init__(self):
        self.DATADIR = ''
        self.CACHEDIR = ''
        self.TMPDIR = ''

    def set_datadir(self, datadir: str):
        """ Sets the DATADIR from config, and exits the program if it cannot be created or is not writeable """
        if not path_isdir(datadir):
            try:
                os.makedirs(datadir)
            except OSError:
                raise SystemExit(f'Could not create data directory: {datadir}. Exit ...')
        if not os.access(datadir, os.W_OK):
            raise SystemExit(f'Cannot write to the data directory: {datadir}. Exit ...')
        self.DATADIR = datadir
        self.CACHEDIR = datadir
        self.TMPDIR = datadir

""" Global access to directories """
DIRS = DirectoryHolder()

## PATH FUNCTIONS

def path_isfile(name: str) -> bool:
    return os.path.isfile(syspath(name))


def path_isdir(name: str) -> bool:
    return os.path.isdir(syspath(name))


def path_exists(name: str) -> bool:
    return os.path.exists(syspath(name))


def path_islink(name: str) -> bool:
    return os.path.islink(syspath(name))

def new_temp_filename() -> str:
    return 'hello'

WINDOWS_MAGIC_PREFIX = u'\\\\?\\'

def syspath(path: str, prefix:bool=True) -> str:
    """Convert a path for use by the operating system. In particular,
    paths on Windows must receive a magic prefix and must be converted
    to Unicode before they are sent to the OS. To disable the magic
    prefix on Windows, set `prefix` to False---but only do this if you
    *really* know what you're doing.
    """
    if lazylibrarian_log.LOGLEVEL & log_fileperms > 0:
        lazylibrarian_log.debug("%s:%s [%s]%s" % (os.path.__name__, sys.version[0:5], repr(path), isinstance(path, str)))

    if os.path.__name__ != 'ntpath':
        return path

    if not isinstance(path, str):
        # Beets currently represents Windows paths internally with UTF-8
        # arbitrarily. But earlier versions used MBCS because it is
        # reported as the FS encoding by Windows. Try both.
        try:
            path = path.decode('utf-8')
        except UnicodeError:
            # The encoding should always be MBCS, Windows' broken
            # Unicode representation.
            encoding = sys.getfilesystemencoding() or sys.getdefaultencoding()
            path = path.decode(encoding, 'replace')

    if 1 < len(path) < 4 and path[1] == ':':  # it's just a drive letter (E: or E:/)
        return path

    # the html cache addressing uses forwardslash as a separator but Windows file system needs backslash
    s = path.find(DIRS.CACHEDIR)
    if s >= 0 and '/' in path:
        path = path.replace('/', '\\')
        # logger.debug("cache path changed [%s] to [%s]" % (opath, path))

    if not path.startswith('.'):  # Don't affect relative paths
        # Add the magic prefix if it isn't already there.
        # http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247.aspx
        if prefix and not path.startswith(WINDOWS_MAGIC_PREFIX):
            if path.startswith(u'\\\\'):
                # UNC path. Final path should look like \\?\UNC\...
                path = u'UNC' + path[1:]
            path = WINDOWS_MAGIC_PREFIX + path

    return path
