#  This file is part of Lazylibrarian.
#
# Purpose:
#    Contain core functions that access the filesystem, as well as
#    core global variables containing references to files
# Constraint:
#    Should not depend on any other LazyLibrarian files, except ConfigDict

import os
import sys
from datetime import datetime
from typing import Optional

from lazylibrarian.formatter import make_bytestr, make_unicode
from lazylibrarian.logger import lazylibrarian_log, log_fileperms

class DirectoryHolder:
    """ Holds all the global directories used by LL """
    DATADIR: str        # Where LL stores its data files
    CACHEDIR: str = ''  # Where LL stores its cache
    TMPDIR: str         # Where LL will store temporary files
    FULL_PATH: str      # Fully qualified name of executable running

    def __init__(self):
        self.DATADIR = ''
        self.CACHEDIR = ''
        self.TMPDIR = ''
        self.tmpsequence = 0

    def set_datadir(self, datadir: str):
        """ Sets the DATADIR from config, and exits the program if it cannot be created or is not writeable.
        This also updates CACHEDIR and TMPDIR """
        ok, msg = self.ensure_dir_is_writeable(datadir)
        if not ok:
            raise SystemExit(f'{msg} Exiting.')
        self.DATADIR = datadir
        self.CACHEDIR = os.path.join(self.DATADIR, 'cache')
        ok, msg = self.ensure_dir_is_writeable(self.CACHEDIR)
        if not ok:
            lazylibrarian_log.error(msg)
            lazylibrarian_log.warn(f'Falling back to {self.DATADIR} for the cache')
            self.CACHEDIR = self.DATADIR
        self.TMPDIR = os.path.join(self.DATADIR, 'tmp')
        ok, msg = self.ensure_dir_is_writeable(self.TMPDIR)
        if not ok:
            lazylibrarian_log.error(msg)
            lazylibrarian_log.warn(f'Falling back to {self.DATADIR} for temporary files')
            self.TMPDIR = self.DATADIR

    @staticmethod
    def ensure_dir_is_writeable(dirname: str) -> (bool, str):
        if not path_isdir(dirname):
            try:
                os.makedirs(dirname)
            except OSError:
                return False, f'Could not create directory: {dirname}.'
        if not os.access(dirname, os.W_OK):
            return False, f'Cannot write to the directory: {dirname}.'

        return True, 'ok'

    def get_mako_cachedir(self):
        """ Return the name of the mako cache dir """
        return os.path.join(self.CACHEDIR, 'mako')

    def get_dbfile(self):
        """ Return the name of the LL database file """
        return os.path.join(self.DATADIR, 'lazylibrarian.db')

    def get_tmpfilename(self, base: Optional[str]=None) -> str:
        """ Get a file named base in the tmp directory.
        If base is not specified, return a unique filename """
        if not base:
            timestr = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            randomstr = str(self.tmpsequence)
            self.tmpsequence += 1
            if self.tmpsequence < 0: self.tmpsequence = 0
            base = f'LL-temp-{timestr}-{randomstr}.tmp'
        return syspath(os.path.join(self.TMPDIR, base))

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


def remove_file(name: str) -> bool:
    """ Remove the file. On error, log an error message. Returns True if successful """
    ok = False
    try:
        os.remove(syspath(name))
        ok = True
    except OSError as err:
        if err.errno == 2:  # does not exist is ok
            pass
        else:
            lazylibrarian_log.warn("Failed to remove %s : %s" % (name, err.strerror))
    except Exception as err:
        lazylibrarian_log.warn("Failed to remove %s : %s" % (name, str(err)))
    return ok


def remove_dir(name: str) -> bool:
    """ Remove the directory. On error, log an error message. Returns True if successful """
    ok = False
    try:
        os.rmdir(syspath(name))
        ok = True
    except OSError as err:
        if err.errno == 2:  # does not exist is ok
            pass
        else:
            lazylibrarian_log.warn("Failed to remove %s : %s" % (name, err.strerror))
    except Exception as err:
        lazylibrarian_log.warn("Failed to remove %s : %s" % (name, str(err)))
    return ok


def listdir(name: str):
    """
    listdir ensuring bytestring for unix,
    so we don't baulk if filename doesn't fit utf-8 on return
    and ensuring utf-8 and adding path requirements for windows
    All returns are unicode
    """
    if os.path.__name__ == 'ntpath':
        dname = syspath(name)
        if not dname.endswith('\\'):
            dname = dname + '\\'
        try:
            return os.listdir(dname)
        except Exception as err:
            lazylibrarian_log.error("Listdir [%s][%s] failed: %s" % (name, dname, str(err)))
            return []

    return [make_unicode(item) for item in os.listdir(make_bytestr(name))]


def walk(top, topdown=True, onerror=None, followlinks=False):
    """
    duplicate of os.walk, except for unix we use bytestrings for listdir
    return top, dirs, nondirs as unicode
    """
    islink, join, isdir = path_islink, os.path.join, path_isdir

    try:
        top = make_unicode(top)
        if os.path.__name__ != 'ntpath':
            names = os.listdir(make_bytestr(top))
            names = [make_unicode(name) for name in names]
        else:
            names = os.listdir(top)
    except (os.error, TypeError) as err:  # Windows can return TypeError if path is too long
        if onerror is not None:
            onerror(err)
        return

    dirs, nondirs = [], []
    for name in names:
        try:
            if isdir(join(top, name)):
                dirs.append(name)
            else:
                nondirs.append(name)
        except Exception as err:
            lazylibrarian_log.error("[%s][%s] %s" % (repr(top), repr(name), str(err)))
    if topdown:
        yield top, dirs, nondirs
    for name in dirs:
        new_path = join(top, name)
        if followlinks or not islink(new_path):
            for x in walk(new_path, topdown, onerror, followlinks):
                yield x
    if not topdown:
        yield top, dirs, nondirs
