#  This file is part of Lazylibrarian.
#
# Purpose:
#    Contain core functions that access the filesystem, as well as
#    core global variables containing references to files
# Constraint:
#    Should not depend on any other LazyLibrarian files, except ConfigDict

import os
import shutil
import sys
from datetime import datetime
from typing import Optional

from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.formatter import make_bytestr, make_unicode, unaccented, replace_all, namedic
from lazylibrarian.logger import lazylibrarian_log, log_fileperms

class DirectoryHolder:
    """ Holds all the global directories used by LL """
    DATADIR: str        # Where LL stores its data files
    CACHEDIR: str = ''  # Where LL stores its cache
    TMPDIR: str         # Where LL will store temporary files
    FULL_PATH: str      # Fully qualified name of executable running
    config: ConfigDict  # A reference to the config being used

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

    def set_config(self, config: ConfigDict):
        self.config = config

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
    Duplicate of os.walk, except that in unix we use bytestrings for listdir
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


def setperm(file_or_dir):
    """
    Force newly created directories to rwxr-xr-x and files to rw-r--r--
    or other value as set in config
    """
    if not file_or_dir:
        return False

    if path_isdir(file_or_dir):
        perm = octal(DIRS.config['DIR_PERM'], 0o755)
    elif path_isfile(file_or_dir):
        perm = octal(DIRS.config['FILE_PERM'], 0o644)
    else:
        # not a file or a directory (symlink?)
        return False

    want_perm = oct(perm)[-3:].zfill(3)
    st = os.stat(syspath(file_or_dir))
    old_perm = oct(st.st_mode)[-3:].zfill(3)
    if old_perm == want_perm:
        if lazylibrarian_log.LOGLEVEL & log_fileperms:
            lazylibrarian_log.debug("Permission for %s is already %s" % (file_or_dir, want_perm))
        return True

    try:
        os.chmod(syspath(file_or_dir), perm)
    except Exception as err:
        lazylibrarian_log.debug("Error setting permission %s for %s: %s %s" % (want_perm, file_or_dir,
                                                                    type(err).__name__, str(err)))
        return False

    st = os.stat(syspath(file_or_dir))
    new_perm = oct(st.st_mode)[-3:].zfill(3)

    if new_perm == want_perm:
        if lazylibrarian_log.LOGLEVEL & log_fileperms:
            lazylibrarian_log.debug("Set permission %s for %s, was %s" % (want_perm, file_or_dir, old_perm))
        return True
    else:
        lazylibrarian_log.debug("Failed to set permission %s for %s, got %s" % (want_perm, file_or_dir, new_perm))
    return False


def octal(value, default: int) -> int:
    """ Return value as int, if it's a valid base-8 number, otherwise default """
    if not value:
        return default
    try:
        value = int(str(value), 8)
        return value
    except ValueError:
        return default


def make_dirs(dest_path, new=False):
    """ os.makedirs only seems to set the right permission on the final leaf directory
        not any intermediate parents it creates on the way, so we'll try to do it ourselves
        setting permissions as we go. Could use recursion but probably aren't many levels to do...
        Build a list of missing intermediate directories in reverse order, exit when we encounter
        an existing directory or hit root level. Set permission on any directories we create.
        If new, try to remove any pre-existing directory and contents.
        return True or False """

    to_make = []
    dest_path = syspath(dest_path)
    if new:
        shutil.rmtree(dest_path, ignore_errors=True)

    while not path_isdir(dest_path):
        # noinspection PyUnresolvedReferences
        to_make.insert(0, dest_path)
        parent = os.path.dirname(dest_path)
        if parent == dest_path:
            break
        else:
            dest_path = parent

    for entry in to_make:
        if lazylibrarian_log.LOGLEVEL & log_fileperms:
            lazylibrarian_log.debug("mkdir: [%s]" % repr(entry))
        try:
            os.mkdir(entry)  # mkdir uses umask, so set perm ourselves
            _ = setperm(entry)  # failing to set perm might not be fatal
        except OSError as why:
            # os.path.isdir() has some odd behaviour on Windows, says the directory does NOT exist
            # then when you try to mkdir complains it already exists.
            # Ignoring the error might just move the problem further on?
            # Something similar seems to occur on Google Drive filestream
            # but that returns Error 5 Access is denied
            # Trap errno 17 (linux file exists) and 183 (windows already exists)
            if why.errno in [17, 183]:
                if lazylibrarian_log.LOGLEVEL & log_fileperms:
                    lazylibrarian_log.debug("Ignoring mkdir already exists errno %s: [%s]" % (why.errno, repr(entry)))
                pass
            elif 'exists' in str(why):
                if lazylibrarian_log.LOGLEVEL & log_fileperms:
                    lazylibrarian_log.debug("Ignoring %s: [%s]" % (why, repr(entry)))
                pass
            else:
                lazylibrarian_log.error('Unable to create directory %s: [%s]' % (why, repr(entry)))
                return False
    return True


def safe_move(src, dst, action='move'):
    """ Move or copy src to dst
        Retry without accents if unicode error as some file systems can't handle (some) accents
        Retry with some characters stripped if bad filename
        e.g. Windows can't handle <>?"*:| (and maybe others) in filenames
        Return (new) dst if success """

    if src == dst:  # nothing to do
        return dst

    while action:  # might have more than one problem...
        try:
            if action == 'copy':
                shutil.copyfile(syspath(src), syspath(dst))
            elif path_isdir(src) and dst.startswith(src):
                shutil.copytree(syspath(src), syspath(dst))
            else:
                shutil.move(syspath(src), syspath(dst))
            return dst

        except UnicodeEncodeError:
            newdst = unaccented(dst)
            if newdst != dst:
                dst = newdst
            else:
                raise

        except (IOError, OSError) as err:  # both needed for different python versions
            if err.errno == 22:  # bad mode or filename
                lazylibrarian_log.debug("src=[%s] dst=[%s]" % (src, dst))
                drive, path = os.path.splitdrive(dst)
                lazylibrarian_log.debug("drive=[%s] path=[%s]" % (drive, path))
                # strip some characters windows can't handle
                newpath = replace_all(path, namedic)
                # windows filenames can't end in space or dot
                while newpath and newpath[-1] in '. ':
                    newpath = newpath[:-1]
                # anything left? has it changed?
                if newpath and newpath != path:
                    dst = os.path.join(drive, newpath)
                    lazylibrarian_log.debug("dst=[%s]" % dst)
                else:
                    raise
            else:
                raise
        except Exception:
            raise
    return dst


def safe_copy(src, dst):
    return safe_move(src, dst, action='copy')


def any_file(search_dir: str, extn: str) -> str:
    """ Find a file with specified extension in a directory, any will do.
    Return full pathname of file, or empty string if none found """
    if search_dir is None or extn is None:
        return ""
    if path_isdir(search_dir):
        for fname in listdir(search_dir):
            if fname.endswith(extn):
                return os.path.join(search_dir, fname)
    return ""


def opf_file(search_dir: str) -> str:
    """ Look for .opf files in search_dir, returning the file name.
    If metadata.opf exists and no other opf file does, return metatadata.
    If metadata.opf and another .opf file exists, return the other one.
    If two or more other .opf files exist, return '' - we don't know which one to use. """
    cnt = 0
    res = ''
    meta = ''
    if path_isdir(search_dir):
        for fname in listdir(search_dir):
            if fname.endswith('.opf'):
                if fname == 'metadata.opf':
                    meta = os.path.join(search_dir, fname)
                else:
                    res = os.path.join(search_dir, fname)
                cnt += 1
        if cnt > 2 or cnt == 2 and not meta:
            lazylibrarian_log.debug("Found %d conflicting opf in %s" % (cnt, search_dir))
            res = ''
        elif res:  # prefer bookname.opf over metadata.opf
            return res
        elif meta:
            return meta
    return res


def bts_file(search_dir: str) -> str:
    """ Find the first .bts file in search_dir, unless .bts files are 'skipped' in config """
    if 'bts' in DIRS.config['SKIPPED_EXT']:
        return ''
    return any_file(search_dir, '.bts')


def csv_file(search_dir: str, library: str) -> str:
    """ Returns the first CSV file in search_dir that matches the library name.
    The library name is 'eBook', 'audio', etc.
    If library name is blank, just return the first CSV file. """
    if search_dir and path_isdir(search_dir):
        try:
            for fname in listdir(search_dir):
                if fname.endswith('.csv'):
                    if not library or library in fname:
                        return os.path.join(search_dir, fname)
        except Exception as err:
            lazylibrarian_log.warn('Listdir error [%s]: %s %s' % (search_dir, type(err).__name__, str(err)))
    return ''


def jpg_file(search_dir: str) -> str:
    """ Returns the name of the first .jpg file in search_dir """
    return any_file(search_dir, '.jpg')
