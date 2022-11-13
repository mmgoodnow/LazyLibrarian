#  This file is part of Lazylibrarian.
#
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

# Purpose:
#   Common, basic functions for LazyLibrary

import datetime
import glob
import os
import platform
import random
import shutil
import string
import sys
import time
import traceback
import subprocess
from lib.apscheduler.scheduler import Scheduler

from six import PY2, text_type

try:
    import zipfile
except ImportError:
    if PY2:
        import lib.zipfile as zipfile
    else:
        import lib3.zipfile as zipfile

if PY2:
    from io import open
import re
import ssl
import sqlite3
import cherrypy

try:
    # noinspection PyUnresolvedReferences
    import psutil
    PSUTIL = True
except ImportError:
    PSUTIL = False

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.formatter import plural, next_run_time, is_valid_booktype, check_int, \
    get_list, make_unicode, unaccented, replace_all, make_bytestr, namedic, thread_name

# Notification Types
NOTIFY_SNATCH = 1
NOTIFY_DOWNLOAD = 2
NOTIFY_FAIL = 3

notifyStrings = {NOTIFY_SNATCH: "Started Download", NOTIFY_DOWNLOAD: "Added to Library", NOTIFY_FAIL: "Download failed"}

# Scheduler
SCHED = None


# list of all ascii and non-ascii quotes/apostrophes
# quote list: https://en.wikipedia.org/wiki/Quotation_mark
quotes = [
    u'\u0022',  # quotation mark (")
    u'\u0027',  # apostrophe (')
    u'\u0060',  # grave-accent
    u'\u00ab',  # left-pointing double-angle quotation mark
    u'\u00bb',  # right-pointing double-angle quotation mark
    u'\u2018',  # left single quotation mark
    u'\u2019',  # right single quotation mark
    u'\u201a',  # single low-9 quotation mark
    u'\u201b',  # single high-reversed-9 quotation mark
    u'\u201c',  # left double quotation mark
    u'\u201d',  # right double quotation mark
    u'\u201e',  # double low-9 quotation mark
    u'\u201f',  # double high-reversed-9 quotation mark
    u'\u2039',  # single left-pointing angle quotation mark
    u'\u203a',  # single right-pointing angle quotation mark
    u'\u300c',  # left corner bracket
    u'\u300d',  # right corner bracket
    u'\u300e',  # left white corner bracket
    u'\u300f',  # right white corner bracket
    u'\u301d',  # reversed double prime quotation mark
    u'\u301e',  # double prime quotation mark
    u'\u301f',  # low double prime quotation mark
    u'\ufe41',  # presentation form for vertical left corner bracket
    u'\ufe42',  # presentation form for vertical right corner bracket
    u'\ufe43',  # presentation form for vertical left corner white bracket
    u'\ufe44',  # presentation form for vertical right corner white bracket
    u'\uff02',  # fullwidth quotation mark
    u'\uff07',  # fullwidth apostrophe
    u'\uff62',  # halfwidth left corner bracket
    u'\uff63',  # halfwidth right corner bracket
]


def elapsed_since(start):
    return time.strftime("%H:%M:%S", time.gmtime(time.time() - start))


def get_process_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


def track(func):
    # decorator to show memory usage and running time of a function
    # to use, from lazylibrarian.common import track
    # then decorate the function(s) to track  eg...
    # @track
    # def search_book():
    def wrapper(*args, **kwargs):
        if PSUTIL:
            mem_before = get_process_memory()
            start = time.time()
            result = func(*args, **kwargs)
            elapsed_time = elapsed_since(start)
            mem_after = get_process_memory()
            logger.debug("{}: memory before: {:,}, after: {:,}, consumed: {:,}; exec time: {}".format(
                func.__name__,
                mem_before, mem_after, mem_after - mem_before,
                elapsed_time))
        else:
            logger.debug("psutil is not installed")
            result = func(*args, **kwargs)
        return result
    return wrapper


def cpu_use():
    if PSUTIL:
        p = psutil.Process()
        blocking = p.cpu_percent(interval=1)
        nonblocking = p.cpu_percent(interval=None)
        return "Blocking %s%% Non-Blocking %s%% %s" % (blocking, nonblocking, p.cpu_times())
    else:
        return "Unknown - install psutil"


def get_user_agent():
    # Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
    if lazylibrarian.CONFIG['USER_AGENT']:
        return lazylibrarian.CONFIG['USER_AGENT']
    else:
        return 'LazyLibrarian' + ' (' + platform.system() + ' ' + platform.release() + ')'


def multibook(foldername, recurse=False):
    # Check for more than one book in the folder(tree). Note we can't rely on basename
    # being the same, so just check for more than one bookfile of the same type
    # Return which type we found multiples of, or empty string if no multiples
    filetypes = get_list(lazylibrarian.CONFIG['EBOOK_TYPE'])

    if recurse:
        for _, _, f in walk(foldername):
            flist = [item for item in f]
            for item in filetypes:
                counter = 0
                for fname in flist:
                    if fname.endswith(item):
                        counter += 1
                        if counter > 1:
                            return item
    else:
        flist = listdir(foldername)
        for item in filetypes:
            counter = 0
            for fname in flist:
                if fname.endswith(item):
                    counter += 1
                    if counter > 1:
                        return item
    return ''


def path_isfile(name):
    return os.path.isfile(syspath(name))


def path_isdir(name):
    return os.path.isdir(syspath(name))


def path_exists(name):
    return os.path.exists(syspath(name))


def path_islink(name):
    return os.path.islink(syspath(name))


def remove(name):
    try:
        os.remove(syspath(name))
    except OSError as err:
        if err.errno == 2:  # does not exist is ok
            pass
        else:
            logger.warn("Failed to remove %s : %s" % (name, err.strerror))
            pass
    except Exception as err:
        logger.warn("Failed to remove %s : %s" % (name, str(err)))
        pass


def listdir(name):
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
            logger.error("Listdir [%s][%s] failed: %s" % (name, dname, str(err)))
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
            logger.error("[%s][%s] %s" % (repr(top), repr(name), str(err)))
    if topdown:
        yield top, dirs, nondirs
    for name in dirs:
        new_path = join(top, name)
        if followlinks or not islink(new_path):
            for x in walk(new_path, topdown, onerror, followlinks):
                yield x
    if not topdown:
        yield top, dirs, nondirs


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
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
            logger.debug("mkdir: [%s]" % repr(entry))
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
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
                    logger.debug("Ignoring mkdir already exists errno %s: [%s]" % (why.errno, repr(entry)))
                pass
            elif 'exists' in str(why):
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
                    logger.debug("Ignoring %s: [%s]" % (why, repr(entry)))
                pass
            else:
                logger.error('Unable to create directory %s: [%s]' % (why, repr(entry)))
                return False
    return True


WINDOWS_MAGIC_PREFIX = u'\\\\?\\'


def syspath(path, prefix=True):
    """Convert a path for use by the operating system. In particular,
    paths on Windows must receive a magic prefix and must be converted
    to Unicode before they are sent to the OS. To disable the magic
    prefix on Windows, set `prefix` to False---but only do this if you
    *really* know what you're doing.
    """
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
        logger.debug("%s:%s [%s]%s" % (os.path.__name__, sys.version[0:5], repr(path), isinstance(path, text_type)))

    if os.path.__name__ != 'ntpath':
        if PY2:
            return make_bytestr(path)
        return path

    if not isinstance(path, text_type):
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
    s = path.find(lazylibrarian.CACHEDIR)
    if s >= 0 and '/' in path:
        path = path.replace('/', '\\')
        # logger.debug("cache path changed [%s] to [%s]" % (opath, path))

    # Add the magic prefix if it isn't already there.
    # http://msdn.microsoft.com/en-us/library/windows/desktop/aa365247.aspx
    if prefix and not path.startswith(WINDOWS_MAGIC_PREFIX):
        if path.startswith(u'\\\\'):
            # UNC path. Final path should look like \\?\UNC\...
            path = u'UNC' + path[1:]
        path = WINDOWS_MAGIC_PREFIX + path

    return path


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
                logger.debug("src=[%s] dst=[%s]" % (src, dst))
                drive, path = os.path.splitdrive(dst)
                logger.debug("drive=[%s] path=[%s]" % (drive, path))
                # strip some characters windows can't handle
                newpath = replace_all(path, namedic)
                # windows filenames can't end in space or dot
                while newpath and newpath[-1] in '. ':
                    newpath = newpath[:-1]
                # anything left? has it changed?
                if newpath and newpath != path:
                    dst = os.path.join(drive, newpath)
                    logger.debug("dst=[%s]" % dst)
                else:
                    raise
            else:
                raise
        except Exception:
            raise
    return dst


def safe_copy(src, dst):
    return safe_move(src, dst, action='copy')


def proxy_list():
    proxies = None
    if lazylibrarian.CONFIG['PROXY_HOST']:
        proxies = {}
        for item in get_list(lazylibrarian.CONFIG['PROXY_TYPE']):
            if item in ['http', 'https']:
                proxies.update({item: lazylibrarian.CONFIG['PROXY_HOST']})
    return proxies


def is_valid_email(emails):
    if not emails:
        return False
    elif ',' in emails:
        emails = get_list(emails)
    else:
        emails = [emails]

    for email in emails:
        if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", email) is None:
            return False
    return True


def pwd_generator(size=10, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def pwd_check(password):
    # password rules:
    # At least 8 digits long
    # with no spaces
    # we don't enforce mix of alnum as longer passwords
    # made of random words are more secure
    if len(password) < 8:
        return False
    # if not any(char.isdigit() for char in password):
    #    return False
    # if not any(char.isalpha() for char in password):
    #    return False
    if any(char.isspace() for char in password):
        return False
    return True


def octal(value, default):
    if not value:
        return default
    try:
        value = int(str(value), 8)
        return value
    except ValueError:
        return default


def setperm(file_or_dir):
    """
    Force newly created directories to rwxr-xr-x and files to rw-r--r--
    or other value as set in config
    """
    if not file_or_dir:
        return False

    if path_isdir(file_or_dir):
        perm = octal(lazylibrarian.CONFIG['DIR_PERM'], 0o755)
    elif path_isfile(file_or_dir):
        perm = octal(lazylibrarian.CONFIG['FILE_PERM'], 0o644)
    else:
        # not a file or a directory (symlink?)
        return False

    want_perm = oct(perm)[-3:].zfill(3)
    st = os.stat(syspath(file_or_dir))
    old_perm = oct(st.st_mode)[-3:].zfill(3)
    if old_perm == want_perm:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
            logger.debug("Permission for %s is already %s" % (file_or_dir, want_perm))
        return True

    try:
        os.chmod(syspath(file_or_dir), perm)
    except Exception as err:
        logger.debug("Error setting permission %s for %s: %s %s" % (want_perm, file_or_dir,
                                                                    type(err).__name__, str(err)))
        return False

    st = os.stat(syspath(file_or_dir))
    new_perm = oct(st.st_mode)[-3:].zfill(3)

    if new_perm == want_perm:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
            logger.debug("Set permission %s for %s, was %s" % (want_perm, file_or_dir, old_perm))
        return True
    else:
        logger.debug("Failed to set permission %s for %s, got %s" % (want_perm, file_or_dir, new_perm))
    return False


def any_file(search_dir=None, extn=None):
    # find a file with specified extension in a directory, any will do
    # return full pathname of file, or empty string if none found
    if search_dir is None or extn is None:
        return ""
    if path_isdir(search_dir):
        for fname in listdir(search_dir):
            if fname.endswith(extn):
                return os.path.join(search_dir, fname)
    return ""


def opf_file(search_dir=None):
    if search_dir is None:
        return ""
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
            logger.debug("Found %d conflicting opf in %s" % (cnt, search_dir))
            res = ''
        elif res:  # prefer bookname.opf over metadata.opf
            return res
        elif meta:
            return meta
    return res


def bts_file(search_dir=None):
    if 'bts' not in get_list(lazylibrarian.CONFIG['SKIPPED_EXT']):
        return ''
    return any_file(search_dir, '.bts')


def csv_file(search_dir=None, library=None):
    if search_dir and path_isdir(search_dir):
        try:
            for fname in listdir(search_dir):
                if fname.endswith('.csv'):
                    if not library or library in fname:
                        return os.path.join(search_dir, fname)
        except Exception as err:
            logger.warn('Listdir error [%s]: %s %s' % (search_dir, type(err).__name__, str(err)))
    return ''


def jpg_file(search_dir=None):
    return any_file(search_dir, '.jpg')


def book_file(search_dir=None, booktype=None, recurse=False):
    # find a book/mag file in this directory (tree), any book will do
    # return full pathname of book/mag as bytes, or empty bytestring if none found
    if booktype is None:
        return ""

    if path_isdir(search_dir):
        if recurse:
            # noinspection PyBroadException
            try:
                for r, _, f in walk(search_dir):
                    # our walk returns unicode
                    for item in f:
                        if is_valid_booktype(item, booktype=booktype):
                            return os.path.join(r, item)
            except Exception:
                logger.error('Unhandled exception in book_file: %s' % traceback.format_exc())
        else:
            # noinspection PyBroadException
            try:
                for fname in listdir(search_dir):
                    if is_valid_booktype(fname, booktype=booktype):
                        return os.path.join(make_unicode(search_dir), fname)
            except Exception:
                logger.error('Unhandled exception in book_file: %s' % traceback.format_exc())
    return ""


def mime_type(filename):
    name = make_unicode(filename).lower()
    if name.endswith('.epub'):
        return 'application/epub+zip'
    elif name.endswith('.mobi') or name.endswith('.azw'):
        return 'application/x-mobipocket-ebook'
    elif name.endswith('.azw3'):
        return 'application/x-mobi8-ebook'
    elif name.endswith('.pdf'):
        return 'application/pdf'
    elif name.endswith('.mp3'):
        return 'audio/mpeg3'
    elif name.endswith('.m4a'):
        return 'audio/mp4'
    elif name.endswith('.m4b'):
        return 'audio/mp4'
    elif name.endswith('.flac'):
        return 'audio/flac'
    elif name.endswith('.ogg'):
        return 'audio/ogg'
    elif name.endswith('.zip'):
        return 'application/x-zip-compressed'
    elif name.endswith('.xml'):
        return 'application/rss+xml'
    elif name.endswith('.cbz'):
        return 'application/x-cbz'
    elif name.endswith('.cbr'):
        return 'application/x-cbr'
    return "application/x-download"


def is_overdue(which="author"):
    overdue = 0
    total = 0
    name = ''
    ident = ''
    days = 0
    maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
    if maxage:
        db = database.DBConnection()
        if which == 'author':
            cmd = 'SELECT AuthorName,AuthorID,Updated from authors WHERE Status="Active" or Status="Loading"'
            cmd += ' or Status="Wanted" '
            if lazylibrarian.CONFIG['BOOK_API'] == 'OpenLibrary':
                cmd += 'and AuthorID LIKE "OL%A" '
            else:
                cmd += 'and AuthorID NOT LIKE "OL%A" '
            cmd += 'order by Updated ASC'
            res = db.select(cmd)
            total = len(res)
            if total:
                name = res[0]['AuthorName']
                ident = res[0]['AuthorID']
                dtnow = time.time()
                days = int((dtnow - res[0]['Updated']) / (24 * 60 * 60))
                for item in res:
                    diff = (dtnow - item['Updated']) / (24 * 60 * 60)
                    if diff > maxage:
                        overdue += 1
                    else:
                        break
        if which == 'series':
            cmd = 'SELECT SeriesName,SeriesID,Updated from Series where Status="Active" or Status="Wanted"'
            cmd += ' order by Updated ASC'
            res = db.select(cmd)
            total = len(res)
            if total:
                name = res[0]['SeriesName']
                ident = res[0]['SeriesID']
                dtnow = time.time()
                days = int((dtnow - res[0]['Updated']) / (24 * 60 * 60))
                for item in res:
                    diff = (dtnow - item['Updated']) / (24 * 60 * 60)
                    if diff > maxage:
                        overdue += 1
                    else:
                        break
    return overdue, total, name, ident, days


def ago(when):
    """ Return human-readable string of how long ago something happened
        when = seconds count """

    diff = time.time() - when
    # calculate whole units, plus round up by adding 1(true) if remainder >= half
    days = int(diff / 86400) + (diff % 86400 >= 43200)
    hours = int(diff / 3600) + (diff % 3600 >= 1800)
    minutes = int(diff / 60) + (diff % 60 >= 30)
    seconds = int(diff)

    if days > 1:
        return "%i days ago" % days
    elif hours > 1:
        return "%i hours ago" % hours
    elif minutes > 1:
        return "%i minutes ago" % minutes
    elif seconds > 1:
        return "%i seconds ago" % seconds
    else:
        return "just now"


def module_available(module_name):
    if sys.version_info < (3, 0):
        import importlib
        # noinspection PyDeprecation
        loader = importlib.find_loader(module_name)
    elif sys.version_info <= (3, 3):
        import pkgutil
        loader = pkgutil.find_loader(module_name)
    elif sys.version_info >= (3, 4):
        import importlib
        loader = importlib.util.find_spec(module_name)
    else:
        loader = None
    return loader is not None


# some mac versions include requests _without_ urllib3, our copy bundles it
if module_available("urllib3") and module_available("requests"):
    # noinspection PyUnresolvedReferences
    import urllib3
    import requests
else:
    try:
        import lib.requests as requests
    except ModuleNotFoundError as e:
        print(str(e))
        print("Unable to continue, please install missing modules")
        exit(0)


def nextrun(target=None, interval=0, action='', hours=False):
    """ Check when a job is next due to run and log it
        Return startdate for the job """
    if target is None:
        return ''

    if action == 'StartNow':
        lazylibrarian.STOPTHREADS = False
        hours = False
        interval = 0

    db = database.DBConnection()
    columns = db.select('PRAGMA table_info(jobs)')
    if not columns:  # no such table
        lastrun = 0
    else:
        res = db.match('SELECT Finish from jobs WHERE Name=?', (target,))
        if res and res['Finish']:
            lastrun = res['Finish']
        else:
            lastrun = 0

    if target == 'sync_to_goodreads':
        newtarget = 'sync_to_gr'
    else:
        newtarget = target

    nextruntime = ''
    for job in SCHED.get_jobs():
        if newtarget in str(job):
            nextruntime = job.split('at: ')[1].split('.')[0].strip(')')
            break

    if nextruntime:
        startdate = datetime.datetime.strptime(nextruntime, '%Y-%m-%d %H:%M:%S')
        msg = "%s %s job in %s" % (action, target, next_run_time(startdate))
    else:
        if hours:
            interval *= 60

        next_run_in = lastrun + (interval * 60) - time.time()
        if next_run_in < 60:
            next_run_in = 60  # overdue, start in 1 minute

        startdate = datetime.datetime.fromtimestamp(time.time() + next_run_in)

        next_run_in = int(next_run_in / 60)
        if next_run_in < 1:
            next_run_in = 1

        if next_run_in <= 120:
            msg = "%s %s job in %s %s" % (action, target, next_run_in, plural(next_run_in, "minute"))
        else:
            hours = int(next_run_in / 60)
            if hours <= 48:
                msg = "%s %s job in %s %s" % (action, target, hours, plural(hours, "hour"))
            else:
                days = int(hours / 24)
                msg = "%s %s job in %s %s" % (action, target, days, plural(days, "day"))
    if lastrun:
        msg += " (Last run %s)" % ago(lastrun)
    logger.debug(msg)

    return startdate

def initscheduler():
    global SCHED
    SCHED = Scheduler(misfire_grace_time=30)

def startscheduler():
    SCHED.start()

def shutdownscheduler():
    if SCHED:
        # noinspection PyUnresolvedReferences
        SCHED.shutdown(wait=False)

def schedule_job(action='Start', target=None):
    """ Start or stop or restart a cron job by name e.g.
        target=search_magazines, target=process_dir, target=search_book """
    if target is None:
        return

    if target == 'PostProcessor':  # more readable
        newtarget = 'process_dir'
    elif target == 'sync_to_goodreads':
        newtarget = 'sync_to_gr'
    else:
        newtarget = target

    if action in ['Stop', 'Restart']:
        for job in SCHED.get_jobs():
            if newtarget in str(job):
                SCHED.unschedule_job(job)
                logger.debug("Stop %s job" % target)
                break

    if action in ['Start', 'Restart', 'StartNow']:
        for job in SCHED.get_jobs():
            if newtarget in str(job):
                logger.debug("%s %s job, already scheduled" % (action, target))
                return  # return if already running, if not, start a new one

        if 'process_dir' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SCAN_INTERVAL'], 0)
            if interval:
                startdate = nextrun("POSTPROCESS", interval, action)
                SCHED.add_interval_job(lazylibrarian.postprocess.cron_process_dir,
                                                     minutes=interval, start_date=startdate)

        elif 'search_magazines' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_MAGINTERVAL'], 0)
            if interval and (lazylibrarian.use_tor() or lazylibrarian.use_nzb()
                             or lazylibrarian.use_rss() or lazylibrarian.use_direct()
                             or lazylibrarian.use_irc()):
                startdate = nextrun("SEARCHALLMAG", interval, action)
                if interval <= 600:  # for bigger intervals switch to hours
                    SCHED.add_interval_job(lazylibrarian.searchmag.cron_search_magazines,
                                                         minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    SCHED.add_interval_job(lazylibrarian.searchmag.cron_search_magazines,
                                                         hours=hours, start_date=startdate)
        elif 'search_book' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_BOOKINTERVAL'], 0)
            if interval and (lazylibrarian.use_nzb() or lazylibrarian.use_tor()
                             or lazylibrarian.use_direct() or lazylibrarian.use_irc()):
                startdate = nextrun("SEARCHALLBOOKS", interval, action)
                if interval <= 600:
                    SCHED.add_interval_job(lazylibrarian.searchbook.cron_search_book,
                                                         minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    SCHED.add_interval_job(lazylibrarian.searchbook.cron_search_book,
                                                         hours=hours, start_date=startdate)
        elif 'search_rss_book' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCHRSS_INTERVAL'], 0)
            if interval and lazylibrarian.use_rss():
                startdate = nextrun("SEARCHALLRSS", interval, action)
                if interval <= 600:
                    SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_rss_book,
                                                         minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_rss_book,
                                                         hours=hours, start_date=startdate)
        elif 'search_wishlist' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['WISHLIST_INTERVAL'], 0)
            if interval and lazylibrarian.use_wishlist():
                startdate = nextrun("SEARCHWISHLIST", interval, action, True)
                SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_wishlist,
                                                     hours=interval, start_date=startdate)

        elif 'search_comics' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_COMICINTERVAL'], 0)
            if interval and (lazylibrarian.use_nzb() or lazylibrarian.use_tor()
                             or lazylibrarian.use_direct() or lazylibrarian.use_irc()):
                startdate = nextrun("SEARCHALLCOMICS", interval, action, True)
                SCHED.add_interval_job(lazylibrarian.comicsearch.cron_search_comics,
                                                     hours=interval, start_date=startdate)

        elif 'check_for_updates' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'], 0)
            if interval:
                startdate = nextrun("VERSIONCHECK", interval, action, True)
                SCHED.add_interval_job(lazylibrarian.versioncheck.check_for_updates,
                                                     hours=interval, start_date=startdate)

        elif 'sync_to_gr' in newtarget and lazylibrarian.CONFIG['GR_SYNC']:
            interval = check_int(lazylibrarian.CONFIG['GOODREADS_INTERVAL'], 0)
            if interval:
                startdate = nextrun("GRSYNC", interval, action, True)
                SCHED.add_interval_job(lazylibrarian.grsync.cron_sync_to_gr,
                                                     hours=interval, start_date=startdate)

        elif 'clean_cache' in newtarget:
            days = lazylibrarian.CONFIG['CACHE_AGE']
            if days:
                interval = 8
                startdate = nextrun("CLEANCACHE", interval, action, True)
                SCHED.add_interval_job(lazylibrarian.cache.clean_cache,
                                                     hours=interval, start_date=startdate)

        elif 'author_update' in newtarget or 'series_update' in newtarget:
            # Try to get all authors/series scanned evenly inside the cache age
            maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
            if maxage:
                typ = newtarget.replace('_update', '')
                if typ == 'author':
                    task = 'AUTHORUPDATE'
                else:
                    task = 'SERIESUPDATE'

                overdue, total, _, _, days = is_overdue(typ)

                if days == maxage:
                    due = "due"
                else:
                    due = "overdue"
                logger.debug("Found %s %s from %s %s update" % (
                             overdue, plural(overdue, typ), total, due))

                interval = maxage * 60 * 24
                interval = interval / max(total, 1)
                interval = int(interval * 0.80)  # allow some update time

                if interval < 5:  # set a minimum interval of 5 minutes, so we don't upset goodreads/librarything api
                    interval = 5

                startdate = nextrun(task, interval, action)
                if interval <= 600:  # for bigger intervals switch to hours
                    if typ == 'author':
                        SCHED.add_interval_job(author_update, minutes=interval, start_date=startdate)
                    else:
                        SCHED.add_interval_job(series_update, minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    if typ == 'author':
                        SCHED.add_interval_job(author_update, hours=hours, start_date=startdate)
                    else:
                        SCHED.add_interval_job(series_update, hours=hours, start_date=startdate)
        else:
            logger.debug("No %s scheduled" % target)


def author_update(restart=True, only_overdue=True):
    threadname = thread_name()
    if "Thread-" in threadname:
        thread_name("AUTHORUPDATE")

    db = database.DBConnection()
    msg = ''

    # noinspection PyBroadException
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        if check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            overdue, total, name, ident, days = is_overdue('author')
            if not total:
                msg = "There are no monitored authors"
            elif not overdue and only_overdue:
                msg = 'Oldest author info (%s) is %s %s old, no update due' % (name,
                                                                               days, plural(days, "day"))
            else:
                logger.info('Starting update for %s' % name)
                lazylibrarian.importer.add_author_to_db(refresh=True, authorid=ident, reason="author_update %s" % name)
                if lazylibrarian.STOPTHREADS:
                    return ''
                msg = 'Updated author %s' % name
            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            if total and restart and not lazylibrarian.STOPTHREADS:
                schedule_job("Restart", "author_update")
    except Exception:
        logger.error('Unhandled exception in AuthorUpdate: %s' % traceback.format_exc())
        msg = "Unhandled exception in AuthorUpdate"
    finally:
        return msg


def series_update(restart=True, only_overdue=True):
    threadname = thread_name()
    if "Thread-" in threadname:
        thread_name("SERIESUPDATE")

    db = database.DBConnection()
    msg = ''

    # noinspection PyBroadException
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        if check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            overdue, total, name, ident, days = is_overdue('series')
            if not total:
                msg = "There are no monitored series"
            elif not overdue and only_overdue:
                msg = 'Oldest series info (%s) is %s %s old, no update due' % (name,
                                                                               days, plural(days, "day"))
            else:
                logger.info('Starting series update for %s' % name)
                lazylibrarian.bookwork.add_series_members(ident)
                msg = 'Updated series %s' % name
            logger.debug(msg)

            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            if total and restart and not lazylibrarian.STOPTHREADS:
                schedule_job("Restart", "series_update")
    except Exception:
        logger.error('Unhandled exception in series_update: %s' % traceback.format_exc())
        msg = "Unhandled exception in series_update"
    finally:
        return msg


def all_author_update(refresh=False):
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        cmd = 'SELECT AuthorID from authors WHERE Status="Active" or Status="Loading" or Status="Wanted"'
        cmd += ' order by Updated ASC'
        activeauthors = db.select(cmd)
        lazylibrarian.AUTHORS_UPDATE = 1
        logger.info('Starting update for %i active %s' % (len(activeauthors), plural(len(activeauthors), "author")))
        for author in activeauthors:
            if lazylibrarian.STOPTHREADS:
                logger.debug("Aborting ActiveAuthorUpdate")
                break
            lazylibrarian.importer.add_author_to_db(refresh=refresh, authorid=author['AuthorID'],
                                                    reason="all_author_update")
        logger.info('Active author update complete')
        msg = 'Updated %i active %s' % (len(activeauthors), plural(len(activeauthors), "author"))
        logger.debug(msg)
    except Exception:
        msg = 'Unhandled exception in all_author_update: %s' % traceback.format_exc()
        logger.error(msg)
    finally:
        lazylibrarian.AUTHORS_UPDATE = 0
    return msg


def restart_jobs(start='Restart'):
    lazylibrarian.STOPTHREADS = start == 'Stop'
    for item in ['PostProcessor', 'search_book', 'search_rss_book', 'search_wishlist', 'series_update',
                 'search_magazines', 'search_comics', 'check_for_updates', 'author_update', 'sync_to_goodreads',
                 'clean_cache']:
        schedule_job(start, item)


def ensure_running(jobname):
    lazylibrarian.STOPTHREADS = False
    found = False
    for job in SCHED.get_jobs():
        if jobname in str(job):
            found = True
            break
    if not found:
        schedule_job('Start', jobname)


def check_running_jobs():
    # make sure the relevant jobs are running
    # search jobs start when something gets marked "wanted" but are
    # not aware of any config changes that happen later, ie enable or disable providers,
    # so we check whenever config is saved
    # postprocessor is started when something gets marked "snatched"
    # and cancels itself once everything is processed so should be ok
    # but check anyway for completeness...

    lazylibrarian.STOPTHREADS = False
    db = database.DBConnection()
    snatched = db.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    seeding = db.match("SELECT count(*) as counter from wanted WHERE Status = 'Seeding'")
    wanted = db.match("SELECT count(*) as counter FROM books WHERE Status = 'Wanted'")
    if snatched or seeding:
        ensure_running('PostProcessor')
    if wanted:
        if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_direct() or \
                lazylibrarian.use_irc():
            ensure_running('search_book')
        if lazylibrarian.use_rss():
            ensure_running('search_rss_book')
    else:
        schedule_job('Stop', 'search_book')
        schedule_job('Stop', 'search_rss_book')
    if lazylibrarian.use_wishlist():
        ensure_running('search_wishlist')
    else:
        schedule_job('Stop', 'search_wishlist')

    if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_rss() or \
            lazylibrarian.use_direct() or lazylibrarian.use_irc():
        ensure_running('search_magazines')
        ensure_running('search_comics')
    else:
        schedule_job('Stop', 'search_magazines')
        schedule_job('Stop', 'search_comics')

    ensure_running('author_update')
    ensure_running('series_update')


def get_calibre_id(data):
    logger.debug(str(data))
    fname = data.get('BookFile', '')
    if fname:  # it's a book
        author = data.get('AuthorName', '')
        title = data.get('BookName', '')
    else:
        title = data.get('IssueDate', '')
        if title:  # it's a magazine issue
            author = data.get('Title', '')
            fname = data.get('IssueFile', '')
        else:  # assume it's a comic issue
            title = data.get('IssueID', '')
            author = data.get('ComicID', '')
            fname = data.get('IssueFile', '')
    try:
        fname = os.path.dirname(fname)
        calibre_id = fname.rsplit('(', 1)[1].split(')')[0]
        if not calibre_id.isdigit():
            calibre_id = ''
    except IndexError:
        calibre_id = ''
    if not calibre_id:
        # ask calibre for id of this issue
        res, err, rc = lazylibrarian.calibre.calibredb('search', ['author:"%s" title:"%s"' % (author, title)])
        if not rc:
            try:
                calibre_id = res.split(',')[0].strip()
            except IndexError:
                calibre_id = ''
    logger.debug('Calibre ID [%s]' % calibre_id)
    return calibre_id


def show_stats():
    gb_status = "Active"
    for entry in lazylibrarian.PROVIDER_BLOCKLIST:
        if entry["name"] == 'googleapis':
            if int(time.time()) < int(entry['resume']):
                gb_status = "Blocked"
            break

    result = ["Cache %i %s, %i miss, " % (check_int(lazylibrarian.CACHE_HIT, 0),
                                          plural(check_int(lazylibrarian.CACHE_HIT, 0), "hit"),
                                          check_int(lazylibrarian.CACHE_MISS, 0)),
              "Sleep %.3f goodreads, %.3f librarything, %.3f comicvine" % (
                  lazylibrarian.TIMERS['SLEEP_GR'], lazylibrarian.TIMERS['SLEEP_LT'],
                  lazylibrarian.TIMERS['SLEEP_CV']),
              "GoogleBooks API %i calls, %s" % (lazylibrarian.GB_CALLS, gb_status)]

    db = database.DBConnection()
    snatched = db.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    if snatched['counter']:
        result.append("%i Snatched %s" % (snatched['counter'], plural(snatched['counter'], "item")))
    else:
        result.append("No Snatched items")

    series_stats = []
    res = db.match("SELECT count(*) as counter FROM series")
    series_stats.append(['Series', res['counter']])
    res = db.match("SELECT count(*) as counter FROM series WHERE Total>0 and Have=0")
    series_stats.append(['Empty', res['counter']])
    res = db.match("SELECT count(*) as counter FROM series WHERE Total>0 AND Have=Total")
    series_stats.append(['Full', res['counter']])
    res = db.match('SELECT count(*) as counter FROM series WHERE Status="Ignored"')
    series_stats.append(['Ignored', res['counter']])
    res = db.match("SELECT count(*) as counter FROM series WHERE Total=0")
    series_stats.append(['Blank', res['counter']])
    res = db.match("SELECT count(*) as counter FROM series WHERE Updated>0")
    series_stats.append(['Monitor', res['counter']])
    overdue = is_overdue('series')[0]
    series_stats.append(['Overdue', overdue])

    mag_stats = []
    if lazylibrarian.SHOW_MAGS:
        res = db.match("SELECT count(*) as counter FROM magazines")
        mag_stats.append(['Magazine', res['counter']])
        res = db.match("SELECT count(*) as counter FROM issues")
        mag_stats.append(['Issues', res['counter']])
        cmd = 'select (select count(*) as counter from issues where magazines.title = issues.title) '
        cmd += 'as counter from magazines where counter=0'
        res = db.match(cmd)
        mag_stats.append(['Empty', len(res)])

    if lazylibrarian.SHOW_COMICS:
        res = db.match("SELECT count(*) as counter FROM comics")
        mag_stats.append(['Comics', res['counter']])
        res = db.match("SELECT count(*) as counter FROM comicissues")
        mag_stats.append(['Issues', res['counter']])
        cmd = 'select (select count(*) as counter from comicissues where comics.comicid = comicissues.comicid) '
        cmd += 'as counter from comics where counter=0'
        res = db.match(cmd)
        mag_stats.append(['Empty', len(res)])

    book_stats = []
    audio_stats = []
    missing_stats = []
    res = db.match("SELECT count(*) as counter FROM books")
    book_stats.append(['eBooks', res['counter']])
    audio_stats.append(['Audio', res['counter']])
    res = db.select("SELECT Status,count(*) as counter from books group by Status")
    statusdict = {}
    for item in res:
        statusdict[item['Status']] = item['counter']
    for item in ['Have', 'Open', 'Wanted', 'Ignored']:
        book_stats.append([item, statusdict.get(item, 0)])
    res = db.select("SELECT AudioStatus,count(*) as counter from books group by AudioStatus")
    statusdict = {}
    for item in res:
        statusdict[item['AudioStatus']] = item['counter']
    for item in ['Have', 'Open', 'Wanted', 'Ignored']:
        audio_stats.append([item, statusdict.get(item, 0)])
    for column in ['BookGenre', 'BookDesc']:
        cmd = "SELECT count(*) as counter FROM books WHERE Status != 'Ignored' and "
        cmd += "(%s is null or %s = '')"
        res = db.match(cmd % (column, column))
        missing_stats.append([column.replace('Book', 'No'), res['counter']])
    cmd = "SELECT count(*) as counter FROM books WHERE Status != 'Ignored' and BookGenre='Unknown'"
    res = db.match(cmd)
    missing_stats.append(['X_Genre', res['counter']])
    cmd = "SELECT count(*) as counter FROM books WHERE Status != 'Ignored' and BookDesc='No Description'"
    res = db.match(cmd)
    missing_stats.append(['X_Desc', res['counter']])
    for column in ['BookISBN', 'BookLang']:
        cmd = "SELECT count(*) as counter FROM books WHERE "
        cmd += "(%s is null or %s = '' or %s = 'Unknown')"
        res = db.match(cmd % (column, column, column))
        missing_stats.append([column.replace('Book', 'No'), res['counter']])
    cmd = "SELECT count(*) as counter FROM genres"
    res = db.match(cmd)
    missing_stats.append(['Genres', res['counter']])

    if not lazylibrarian.SHOW_AUDIO:
        audio_stats = []

    author_stats = []
    res = db.match("SELECT count(*) as counter FROM authors")
    author_stats.append(['Authors', res['counter']])
    for status in ['Active', 'Wanted', 'Ignored', 'Paused']:
        res = db.match('SELECT count(*) as counter FROM authors WHERE Status="%s"' % status)
        author_stats.append([status, res['counter']])
    res = db.match("SELECT count(*) as counter FROM authors WHERE HaveEBooks+HaveAudioBooks=0")
    author_stats.append(['Empty', res['counter']])
    res = db.match("SELECT count(*) as counter FROM authors WHERE TotalBooks=0")
    author_stats.append(['Blank', res['counter']])
    overdue = is_overdue('author')[0]
    author_stats.append(['Overdue', overdue])
    for stats in [author_stats, book_stats, missing_stats, series_stats, audio_stats, mag_stats]:
        if len(stats):
            header = ''
            data = ''
            for item in stats:
                header += "%8s" % item[0]
                data += "%8i" % item[1]
            result.append('')
            result.append(header)
            result.append(data)
    return result


def show_jobs():
    result = []
    db = database.DBConnection()
    for job in SCHED.get_jobs():
        job = str(job)
        if "search_magazines" in job:
            jobname = "Magazine search"
            threadname = "SEARCHALLMAG"
        elif "search_comics" in job:
            jobname = "Comic search"
            threadname = "SEARCHALLCOMICS"
        elif "check_for_updates" in job:
            jobname = "Check for Update"
            threadname = "VERSIONCHECK"
        elif "search_book" in job:
            jobname = "Book search"
            threadname = "SEARCHALLBOOKS"
        elif "search_rss_book" in job:
            jobname = "rss book search"
            threadname = "SEARCHALLRSS"
        elif "search_wishlist" in job:
            jobname = "Wishlist search"
            threadname = "SEARCHWISHLIST"
        elif "PostProcessor" in job:
            jobname = "PostProcessor"
            threadname = "POSTPROCESS"
        elif "cron_process_dir" in job:
            jobname = "PostProcessor"
            threadname = "POSTPROCESS"
        elif "author_update" in job:
            jobname = "Update authors"
            threadname = "AUTHORUPDATE"
        elif "series_update" in job:
            jobname = "Update series"
            threadname = "SERIESUPDATE"
        elif "sync_to_gr" in job:
            jobname = "Goodreads Sync"
            threadname = "GRSYNC"
        elif "clean_cache" in job:
            jobname = "Clean cache"
            threadname = "CLEANCACHE"
        else:
            jobname = job.split(' ')[0].split('.')[2]
            threadname = jobname.upper()

        # jobinterval = job.split('[')[1].split(']')[0]
        jobtime = job.split('at: ')[1].split('.')[0].strip(')')
        jobtime = next_run_time(jobtime)
        timeparts = jobtime.split(' ')
        if timeparts[0] == '1' and timeparts[1].endswith('s'):
            timeparts[1] = timeparts[1][:-1]
        jobinfo = "%s: Next run in %s %s" % (jobname, timeparts[0], timeparts[1])
        res = db.match('SELECT Start,Finish from jobs WHERE Name="%s"' % threadname)

        if res:
            if res['Start'] > res['Finish']:
                jobinfo += " (Running since %s)" % ago(res['Start'])
            elif res['Finish']:
                jobinfo += " (Last run %s)" % ago(res['Finish'])
        result.append(jobinfo)

    result.append(' ')
    overdue, total, name, _, days = is_overdue('author')
    if name:
        result.append('Oldest author info (%s) is %s %s old' % (name, days, plural(days, "day")))
    if not overdue:
        result.append("There are no authors needing update")
    elif days == check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
        result.append("Found %s %s from %s due update" % (overdue, plural(overdue, "author"), total))
    else:
        result.append("Found %s %s from %s overdue update" % (overdue, plural(overdue, "author"), total))

    overdue, total, name, _, days = is_overdue('series')
    if name:
        result.append('Oldest series info (%s) is %s %s old' % (name, days, plural(days, "day")))
    if not overdue:
        result.append("There are no series needing update")
    elif days == check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
        result.append("Found %s series from %s due update" % (overdue, total))
    else:
        result.append("Found %s series from %s overdue update" % (overdue, total))
    return result


def clear_log():
    lazylibrarian.LOGLIST = []
    error = False
    if os.name == 'nt':
        return "Screen log cleared"

    logger.lazylibrarian_log.stop_logger()
    for f in glob.glob(lazylibrarian.CONFIG['LOGDIR'] + "/*.log*"):
        try:
            os.remove(syspath(f))
        except OSError as err:
            error = err.strerror
            logger.debug("Failed to remove %s : %s" % (f, error))

    logger.lazylibrarian_log.init_logger(loglevel=lazylibrarian.LOGLEVEL)

    if error:
        return 'Failed to clear logfiles: %s' % error
    else:
        return "Log cleared, level set to [%s]- Log Directory is [%s]" % (
            lazylibrarian.LOGLEVEL, lazylibrarian.CONFIG['LOGDIR'])


# noinspection PyUnresolvedReferences,PyPep8Naming
def log_header(online=True):
    from lazylibrarian.config import CONFIG_GIT

    popen_list = [sys.executable, lazylibrarian.FULL_PATH]
    popen_list += lazylibrarian.ARGS
    header = "Startup cmd: %s\n" % str(popen_list)
    header += "config file: %s\n" % lazylibrarian.CONFIGFILE
    header += 'Interface: %s\n' % lazylibrarian.CONFIG['HTTP_LOOK']
    header += 'Loglevel: %s\n' % lazylibrarian.LOGLEVEL
    header += 'Sys_Encoding: %s\n' % lazylibrarian.SYS_ENCODING
    for item in CONFIG_GIT:
        if item == 'GIT_UPDATED':
            timestamp = check_int(lazylibrarian.CONFIG[item], 0)
            header += '%s: %s\n' % (item.lower(), time.ctime(timestamp))
        else:
            header += '%s: %s\n' % (item.lower(), lazylibrarian.CONFIG[item])
    try:
        header += 'package version: %s\n' % lazylibrarian.version.PACKAGE_VERSION
    except AttributeError:
        pass
    try:
        header += 'packaged by: %s\n' % lazylibrarian.version.PACKAGED_BY
    except AttributeError:
        pass

    db_version = 0
    db = database.DBConnection()
    result = db.match('PRAGMA user_version')
    if result and result[0]:
        value = str(result[0])
        if value.isdigit():
            db_version = int(value)
    uname = platform.uname()
    header += "db version: %s\n" % db_version
    header += "Python version: %s\n" % sys.version.split('\n')
    header += "uname: %s\n" % str(uname)
    header += "Platform: %s\n" % platform.platform(aliased=True)
    if uname[0] == 'Darwin':
        header += "mac_ver: %s\n" % str(platform.mac_ver())
    elif uname[0] == 'Windows':
        header += "win_ver: %s\n" % str(platform.win32_ver())
    if 'urllib3' in globals():
        header += "urllib3: %s\n" % getattr(urllib3, '__version__', None)
    else:
        header += "urllib3: missing\n"
    header += "requests: %s\n" % getattr(requests, '__version__', None)
    if online:
        try:
            if lazylibrarian.CONFIG['SSL_VERIFY']:
                tls_version = requests.get('https://www.howsmyssl.com/a/check', timeout=30,
                                        verify=lazylibrarian.CONFIG['SSL_CERTS']
                                        if lazylibrarian.CONFIG['SSL_CERTS'] else True).json()['tls_version']
            else:
                logger.info('Checking TLS version, you can ignore any "InsecureRequestWarning" message')
                tls_version = requests.get('https://www.howsmyssl.com/a/check', timeout=30,
                                        verify=False).json()['tls_version']
            if '1.2' not in tls_version and '1.3' not in tls_version:
                header += 'tls: missing required functionality. Try upgrading to v1.2 or newer. You have '
        except Exception as err:
            tls_version = str(err)
        header += "tls: %s\n" % tls_version

    header += "cherrypy: %s\n" % getattr(cherrypy, '__version__', None)
    header += "sqlite3: %s\n" % getattr(sqlite3, 'sqlite_version', None)

    if lazylibrarian.APPRISE and lazylibrarian.APPRISE[0].isdigit():
        header += "apprise: %s\n" % lazylibrarian.APPRISE
    else:
        header += "apprise: library missing\n"
    if lazylibrarian.UNRARLIB == 1:
        vers = lazylibrarian.RARFILE.unrarlib.RARGetDllVersion()
        header += "unrar: %s\n" % vers
    elif lazylibrarian.UNRARLIB == 2:
        import lib.UnRAR2 as UnRAR2
        vers = getattr(UnRAR2, '__version__', None)
        header += "unrar2: %s\n" % vers
        if os.name == 'nt':
            vers = UnRAR2.windows.RARGetDllVersion()
            header += "unrar dll: %s\n" % vers
    else:
        header += "unrar: library missing\n"

    if module_available("bs4") and module_available("html5lib"):
        import bs4
        vers = getattr(bs4, '__version__', None)
        header += "bs4: %s\n" % vers
        import html5lib
        vers = getattr(html5lib, '__version__', None)
        header += "html5lib: %s\n" % vers
    else:
        if PY2:
            import lib.bs4 as bs4
            bs4vers = getattr(bs4, '__version__', None)
            # noinspection PyProtectedMember
            h5vers = getattr(bs4.builder._html5lib.html5lib, '__version__', None)
        else:
            import lib3.bs4 as bs4
            bs4vers = getattr(bs4, '__version__', None)
            # noinspection PyProtectedMember
            try:
                # noinspection PyProtectedMember
                h5vers = getattr(bs4.builder._html5lib.html5lib, '__version__', None)
            except AttributeError:
                h5vers = "library missing"
        header += "local bs4: %s\n" % bs4vers
        header += "local html5lib: %s\n" % h5vers

    try:
        import PIL
        vers = getattr(PIL, '__version__', None)
        header += "python imaging: %s\n" % vers
    except ImportError:
        header += "python imaging: library missing\n"

    header += "openssl: %s\n" % getattr(ssl, 'OPENSSL_VERSION', None)
    X509 = None
    cryptography = None
    try:
        # pyOpenSSL 0.14 and above use cryptography for OpenSSL bindings. The _x509
        # attribute is only present on those versions.
        # noinspection PyUnresolvedReferences
        import OpenSSL
    except ImportError:
        header += "pyOpenSSL: module missing\n"
        OpenSSL = None

    if OpenSSL:
        try:
            # noinspection PyUnresolvedReferences
            from OpenSSL.crypto import X509
        except ImportError:
            header += "pyOpenSSL.crypto X509: module missing\n"

    if X509:
        # noinspection PyCallingNonCallable
        x509 = X509()
        if getattr(x509, "_x509", None) is None:
            header += "pyOpenSSL: module missing required functionality. Try upgrading to v0.14 or newer. You have "
        header += "pyOpenSSL: %s\n" % getattr(OpenSSL, '__version__', None)

    if OpenSSL:
        try:
            import OpenSSL.SSL
        except (ImportError, AttributeError) as err:
            header += 'pyOpenSSL missing SSL module/attribute: %s\n' % err

    if OpenSSL:
        try:
            # get_extension_for_class method added in `cryptography==1.1`; not available in older versions
            # but need cryptography >= 1.3.4 for access from pyopenssl >= 0.14
            # noinspection PyUnresolvedReferences
            import cryptography
        except ImportError:
            header += "cryptography: module missing\n"

    if cryptography:
        try:
            # noinspection PyUnresolvedReferences
            from cryptography.x509.extensions import Extensions
            if getattr(Extensions, "get_extension_for_class", None) is None:
                header += "cryptography: module missing required functionality."
                header += " Try upgrading to v1.3.4 or newer. You have "
            header += "cryptography: %s\n" % getattr(cryptography, '__version__', None)
        except ImportError:
            header += "cryptography Extensions: module missing\n"

    # noinspection PyBroadException
    try:
        import lib.thefuzz as fuzz
        vers = fuzz.__dict__['__version__']
    except Exception:
        vers = 'None'
    if vers:
        header += "fuzz: %s\n" % vers
        # noinspection PyBroadException
        try:
            import Levenshtein
            vers = "installed"
        except Exception:
            vers = "None"
        header += "Levenshtein: %s\n" % vers
    # noinspection PyBroadException
    try:
        import magic
    except Exception:
        # noinspection PyBroadException
        try:
            import lib.magic as magic
        except Exception:
            magic = None

    if magic:
        try:
            # noinspection PyProtectedMember
            ver = magic.libmagic._name
        except AttributeError:
            ver = 'missing'
        header += "magic: %s\n" % ver
    else:
        header += "magic: missing\n"

    return header


def set_redactlist():
    if len(lazylibrarian.REDACTLIST):
        return

    lazylibrarian.REDACTLIST = []
    wordlist = ['PASS', 'TOKEN', 'SECRET', '_API', '_USER', '_DEV']
    if lazylibrarian.CONFIG['HOSTREDACT']:
        wordlist.append('_HOST')
    for key in lazylibrarian.CONFIG.keys():
        if key not in ['BOOK_API', 'GIT_USER', 'SINGLE_USER']:
            for word in wordlist:
                if word in key and lazylibrarian.CONFIG[key]:
                    lazylibrarian.REDACTLIST.append(u"%s" % lazylibrarian.CONFIG[key])
    for key in ['EMAIL_FROM', 'EMAIL_TO', 'SSL_CERTS']:
        if lazylibrarian.CONFIG[key]:
            lazylibrarian.REDACTLIST.append(u"%s" % lazylibrarian.CONFIG[key])
    for item in lazylibrarian.NEWZNAB_PROV:
        if item['API']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['API'])
        if lazylibrarian.CONFIG['HOSTREDACT'] and item['HOST']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['HOST'])
    for item in lazylibrarian.TORZNAB_PROV:
        if item['API']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['API'])
        if lazylibrarian.CONFIG['HOSTREDACT'] and item['HOST']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['HOST'])
    for item in lazylibrarian.RSS_PROV:
        if lazylibrarian.CONFIG['HOSTREDACT'] and item['HOST']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['HOST'])
    for item in lazylibrarian.GEN_PROV:
        if lazylibrarian.CONFIG['HOSTREDACT'] and item['HOST']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['HOST'])
    for item in lazylibrarian.APPRISE_PROV:
        if lazylibrarian.CONFIG['HOSTREDACT'] and item['URL']:
            lazylibrarian.REDACTLIST.append(u"%s" % item['URL'])

    logger.debug("Redact list has %d %s" % (len(lazylibrarian.REDACTLIST),
                                            plural(len(lazylibrarian.REDACTLIST), "entry")))


def save_log():
    if not path_exists(lazylibrarian.CONFIG['LOGDIR']):
        return 'LOGDIR does not exist'

    basename = os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'lazylibrarian.log')
    outfile = os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'debug')
    set_redactlist()

    out = open(syspath(outfile + '.tmp'), 'w', encoding='utf-8')

    nextfile = True
    extn = 0
    redacts = 0
    while nextfile:
        fname = basename
        if extn > 0:
            fname = fname + '.' + str(extn)
        if not path_exists(fname):
            logger.debug("logfile [%s] does not exist" % fname)
            nextfile = False
        else:
            logger.debug('Processing logfile [%s]' % fname)
            linecount = 0
            if PY2:
                lines = reversed(open(syspath(fname), 'r', encoding="utf-8").readlines())
                lines = [make_unicode(lyne) for lyne in lines]
            else:
                lines = reversed(list(open(syspath(fname), 'r', encoding="utf-8")))
            for line in lines:
                for item in lazylibrarian.REDACTLIST:
                    if item in line:
                        line = line.replace(item, '<redacted>')
                        redacts += 1

                item = "Apprise: url:"
                startpos = line.find(item)
                if startpos >= 0:
                    startpos += len(item)
                    endpos = line.find('//', startpos)
                    line = line[:endpos] + '<redacted>'
                    redacts += 1

                out.write(line)
                if "Debug log ON" in line:
                    logger.debug('Found "Debug log ON" line %s in %s' % (linecount, fname))
                    nextfile = False
                    break
                linecount += 1
            extn += 1

    if path_exists(lazylibrarian.CONFIGFILE):
        out.write(u'---END-CONFIG---------------------------------\n')
        if PY2:
            lines = reversed(open(syspath(lazylibrarian.CONFIGFILE), 'r', encoding="utf-8").readlines())
            lines = [make_unicode(lyne) for lyne in lines]
        else:
            lines = reversed(list(open(syspath(lazylibrarian.CONFIGFILE), 'r', encoding="utf-8")))
        for line in lines:
            for item in lazylibrarian.REDACTLIST:
                if item in line:
                    line = line.replace(item, '<redacted>')
                    redacts += 1
            out.write(line)
        out.write(u'---CONFIG-------------------------------------\n')
    out.close()
    logfile = open(syspath(outfile + '.log'), 'w', encoding='utf-8')
    logfile.write(log_header())
    linecount = 0
    if PY2:
        lines = reversed(open(syspath(outfile + '.tmp'), 'r', encoding="utf-8").readlines())
        lines = [make_unicode(lyne) for lyne in lines]
    else:
        lines = reversed(list(open(syspath(outfile + '.tmp'), 'r', encoding="utf-8")))
    for line in lines:
        logfile.write(line)
        linecount += 1
    remove(outfile + '.tmp')
    logger.debug("Redacted %s passwords/apikeys" % redacts)
    logger.debug("%s log lines written to %s" % (linecount, outfile + '.log'))
    with zipfile.ZipFile(outfile + '.zip', 'w') as myzip:
        myzip.write(outfile + '.log', 'debug.log')
    remove(outfile + '.log')
    return "Debug log saved as %s" % (outfile + '.zip')


def zip_audio(source, zipname, bookid):
    """ Zip up all the audiobook parts in source folder to zipname
        Check if zipfile already exists, if not create a new one
        Doesn't actually check for audiobook parts, just zips everything
        including any .jpg etc.
        Return full path to zipfile
    """
    zip_file = os.path.join(source, zipname + '.zip')
    if not path_exists(zip_file):
        logger.debug('Zipping up %s' % zipname)
        namevars = lazylibrarian.bookrename.name_vars(bookid)
        singlefile = namevars['AudioSingleFile']

        cnt = 0
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as myzip:
            for rootdir, _, filenames in walk(source):
                for filename in filenames:
                    # don't include self or our special index file
                    if not filename.endswith('.zip') and not filename.endswith('.ll'):
                        bname, extn = os.path.splitext(filename)
                        # don't include singlefile
                        if bname != singlefile:
                            cnt += 1
                            myzip.write(os.path.join(rootdir, filename), filename)
        logger.debug('Zipped up %s files' % cnt)
        _ = setperm(zip_file)
    return zip_file


def run_script(params):
    if os.name == 'nt' and params[0].endswith('.py'):
        params.insert(0, sys.executable)
    logger.debug(str(params))
    try:
        if os.name != 'nt':
            p = subprocess.Popen(params, preexec_fn=lambda: os.nice(10),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res, err = p.communicate()
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug(make_unicode(res))
            logger.debug(make_unicode(err))
        return p.returncode, make_unicode(res), make_unicode(err)
    except Exception as er:
        err = "run_script exception: %s %s" % (type(er).__name__, str(er))
        logger.error(err)
        return 1, '', err


def calibre_prg(prgname):
    # Try to locate a calibre ancilliary program
    # Try explicit path or in the calibredb location
    # or in current path or system path
    target = ''
    if prgname == 'ebook-convert':
        target = lazylibrarian.CONFIG['EBOOK_CONVERT']
    if not target:
        calibre = lazylibrarian.CONFIG['IMP_CALIBREDB']
        if calibre:
            target = os.path.join(os.path.dirname(calibre), prgname)
        else:
            logger.debug("No calibredb configured")

    if not target or not os.path.exists(target):
        target = os.path.join(os.getcwd(), prgname)
        if not os.path.exists(target):
            logger.debug("%s not found" % target)
            if os.name == 'nt':
                try:
                    params = ["where", prgname]
                    res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                    target = make_unicode(res).strip()
                except Exception as err:
                    logger.debug("where %s failed: %s %s" % (prgname, type(err).__name__, str(err)))
                    target = ''
            else:
                try:
                    params = ["which", prgname]
                    res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                    target = make_unicode(res).strip()
                except Exception as err:
                    logger.debug("which %s failed: %s %s" % (prgname, type(err).__name__, str(err)))
                    target = ''
    if target:
        logger.debug("Using %s" % target)
        try:
            params = [target, "--version"]
            res = subprocess.check_output(params, stderr=subprocess.STDOUT)
            res = make_unicode(res).strip().split("(")[1].split(")")[0]
            logger.debug("Found %s version %s" % (prgname, res))
        except Exception as err:
            logger.debug("%s --version failed: %s %s" % (prgname, type(err).__name__, str(err)))
            target = ''
    return target


def only_punctuation(value):
    for c in value:
        if c not in string.punctuation and c not in string.whitespace:
            return False
    return True

