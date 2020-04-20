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

import datetime
import glob
import os
import platform
import random
import shutil
import string
import sys
import threading
import time
import traceback
from subprocess import Popen, PIPE

from lib.six import PY2, text_type

try:
    import zipfile
except ImportError:
    if PY2:
        import lib.zipfile as zipfile
    else:
        import lib3.zipfile as zipfile

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

# some mac versions include requests _without_ urllib3, our copy bundles it
try:
    # noinspection PyUnresolvedReferences
    import urllib3
    import requests
except ImportError:
    import lib.requests as requests

import lazylibrarian
from lazylibrarian import logger, database, version
from lazylibrarian.formatter import plural, next_run, is_valid_booktype, check_int, \
    getList, makeUnicode, unaccented, replace_all, makeBytestr

# Notification Types
NOTIFY_SNATCH = 1
NOTIFY_DOWNLOAD = 2

notifyStrings = {NOTIFY_SNATCH: "Started Download", NOTIFY_DOWNLOAD: "Added to Library"}

# dict to remove/replace characters we don't want in a filename - this might be too strict?
namedic = {'<': '', '>': '', '...': '', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', '|': '',
           ' + ': ' ', '"': '', ',': '', '*': '', ':': '', ';': '', '\'': '', '//': '/', '\\\\': '\\'}

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


def getUserAgent():
    # Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
    if lazylibrarian.CONFIG['USER_AGENT']:
        return lazylibrarian.CONFIG['USER_AGENT']
    else:
        return 'LazyLibrarian' + ' (' + platform.system() + ' ' + platform.release() + ')'


def multibook(foldername, recurse=False):
    # Check for more than one book in the folder(tree). Note we can't rely on basename
    # being the same, so just check for more than one bookfile of the same type
    # Return which type we found multiples of, or empty string if no multiples
    filetypes = getList(lazylibrarian.CONFIG['EBOOK_TYPE'])

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


def listdir(name):
    """
    listdir ensuring bytestring for unix
    so we don't baulk if filename doesn't fit utf-8 on return
    and ensuring utf-8 and adding path requirements for windows
    All returns are unicode
    """
    if os.path.__name__ == 'ntpath':
        name = syspath(name)
        if not name.endswith('\\'):
            name = name + '\\'
        return os.listdir(name)
    return [makeUnicode(item) for item in os.listdir(makeBytestr(name))]


def walk(top, topdown=True, onerror=None, followlinks=False):
    """
    duplicate of os.walk, except for unix we use bytestrings for listdir
    return top, dirs, nondirs as unicode
    """
    islink, join, isdir = path_islink, os.path.join, path_isdir

    try:
        top = makeUnicode(top)
        if os.path.__name__ != 'ntpath':
            names = os.listdir(makeBytestr(top))
            names = [makeUnicode(name) for name in names]
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
        except Exception as e:
            logger.error("[%s][%s] %s" % (repr(top), repr(name), str(e)))
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
            # os.path.isdir() has some odd behaviour on windows, says the directory does NOT exist
            # then when you try to mkdir complains it already exists.
            # Ignoring the error might just move the problem further on?
            # Something similar seems to occur on google drive filestream
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
    # Don't do anything if we're not on windows
    if os.path.__name__ != 'ntpath':
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

    # the html cache addressing uses forwardslash as a separator but windows file system needs backslash
    opath = path
    s = path.find(lazylibrarian.CACHEDIR)
    if s >= 0 and '/' in path:
        path = path.replace('/', '\\')
        logger.debug("cache path changed [%s] to [%s]" % (opath, path))

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
        eg windows can't handle <>?":| (and maybe others) in filenames
        Return (new) dst if success """

    src = syspath(src)
    dst = syspath(dst)
    if src == dst:  # nothing to do
        return dst

    while action:  # might have more than one problem...
        try:
            if action == 'copy':
                shutil.copy(src, dst)
            elif path_isdir(src) and dst.startswith(src):
                shutil.copytree(src, dst)
            else:
                shutil.move(src, dst)
            return dst

        except UnicodeEncodeError:
            newdst = unaccented(dst)
            if newdst != dst:
                dst = newdst
            else:
                raise

        except IOError as e:
            if e.errno == 22:  # bad mode or filename
                drive, path = os.path.splitdrive(dst)
                # strip some characters windows can't handle
                newpath = replace_all(path, namedic)
                # windows filenames can't end in space or dot
                while newpath and newpath[-1] in '. ':
                    newpath = newpath[:-1]
                # anything left? has it changed?
                if newpath and newpath != path:
                    dst = os.path.join(drive, newpath)
                else:
                    raise
            else:
                raise
        except Exception:
            raise
    return dst


def safe_copy(src, dst):
    return safe_move(src, dst, action='copy')


def proxyList():
    proxies = None
    if lazylibrarian.CONFIG['PROXY_HOST']:
        proxies = {}
        for item in getList(lazylibrarian.CONFIG['PROXY_TYPE']):
            if item in ['http', 'https']:
                proxies.update({item: lazylibrarian.CONFIG['PROXY_HOST']})
    return proxies


def isValidEmail(emails):
    if not emails:
        return False
    elif ',' in emails:
        emails = getList(emails)
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
    st = os.stat(file_or_dir)
    old_perm = oct(st.st_mode)[-3:].zfill(3)
    if old_perm == want_perm:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_fileperms:
            logger.debug("Permission for %s is already %s" % (file_or_dir, want_perm))
        return True

    try:
        os.chmod(file_or_dir, perm)
    except Exception as e:
        logger.debug("Error setting permission %s for %s: %s %s" % (want_perm, file_or_dir, type(e).__name__, str(e)))
        return False

    st = os.stat(file_or_dir)
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
    if 'bts' not in getList(lazylibrarian.CONFIG['SKIPPED_EXT']):
        return ''
    return any_file(search_dir, '.bts')


def csv_file(search_dir=None, library=None):
    if search_dir and path_isdir(search_dir):
        try:
            for fname in listdir(search_dir):
                if fname.endswith('.csv'):
                    if not library or library in fname:
                        return os.path.join(search_dir, fname)
        except Exception as e:
            logger.warn('Listdir error [%s]: %s %s' % (search_dir, type(e).__name__, str(e)))
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
                        return os.path.join(makeUnicode(search_dir), fname)
            except Exception:
                logger.error('Unhandled exception in book_file: %s' % traceback.format_exc())
    return ""


def mimeType(filename):
    name = makeUnicode(filename).lower()
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
    return "application/x-download"


def is_overdue(which="Author"):
    overdue = 0
    total = 0
    name = ''
    ident = ''
    days = 0
    maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
    if maxage:
        myDB = database.DBConnection()
        if which == 'Author':
            cmd = 'SELECT AuthorName,AuthorID,Updated from authors WHERE Status="Active" or Status="Loading"'
            cmd += ' or Status="Wanted" order by Updated ASC'
            res = myDB.select(cmd)
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
        if which == 'Series':
            cmd = 'SELECT SeriesName,SeriesID,Updated from Series where Status="Active" or Status="Wanted"'
            cmd += ' order by Updated ASC'
            res = myDB.select(cmd)
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
    """ Return human readable string of how long ago something happened
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


def nextRun(target=None, interval=0, action='', hours=False):
    """ Check when a job is next due to run and log it
        Return startdate for the job """
    if target is None:
        return ''

    if action == 'StartNow':
        hours = False
        interval = 0

    myDB = database.DBConnection()
    columns = myDB.select('PRAGMA table_info(jobs)')
    if not columns:  # no such table
        lastrun = 0
    else:
        res = myDB.match('SELECT LastRun from jobs WHERE Name=?', (target,))
        if res and res['LastRun'] > 0:
            lastrun = res['LastRun']
        else:
            lastrun = 0

    if target == 'PostProcessor':  # more readable
        newtarget = 'processDir'
    elif target == 'syncToGoodreads':
        newtarget = 'sync_to_gr'
    else:
        newtarget = target

    nextruntime = ''
    for job in lazylibrarian.SCHED.get_jobs():
        if newtarget in str(job):
            nextruntime = job.split('at: ')[1].split('.')[0].strip(')')
            break

    if nextruntime:
        startdate = datetime.datetime.strptime(nextruntime, '%Y-%m-%d %H:%M:%S')
        msg = "%s %s job in %s" % (action, target, next_run(startdate))
    else:
        if hours:
            interval *= 60

        nextrun = lastrun + (interval * 60) - time.time()
        if nextrun < 60:
            nextrun = 60  # overdue, start in 1 minute

        startdate = datetime.datetime.fromtimestamp(time.time() + nextrun)

        nextrun = int(nextrun / 60)
        if nextrun < 1:
            nextrun = 1

        if nextrun <= 120:
            msg = "%s %s job in %s %s" % (action, target, nextrun, plural(nextrun, "minute"))
        else:
            hours = int(nextrun / 60)
            if hours <= 48:
                msg = "%s %s job in %s %s" % (action, target, hours, plural(hours, "hour"))
            else:
                days = int(hours / 24)
                msg = "%s %s job in %s %s" % (action, target, days, plural(days, "day"))
    if lastrun:
        msg += " (Last run %s)" % ago(lastrun)
    logger.debug(msg)

    return startdate


def scheduleJob(action='Start', target=None):
    """ Start or stop or restart a cron job by name eg
        target=search_magazines, target=processDir, target=search_book """
    if target is None:
        return

    if target == 'PostProcessor':  # more readable
        newtarget = 'processDir'
    elif target == 'syncToGoodreads':
        newtarget = 'sync_to_gr'
    else:
        newtarget = target

    if action in ['Stop', 'Restart']:
        for job in lazylibrarian.SCHED.get_jobs():
            if newtarget in str(job):
                lazylibrarian.SCHED.unschedule_job(job)
                logger.debug("Stop %s job" % target)
                break

    if action in ['Start', 'Restart', 'StartNow']:
        for job in lazylibrarian.SCHED.get_jobs():
            if newtarget in str(job):
                logger.debug("%s %s job, already scheduled" % (action, target))
                return  # return if already running, if not, start a new one

        if 'processDir' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SCAN_INTERVAL'], 0)
            if interval:
                startdate = nextRun("POSTPROCESS", interval, action)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.postprocess.cron_processDir,
                                                     minutes=interval, start_date=startdate)

        elif 'search_magazines' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_MAGINTERVAL'], 0)
            if interval and (lazylibrarian.USE_TOR() or lazylibrarian.USE_NZB()
                             or lazylibrarian.USE_RSS() or lazylibrarian.USE_DIRECT()
                             or lazylibrarian.USE_IRC()):
                startdate = nextRun("SEARCHALLMAG", interval, action)
                if interval <= 600:  # for bigger intervals switch to hours
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchmag.cron_search_magazines,
                                                         minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchmag.cron_search_magazines,
                                                         hours=hours, start_date=startdate)
        elif 'search_book' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_BOOKINTERVAL'], 0)
            if interval and (lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR()
                             or lazylibrarian.USE_DIRECT() or lazylibrarian.USE_IRC()):
                startdate = nextRun("SEARCHALLBOOKS", interval, action)
                if interval <= 600:
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchbook.cron_search_book,
                                                         minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchbook.cron_search_book,
                                                         hours=hours, start_date=startdate)
        elif 'search_rss_book' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCHRSS_INTERVAL'], 0)
            if interval and lazylibrarian.USE_RSS():
                startdate = nextRun("SEARCHALLRSS", interval, action)
                if interval <= 600:
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_rss_book,
                                                         minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_rss_book,
                                                         hours=hours, start_date=startdate)
        elif 'search_wishlist' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['WISHLIST_INTERVAL'], 0)
            if interval and lazylibrarian.USE_WISHLIST():
                startdate = nextRun("SEARCHWISHLIST", interval, action, True)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_wishlist,
                                                     hours=interval, start_date=startdate)

        elif 'search_comics' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_COMICINTERVAL'], 0)
            if interval and (lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR()
                             or lazylibrarian.USE_DIRECT() or lazylibrarian.USE_IRC()):
                startdate = nextRun("SEARCHALLCOMICS", interval, action, True)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.comicsearch.cron_search_comics,
                                                     hours=interval, start_date=startdate)

        elif 'checkForUpdates' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'], 0)
            if interval:
                startdate = nextRun("VERSIONCHECK", interval, action, True)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.versioncheck.checkForUpdates,
                                                     hours=interval, start_date=startdate)

        elif 'sync_to_gr' in newtarget and lazylibrarian.CONFIG['GR_SYNC']:
            interval = check_int(lazylibrarian.CONFIG['GOODREADS_INTERVAL'], 0)
            if interval:
                startdate = nextRun("GRSYNC", interval, action, True)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.grsync.cron_sync_to_gr,
                                                     hours=interval, start_date=startdate)

        elif 'cleanCache' in newtarget:
            days = lazylibrarian.CONFIG['CACHE_AGE']
            if days:
                interval = 24
                startdate = nextRun("CLEANCACHE", interval, action, True)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.cache.cleanCache,
                                                     hours=interval, start_date=startdate)

        elif 'authorUpdate' in newtarget or 'seriesUpdate' in newtarget:
            # Try to get all authors/series scanned evenly inside the cache age
            maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
            if maxage:
                pl = ''
                if 'authorUpdate' in target:
                    typ = 'Author'
                    task = 'AUTHORUPDATE'
                else:
                    typ = 'Series'
                    task = 'SERIESUPDATE'

                overdue, total, _, _, days = is_overdue(typ)
                if not overdue:
                    if typ == 'Author':
                        pl = 's'
                    logger.debug("There are no %s%s to update" % (typ, pl))
                    delay = maxage - days
                    if delay > 1:
                        if delay > 7:
                            delay = 8
                        interval = 60 * 24 * (delay - 1)  # nothing today, check again in a few days
                    else:
                        interval = 60
                else:
                    if typ == 'Author' and overdue != 1:
                        pl = 's'
                    if days == maxage:
                        due = "due"
                    else:
                        due = "overdue"
                    logger.debug("Found %s %s%s from %s %s update" % (
                                 overdue, typ, pl, total, due))
                    interval = maxage * 60 * 24
                    interval = int(interval / total)
                    interval -= 5  # average update time

                if interval < 10:  # set a minimum interval of 10 minutes so we don't upset goodreads/librarything api
                    interval = 10

                startdate = nextRun(task, interval, action)
                if interval <= 600:  # for bigger intervals switch to hours
                    if typ == 'Author':
                        lazylibrarian.SCHED.add_interval_job(authorUpdate, minutes=interval, start_date=startdate)
                    else:
                        lazylibrarian.SCHED.add_interval_job(seriesUpdate, minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    if typ == 'Author':
                        lazylibrarian.SCHED.add_interval_job(authorUpdate, hours=hours, start_date=startdate)
                    else:
                        lazylibrarian.SCHED.add_interval_job(seriesUpdate, hours=hours, start_date=startdate)
        else:
            logger.debug("No %s scheduled" % target)


def authorUpdate(restart=True):
    threadname = threading.currentThread().name
    if "Thread-" in threadname:
        threading.currentThread().name = "AUTHORUPDATE"
    # noinspection PyBroadException
    try:
        myDB = database.DBConnection()
        if check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            overdue, total, name, ident, days = is_overdue('Author')
            if not total:
                msg = "There are no monitored authors"
            elif not overdue:
                msg = 'Oldest author info (%s) is %s %s old, no update due' % (name,
                                                                               days, plural(days, "day"))
            else:
                logger.info('Starting update for %s' % name)
                lazylibrarian.importer.addAuthorToDB(refresh=True, authorid=ident, reason="authorUpdate %s" % name)
                if lazylibrarian.STOPTHREADS:
                    return ''
                msg = 'Updated author %s' % name
            myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})
            if total and restart and not lazylibrarian.STOPTHREADS:
                scheduleJob("Restart", "authorUpdate")
            return msg
        return ''
    except Exception:
        logger.error('Unhandled exception in AuthorUpdate: %s' % traceback.format_exc())
        return "Unhandled exception in AuthorUpdate"


def seriesUpdate(restart=True):
    threadname = threading.currentThread().name
    if "Thread-" in threadname:
        threading.currentThread().name = "SERIESUPDATE"
    # noinspection PyBroadException
    try:
        myDB = database.DBConnection()
        if check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            overdue, total, name, ident, days = is_overdue('Series')
            if not total:
                msg = "There are no monitored series"
            elif not overdue:
                msg = 'Oldest series info (%s) is %s %s old, no update due' % (name,
                                                                               days, plural(days, "day"))
            else:
                logger.info('Starting series update for %s' % name)
                lazylibrarian.bookwork.addSeriesMembers(ident)
                msg = 'Updated series %s' % name
            logger.debug(msg)

            myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})
            if total and restart and not lazylibrarian.STOPTHREADS:
                scheduleJob("Restart", "seriesUpdate")
            return msg
        return ''
    except Exception:
        logger.error('Unhandled exception in seriesUpdate: %s' % traceback.format_exc())
        return "Unhandled exception in seriesUpdate"


def aaUpdate(refresh=False):
    # noinspection PyBroadException
    try:
        myDB = database.DBConnection()
        cmd = 'SELECT AuthorID from authors WHERE Status="Active" or Status="Loading" or Status="Wanted"'
        cmd += ' order by Updated ASC'
        activeauthors = myDB.select(cmd)
        lazylibrarian.AUTHORS_UPDATE = 1
        logger.info('Starting update for %i active %s' % (len(activeauthors), plural(len(activeauthors), "author")))
        for author in activeauthors:
            lazylibrarian.importer.addAuthorToDB(refresh=refresh, authorid=author['AuthorID'], reason="aaUpdate")
        logger.info('Active author update complete')
        msg = 'Updated %i active %s' % (len(activeauthors), plural(len(activeauthors), "author"))
        logger.debug(msg)
    except Exception:
        msg = 'Unhandled exception in aaUpdate: %s' % traceback.format_exc()
        logger.error(msg)
    finally:
        lazylibrarian.AUTHORS_UPDATE = 0
    return msg


def restartJobs(start='Restart'):
    lazylibrarian.STOPTHREADS = start == 'Stop'
    for item in ['PostProcessor', 'search_book', 'search_rss_book', 'search_wishlist', 'seriesUpdate',
                 'search_magazines', 'search_comics', 'checkForUpdates', 'authorUpdate', 'syncToGoodreads',
                 'cleanCache']:
        scheduleJob(start, item)


def ensureRunning(jobname):
    lazylibrarian.STOPTHREADS = False
    found = False
    for job in lazylibrarian.SCHED.get_jobs():
        if jobname in str(job):
            found = True
            break
    if not found:
        scheduleJob('Start', jobname)


def checkRunningJobs():
    # make sure the relevant jobs are running
    # search jobs start when something gets marked "wanted" but are
    # not aware of any config changes that happen later, ie enable or disable providers,
    # so we check whenever config is saved
    # postprocessor is started when something gets marked "snatched"
    # and cancels itself once everything is processed so should be ok
    # but check anyway for completeness...

    lazylibrarian.STOPTHREADS = False
    myDB = database.DBConnection()
    snatched = myDB.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    seeding = myDB.match("SELECT count(*) as counter from wanted WHERE Status = 'Seeding'")
    wanted = myDB.match("SELECT count(*) as counter FROM books WHERE Status = 'Wanted'")
    if snatched or seeding:
        ensureRunning('PostProcessor')
    if wanted:
        if lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR() or lazylibrarian.USE_DIRECT() or \
                lazylibrarian.USE_IRC():
            ensureRunning('search_book')
        if lazylibrarian.USE_RSS():
            ensureRunning('search_rss_book')
    else:
        scheduleJob('Stop', 'search_book')
        scheduleJob('Stop', 'search_rss_book')
    if lazylibrarian.USE_WISHLIST():
        ensureRunning('search_wishlist')
    else:
        scheduleJob('Stop', 'search_wishlist')

    if lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR() or lazylibrarian.USE_RSS() or \
            lazylibrarian.USE_DIRECT() or lazylibrarian.USE_IRC():
        ensureRunning('search_magazines')
        ensureRunning('search_comics')
    else:
        scheduleJob('Stop', 'search_magazines')
        scheduleJob('Stop', 'search_comics')

    ensureRunning('authorUpdate')
    ensureRunning('seriesUpdate')


def showStats():
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
                  lazylibrarian.GR_SLEEP, lazylibrarian.LT_SLEEP, lazylibrarian.CV_SLEEP),
              "GoogleBooks API %i calls, %s" % (lazylibrarian.GB_CALLS, gb_status)]

    myDB = database.DBConnection()
    snatched = myDB.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    if snatched['counter']:
        result.append("%i Snatched %s" % (snatched['counter'], plural(snatched['counter'], "item")))
    else:
        result.append("No Snatched items")

    series_stats = []
    res = myDB.match("SELECT count(*) as counter FROM series")
    series_stats.append(['Series', res['counter']])
    res = myDB.match("SELECT count(*) as counter FROM series WHERE Total>0 and Have=0")
    series_stats.append(['Empty', res['counter']])
    res = myDB.match("SELECT count(*) as counter FROM series WHERE Total>0 AND Have=Total")
    series_stats.append(['Full', res['counter']])
    res = myDB.match('SELECT count(*) as counter FROM series WHERE Status="Ignored"')
    series_stats.append(['Ignored', res['counter']])
    res = myDB.match("SELECT count(*) as counter FROM series WHERE Total=0")
    series_stats.append(['Blank', res['counter']])
    res = myDB.match("SELECT count(*) as counter FROM series WHERE Updated>0")
    series_stats.append(['Monitor', res['counter']])
    overdue = is_overdue('Series')[0]
    series_stats.append(['Overdue', overdue])

    mag_stats = []
    if lazylibrarian.SHOW_MAGS:
        res = myDB.match("SELECT count(*) as counter FROM magazines")
        mag_stats.append(['Magazine', res['counter']])
        res = myDB.match("SELECT count(*) as counter FROM issues")
        mag_stats.append(['Issues', res['counter']])
        cmd = 'select (select count(*) as counter from issues where magazines.title = issues.title) '
        cmd += 'as counter from magazines where counter=0'
        res = myDB.match(cmd)
        mag_stats.append(['Empty', len(res)])

    if lazylibrarian.SHOW_COMICS:
        res = myDB.match("SELECT count(*) as counter FROM comics")
        mag_stats.append(['Comics', res['counter']])
        res = myDB.match("SELECT count(*) as counter FROM comicissues")
        mag_stats.append(['Issues', res['counter']])
        cmd = 'select (select count(*) as counter from comicissues where comics.comicid = comicissues.comicid) '
        cmd += 'as counter from comics where counter=0'
        res = myDB.match(cmd)
        mag_stats.append(['Empty', len(res)])

    book_stats = []
    audio_stats = []
    missing_stats = []
    res = myDB.match("SELECT count(*) as counter FROM books")
    book_stats.append(['eBooks', res['counter']])
    audio_stats.append(['Audio', res['counter']])
    for status in ['Have', 'Open', 'Wanted', 'Ignored']:
        res = myDB.match('SELECT count(*) as counter FROM books WHERE Status="%s"' % status)
        book_stats.append([status, res['counter']])
        res = myDB.match('SELECT count(*) as counter FROM books WHERE AudioStatus="%s"' % status)
        audio_stats.append([status, res['counter']])
    for column in ['BookGenre', 'BookDesc']:
        cmd = "SELECT count(*) as counter FROM books WHERE Status != 'Ignored' and "
        cmd += "(%s is null or %s = '')"
        res = myDB.match(cmd % (column, column))
        missing_stats.append([column.replace('Book', 'No'), res['counter']])
    cmd = "SELECT count(*) as counter FROM books WHERE Status != 'Ignored' and BookGenre='Unknown'"
    res = myDB.match(cmd)
    missing_stats.append(['X_Genre', res['counter']])
    cmd = "SELECT count(*) as counter FROM books WHERE Status != 'Ignored' and BookDesc='No Description'"
    res = myDB.match(cmd)
    missing_stats.append(['X_Desc', res['counter']])
    for column in ['BookISBN', 'BookLang']:
        cmd = "SELECT count(*) as counter FROM books WHERE "
        cmd += "(%s is null or %s = '' or %s = 'Unknown')"
        res = myDB.match(cmd % (column, column, column))
        missing_stats.append([column.replace('Book', 'No'), res['counter']])
    cmd = "SELECT count(*) as counter FROM genres"
    res = myDB.match(cmd)
    missing_stats.append(['Genres', res['counter']])

    if not lazylibrarian.SHOW_AUDIO:
        audio_stats = []

    author_stats = []
    res = myDB.match("SELECT count(*) as counter FROM authors")
    author_stats.append(['Authors', res['counter']])
    for status in ['Active', 'Wanted', 'Ignored', 'Paused']:
        res = myDB.match('SELECT count(*) as counter FROM authors WHERE Status="%s"' % status)
        author_stats.append([status, res['counter']])
    res = myDB.match("SELECT count(*) as counter FROM authors WHERE HaveBooks=0")
    author_stats.append(['Empty', res['counter']])
    res = myDB.match("SELECT count(*) as counter FROM authors WHERE TotalBooks=0")
    author_stats.append(['Blank', res['counter']])
    overdue = is_overdue('Author')[0]
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


def showJobs():
    result = []
    myDB = database.DBConnection()
    for job in lazylibrarian.SCHED.get_jobs():
        job = str(job)
        if "search_magazines" in job:
            jobname = "Magazine search"
            threadname = "SEARCHALLMAG"
        elif "search_comics" in job:
            jobname = "Comic search"
            threadname = "SEARCHALLCOMICS"
        elif "checkForUpdates" in job:
            jobname = "Check for Update"
            threadname = "VERSIONCHECK"
        elif "search_book" in job:
            jobname = "Book search"
            threadname = "SEARCHALLBOOKS"
        elif "search_rss_book" in job:
            jobname = "RSS book search"
            threadname = "SEARCHALLRSS"
        elif "search_wishlist" in job:
            jobname = "Wishlist search"
            threadname = "SEARCHWISHLIST"
        elif "PostProcessor" in job:
            jobname = "PostProcessor"
            threadname = "POSTPROCESS"
        elif "cron_processDir" in job:
            jobname = "PostProcessor"
            threadname = "POSTPROCESS"
        elif "authorUpdate" in job:
            jobname = "Update authors"
            threadname = "AUTHORUPDATE"
        elif "seriesUpdate" in job:
            jobname = "Update series"
            threadname = "SERIESUPDATE"
        elif "sync_to_gr" in job:
            jobname = "Goodreads Sync"
            threadname = "GRSYNC"
        elif "cleanCache" in job:
            jobname = "Clean cache"
            threadname = "CLEANCACHE"
        else:
            jobname = job.split(' ')[0].split('.')[2]
            threadname = jobname.upper()

        # jobinterval = job.split('[')[1].split(']')[0]
        jobtime = job.split('at: ')[1].split('.')[0].strip(')')
        jobtime = next_run(jobtime)
        timeparts = jobtime.split(' ')
        if timeparts[0] == '1' and timeparts[1].endswith('s'):
            timeparts[1] = timeparts[1][:-1]
        jobinfo = "%s: Next run in %s %s" % (jobname, timeparts[0], timeparts[1])
        res = myDB.match('SELECT LastRun from jobs WHERE Name="%s"' % threadname)
        if res and res['LastRun'] > 0:
            jobinfo += " (Last run %s)" % ago(res['LastRun'])
        result.append(jobinfo)

    result.append(' ')
    overdue, total, name, _, days = is_overdue('Author')
    if name:
        result.append('Oldest author info (%s) is %s %s old' % (name, days, plural(days, "day")))
    if not overdue:
        result.append("There are no authors needing update")
    elif days == check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
        result.append("Found %s %s from %s due update" % (overdue, plural(overdue, "author"), total))
    else:
        result.append("Found %s %s from %s overdue update" % (overdue, plural(overdue, "author"), total))

    overdue, total, name, _, days = is_overdue('Series')
    if name:
        result.append('Oldest series info (%s) is %s %s old' % (name, days, plural(days, "day")))
    if not overdue:
        result.append("There are no series needing update")
    elif days == check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
        result.append("Found %s series from %s due update" % (overdue, total))
    else:
        result.append("Found %s series from %s overdue update" % (overdue, total))
    return result


def clearLog():
    lazylibrarian.LOGLIST = []
    error = False
    if os.name == 'nt':
        return "Screen log cleared"

    logger.lazylibrarian_log.stopLogger()
    for f in glob.glob(lazylibrarian.CONFIG['LOGDIR'] + "/*.log*"):
        try:
            os.remove(syspath(f))
        except OSError as e:
            error = e.strerror
            logger.debug("Failed to remove %s : %s" % (f, error))

    logger.lazylibrarian_log.initLogger(loglevel=lazylibrarian.LOGLEVEL)

    if error:
        return 'Failed to clear logfiles: %s' % error
    else:
        return "Log cleared, level set to [%s]- Log Directory is [%s]" % (
            lazylibrarian.LOGLEVEL, lazylibrarian.CONFIG['LOGDIR'])


# noinspection PyUnresolvedReferences
def logHeader():
    popen_list = [sys.executable, lazylibrarian.FULL_PATH]
    popen_list += lazylibrarian.ARGS
    header = "Startup cmd: %s\n" % str(popen_list)
    header += 'Interface: %s\n' % lazylibrarian.CONFIG['HTTP_LOOK']
    header += 'Loglevel: %s\n' % lazylibrarian.LOGLEVEL
    header += 'Sys_Encoding: %s\n' % lazylibrarian.SYS_ENCODING
    for item in lazylibrarian.CONFIG_GIT:
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
    myDB = database.DBConnection()
    result = myDB.match('PRAGMA user_version')
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
    except Exception as e:
        tls_version = str(e)
    header += "tls: %s\n" % tls_version
    header += "cherrypy: %s\n" % getattr(cherrypy, '__version__', None)
    if not lazylibrarian.FOREIGN_KEY:
        # 3.6.19 is the earliest version with FOREIGN_KEY which we use, but is not essential
        header += 'sqlite3: missing required functionality. Try upgrading to v3.6.19 or newer. You have '
    header += "sqlite3: %s\n" % getattr(sqlite3, 'sqlite_version', None)

    if lazylibrarian.UNRARLIB == 1:
        vers = lazylibrarian.RARFILE.unrarlib.RARGetDllVersion()
        header += "unrar: library version %s\n" % vers
    elif lazylibrarian.UNRARLIB == 2:
        import lib.UnRAR2 as UnRAR2
        vers = getattr(UnRAR2, '__version__', None)
        header += "unrar2: library version %s\n" % vers
        if os.name == 'nt':
            vers = UnRAR2.windows.RARGetDllVersion()
            header += "windows dll version %s\n" % vers
    else:
        header += "unrar: library missing\n"

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
        except (ImportError, AttributeError) as e:
            header += 'pyOpenSSL missing SSL module/attribute: %s\n' % e

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
        import fuzzywuzzy
        vers = fuzzywuzzy.__dict__['__version__']
    except Exception:
        # noinspection PyBroadException
        try:
            import lib.fuzzywuzzy as fuzzywuzzy
            vers = fuzzywuzzy.__dict__['__version__']
        except Exception:
            vers = 'None'
    if vers:
        header += "fuzzywuzzy: %s\n" % vers
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


def saveLog():
    if not path_exists(lazylibrarian.CONFIG['LOGDIR']):
        return 'LOGDIR does not exist'

    basename = os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'lazylibrarian.log')
    outfile = os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'debug')
    passchars = string.ascii_letters + string.digits + ':_/*!%'  # used by slack, telegram and googlebooks
    redactlist = ['api -> ', 'key -> ', 'secret -> ', 'pass -> ', 'password -> ', 'token -> ', 'keys -> ',
                  'apitoken -> ', 'username -> ', '&r=', 'using api [', 'apikey=', 'key=', 'apikey%3D', "apikey': ",
                  "'--password', u'", "'--password', '", "api:", "keys:", "token:", "secret=", "email_from -> ",
                  "'--password', u\"", "'--password', \"", "email_to -> ", "email_smtp_user -> "]
    with open(syspath(outfile + '.tmp'), 'w') as out:
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
                    lines = reversed(open(fname).readlines())
                else:
                    lines = reversed(list(open(fname)))
                for line in lines:
                    for item in redactlist:
                        startpos = line.find(item)
                        if startpos >= 0:
                            startpos += len(item)
                            endpos = startpos
                            while endpos < len(line) and not line[endpos] in passchars:
                                endpos += 1
                            while endpos < len(line) and line[endpos] in passchars:
                                endpos += 1
                            if endpos != startpos:
                                line = line[:startpos] + '<redacted>' + line[endpos:]
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
            out.write('---END-CONFIG---------------------------------\n')
            if PY2:
                lines = reversed(open(lazylibrarian.CONFIGFILE).readlines())
            else:
                lines = reversed(list(open(lazylibrarian.CONFIGFILE)))
            for line in lines:
                for item in redactlist:
                    item = item.replace('->', '=')
                    startpos = line.find(item)
                    if startpos >= 0:
                        startpos += len(item)
                        endpos = startpos
                        while endpos < len(line) and not line[endpos] in passchars:
                            endpos += 1
                        while endpos < len(line) and line[endpos] in passchars:
                            endpos += 1
                        if endpos != startpos:
                            line = line[:startpos] + '<redacted>' + line[endpos:]
                            redacts += 1
                out.write(line)
            out.write('---CONFIG-------------------------------------\n')

    with open(syspath(outfile + '.log'), 'w') as logfile:
        logfile.write(logHeader())
        linecount = 0
        if PY2:
            lines = reversed(open(outfile + '.tmp').readlines())
        else:
            lines = reversed(list(open(outfile + '.tmp')))
        for line in lines:
            logfile.write(line)
            linecount += 1
    os.remove(syspath(outfile + '.tmp'))
    logger.debug("Redacted %s passwords/apikeys" % redacts)
    logger.debug("%s log lines written to %s" % (linecount, outfile + '.log'))
    with zipfile.ZipFile(outfile + '.zip', 'w') as myzip:
        myzip.write(outfile + '.log', 'debug.log')
    os.remove(syspath(outfile + '.log'))
    return "Debug log saved as %s" % (outfile + '.zip')


def zipAudio(source, zipname):
    """ Zip up all the audiobook parts in source folder to zipname
        Check if zipfile already exists, if not create a new one
        Doesn't actually check for audiobook parts, just zips everything
        including any .jpg etc
        Return full path to zipfile
    """
    zip_file = os.path.join(source, zipname + '.zip')
    if not path_exists(zip_file):
        logger.debug('Zipping up %s' % zipname)
        cnt = 0
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as myzip:
            for rootdir, _, filenames in walk(source):
                for filename in filenames:
                    # don't include self or our special index file
                    if not filename.endswith('.zip') and not filename.endswith('.ll'):
                        cnt += 1
                        myzip.write(os.path.join(rootdir, filename), filename)
        logger.debug('Zipped up %s files' % cnt)
    return zip_file


def runScript(params):
    if os.name == 'nt' and params[0].endswith('.py'):
        params.insert(0, sys.executable)
    logger.debug(str(params))
    try:
        if os.name != 'nt':
            p = Popen(params, preexec_fn=lambda: os.nice(10), stdout=PIPE, stderr=PIPE)
        else:
            p = Popen(params, stdout=PIPE, stderr=PIPE)
        res, err = p.communicate()
        return p.returncode, makeUnicode(res), makeUnicode(err)
    except Exception as e:
        err = "runScript exception: %s %s" % (type(e).__name__, str(e))
        logger.error(err)
        return 1, '', err
