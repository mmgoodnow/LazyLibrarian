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
#
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import os
import glob
import sys
import platform
import string
import random
import shutil
import threading
import time
import traceback
from lib.six import PY2, text_type
from subprocess import Popen, PIPE

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

# some mac versions include requests _without_ urllib3, our copy bundles it
try:
    # noinspection PyUnresolvedReferences
    import urllib3
    import requests
except ImportError:
    import lib.requests as requests

import lazylibrarian
from lazylibrarian import logger, database, version
from lazylibrarian.formatter import plural, next_run, is_valid_booktype, datecompare, check_int, \
    getList, makeUnicode, unaccented, replace_all, makeBytestr

# Notification Types
NOTIFY_SNATCH = 1
NOTIFY_DOWNLOAD = 2

notifyStrings = {NOTIFY_SNATCH: "Started Download", NOTIFY_DOWNLOAD: "Added to Library"}

# dict to remove/replace characters we don't want in a filename - this might be too strict?
__dic__ = {'<': '', '>': '', '...': '', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', '|': '',
           ' + ': ' ', '"': '', ',': '', '*': '', ':': '', ';': '', '\'': '', '//': '/', '\\\\': '\\'}


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
            flist = [makeUnicode(item) for item in f]
            for item in filetypes:
                counter = 0
                for fname in flist:
                    if fname.endswith(item):
                        counter += 1
                        if counter > 1:
                            return item
    else:
        flist = os.listdir(makeBytestr(foldername))
        flist = [makeUnicode(item) for item in flist]
        for item in filetypes:
            counter = 0
            for fname in flist:
                if fname.endswith(item):
                    counter += 1
                    if counter > 1:
                        return item
    return ''


def walk(top, topdown=True, onerror=None, followlinks=False):
    """
    duplicate of os.walk, except we do a forced decode to utf-8 bytes after listdir
    """
    islink, join, isdir = os.path.islink, os.path.join, os.path.isdir

    try:
        top = makeBytestr(top)
        names = os.listdir(top)
        names = [makeBytestr(name) for name in names]
    except os.error as err:
        if onerror is not None:
            onerror(err)
        return

    dirs, nondirs = [], []
    for name in names:
        if isdir(join(top, name)):
            dirs.append(name)
        else:
            nondirs.append(name)

    if topdown:
        yield top, dirs, nondirs
    for name in dirs:
        new_path = join(top, name)
        if followlinks or not islink(new_path):
            for x in walk(new_path, topdown, onerror, followlinks):
                yield x
    if not topdown:
        yield top, dirs, nondirs


def make_dirs(dest_path):
    """ os.makedirs only seems to set the right permission on the final leaf directory
        not any intermediate parents it creates on the way, so we'll try to do it ourselves
        setting permissions as we go. Could use recursion but probably aren't many levels to do...
        Build a list of missing intermediate directories in reverse order, exit when we encounter
        an existing directory or hit root level. Set permission on any directories we create.
        return True or False """

    to_make = []
    while not os.path.isdir(dest_path):
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
            elif os.path.isdir(src) and dst.startswith(src):
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
                newpath = replace_all(path, __dic__)
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

    if os.path.isdir(file_or_dir):
        perm = octal(lazylibrarian.CONFIG['DIR_PERM'], 0o755)
    elif os.path.isfile(file_or_dir):
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
    if os.path.isdir(search_dir):
        for fname in os.listdir(makeBytestr(search_dir)):
            fname = makeUnicode(fname)
            if fname.endswith(extn):
                return os.path.join(search_dir, fname)
    return ""


def opf_file(search_dir=None):
    if search_dir is None:
        return ""
    cnt = 0
    res = ''
    meta = ''
    if os.path.isdir(search_dir):
        for fname in os.listdir(makeBytestr(search_dir)):
            fname = makeUnicode(fname)
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
    if search_dir and os.path.isdir(search_dir):
        try:
            for fname in os.listdir(makeBytestr(search_dir)):
                fname = makeUnicode(fname)
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
    # return full pathname of book/mag, or empty string if none found
    if booktype is None:
        return ""

    if os.path.isdir(search_dir):
        if recurse:
            # noinspection PyBroadException
            try:
                for r, _, f in walk(search_dir):
                    for item in f:
                        if is_valid_booktype(makeUnicode(item), booktype=booktype):
                            return os.path.join(r, item)
            except Exception:
                logger.error('Unhandled exception in book_file: %s' % traceback.format_exc())
        else:
            # noinspection PyBroadException
            try:
                for fname in os.listdir(makeBytestr(search_dir)):
                    if is_valid_booktype(makeUnicode(fname), booktype=booktype):
                        return os.path.join(makeBytestr(search_dir), fname)
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
    days = 0
    maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
    if maxage:
        myDB = database.DBConnection()
        if which == 'Author':
            cmd = 'SELECT AuthorName,DateAdded from authors WHERE Status="Active" or Status="Loading"'
            cmd += ' or Status="Wanted" and DateAdded is not null order by DateAdded ASC'
            res = myDB.select(cmd)
            total = len(res)
            if total:
                name = res[0]['AuthorName']
                dtnow = datetime.datetime.now()
                days = datecompare(dtnow.strftime("%Y-%m-%d"), res[0]['DateAdded'])
                for item in res:
                    diff = datecompare(dtnow.strftime("%Y-%m-%d"), item['DateAdded'])
                    if diff > maxage:
                        overdue += 1
                    else:
                        break
        if which == 'Series':
            cmd = 'SELECT SeriesName,Updated from Series where Updated > 0 order by Updated ASC'
            res = myDB.select(cmd)
            total = len(res)
            if total:
                name = res[0]['SeriesName']
                dtnow = time.time()
                days = int((dtnow - res[0]['Updated']) / (24 * 60 * 60))
                for item in res:
                    diff = (dtnow - item['Updated']) / (24 * 60 * 60)
                    if diff > maxage:
                        overdue += 1
                    else:
                        break
    return overdue, total, name, days


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
    else:
        return "%i second%s ago" % (seconds, plural(seconds))


def scheduleJob(action='Start', target=None):
    """ Start or stop or restart a cron job by name eg
        target=search_magazines, target=processDir, target=search_book """
    if target is None:
        return

    myDB = database.DBConnection()
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

    if action in ['Start', 'Restart']:
        for job in lazylibrarian.SCHED.get_jobs():
            if newtarget in str(job):
                logger.debug("%s %s job, already scheduled" % (action, target))
                return  # return if already running, if not, start a new one

        if action == 'Start':
            soon = datetime.datetime.fromtimestamp(time.time() + 60)
        else:
            soon = None

        if 'processDir' in newtarget and check_int(lazylibrarian.CONFIG['SCAN_INTERVAL'], 0):
            minutes = check_int(lazylibrarian.CONFIG['SCAN_INTERVAL'], 0)
            lazylibrarian.SCHED.add_interval_job(lazylibrarian.postprocess.cron_processDir,
                                                 minutes=minutes, start_date=soon)
            if soon:
                minutes = 1
            msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
            res = myDB.match('SELECT LastRun from jobs WHERE Name="POSTPROCESS"')
            if res and res['LastRun'] > 0:
                msg += " (Last run %s)" % ago(res['LastRun'])
            logger.debug(msg)
        elif 'search_magazines' in newtarget and check_int(lazylibrarian.CONFIG['SEARCH_MAGINTERVAL'], 0):
            minutes = check_int(lazylibrarian.CONFIG['SEARCH_MAGINTERVAL'], 0)
            if lazylibrarian.USE_TOR() or lazylibrarian.USE_NZB() \
                    or lazylibrarian.USE_RSS() or lazylibrarian.USE_DIRECT():
                if minutes <= 600:  # for bigger intervals switch to hours
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchmag.cron_search_magazines,
                                                         minutes=minutes, start_date=soon)
                    if soon:
                        minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                else:
                    hours = int(minutes / 60)
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchmag.cron_search_magazines,
                                                         hours=hours, start_date=soon)
                    if soon:
                        minutes = 1
                        msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                    elif hours <= 48:
                        msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                    else:
                        days = int(hours / 24)
                        msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                res = myDB.match('SELECT LastRun from jobs WHERE Name="SEARCHALLMAG"')
                if res and res['LastRun'] > 0:
                    msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)

        elif 'search_book' in newtarget and check_int(lazylibrarian.CONFIG['SEARCH_BOOKINTERVAL'], 0):
            minutes = check_int(lazylibrarian.CONFIG['SEARCH_BOOKINTERVAL'], 0)
            if lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR() or lazylibrarian.USE_DIRECT():
                if minutes <= 600:
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchbook.cron_search_book,
                                                         minutes=minutes, start_date=soon)
                    if soon:
                        minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                else:
                    hours = int(minutes / 60)
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchbook.cron_search_book,
                                                         hours=hours, start_date=soon)
                    if soon:
                        minutes = 1
                        msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                    elif hours <= 48:
                        msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                    else:
                        days = int(hours / 24)
                        msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                res = myDB.match('SELECT LastRun from jobs WHERE Name="SEARCHALLBOOKS"')
                if res and res['LastRun'] > 0:
                    msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)

        elif 'search_rss_book' in newtarget and check_int(lazylibrarian.CONFIG['SEARCHRSS_INTERVAL'], 0):
            if lazylibrarian.USE_RSS():
                minutes = check_int(lazylibrarian.CONFIG['SEARCHRSS_INTERVAL'], 0)
                if minutes <= 600:
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_rss_book,
                                                         minutes=minutes, start_date=soon)
                    if soon:
                        minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                else:
                    hours = int(minutes / 60)
                    lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_rss_book,
                                                         hours=hours, start_date=soon)
                    if soon:
                        minutes = 1
                        msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                    elif hours <= 48:
                        msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                    else:
                        days = int(hours / 24)
                        msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                res = myDB.match('SELECT LastRun from jobs WHERE Name="SEARCHALLRSS"')
                if res and res['LastRun'] > 0:
                    msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)

        elif 'search_wishlist' in newtarget and check_int(lazylibrarian.CONFIG['WISHLIST_INTERVAL'], 0):
            if lazylibrarian.USE_WISHLIST():
                hours = check_int(lazylibrarian.CONFIG['WISHLIST_INTERVAL'], 0)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.searchrss.cron_search_wishlist,
                                                     hours=hours, start_date=soon)
                if soon:
                    minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                elif hours <= 48:
                    msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                else:
                    days = int(hours / 24)
                    msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                res = myDB.match('SELECT LastRun from jobs WHERE Name="SEARCHWISHLIST"')
                if res and res['LastRun'] > 0:
                    msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)

        elif 'search_comics' in newtarget and check_int(lazylibrarian.CONFIG['SEARCH_COMICINTERVAL'], 0):
            if lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR() or lazylibrarian.USE_DIRECT():
                hours = check_int(lazylibrarian.CONFIG['SEARCH_COMICINTERVAL'], 0)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.comicsearch.cron_search_comics,
                                                     hours=hours, start_date=soon)
                if soon:
                    minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                elif hours <= 48:
                    msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                else:
                    days = int(hours / 24)
                    msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                res = myDB.match('SELECT LastRun from jobs WHERE Name="SEARCHALLCOMICS"')
                if res and res['LastRun'] > 0:
                    msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)

        elif 'checkForUpdates' in newtarget and check_int(lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'], 0):
            hours = check_int(lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'], 0)
            lazylibrarian.SCHED.add_interval_job(lazylibrarian.versioncheck.checkForUpdates,
                                                 hours=hours, start_date=soon)
            if soon:
                minutes = 1
                msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
            elif hours <= 48:
                msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
            else:
                days = int(hours / 24)
                msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
            res = myDB.match('SELECT LastRun from jobs WHERE Name="VERSIONCHECK"')
            if res and res['LastRun'] > 0:
                msg += " (Last run %s)" % ago(res['LastRun'])
            logger.debug(msg)

        elif 'sync_to_gr' in newtarget and lazylibrarian.CONFIG['GR_SYNC']:
            if check_int(lazylibrarian.CONFIG['GOODREADS_INTERVAL'], 0):
                hours = check_int(lazylibrarian.CONFIG['GOODREADS_INTERVAL'], 0)
                lazylibrarian.SCHED.add_interval_job(lazylibrarian.grsync.cron_sync_to_gr, hours=hours,
                                                     start_date=soon)
                if soon:
                    minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                elif hours <= 48:
                    msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                else:
                    days = int(hours / 24)
                    msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                res = myDB.match('SELECT LastRun from jobs WHERE Name="GRSYNC"')
                if res and res['LastRun'] > 0:
                    msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)

        elif ('authorUpdate' in newtarget or 'seriesUpdate' in newtarget) and \
                check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            # Try to get all authors/series scanned evenly inside the cache age
            maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
            if maxage:
                pl = ''
                if 'authorUpdate' in target:
                    typ = 'Author'
                else:
                    typ = 'Series'

                overdue, total, _, days = is_overdue(typ)
                if not overdue:
                    if typ == 'Author':
                        pl = 's'
                    logger.debug("There are no %s%s to update" % (typ, pl))
                    delay = maxage - days
                    if delay > 1:
                        if delay > 7:
                            delay = 8
                        minutes = 60 * 24 * (delay - 1)  # nothing today, check again in a few days
                    else:
                        minutes = 60
                else:
                    if typ == 'Author' and overdue != 1:
                        pl = 's'
                    logger.debug("Found %s %s%s from %s overdue update" % (
                                 overdue, typ, pl, total))
                    minutes = maxage * 60 * 24
                    minutes = int(minutes / total)
                    minutes -= 5  # average update time

                if minutes < 10:  # set a minimum interval of 10 minutes so we don't upset goodreads/librarything api
                    minutes = 10
                if minutes <= 600:  # for bigger intervals switch to hours
                    if typ == 'Author':
                        lazylibrarian.SCHED.add_interval_job(authorUpdate, minutes=minutes, start_date=soon)
                    else:
                        lazylibrarian.SCHED.add_interval_job(seriesUpdate, minutes=minutes, start_date=soon)
                    if soon:
                        minutes = 1
                    msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                else:
                    hours = int(minutes / 60)
                    if typ == 'Author':
                        lazylibrarian.SCHED.add_interval_job(authorUpdate, hours=hours, start_date=soon)
                    else:
                        lazylibrarian.SCHED.add_interval_job(seriesUpdate, hours=hours, start_date=soon)
                    if soon:
                        minutes = 1
                        msg = "%s %s job in %s minute%s" % (action, target, minutes, plural(minutes))
                    elif hours <= 48:
                        msg = "%s %s job in %s hour%s" % (action, target, hours, plural(hours))
                    else:
                        days = int(hours / 24)
                        msg = "%s %s job in %s day%s" % (action, target, days, plural(days))
                    res = myDB.match('SELECT LastRun from jobs WHERE Name="%sUPDATE"' % typ.upper())
                    if res and res['LastRun'] > 0:
                        msg += " (Last run %s)" % ago(res['LastRun'])
                logger.debug(msg)
            else:
                logger.debug("No %s scheduled" % target)


def authorUpdate(restart=True):
    threadname = threading.currentThread().name
    if "Thread-" in threadname:
        threading.currentThread().name = "AUTHORUPDATE"
    # noinspection PyBroadException
    try:
        myDB = database.DBConnection()
        cmd = 'SELECT AuthorID, AuthorName, DateAdded from authors WHERE Status="Active" or Status="Loading"'
        cmd += ' or Status="Wanted" and DateAdded is not null order by DateAdded ASC'
        author = myDB.match(cmd)
        if author and check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            dtnow = datetime.datetime.now()
            diff = datecompare(dtnow.strftime("%Y-%m-%d"), author['DateAdded'])
            msg = 'Oldest author info (%s) is %s day%s old, no update due' % (author['AuthorName'],
                                                                              diff, plural(diff))
            if diff > check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
                logger.info('Starting update for %s' % author['AuthorName'])
                lazylibrarian.importer.addAuthorToDB(refresh=True, authorid=author['AuthorID'])
                msg = 'Updated author %s' % author['AuthorName']
            else:
                logger.debug(msg)

            myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})
            if restart:
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
        cmd = 'SELECT SeriesName,SeriesID,Updated from Series where '
        cmd += 'Status != "Ignored" and Status != "Skipped" and Updated > 0 order by Updated ASC'
        res = myDB.match(cmd)
        if res and check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            name = res['SeriesName']
            dtnow = time.time()
            diff = int((dtnow - res['Updated']) / (24 * 60 * 60))
            msg = 'Oldest series info (%s) is %s day%s old, no update due' % (name,
                                                                              diff, plural(diff))
            if diff > check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
                logger.info('Starting series update for %s' % name)
                lazylibrarian.bookwork.addSeriesMembers(res['SeriesID'])
                msg = 'Updated series %s' % name
            logger.debug(msg)

            myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})
            if restart:
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
        cmd += ' order by DateAdded ASC'
        activeauthors = myDB.select(cmd)
        lazylibrarian.AUTHORS_UPDATE = True
        logger.info('Starting update for %i active author%s' % (len(activeauthors), plural(len(activeauthors))))
        for author in activeauthors:
            lazylibrarian.importer.addAuthorToDB(refresh=refresh, authorid=author['AuthorID'])
        logger.info('Active author update complete')
        lazylibrarian.AUTHORS_UPDATE = False
        msg = 'Updated %i active author%s' % (len(activeauthors), plural(len(activeauthors)))
        logger.debug(msg)
    except Exception:
        lazylibrarian.AUTHORS_UPDATE = False
        msg = 'Unhandled exception in aaUpdate: %s' % traceback.format_exc()
        logger.error(msg)
    return msg


def restartJobs(start='Restart'):
    for item in ['PostProcessor', 'search_book', 'search_rss_book', 'search_wishlist', 'seriesUpdate',
                 'search_magazines', 'search_comics', 'checkForUpdates', 'authorUpdate', 'syncToGoodreads']:
        scheduleJob(start, item)


def ensureRunning(jobname):
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

    myDB = database.DBConnection()
    snatched = myDB.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    seeding = myDB.match("SELECT count(*) as counter from wanted WHERE Status = 'Seeding'")
    wanted = myDB.match("SELECT count(*) as counter FROM books WHERE Status = 'Wanted'")
    if snatched or seeding:
        ensureRunning('PostProcessor')
    if wanted:
        if lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR() or lazylibrarian.USE_DIRECT():
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

    if lazylibrarian.USE_NZB() or lazylibrarian.USE_TOR() or lazylibrarian.USE_RSS() or lazylibrarian.USE_DIRECT():
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

    result = ["Cache %i hit%s, %i miss, " % (check_int(lazylibrarian.CACHE_HIT, 0),
                                             plural(check_int(lazylibrarian.CACHE_HIT, 0)),
                                             check_int(lazylibrarian.CACHE_MISS, 0)),
              "Sleep %.3f goodreads, %.3f librarything, %.3f comicvine" % (
                  lazylibrarian.GR_SLEEP, lazylibrarian.LT_SLEEP, lazylibrarian.CV_SLEEP),
              "GoogleBooks API %i calls, %s" % (lazylibrarian.GB_CALLS, gb_status)]

    myDB = database.DBConnection()
    snatched = myDB.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    if snatched['counter']:
        result.append("%i Snatched item%s" % (snatched['counter'], plural(snatched['counter'])))
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
    overdue, _, _, _ = is_overdue('Series')
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
    overdue, _, _, _ = is_overdue('Author')
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
            jobname = "Check LazyLibrarian version"
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

    overdue, total, name, days = is_overdue('Author')
    if name:
        result.append('Oldest author info (%s) is %s day%s old' % (name, days, plural(days)))
    if not overdue:
        result.append("There are no authors overdue update")
    else:
        result.append("Found %s author%s from %s overdue update" % (overdue, plural(overdue), total))

    overdue, total, name, days = is_overdue('Series')
    if name:
        result.append('Oldest series info (%s) is %s day%s old' % (name, days, plural(days)))
    if not overdue:
        result.append("There are no series overdue update")
    else:
        result.append("Found %s series from %s overdue update" % (overdue, total))
    return result


def clearLog():
    lazylibrarian.LOGLIST = []
    error = False
    if 'windows' in platform.system().lower():
        return "Screen log cleared"

    logger.lazylibrarian_log.stopLogger()
    for f in glob.glob(lazylibrarian.CONFIG['LOGDIR'] + "/*.log*"):
        try:
            os.remove(f)
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
        if lazylibrarian.CONFIG['SSL_CERTS']:
            tls_version = requests.get('https://www.howsmyssl.com/a/check', timeout=30,
                                       verify=lazylibrarian.CONFIG['SSL_CERTS']).json()['tls_version']
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
    rarfile = None
    UnRAR2 = None
    # noinspection PyBroadException
    try:
        from unrar import rarfile
        unrarlib = 1
    except Exception:
        # noinspection PyBroadException
        try:
            from lib.unrar import rarfile
            unrarlib = 1
        except Exception:
            unrarlib = 0
    if not unrarlib:
        # noinspection PyBroadException
        try:
            import lib.UnRAR2 as UnRAR2
            unrarlib = 2
        except Exception as e:
            header += "unrar: missing: %s\n" % str(e)
            unrarlib = 0
    if unrarlib:
        if unrarlib == 1:
            vers = rarfile.unrarlib.RARGetDllVersion()
            header += "unrar: library version %s\n" % vers
        elif unrarlib == 2:
            vers = getattr(UnRAR2, '__version__', None)
            header += "unrar2: library version %s\n" % vers
            if platform.system() == "Windows":
                vers = UnRAR2.windows.RARGetDllVersion()
                header += "windows dll version %s\n" % vers

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
    if not os.path.exists(lazylibrarian.CONFIG['LOGDIR']):
        return 'LOGDIR does not exist'

    basename = os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'lazylibrarian.log')
    outfile = os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'debug')
    passchars = string.ascii_letters + string.digits + ':_/'  # used by slack, telegram and googlebooks
    redactlist = ['api -> ', 'key -> ', 'secret -> ', 'pass -> ', 'password -> ', 'token -> ', 'keys -> ',
                  'apitoken -> ', 'username -> ', '&r=', 'using api [', 'apikey=', 'key=', 'apikey%3D', "apikey': ",
                  "'--password', u'", "'--password', '", "api:", "keys:", "token:", "secret=", "email_from -> ",
                  "'--password', u\"", "'--password', \"", "email_to -> ", "email_smtp_user -> "]
    with open(outfile + '.tmp', 'w') as out:
        nextfile = True
        extn = 0
        redacts = 0
        while nextfile:
            fname = basename
            if extn > 0:
                fname = fname + '.' + str(extn)
            if not os.path.exists(fname):
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

                    out.write(line)
                    if "Debug log ON" in line:
                        logger.debug('Found "Debug log ON" line %s in %s' % (linecount, fname))
                        nextfile = False
                        break
                    linecount += 1
                extn += 1

        if os.path.exists(lazylibrarian.CONFIGFILE):
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

    with open(outfile + '.log', 'w') as logfile:
        logfile.write(logHeader())
        linecount = 0
        if PY2:
            lines = reversed(open(outfile + '.tmp').readlines())
        else:
            lines = reversed(list(open(outfile + '.tmp')))
        for line in lines:
            logfile.write(line)
            linecount += 1
    os.remove(outfile + '.tmp')
    logger.debug("Redacted %s passwords/apikeys" % redacts)
    logger.debug("%s log lines written to %s" % (linecount, outfile + '.log'))
    with zipfile.ZipFile(outfile + '.zip', 'w') as myzip:
        myzip.write(outfile + '.log', 'debug.log')
    os.remove(outfile + '.log')
    return "Debug log saved as %s" % (outfile + '.zip')


def zipAudio(source, zipname):
    """ Zip up all the audiobook parts in source folder to zipname
        Check if zipfile already exists, if not create a new one
        Doesn't actually check for audiobook parts, just zips everything
        including any .jpg etc
        Return full path to zipfile
    """
    zip_file = os.path.join(source, zipname + '.zip')
    if not os.path.exists(zip_file):
        logger.debug('Zipping up %s' % zipname)
        cnt = 0
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as myzip:
            for rootdir, _, filenames in walk(source):
                rootdir = makeUnicode(rootdir)
                filenames = [makeUnicode(item) for item in filenames]
                for filename in filenames:
                    # don't include self or our special index file
                    if not filename.endswith('.zip') and not filename.endswith('.ll'):
                        cnt += 1
                        myzip.write(os.path.join(rootdir, filename), filename)
        logger.debug('Zipped up %s files' % cnt)
    return zip_file


def runScript(params):
    if platform.system() == "Windows" and params[0].endswith('.py'):
        params.insert(0, sys.executable)
    logger.debug(str(params))
    try:
        p = Popen(params, stdout=PIPE, stderr=PIPE)
        res, err = p.communicate()
        return p.returncode, makeUnicode(res), makeUnicode(err)
    except Exception as e:
        err = "runScript exception: %s %s" % (type(e).__name__, str(e))
        logger.error(err)
        return 1, '', err
