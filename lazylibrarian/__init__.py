#  This file is part of Lazylibrarian.
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

# Purpose:
#   Hold all of the global variables needed/used by LL
#   Hold a few basic routines used widely, until they can be moved out


from __future__ import print_function
from __future__ import with_statement

import os
import sys
import threading
import time
from shutil import rmtree

from lazylibrarian import logger, database, notifiers # Must keep notifiers here
from lazylibrarian.common import path_isdir, syspath, module_available
from lazylibrarian.formatter import get_list, make_unicode
from lazylibrarian.providers import provider_is_blocked
import configparser
import urllib3
import requests


# Transient globals NOT stored in config
# These are used/modified by LazyLibrarian.py before config.ini is read
FULL_PATH = None
PROG_DIR = ''
ARGS = None
DAEMON = False
SIGNAL = None
PIDFILE = ''
DATADIR = ''
CONFIGFILE = ''
SYS_ENCODING = ''
LOGLEVEL = 1
LOGINUSER = None
CONFIG = {} # The configuration used, read from config.ini
CFG = None # A ConfigParser used to read the .ini file
DBFILE = None
COMMIT_LIST = None
SHOWLOGOUT = 1
CHERRYPYLOG = 0
REQUESTSLOG = 0
DOCKER = False
STOPTHREADS = False

# APPRISE not defined here, but in notifiers

# Transients used by logger process
LOGLIST = []
LOGTOGGLE = 2  # normal debug
LOGTYPE = ''

# These are globals
SUPPRESS_UPDATE = False
UPDATE_MSG = ''
TIMERS = {
            'NO_TOR_MSG': 0,
            'NO_RSS_MSG': 0,
            'NO_NZB_MSG': 0,
            'NO_CV_MSG': 0,
            'NO_DIRECT_MSG': 0,
            'NO_IRC_MSG': 0,
            'LAST_GR': 0,
            'LAST_LT': 0,
            'LAST_CV': 0,
            'LAST_BOK': 0,
            'LAST_BFI': 0,
            'SLEEP_GR': 0.0,
            'SLEEP_LT': 0.0,
            'SLEEP_CV': 0.0,
            'SLEEP_BOK': 0.0,
        }
IGNORED_AUTHORS = 0
CURRENT_TAB = '1'
CACHE_HIT = 0
CACHE_MISS = 0
IRC_CACHE_EXPIRY = 2 * 24 * 3600
GB_CALLS = 0
MONTHNAMES = []
CACHEDIR = ''
NEWZNAB_PROV = []
TORZNAB_PROV = []
NABAPICOUNT = ''
RSS_PROV = []
IRC_PROV = []
GEN_PROV = []
APPRISE_PROV = []
BOOKSTRAP_THEMELIST = []
PROVIDER_BLOCKLIST = []
USER_BLOCKLIST = []
SHOW_EBOOK = 1
SHOW_MAGS = 1
SHOW_SERIES = 1
SHOW_AUDIO = 0
SHOW_COMICS = 0
MAG_UPDATE = 0
EBOOK_UPDATE = 0
AUDIO_UPDATE = 0
COMIC_UPDATE = 0
SERIES_UPDATE = 0
AUTHORS_UPDATE = 0
LOGIN_MSG = ''
HIST_REFRESH = 1000
GITLAB_TOKEN = 'gitlab+deploy-token-26212:Hbo3d8rfZmSx4hL1Fdms@gitlab.com'
GRGENRES = {}
GC_BEFORE = {}
GC_AFTER = {}
UNRARLIB = 0
RARFILE = None
REDACTLIST = []
FFMPEGVER = ''
FFMPEGAAC = ''
SAB_VER = (0, 0, 0)
NEWUSER_MSG = ''
NEWFILE_MSG = ''

# extended loglevels
log_matching = 1 << 2  # 4 magazine/comic date/name matching
log_searching = 1 << 3  # 8 extra search logging
log_dlcomms = 1 << 4  # 16 detailed downloader communication
log_dbcomms = 1 << 5  # 32 database comms
log_postprocess = 1 << 6  # 64 detailed postprocessing
log_fuzz = 1 << 7  # 128 fuzzy logic
log_serverside = 1 << 8  # 256 serverside processing
log_fileperms = 1 << 9  # 512 changes to file permissions
log_grsync = 1 << 10  # 1024 detailed goodreads sync
log_cache = 1 << 11  # 2048 cache results
log_libsync = 1 << 12  # 4096 librarysync details
log_admin = 1 << 13  # 8192 admin logging
log_cherrypy = 1 << 14  # 16384 cherrypy logging
log_requests = 1 << 15  # 32768 requests httpclient logging

# user permissions
perm_config = 1 << 0  # 1 access to config page
perm_logs = 1 << 1  # 2 access to logs
perm_history = 1 << 2  # 4 access to history
perm_managebooks = 1 << 3  # 8 access to manage page
perm_magazines = 1 << 4  # 16 access to magazines/issues/pastissues
perm_audio = 1 << 5  # 32 access to audiobooks page
perm_ebook = 1 << 6  # 64 can access ebooks page
perm_series = 1 << 7  # 128 access to series/seriesmembers
perm_edit = 1 << 8  # 256 can edit book or author details
perm_search = 1 << 9  # 512 can search goodreads/googlebooks for books/authors
perm_status = 1 << 10  # 1024 can change book status (wanted/skipped etc)
perm_force = 1 << 11  # 2048 can use background tasks (refresh authors/libraryscan/postprocess/searchtasks)
perm_download = 1 << 12  # 4096 can download existing books/mags
perm_comics = 1 << 13  # 8192 access to comics

perm_authorbooks = perm_audio + perm_ebook
perm_guest = perm_download + perm_series + perm_authorbooks + perm_magazines + perm_comics
perm_friend = perm_guest + perm_search + perm_status
perm_admin = 65535

# user prefs
pref_myauthors = 1 << 0
pref_myseries = 1 << 1
pref_mymags = 1 << 2
pref_mycomics = 1 << 3
pref_myfeeds = 1 << 4
pref_myafeeds = 1 << 5

# Shared dictionaries
isbn_979_dict = {
    "10": "fre",
    "11": "kor",
    "12": "ita"
}
isbn_978_dict = {
    "0": "eng",
    "1": "eng",
    "2": "fre",
    "3": "ger",
    "4": "jap",
    "5": "rus",
    "7": "chi",
    "80": "cze",
    "82": "pol",
    "83": "nor",
    "84": "spa",
    "85": "bra",
    "87": "den",
    "88": "ita",
    "89": "kor",
    "91": "swe",
    "93": "ind"
}


def directory(dirname):
    usedir = ''
    if dirname == "eBook":
        usedir = CONFIG['EBOOK_DIR']
    elif dirname == "AudioBook" or dirname == "Audio":
        usedir = CONFIG['AUDIO_DIR']
    elif dirname == "Download":
        try:
            usedir = get_list(CONFIG['DOWNLOAD_DIR'], ',')[0]
        except IndexError:
            usedir = ''
    elif dirname == "Alternate":
        usedir = CONFIG['ALTERNATE_DIR']
    elif dirname == "Testdata":
        usedir = CONFIG['TESTDATA_DIR']
    else:
        return usedir
    # ./ and .\ denotes relative to program path, useful for testing
    if usedir and len(usedir) >= 2 and usedir[0] == ".":
        if usedir[1] == "/" or usedir[1] == "\\":
           usedir = PROG_DIR + "/" + usedir[2:]
           if os.path.__name__ == 'ntpath': 
               usedir = usedir.replace('/', '\\')
    if usedir and not path_isdir(usedir):
        try:
            os.makedirs(syspath(usedir))
            logger.info("Created new %s folder: %s" % (dirname, usedir))
        except OSError as e:
            logger.warn('Unable to create folder %s: %s, using %s' % (usedir, str(e), DATADIR))
            usedir = DATADIR
    if usedir and path_isdir(usedir):
        try:
            with open(syspath(os.path.join(usedir, 'll_temp')), 'w') as f:
                f.write('test')
            os.remove(syspath(os.path.join(usedir, 'll_temp')))
        except Exception as why:
            logger.warn("%s dir [%s] not writeable, using %s: %s" % (dirname, repr(usedir), DATADIR, str(why)))
            usedir = syspath(usedir)
            logger.debug("Folder: %s Mode: %s UID: %s GID: %s W_OK: %s X_OK: %s" % (usedir,
                                                                                    oct(os.stat(usedir).st_mode),
                                                                                    os.stat(usedir).st_uid,
                                                                                    os.stat(usedir).st_gid,
                                                                                    os.access(usedir, os.W_OK),
                                                                                    os.access(usedir, os.X_OK)))
            usedir = DATADIR
    else:
        logger.warn("%s dir [%s] not found, using %s" % (dirname, repr(usedir), DATADIR))
        usedir = DATADIR

    return make_unicode(usedir)



def wishlist_type(host):
    """ 
    Return type of wishlist at host, or empty string if host is not a wishlist 
    (Quite fragile, take care)
    """
    # GoodReads rss feeds
    if 'goodreads' in host and 'list_rss' in host:
        return 'goodreads'
    # GoodReads Listopia html pages
    if 'goodreads' in host and '/list/show/' in host:
        return 'listopia'
    # GoodReads most_read html pages (Listopia format)
    if 'goodreads' in host and '/book/' in host:
        return 'listopia'
    # Amazon charts html pages
    if 'amazon' in host and '/charts' in host:
        return 'amazon'
    # NYTimes best-sellers html pages
    if 'nytimes' in host and 'best-sellers' in host:
        return 'ny_times'
    # Publisherweekly best-seller in category
    if 'publishersweekly' in host and '/pw/' in host:
        return 'publishersweekly'
    # Publisherweekly best-seller in category
    if 'apps.npr.org' in host and '/best-books/' in host:
        return 'apps.npr.org'
    if 'penguinrandomhouse' in host:
        return 'penguinrandomhouse'
    if 'barnesandnoble' in host:
        return 'barnesandnoble'
 
    return ''


def bok_dlcount():
    db = database.DBConnection()
    yesterday = time.time() - 24*60*60
    grabs = db.select('SELECT completed from wanted WHERE nzbprov="zlibrary" and completed > ? order by completed',
                      (yesterday,))
    db.close()
    if grabs:
        return len(grabs), grabs[0]['completed']
    return 0, 0


def use_rss():
    """
    Returns number of RSS providers that are not wishlists, and are not blocked
    """
    count = 0
    for provider in RSS_PROV:
        if bool(provider['ENABLED']) and not wishlist_type(provider['HOST']) and not \
                provider_is_blocked(provider['HOST']):
            count += 1
    return count


def use_irc():
    """
    Returns number of IRC active providers that are not blocked
    """
    count = 0
    for provider in IRC_PROV:
        if bool(provider['ENABLED']) and not provider_is_blocked(provider['SERVER']):
            count += 1
    return count


def use_wishlist():
    """
    Returns number of RSS providers that are wishlists and not blocked
    """
    count = 0
    for provider in RSS_PROV:
        if bool(provider['ENABLED']) and wishlist_type(provider['HOST']) and not provider_is_blocked(provider['HOST']):
            count += 1
    return count


def use_nzb():
    """
    Returns number of nzb active providers that are not blocked
    (Includes Newznab and Torznab providers)
    """
    count = 0
    for provider in NEWZNAB_PROV:
        if bool(provider['ENABLED']) and not provider_is_blocked(provider['HOST']):
            count += 1
    for provider in TORZNAB_PROV:
        if bool(provider['ENABLED']) and not provider_is_blocked(provider['HOST']):
            count += 1
    return count


def use_tor():
    """
    Returns number of TOR providers that are not blocked
    """
    count = 0
    for provider in ['KAT', 'WWT', 'TPB', 'ZOO', 'LIME', 'TDL', 'TRF']:
        if bool(CONFIG[provider]) and not provider_is_blocked(provider):
            count += 1
    return count


def use_direct():
    """
    Returns number of enabled direct book providers
    """
    count = 0
    for provider in GEN_PROV:
        if bool(provider['ENABLED']) and not provider_is_blocked(provider['HOST']):
            count += 1
    if bool(CONFIG['BOK']) and not provider_is_blocked('BOK'):
        count += 1
    if bool(CONFIG['BFI']) and not provider_is_blocked('BFI'):
        count += 1
    return count


def daemonize():
    """
    Fork off as a daemon
    """
    # active_count in python 3.9 but camelCase name still supported
    if 'activeCount' in dir(threading):
        # noinspection PyDeprecation
        threadcount = threading.activeCount()
    else:
        threadcount = threading.active_count()
    if threadcount != 1:
        logger.warn('There are %d active threads. Daemonizing may cause strange behavior.' % threadcount)

    sys.stdout.flush()
    sys.stderr.flush()

    # Make a non-session-leader child process
    try:
        pid = os.fork()  # @UndefinedVariable - only available in UNIX
        if pid != 0:
            sys.exit(0)
    except OSError as e:
        raise RuntimeError("1st fork failed: %s [%d]" % (e.strerror, e.errno))

    os.setsid()  # @UndefinedVariable - only available in UNIX

    # Make sure I can read my own files and shut out others
    prev = os.umask(0)
    os.umask(prev and int('077', 8))

    # Make the child a session-leader by detaching from the terminal
    try:
        pid = os.fork()  # @UndefinedVariable - only available in UNIX
        if pid != 0:
            sys.exit(0)
    except OSError as e:
        raise RuntimeError("2nd fork failed: %s [%d]" % (e.strerror, e.errno))

    dev_null = open('/dev/null', 'r')
    os.dup2(dev_null.fileno(), sys.stdin.fileno())

    si = open('/dev/null', "r")
    so = open('/dev/null', "a+")
    se = open('/dev/null', "a+")

    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    pid = os.getpid()
    logger.debug("Daemonized to PID %d" % pid)

    if PIDFILE:
        logger.debug("Writing PID %d to %s" % (pid, PIDFILE))
        with open(syspath(PIDFILE), 'w') as pidfile:
            pidfile.write("%s\n" % pid)

