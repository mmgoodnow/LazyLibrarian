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

from __future__ import print_function
from __future__ import with_statement

import calendar
import json
import locale
import os
import signal
import subprocess
import sys
import threading
import time
import sqlite3
import traceback
import tarfile
import cherrypy
from shutil import rmtree

from lazylibrarian import logger, database, versioncheck, postprocess, searchbook, searchmag, searchrss, \
    importer, grsync, comicscan
from lazylibrarian.cache import fetchURL
from lazylibrarian.common import restartJobs, logHeader, scheduleJob, listdir, \
    path_isdir, path_isfile, path_exists, syspath
from lazylibrarian.formatter import getList, bookSeries, unaccented, check_int, unaccented_bytes, \
    makeUnicode, makeBytestr
from lazylibrarian.dbupgrade import check_db, db_current_version
from lazylibrarian.providers import ProviderIsBlocked

from lib.apscheduler.scheduler import Scheduler
from six import PY2, text_type
# noinspection PyUnresolvedReferences
from six.moves import configparser

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
CONFIG = {}
CFG = None
DBFILE = None
COMMIT_LIST = None
SHOWLOGOUT = 1
CHERRYPYLOG = 0
DOCKER = False
# APPRISE not declared here, done in notifier

# These are only used in startup
SCHED = None
INIT_LOCK = threading.Lock()
__INITIALIZED__ = False
started = False

# Transients used by logger process
LOGLIST = []
LOGTOGGLE = 2  # normal debug
LOGTYPE = ''

# These are globals
SUPPRESS_UPDATE = False
UPDATE_MSG = ''
NO_TOR_MSG = 0
NO_RSS_MSG = 0
NO_NZB_MSG = 0
NO_CV_MSG = 0
NO_DIRECT_MSG = 0
NO_IRC_MSG = 0
IGNORED_AUTHORS = 0
CURRENT_TAB = '1'
CACHE_HIT = 0
CACHE_MISS = 0
IRC_CACHE_EXPIRY = 2 * 24 * 3600
LAST_GOODREADS = 0
LAST_LIBRARYTHING = 0
LAST_COMICVINE = 0
LAST_ZLIBRARY = 0
LAST_BOOKFI = 0
GR_SLEEP = 0.0
LT_SLEEP = 0.0
CV_SLEEP = 0.0
GB_CALLS = 0
MONTHNAMES = []
CACHEDIR = ''
NEWZNAB_PROV = []
TORZNAB_PROV = []
NABAPICOUNT = ''
BOK_DLCOUNT = 0
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
STOPTHREADS = False

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
# These are the items in config.ini
# Not all are accessible from the web ui
# Any undefined on startup will be set to the default value
# Any _NOT_ in the web ui will remain unchanged on config save
CONFIG_GIT = ['GIT_REPO', 'GIT_USER', 'GIT_BRANCH', 'LATEST_VERSION', 'GIT_UPDATED', 'CURRENT_VERSION',
              'GIT_HOST', 'COMMITS_BEHIND', 'INSTALL_TYPE', 'AUTO_UPDATE']
CONFIG_NONWEB = ['BLOCKLIST_TIMER', 'DISPLAYLENGTH', 'ISBN_LOOKUP', 'WALL_COLUMNS', 'HTTP_TIMEOUT',
                 'PROXY_LOCAL', 'SKIPPED_EXT', 'CHERRYPYLOG', 'SYS_ENCODING', 'HIST_REFRESH',
                 'HTTP_EXT_TIMEOUT', 'CALIBRE_RENAME', 'NAME_RATIO', 'NAME_PARTIAL', 'NAME_PARTNAME',
                 'PREF_UNRARLIB', 'SEARCH_RATELIMIT', 'EMAIL_LIMIT', 'AUDIO_NARRATOR', 'AUDIO_AUTHOR',
                 'DELUGE_TIMEOUT', 'OL_URL', 'GR_URL', 'GB_URL', 'LT_URL', 'CV_URL', 'CX_URL']
# default interface does not know about these items, so leaves them unchanged
CONFIG_NONDEFAULT = ['BOOKSTRAP_THEME', 'AUDIOBOOK_TYPE', 'AUDIO_DIR', 'AUDIO_TAB', 'REJECT_AUDIO',
                     'REJECT_MAXAUDIO', 'REJECT_MINAUDIO', 'NEWAUDIO_STATUS', 'TOGGLES', 'FOUND_STATUS',
                     'USER_ACCOUNTS', 'GR_SYNC', 'GR_SECRET', 'GR_OAUTH_TOKEN', 'GR_OAUTH_SECRET',
                     'GR_OWNED', 'GR_WANTED', 'GR_UNIQUE', 'GR_FOLLOW', 'GR_FOLLOWNEW', 'GOODREADS_INTERVAL',
                     'AUDIOBOOK_DEST_FILE', 'SINGLE_USER', 'FMT_SERNAME', 'FMT_SERNUM', 'FMT_SERIES',
                     'AUTOADDMAG', 'AUTOADD_MAGONLY', 'TRANSMISSION_DIR', 'DELUGE_DIR', 'QBITTORRENT_DIR',
                     'BANNED_EXT', 'MAG_RENAME', 'LOGFILES', 'LOGSIZE', 'ISS_FORMAT', 'DATE_FORMAT',
                     'NO_ISBN', 'NO_SETS', 'NO_LANG', 'NO_PUBDATE', 'IMP_IGNORE', 'IMP_GOOGLEIMAGE', 'DELETE_CSV',
                     'BLACKLIST_FAILED', 'BLACKLIST_PROCESSED', 'WISHLIST_INTERVAL', 'EXT_PREPROCESS',
                     'OPDS_ENABLED', 'OPDS_AUTHENTICATION', 'OPDS_USERNAME', 'OPDS_PASSWORD', 'OPDS_METAINFO',
                     'OPDS_PAGE', 'DELAYSEARCH', 'SEED_WAIT', 'GR_AOWNED', 'GR_AWANTED', 'MAG_DELFOLDER',
                     'ADMIN_EMAIL', 'RSS_ENABLED', 'RSS_HOST', 'RSS_PODCAST', 'COMIC_TAB', 'COMIC_DEST_FOLDER',
                     'COMIC_RELATIVE', 'COMIC_DELFOLDER', 'COMIC_TYPE', 'WISHLIST_GENRES', 'DIR_PERM', 'FILE_PERM',
                     'SEARCH_COMICINTERVAL', 'CV_APIKEY', 'CV_WEBSEARCH', 'HIDE_OLD_NOTIFIERS', 'EBOOK_TAB',
                     'REJECT_PUBLISHER', 'SAB_EXTERNAL_HOST', 'MAG_COVERSWAP', 'IGNORE_PAUSED',
                     'NAME_POSTFIX', 'NEWSERIES_STATUS', 'NO_SINGLE_BOOK_SERIES', 'NOTIFY_WITH_TITLE',
                     'NOTIFY_WITH_URL', 'USER_AGENT', 'RATESTARS', 'NO_NONINTEGER_SERIES', 'IMP_NOSPLIT',
                     'NAME_DEFINITE', 'PP_DELAY', 'DEL_FAILED', 'DEL_COMPLETED', 'AUDIOBOOK_SINGLE_FILE',
                     'AUTH_TYPE']

CONFIG_DEFINITIONS = {
    # Name      Type   Section   Default
    'OL_URL': ('str', 'General', 'https://openlibrary.org'),
    'GR_URL': ('str', 'General', 'https://goodreads.com'),
    'GB_URL': ('str', 'General', 'https://www.googleapis.com'),
    'LT_URL': ('str', 'General', 'https://www.librarything.com'),
    'CV_URL': ('str', 'General', 'https://www.comicvine.gamespot.com'),
    'CX_URL': ('str', 'General', 'https://www.comixology.com'),
    'USER_ACCOUNTS': ('bool', 'General', 0),
    'SINGLE_USER': ('bool', 'General', 0),
    'ADMIN_EMAIL': ('str', 'General', ''),
    'SYS_ENCODING': ('str', 'General', ''),
    'LOGDIR': ('str', 'General', ''),
    'LOGLIMIT': ('int', 'General', 500),
    'LOGFILES': ('int', 'General', 10),
    'LOGSIZE': ('int', 'General', 204800),
    'AUTH_TYPE': ('str', 'General', "BASIC"),
    'LOGLEVEL': ('int', 'General', 1),
    'WALL_COLUMNS': ('int', 'General', 6),
    'FILE_PERM': ('str', 'General', '0o644'),
    'DIR_PERM': ('str', 'General', '0o755'),
    'BLOCKLIST_TIMER': ('int', 'General', 3600),
    'MAX_PAGES': ('int', 'General', 0),
    'MAX_BOOKPAGES': ('int', 'General', 0),
    'MAX_WALL': ('int', 'General', 0),
    'MATCH_RATIO': ('int', 'General', 80),
    'DLOAD_RATIO': ('int', 'General', 90),
    'NAME_RATIO': ('int', 'General', 90),
    'NAME_PARTIAL': ('int', 'General', 95),
    'NAME_PARTNAME': ('int', 'General', 95),
    'DISPLAYLENGTH': ('int', 'General', 10),
    'HIST_REFRESH': ('int', 'General', 1000),
    'HTTP_PORT': ('int', 'General', 5299),
    'HTTP_HOST': ('str', 'General', '0.0.0.0'),
    'HTTP_USER': ('str', 'General', ''),
    'HTTP_PASS': ('str', 'General', ''),
    'HTTP_PROXY': ('bool', 'General', 0),
    'HTTP_ROOT': ('str', 'General', ''),
    'HTTP_LOOK': ('str', 'General', 'bookstrap'),
    'HTTPS_ENABLED': ('bool', 'General', 0),
    'HTTPS_CERT': ('str', 'General', ''),
    'HTTPS_KEY': ('str', 'General', ''),
    'SSL_CERTS': ('str', 'General', ''),
    'SSL_VERIFY': ('bool', 'General', 0),
    'HTTP_TIMEOUT': ('int', 'General', 30),
    'HTTP_EXT_TIMEOUT': ('int', 'General', 90),
    'BOOKSTRAP_THEME': ('str', 'General', 'slate'),
    'MAG_SINGLE': ('bool', 'General', 1),
    'AUTHOR_IMG': ('bool', 'General', 1),
    'BOOK_IMG': ('bool', 'General', 1),
    'MAG_IMG': ('bool', 'General', 1),
    'COMIC_IMG': ('bool', 'General', 1),
    'SERIES_TAB': ('bool', 'General', 1),
    'MAG_TAB': ('bool', 'General', 1),
    'COMIC_TAB': ('bool', 'General', 0),
    'AUDIO_TAB': ('bool', 'General', 1),
    'EBOOK_TAB': ('bool', 'General', 1),
    'TOGGLES': ('bool', 'General', 1),
    'SORT_DEFINITE': ('bool', 'General', 0),
    'SORT_SURNAME': ('bool', 'General', 0),
    'SHOW_GENRES': ('bool', 'General', 0),
    'IGNORE_PAUSED': ('bool', 'General', 0),
    'LAUNCH_BROWSER': ('bool', 'General', 1),
    'API_ENABLED': ('bool', 'General', 0),
    'API_KEY': ('str', 'General', ''),
    'PROXY_HOST': ('str', 'General', ''),
    'PROXY_TYPE': ('str', 'General', ''),
    'PROXY_LOCAL': ('str', 'General', ''),
    'NAME_POSTFIX': ('str', 'General', 'snr, jnr, jr, sr, phd'),
    'NAME_DEFINITE': ('str', 'General', 'the, a'),
    'SKIPPED_EXT': ('str', 'General', 'fail, part, bts, !ut, torrent, magnet, nzb, unpack'),
    'BANNED_EXT': ('str', 'General', 'avi, mp4, mov, iso, m4v'),
    'IMP_PREFLANG': ('str', 'General', 'en, eng, en-US, en-GB'),
    'ISS_FORMAT': ('str', 'General', '$Y-$m-$d'),
    'DATE_FORMAT': ('str', 'General', '$Y-$m-$d'),
    'IMP_MONTHLANG': ('str', 'General', ''),
    'IMP_AUTOADD': ('str', 'General', ''),
    'IMP_AUTOADD_COPY': ('bool', 'General', 1),
    'IMP_AUTOADD_BOOKONLY': ('bool', 'General', 0),
    'IMP_AUTOADDMAG': ('str', 'General', ''),
    'IMP_AUTOADDMAG_COPY': ('bool', 'General', 1),
    'IMP_AUTOADD_MAGONLY': ('bool', 'General', 0),
    'IMP_AUTOSEARCH': ('bool', 'General', 0),
    'IMP_CALIBREDB': ('str', 'General', ''),
    'IMP_CALIBRE_EBOOK': ('bool', 'General', 0),
    'IMP_CALIBRE_COMIC': ('bool', 'General', 0),
    'IMP_CALIBRE_MAGAZINE': ('bool', 'General', 0),
    'IMP_CALIBRE_MAGTITLE': ('bool', 'General', 1),
    'IMP_CALIBRE_MAGISSUE': ('bool', 'General', 0),
    'BLACKLIST_FAILED': ('bool', 'General', 1),
    'BLACKLIST_PROCESSED': ('bool', 'General', 0),
    'CALIBRE_USE_SERVER': ('bool', 'General', 0),
    'CALIBRE_SERVER': ('str', 'General', ''),
    'CALIBRE_USER': ('str', 'General', ''),
    'CALIBRE_PASS': ('str', 'General', ''),
    'CALIBRE_RENAME': ('bool', 'General', 0),
    'IMP_SINGLEBOOK': ('bool', 'General', 0),
    'IMP_RENAME': ('bool', 'General', 0),
    'MAG_RENAME': ('bool', 'General', 0),
    'IMP_MAGOPF': ('bool', 'General', 1),
    'IMP_COMICOPF': ('bool', 'General', 0),
    'IMP_MAGCOVER': ('bool', 'General', 1),
    'IMP_COMICCOVER': ('bool', 'General', 1),
    'IMP_CONVERT': ('str', 'General', ''),
    'IMP_NOSPLIT': ('str', 'General', ''),
    'EXT_PREPROCESS': ('str', 'General', ''),
    'GIT_PROGRAM': ('str', 'General', ''),
    'CACHE_AGE': ('int', 'General', 30),
    'TASK_AGE': ('int', 'General', 2),
    'OPF_TAGS': ('bool', 'General', 1),
    'GENRE_TAGS': ('bool', 'General', 0),
    'WISHLIST_TAGS': ('bool', 'General', 1),
    'WISHLIST_GENRES': ('bool', 'General', 1),
    'NOTIFY_WITH_TITLE': ('bool', 'General', 0),
    'NOTIFY_WITH_URL': ('bool', 'General', 0),
    'GIT_HOST': ('str', 'Git', 'gitlab.com'),
    'GIT_USER': ('str', 'Git', 'LazyLibrarian'),
    'GIT_REPO': ('str', 'Git', 'lazylibrarian'),
    'GIT_BRANCH': ('str', 'Git', 'master'),
    'GIT_UPDATED': ('int', 'Git', 0),
    'INSTALL_TYPE': ('str', 'Git', ''),
    'CURRENT_VERSION': ('str', 'Git', ''),
    'LATEST_VERSION': ('str', 'Git', ''),
    'COMMITS_BEHIND': ('int', 'Git', 0),
    'AUTO_UPDATE': ('int', 'Git', 0),
    'SAB_HOST': ('str', 'SABnzbd', ''),
    'SAB_PORT': ('int', 'SABnzbd', 0),
    'SAB_SUBDIR': ('str', 'SABnzbd', ''),
    'SAB_USER': ('str', 'SABnzbd', ''),
    'SAB_PASS': ('str', 'SABnzbd', ''),
    'SAB_API': ('str', 'SABnzbd', ''),
    'SAB_CAT': ('str', 'SABnzbd', ''),
    'SAB_DELETE': ('bool', 'SABnzbd', 1),
    'SAB_EXTERNAL_HOST': ('str', 'SABnzbd', ''),
    'NZBGET_HOST': ('str', 'NZBGet', ''),
    'NZBGET_PORT': ('int', 'NZBGet', '0'),
    'NZBGET_USER': ('str', 'NZBGet', ''),
    'NZBGET_PASS': ('str', 'NZBGet', ''),
    'NZBGET_CATEGORY': ('str', 'NZBGet', ''),
    'NZBGET_PRIORITY': ('int', 'NZBGet', '0'),
    'DESTINATION_COPY': ('bool', 'General', 0),
    'EBOOK_DIR': ('str', 'General', ''),
    'AUDIO_DIR': ('str', 'General', ''),
    'ALTERNATE_DIR': ('str', 'General', ''),
    'DELETE_CSV': ('bool', 'General', 0),
    'DOWNLOAD_DIR': ('str', 'General', ''),
    'NZB_DOWNLOADER_SABNZBD': ('bool', 'USENET', 0),
    'NZB_DOWNLOADER_NZBGET': ('bool', 'USENET', 0),
    'NZB_DOWNLOADER_SYNOLOGY': ('bool', 'USENET', 0),
    'NZB_DOWNLOADER_BLACKHOLE': ('bool', 'USENET', 0),
    'NZB_BLACKHOLEDIR': ('str', 'USENET', ''),
    'USENET_RETENTION': ('int', 'USENET', 0),
    'NZBMATRIX_USER': ('str', 'NZBMatrix', ''),
    'NZBMATRIX_API': ('str', 'NZBMatrix', ''),
    'NZBMATRIX': ('bool', 'NZBMatrix', 0),
    'TOR_DOWNLOADER_BLACKHOLE': ('bool', 'TORRENT', 0),
    'TOR_CONVERT_MAGNET': ('bool', 'TORRENT', 0),
    'TOR_DOWNLOADER_UTORRENT': ('bool', 'TORRENT', 0),
    'TOR_DOWNLOADER_RTORRENT': ('bool', 'TORRENT', 0),
    'TOR_DOWNLOADER_QBITTORRENT': ('bool', 'TORRENT', 0),
    'TOR_DOWNLOADER_TRANSMISSION': ('bool', 'TORRENT', 0),
    'TOR_DOWNLOADER_SYNOLOGY': ('bool', 'TORRENT', 0),
    'TOR_DOWNLOADER_DELUGE': ('bool', 'TORRENT', 0),
    'NUMBEROFSEEDERS': ('int', 'TORRENT', 10),
    'KEEP_SEEDING': ('bool', 'TORRENT', 1),
    'SEED_WAIT': ('bool', 'TORRENT', 1),
    'PREFER_MAGNET': ('bool', 'TORRENT', 1),
    'TORRENT_DIR': ('str', 'TORRENT', ''),
    'RTORRENT_HOST': ('str', 'RTORRENT', ''),
    'RTORRENT_USER': ('str', 'RTORRENT', ''),
    'RTORRENT_PASS': ('str', 'RTORRENT', ''),
    'RTORRENT_LABEL': ('str', 'RTORRENT', ''),
    'RTORRENT_DIR': ('str', 'RTORRENT', ''),
    'UTORRENT_HOST': ('str', 'UTORRENT', ''),
    'UTORRENT_PORT': ('int', 'UTORRENT', 0),
    'UTORRENT_BASE': ('str', 'UTORRENT', ''),
    'UTORRENT_USER': ('str', 'UTORRENT', ''),
    'UTORRENT_PASS': ('str', 'UTORRENT', ''),
    'UTORRENT_LABEL': ('str', 'UTORRENT', ''),
    'QBITTORRENT_HOST': ('str', 'QBITTORRENT', ''),
    'QBITTORRENT_PORT': ('int', 'QBITTORRENT', 0),
    'QBITTORRENT_BASE': ('str', 'QBITTORRENT', ''),
    'QBITTORRENT_USER': ('str', 'QBITTORRENT', ''),
    'QBITTORRENT_PASS': ('str', 'QBITTORRENT', ''),
    'QBITTORRENT_LABEL': ('str', 'QBITTORRENT', ''),
    'QBITTORRENT_DIR': ('str', 'QBITTORRENT', ''),
    'TRANSMISSION_HOST': ('str', 'TRANSMISSION', ''),
    'TRANSMISSION_BASE': ('str', 'TRANSMISSION', ''),
    'TRANSMISSION_PORT': ('int', 'TRANSMISSION', 0),
    'TRANSMISSION_USER': ('str', 'TRANSMISSION', ''),
    'TRANSMISSION_PASS': ('str', 'TRANSMISSION', ''),
    'TRANSMISSION_DIR': ('str', 'TRANSMISSION', ''),
    'DELUGE_CERT': ('str', 'DELUGE', ''),
    'DELUGE_HOST': ('str', 'DELUGE', ''),
    'DELUGE_BASE': ('str', 'DELUGE', ''),
    'DELUGE_PORT': ('int', 'DELUGE', 0),
    'DELUGE_USER': ('str', 'DELUGE', ''),
    'DELUGE_PASS': ('str', 'DELUGE', ''),
    'DELUGE_LABEL': ('str', 'DELUGE', ''),
    'DELUGE_DIR': ('str', 'DELUGE', ''),
    'DELUGE_TIMEOUT': ('int', 'DELUGE', 3600),
    'SYNOLOGY_HOST': ('str', 'SYNOLOGY', ''),
    'SYNOLOGY_PORT': ('int', 'SYNOLOGY', 0),
    'SYNOLOGY_USER': ('str', 'SYNOLOGY', ''),
    'SYNOLOGY_PASS': ('str', 'SYNOLOGY', ''),
    'SYNOLOGY_DIR': ('str', 'SYNOLOGY', 'Multimedia/Download'),
    'USE_SYNOLOGY': ('bool', 'SYNOLOGY', 0),
    'KAT_HOST': ('str', 'KAT', 'kickass.cd'),
    'KAT': ('bool', 'KAT', 0),
    'KAT_DLPRIORITY': ('int', 'KAT', 0),
    'KAT_DLTYPES': ('str', 'KAT', 'A,E,M'),
    'KAT_SEEDERS': ('int', 'KAT', 0),
    'WWT_HOST': ('str', 'WWT', 'https://worldwidetorrents.me'),
    'WWT': ('bool', 'WWT', 0),
    'WWT_DLPRIORITY': ('int', 'WWT', 0),
    'WWT_DLTYPES': ('str', 'WWT', 'A,E,M'),
    'WWT_SEEDERS': ('int', 'WWT', 0),
    'TPB_HOST': ('str', 'TPB', 'https://pirateproxy.cc'),
    'TPB': ('bool', 'TPB', 0),
    'TPB_DLPRIORITY': ('int', 'TPB', 0),
    'TPB_DLTYPES': ('str', 'TPB', 'A,C,E,M'),
    'TPB_SEEDERS': ('int', 'TPB', 0),
    'ZOO_HOST': ('str', 'ZOO', 'https://zooqle.com'),
    'ZOO': ('bool', 'ZOO', 0),
    'ZOO_DLPRIORITY': ('int', 'ZOO', 0),
    'ZOO_DLTYPES': ('str', 'ZOO', 'A,E,M'),
    'ZOO_SEEDERS': ('int', 'ZOO', 0),
    'TRF_HOST': ('str', 'Torrof', 'torrof.com'),
    'TRF': ('bool', 'Torrof', 0),
    'TRF_DLPRIORITY': ('int', 'Torrof', 0),
    'TRF_DLTYPES': ('str', 'Torrof', 'A,E,M'),
    'TRF_SEEDERS': ('int', 'TRF', 0),
    'TDL_HOST': ('str', 'TDL', 'torrentdownloads.me'),
    'TDL': ('bool', 'TDL', 0),
    'TDL_DLPRIORITY': ('int', 'TDL', 0),
    'TDL_DLTYPES': ('str', 'TDL', 'A,E,M'),
    'TDL_SEEDERS': ('int', 'TDL', 0),
    'BOK_HOST': ('str', 'BOK', 'b-ok.cc'),
    'BOK': ('bool', 'BOK', 0),
    'BOK_DLPRIORITY': ('int', 'BOK', 0),
    'BOK_DLLIMIT': ('int', 'BOK', 5),
    'BOK_DLTYPES': ('str', 'BOK', 'E'),
    'BFI_HOST': ('str', 'BFI', 'en.bookfi.net'),
    'BFI': ('bool', 'BFI', 0),
    'BFI_DLPRIORITY': ('int', 'BFI', 0),
    'BFI_DLTYPES': ('str', 'BFI', 'E'),
    'LIME_HOST': ('str', 'LIME', 'https://www.limetorrents.cc'),
    'LIME': ('bool', 'LIME', 0),
    'LIME_DLPRIORITY': ('int', 'LIME', 0),
    'LIME_DLTYPES': ('str', 'LIME', 'A,E,M'),
    'LIME_SEEDERS': ('int', 'LIME', 0),
    'NEWZBIN_UID': ('str', 'Newzbin', ''),
    'NEWZBIN_PASS': ('str', 'Newzbin', ''),
    'NEWZBIN': ('bool', 'Newzbin', 0),
    'EBOOK_TYPE': ('str', 'General', 'epub, mobi, pdf'),
    'AUDIOBOOK_TYPE': ('str', 'General', 'mp3'),
    'MAG_TYPE': ('str', 'General', 'pdf'),
    'REJECT_PUBLISHER': ('str', 'General', ''),
    'REJECT_WORDS': ('str', 'General', 'audiobook, mp3'),
    'REJECT_AUDIO': ('str', 'General', 'epub, mobi'),
    'REJECT_MAGS': ('str', 'General', ''),
    'REJECT_MAXSIZE': ('int', 'General', 0),
    'REJECT_MINSIZE': ('int', 'General', 0),
    'REJECT_MAXAUDIO': ('int', 'General', 0),
    'REJECT_MINAUDIO': ('int', 'General', 0),
    'REJECT_MAGSIZE': ('int', 'General', 0),
    'REJECT_MAGMIN': ('int', 'General', 0),
    'REJECT_COMIC': ('str', 'General', 'epub, mobi'),
    'REJECT_MAXCOMIC': ('int', 'General', 0),
    'REJECT_MINCOMIC': ('int', 'General', 0),
    'MAG_AGE': ('int', 'General', 31),
    'SEARCH_BOOKINTERVAL': ('int', 'SearchScan', '360'),
    'SEARCH_MAGINTERVAL': ('int', 'SearchScan', '360'),
    'SCAN_INTERVAL': ('int', 'SearchScan', '10'),
    'SEARCHRSS_INTERVAL': ('int', 'SearchScan', '20'),
    'WISHLIST_INTERVAL': ('int', 'SearchScan', '24'),
    'SEARCH_COMICINTERVAL': ('int', 'SearchScan', '24'),
    'VERSIONCHECK_INTERVAL': ('int', 'SearchScan', '24'),
    'GOODREADS_INTERVAL': ('int', 'SearchScan', '48'),
    'DELAYSEARCH': ('bool', 'SearchScan', 0),
    'SEARCH_RATELIMIT': ('int', 'SearchScan', 0),
    'FULL_SCAN': ('bool', 'LibraryScan', 0),
    'ADD_AUTHOR': ('bool', 'LibraryScan', 1),
    'ADD_SERIES': ('bool', 'LibraryScan', 1),
    'NOTFOUND_STATUS': ('str', 'LibraryScan', 'Skipped'),
    'FOUND_STATUS': ('str', 'LibraryScan', 'Open'),
    'NO_SINGLE_BOOK_SERIES': ('bool', 'LibraryScan', 0),
    'NO_NONINTEGER_SERIES': ('bool', 'LibraryScan', 0),
    'NEWSERIES_STATUS': ('str', 'LibraryScan', 'Paused'),
    'NEWBOOK_STATUS': ('str', 'LibraryScan', 'Skipped'),
    'NEWAUDIO_STATUS': ('str', 'LibraryScan', 'Skipped'),
    'NEWAUTHOR_STATUS': ('str', 'LibraryScan', 'Skipped'),
    'NEWAUTHOR_AUDIO': ('str', 'LibraryScan', 'Skipped'),
    'NEWAUTHOR_BOOKS': ('bool', 'LibraryScan', 0),
    'NO_FUTURE': ('bool', 'LibraryScan', 0),
    'NO_PUBDATE': ('bool', 'LibraryScan', 0),
    'NO_ISBN': ('bool', 'LibraryScan', 0),
    'NO_SETS': ('bool', 'LibraryScan', 0),
    'NO_LANG': ('bool', 'LibraryScan', 0),
    'ISBN_LOOKUP': ('bool', 'LibraryScan', 1),
    'IMP_IGNORE': ('bool', 'LibraryScan', 0),
    'IMP_GOOGLEIMAGE': ('bool', 'LibraryScan', 0),
    'EBOOK_DEST_FOLDER': ('str', 'PostProcess', '$Author/$Title'),
    'EBOOK_DEST_FILE': ('str', 'PostProcess', '$Title - $Author'),
    'AUDIOBOOK_DEST_FILE': ('str', 'PostProcess', '$Author - $Title Part $Part of $Total'),
    'AUDIOBOOK_SINGLE_FILE': ('str', 'PostProcess', ''),
    'AUDIOBOOK_DEST_FOLDER': ('str', 'PostProcess', 'None'),
    'ONE_FORMAT': ('bool', 'PostProcess', 0),
    'COMIC_DEST_FOLDER': ('str', 'PostProcess', '_Comics/$Title/$Issue'),
    'COMIC_RELATIVE': ('bool', 'PostProcess', 1),
    'COMIC_DELFOLDER': ('bool', 'PostProcess', 1),
    'COMIC_TYPE': ('str', 'General', 'cbr, cbz'),
    'COMIC_SINGLE': ('bool', 'General', 1),
    'MAG_COVERSWAP': ('str', 'PostProcess', ''),
    'MAG_DEST_FOLDER': ('str', 'PostProcess', '_Magazines/$Title/$IssueDate'),
    'MAG_DEST_FILE': ('str', 'PostProcess', '$IssueDate - $Title'),
    'MAG_RELATIVE': ('bool', 'PostProcess', 1),
    'MAG_DELFOLDER': ('bool', 'PostProcess', 1),
    'PP_DELAY': ('int', 'PostProcess', 0),
    'DEL_FAILED': ('bool', 'PostProcess', 1),
    'DEL_COMPLETED': ('bool', 'PostProcess', 1),
    'HIDE_OLD_NOTIFIERS': ('bool', 'General', 0),
    'USE_TWITTER': ('bool', 'Twitter', 0),
    'TWITTER_NOTIFY_ONSNATCH': ('bool', 'Twitter', 0),
    'TWITTER_NOTIFY_ONDOWNLOAD': ('bool', 'Twitter', 0),
    'TWITTER_USERNAME': ('str', 'Twitter', ''),
    'TWITTER_PASSWORD': ('str', 'Twitter', ''),
    'TWITTER_PREFIX': ('str', 'Twitter', 'LazyLibrarian'),
    'USE_BOXCAR': ('bool', 'Boxcar', 0),
    'BOXCAR_NOTIFY_ONSNATCH': ('bool', 'Boxcar', 0),
    'BOXCAR_NOTIFY_ONDOWNLOAD': ('bool', 'Boxcar', 0),
    'BOXCAR_TOKEN': ('str', 'Boxcar', ''),
    'USE_PUSHBULLET': ('bool', 'Pushbullet', 0),
    'PUSHBULLET_NOTIFY_ONSNATCH': ('bool', 'Pushbullet', 0),
    'PUSHBULLET_NOTIFY_ONDOWNLOAD': ('bool', 'Pushbullet', 0),
    'PUSHBULLET_TOKEN': ('str', 'Pushbullet', ''),
    'PUSHBULLET_DEVICEID': ('str', 'Pushbullet', ''),
    'USE_PUSHOVER': ('bool', 'Pushover', 0),
    'PUSHOVER_ONSNATCH': ('bool', 'Pushover', 0),
    'PUSHOVER_ONDOWNLOAD': ('bool', 'Pushover', 0),
    'PUSHOVER_KEYS': ('str', 'Pushover', ''),
    'PUSHOVER_APITOKEN': ('str', 'Pushover', ''),
    'PUSHOVER_PRIORITY': ('int', 'Pushover', 0),
    'PUSHOVER_DEVICE': ('str', 'Pushover', ''),
    'USE_ANDROIDPN': ('bool', 'AndroidPN', 0),
    'ANDROIDPN_NOTIFY_ONSNATCH': ('bool', 'AndroidPN', 0),
    'ANDROIDPN_NOTIFY_ONDOWNLOAD': ('bool', 'AndroidPN', 0),
    'ANDROIDPN_URL': ('str', 'AndroidPN', ''),
    'ANDROIDPN_USERNAME': ('str', 'AndroidPN', ''),
    'ANDROIDPN_BROADCAST': ('bool', 'AndroidPN', 0),
    'USE_TELEGRAM': ('bool', 'Telegram', 0),
    'TELEGRAM_TOKEN': ('str', 'Telegram', ''),
    'TELEGRAM_USERID': ('str', 'Telegram', ''),
    'TELEGRAM_ONSNATCH': ('bool', 'Telegram', 0),
    'TELEGRAM_ONDOWNLOAD': ('bool', 'Telegram', 0),
    'USE_PROWL': ('bool', 'Prowl', 0),
    'PROWL_APIKEY': ('str', 'Prowl', ''),
    'PROWL_PRIORITY': ('int', 'Prowl', 0),
    'PROWL_ONSNATCH': ('bool', 'Prowl', 0),
    'PROWL_ONDOWNLOAD': ('bool', 'Prowl', 0),
    'USE_GROWL': ('bool', 'Growl', 0),
    'GROWL_HOST': ('str', 'Growl', ''),
    'GROWL_PASSWORD': ('str', 'Growl', ''),
    'GROWL_ONSNATCH': ('bool', 'Growl', 0),
    'GROWL_ONDOWNLOAD': ('bool', 'Growl', 0),
    'USE_SLACK': ('bool', 'Slack', 0),
    'SLACK_NOTIFY_ONSNATCH': ('bool', 'Slack', 0),
    'SLACK_NOTIFY_ONDOWNLOAD': ('bool', 'Slack', 0),
    'SLACK_TOKEN': ('str', 'Slack', ''),
    'SLACK_URL': ('str', 'Slack', "https://hooks.slack.com/services/"),
    'USE_CUSTOM': ('bool', 'Custom', 0),
    'CUSTOM_NOTIFY_ONSNATCH': ('bool', 'Custom', 0),
    'CUSTOM_NOTIFY_ONDOWNLOAD': ('bool', 'Custom', 0),
    'CUSTOM_SCRIPT': ('str', 'Custom', ''),
    'USE_EMAIL': ('bool', 'Email', 0),
    'EMAIL_NOTIFY_ONSNATCH': ('bool', 'Email', 0),
    'EMAIL_NOTIFY_ONDOWNLOAD': ('bool', 'Email', 0),
    'EMAIL_SENDFILE_ONDOWNLOAD': ('bool', 'Email', 0),
    'EMAIL_FROM': ('str', 'Email', ''),
    'EMAIL_TO': ('str', 'Email', ''),
    'EMAIL_SSL': ('bool', 'Email', 0),
    'EMAIL_SMTP_SERVER': ('str', 'Email', ''),
    'EMAIL_SMTP_PORT': ('int', 'Email', 25),
    'EMAIL_TLS': ('bool', 'Email', 0),
    'EMAIL_SMTP_USER': ('str', 'Email', ''),
    'EMAIL_SMTP_PASSWORD': ('str', 'Email', ''),
    'EMAIL_LIMIT': ('int', 'Email', 20),
    'USE_EMAIL_CUSTOM_FORMAT': ('bool', 'Email', 0),
    'EMAIL_CONVERT_FROM': ('str', 'Email', ''),
    'EMAIL_SEND_TYPE': ('str', 'Email', ''),
    'BOOK_API': ('str', 'API', 'OpenLibrary'),
    'LT_DEVKEY': ('str', 'API', ''),
    'CV_APIKEY': ('str', 'API', ''),
    'CV_WEBSEARCH': ('bool', 'API', 0),
    'GR_API': ('str', 'API', 'ckvsiSDsuqh7omh74ZZ6Q'),
    'GR_SYNC': ('bool', 'API', 0),
    'GR_SYNCUSER': ('bool', 'API', 0),
    'GR_USER': ('str', 'API', ''),
    'GR_SYNCREADONLY': ('bool', 'API', 0),
    'GR_SECRET': ('str', 'API', ''),  # tied to users own api key
    'GR_OAUTH_TOKEN': ('str', 'API', ''),  # gives access to users bookshelves
    'GR_OAUTH_SECRET': ('str', 'API', ''),  # gives access to users bookshelves
    'GR_WANTED': ('str', 'API', ''),  # sync wanted to this shelf
    'GR_OWNED': ('str', 'API', ''),  # sync open/have to this shelf
    'GR_AWANTED': ('str', 'API', ''),  # sync wanted to this shelf
    'GR_AOWNED': ('str', 'API', ''),  # sync open/have to this shelf
    'GR_UNIQUE': ('bool', 'API', 0),  # delete from wanted if already owned
    'GR_FOLLOW': ('bool', 'API', 0),  # follow authors on goodreads
    'GR_FOLLOWNEW': ('bool', 'API', 0),  # follow new authors on goodreads
    'GB_API': ('str', 'API', ''),  # API key has daily limits, each user needs their own
    'GB_COUNTRY': ('str', 'API', ''),  # optional two letter country code for geographically restricted results
    'FMT_SERNAME': ('str', 'FMT', '$SerName'),
    'FMT_SERNUM': ('str', 'FMT', 'Book #$SerNum -$$'),
    'FMT_SERIES': ('str', 'FMT', '( $FmtName $FmtNum )'),
    'OPDS_ENABLED': ('bool', 'OPDS', 0),
    'OPDS_AUTHENTICATION': ('bool', 'OPDS', 0),
    'OPDS_USERNAME': ('str', 'OPDS', ''),
    'OPDS_PASSWORD': ('str', 'OPDS', ''),
    'OPDS_METAINFO': ('bool', 'OPDS', 0),
    'OPDS_PAGE': ('int', 'OPDS', 30),
    'RSS_ENABLED': ('bool', 'RSS', 1),
    'RSS_PODCAST': ('bool', 'RSS', 1),
    'RSS_HOST': ('str', 'RSS', ''),
    'PREF_UNRARLIB': ('int', 'General', 1),
    'USER_AGENT': ('str', 'General', 'Mozilla/5.0 (X11; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0'),
    'RATESTARS': ('bool', 'General', 1),
    'EBOOK_WANTED_FORMATS': ('str', 'Preprocess', ''),
    'DELETE_OTHER_FORMATS': ('bool', 'Preprocess', 0),
    'EBOOK_CONVERT': ('str', 'Preprocess', 'ebook-convert'),
    'KEEP_OPF': ('bool', 'Preprocess', 1),
    'KEEP_JPG': ('bool', 'Preprocess', 1),
    'FFMPEG': ('str', 'Preprocess', 'ffmpeg'),
    'FFMPEG_OUT': ('str', 'Preprocess', ''),
    'AUDIO_OPTIONS': ('str', 'Preprocess', '-vn -b:a 128k -f mp3'),
    'CREATE_SINGLEAUDIO': ('bool', 'Preprocess', 0),
    'KEEP_SEPARATEAUDIO': ('bool', 'Preprocess', 0),
    'WRITE_AUDIOTAGS': ('bool', 'Preprocess', 0),
    'SWAP_COVERPAGE': ('bool', 'Preprocess', 0),
    'SHRINK_MAG': ('int', 'Preprocess', 0),
    # 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'),
}
if os.name == 'nt':
    for k in ['EBOOK_DEST_FOLDER', 'MAG_DEST_FOLDER', 'COMIC_DEST_FOLDER']:
        val = CONFIG_DEFINITIONS[k]
        CONFIG_DEFINITIONS[k] = (val[0], val[1], val[2].replace('/', '\\'))


def check_section(sec):
    """ Check if INI section exists, if not create it """
    # noinspection PyUnresolvedReferences
    if CFG.has_section(sec):
        return True
    else:
        # noinspection PyUnresolvedReferences
        CFG.add_section(sec)
        return False


def check_setting(cfg_type, cfg_name, item_name, def_val, log=True):
    """ Check option exists, coerce to correct type, or return default"""
    my_val = def_val
    if cfg_type == 'int':
        try:
            # noinspection PyUnresolvedReferences
            my_val = CFG.getint(cfg_name, item_name)
        except configparser.Error:
            # no such item, might be a new entry
            my_val = int(def_val)
        except Exception as e:
            if LOGLEVEL & log_admin:
                logger.warn('Invalid int for %s: %s, using default %s' % (cfg_name, item_name, int(def_val)))
                logger.debug(str(e))
            my_val = int(def_val)

    elif cfg_type == 'bool':
        try:
            # noinspection PyUnresolvedReferences
            my_val = CFG.getboolean(cfg_name, item_name)
        except configparser.Error:
            my_val = bool(def_val)
        except Exception as e:
            if LOGLEVEL & log_admin:
                logger.warn('Invalid bool for %s: %s, using default %s' % (cfg_name, item_name, bool(def_val)))
                logger.debug(str(e))
            my_val = bool(def_val)

    elif cfg_type == 'str':
        try:
            # noinspection PyUnresolvedReferences
            my_val = CFG.get(cfg_name, item_name)
            # Old config file format had strings in quotes. ConfigParser doesn't.
            if my_val.startswith('"') and my_val.endswith('"'):
                my_val = my_val[1:-1]
            if not len(my_val):
                my_val = def_val
        except configparser.Error:
            my_val = str(def_val)
        except Exception as e:
            if LOGLEVEL & log_admin:
                logger.warn('Invalid str for %s: %s, using default %s' % (cfg_name, item_name, str(def_val)))
                logger.debug(str(e))
            my_val = str(def_val)
        finally:
            my_val = makeUnicode(my_val)

    check_section(cfg_name)
    # noinspection PyUnresolvedReferences
    CFG.set(cfg_name, item_name, my_val)
    if log:
        if LOGLEVEL & log_admin:
            logger.debug("%s : %s -> %s" % (cfg_name, item_name, my_val))

    return my_val


def get_unrarlib():
    """ Detect presence of unrar library
        Return type of library and rarfile()
    """
    rarfile = None
    # noinspection PyBroadException
    try:
        # noinspection PyUnresolvedReferences
        from unrar import rarfile
        if CONFIG['PREF_UNRARLIB'] == 1:
            return 1, rarfile
    except Exception:
        # noinspection PyBroadException
        try:
            from lib.unrar import rarfile
            if CONFIG['PREF_UNRARLIB'] == 1:
                return 1, rarfile
        except Exception:
            pass

    if not rarfile or CONFIG['PREF_UNRARLIB'] == 2:
        # noinspection PyBroadException
        try:
            from lib.UnRAR2 import RarFile
            return 2, RarFile
        except Exception:
            if rarfile:
                return 1, rarfile
    return 0, None


def initialize():
    global FULL_PATH, PROG_DIR, ARGS, DAEMON, SIGNAL, PIDFILE, DATADIR, CONFIGFILE, SYS_ENCODING, LOGLEVEL, \
        CONFIG, CFG, DBFILE, COMMIT_LIST, SCHED, INIT_LOCK, __INITIALIZED__, started, LOGLIST, LOGTOGGLE, \
        UPDATE_MSG, CURRENT_TAB, CACHE_HIT, CACHE_MISS, LAST_LIBRARYTHING, LAST_GOODREADS, SHOW_SERIES, SHOW_MAGS, \
        SHOW_AUDIO, CACHEDIR, BOOKSTRAP_THEMELIST, MONTHNAMES, CONFIG_DEFINITIONS, isbn_979_dict, isbn_978_dict, \
        CONFIG_NONWEB, CONFIG_NONDEFAULT, CONFIG_GIT, MAG_UPDATE, AUDIO_UPDATE, EBOOK_UPDATE, COMIC_UPDATE, \
        GR_SLEEP, LT_SLEEP, GB_CALLS, GRGENRES, SHOW_COMICS, LAST_COMICVINE, CV_SLEEP, \
        SERIES_UPDATE, SHOW_EBOOK, UNRARLIB, RARFILE, SUPPRESS_UPDATE, LOGINUSER

    with INIT_LOCK:

        if __INITIALIZED__:
            return False

        SCHED = Scheduler(misfire_grace_time=30)

        check_section('General')
        # False to silence logging until logger initialised
        for key in ['LOGLIMIT', 'LOGFILES', 'LOGSIZE', 'LOGDIR']:
            item_type, section, default = CONFIG_DEFINITIONS[key]
            CONFIG[key.upper()] = check_setting(item_type, section, key.lower(), default, log=False)

        if not CONFIG['LOGDIR']:
            CONFIG['LOGDIR'] = os.path.join(DATADIR, 'Logs')

        # Create logdir
        if not path_isdir(CONFIG['LOGDIR']):
            try:
                os.makedirs(CONFIG['LOGDIR'])
            except OSError as e:
                print('%s : Unable to create folder for logs: %s' % (CONFIG['LOGDIR'], str(e)))

        # Start the logger, silence console logging if we need to
        CFGLOGLEVEL = check_int(check_setting('int', 'General', 'loglevel', 1, log=False), 9)
        if LOGLEVEL == 1:  # default if no debug or quiet on cmdline
            if CFGLOGLEVEL == 9:  # default value if none in config
                LOGLEVEL = 1  # If not set in Config or cmdline, then lets set to NORMAL
            else:
                LOGLEVEL = CFGLOGLEVEL  # Config setting picked up

        CONFIG['LOGLEVEL'] = LOGLEVEL
        logger.lazylibrarian_log.initLogger(loglevel=CONFIG['LOGLEVEL'])
        logger.info("Log (%s) Level set to [%s]- Log Directory is [%s] - Config level is [%s]" % (
            LOGTYPE, CONFIG['LOGLEVEL'], CONFIG['LOGDIR'], CFGLOGLEVEL))
        if CONFIG['LOGLEVEL'] > 2:
            logger.info("Screen Log set to EXTENDED DEBUG")
        elif CONFIG['LOGLEVEL'] == 2:
            logger.info("Screen Log set to DEBUG")
        elif CONFIG['LOGLEVEL'] == 1:
            logger.info("Screen Log set to INFO")
        else:
            logger.info("Screen Log set to WARN/ERROR")

        config_read()

        # override detected encoding if required
        if CONFIG['SYS_ENCODING']:
            SYS_ENCODING = CONFIG['SYS_ENCODING']

        # Put the cache dir in the data dir for now
        CACHEDIR = os.path.join(DATADIR, 'cache')
        if not path_isdir(CACHEDIR):
            try:
                os.makedirs(CACHEDIR)
            except OSError as e:
                logger.error('Could not create cachedir; %s' % e)

        for item in ['book', 'author', 'SeriesCache', 'JSONCache', 'XMLCache', 'WorkCache', 'HTMLCache',
                     'magazine', 'comic', 'IRCCache', 'icrawler', 'mako']:
            cachelocation = os.path.join(CACHEDIR, item)
            try:
                os.makedirs(cachelocation)
            except OSError as e:
                if not path_isdir(cachelocation):
                    logger.error('Could not create %s: %s' % (cachelocation, e))

        # nest these caches 2 levels to make smaller/faster directory lists
        caches = ["XMLCache", "JSONCache", "WorkCache", "HTMLCache"]
        for item in caches:
            pth = os.path.join(CACHEDIR, item)
            for i in '0123456789abcdef':
                for j in '0123456789abcdef':
                    cachelocation = os.path.join(pth, i, j)
                    try:
                        os.makedirs(cachelocation)
                    except OSError as e:
                        if not path_isdir(cachelocation):
                            logger.error('Could not create %s: %s' % (cachelocation, e))
            for itm in listdir(pth):
                if len(itm) > 2:
                    os.rename(syspath(os.path.join(pth, itm)),
                              syspath(os.path.join(pth, itm[0], itm[1], itm)))

        last_run_version = None
        last_run_interface = None
        makocache = os.path.join(CACHEDIR, 'mako')
        version_file = os.path.join(makocache, 'python_version.txt')

        if os.path.isfile(version_file):
            with open(version_file, 'r') as fp:
                last_time = fp.read().strip(' \n\r')
            if ':' in last_time:
                last_run_version, last_run_interface = last_time.split(':', 1)
            else:
                last_run_version = last_time

        clean_cache = False
        if last_run_version != sys.version.split()[0]:
            if last_run_version:
                logger.debug("Python version change (%s to %s)" % (last_run_version, sys.version.split()[0]))
            else:
                logger.debug("Previous python version unknown, now %s" % sys.version.split()[0])
            clean_cache = True
        if last_run_interface != CONFIG['HTTP_LOOK']:
            if last_run_interface:
                logger.debug("Interface change (%s to %s)" % (last_run_interface, CONFIG['HTTP_LOOK']))
            else:
                logger.debug("Previous interface unknown, now %s" % CONFIG['HTTP_LOOK'])
            clean_cache = True
        if clean_cache:
            logger.debug("Clearing mako cache")
            rmtree(makocache)
            os.makedirs(makocache)
            with open(version_file, 'w') as fp:
                fp.write(sys.version.split()[0] + ':' + CONFIG['HTTP_LOOK'])

        # keep track of last api calls so we don't call more than once per second
        # to respect api terms, but don't wait un-necessarily either
        # keep track of how long we slept
        time_now = int(time.time())
        LAST_LIBRARYTHING = time_now
        LAST_GOODREADS = time_now
        LAST_COMICVINE = time_now
        GR_SLEEP = 0.0
        LT_SLEEP = 0.0
        CV_SLEEP = 0.0
        GB_CALLS = 0

        if CONFIG['BOOK_API'] != 'GoodReads':
            CONFIG['GR_SYNC'] = 0
            CONFIG['GR_FOLLOW'] = 0
            CONFIG['GR_FOLLOWNEW'] = 0

        UNRARLIB, RARFILE = get_unrarlib()

        # Initialize the database
        try:
            myDB = database.DBConnection()
            result = myDB.match('PRAGMA user_version')
            check = myDB.match('PRAGMA integrity_check')
            if result:
                version = result[0]
            else:
                version = 0
            logger.info("Database is v%s, integrity check: %s" % (version, check[0]))
        except Exception as e:
            logger.error("Can't connect to the database: %s %s" % (type(e).__name__, str(e)))
            sys.exit(0)

        if version:
            db_changes = check_db()
            if db_changes:
                myDB.action('PRAGMA user_version=%s' % db_current_version)
                myDB.action('vacuum')
                logger.debug("Upgraded database schema to v%s with %s changes" % (db_current_version, db_changes))

        myDB.close()
        # group_concat needs sqlite3 >= 3.5.4
        # foreign_key needs sqlite3 >= 3.6.19 (Oct 2009)
        try:
            sqlv = getattr(sqlite3, 'sqlite_version', None)
            parts = sqlv.split('.')
            if int(parts[0]) == 3:
                if int(parts[1]) < 6 or int(parts[1]) == 6 and int(parts[2]) < 19:
                    logger.error("Your version of sqlite3 is too old, please upgrade to at least v3.6.19")
                    sys.exit(0)
        except Exception as e:
            logger.warn("Unable to parse sqlite3 version: %s %s" % (type(e).__name__, str(e)))

        debuginfo = logHeader()
        for item in debuginfo.splitlines():
            if 'missing' in item:
                logger.warn(item)

        GRGENRES = build_genres()
        MONTHNAMES = build_monthtable()

        try:  # optional module, check database health, could also be upgraded to modify/repair db or run other code
            # noinspection PyUnresolvedReferences
            from lazylibrarian import dbcheck
            dbcheck.dbcheck()
        except ImportError:
            pass

        BOOKSTRAP_THEMELIST = build_bookstrap_themes(PROG_DIR)

        __INITIALIZED__ = True
        return True


# noinspection PyUnresolvedReferences
def config_read(reloaded=False):
    global CONFIG, CONFIG_DEFINITIONS, CONFIG_NONWEB, CONFIG_NONDEFAULT, NEWZNAB_PROV, TORZNAB_PROV, RSS_PROV, \
        CONFIG_GIT, SHOW_SERIES, SHOW_MAGS, SHOW_AUDIO, NABAPICOUNT, SHOW_COMICS, APPRISE_PROV, SHOW_EBOOK, \
        IRC_PROV, GEN_PROV

    # legacy name conversion
    if CFG.has_section('GEN'):
        check_section('GEN_0')
        CFG.set('GEN_0', 'ENABLED', CFG.get('GEN', 'GEN'))
        CFG.set('GEN_0', 'DISPNAME', 'GEN_0')
        CFG.set('GEN_0', 'HOST', CFG.get('GEN', 'GEN_HOST'))
        CFG.set('GEN_0', 'SEARCH', CFG.get('GEN', 'GEN_SEARCH'))
        CFG.set('GEN_0', 'DLPRIORITY', CFG.get('GEN', 'GEN_DLPRIORITY'))
        CFG.set('GEN_0', 'DLTYPES', CFG.get('GEN', 'GEN_DLTYPES'))
        check_section('GEN_1')
        CFG.set('GEN_1', 'ENABLED', CFG.get('GEN', 'GEN2'))
        CFG.set('GEN_1', 'DISPNAME', 'GEN_1')
        CFG.set('GEN_1', 'HOST', CFG.get('GEN', 'GEN2_HOST'))
        CFG.set('GEN_1', 'SEARCH', CFG.get('GEN', 'GEN2_SEARCH'))
        CFG.set('GEN_1', 'DLPRIORITY', CFG.get('GEN', 'GEN2_DLPRIORITY'))
        CFG.set('GEN_1', 'DLTYPES', CFG.get('GEN2', 'GEN2_DLTYPES'))
        CFG.remove_section('GEN')
        CFG.remove_section('GEN2')

    count = 0
    while CFG.has_section('Newznab%i' % count):
        newz_name = 'Newznab%i' % count
        disp_name = check_setting('str', newz_name, 'dispname', newz_name)

        NEWZNAB_PROV.append({"NAME": newz_name,
                             "DISPNAME": disp_name,
                             "ENABLED": check_setting('bool', newz_name, 'enabled', 0),
                             "HOST": check_setting('str', newz_name, 'host', ''),
                             "API": check_setting('str', newz_name, 'api', ''),
                             "GENERALSEARCH": check_setting('str', newz_name, 'generalsearch', 'search'),
                             "BOOKSEARCH": check_setting('str', newz_name, 'booksearch', ''),
                             "MAGSEARCH": check_setting('str', newz_name, 'magsearch', ''),
                             "AUDIOSEARCH": check_setting('str', newz_name, 'audiosearch', ''),
                             "COMICSEARCH": check_setting('str', newz_name, 'comicsearch', ''),
                             "BOOKCAT": check_setting('str', newz_name, 'bookcat', '7000,7020'),
                             "MAGCAT": check_setting('str', newz_name, 'magcat', '7010'),
                             "AUDIOCAT": check_setting('str', newz_name, 'audiocat', '3030'),
                             "COMICCAT": check_setting('str', newz_name, 'comiccat', '7030'),
                             "EXTENDED": check_setting('str', newz_name, 'extended', '1'),
                             "UPDATED": check_setting('str', newz_name, 'updated', ''),
                             "MANUAL": check_setting('bool', newz_name, 'manual', 0),
                             "APILIMIT": check_setting('int', newz_name, 'apilimit', 0),
                             "APICOUNT": 0,
                             "RATELIMIT": check_setting('int', newz_name, 'ratelimit', 0),
                             "DLPRIORITY": check_setting('int', newz_name, 'dlpriority', 0),
                             "DLTYPES": check_setting('str', newz_name, 'dltypes', 'A,E,M'),
                             })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_newz_slot()

    count = 0
    while CFG.has_section('Torznab%i' % count):
        torz_name = 'Torznab%i' % count
        disp_name = check_setting('str', torz_name, 'dispname', torz_name)

        TORZNAB_PROV.append({"NAME": torz_name,
                             "DISPNAME": disp_name,
                             "ENABLED": check_setting('bool', torz_name, 'enabled', 0),
                             "HOST": check_setting('str', torz_name, 'host', ''),
                             "API": check_setting('str', torz_name, 'api', ''),
                             "GENERALSEARCH": check_setting('str', torz_name, 'generalsearch', 'search'),
                             "BOOKSEARCH": check_setting('str', torz_name, 'booksearch', ''),
                             "MAGSEARCH": check_setting('str', torz_name, 'magsearch', ''),
                             "AUDIOSEARCH": check_setting('str', torz_name, 'audiosearch', ''),
                             "COMICSEARCH": check_setting('str', torz_name, 'comicsearch', ''),
                             "BOOKCAT": check_setting('str', torz_name, 'bookcat', '8000,8010'),
                             "MAGCAT": check_setting('str', torz_name, 'magcat', '8030'),
                             "AUDIOCAT": check_setting('str', torz_name, 'audiocat', '3030'),
                             "COMICCAT": check_setting('str', torz_name, 'comiccat', '8020'),
                             "EXTENDED": check_setting('str', torz_name, 'extended', '1'),
                             "UPDATED": check_setting('str', torz_name, 'updated', ''),
                             "MANUAL": check_setting('bool', torz_name, 'manual', 0),
                             "APILIMIT": check_setting('int', torz_name, 'apilimit', 0),
                             "APICOUNT": 0,
                             "RATELIMIT": check_setting('int', torz_name, 'ratelimit', 0),
                             "DLPRIORITY": check_setting('int', torz_name, 'dlpriority', 0),
                             "DLTYPES": check_setting('str', torz_name, 'dltypes', 'A,E,M'),
                             "SEEDERS": check_setting('int', torz_name, 'seeders', 0),
                             })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_torz_slot()

    count = 0
    while CFG.has_section('RSS_%i' % count):
        rss_name = 'RSS_%i' % count
        disp_name = check_setting('str', rss_name, 'dispname', rss_name)

        RSS_PROV.append({"NAME": rss_name,
                         "DISPNAME": disp_name,
                         "ENABLED": check_setting('bool', rss_name, 'ENABLED', 0),
                         "HOST": check_setting('str', rss_name, 'HOST', ''),
                         "DLPRIORITY": check_setting('int', rss_name, 'DLPRIORITY', 0),
                         "DLTYPES": check_setting('str', rss_name, 'dltypes', 'E'),
                         })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_rss_slot()

    count = 0
    while CFG.has_section('IRC_%i' % count):
        irc_name = 'IRC_%i' % count
        disp_name = check_setting('str', irc_name, 'dispname', irc_name)

        IRC_PROV.append({"NAME": irc_name,
                         "DISPNAME": disp_name,
                         "ENABLED": check_setting('bool', irc_name, 'ENABLED', 0),
                         "SERVER": check_setting('str', irc_name, 'SERVER', ''),
                         "CHANNEL": check_setting('str', irc_name, 'CHANNEL', ''),
                         "BOTNICK": check_setting('str', irc_name, 'BOTNICK', ''),
                         "BOTPASS": check_setting('str', irc_name, 'BOTPASS', ''),
                         "DLPRIORITY": check_setting('int', irc_name, 'DLPRIORITY', 0),
                         "DLTYPES": check_setting('str', irc_name, 'dltypes', 'E'),
                         })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_irc_slot()

    count = 0
    while CFG.has_section('GEN_%i' % count):
        gen_name = 'GEN_%i' % count
        disp_name = check_setting('str', gen_name, 'DISPNAME', gen_name)

        GEN_PROV.append({"NAME": gen_name,
                         "DISPNAME": disp_name,
                         "ENABLED": check_setting('bool', gen_name, 'ENABLED', 0),
                         "HOST": check_setting('str', gen_name, 'HOST', ''),
                         "SEARCH": check_setting('str', gen_name, 'SEARCH', ''),
                         "DLPRIORITY": check_setting('int', gen_name, 'DLPRIORITY', 0),
                         "DLTYPES": check_setting('str', gen_name, 'DLTYPES', 'E'),
                         })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_gen_slot()

    count = 0
    while CFG.has_section('APPRISE_%i' % count):
        apprise_name = 'APPRISE_%i' % count
        APPRISE_PROV.append({"NAME": check_setting('str', apprise_name, 'NAME', apprise_name),
                             "DISPNAME": check_setting('str', apprise_name, 'DISPNAME', apprise_name),
                             "SNATCH": check_setting('bool', apprise_name, 'SNATCH', 0),
                             "DOWNLOAD": check_setting('bool', apprise_name, 'DOWNLOAD', 0),
                             "URL": check_setting('str', apprise_name, 'URL', ''),
                             })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_apprise_slot()

    for key in list(CONFIG_DEFINITIONS.keys()):
        item_type, section, default = CONFIG_DEFINITIONS[key]
        CONFIG[key.upper()] = check_setting(item_type, section, key.lower(), default)

    # new config options...
    if CONFIG['AUDIOBOOK_DEST_FOLDER'] == 'None':
        CFG.set('PostProcess', 'audiobook_dest_folder', CONFIG['EBOOK_DEST_FOLDER'])
        CONFIG['AUDIOBOOK_DEST_FOLDER'] = CONFIG['EBOOK_DEST_FOLDER']

    if not CONFIG['LOGDIR']:
        CONFIG['LOGDIR'] = os.path.join(DATADIR, 'Logs')
    if CONFIG['HTTP_PORT'] < 21 or CONFIG['HTTP_PORT'] > 65535:
        CONFIG['HTTP_PORT'] = 5299

    # to make extension matching easier
    for item in ['EBOOK_TYPE', 'EMAIL_CONVERT_FROM', 'EMAIL_SEND_TYPE', 'AUDIOBOOK_TYPE', 'MAG_TYPE',
                 'COMIC_TYPE', 'REJECT_MAGS', 'REJECT_WORDS', 'REJECT_AUDIO', 'REJECT_COMIC',
                 'REJECT_PUBLISHER', 'BANNED_EXT', 'NAME_POSTFIX', 'NAME_DEFINITE']:
        CONFIG[item] = CONFIG[item].lower()

    if os.name == 'nt':
        for fname in ['EBOOK_DEST_FOLDER', 'MAG_DEST_FOLDER', 'COMIC_DEST_FOLDER']:
            if '/' in CONFIG[fname]:
                logger.warn('Please check your %s setting' % fname)
                CONFIG[fname] = CONFIG[fname].replace('/', '\\')

    for fname in ['EBOOK_DEST_FILE', 'MAG_DEST_FILE', 'AUDIOBOOK_DEST_FILE', 'AUDIOBOOK_SINGLE_FILE']:
        if os.sep in CONFIG[fname]:
            logger.warn('Please check your %s setting, contains "%s"' % (fname, os.sep))
    if CONFIG['HTTP_LOOK'] == 'default':
        logger.warn('default interface is deprecated, new features are in bookstrap')
        CONFIG['HTTP_LOOK'] = 'legacy'

    for item in ['OL_URL', 'GR_URL', 'GB_URL', 'LT_URL', 'CV_URL', 'CX_URL']:
        url = CONFIG[item].rstrip('/')
        if not url.startswith('http'):
            url = 'http://' + url
        CONFIG[item] = url

    ###################################################################
    # ensure all these are boolean 1 0, not True False for javascript #
    ###################################################################
    # Suppress series tab if there are none and user doesn't want to add any
    if CONFIG['ADD_SERIES']:
        SHOW_SERIES = 1
    # Or suppress if tab is disabled
    if not CONFIG['SERIES_TAB']:
        SHOW_SERIES = 0
    # Suppress tabs if disabled
    SHOW_EBOOK = 1 if CONFIG['EBOOK_TAB'] else 0
    SHOW_AUDIO = 1 if CONFIG['AUDIO_TAB'] else 0
    SHOW_MAGS = 1 if CONFIG['MAG_TAB'] else 0
    SHOW_COMICS = 1 if CONFIG['COMIC_TAB'] else 0
    # Suppress audio/comic tabs if on legacy interface
    if CONFIG['HTTP_LOOK'] == 'legacy':
        SHOW_AUDIO = 0
        SHOW_COMICS = 0
        SHOW_EBOOK = 1

    for item in ['BOOK_IMG', 'MAG_IMG', 'COMIC_IMG', 'AUTHOR_IMG', 'TOGGLES']:
        CONFIG[item] = 1 if CONFIG[item] else 0

    if CONFIG['SSL_CERTS'] and not path_exists(CONFIG['SSL_CERTS']):
        logger.warn("SSL_CERTS [%s] not found" % CONFIG['SSL_CERTS'])
        CONFIG['SSL_CERTS'] = ''

    if reloaded:
        logger.info('Config file reloaded')
    else:
        logger.info('Config file loaded')


# noinspection PyUnresolvedReferences
def config_write(part=None):
    global SHOW_SERIES, SHOW_MAGS, SHOW_AUDIO, CONFIG_NONWEB, CONFIG_NONDEFAULT, CONFIG_GIT, LOGLEVEL, NEWZNAB_PROV, \
        TORZNAB_PROV, RSS_PROV, SHOW_COMICS, APPRISE_PROV, SHOW_EBOOK, IRC_PROV, GEN_PROV

    if part:
        logger.info("Writing config for section [%s]" % part)

    currentname = threading.currentThread().name
    threading.currentThread().name = "CONFIG_WRITE"

    interface = CFG.get('General', 'http_look')
    if CONFIG['HTTP_LOOK'] != interface:
        makocache = os.path.join(CACHEDIR, 'mako')
        logger.debug("Clearing mako cache")
        rmtree(makocache)
        os.makedirs(makocache)
        version_file = os.path.join(makocache, 'python_version.txt')
        with open(version_file, 'w') as fp:
            fp.write(sys.version.split()[0] + ':' + CONFIG['HTTP_LOOK'])

    for key in list(CONFIG_DEFINITIONS.keys()):
        _, section, _ = CONFIG_DEFINITIONS[key]
        if key in ['FILE_PERM', 'DIR_PERM']:
            if key == 'FILE_PERM':
                def_val = '644'
            else:
                def_val = '755'
            value = CONFIG[key]
            if len(value) in [5, 6]:
                value = value[2:]
            if len(value) in [3, 4]:
                try:
                    _ = int(value, 8)
                except ValueError:
                    value = def_val
            else:
                value = def_val
            value = '0o' + value
            CONFIG[key] = value

        elif key in ['WALL_COLUMNS', 'DISPLAY_LENGTH']:  # may be modified by user interface but not on config page
            value = check_int(CONFIG[key], 5)
        elif part and section != part:
            value = CFG.get(section, key.lower())  # keep the old value
            if LOGLEVEL & log_admin:
                logger.debug("Leaving %s unchanged (%s)" % (key, value))
        elif key not in CONFIG_NONWEB and not (interface == 'legacy' and key in CONFIG_NONDEFAULT):
            check_section(section)
            value = CONFIG[key]
            if key == 'LOGLEVEL':
                LOGLEVEL = check_int(value, 1)
            elif key in ['REJECT_WORDS', 'REJECT_AUDIO', 'REJECT_MAGS', 'REJECT_COMIC',
                         'MAG_TYPE', 'EBOOK_TYPE', 'EMAIL_CONVERT_FROM', 'EMAIL_SEND_TYPE', 'COMIC_TYPE', 'BANNED_EXT',
                         'AUDIOBOOK_TYPE', 'REJECT_PUBLISHER']:
                value = value.lower()
        else:
            # keep the old value
            value = CFG.get(section, key.lower())
            CONFIG[key] = value
            # if CONFIG['LOGLEVEL'] > 2:
            #    logger.debug("Leaving %s unchanged (%s)" % (key, value))

        if isinstance(value, text_type):
            if PY2:
                try:
                    value = value.encode(SYS_ENCODING)
                except UnicodeError:
                    logger.debug("Unable to convert value of %s (%s) to SYS_ENCODING" % (key, repr(value)))
                    if PY2:
                        value = unaccented_bytes(value)
                    else:
                        value = unaccented(value)
            value = value.strip()
            if 'DLTYPES' in key:
                value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                if not value:
                    value = 'E'
                CONFIG[key] = value

        if key in ['SEARCH_BOOKINTERVAL', 'SEARCH_MAGINTERVAL', 'SCAN_INTERVAL', 'VERSIONCHECK_INTERVAL',
                   'SEARCHRSS_INTERVAL', 'GOODREADS_INTERVAL', 'WISHLIST_INTERVAL', 'SEARCH_COMICINTERVAL']:
            oldvalue = CFG.get(section, key.lower())
            if value != oldvalue:
                if key == 'SEARCH_BOOKINTERVAL':
                    scheduleJob('Restart', 'search_book')
                elif key == 'SEARCH_MAGINTERVAL':
                    scheduleJob('Restart', 'search_magazines')
                elif key == 'SEARCHRSS_INTERVAL':
                    scheduleJob('Restart', 'search_rss_book')
                elif key == 'WISHLIST_INTERVAL':
                    scheduleJob('Restart', 'search_wishlist')
                elif key == 'SEARCH_COMICINTERVAL':
                    scheduleJob('Restart', 'search_comics')
                elif key == 'SCAN_INTERVAL':
                    scheduleJob('Restart', 'PostProcessor')
                elif key == 'VERSIONCHECK_INTERVAL':
                    scheduleJob('Restart', 'checkForUpdates')
                elif key == 'GOODREADS_INTERVAL' and CONFIG['GR_SYNC']:
                    scheduleJob('Restart', 'sync_to_gr')

        CFG.set(section, key.lower(), value)

    # sanity check for typos...
    for key in list(CONFIG.keys()):
        if key not in list(CONFIG_DEFINITIONS.keys()):
            logger.warn('Unsaved/invalid config key: %s' % key)

    if not part or part.lower().startswith('newznab') or part.lower().startswith('torznab'):
        NAB_ITEMS = ['ENABLED', 'DISPNAME', 'HOST', 'API', 'GENERALSEARCH', 'BOOKSEARCH', 'MAGSEARCH',
                     'AUDIOSEARCH', 'BOOKCAT', 'MAGCAT', 'AUDIOCAT', 'EXTENDED', 'DLPRIORITY', 'DLTYPES',
                     'UPDATED', 'MANUAL', 'APILIMIT', 'RATELIMIT', 'COMICSEARCH', 'COMICCAT']
        for entry in [[NEWZNAB_PROV, 'Newznab', []], [TORZNAB_PROV, 'Torznab', ['SEEDERS']]]:
            new_list = []
            # strip out any empty slots
            for provider in entry[0]:  # type: dict
                if provider['HOST']:
                    new_list.append(provider)

            if part:  # only update the named provider
                part = part.replace('nab_', 'nab')
                for provider in new_list:
                    if provider['NAME'].lower() != part.lower():  # keep old values
                        if CONFIG['LOGLEVEL'] > 2:
                            logger.debug("Keep %s" % provider['NAME'])
                        for item in NAB_ITEMS + entry[2]:
                            try:
                                provider[item] = CFG.get(provider['NAME'], item.lower())
                            except configparser.NoOptionError:
                                logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                                pass

            # renumber the items
            for index, item in enumerate(new_list):
                item['NAME'] = '%s%i' % (entry[1], index)

            # delete the old entries
            sections = CFG.sections()
            for item in sections:
                if item.startswith(entry[1]):
                    CFG.remove_section(item)

            for provider in new_list:
                check_section(provider['NAME'])
                for item in NAB_ITEMS + entry[2]:
                    value = provider[item]
                    if isinstance(value, text_type):
                        value = value.strip()
                    if item == 'DLTYPES':
                        value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                        if not value:
                            value = 'E'
                        provider['DLTYPES'] = value

                    CFG.set(provider['NAME'], item, value)

            if entry[1] == 'Newznab':
                NEWZNAB_PROV = new_list
                add_newz_slot()
            else:
                TORZNAB_PROV = new_list
                add_torz_slot()

    if not part or part.startswith('rss_'):
        RSS_ITEMS = ['ENABLED', 'DISPNAME', 'HOST', 'DLPRIORITY', 'DLTYPES']
        new_list = []
        # strip out any empty slots
        for provider in RSS_PROV:
            if provider['HOST']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].lower() != part:  # keep old values
                    if CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in RSS_ITEMS:
                        try:
                            provider[item] = CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'RSS_%i' % index

        # strip out the old config entries
        sections = CFG.sections()
        for item in sections:
            if item.startswith('RSS_'):
                CFG.remove_section(item)

        for provider in new_list:
            check_section(provider['NAME'])
            for item in RSS_ITEMS:
                value = provider[item]
                if isinstance(value, text_type):
                    value = value.strip()
                if item == 'DLTYPES':
                    value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                    if not value:
                        value = 'E'
                    provider['DLTYPES'] = value
                CFG.set(provider['NAME'], item, value)

        RSS_PROV = new_list
        add_rss_slot()

    if not part or part.startswith('GEN_'):
        GEN_ITEMS = ['ENABLED', 'DISPNAME', 'HOST', 'SEARCH', 'DLPRIORITY', 'DLTYPES']
        new_list = []
        # strip out any empty slots
        for provider in GEN_PROV:
            if provider['HOST']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].lower() != part:  # keep old values
                    if CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in GEN_ITEMS:
                        try:
                            provider[item] = CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'GEN_%i' % index

        # strip out the old config entries
        sections = CFG.sections()
        for item in sections:
            if item.startswith('GEN'):
                CFG.remove_section(item)

        for provider in new_list:
            check_section(provider['NAME'])
            for item in GEN_ITEMS:
                value = provider[item]
                if isinstance(value, text_type):
                    value = value.strip()
                if item == 'DLTYPES':
                    value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                    if not value:
                        value = 'E'
                    provider['DLTYPES'] = value
                CFG.set(provider['NAME'], item, value)

        GEN_PROV = new_list
        add_gen_slot()

    if not part or part.startswith('IRC_'):
        IRC_ITEMS = ['ENABLED', 'DISPNAME', 'SERVER', 'CHANNEL', 'BOTNICK', 'BOTPASS',
                     'DLPRIORITY', 'DLTYPES']
        new_list = []
        # strip out any empty slots
        for provider in IRC_PROV:
            if provider['SERVER']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].lower() != part:  # keep old values
                    if CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in IRC_ITEMS:
                        try:
                            provider[item] = CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'IRC_%i' % index

        # strip out the old config entries
        sections = CFG.sections()
        for item in sections:
            if item.startswith('IRC_'):
                CFG.remove_section(item)

        for provider in new_list:
            check_section(provider['NAME'])
            for item in IRC_ITEMS:
                value = provider[item]
                if isinstance(value, text_type):
                    value = value.strip()
                if item == 'DLTYPES':
                    value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                    if not value:
                        value = 'E'
                    provider['DLTYPES'] = value
                CFG.set(provider['NAME'], item, value)

        IRC_PROV = new_list
        add_irc_slot()

    if not part or part.startswith('apprise_'):
        APPRISE_ITEMS = ['NAME', 'DISPNAME', 'SNATCH', 'DOWNLOAD', 'URL']
        new_list = []
        # strip out any empty slots
        for provider in APPRISE_PROV:
            if provider['URL']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].lower() != part:  # keep old values
                    if CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in APPRISE_ITEMS:
                        try:
                            provider[item] = CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'APPRISE_%i' % index

        # strip out the old config entries
        sections = CFG.sections()
        for item in sections:
            if item.startswith('APPRISE_'):
                CFG.remove_section(item)

        for provider in new_list:
            check_section(provider['NAME'])
            for item in APPRISE_ITEMS:
                value = provider[item]
                if isinstance(value, text_type):
                    value = value.strip()
                CFG.set(provider['NAME'], item, value)
        APPRISE_PROV = new_list

        add_apprise_slot()
    #
    if CONFIG['ADD_SERIES']:
        SHOW_SERIES = 1
    if not CONFIG['SERIES_TAB']:
        SHOW_SERIES = 0

    SHOW_MAGS = 1 if CONFIG['MAG_TAB'] else 0
    SHOW_COMICS = 1 if CONFIG['COMIC_TAB'] else 0
    SHOW_EBOOK = 1 if CONFIG['EBOOK_TAB'] else 0
    SHOW_AUDIO = 1 if CONFIG['AUDIO_TAB'] else 0

    if CONFIG['HTTP_LOOK'] == 'legacy':
        SHOW_AUDIO = 0
        SHOW_COMICS = 0
        SHOW_EBOOK = 1

    if CONFIG['NO_SINGLE_BOOK_SERIES']:
        myDB = database.DBConnection()
        myDB.action('DELETE from series where total=1')
        myDB.close()
    msg = None
    try:
        if PY2:
            fmode = 'wb'
        else:
            fmode = 'w'
        with open(syspath(CONFIGFILE + '.new'), fmode) as configfile:
            CFG.write(configfile)
    except Exception as e:
        msg = '{} {} {} {}'.format('Unable to create new config file:', CONFIGFILE, type(e).__name__, str(e))
        logger.warn(msg)
        threading.currentThread().name = currentname
        return
    try:
        os.remove(syspath(CONFIGFILE + '.bak'))
    except OSError as e:
        if e.errno != 2:  # doesn't exist is ok
            msg = '{} {}{} {} {}'.format(type(e).__name__, 'deleting backup file:', CONFIGFILE, '.bak', e.strerror)
            logger.warn(msg)
    try:
        os.rename(syspath(CONFIGFILE), syspath(CONFIGFILE + '.bak'))
    except OSError as e:
        if e.errno != 2:  # doesn't exist is ok as wouldn't exist until first save
            msg = '{} {} {} {}'.format('Unable to backup config file:', CONFIGFILE, type(e).__name__, e.strerror)
            logger.warn(msg)
    try:
        os.rename(syspath(CONFIGFILE + '.new'), syspath(CONFIGFILE))
    except OSError as e:
        msg = '{} {} {} {}'.format('Unable to rename new config file:', CONFIGFILE, type(e).__name__, e.strerror)
        logger.warn(msg)

    if not msg:
        if part is None:
            part = ''
        msg = 'Config file [%s] %s has been updated' % (CONFIGFILE, part)
        logger.info(msg)

    threading.currentThread().name = currentname


# noinspection PyUnresolvedReferences
def add_newz_slot():
    count = len(NEWZNAB_PROV)
    if count == 0 or len(CFG.get('Newznab%i' % int(count - 1), 'HOST')):
        prov_name = 'Newznab%i' % count
        empty = {"NAME": prov_name,
                 "DISPNAME": prov_name,
                 "ENABLED": 0,
                 "HOST": '',
                 "API": '',
                 "GENERALSEARCH": 'search',
                 "BOOKSEARCH": 'book',
                 "MAGSEARCH": '',
                 "AUDIOSEARCH": '',
                 "COMICSEARCH": '',
                 "BOOKCAT": '7000,7020',
                 "MAGCAT": '7010',
                 "AUDIOCAT": '3030',
                 "COMICCAT": '7030',
                 "EXTENDED": '1',
                 "UPDATED": '',
                 "MANUAL": 0,
                 "APILIMIT": 0,
                 "APICOUNT": 0,
                 "RATELIMIT": 0,
                 "DLPRIORITY": 0,
                 "DLTYPES": 'A,C,E,M'
                 }
        NEWZNAB_PROV.append(empty)

        check_section(prov_name)
        for item in empty:
            if item != 'NAME':
                CFG.set(prov_name, item, empty[item])


# noinspection PyUnresolvedReferences
def add_torz_slot():
    count = len(TORZNAB_PROV)
    if count == 0 or len(CFG.get('Torznab%i' % int(count - 1), 'HOST')):
        prov_name = 'Torznab%i' % count
        empty = {"NAME": prov_name,
                 "DISPNAME": prov_name,
                 "ENABLED": 0,
                 "HOST": '',
                 "API": '',
                 "GENERALSEARCH": 'search',
                 "BOOKSEARCH": 'book',
                 "MAGSEARCH": '',
                 "AUDIOSEARCH": '',
                 "COMICSEARCH": '',
                 "BOOKCAT": '8000,8010',
                 "MAGCAT": '8030',
                 "AUDIOCAT": '3030',
                 "COMICCAT": '8020',
                 "EXTENDED": '1',
                 "UPDATED": '',
                 "MANUAL": 0,
                 "APILIMIT": 0,
                 "APICOUNT": 0,
                 "RATELIMIT": 0,
                 "DLPRIORITY": 0,
                 "DLTYPES": 'A,C,E,M',
                 "SEEDERS": 0
                 }
        TORZNAB_PROV.append(empty)

        check_section(prov_name)
        for item in empty:
            if item != 'NAME':
                CFG.set(prov_name, item, empty[item])


def DIRECTORY(dirname):
    usedir = ''
    if dirname == "eBook":
        usedir = CONFIG['EBOOK_DIR']
    elif dirname == "AudioBook" or dirname == "Audio":
        usedir = CONFIG['AUDIO_DIR']
    elif dirname == "Download":
        try:
            usedir = getList(CONFIG['DOWNLOAD_DIR'], ',')[0]
        except IndexError:
            usedir = ''
    elif dirname == "Alternate":
        usedir = CONFIG['ALTERNATE_DIR']
    else:
        return usedir
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

    return makeUnicode(usedir)


# noinspection PyUnresolvedReferences
def add_rss_slot():
    count = len(RSS_PROV)
    if count == 0 or len(CFG.get('RSS_%i' % int(count - 1), 'HOST')):
        rss_name = 'RSS_%i' % count
        check_section(rss_name)
        CFG.set(rss_name, 'ENABLED', False)
        CFG.set(rss_name, 'HOST', '')
        # CFG.set(rss_name, 'USER', '')
        # CFG.set(rss_name, 'PASS', '')
        RSS_PROV.append({"NAME": rss_name,
                         "DISPNAME": rss_name,
                         "ENABLED": 0,
                         "HOST": '',
                         "DLPRIORITY": 0,
                         "DLTYPES": 'E'
                         })


# noinspection PyUnresolvedReferences
def add_irc_slot():
    count = len(IRC_PROV)
    if count == 0 or len(CFG.get('IRC_%i' % int(count - 1), 'SERVER')):
        irc_name = 'IRC_%i' % count
        check_section(irc_name)
        CFG.set(irc_name, 'ENABLED', False)
        CFG.set(irc_name, 'SERVER', '')
        CFG.set(irc_name, 'CHANNEL', '')
        CFG.set(irc_name, 'BOTNICK', '')
        CFG.set(irc_name, 'BOTPASS', '')
        CFG.set(irc_name, 'DLPRIORITY', 0)
        CFG.set(irc_name, 'DLTYPES', 'E')
        IRC_PROV.append({"NAME": irc_name,
                         "DISPNAME": irc_name,
                         "ENABLED": 0,
                         "SERVER": '',
                         "CHANNEL": '',
                         "BOTNICK": '',
                         "BOTPASS": '',
                         "DLPRIORITY": 0,
                         "DLTYPES": 'E'
                         })


# noinspection PyUnresolvedReferences
def add_gen_slot():
    count = len(GEN_PROV)
    if count == 0 or len(CFG.get('GEN_%i' % int(count - 1), 'HOST')):
        gen_name = 'GEN_%i' % count
        check_section(gen_name)
        CFG.set(gen_name, 'ENABLED', False)
        CFG.set(gen_name, 'HOST', '')
        CFG.set(gen_name, 'SEARCH', '')
        CFG.set(gen_name, 'DLPRIORITY', 0)
        CFG.set(gen_name, 'DLTYPES', 'E')
        GEN_PROV.append({"NAME": gen_name,
                         "DISPNAME": gen_name,
                         "ENABLED": 0,
                         "HOST": '',
                         "SEARCH": '',
                         "DLPRIORITY": 0,
                         "DLTYPES": 'E'
                         })


# noinspection PyUnresolvedReferences
def add_apprise_slot():
    count = len(APPRISE_PROV)
    if count == 0 or len(CFG.get('APPRISE_%i' % int(count - 1), 'URL')):
        apprise_name = 'APPRISE_%i' % count
        check_section(apprise_name)
        CFG.set(apprise_name, 'NAME', apprise_name)
        CFG.set(apprise_name, 'DISPNAME', apprise_name)
        CFG.set(apprise_name, 'SNATCH', False)
        CFG.set(apprise_name, 'DOWNLOAD', False)
        CFG.set(apprise_name, 'URL', '')
        APPRISE_PROV.append({"NAME": apprise_name, "DISPNAME": apprise_name, "SNATCH": 0, "DOWNLOAD": 0, "URL": ''})


def WishListType(host):
    """ Return type of wishlist or empty string if not a wishlist """
    # GoodReads rss feeds
    if 'goodreads' in host and 'list_rss' in host:
        return 'GOODREADS'
    # GoodReads Listopia html pages
    if 'goodreads' in host and '/list/show/' in host:
        return 'LISTOPIA'
    # GoodReads most_read html pages (Listopia format)
    if 'goodreads' in host and '/book/' in host:
        return 'LISTOPIA'
    # Amazon charts html pages
    if 'amazon' in host and '/charts' in host:
        return 'AMAZON'
    # NYTimes best-sellers html pages
    if 'nytimes' in host and 'best-sellers' in host:
        return 'NYTIMES'
    return ''


def USE_RSS():
    count = 0
    for provider in RSS_PROV:
        if bool(provider['ENABLED']) and not WishListType(provider['HOST']) and not ProviderIsBlocked(provider['HOST']):
            count += 1
    return count


def USE_IRC():
    count = 0
    for provider in IRC_PROV:
        if bool(provider['ENABLED']) and not ProviderIsBlocked(provider['SERVER']):
            count += 1
    return count


def USE_WISHLIST():
    count = 0
    for provider in RSS_PROV:
        if bool(provider['ENABLED']) and WishListType(provider['HOST']) and not ProviderIsBlocked(provider['HOST']):
            count += 1
    return count


def USE_NZB():
    # Count how many nzb providers are active and not blocked
    count = 0
    for provider in NEWZNAB_PROV:
        if bool(provider['ENABLED']) and not ProviderIsBlocked(provider['HOST']):
            count += 1
    for provider in TORZNAB_PROV:
        if bool(provider['ENABLED']) and not ProviderIsBlocked(provider['HOST']):
            count += 1
    return count


def USE_TOR():
    count = 0
    for provider in ['KAT', 'WWT', 'TPB', 'ZOO', 'LIME', 'TDL', 'TRF']:
        if bool(CONFIG[provider]) and not ProviderIsBlocked(provider):
            count += 1
    return count


def USE_DIRECT():
    count = 0
    for provider in GEN_PROV:
        if bool(provider['ENABLED']) and not ProviderIsBlocked(provider['HOST']):
            count += 1
    if bool(CONFIG['BOK']) and not ProviderIsBlocked('BOK'):
        count += 1
    if bool(CONFIG['BFI']) and not ProviderIsBlocked('BFI'):
        count += 1
    return count


def build_bookstrap_themes(prog_dir):
    themelist = []
    if not path_isdir(os.path.join(prog_dir, 'data', 'interfaces', 'bookstrap')):
        return themelist  # return empty if bookstrap interface not installed

    URL = 'http://bootswatch.com/api/3.json'
    result, success = fetchURL(URL, headers=None, retry=False)
    if not success:
        logger.debug("Error getting bookstrap themes : %s" % result)
        return themelist

    try:
        results = json.loads(result)
        for theme in results['themes']:
            themelist.append(theme['name'].lower())
    except Exception as e:
        # error reading results
        logger.warn('JSON Error reading bookstrap themes, %s %s' % (type(e).__name__, str(e)))

    logger.info("Bookstrap found %i themes" % len(themelist))
    return themelist


def build_genres():
    for json_file in [os.path.join(DATADIR, 'genres.json'), os.path.join(PROG_DIR, 'example.genres.json')]:
        if path_isfile(json_file):
            try:
                if PY2:
                    with open(syspath(json_file), 'r') as json_data:
                        res = json.load(json_data)
                else:
                    # noinspection PyArgumentList
                    with open(syspath(json_file), 'r', encoding='utf-8') as json_data:
                        res = json.load(json_data)
                logger.info("Loaded genres from %s" % json_file)
                return res
            except Exception as e:
                logger.error('Failed to load %s, %s %s' % (json_file, type(e).__name__, str(e)))
    logger.error('No valid genres.json file found')
    return {"genreLimit": 4, "genreUsers": 10, "genreExclude": [], "genreExcludeParts": [], "genreReplace": {}}


def build_monthtable():
    table = []
    json_file = os.path.join(DATADIR, 'monthnames.json')
    if path_isfile(json_file):
        try:
            with open(syspath(json_file)) as json_data:
                table = json.load(json_data)
            mlist = ''
            # list alternate entries as each language is in twice (long and short month names)
            for item in table[0][::2]:
                mlist += item + ' '
            logger.debug('Loaded monthnames.json : %s' % mlist)
        except Exception as e:
            logger.error('Failed to load monthnames.json, %s %s' % (type(e).__name__, str(e)))

    if not table:
        # Default Month names table to hold long/short month names for multiple languages
        # which we can match against magazine issues
        table = [
            ['en_GB.UTF-8', 'en_GB.UTF-8'],
            ['january', 'jan'],
            ['february', 'feb'],
            ['march', 'mar'],
            ['april', 'apr'],
            ['may', 'may'],
            ['june', 'jun'],
            ['july', 'jul'],
            ['august', 'aug'],
            ['september', 'sep'],
            ['october', 'oct'],
            ['november', 'nov'],
            ['december', 'dec']
        ]

    if len(getList(CONFIG['IMP_MONTHLANG'])) == 0:  # any extra languages wanted?
        return table
    try:
        current_locale = locale.setlocale(locale.LC_ALL, '')  # read current state.
        if 'LC_CTYPE' in current_locale:
            current_locale = locale.setlocale(locale.LC_CTYPE, '')
        # getdefaultlocale() doesnt seem to work as expected on windows, returns 'None'
        logger.debug('Current locale is %s' % current_locale)
    except locale.Error as e:
        logger.debug("Error getting current locale : %s" % str(e))
        return table

    lang = str(current_locale)
    # check not already loaded, also all english variants and 'C' use the same month names
    if lang in table[0] or ((lang.startswith('en_') or lang == 'C') and 'en_' in str(table[0])):
        logger.debug('Month names for %s already loaded' % lang)
    else:
        logger.debug('Loading month names for %s' % lang)
        table[0].append(lang)
        for f in range(1, 13):
            table[f].append(unaccented(calendar.month_name[f]).lower())
        table[0].append(lang)
        for f in range(1, 13):
            table[f].append(unaccented(calendar.month_abbr[f]).lower().strip('.'))
        logger.info("Added month names for locale [%s], %s, %s ..." % (
            lang, table[1][len(table[1]) - 2], table[1][len(table[1]) - 1]))

    for lang in getList(CONFIG['IMP_MONTHLANG']):
        try:
            if lang in table[0] or ((lang.startswith('en_') or lang == 'C') and 'en_' in str(table[0])):
                logger.debug('Month names for %s already loaded' % lang)
            else:
                locale.setlocale(locale.LC_ALL, lang)
                logger.debug('Loading month names for %s' % lang)
                table[0].append(lang)
                for f in range(1, 13):
                    table[f].append(unaccented(calendar.month_name[f]).lower())
                table[0].append(lang)
                for f in range(1, 13):
                    table[f].append(unaccented(calendar.month_abbr[f]).lower().strip('.'))
                locale.setlocale(locale.LC_ALL, current_locale)  # restore entry state
                logger.info("Added month names for locale [%s], %s, %s ..." % (
                    lang, table[1][len(table[1]) - 2], table[1][len(table[1]) - 1]))
        except Exception as e:
            locale.setlocale(locale.LC_ALL, current_locale)  # restore entry state
            logger.warn("Unable to load requested locale [%s] %s %s" % (lang, type(e).__name__, str(e)))
            try:
                wanted_lang = lang.split('_')[0]
                params = ['locale', '-a']
                res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                all_locales = makeUnicode(res).split()
                locale_list = []
                for a_locale in all_locales:
                    if a_locale.startswith(wanted_lang):
                        locale_list.append(a_locale)
                if locale_list:
                    logger.warn("Found these alternatives: " + str(locale_list))
                else:
                    logger.warn("Unable to find an alternative")
            except Exception as e:
                logger.warn("Unable to get a list of alternatives, %s %s" % (type(e).__name__, str(e)))
            logger.debug("Set locale back to entry state %s" % current_locale)

    # with open(json_file, 'w') as f:
    #    json.dump(table, f)
    return table


def daemonize():
    """
    Fork off as a daemon
    """
    threadcount = threading.activeCount()
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


def launch_browser(host, port, root):
    import webbrowser
    if host == '0.0.0.0':
        host = 'localhost'

    if CONFIG['HTTPS_ENABLED']:
        protocol = 'https'
    else:
        protocol = 'http'
    if root and not root.startswith('/'):
        root = '/' + root
    try:
        webbrowser.open('%s://%s:%i%s/home' % (protocol, host, port, root))
    except Exception as e:
        logger.error('Could not launch browser:%s  %s' % (type(e).__name__, str(e)))


def start():
    global __INITIALIZED__, started, SHOW_SERIES, SHOW_MAGS, SHOW_AUDIO, SHOW_COMICS, SHOW_EBOOK

    if __INITIALIZED__:
        if not UPDATE_MSG:
            if CONFIG['HTTP_LOOK'] == 'legacy':
                SHOW_EBOOK = 1
                SHOW_AUDIO = 0
                SHOW_COMICS = 0
                SHOW_SERIES = 0
            else:
                SHOW_EBOOK = 1 if CONFIG['EBOOK_TAB'] else 0
                SHOW_AUDIO = 1 if CONFIG['AUDIO_TAB'] else 0
                SHOW_MAGS = 1 if CONFIG['MAG_TAB'] else 0
                SHOW_COMICS = 1 if CONFIG['COMIC_TAB'] else 0

                if CONFIG['ADD_SERIES']:
                    SHOW_SERIES = 1
                if not CONFIG['SERIES_TAB']:
                    SHOW_SERIES = 0

        # Crons and scheduled jobs started here
        # noinspection PyUnresolvedReferences
        SCHED.start()
        restartJobs(start='Start')
        started = True


def logmsg(level, msg):
    # log messages to logger if initialised, or print if not.
    if __INITIALIZED__:
        if level == 'error':
            logger.error(msg)
        elif level == 'debug':
            logger.debug(msg)
        elif level == 'warn':
            logger.warn(msg)
        else:
            logger.info(msg)
    else:
        print(level.upper(), msg)


def shutdown(restart=False, update=False):
    global __INITIALIZED__
    cherrypy.engine.exit()
    if SCHED:
        # noinspection PyUnresolvedReferences
        SCHED.shutdown(wait=False)
    # config_write() don't automatically rewrite config on exit

    if not restart and not update:
        logmsg('info', 'LazyLibrarian (pid %s) is shutting down...' % os.getpid())
        if DOCKER:
            # force container to shutdown
            # NOTE we don't seem to have sufficient permission to so this, so disabled the shutdown button
            os.kill(1, signal.SIGKILL)

    updated = False
    if update:
        logmsg('info', 'LazyLibrarian is updating...')
        try:
            updated = versioncheck.update()
            if updated:
                logmsg('info', 'Lazylibrarian version updated')
                if __INITIALIZED__:
                    CONFIG['GIT_UPDATED'] = str(int(time.time()))
                    config_write('Git')
        except Exception as e:
            logmsg('warn', 'LazyLibrarian failed to update: %s %s. Restarting.' % (type(e).__name__, str(e)))
            logmsg('error', str(traceback.format_exc()))
    if PIDFILE:
        logmsg('info', 'Removing pidfile %s' % PIDFILE)
        os.remove(syspath(PIDFILE))

    if restart:
        logmsg('info', 'LazyLibrarian is restarting ...')
        if not DOCKER:
            # Try to use the currently running python executable, as it is known to work
            # if not able to determine, sys.executable returns empty string or None
            # and we have to go looking for it...
            executable = sys.executable

            if not executable:
                if PY2:
                    prg = "python2"
                else:
                    prg = "python3"
                if os.name == 'nt':
                    params = ["where", prg]
                    try:
                        executable = subprocess.check_output(params, stderr=subprocess.STDOUT)
                        executable = makeUnicode(executable).strip()
                    except Exception as e:
                        logger.debug("where %s failed: %s %s" % (prg, type(e).__name__, str(e)))
                else:
                    params = ["which", prg]
                    try:
                        executable = subprocess.check_output(params, stderr=subprocess.STDOUT)
                        executable = makeUnicode(executable).strip()
                    except Exception as e:
                        logger.debug("which %s failed: %s %s" % (prg, type(e).__name__, str(e)))

            if not executable:
                executable = 'python'  # default if not found

            popen_list = [executable, FULL_PATH]
            popen_list += ARGS
            if '--update' in popen_list:
                popen_list.remove('--update')
            if LOGLEVEL:
                for item in ['--quiet', '-q', '--debug']:
                    if item in popen_list:
                        popen_list.remove(item)
            if '--nolaunch' not in popen_list:
                popen_list += ['--nolaunch']

            with open(syspath(os.path.join(CONFIG['LOGDIR'], 'upgrade.log')), 'a') as upgradelog:
                if updated:
                    upgradelog.write("%s %s\n" % (time.ctime(),
                                     'Restarting LazyLibrarian with ' + str(popen_list)))
                subprocess.Popen(popen_list, cwd=os.getcwd())

                if 'HTTP_HOST' in CONFIG:
                    # updating a running instance, not an --update
                    # wait for it to open the httpserver
                    host = CONFIG['HTTP_HOST']
                    if '0.0.0.0' in host:
                        host = 'localhost'  # windows doesn't like 0.0.0.0

                    if not host.startswith('http'):
                        host = 'http://' + host

                    # depending on proxy might need host:port/root or just host/root
                    if CONFIG['HTTP_ROOT']:
                        server1 = host + ':' + CONFIG['HTTP_PORT'] + '/' + CONFIG['HTTP_ROOT'].lstrip('/')
                        server2 = host + '/' + CONFIG['HTTP_ROOT'].lstrip('/')
                    else:
                        server1 = host + ':' + CONFIG['HTTP_PORT']
                        server2 = ''

                    msg = "Waiting for %s to start" % server1
                    if updated:
                        upgradelog.write("%s %s\n" % (time.ctime(), msg))
                    logmsg("info", msg)
                    pawse = 12
                    success = False
                    res = ''
                    while pawse:
                        result, success = fetchURL(server1, retry=False)
                        if not success and server2:
                            result, success = fetchURL(server2, retry=False)
                        if success:
                            try:
                                res = result.split('<title>')[1].split('</title>')[0]
                            except IndexError:
                                res = ''
                            success = res.startswith('LazyLibrarian')
                            if success:
                                break
                        else:
                            print("Waiting... %s" % pawse)
                            time.sleep(5)
                        pawse -= 1

                    archivename = 'backup.tgz'
                    if success:
                        msg = 'Reached webserver page %s, deleting backup' % res
                        if updated:
                            upgradelog.write("%s %s\n" % (time.ctime(), msg))
                        logmsg("info", msg)
                        try:
                            os.remove(syspath(archivename))
                        except OSError as e:
                            if e.errno != 2:  # doesn't exist is ok
                                msg = '{} {} {} {}'.format(type(e).__name__, 'deleting backup file:',
                                                           archivename, e.strerror)
                                logmsg("warn", msg)
                    else:
                        msg = 'Webserver failed to start, reverting update'
                        upgradelog.write("%s %s\n" % (time.ctime(), msg))
                        logmsg("info", msg)
                        if tarfile.is_tarfile(archivename):
                            try:
                                with tarfile.open(archivename) as tar:
                                    tar.extractall()
                                success = True
                            except Exception as e:
                                msg = 'Failed to unpack tarfile %s (%s): %s' % \
                                      (type(e).__name__, archivename, str(e))
                                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                logmsg("warn", msg)
                        else:
                            msg = "Invalid archive"
                            upgradelog.write("%s %s\n" % (time.ctime(), msg))
                            logmsg("warn", msg)
                        if success:
                            msg = "Restarting from backup"
                            upgradelog.write("%s %s\n" % (time.ctime(), msg))
                            logmsg("info", msg)
                            subprocess.Popen(popen_list, cwd=os.getcwd())

    logmsg('info', 'Lazylibrarian (pid %s) is exiting now' % os.getpid())
    sys.exit(0)
