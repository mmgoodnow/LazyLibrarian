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
#   Read and write LL config.ini file
#   Helpers to set up initial config

import os
import sys

import lazylibrarian
from lazylibrarian.formatter import make_unicode, thread_name, check_int, unaccented_bytes, unaccented
from lazylibrarian.common import logger, path_exists, schedule_job, syspath
from lazylibrarian import database 
from shutil import rmtree
import configparser

# These are the items in config.ini
# Not all are accessible from the web ui
# Any undefined on startup will be set to the default value
# Any _NOT_ in the web ui will remain unchanged on config save
CONFIG_GIT = ['GIT_REPO', 'GIT_USER', 'GIT_BRANCH', 'LATEST_VERSION', 'GIT_UPDATED', 'CURRENT_VERSION',
              'GIT_HOST', 'COMMITS_BEHIND', 'INSTALL_TYPE', 'AUTO_UPDATE']

CONFIG_NONWEB = ['BLOCKLIST_TIMER', 'DISPLAYLENGTH', 'ISBN_LOOKUP', 'WALL_COLUMNS', 'HTTP_TIMEOUT',
                 'PROXY_LOCAL', 'SKIPPED_EXT', 'CHERRYPYLOG', 'SYS_ENCODING', 'HIST_REFRESH',
                 'HTTP_EXT_TIMEOUT', 'CALIBRE_RENAME', 'NAME_RATIO', 'NAME_PARTIAL', 'NAME_PARTNAME',
                 'PREF_UNRARLIB', 'SEARCH_RATELIMIT', 'EMAIL_LIMIT', 'BOK_LOGIN',
                 'DELUGE_TIMEOUT', 'OL_URL', 'GR_URL', 'GB_URL', 'LT_URL', 'CV_URL', 'CX_URL']

CONFIG_DEFINITIONS = {
    # Name      Type   Section   Default
    'OL_URL': ('str', 'General', 'https://www.openlibrary.org'),
    'GR_URL': ('str', 'General', 'https://www.goodreads.com'),
    'GB_URL': ('str', 'General', 'https://www.googleapis.com'),
    'LT_URL': ('str', 'General', 'https://www.librarything.com'),
    'CV_URL': ('str', 'General', 'https://www.comicvine.gamespot.com'),
    'CX_URL': ('str', 'General', 'https://www.comixology.com'),
    'SHOW_NEWZ_PROV': ('bool', 'General', 1),
    'SHOW_TORZ_PROV': ('bool', 'General', 1),
    'SHOW_TOR_PROV': ('bool', 'General', 1),
    'SHOW_RSS_PROV': ('bool', 'General', 1),
    'SHOW_IRC_PROV': ('bool', 'General', 1),
    'SHOW_GEN_PROV': ('bool', 'General', 1),
    'SHOW_DIRECT_PROV': ('bool', 'General', 1),
    'MULTI_SOURCE': ('bool', 'General', 0),
    'USER_ACCOUNTS': ('bool', 'General', 0),
    'SINGLE_USER': ('bool', 'General', 0),
    'ADMIN_EMAIL': ('str', 'General', ''),
    'SYS_ENCODING': ('str', 'General', ''),
    'HOMEPAGE': ('str', 'General', ''),
    'LOGDIR': ('str', 'General', ''),
    'LOGLIMIT': ('int', 'General', 500),
    'LOGFILES': ('int', 'General', 10),
    'LOGSIZE': ('int', 'General', 204800),
    'LOGREDACT': ('bool', 'General', 0),
    'HOSTREDACT': ('bool', 'General', 0),
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
    'SSL_VERIFY': ('bool', 'General', 1),
    'NO_IPV6': ('bool', 'General', 0),
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
    'AUTHOR_DATE_FORMAT': ('str', 'General', '$d-$m-$Y'),
    'ISSUE_NOUNS': ('str', 'General', 'issue, iss, no, nr, #, n'),
    'VOLUME_NOUNS': ('str', 'General', "vol, volume"),
    'MAG_NOUNS': ('str', 'General', "winter, spring, summer, fall, autumn, christmas, edition, special"),
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
    'SERVER_ID': ('str', 'Telemetry', ''),
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
    'TESTDATA_DIR': ('str', 'General', ''),
    'DELETE_CSV': ('bool', 'General', 0),
    'DOWNLOAD_DIR': ('str', 'General', ''),
    'NZB_DOWNLOADER_SABNZBD': ('bool', 'USENET', 0),
    'NZB_DOWNLOADER_NZBGET': ('bool', 'USENET', 0),
    'NZB_DOWNLOADER_SYNOLOGY': ('bool', 'USENET', 0),
    'NZB_DOWNLOADER_BLACKHOLE': ('bool', 'USENET', 0),
    'NZB_BLACKHOLEDIR': ('str', 'USENET', ''),
    'NZB_PAUSED': ('bool', 'USENET', 0),
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
    'TORRENT_PAUSED': ('bool', 'TORRENT', 0),
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
    'TRANSMISSION_LABEL': ('str', 'TRANSMISSION', ''),
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
    'BOK_LOGIN': ('str', 'BOK', "https://singlelogin.me/rpc.php"),
    'BOK_USER': ('str', 'BOK', ''),
    'BOK_PASS': ('str', 'BOK', ''),
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
    'AUDIOBOOK_TYPE': ('str', 'General', 'mp3, m4b'),
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
    'CREATE_LINK': ('str', 'PostProcess', ''),
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
    'DEL_DOWNLOADFAILED': ('bool', 'Postprocess', 0),
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
    'RSS_ENABLED': ('bool', 'rss', 1),
    'RSS_PODCAST': ('bool', 'rss', 1),
    'RSS_HOST': ('str', 'rss', ''),
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
    'ZIP_AUDIOPARTS': ('bool', 'Preprocess', 0),
    'SWAP_COVERPAGE': ('bool', 'Preprocess', 0),
    'SHRINK_MAG': ('int', 'Preprocess', 0),
    # 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'),
}

FORCE_LOWER = ['EBOOK_TYPE', 'EMAIL_CONVERT_FROM', 'EMAIL_SEND_TYPE', 'AUDIOBOOK_TYPE', 'MAG_TYPE',
               'COMIC_TYPE', 'REJECT_MAGS', 'REJECT_WORDS', 'REJECT_AUDIO', 'REJECT_COMIC',
               'REJECT_PUBLISHER', 'BANNED_EXT', 'NAME_POSTFIX', 'NAME_DEFINITE', 'IMP_NOSPLIT',
               'ISSUE_NOUNS', 'VOLUME_NOUNS', 'MAG_NOUNS']

if os.name == 'nt':
    for k in ['EBOOK_DEST_FOLDER', 'MAG_DEST_FOLDER', 'COMIC_DEST_FOLDER']:
        val = CONFIG_DEFINITIONS[k]
        CONFIG_DEFINITIONS[k] = (val[0], val[1], val[2].replace('/', '\\'))

def check_ini_section(sec):
    """ Check if INI section exists, if not create it """
    # noinspection PyUnresolvedReferences
    if lazylibrarian.CFG.has_section(sec):
        return True
    else:
        # noinspection PyUnresolvedReferences
        lazylibrarian.CFG.add_section(sec)
        return False

def check_setting(cfg_type, cfg_name, item_name, def_val, log=True):
    """ Check option exists, coerce to correct type, or return default"""
    my_val = def_val
    if cfg_type == 'int':
        try:
            # noinspection PyUnresolvedReferences
            my_val = lazylibrarian.CFG.getint(cfg_name, item_name)
        except configparser.Error:
            # no such item, might be a new entry
            my_val = int(def_val)
        except Exception as e:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_admin:
                logger.warn('Invalid int for %s: %s, using default %s' % (cfg_name, item_name, int(def_val)))
                logger.debug(str(e))
            my_val = int(def_val)

    elif cfg_type == 'bool':
        try:
            # noinspection PyUnresolvedReferences
            my_val = lazylibrarian.CFG.getboolean(cfg_name, item_name)
        except configparser.Error:
            my_val = bool(def_val)
        except Exception as e:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_admin:
                logger.warn('Invalid bool for %s: %s, using default %s' % (cfg_name, item_name, bool(def_val)))
                logger.debug(str(e))
            my_val = bool(def_val)

    elif cfg_type == 'str':
        try:
            # noinspection PyUnresolvedReferences
            my_val = lazylibrarian.CFG.get(cfg_name, item_name)
            # Old config file format had strings in quotes. ConfigParser doesn't.
            if my_val.startswith('"') and my_val.endswith('"'):
                my_val = my_val[1:-1]
            if not len(my_val):
                my_val = def_val
        except configparser.Error:
            my_val = str(def_val)
        except Exception as e:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_admin:
                logger.warn('Invalid str for %s: %s, using default %s' % (cfg_name, item_name, str(def_val)))
                logger.debug(str(e))
            my_val = str(def_val)
        finally:
            my_val = make_unicode(my_val)

    check_ini_section(cfg_name)
    # noinspection PyUnresolvedReferences
    lazylibrarian.CFG.set(cfg_name, item_name, my_val)
    if log:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_admin:
            logger.debug("%s : %s -> %s" % (cfg_name, item_name, my_val))

    return my_val

class LLConfigParser(configparser.RawConfigParser):

    def _is_modified(self, section, key, value):
        if key.upper() in CONFIG_DEFINITIONS.keys():
            dtype, dsection, default = CONFIG_DEFINITIONS[key.upper()]
            if dsection != section:
                return True

            if dtype == 'int':
                try:
                    # Some of these are represented as '1', so convert first
                    default = int(default)
                    return value != default
                except:
                    pass

            return value != '' and value != default
        else:
            # Don't store empty values
            return value != '' 

    # Override _write_section, to not write non-default values
    def _write_section(self, fp, section_name, section_items, delimiter):
        """Write a single section to the specified `fp'."""
        towrite = []
        for key, value in section_items:
            value = self._interpolation.before_write(self, section_name, key,
                                                     value)
            if self._is_modified(section_name, key, value):
                if value is not None or not self._allow_no_value:
                    value = delimiter + str(value).replace('\n', '\n\t')
                else:
                    value = ""

                towrite.append((key, value))

        if len(towrite):
            # Only write the section if anything is non-default
            fp.write("[{}]\n".format(section_name))
            for key, value in towrite:
                fp.write(f"{key}{value}\n")
            fp.write("\n")


def readConfigFile():
    """
    Read the config.ini file, but do not yet process it - that happens in config_read
    """
    lazylibrarian.CFG = LLConfigParser()
    lazylibrarian.CFG.read(lazylibrarian.CONFIGFILE)


# noinspection PyUnresolvedReferences
def config_read(reloaded=False):
    # legacy name conversion
    if lazylibrarian.CFG.has_section('GEN'):
        check_ini_section('GEN_0')
        lazylibrarian.CFG.set('GEN_0', 'ENABLED', lazylibrarian.CFG.get('GEN', 'GEN'))
        lazylibrarian.CFG.set('GEN_0', 'DISPNAME', 'GEN_0')
        lazylibrarian.CFG.set('GEN_0', 'HOST', lazylibrarian.CFG.get('GEN', 'GEN_HOST'))
        lazylibrarian.CFG.set('GEN_0', 'SEARCH', lazylibrarian.CFG.get('GEN', 'GEN_SEARCH'))
        lazylibrarian.CFG.set('GEN_0', 'DLPRIORITY', lazylibrarian.CFG.get('GEN', 'GEN_DLPRIORITY'))
        lazylibrarian.CFG.set('GEN_0', 'DLTYPES', lazylibrarian.CFG.get('GEN', 'GEN_DLTYPES'))
        check_ini_section('GEN_1')
        lazylibrarian.CFG.set('GEN_1', 'ENABLED', lazylibrarian.CFG.get('GEN', 'GEN2'))
        lazylibrarian.CFG.set('GEN_1', 'DISPNAME', 'GEN_1')
        lazylibrarian.CFG.set('GEN_1', 'HOST', lazylibrarian.CFG.get('GEN', 'GEN2_HOST'))
        lazylibrarian.CFG.set('GEN_1', 'SEARCH', lazylibrarian.CFG.get('GEN', 'GEN2_SEARCH'))
        lazylibrarian.CFG.set('GEN_1', 'DLPRIORITY', lazylibrarian.CFG.get('GEN', 'GEN2_DLPRIORITY'))
        lazylibrarian.CFG.set('GEN_1', 'DLTYPES', lazylibrarian.CFG.get('GEN2', 'GEN2_DLTYPES'))
        lazylibrarian.CFG.remove_section('GEN')
        lazylibrarian.CFG.remove_section('GEN2')

    count = 0
    while lazylibrarian.CFG.has_section('Newznab%i' % count):
        newz_name = 'Newznab%i' % count
        disp_name = check_setting('str', newz_name, 'dispname', newz_name)

        lazylibrarian.NEWZNAB_PROV.append({"NAME": newz_name,
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
    while lazylibrarian.CFG.has_section('Torznab%i' % count):
        torz_name = 'Torznab%i' % count
        disp_name = check_setting('str', torz_name, 'dispname', torz_name)

        lazylibrarian.TORZNAB_PROV.append({"NAME": torz_name,
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
    while lazylibrarian.CFG.has_section('RSS_%i' % count):
        rss_name = 'RSS_%i' % count
        disp_name = check_setting('str', rss_name, 'dispname', rss_name)

        lazylibrarian.RSS_PROV.append({"NAME": rss_name,
                         "DISPNAME": disp_name,
                         "ENABLED": check_setting('bool', rss_name, 'ENABLED', 0),
                         "HOST": check_setting('str', rss_name, 'HOST', ''),
                         "DLPRIORITY": check_setting('int', rss_name, 'DLPRIORITY', 0),
                         "DLTYPES": check_setting('str', rss_name, 'dltypes', 'E'),
                         "LABEL": check_setting('str', rss_name, 'label', ''),
                         })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_rss_slot()

    count = 0
    while lazylibrarian.CFG.has_section('IRC_%i' % count):
        irc_name = 'IRC_%i' % count
        disp_name = check_setting('str', irc_name, 'dispname', irc_name)

        lazylibrarian.IRC_PROV.append({"NAME": irc_name,
                         "DISPNAME": disp_name,
                         "ENABLED": check_setting('bool', irc_name, 'ENABLED', 0),
                         "SERVER": check_setting('str', irc_name, 'SERVER', ''),
                         "CHANNEL": check_setting('str', irc_name, 'CHANNEL', ''),
                         "BOTNICK": check_setting('str', irc_name, 'BOTNICK', ''),
                         "BOTPASS": check_setting('str', irc_name, 'BOTPASS', ''),
                         "SEARCH": check_setting('str', irc_name, 'SEARCH', '@search'),
                         "DLPRIORITY": check_setting('int', irc_name, 'DLPRIORITY', 0),
                         "DLTYPES": check_setting('str', irc_name, 'dltypes', 'E'),
                         })
        count += 1
    # if the last slot is full, add an empty one on the end
    add_irc_slot()

    count = 0
    while lazylibrarian.CFG.has_section('GEN_%i' % count):
        gen_name = 'GEN_%i' % count
        disp_name = check_setting('str', gen_name, 'DISPNAME', gen_name)

        lazylibrarian.GEN_PROV.append({"NAME": gen_name,
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
    while lazylibrarian.CFG.has_section('APPRISE_%i' % count):
        apprise_name = 'APPRISE_%i' % count
        lazylibrarian.APPRISE_PROV.append({"NAME": check_setting('str', apprise_name, 'NAME', apprise_name),
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
        lazylibrarian.CONFIG[key.upper()] = check_setting(item_type, section, key.lower(), default)

    # new config options...
    if lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER'] == 'None':
        lazylibrarian.CFG.set('PostProcess', 'audiobook_dest_folder', lazylibrarian.CONFIG['EBOOK_DEST_FOLDER'])
        lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER'] = lazylibrarian.CONFIG['EBOOK_DEST_FOLDER']

    if not lazylibrarian.CONFIG['LOGDIR']:
        lazylibrarian.CONFIG['LOGDIR'] = os.path.join(lazylibrarian.DATADIR, 'Logs')
    if lazylibrarian.CONFIG['HTTP_PORT'] < 21 or lazylibrarian.CONFIG['HTTP_PORT'] > 65535:
        lazylibrarian.CONFIG['HTTP_PORT'] = 5299

    # to make matching easier/faster
    for item in FORCE_LOWER:
        lazylibrarian.CONFIG[item] = lazylibrarian.CONFIG[item].lower()

    if os.name == 'nt':
        for fname in ['EBOOK_DEST_FOLDER', 'MAG_DEST_FOLDER', 'COMIC_DEST_FOLDER']:
            if '/' in lazylibrarian.CONFIG[fname]:
                logger.warn('Please check your %s setting' % fname)
                lazylibrarian.CONFIG[fname] = lazylibrarian.CONFIG[fname].replace('/', '\\')

    for fname in ['EBOOK_DEST_FILE', 'MAG_DEST_FILE', 'AUDIOBOOK_DEST_FILE', 'AUDIOBOOK_SINGLE_FILE']:
        if os.sep in lazylibrarian.CONFIG[fname]:
            logger.warn('Please check your %s setting, contains "%s"' % (fname, os.sep))
    if lazylibrarian.CONFIG['HTTP_LOOK'] in ['legacy', 'default']:
        logger.warn('configured interface is deprecated, new features are in bookstrap')
        lazylibrarian.CONFIG['HTTP_LOOK'] = 'bookstrap'

    for item in ['OL_URL', 'GR_URL', 'GB_URL', 'LT_URL', 'CV_URL', 'CX_URL']:
        url = lazylibrarian.CONFIG[item].rstrip('/')
        if not url.startswith('http'):
            url = 'http://' + url
        lazylibrarian.CONFIG[item] = url

    ###################################################################
    # ensure all these are boolean 1 0, not True False for javascript #
    ###################################################################
    # Suppress series tab if there are none and user doesn't want to add any
    if lazylibrarian.CONFIG['ADD_SERIES']:
        lazylibrarian.SHOW_SERIES = 1
    # Or suppress if tab is disabled
    if not lazylibrarian.CONFIG['SERIES_TAB']:
        lazylibrarian.SHOW_SERIES = 0
    # Suppress tabs if disabled
    lazylibrarian.SHOW_EBOOK = 1 if lazylibrarian.CONFIG['EBOOK_TAB'] else 0
    lazylibrarian.SHOW_AUDIO = 1 if lazylibrarian.CONFIG['AUDIO_TAB'] else 0
    lazylibrarian.SHOW_MAGS = 1 if lazylibrarian.CONFIG['MAG_TAB'] else 0
    lazylibrarian.SHOW_COMICS = 1 if lazylibrarian.CONFIG['COMIC_TAB'] else 0
    # Suppress audio/comic tabs if on legacy interface
    if lazylibrarian.CONFIG['HTTP_LOOK'] == 'legacy':
        lazylibrarian.SHOW_AUDIO = 0
        lazylibrarian.SHOW_COMICS = 0
        lazylibrarian.SHOW_EBOOK = 1
    else:
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'eBooks' and not lazylibrarian.SHOW_EBOOK:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'AudioBooks' and not lazylibrarian.SHOW_AUDIO:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'Magazines' and not lazylibrarian.SHOW_MAGS:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'Comics' and not lazylibrarian.SHOW_COMICS:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'Series' and not lazylibrarian.SHOW_SERIES:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''

    for item in ['BOOK_IMG', 'MAG_IMG', 'COMIC_IMG', 'AUTHOR_IMG', 'TOGGLES']:
        lazylibrarian.CONFIG[item] = 1 if lazylibrarian.CONFIG[item] else 0

    if lazylibrarian.CONFIG['SSL_CERTS'] and not path_exists(lazylibrarian.CONFIG['SSL_CERTS']):
        logger.warn("SSL_CERTS [%s] not found" % lazylibrarian.CONFIG['SSL_CERTS'])
        lazylibrarian.CONFIG['SSL_CERTS'] = ''

    if reloaded:
        logger.info('Config file reloaded')
    else:
        logger.info('Config file loaded')

# noinspection PyUnresolvedReferences
def config_write(part=None):
    lazylibrarian.REDACTLIST = []  # invalidate redactlist as config has changed

    if part:
        logger.info("Writing config for section [%s]" % part)

    currentname = thread_name()
    thread_name("CONFIG_WRITE")

    interface = lazylibrarian.CFG.get('General', 'http_look')
    if lazylibrarian.CONFIG['HTTP_LOOK'] != interface:
        makocache = os.path.join(lazylibrarian.CACHEDIR, 'mako')
        logger.debug("Clearing mako cache")
        rmtree(makocache)
        os.makedirs(makocache)
        version_file = os.path.join(makocache, 'python_version.txt')
        with open(version_file, 'w') as fp:
            fp.write(sys.version.split()[0] + ':' + lazylibrarian.CONFIG['HTTP_LOOK'])

    for key in list(CONFIG_DEFINITIONS.keys()):
        _, section, _ = CONFIG_DEFINITIONS[key]
        if key in ['FILE_PERM', 'DIR_PERM']:
            if key == 'FILE_PERM':
                def_val = '644'
            else:
                def_val = '755'
            value = lazylibrarian.CONFIG[key]
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
            lazylibrarian.CONFIG[key] = value

        elif key in ['WALL_COLUMNS', 'DISPLAY_LENGTH']:  # may be modified by user interface but not on config page
            value = check_int(lazylibrarian.CONFIG[key], 5)
        elif part and section != part:
            value = lazylibrarian.CFG.get(section, key.lower())  # keep the old value
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_admin:
                logger.debug("Leaving %s unchanged (%s)" % (key, value))
        elif key not in CONFIG_NONWEB:
            check_ini_section(section)
            value = lazylibrarian.CONFIG[key]
            if key == 'LOGLEVEL':
                lazylibrarian.LOGLEVEL = check_int(value, 1)
            elif key in FORCE_LOWER:
                value = value.lower()
        else:
            # keep the old value
            value = lazylibrarian.CFG.get(section, key.lower())
            lazylibrarian.CONFIG[key] = value
            # if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
            #    logger.debug("Leaving %s unchanged (%s)" % (key, value))

        if isinstance(value, str):
            value = value.strip()
            if 'DLTYPES' in key:
                value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                if not value:
                    value = 'E'
                lazylibrarian.CONFIG[key] = value

        if key in ['SEARCH_BOOKINTERVAL', 'SEARCH_MAGINTERVAL', 'SCAN_INTERVAL', 'VERSIONCHECK_INTERVAL',
                   'SEARCHRSS_INTERVAL', 'GOODREADS_INTERVAL', 'WISHLIST_INTERVAL', 'SEARCH_COMICINTERVAL']:
            oldvalue = lazylibrarian.CFG.get(section, key.lower())
            if value != oldvalue:
                if key == 'SEARCH_BOOKINTERVAL':
                    schedule_job('Restart', 'search_book')
                elif key == 'SEARCH_MAGINTERVAL':
                    schedule_job('Restart', 'search_magazines')
                elif key == 'SEARCHRSS_INTERVAL':
                    schedule_job('Restart', 'search_rss_book')
                elif key == 'WISHLIST_INTERVAL':
                    schedule_job('Restart', 'search_wishlist')
                elif key == 'SEARCH_COMICINTERVAL':
                    schedule_job('Restart', 'search_comics')
                elif key == 'SCAN_INTERVAL':
                    schedule_job('Restart', 'PostProcessor')
                elif key == 'VERSIONCHECK_INTERVAL':
                    schedule_job('Restart', 'check_for_updates')
                elif key == 'GOODREADS_INTERVAL' and lazylibrarian.CONFIG['GR_SYNC']:
                    schedule_job('Restart', 'sync_to_gr')

        lazylibrarian.CFG.set(section, key.lower(), value)

    # sanity check for typos...
    for key in list(lazylibrarian.CONFIG.keys()):
        if key not in list(CONFIG_DEFINITIONS.keys()):
            logger.warn('Unsaved/invalid config key: %s' % key)

    if not part or part.lower().startswith('newznab') or part.lower().startswith('torznab'):
        nab_items = ['ENABLED', 'DISPNAME', 'HOST', 'API', 'GENERALSEARCH', 'BOOKSEARCH', 'MAGSEARCH',
                     'AUDIOSEARCH', 'BOOKCAT', 'MAGCAT', 'AUDIOCAT', 'EXTENDED', 'DLPRIORITY', 'DLTYPES',
                     'UPDATED', 'MANUAL', 'APILIMIT', 'RATELIMIT', 'COMICSEARCH', 'COMICCAT']
        for entry in [[lazylibrarian.NEWZNAB_PROV, 'Newznab', []], [lazylibrarian.TORZNAB_PROV, 'Torznab', ['SEEDERS']]]:
            new_list = []
            # strip out any empty slots
            for provider in entry[0]:  # type: dict
                if provider['HOST']:
                    new_list.append(provider)

            if part:  # only update the named provider
                part = part.replace('nab_', 'nab')
                for provider in new_list:
                    if provider['NAME'].lower() != part.lower():  # keep old values
                        if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
                            logger.debug("Keep %s" % provider['NAME'])
                        for item in nab_items + entry[2]:
                            try:
                                provider[item] = lazylibrarian.CFG.get(provider['NAME'], item.lower())
                            except configparser.NoSectionError:
                                logger.debug("No section [%s]" % provider['NAME'])
                                break
                            except configparser.NoOptionError:
                                logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                                pass

            # renumber the items
            for index, item in enumerate(new_list):
                item['NAME'] = '%s%i' % (entry[1], index)

            # delete the old entries
            sections = lazylibrarian.CFG.sections()
            for item in sections:
                if item.startswith(entry[1]):
                    lazylibrarian.CFG.remove_section(item)

            for provider in new_list:
                check_ini_section(provider['NAME'])
                for item in nab_items + entry[2]:
                    value = provider[item]
                    if isinstance(value, str):
                        value = value.strip()
                    if item == 'DLTYPES':
                        value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                        if not value:
                            value = 'E'
                        provider['DLTYPES'] = value

                    lazylibrarian.CFG.set(provider['NAME'], item, value)

            if entry[1] == 'Newznab':
                lazylibrarian.NEWZNAB_PROV = new_list
                add_newz_slot()
            else:
                lazylibrarian.TORZNAB_PROV = new_list
                add_torz_slot()

    if not part or part.startswith('rss_'):
        rss_items = ['ENABLED', 'DISPNAME', 'HOST', 'DLPRIORITY', 'DLTYPES', 'LABEL']
        new_list = []
        # strip out any empty slots
        for provider in lazylibrarian.RSS_PROV:
            if provider['HOST']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].lower() != part:  # keep old values
                    if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in rss_items:
                        try:
                            provider[item] = lazylibrarian.CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'RSS_%i' % index

        # strip out the old config entries
        sections = lazylibrarian.CFG.sections()
        for item in sections:
            if item.startswith('RSS_'):
                lazylibrarian.CFG.remove_section(item)

        for provider in new_list:
            check_ini_section(provider['NAME'])
            for item in rss_items:
                value = provider[item]
                if isinstance(value, str):
                    value = value.strip()
                if item == 'DLTYPES':
                    value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                    if not value:
                        value = 'E'
                    provider['DLTYPES'] = value
                lazylibrarian.CFG.set(provider['NAME'], item, value)

        lazylibrarian.RSS_PROV = new_list
        add_rss_slot()

    if not part or part.startswith('GEN_'):
        gen_items = ['ENABLED', 'DISPNAME', 'HOST', 'SEARCH', 'DLPRIORITY', 'DLTYPES']
        new_list = []
        # strip out any empty slots
        for provider in lazylibrarian.GEN_PROV:
            if provider['HOST']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].upper() != part:  # keep old values
                    if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in gen_items:
                        try:
                            provider[item] = lazylibrarian.CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'GEN_%i' % index

        # strip out the old config entries
        sections = lazylibrarian.CFG.sections()
        for item in sections:
            if item.startswith('GEN'):
                lazylibrarian.CFG.remove_section(item)

        for provider in new_list:
            check_ini_section(provider['NAME'])
            for item in gen_items:
                value = provider[item]
                if isinstance(value, str):
                    value = value.strip()
                if item == 'DLTYPES':
                    value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                    if not value:
                        value = 'E'
                    provider['DLTYPES'] = value
                lazylibrarian.CFG.set(provider['NAME'], item, value)

        lazylibrarian.GEN_PROV = new_list
        add_gen_slot()

    if not part or part.startswith('IRC_'):
        irc_items = ['ENABLED', 'DISPNAME', 'SERVER', 'CHANNEL', 'BOTNICK', 'BOTPASS', 'SEARCH',
                     'DLPRIORITY', 'DLTYPES']
        new_list = []
        # strip out any empty slots
        for provider in lazylibrarian.IRC_PROV:
            if provider['SERVER']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].upper() != part:  # keep old values
                    if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in irc_items:
                        try:
                            provider[item] = lazylibrarian.CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'IRC_%i' % index

        # strip out the old config entries
        sections = lazylibrarian.CFG.sections()
        for item in sections:
            if item.startswith('IRC_'):
                lazylibrarian.CFG.remove_section(item)

        for provider in new_list:
            check_ini_section(provider['NAME'])
            for item in irc_items:
                value = provider[item]
                if isinstance(value, str):
                    value = value.strip()
                if item == 'DLTYPES':
                    value = ','.join(sorted(set([i for i in value.upper() if i in 'ACEM'])))
                    if not value:
                        value = 'E'
                    provider['DLTYPES'] = value
                lazylibrarian.CFG.set(provider['NAME'], item, value)

        lazylibrarian.IRC_PROV = new_list
        add_irc_slot()

    if not part or part.startswith('apprise_'):
        apprise_items = ['NAME', 'DISPNAME', 'SNATCH', 'DOWNLOAD', 'URL']
        new_list = []
        # strip out any empty slots
        for provider in lazylibrarian.APPRISE_PROV:
            if provider['URL']:
                new_list.append(provider)

        if part:  # only update the named provider
            for provider in new_list:
                if provider['NAME'].lower() != part:  # keep old values
                    if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
                        logger.debug("Keep %s" % provider['NAME'])
                    for item in apprise_items:
                        try:
                            provider[item] = lazylibrarian.CFG.get(provider['NAME'], item.lower())
                        except configparser.NoOptionError:
                            logger.debug("No option [%s] in %s" % (item, provider['NAME']))
                            pass

        # renumber the items
        for index, item in enumerate(new_list):
            item['NAME'] = 'APPRISE_%i' % index

        # strip out the old config entries
        sections = lazylibrarian.CFG.sections()
        for item in sections:
            if item.startswith('APPRISE_'):
                lazylibrarian.CFG.remove_section(item)

        for provider in new_list:
            check_ini_section(provider['NAME'])
            for item in apprise_items:
                value = provider[item]
                if isinstance(value, str):
                    value = value.strip()
                lazylibrarian.CFG.set(provider['NAME'], item, value)
        lazylibrarian.APPRISE_PROV = new_list

        add_apprise_slot()
    #
    if lazylibrarian.CONFIG['ADD_SERIES']:
        lazylibrarian.SHOW_SERIES = 1
    if not lazylibrarian.CONFIG['SERIES_TAB']:
        lazylibrarian.SHOW_SERIES = 0

    lazylibrarian.SHOW_MAGS = 1 if lazylibrarian.CONFIG['MAG_TAB'] else 0
    lazylibrarian.SHOW_COMICS = 1 if lazylibrarian.CONFIG['COMIC_TAB'] else 0
    lazylibrarian.SHOW_EBOOK = 1 if lazylibrarian.CONFIG['EBOOK_TAB'] else 0
    lazylibrarian.SHOW_AUDIO = 1 if lazylibrarian.CONFIG['AUDIO_TAB'] else 0

    if lazylibrarian.CONFIG['HTTP_LOOK'] == 'legacy':
        lazylibrarian.SHOW_AUDIO = 0
        lazylibrarian.SHOW_COMICS = 0
        lazylibrarian.SHOW_EBOOK = 1
    else:
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'eBooks' and not lazylibrarian.SHOW_EBOOK:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'AudioBooks' and not lazylibrarian.SHOW_AUDIO:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'Magazines' and not lazylibrarian.SHOW_MAGS:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'Comics' and not lazylibrarian.SHOW_COMICS:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''
        if lazylibrarian.CONFIG['HOMEPAGE'] == 'Series' and not lazylibrarian.SHOW_SERIES:
            lazylibrarian.CONFIG['HOMEPAGE'] = ''

    if lazylibrarian.CONFIG['NO_SINGLE_BOOK_SERIES']:
        db = database.DBConnection()
        db.action('DELETE from series where total=1')
        db.close()
    msg = None
    try:
        with open(syspath(lazylibrarian.CONFIGFILE + '.new'), "w") as configfile:
            lazylibrarian.CFG.write(configfile)
    except Exception as e:
        msg = '{} {} {} {}'.format('Unable to create new config file:', lazylibrarian.CONFIGFILE, type(e).__name__, str(e))
        logger.warn(msg)
        thread_name(currentname)
        return
    try:
        os.remove(syspath(lazylibrarian.CONFIGFILE + '.bak'))
    except OSError as e:
        if e.errno != 2:  # doesn't exist is ok
            msg = '{} {}{} {} {}'.format(type(e).__name__, 'deleting backup file:', lazylibrarian.CONFIGFILE, '.bak', e.strerror)
            logger.warn(msg)
    try:
        os.rename(syspath(lazylibrarian.CONFIGFILE), syspath(lazylibrarian.CONFIGFILE + '.bak'))
    except OSError as e:
        if e.errno != 2:  # doesn't exist is ok as wouldn't exist until first save
            msg = '{} {} {} {}'.format('Unable to backup config file:', lazylibrarian.CONFIGFILE, type(e).__name__, e.strerror)
            logger.warn(msg)
    try:
        os.rename(syspath(lazylibrarian.CONFIGFILE + '.new'), syspath(lazylibrarian.CONFIGFILE))
    except OSError as e:
        msg = '{} {} {} {}'.format('Unable to rename new config file:', lazylibrarian.CONFIGFILE, type(e).__name__, e.strerror)
        logger.warn(msg)

    if not msg:
        if part is None:
            part = ''
        msg = 'Config file [%s] %s has been updated' % (lazylibrarian.CONFIGFILE, part)
        logger.info(msg)

    thread_name(currentname)

# noinspection PyUnresolvedReferences
def add_newz_slot():
    count = len(lazylibrarian.NEWZNAB_PROV)

    if count == 0 or len(lazylibrarian.CFG.get('Newznab%i' % int(count - 1), 'HOST')):
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
        lazylibrarian.NEWZNAB_PROV.append(empty)

        check_ini_section(prov_name)
        for item in empty:
            if item != 'NAME':
                lazylibrarian.CFG.set(prov_name, item, empty[item])


# noinspection PyUnresolvedReferences
def add_torz_slot():
    count = len(lazylibrarian.TORZNAB_PROV)
    if count == 0 or len(lazylibrarian.CFG.get('Torznab%i' % int(count - 1), 'HOST')):
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
        lazylibrarian.TORZNAB_PROV.append(empty)

        check_ini_section(prov_name)
        for item in empty:
            if item != 'NAME':
                lazylibrarian.CFG.set(prov_name, item, empty[item])

# noinspection PyUnresolvedReferences
def add_rss_slot():
    count = len(lazylibrarian.RSS_PROV)
    if count == 0 or len(lazylibrarian.CFG.get('RSS_%i' % int(count - 1), 'HOST')):
        rss_name = 'RSS_%i' % count
        check_ini_section(rss_name)
        lazylibrarian.CFG.set(rss_name, 'ENABLED', False)
        lazylibrarian.CFG.set(rss_name, 'HOST', '')
        # CFG.set(rss_name, 'USER', '')
        # CFG.set(rss_name, 'PASS', '')
        lazylibrarian.RSS_PROV.append({"NAME": rss_name,
                         "DISPNAME": rss_name,
                         "ENABLED": 0,
                         "HOST": '',
                         "DLPRIORITY": 0,
                         "DLTYPES": 'E',
                         'LABEL': '',
                         })


# noinspection PyUnresolvedReferences
def add_irc_slot():
    count = len(lazylibrarian.IRC_PROV)
    if count == 0 or len(lazylibrarian.CFG.get('IRC_%i' % int(count - 1), 'SERVER')):
        irc_name = 'IRC_%i' % count
        check_ini_section(irc_name)
        lazylibrarian.CFG.set(irc_name, 'ENABLED', False)
        lazylibrarian.CFG.set(irc_name, 'SERVER', '')
        lazylibrarian.CFG.set(irc_name, 'CHANNEL', '')
        lazylibrarian.CFG.set(irc_name, 'BOTNICK', '')
        lazylibrarian.CFG.set(irc_name, 'BOTPASS', '')
        lazylibrarian.CFG.set(irc_name, 'SEARCH', '')
        lazylibrarian.CFG.set(irc_name, 'DLPRIORITY', 0)
        lazylibrarian.CFG.set(irc_name, 'DLTYPES', 'E')
        lazylibrarian.IRC_PROV.append({"NAME": irc_name,
                         "DISPNAME": irc_name,
                         "ENABLED": 0,
                         "SERVER": '',
                         "CHANNEL": '',
                         "BOTNICK": '',
                         "BOTPASS": '',
                         "SEARCH": '',
                         "DLPRIORITY": 0,
                         "DLTYPES": 'E'
                         })


# noinspection PyUnresolvedReferences
def add_gen_slot():
    count = len(lazylibrarian.GEN_PROV)
    if count == 0 or len(lazylibrarian.CFG.get('GEN_%i' % int(count - 1), 'HOST')):
        gen_name = 'GEN_%i' % count
        check_ini_section(gen_name)
        lazylibrarian.CFG.set(gen_name, 'ENABLED', False)
        lazylibrarian.CFG.set(gen_name, 'HOST', '')
        lazylibrarian.CFG.set(gen_name, 'SEARCH', '')
        lazylibrarian.CFG.set(gen_name, 'DLPRIORITY', 0)
        lazylibrarian.CFG.set(gen_name, 'DLTYPES', 'E')
        lazylibrarian.GEN_PROV.append({"NAME": gen_name,
                         "DISPNAME": gen_name,
                         "ENABLED": 0,
                         "HOST": '',
                         "SEARCH": '',
                         "DLPRIORITY": 0,
                         "DLTYPES": 'E'
                         })


# noinspection PyUnresolvedReferences
def add_apprise_slot():
    count = len(lazylibrarian.APPRISE_PROV)
    if count == 0 or len(lazylibrarian.CFG.get('APPRISE_%i' % int(count - 1), 'URL')):
        apprise_name = 'APPRISE_%i' % count
        check_ini_section(apprise_name)
        lazylibrarian.CFG.set(apprise_name, 'NAME', apprise_name)
        lazylibrarian.CFG.set(apprise_name, 'DISPNAME', apprise_name)
        lazylibrarian.CFG.set(apprise_name, 'SNATCH', False)
        lazylibrarian.CFG.set(apprise_name, 'DOWNLOAD', False)
        lazylibrarian.CFG.set(apprise_name, 'URL', '')
        lazylibrarian.APPRISE_PROV.append({"NAME": apprise_name, "DISPNAME": apprise_name, "SNATCH": 0, "DOWNLOAD": 0, "URL": ''})
