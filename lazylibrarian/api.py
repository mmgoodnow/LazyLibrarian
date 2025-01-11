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


import configparser
import json
import logging
import os
import shutil
import sys
import threading
from queue import Queue
from urllib.parse import urlsplit, urlunsplit

import dateutil.parser as dp

import cherrypy
import lazylibrarian
from lazylibrarian import database
from lazylibrarian.blockhandler import BLOCKHANDLER
from lazylibrarian.bookrename import audio_rename, name_vars, book_rename
from lazylibrarian.bookwork import set_work_pages, get_work_series, get_work_page, set_all_book_series, \
    get_series_members, get_series_authors, delete_empty_series, get_book_authors, set_all_book_authors, \
    set_work_id, get_gb_info, set_genres, genre_filter, get_book_pubdate, add_series_members
from lazylibrarian.cache import cache_img, clean_cache, ImageType
from lazylibrarian.calibre import sync_calibre_list, calibre_list
from lazylibrarian.comicid import cv_identify, cx_identify, comic_metadata
from lazylibrarian.comicscan import comic_scan
from lazylibrarian.comicsearch import search_comics
from lazylibrarian.common import log_header, create_support_zip, get_readinglist, dbbackup
from lazylibrarian.config2 import CONFIG, wishlist_type
from lazylibrarian.configtypes import ConfigBool, ConfigInt
from lazylibrarian.csvfile import import_csv, export_csv, dump_table
from lazylibrarian.filesystem import DIRS, path_isfile, path_isdir, syspath, listdir, setperm
from lazylibrarian.formatter import today, format_author_name, check_int, plural, replace_all, get_list, thread_name
from lazylibrarian.gb import GoogleBooks
from lazylibrarian.gr import GoodReads
from lazylibrarian.grsync import grfollow, grsync
from lazylibrarian.hc import HardCover
from lazylibrarian.images import get_author_image, get_author_images, get_book_cover, get_book_covers, \
    create_mag_covers, create_mag_cover, shrink_mag
from lazylibrarian.importer import add_author_to_db, add_author_name_to_db, update_totals, de_duplicate
from lazylibrarian.librarysync import library_scan
from lazylibrarian.logconfig import LOGCONFIG
from lazylibrarian.magazinescan import magazine_scan
from lazylibrarian.manualbook import search_item
from lazylibrarian.ol import OpenLibrary
from lazylibrarian.postprocess import process_dir, process_alternate, create_opf, process_img, \
    process_book_from_dir, process_mag_from_file
from lazylibrarian.preprocessor import preprocess_ebook, preprocess_audio, preprocess_magazine
from lazylibrarian.processcontrol import get_cpu_use, get_process_memory
from lazylibrarian.providers import get_capabilities
from lazylibrarian.rssfeed import gen_feed
from lazylibrarian.scheduling import show_jobs, restart_jobs, check_running_jobs, all_author_update, \
    author_update, series_update, show_stats, SchedulerCommand
from lazylibrarian.searchbook import search_book
from lazylibrarian.searchmag import search_magazines, get_issue_date
from lazylibrarian.searchrss import search_rss_book, search_wishlist
from lazylibrarian.telemetry import TELEMETRY, telemetry_send

# dict of known commands. 0 = any valid api key, 1 = not available with the read-only key
cmd_dict = {'help': (0, 'list available commands. Time consuming commands take an optional &wait parameter '
                        'if you want to wait for completion, otherwise they return OK straight away '
                        'and run in the background'),
            'showMonths': (0, 'List installed monthnames'),
            'dumpMonths': (1, 'Save installed monthnames to file'),
            'deduplicate': (1, '&id= [&wait] De-duplicate authors books'),
            'saveTable': (0, '&table= Save a database table to a file'),
            'getIndex': (0, 'list all authors'),
            'getAuthor': (0, '&id= get author by AuthorID and list their books'),
            'getAuthorImage': (0, '&id= [&refresh] [&max] get one or more images for this author'),
            'setAuthorImage': (1, '&id= &img= set a new image for this author'),
            'setAuthorLock': (1, '&id= lock author name/image/dates'),
            'setAuthorUnlock': (1, '&id= unlock author name/image/dates'),
            'setBookLock': (1, '&id= lock book details'),
            'setBookUnlock': (1, '&id= unlock book details'),
            'setBookImage': (1, '&id= &img= set a new image for this book'),
            'shrinkMag': (1, '&name= &size= shrink magazine size'),
            'getAuthorImages': (1, '[&wait] get images for all authors without one'),
            'getWanted': (0, 'list wanted books'),
            'getRead': (1, '&id= list read books for current user'),
            'getReading': (1, '&id= list currently-reading books for user'),
            'getToRead': (1, '&id= list to-read books for user'),
            'getAbandoned': (1, '&id= list abandoned books for user'),
            'getSnatched': (0, 'list snatched books'),
            'getHistory': (0, 'list history'),
            'getDebug': (0, 'show debug log header'),
            'getModules': (0, 'show installed modules'),
            'checkModules': (0, 'Check using lazylibrarian library modules'),
            'createSupportZip': (0, 'Create support.zip. Requires that LOGFILEREDACT is enabled'),
            'clearLogs': (1, 'clear current log'),
            'getMagazines': (0, 'list magazines'),
            'getIssues': (0, '[&name=] [&sort=] [&limit=] list issues of named magazine'),
            'getIssueName': (0, '&name= get name of issue from path/filename'),
            'createMagCovers': (1, '[&wait] [&refresh] create covers for magazines, optionally refresh existing ones'),
            'createMagCover': (1, '&file= [&refresh] [&page=] create cover for magazine issue, optional page number'),
            'forceMagSearch': (1, '[&wait] search for all wanted magazines'),
            'forceBookSearch': (1, '[&wait] [&type=eBook/AudioBook] search for all wanted books'),
            'forceRSSSearch': (1, '[&wait] search all entries in rss feeds'),
            'forceComicSearch': (1, '[&wait] search for all wanted comics'),
            'getRSSFeed': (0, '&feed= [&limit=] show rss feed entries'),
            'forceWishlistSearch': (1, '[&wait] search all entries in wishlists'),
            'forceProcess': (1, '[&dir] [ignorekeepseeding] process books/mags in download or named dir'),
            'pauseAuthor': (1, '&id= pause author by AuthorID'),
            'resumeAuthor': (1, '&id= resume author by AuthorID'),
            'ignoreAuthor': (1, '&id= ignore author by AuthorID'),
            'refreshAuthor': (1, '&name= [&refresh] reload author (and their books) by name, optionally refresh cache'),
            'authorUpdate': (1, 'update the oldest author'),
            'seriesUpdate': (1, 'update the oldest series'),
            'forceActiveAuthorsUpdate': (1, '[&wait] [&refresh] reload all active authors and book data, '
                                            'refresh cache'),
            'forceLibraryScan': (1, '[&wait] [&remove] [&dir=] [&id=] rescan whole or part book library'),
            'forceComicScan': (1, '[&wait] [&id=] rescan whole or part comic library'),
            'forceAudioBookScan': (1, '[&wait] [&remove] [&dir=] [&id=] rescan whole or part audiobook library'),
            'forceMagazineScan': (1, '[&wait] [&title=] rescan whole or part magazine library'),
            'getVersion': (0, 'show lazylibrarian current/git version'),
            'getCurrentVersion': (0, 'show lazylibrarian current version'),
            'shutdown': (1, 'stop lazylibrarian'),
            'restart': (1, 'restart lazylibrarian'),
            'update': (1, 'update lazylibrarian'),
            'findAuthor': (0, '&name= search goodreads/googlebooks for named author'),
            'findAuthorID': (0, '&name= [&source=] find AuthorID for named author'),
            'findMissingAuthorID': (0, '[&source=] find authorid from named source for any authors without id'),
            'findBook': (0, '&name= search goodreads/googlebooks for named book'),
            'addBook': (1, '&id= add book details to the database'),
            'moveBooks': (1, '&fromname= &toname= move all books from one author to another by AuthorName'),
            'moveBook': (1, '&id= &toid= move one book to new author by BookID and AuthorID'),
            'addAuthor': (1, '&name= [&books] add author to database by name, optionally add their books'),
            'addAuthorID': (1, '&id= add author to database by AuthorID, optionally add their books'),
            'removeAuthor': (1, '&id= remove author from database by AuthorID'),
            'addMagazine': (1, '&name= add magazine to database by name'),
            'removeMagazine': (1, '&name= remove magazine and all of its issues from database by name'),
            'queueBook': (1, '&id= [&type=eBook/AudioBook] mark book as Wanted, default eBook'),
            'unqueueBook': (1, '&id= [&type=eBook/AudioBook] mark book as Skipped, default eBook'),
            'readCFG': (1, '&name=&group= read value of config variable "name" in section "group"'),
            'writeCFG': (1, '&name=&group=&value= set config variable "name" in section "group" to value'),
            'loadCFG': (1, 'reload config from file'),
            'getBookCover': (0, '&id= [&src=] fetch cover link from cache/cover/librarything/goodreads/google '
                                'for BookID'),
            'getAllBooks': (0, '[&sort=] [&limit=] list all books in the database'),
            'listNoLang': (0, 'list all books in the database with unknown language'),
            'listNoDesc': (0, 'list all books in the database with no description'),
            'listNoISBN': (0, 'list all books in the database with no isbn'),
            'listNoGenre': (0, 'list all books in the database with no genre'),
            'listNoBooks': (0, 'list all authors in the database with no books'),
            'listDupeBooks': (0, 'list all books in the database with more than one entry'),
            'listDupeBookStatus': (0, 'list all copies of books in the database with more than one entry'),
            'removeNoBooks': (1, 'delete all authors in the database with no books'),
            'listIgnoredAuthors': (0, 'list all authors in the database marked ignored'),
            'listIgnoredBooks': (0, 'list all books in the database marked ignored'),
            'listIgnoredSeries': (0, 'list all series in the database marked ignored'),
            'listMissingWorkpages': (0, 'list all books with errorpage or no workpage'),
            'searchBook': (1, '&id= [&wait] [&type=eBook/AudioBook] search for one book by BookID'),
            'searchItem': (1, '&item= get search results for an item (author, title, isbn)'),
            'showStats': (0, '[&json] show database statistics'),
            'showJobs': (0, '[&json] show status of running jobs'),
            'restartJobs': (1, 'restart background jobs'),
            'showThreads': (0, 'show threaded processes'),
            'checkRunningJobs': (0, 'ensure all needed jobs are running'),
            'vacuum': (1, 'vacuum the database'),
            'getWorkSeries': (0, '&id= &source= Get series from Librarything using BookID or GoodReads using WorkID'),
            'addSeriesMembers': (1, '&id= add series members to database using SeriesID'),
            'getSeriesMembers': (0, '&id= Get list of series members using SeriesID'),
            'getSeriesAuthors': (1, '&id= Get all authors for a series and import them'),
            'getWorkPage': (0, '&id= Get url of Librarything BookWork using BookID'),
            'getBookCovers': (1, '[&wait] Check all books for cached cover and download one if missing'),
            'getBookAuthors': (0, '&id= Get list of authors associated with this book'),
            'cleanCache': (1, '[&wait] Clean unused and expired files from the LazyLibrarian caches'),
            'deleteEmptySeries': (1, 'Delete any book series that have no members'),
            'setNoDesc': (1, '[&refresh] Set descriptions for all books, include "No Description" entries on refresh'),
            'setNoGenre': (1, '[&refresh] Set book genre for all books without one, include "Unknown" '
                              'entries on refresh'),
            'setWork_Pages': (1, '[&wait] Set the WorkPages links in the database'),
            'setAllBookSeries': (1, '[&wait] Set the series details from goodreads or librarything workpages'),
            'setAllBookAuthors': (1, '[&wait] Set all authors for all books from book workpages'),
            'setWorkID': (1, '[&wait] [&bookids] Set WorkID for all books that dont have one, or bookids'),
            'importAlternate': (1, '[&wait] [&dir=] [&library=] Import ebooks/audiobooks from named or '
                                   'alternate folder and any subfolders'),
            'includeAlternate': (1, '[&wait] [&dir=] [&library=] Include links to ebooks/audiobooks from named '
                                    'or  alternate folder and any subfolders'),
            'importCSVwishlist': (1, '[&wait] [&status=Wanted] [&library=eBook] [&dir=] Import a CSV wishlist '
                                     'from named or alternate directory'),
            'exportCSVwishlist': (1, '[&wait] [&status=Wanted] [&library=eBook] [&dir=] Export a CSV wishlist '
                                     'to named or alternate directory'),
            'grSync': (1, '&status= &shelf= [&library=] [&reset] Sync books with given status to a goodreads '
                          'shelf, or reset goodreads shelf to match lazylibrarian'),
            'grFollow': (1, '&id= Follow an author on goodreads'),
            'grFollowAll': (1, 'Follow all lazylibrarian authors on goodreads'),
            'grUnfollow': (1, '&id= Unfollow an author on goodreads'),
            'hcSync': (1, '[&library=] Sync readinglists to hardcover'),
            'writeOPF': (1, '&id= [&refresh] write out an opf file for a bookid, optionally overwrite existing opf'),
            'writeAllOPF': (1, '[&refresh] write out opf files for all books, optionally overwrite existing opf'),
            'renameAudio': (1, '&id Rename an audiobook using configured pattern'),
            'createPlaylist': (1, '&id Create playlist for an audiobook'),
            'nameVars': (0, '&id Show the name variables that would be used for a bookid'),
            'showCaps': (0, '&provider= get a list of capabilities from a provider'),
            'calibreList': (0, '[&toread=] [&read=] get a list of books in calibre library'),
            'syncCalibreList': (0, '[&toread=] [&read=] sync list of read/toread books with calibre'),
            'logMessage': (1, '&level= &text=  send a message to lazylibrarian logger'),
            'comicid': (0, '&name= &source= [&best] try to identify comic from name'),
            'comicmeta': (0, '&name= [&xml] get metadata from comic archive, xml or dictionary'),
            'getBookPubdate': (0, '&id= get original publication date of a book by bookid'),
            'gc_init': (1, 'Initialise gc_before state'),
            'gc_stats': (1, 'Show difference since gc_init'),
            'gc_collect': (1, 'Run garbage collection & return how many items'),
            'listNewAuthors': (0, '[&limit=] List newest authors and show when added and reason for adding'),
            'listNewBooks': (0, '[&limit=] List newest books and show when added and reason for adding'),
            'importBook': (1, '[&library=] &id= &dir= add library [eBook|Audio] bookid from folder'),
            'importMag': (1, '&title= &num= &file= add magazine issue from file'),
            'preprocessAudio': (1, '&dir= &author= &title= [&id=] [&tag] [&merge] preprocess an audiobook folder'),
            'preprocessBook': (1, '&dir= preprocess an ebook folder'),
            'preprocessAllBooks': (1, 'run preprocessor on all existing ebooks'),
            'preprocessMagazine': (1, '&dir= &cover= preprocess a magazine folder'),
            'memUse': (0, 'memory usage of the program in kB'),
            'cpuUse': (0, 'recent cpu usage of the program'),
            'nice': (0, 'show current nice level'),
            'nicer': (1, 'make a little nicer'),
            'subscribe': (1, '&user= &feed= subscribe a user to a feed'),
            'unsubscribe': (1, '&user= &feed= remove a user from a feed'),
            'listAlienAuthors': (0, 'List authors not matching current book api'),
            'listAlienBooks': (0, 'List books not matching current book api'),
            'listNabProviders': (0, 'List all newznab/torznab providers, prowlarr compatible format'),
            'listRSSProviders': (0, 'List all rss/wishlist providers'),
            'listTorrentProviders': (0, 'List all torrent providers'),
            'listIRCProviders': (0, 'List all irc providers'),
            'listDirectProviders': (0, 'List all direct providers'),
            'listProviders': (0, 'List all providers'),
            'changeProvider': (1, '&name= &xxx= Change values for a provider'),
            'addProvider': (1, '&type= &xxx= Add a new provider'),
            'delProvider': (1, '&name= Delete a provider'),
            'renameBook': (1, '&id= Rename a book to match configured pattern'),
            'newAuthorid': (1, '&id= &newid= update an authorid'),
            'telemetryShow': (0, 'show the current telemetry data'),
            'telemetrySend': (1, 'send the latest telemetry data, if configured'),
            'backup': (1, 'Backup database and config files'),
            }


def get_case_insensitive_key_value(input_dict, key):
    return next((value for dict_key, value in input_dict.items() if dict_key.lower() == key.lower()), None)


class Api(object):
    def __init__(self):

        self.apikey = None
        self.cmd = None
        self.id = None
        self.kwargs = None
        self.data = None
        self.callback = None
        self.lower_cmds = [key.lower() for key, _ in cmd_dict.items()]
        self.logger = logging.getLogger(__name__)
        self.loggerdlcomms = logging.getLogger('special.dlcomms')

    def check_params(self, **kwargs):
        TELEMETRY.record_usage_data()

        if not CONFIG.get_bool('API_ENABLED'):
            self.data = {'Success': False, 'Data': '', 'Error': {'Code': 501, 'Message': 'API not enabled'}}
            return
        if not CONFIG.get_str('API_KEY'):
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 501, 'Message': 'No API key'}}
            return
        if len(CONFIG.get_str('API_KEY')) != 32:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 503, 'Message': 'Invalid API key'}}
            return

        if 'apikey' not in kwargs:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Missing parameter: apikey'}}
            return

        if kwargs['apikey'] != CONFIG.get_str('API_KEY') and kwargs['apikey'] != CONFIG.get_str('API_RO_KEY'):
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 401, 'Message': 'Incorrect API key'}}
            return
        else:
            self.apikey = kwargs.pop('apikey')

        if 'cmd' not in kwargs:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 405,
                                                                  'Message': 'Missing parameter: cmd, try cmd=help'}}
            return

        if kwargs['cmd'].lower() not in self.lower_cmds:
            self.data = {'Success': False, 'Data': '',
                         'Error':  {'Code': 405, 'Message': 'Unknown command: %s, try cmd=help' % kwargs['cmd']}}
            return

        if get_case_insensitive_key_value(cmd_dict, kwargs['cmd'])[0] != 0 and self.apikey != CONFIG.get_str('API_KEY'):
            self.data = {'Success': False, 'Data': '',
                         'Error':  {'Code': 405, 'Message': 'Command: %s not available with read-only '
                                                            'api access key, try cmd=help' % kwargs['cmd']}}
            return

        self.cmd = kwargs.pop('cmd')
        self.kwargs = kwargs
        self.data = 'OK'

    @property
    def fetch_data(self):
        TELEMETRY.record_usage_data()
        thread_name("API")
        if self.data == 'OK':
            remote_ip = cherrypy.request.headers.get('X-Forwarded-For')  # apache2
            if not remote_ip:
                remote_ip = cherrypy.request.headers.get('X-Host')  # lighthttpd
            if not remote_ip:
                remote_ip = cherrypy.request.headers.get('Remote-Addr')
            if not remote_ip:
                remote_ip = cherrypy.request.remote.ip
            self.logger.debug('Received API command from %s: %s %s' % (remote_ip, self.cmd, self.kwargs))
            method_to_call = getattr(self, "_" + self.cmd.lower())
            method_to_call(**self.kwargs)

            if 'callback' not in self.kwargs:
                self.loggerdlcomms.debug(str(self.data))
                if isinstance(self.data, str):
                    return self.data
                else:
                    return json.dumps(self.data)
            else:
                self.callback = self.kwargs['callback']
                self.data = json.dumps(self.data)
                self.data = self.callback + '(' + self.data + ');'
                return self.data

        elif isinstance(self.data, str):
            return self.data
        return json.dumps(self.data)

    @staticmethod
    def _dic_from_query(query):

        db = database.DBConnection()
        try:
            rows = db.select(query)
        finally:
            db.close()

        rows_as_dic = []

        for row in rows:
            # noinspection PyTypeChecker
            row_as_dic = dict(list(zip(list(row.keys()), row)))
            for key in ['BookLibrary', 'AudioLibrary', 'BookAdded']:
                if row_as_dic.get(key):
                    try:
                        row_as_dic[key] = dp.parse(row_as_dic[key]).isoformat() + 'Z'
                    except dp._parser.ParserError:
                        pass
            rows_as_dic.append(row_as_dic)

        return rows_as_dic

    def _backup(self):
        TELEMETRY.record_usage_data()
        backup_file, err = dbbackup('api')
        success = backup_file != ''
        self.data = {'Success': success != '', 'Data': backup_file, 'Error':  {'Code': 200, 'Message': err}}

    def _renamebook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Missing parameter: id'}}
        else:
            fname, err = book_rename(kwargs['id'])
            self.data = {'Success': fname != '', 'Data': fname, 'Error':  {'Code': 200, 'Message': err}}
        return

    def _newauthorid(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Missing parameter: id'}}
        elif 'newid' not in kwargs:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Missing parameter: newid'}}
        elif kwargs['id'].startswith('OL') and not kwargs['newid'].startswith('OL'):
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Invalid parameter: newid'}}
        elif not kwargs['id'].startswith('OL') and kwargs['newid'].startswith('OL'):
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Invalid parameter: newid'}}
        else:
            db = database.DBConnection()
            try:
                res = db.match('SELECT * from authors WHERE authorid=?', (kwargs['id'],))
                if not res:
                    self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                          'Message': 'Invalid parameter: id'}}
                else:
                    db.action("PRAGMA foreign_keys = OFF")
                    db.action('UPDATE books SET AuthorID=? WHERE AuthorID=?',
                              (kwargs['newid'], kwargs['id']))
                    db.action('UPDATE seriesauthors SET AuthorID=? WHERE AuthorID=?',
                              (kwargs['newid'], kwargs['id']), suppress='UNIQUE')
                    if kwargs['newid'].startswith('OL'):
                        db.action('UPDATE authors SET AuthorID=?,ol_id=? WHERE AuthorID=?',
                                  (kwargs['newid'], kwargs['newid'], kwargs['id']), suppress='UNIQUE')
                    else:
                        db.action('UPDATE authors SET AuthorID=?,gr_id=? WHERE AuthorID=?',
                                  (kwargs['newid'], kwargs['newid'], kwargs['id']), suppress='UNIQUE')

                    db.action("PRAGMA foreign_keys = ON")
                    self.data = {'Success': True,
                                 'Data': {'AuthorID': kwargs['newid']},
                                 'Error':  {'Code': 200, 'Message': 'OK'}
                                 }
            finally:
                db.close()
        return

    @staticmethod
    def _provider_array(prov_type):
        # convert provider config values to a regular array of dicts with correct types
        array = CONFIG.providers(prov_type)
        providers = []
        for provider in array:
            thisprov = {}
            for key in provider:
                if isinstance(key, ConfigBool):
                    thisprov[key] = provider.get_bool(key)
                elif isinstance(key, ConfigInt):
                    thisprov[key] = provider.get_int(key)
                else:
                    thisprov[key] = provider.get_str(key)
            providers.append(thisprov)
        return providers

    def _listproviders(self):
        TELEMETRY.record_usage_data()
        self._listdirectproviders()
        direct = self.data
        self._listtorrentproviders()
        torrent = self.data
        self.data = {'newznab': self._provider_array('NEWZNAB'),
                     'torznab': self._provider_array('TORZNAB'),
                     'rss': self._provider_array('RSS'),
                     'irc': self._provider_array('IRC'),
                     'torrent': torrent,
                     'direct': direct,
                     }
        tot = 0
        for item in self.data:
            tot += len(item)
        self.logger.debug("Returning %s %s" % (tot, plural(tot, "entry")))

    def _listnabproviders(self):
        TELEMETRY.record_usage_data()
        # custom output format for prowlarr
        oldnewzlist = self._provider_array('NEWZNAB')
        newzlist = []
        for item in oldnewzlist:
            entry = {'Name': item['DISPNAME'], 'Dispname': item['DISPNAME'], 'Host': item['HOST'],
                     'Apikey': item['API'], 'Enabled': 1 if bool(item['ENABLED']) else 0, 'Categories': ''}
            # merge prowlarr categories
            for key in ['BOOKCAT', 'MAGCAT', 'AUDIOCAT', 'COMICCAT']:
                if item[key]:
                    if entry['Categories']:
                        entry['Categories'] += ','
                    entry['Categories'] += item[key]
            newzlist.append(entry)

        oldtorzlist = self._provider_array('TORZNAB')
        torzlist = []
        for item in oldtorzlist:
            entry = {'Name': item['DISPNAME'], 'Dispname': item['DISPNAME'], 'Host': item['HOST'],
                     'Apikey': item['API'], 'Enabled': 1 if bool(item['ENABLED']) else 0, 'Categories': ''}
            for key in ['BOOKCAT', 'MAGCAT', 'AUDIOCAT', 'COMICCAT']:
                if item[key]:
                    if entry['Categories']:
                        entry['Categories'] += ','
                    entry['Categories'] += item[key]
            torzlist.append(entry)

        tot = len(newzlist) + len(torzlist)
        self.logger.debug("Returning %s %s" % (tot, plural(tot, "entry")))
        self.data = {'Success': True,
                     'Data': {
                        'Newznabs': newzlist,
                        'Torznabs': torzlist,
                     },
                     'Error':  {'Code': 200, 'Message': 'OK'}
                     }

    def _listrssproviders(self):
        TELEMETRY.record_usage_data()
        providers = self._provider_array('RSS')
        tot = len(providers)
        self.logger.debug("Returning %s %s" % (tot, plural(tot, "entry")))
        self.data = providers

    def _listircproviders(self):
        TELEMETRY.record_usage_data()
        providers = self._provider_array('IRC')
        tot = len(providers)
        self.logger.debug("Returning %s %s" % (tot, plural(tot, "entry")))
        self.data = providers

    def _listtorrentproviders(self):
        TELEMETRY.record_usage_data()
        providers = []
        for provider in ['KAT', 'TPB', 'LIME', 'TDL']:
            mydict = {'NAME': provider, 'ENABLED': CONFIG.get_bool(provider)}
            for item in ['HOST', 'DLTYPES']:
                name = "%s_%s" % (provider, item)
                mydict[name] = CONFIG.get_str(name)
            for item in ['DLPRIORITY', 'SEEDERS']:
                name = "%s_%s" % (provider, item)
                mydict[name] = CONFIG.get_int(name)
            providers.append(mydict)
        self.logger.debug("Returning %s %s" % (len(providers), plural(len(providers), "entry")))
        self.data = providers

    def _listdirectproviders(self):
        TELEMETRY.record_usage_data()
        providers = self._provider_array('GEN')
        mydict = {'NAME': 'BOK', 'ENABLED': CONFIG.get_bool('BOK')}
        for item in ['HOST', 'LOGIN', 'USER', 'PASS', 'DLTYPES']:
            name = "%s_%s" % ('BOK', item)
            mydict[name] = CONFIG.get_str(name)
        for item in ['DLPRIORITY', 'DLLIMIT']:
            name = "%s_%s" % ('BOK', item)
            mydict[name] = CONFIG.get_int(name)
        providers.append(mydict)
        mydict = {'NAME': 'BFI', 'ENABLED': CONFIG.get_bool('BFI')}
        for item in ['HOST', 'DLTYPES']:
            name = "%s_%s" % ('BFI', item)
            mydict[name] = CONFIG.get_str(name)
        for item in ['DLPRIORITY']:
            name = "%s_%s" % ('BFI', item)
            mydict[name] = CONFIG.get_int(name)
        providers.append(mydict)
        tot = len(providers)
        self.logger.debug("Returning %s %s" % (tot, plural(tot, "entry")))
        self.data = providers

    def _delprovider(self, **kwargs):
        TELEMETRY.record_usage_data()
        if not kwargs.get('name', '') and not kwargs.get('NAME', ''):
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Missing parameter: name'}}
            return

        name = kwargs.get('name', '')
        if not name:
            name = kwargs.get('NAME', '')

        if name.startswith('Newznab') or kwargs.get('providertype', '') == 'newznab':
            providers = CONFIG.providers('NEWZNAB')
            section = 'newznab'
            clear = 'HOST'
        elif name.startswith('Torznab') or kwargs.get('providertype', '') == 'torznab':
            providers = CONFIG.providers('TORZNAB')
            section = 'torznab'
            clear = 'HOST'
        elif name.startswith('RSS_'):
            providers = CONFIG.providers('RSS')
            section = 'rss_'
            clear = 'HOST'
        elif name.startswith('GEN_'):
            providers = CONFIG.providers('GEN')
            section = 'GEN_'
            clear = 'HOST'
        elif name.startswith('IRC_'):
            providers = CONFIG.providers('IRC')
            section = 'IRC_'
            clear = 'SERVER'
        else:
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Invalid parameter: name'}}
            return

        for item in providers:
            if item['NAME'] == name or (kwargs.get('providertype', '') and item['DISPNAME'] == name):
                item[clear] = ''
                CONFIG.save_config_and_backup_old(section=section)
                self.data = {'Success': True, 'Data': 'Deleted %s' % name,
                             'Error':  {'Code': 200, 'Message': 'OK'}}
                return
        self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 404,
                                                              'Message': 'Provider %s not found' % name}}
        return

    def _changeprovider(self, **kwargs):
        TELEMETRY.record_usage_data()
        if not kwargs.get('name', '') and not kwargs.get('NAME', ''):
            self.data = {'Success': False, 'Data': '', 'Error':  {'Code': 400,
                                                                  'Message': 'Missing parameter: name'}}
            return

        hit = []
        miss = []
        name = kwargs.get('NAME', '')
        if not name:
            name = kwargs.get('name', '')

        # prowlarr gives us  providertype
        if name.startswith('Newznab') or kwargs.get('providertype', '') == 'newznab':
            providers = CONFIG.providers('NEWZNAB')
        elif name.startswith('Torznab') or kwargs.get('providertype', '') == 'torznab':
            providers = CONFIG.providers('TORZNAB')
        elif name.startswith('RSS_'):
            providers = CONFIG.providers('RSS')
        elif name.startswith('IRC_'):
            providers = CONFIG.providers('IRC')
        elif name.startswith('GEN_'):
            providers = CONFIG.providers('GEN')
        elif name in ['BOK', 'BFI', 'KAT', 'TPB', 'LIME', 'TDL']:
            for arg in kwargs:
                if arg in ['HOST', 'DLTYPES', 'DLPRIORITY', 'DLLIMIT', 'SEEDERS']:
                    itemname = "%s_%s" % (name, arg)
                    if itemname in CONFIG:
                        if arg in ['DLPRIORITY', 'DLLIMIT', 'SEEDERS']:
                            CONFIG.set_int(itemname, kwargs[arg])
                        else:
                            CONFIG.set_str(itemname, kwargs[arg])
                        hit += arg
                elif arg == 'ENABLED':
                    hit.append(arg)
                    if kwargs[arg] in ['1', 1, True, 'True', 'true']:
                        val = True
                    else:
                        val = False
                    CONFIG.set_bool(name, val)
                else:
                    miss.append(arg)
            CONFIG.save_config_and_backup_old(section=name)
            self.data = {'Success': True, 'Data': 'Changed %s [%s]' % (name, ','.join(hit)),
                         'Error':  {'Code': 200, 'Message': 'OK'}}
            if miss:
                self.data['Data'] += " Invalid parameters [%s]" % ','.join(miss)
            return
        else:
            self.data = {'Success': False, 'Data': '',
                         'Error':  {'Code': 400, 'Message': 'Invalid parameter: name'}}
            return

        for item in providers:
            if item['NAME'] == name or (kwargs.get('providertype', '') and item['DISPNAME'] == name):
                for arg in kwargs:
                    if arg.upper() == 'NAME':
                        # don't allow api to change our internal name
                        continue
                    elif arg == 'altername':  # prowlarr
                        hit.append(arg)
                        item['DISPNAME'] = kwargs[arg]
                    elif arg.upper() in ['ENABLED', 'MANUAL']:
                        hit.append(arg)
                        if kwargs[arg] in ['1', 1, True, 'True', 'true']:
                            val = True
                        else:
                            val = False
                        item.set_bool(arg.upper(), val)
                    elif arg.upper() in item:
                        hit.append(arg)
                        if arg.upper() in ['EXTENDED', 'APICOUNT', 'APILIMIT', 'RATELIMIT', 'DLPRIORITY', 'LASTUSED',
                                           'SEEDERS', 'SEED_DURATION']:
                            item.set_int(arg.upper(), kwargs[arg])
                        elif arg.upper() in ['SEED_RATIO']:
                            item.set_float(arg.upper(), kwargs[arg])
                        else:
                            item.set_str(arg.upper(), kwargs[arg])
                    elif arg == 'prov_apikey':  # prowlarr
                        hit.append(arg)
                        item.set_str('API', kwargs[arg])
                    elif arg == 'categories' and 'BOOKCAT' in providers[0]:
                        hit.append(arg)
                        # prowlarr only gives us one category list
                        catlist = get_list(kwargs[arg])
                        bookcat = ''
                        audiocat = ''
                        for catnum in catlist:
                            if catnum.startswith('3'):
                                if audiocat:
                                    audiocat += ','
                                audiocat += catnum
                            elif catnum.startswith('7'):
                                if bookcat:
                                    bookcat += ','
                                bookcat += catnum
                        item.set_str('BOOKCAT', bookcat)
                        item.set_str('AUDIOCAT', audiocat)
                    else:
                        miss.append(arg)

                get_capabilities(item, True)

                CONFIG.save_config_and_backup_old(section=item['NAME'])
                self.data = {'Success': True, 'Data': 'Changed %s [%s]' % (item['NAME'], ','.join(hit)),
                             'Error':  {'Code': 200, 'Message': 'OK'}}
                if miss:
                    self.data['Data'] += " Invalid parameters [%s]" % ','.join(miss)
                return
        self.data = {'Success': False, 'Data': '',
                     'Error':  {'Code': 404, 'Message': 'Provider %s not found' % name}}
        return

    def _addprovider(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'type' not in kwargs and 'providertype' not in kwargs:
            self.data = {'Success': False, 'Data': '',
                         'Error':  {'Code': 400, 'Message': 'Missing parameter: type'}}
            return
        if 'HOST' not in kwargs and 'SERVER' not in kwargs and 'host' not in kwargs:
            self.data = {'Success': False, 'Data': '',
                         'Error':  {'Code': 400, 'Message': 'Missing parameter: HOST or SERVER'}}
            return
        if kwargs.get('type', '') == 'newznab' or kwargs.get('providertype', '') == 'newznab':
            providers = CONFIG.providers('NEWZNAB')
            provname = 'Newznab'
            section = 'Newznab'
        elif kwargs.get('type', '') == 'torznab' or kwargs.get('providertype', '') == 'torznab':
            providers = CONFIG.providers('TORZNAB')
            provname = 'Torznab'
            section = 'Torznab'
        elif kwargs['type'] == 'rss':
            providers = CONFIG.providers('RSS')
            provname = 'RSS_'
            section = 'rss_'
        elif kwargs['type'] == 'gen':
            providers = CONFIG.providers('GEN')
            provname = 'GEN_'
            section = 'GEN_'
        elif kwargs['type'] == 'irc':
            providers = CONFIG.providers('IRC')
            provname = 'IRC_'
            section = 'IRC_'
        else:
            self.data = {'Success': False,
                         'Data': '',
                         'Error':  {'Code': 400,
                                    'Message': 'Invalid parameter: type. Should be newznab,torznab,rss,gen,irc'}
                         }
            return

        num = len(providers)
        empty_slot = providers[len(providers) - 1]

        hit = []
        miss = []
        provname = "%s_%s" % (provname, num - 1)
        empty_slot['DISPNAME'] = provname
        for arg in kwargs:
            if arg == 'prov_apikey':
                hit.append(arg)
                empty_slot['API'] = kwargs[arg]
            elif arg == 'enabled':
                hit.append(arg)
                empty_slot['ENABLED'] = kwargs[arg] == 'true'
            elif arg in ['altername', 'name']:
                for existing in providers:
                    if kwargs[arg] and existing['DISPNAME'] == kwargs[arg]:
                        self.data = {'Success': False,
                                     'Data': '',
                                     'Error':  {'Code': 409,
                                                'Message': '%s Already Exists' % kwargs[arg]}
                                     }
                        return
                hit.append(arg)
                empty_slot['DISPNAME'] = kwargs[arg]
            elif arg == 'categories' and 'BOOKCAT' in providers[0]:
                hit.append(arg)
                # prowlarr only gives us one category list
                catlist = get_list(kwargs[arg])
                bookcat = ''
                audiocat = ''
                for item in catlist:
                    if item.startswith('3'):
                        if audiocat:
                            audiocat += ','
                        audiocat += item
                    else:
                        if bookcat:
                            bookcat += ','
                        bookcat += item
                empty_slot['BOOKCAT'] = bookcat
                empty_slot['AUDIOCAT'] = audiocat
            elif arg in ['providertype', 'type']:
                hit.append(arg)
            elif arg.upper() in providers[0]:
                hit.append(arg)
                empty_slot[arg.upper()] = kwargs[arg]
                if arg.upper() in ['EXTENDED', 'APICOUNT', 'APILIMIT', 'RATELIMIT', 'DLPRIORITY', 'LASTUSED',
                                   'SEEDERS', 'SEED_DURATION']:
                    empty_slot.set_int(arg.upper(), kwargs[arg])
                elif arg.upper() in ['SEED_RATIO']:
                    empty_slot.set_float(arg.upper(), kwargs[arg])
                else:
                    empty_slot.set_str(arg.upper(), kwargs[arg])
            else:
                miss.append(arg)

        get_capabilities(empty_slot, True)

        CONFIG.save_config_and_backup_old(section=section)
        self.data = {'Success': True, 'Data': 'Added %s [%s]' % (section, ','.join(hit)),
                     'Error':  {'Code': 200, 'Message': 'OK'}}
        if miss:
            self.data['Data'] += " Invalid parameters [%s]" % ','.join(miss)
        return

    def _memuse(self):
        TELEMETRY.record_usage_data()
        """ Current Memory usage in kB """
        if os.name == 'nt':
            ok, self.data = get_process_memory()
            if not ok:
                self.data = {'Success': False, 'Data': '', 'Error': {'Code': 501,
                                                                     'Message': 'Needs psutil module installed'}}
        else:
            with open('/proc/self/status') as f:
                memusage = f.read().split('VmRSS:')[1].split('\n')[0][:-3]
            self.data = memusage.strip()

    def _cpuuse(self):
        TELEMETRY.record_usage_data()
        ok, self.data = get_cpu_use()
        if not ok:
            self.data = {'Success': False, 'Data': '', 'Error': {'Code': 501,
                                                                 'Message': 'Needs psutil module installed'}}

    def _nice(self):
        TELEMETRY.record_usage_data()
        if os.name == 'nt':
            self.data = {'Success': False, 'Data': '', 'Error': {'Code': 501, 'Message': 'Unsupported in Windows'}}
        else:
            self.data = os.nice(0)

    def _nicer(self):
        TELEMETRY.record_usage_data()
        if os.name == 'nt':
            self.data = {'Success': False, 'Data': '', 'Error': {'Code': 501, 'Message': 'Unsupported in Windows'}}
        else:
            self.data = os.nice(1)

    @staticmethod
    def _gc_init():
        TELEMETRY.record_usage_data()
        from collections import defaultdict
        from gc import get_objects
        lazylibrarian.GC_BEFORE = defaultdict(int)
        for i in get_objects():
            lazylibrarian.GC_BEFORE[type(i)] += 1

    def _gc_collect(self):
        TELEMETRY.record_usage_data()
        from gc import collect
        self.data = collect()

    def _gc_stats(self):
        TELEMETRY.record_usage_data()
        if not lazylibrarian.GC_BEFORE:
            self.data = 'Not initialised'
            return
        from collections import defaultdict
        from gc import get_objects
        lazylibrarian.GC_AFTER = defaultdict(int)
        for i in get_objects():
            lazylibrarian.GC_AFTER[type(i)] += 1

        res = ''
        for k in lazylibrarian.GC_AFTER.keys():
            if k in lazylibrarian.GC_BEFORE:
                n = int(lazylibrarian.GC_AFTER[k] - lazylibrarian.GC_BEFORE[k])
            else:
                n = int(lazylibrarian.GC_AFTER[k])
            if n:
                changed = "%s %s<br>" % (n, str(k).split("'")[1])
                res += changed
        self.data = res

    def _getrssfeed(self, **kwargs):
        TELEMETRY.record_usage_data()
        ftype = kwargs.get('feed', 'eBook')
        limit = kwargs.get('limit', 10)
        authorid = kwargs.get('authorid', '')

        # url might end in .xml
        if not str(limit).isdigit():
            try:
                limit = int(str(limit).split('.')[0])
            except (IndexError, ValueError):
                limit = 10

        userid = 0
        scheme, netloc, path, qs, anchor = urlsplit(cherrypy.url())
        netloc = cherrypy.request.headers.get('X-Forwarded-Host')
        if not netloc:
            netloc = cherrypy.request.headers.get('Host')
        path = path.replace('rss_feed', '').rstrip('/')
        baseurl = urlunsplit((scheme, netloc, path, qs, anchor))
        self.data = gen_feed(ftype, limit=limit, user=userid, baseurl=baseurl, authorid=authorid)

    def _synccalibrelist(self, **kwargs):
        TELEMETRY.record_usage_data()
        col1 = kwargs.get('read')
        col2 = kwargs.get('toread')
        self.data = sync_calibre_list(col1, col2)

    def _subscribe(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['user', 'feed']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        db = database.DBConnection()
        try:
            res = db.match('SELECT UserID from users WHERE userid=?', (kwargs['user'],))
            if not res:
                self.data = 'Invalid userid'
                return
            for provider in CONFIG.providers('RSS'):
                if provider['DISPNAME'] == kwargs['feed']:
                    if wishlist_type(provider['HOST']):
                        db.action('INSERT into subscribers (UserID , Type, WantID ) VALUES (?, ?, ?)',
                                  (kwargs['user'], 'feed', kwargs['feed']))
                        self.data = 'OK'
                        return
        finally:
            db.close()
        self.data = 'Invalid feed'
        return

    def _unsubscribe(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['user', 'feed']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        db = database.DBConnection()
        try:
            db.action('DELETE FROM subscribers WHERE UserID=? and Type=? and WantID=?',
                      (kwargs['user'], 'feed', kwargs['feed']))
        finally:
            db.close()
        self.data = 'OK'
        return

    def _calibrelist(self, **kwargs):
        TELEMETRY.record_usage_data()
        col1 = kwargs.get('read')
        col2 = kwargs.get('toread')
        self.data = calibre_list(col1, col2)

    def _showcaps(self, **kwargs):
        TELEMETRY.record_usage_data()
        prov = kwargs.get('provider')
        if not prov:
            self.data = 'Missing parameter: provider'
            return
        match = False
        for provider in CONFIG.providers('NEWZNAB'):
            if prov == provider['HOST']:
                prov = provider
                match = True
                break
        if not match:
            for provider in CONFIG.providers('TORZNAB'):
                if prov == provider['HOST']:
                    prov = provider
                    match = True
                    break
        if not match:
            self.data = 'Invalid parameter: provider'
            return
        self.data = get_capabilities(prov, True)

    def _help(self):
        TELEMETRY.record_usage_data()
        res = '<html>' \
              '<p>Sample use: http://localhost:5299/api?apikey=VALIDKEYHERE?cmd=COMMAND</p>' \
              '<p>Valid commands:</p>' \
              '<p/>\n\n' \
              '<ul>\n'
        for key in sorted(cmd_dict):
            # list all commands if full access api_key, or only the read-only commands
            if self.apikey == CONFIG.get_str('API_KEY') or cmd_dict[key][0] == 0:
                res += f"<li>{key}: {cmd_dict[key][1]}</li>\n"
            res += '</ul></html>'
        self.data = res

    def _listalienauthors(self):
        TELEMETRY.record_usage_data()
        cmd = "SELECT AuthorID,AuthorName from authors WHERE AuthorID "
        if CONFIG.get_str('BOOK_API') != 'OpenLibrary':
            cmd += "NOT "
        cmd += "LIKE 'OL%A'"
        self.data = self._dic_from_query(cmd)

    def _listalienbooks(self):
        TELEMETRY.record_usage_data()
        cmd = "SELECT BookID,BookName from books WHERE BookID "
        if CONFIG.get_str('BOOK_API') != 'OpenLibrary':
            cmd += "NOT "
        cmd += "LIKE 'OL%W'"
        self.data = self._dic_from_query(cmd)

    def _gethistory(self):
        TELEMETRY.record_usage_data()
        self.data = self._dic_from_query(
            "SELECT * from wanted WHERE Status != 'Skipped' and Status != 'Ignored'")

    def _listnewauthors(self, **kwargs):
        TELEMETRY.record_usage_data()
        limit = kwargs.get('limit', '')
        if limit:
            limit = "limit " + limit
        self.data = self._dic_from_query(
            "SELECT authorid,authorname,dateadded,reason,status from authors order by dateadded desc %s" % limit)

    def _listnewbooks(self, **kwargs):
        TELEMETRY.record_usage_data()
        limit = kwargs.get('limit', '')
        if limit:
            limit = "limit " + limit
        self.data = self._dic_from_query(
            "SELECT bookid,bookname,bookadded,scanresult,status from books order by bookadded desc %s" % limit)

    def _showthreads(self):
        TELEMETRY.record_usage_data()
        self.data = [n.name for n in [t for t in threading.enumerate()]]

    def _showmonths(self):
        TELEMETRY.record_usage_data()
        self.data = lazylibrarian.MONTHNAMES

    def _renameaudio(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            self.data = audio_rename(kwargs['id'], rename=True)

    def _getbookpubdate(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_book_pubdate(kwargs['id'])

    def _createplaylist(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            self.data = audio_rename(kwargs['id'], playlist=True)

    def _preprocessaudio(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['dir', 'title', 'author']:
            if item not in kwargs:
                self.data = 'Missing parameter: %s' % item
                return
        tag = True if 'tag' in kwargs else None
        merge = True if 'merge' in kwargs else None
        bookid = kwargs['id'] if 'id' in kwargs else 0
        preprocess_audio(kwargs['dir'], bookid, kwargs['author'], kwargs['title'], merge=merge, tag=tag)
        self.data = 'OK'

    def _preprocessbook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'dir' not in kwargs:
            self.data = 'Missing parameter: dir'
            return
        preprocess_ebook(kwargs['dir'])
        self.data = 'OK'

    def _preprocessallbooks(self):
        TELEMETRY.record_usage_data()
        q = "SELECT BookFile from books where BookFile != '' and BookFile is not null"
        res = self._dic_from_query(q)
        cnt = 0
        for item in res:
            folder = os.path.dirname(item['BookFile'])
            cnt += 1
            if os.path.isdir(folder):
                self.logger.debug(f"Preprocessing {cnt} of {len(res)}")
                preprocess_ebook(folder)
        self.data = 'OK'

    def _preprocessmagazine(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['dir', 'cover']:
            if item not in kwargs:
                self.data = 'Missing parameter: %s' % item
                return
        preprocess_magazine(kwargs['dir'], check_int(kwargs['cover'], 0))
        self.data = 'OK'

    def _importbook(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['id', 'dir']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        library = kwargs.get('library', 'eBook')
        self.data = process_book_from_dir(kwargs['dir'], library, kwargs['id'])

    def _importmag(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['title', 'num', 'file']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        self.data = process_mag_from_file(kwargs['file'], kwargs['title'], kwargs['num'])

    def _namevars(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            self.data = name_vars(kwargs['id'])

    def _savetable(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'table' not in kwargs:
            self.data = 'Missing parameter: table'
            return
        valid = ['users', 'magazines']
        if kwargs['table'] not in valid:
            self.data = 'Invalid table. Only %s' % str(valid)
            return
        self.data = "Saved %s" % dump_table(kwargs['table'], DIRS.DATADIR)

    def _writeallopf(self, **kwargs):
        TELEMETRY.record_usage_data()
        db = database.DBConnection()
        try:
            books = db.select('select BookID from books where BookFile is not null')
        finally:
            db.close()
        counter = 0
        if books:
            for book in books:
                bookid = book['BookID']
                if 'refresh' in kwargs:
                    self._writeopf(id=bookid, refresh=True)
                else:
                    self._writeopf(id=bookid)
                try:
                    if self.data[1] is True:
                        counter += 1
                except IndexError:
                    continue
        self.data = 'Updated opf for %s %s' % (counter, plural(counter, "book"))

    def _writeopf(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
            return

        db = database.DBConnection()
        try:
            cmd = ("SELECT AuthorName,BookID,BookName,BookDesc,BookIsbn,BookImg,BookDate,BookLang,BookPub,BookFile,"
                   "BookRate from books,authors WHERE BookID=? and books.AuthorID = authors.AuthorID")
            res = db.match(cmd, (kwargs['id'],))
        finally:
            db.close()
        if not res:
            self.data = 'No data found for bookid %s' % kwargs['id']
            return
        if not res['BookFile'] or not path_isfile(res['BookFile']):
            self.data = 'No bookfile found for bookid %s' % kwargs['id']
            return
        dest_path = os.path.dirname(res['BookFile'])
        global_name = os.path.splitext(os.path.basename(res['BookFile']))[0]
        refresh = 'refresh' in kwargs
        process_img(dest_path, kwargs['id'], res['BookImg'], global_name, refresh)
        self.data = create_opf(dest_path, res, global_name, refresh)

    @staticmethod
    def _dumpmonths():
        TELEMETRY.record_usage_data()
        json_file = os.path.join(DIRS.DATADIR, 'monthnames.json')
        with open(syspath(json_file), 'w') as f:
            json.dump(lazylibrarian.MONTHNAMES, f)

    def _getwanted(self):
        TELEMETRY.record_usage_data()
        self.data = self._dic_from_query(
            "SELECT * from books WHERE Status='Wanted'")

    def _getread(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_readinglist("HaveRead", self.id)

    def _gettoread(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_readinglist("ToRead", self.id)

    def _getreading(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_readinglist("Reading", self.id)

    def _getabandoned(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_readinglist("Abandoned", self.id)

    def _vacuum(self):
        TELEMETRY.record_usage_data()
        msg1 = self._dic_from_query("vacuum")
        msg2 = self._dic_from_query("pragma integrity_check")
        self.data = str(msg1) + str(msg2)

    def _getsnatched(self):
        TELEMETRY.record_usage_data()
        cmd = ("SELECT * from books,wanted WHERE books.bookid=wanted.bookid and books.Status='Snatched' "
               "or AudioStatus='Snatched'")
        self.data = self._dic_from_query(cmd)

    def _logmessage(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['level', 'text']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        self.data = kwargs['text']
        if kwargs['level'].upper() == 'INFO':
            self.logger.info(self.data)
        elif kwargs['level'].upper() == 'WARN':
            self.logger.warning(self.data)
        elif kwargs['level'].upper() == 'ERROR':
            self.logger.error(self.data)
        elif kwargs['level'].upper() == 'DEBUG':
            self.logger.debug(self.data)
        else:
            self.data = 'Invalid level: %s' % kwargs['level']
        return

    def _getdebug(self):
        TELEMETRY.record_usage_data()
        self.data = log_header().replace('\n', '<br>')

    def _getmodules(self):
        TELEMETRY.record_usage_data()
        lst = ''
        for item in sys.modules:
            lst += "%s: %s<br>" % (item, str(sys.modules[item]).replace('<', '').replace('>', ''))
        self.data = lst

    def _checkmodules(self):
        TELEMETRY.record_usage_data()
        lst = []
        for item in sys.modules:
            data = str(sys.modules[item]).replace('<', '').replace('>', '')
            for libname in ['apscheduler', 'mobi', 'oauth2', 'pynma', 'pythontwitter', 'unrar']:
                if libname in data and 'dist-packages' in data:
                    lst.append("%s: %s" % (item, data))
        self.data = lst

    def _clearlogs(self):
        TELEMETRY.record_usage_data()
        LOGCONFIG.clear_ui_log()
        self.data = LOGCONFIG.delete_log_files(CONFIG['LOGDIR'])

    def _createsupportzip(self):
        TELEMETRY.record_usage_data()
        msg, filename = create_support_zip()
        self.data = f'{msg}\nFile created is {filename}'

    def _getindex(self):
        TELEMETRY.record_usage_data()
        self.data = self._dic_from_query(
            'SELECT * from authors order by AuthorName COLLATE NOCASE')

    def _listnolang(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT BookID,BookISBN,BookName,AuthorName from books,authors where '
        q += '(BookLang="Unknown" or BookLang="" or BookLang is NULL) and books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _listnogenre(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT BookID,BookName,AuthorName from books,authors where books.Status != "Ignored" and '
        q += '(BookGenre="Unknown" or BookGenre="" or BookGenre is NULL) and books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _listnodesc(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT BookID,BookName,AuthorName from books,authors where books.Status != "Ignored" and '
        q += '(BookDesc="" or BookDesc is NULL) and books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _setnodesc(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'refresh' in kwargs:
            expire = True
            extra = ' or BookDesc="No Description"'
        else:
            expire = False
            extra = ''
        q = 'SELECT BookID,BookName,AuthorName,BookISBN from books,authors where books.Status != "Ignored" and '
        q += '(BookDesc="" or BookDesc is NULL' + extra + ') and books.AuthorID = authors.AuthorID'
        db = database.DBConnection()
        try:
            res = db.select(q)
            descs = 0
            cnt = 0
            self.logger.debug("Checking description for %s %s" % (len(res), plural(len(res), "book")))
            # ignore all errors except blocked (not found etc)
            blocked = False
            for item in res:
                cnt += 1
                isbn = item['BookISBN']
                auth = item['AuthorName']
                book = item['BookName']
                data = get_gb_info(isbn, auth, book, expire=expire)
                if data and data['desc']:
                    descs += 1
                    self.logger.debug("Updated description for %s:%s" % (auth, book))
                    db.action('UPDATE books SET bookdesc=? WHERE bookid=?', (data['desc'], item['BookID']))
                elif data is None:  # error, see if it's because we are blocked
                    if BLOCKHANDLER.is_blocked('googleapis'):
                        blocked = True
                    if blocked:
                        break
        finally:
            db.close()
        msg = "Scanned %s %s, found %s new %s from %s" % \
              (cnt, plural(cnt, "book"), descs, plural(descs, "description"), len(res))
        if blocked:
            msg += ': Access Blocked'
        self.data = msg
        self.logger.info(self.data)

    def _setnogenre(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'refresh' in kwargs:
            expire = True
            extra = ' or BookGenre="Unknown"'
        else:
            expire = False
            extra = ''
        q = 'SELECT BookID,BookName,AuthorName,BookISBN from books,authors where books.Status != "Ignored" and '
        q += '(BookGenre="" or BookGenre is NULL' + extra + ') and books.AuthorID = authors.AuthorID'
        db = database.DBConnection()
        try:
            res = db.select(q)
        finally:
            db.close()
        genre = 0
        cnt = 0
        self.logger.debug("Checking genre for %s %s" % (len(res), plural(len(res), "book")))
        # ignore all errors except blocked (not found etc)
        blocked = False
        for item in res:
            cnt += 1
            isbn = item['BookISBN']
            auth = item['AuthorName']
            book = item['BookName']
            data = get_gb_info(isbn, auth, book, expire=expire)
            if data and data['genre']:
                genre += 1
                newgenre = genre_filter(data['genre'])
                self.logger.debug("Updated genre for %s:%s [%s]" % (auth, book, newgenre))
                set_genres([newgenre], item['BookID'])
            elif data is None:
                if BLOCKHANDLER.is_blocked('googleapis'):
                    blocked = True
                if blocked:
                    break
        msg = "Scanned %s %s, found %s new %s from %s" % (cnt, plural(cnt, "book"), genre,
                                                          plural(genre, "genre"), len(res))
        if blocked:
            msg += ': Access Blocked'
        self.data = msg
        self.logger.info(self.data)

    def _listnoisbn(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT BookID,BookName,AuthorName from books,authors where books.AuthorID = authors.AuthorID'
        q += ' and (BookISBN="" or BookISBN is NULL)'
        self.data = self._dic_from_query(q)

    def _listnobooks(self):
        TELEMETRY.record_usage_data()
        q = 'select authorid,authorname,reason from authors where haveebooks+haveaudiobooks=0 and '
        q += "instr(reason, 'Series') = 0 except select authors.authorid,authorname,reason from books,authors where "
        q += 'books.authorid=authors.authorid and books.status=="Wanted";'
        self.data = self._dic_from_query(q)

    def _removenobooks(self):
        TELEMETRY.record_usage_data()
        self._listnobooks()
        if self.data:
            db = database.DBConnection()
            try:
                for auth in self.data:
                    self.logger.debug("Deleting %s" % auth['AuthorName'])
                    db.action("DELETE from authors WHERE authorID=?", (auth['AuthorID'],))
            finally:
                db.close()

    def _listignoredseries(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT SeriesID,SeriesName from series where Status="Ignored"'
        self.data = self._dic_from_query(q)

    def _listdupebooks(self):
        TELEMETRY.record_usage_data()
        self.data = []
        q = "select authorid,authorname from authors"
        res = self._dic_from_query(q)
        for author in res:
            q = "select count('bookname'),authorid,bookname from books where "
            q += "AuthorID=%s " % (author['AuthorID'])
            q += "and ( Status != 'Ignored' or AudioStatus != 'Ignored' ) "
            q += "group by bookname having ( count(bookname) > 1 )"
            r = self._dic_from_query(q)
            self.data += r

    def _listdupebookstatus(self):
        TELEMETRY.record_usage_data()
        self._listdupebooks()
        res = self.data
        self.data = []
        for item in res:
            q = 'select BookID,BookName,AuthorName,books.Status,AudioStatus from books,authors where '
            q += 'books.authorid=authors.authorid and books.authorid=%s ' % item['AuthorID']
            q += 'and BookName="%s" ' % item['BookName']
            q += "and ( books.Status != 'Ignored' or AudioStatus != 'Ignored' )"
            r = self._dic_from_query(q)
            self.data += r

    def _listignoredbooks(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT BookID,BookName from books where Status="Ignored"'
        self.data = self._dic_from_query(q)

    def _listignoredauthors(self):
        TELEMETRY.record_usage_data()
        q = 'SELECT AuthorID,AuthorName from authors where Status="Ignored"'
        self.data = self._dic_from_query(q)

    def _listmissingworkpages(self):
        TELEMETRY.record_usage_data()
        # first the ones with no workpage
        q = 'SELECT BookID from books where length(WorkPage) < 4'
        res = self._dic_from_query(q)
        # now the ones with an error page
        cache = os.path.join(DIRS.CACHEDIR, "WorkCache")
        if path_isdir(cache):
            for cached_file in listdir(cache):
                target = os.path.join(cache, cached_file)
                if path_isfile(target):
                    if os.path.getsize(syspath(target)) < 500 and '.' in cached_file:
                        bookid = cached_file.split('.')[0]
                        res.append({"BookID": bookid})
        self.data = res

    def _getauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
            return
        author = self._dic_from_query(
            'SELECT * from authors WHERE AuthorID="' + self.id + '"')
        books = self._dic_from_query(
            'SELECT * from books WHERE AuthorID="' + self.id + '"')

        self.data = {'author': author, 'books': books}

    def _getmagazines(self):
        TELEMETRY.record_usage_data()
        self.data = self._dic_from_query('SELECT * from magazines order by Title COLLATE NOCASE')

    def _getallbooks(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.limit = kwargs.get('limit')
        self.sort = kwargs.get('sort')
        q = '''SELECT authors.AuthorID,AuthorName,AuthorLink,BookName,BookSub,BookGenre,BookIsbn,BookPub,
                BookRate,BookImg,BookPages,BookLink,BookID,BookDate,BookLang,BookAdded,books.Status,
                audiostatus,booklibrary,audiolibrary from books,authors where books.AuthorID = authors.AuthorID'''

        if self.sort:
            q += f' order by {self.sort}'
        if self.limit and self.limit.isnumeric():
            q += f' limit {self.limit}'
        self.data = self._dic_from_query(q)

    def _getissues(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('name')
        self.limit = kwargs.get('limit')
        self.sort = kwargs.get('sort')
        if self.id:
            magazine = self._dic_from_query(
                'SELECT * from magazines WHERE Title="' + self.id + '"')
            q = 'SELECT * from issues WHERE Title="' + self.id + '"'
            if self.sort:
                q += f' order by {self.sort}'
            if self.limit and self.limit.isnumeric():
                q += f' limit {self.limit}'
            issues = self._dic_from_query(q)
            self.data = {'magazine': magazine, 'issues': issues}
        else:
            q = 'SELECT * from issues'
            if self.sort:
                q += f' order by {self.sort}'
            if self.limit and self.limit.isnumeric():
                q += f' limit {self.limit}'
            self.data = self._dic_from_query(q)

    def _shrinkmag(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['name', 'dpi']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        self.data = shrink_mag(kwargs['name'], check_int(kwargs['dpi'], 0))

    def _getissuename(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        self.data = ''

        dirname = os.path.dirname(kwargs['name'])

        dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '', '[': ' ', ']': ' ', '#': '# '}
        name_exploded = replace_all(kwargs['name'], dic).split()

        regex_pass, issuedate, year = get_issue_date(name_exploded)

        if regex_pass:
            if int(regex_pass) > 9:  # we think it's an issue number
                if issuedate.isdigit():
                    issuedate = issuedate.zfill(4)  # pad with leading zeros
            if dirname:
                title = os.path.basename(dirname)
                if '$Title' in CONFIG.get_str('MAG_DEST_FILE'):
                    fname = CONFIG.get_str('MAG_DEST_FILE').replace('$IssueDate', issuedate).replace(
                        '$Title', title)
                else:
                    fname = CONFIG.get_str('MAG_DEST_FILE').replace('$IssueDate', issuedate)
                self.data = os.path.join(dirname, fname + '.' + name_exploded[-1])
            else:
                self.data = "Regex %s [%s] %s" % (regex_pass, issuedate, year)
        else:
            self.data = "Regex %s [%s] %s" % (regex_pass, issuedate, year)

    def _createmagcovers(self, **kwargs):
        TELEMETRY.record_usage_data()
        refresh = 'refresh' in kwargs
        if 'wait' in kwargs:
            self.data = create_mag_covers(refresh=refresh)
        else:
            threading.Thread(target=create_mag_covers, name='API-MAGCOVERS', args=[refresh]).start()

    def _createmagcover(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'file' not in kwargs:
            self.data = 'Missing parameter: file'
            return
        refresh = 'refresh' in kwargs
        if 'page' in kwargs:
            self.data = create_mag_cover(issuefile=kwargs['file'], refresh=refresh, pagenum=kwargs['page'])
        else:
            self.data = create_mag_cover(issuefile=kwargs['file'], refresh=refresh)

    def _getbook(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
            return
        book = self._dic_from_query('SELECT * from books WHERE BookID="' + self.id + '"')
        self.data = {'book': book}

    def _queuebook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            try:
                res = db.match('SELECT Status,AudioStatus from books WHERE BookID=?', (kwargs['id'],))
                if not res:
                    self.data = "Invalid id: %s" % kwargs['id']
                else:
                    if kwargs.get('type', '') == 'AudioBook':
                        db.action("UPDATE books SET AudioStatus='Wanted' WHERE BookID=?", (kwargs["id"],))
                    else:
                        db.action("UPDATE books SET Status='Wanted' WHERE BookID=?", (kwargs["id"],))
                    self.data = 'OK'
            finally:
                db.close()

    def _unqueuebook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            try:
                res = db.match('SELECT Status,AudioStatus from books WHERE BookID=?', (kwargs['id'],))
                if not res:
                    self.data = "Invalid id: %s" % kwargs['id']
                else:
                    if kwargs.get('type', '') == 'AudioBook':
                        db.action("UPDATE books SET AudioStatus='Skipped' WHERE BookID=?", (kwargs["id"],))
                    else:
                        db.action("UPDATE books SET Status='Skipped' WHERE BookID=?", (kwargs["id"],))
                    self.data = 'OK'
            finally:
                db.close()

    def _addmagazine(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('name')
        if not self.id:
            self.data = 'Missing parameter: name'
            return
        control_value_dict = {"Title": self.id}
        new_value_dict = {
            "Regex": None,
            "Status": "Active",
            "MagazineAdded": today(),
            "IssueStatus": "Wanted",
            "Reject": None
        }
        db = database.DBConnection()
        try:
            db.upsert("magazines", new_value_dict, control_value_dict)
        finally:
            db.close()

    def _removemagazine(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('name')
        if not self.id:
            self.data = 'Missing parameter: name'
            return
        db = database.DBConnection()
        try:
            db.action('DELETE from magazines WHERE Title=?', (self.id,))
            db.action('DELETE from wanted WHERE BookID=?', (self.id,))
        finally:
            db.close()

    def _pauseauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            try:
                res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
                if not res:
                    self.data = "Invalid id: %s" % kwargs['id']
                else:
                    db.action("UPDATE authors SET Status='Paused' WHERE AuthorID=?", (kwargs["id"],))
                    self.data = 'OK'
            finally:
                db.close()

    def _ignoreauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            try:
                res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
                if not res:
                    self.data = "Invalid id: %s" % kwargs['id']
                else:
                    db.action("UPDATE authors SET Status='Ignored' WHERE AuthorID=?", (kwargs["id"],))
                    self.data = 'OK'
            finally:
                db.close()

    def _resumeauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            try:
                res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
                if not res:
                    self.data = "Invalid id: %s" % kwargs['id']
                else:
                    db.action("UPDATE authors SET Status='Active' WHERE AuthorID=?", (kwargs["id"],))
                    self.data = 'OK'
            finally:
                db.close()

    def _authorupdate(self):
        TELEMETRY.record_usage_data()
        try:
            self.data = author_update(restart=False, only_overdue=False)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _seriesupdate(self):
        TELEMETRY.record_usage_data()
        try:
            self.data = series_update(restart=False, only_overdue=False)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _refreshauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        refresh = 'refresh' in kwargs
        self.id = kwargs.get('name')
        if not self.id:
            self.data = 'Missing parameter: name'
            return
        try:
            add_author_to_db(self.id, refresh=refresh, reason="API refresh_author %s" % self.id)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _forceactiveauthorsupdate(self, **kwargs):
        TELEMETRY.record_usage_data()
        refresh = 'refresh' in kwargs
        if 'wait' in kwargs:
            self.data = all_author_update(refresh=refresh)
        else:
            threading.Thread(target=all_author_update, name='API-AAUPDATE', args=[refresh]).start()

    def _forcemagsearch(self, **kwargs):
        TELEMETRY.record_usage_data()
        if CONFIG.use_any():
            if 'wait' in kwargs:
                search_magazines(None, True)
            else:
                threading.Thread(target=search_magazines, name='API-SEARCHMAGS', args=[None, True]).start()
        else:
            self.data = 'No search methods set, check config'

    def _forcersssearch(self, **kwargs):
        TELEMETRY.record_usage_data()
        if CONFIG.use_rss():
            if 'wait' in kwargs:
                search_rss_book()
            else:
                threading.Thread(target=search_rss_book, name='API-SEARCHRSS', args=[]).start()
        else:
            self.data = 'No rss feeds set, check config'

    def _forcecomicsearch(self, **kwargs):
        TELEMETRY.record_usage_data()
        if CONFIG.use_any():
            if 'wait' in kwargs:
                search_comics()
            else:
                threading.Thread(target=search_comics, name='API-SEARCHCOMICS', args=[]).start()
        else:
            self.data = 'No search methods set, check config'

    def _forcewishlistsearch(self, **kwargs):
        TELEMETRY.record_usage_data()
        if CONFIG.use_wishlist():
            if 'wait' in kwargs:
                search_wishlist()
            else:
                threading.Thread(target=search_wishlist, name='API-SEARCHWISHLIST', args=[]).start()
        else:
            self.data = 'No wishlists set, check config'

    def _forcebooksearch(self, **kwargs):
        TELEMETRY.record_usage_data()
        library = kwargs.get('type')
        if CONFIG.use_any():
            if 'wait' in kwargs:
                search_book(library=library)
            else:
                threading.Thread(target=search_book, name='API-SEARCHALLBOOK', args=[None, library]).start()
        else:
            self.data = "No search methods set, check config"

    @staticmethod
    def _forceprocess(**kwargs):
        TELEMETRY.record_usage_data()
        startdir = kwargs.get('dir')
        ignoreclient = 'ignoreclient' in kwargs
        process_dir(startdir=startdir, ignoreclient=ignoreclient)

    @staticmethod
    def _forcelibraryscan(**kwargs):
        TELEMETRY.record_usage_data()
        startdir = kwargs.get('dir')
        authid = kwargs.get('id')
        remove = 'remove' in kwargs
        if 'wait' in kwargs:
            library_scan(startdir=startdir, library='eBook', authid=authid, remove=remove)
        else:
            threading.Thread(target=library_scan, name='API-LIBRARYSCAN',
                             args=[startdir, 'eBook', authid, remove]).start()

    @staticmethod
    def _forcecomicscan(**kwargs):
        TELEMETRY.record_usage_data()
        comicid = kwargs.get('id')
        if 'wait' in kwargs:
            comic_scan(comicid=comicid)
        else:
            threading.Thread(target=comic_scan, name='API-COMICSCAN',
                             args=[comicid]).start()

    @staticmethod
    def _forceaudiobookscan(**kwargs):
        TELEMETRY.record_usage_data()
        startdir = kwargs.get('dir')
        authid = kwargs.get('id')
        remove = 'remove' in kwargs
        if 'wait' in kwargs:
            library_scan(startdir=startdir, library='AudioBook', authid=authid, remove=remove)
        else:
            threading.Thread(target=library_scan, name='API-LIBRARYSCAN',
                             args=[startdir, 'AudioBook', authid, remove]).start()

    @staticmethod
    def _forcemagazinescan(**kwargs):
        TELEMETRY.record_usage_data()
        title = kwargs.get('title')
        if 'wait' in kwargs:
            magazine_scan(title)
        else:
            threading.Thread(target=magazine_scan, name='API-MAGSCAN', args=[title]).start()

    def _deleteemptyseries(self):
        TELEMETRY.record_usage_data()
        self.data = delete_empty_series()

    def _cleancache(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'wait' in kwargs:
            self.data = clean_cache()
        else:
            threading.Thread(target=clean_cache, name='API-CLEANCACHE', args=[]).start()

    def _setworkpages(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'wait' in kwargs:
            self.data = set_work_pages()
        else:
            threading.Thread(target=set_work_pages, name='API-SETWORKPAGES', args=[]).start()

    def _setworkid(self, **kwargs):
        TELEMETRY.record_usage_data()
        ids = kwargs.get('bookids')
        if 'wait' in kwargs:
            self.data = set_work_id(ids)
        else:
            threading.Thread(target=set_work_id, name='API-SETWORKID', args=[ids]).start()

    def _setallbookseries(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'wait' in kwargs:
            self.data = set_all_book_series()
        else:
            threading.Thread(target=set_all_book_series, name='API-SETALLBOOKSERIES', args=[]).start()

    def _setallbookauthors(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'wait' in kwargs:
            self.data = set_all_book_authors()
        else:
            threading.Thread(target=set_all_book_authors, name='API-SETALLBOOKAUTHORS', args=[]).start()

    def _getbookcovers(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'wait' in kwargs:
            self.data = get_book_covers()
        else:
            threading.Thread(target=get_book_covers, name='API-GETBOOKCOVERS', args=[]).start()

    def _getauthorimages(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'wait' in kwargs:
            self.data = get_author_images()
        else:
            threading.Thread(target=get_author_images, name='API-GETAUTHORIMAGES', args=[]).start()

    def _getversion(self):
        TELEMETRY.record_usage_data()
        self.data = {
            'Success': True,
            'install_type': CONFIG.get_str('INSTALL_TYPE'),
            'current_version': CONFIG.get_str('CURRENT_VERSION'),
            'latest_version': CONFIG.get_str('LATEST_VERSION'),
            'commits_behind': CONFIG.get_int('COMMITS_BEHIND'),
        }

    def _getcurrentversion(self):
        TELEMETRY.record_usage_data()
        self.data = {
            'Success': True,
            'Data': CONFIG.get_str('CURRENT_VERSION'),
            'Error':  {'Code': 200, 'Message': 'OK'}
        }

    @staticmethod
    def _shutdown():
        TELEMETRY.record_usage_data()
        lazylibrarian.SIGNAL = 'shutdown'

    @staticmethod
    def _restart():
        TELEMETRY.record_usage_data()
        lazylibrarian.SIGNAL = 'restart'

    @staticmethod
    def _update():
        TELEMETRY.record_usage_data()
        lazylibrarian.SIGNAL = 'update'

    def _findauthorid(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        if 'source' in kwargs:
            source = kwargs['source']
        else:
            source = CONFIG.get_str('BOOK_API')
        authorname = format_author_name(kwargs['name'], postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
        if source == 'GoodReads':
            gr = GoodReads(authorname)
            self.data = gr.find_author_id()
        if source == 'OpenLibrary':
            ol = OpenLibrary(authorname)
            self.data = ol.find_author_id()
        if source == 'HardCover':
            hc = HardCover(authorname)
            self.data = hc.find_author_id()

    def _findmissingauthorid(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'source' in kwargs:
            source = kwargs['source']
        else:
            source = CONFIG.get_str('BOOK_API')
        cnt = 0
        db = database.DBConnection()
        key = ''
        if source == 'GoodReads' and CONFIG['GR_API']:
            key = 'gr_id'
        elif source == 'OpenLibrary' and CONFIG['OL_API']:
            key = 'ol_id'
        elif source == 'HardCover' and CONFIG['HC_API']:
            key = 'hc_id'
        if not key:
            self.data = f"Invalid or disabled source [{source}]"
            return
        try:
            authordata = db.select(f"SELECT AuthorName from authors WHERE {key}='' or {key} is null")
            print(len(authordata))
            api = None
            res = {}
            for author in authordata:
                if source == 'GoodReads':
                    api = GoodReads(author['AuthorName'])
                elif source == 'OpenLibrary':
                    api = OpenLibrary(author['AuthorName'])
                elif source == 'HardCover':
                    api = HardCover(author['AuthorName'])
                if api:
                    res = api.find_author_id()
                if res.get('authorid'):
                    db.action(f"update authors set {key}=? where authorname=?",
                              (res.get('authorid'), author['AuthorName']))
                    cnt += 1
        finally:
            db.close()
        self.data = f"Updated {source} authorid for {cnt} authors"

    def _findauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return

        authorname = format_author_name(kwargs['name'], postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
        if CONFIG.get_str('BOOK_API') == "GoogleBooks":
            gb = GoogleBooks(authorname)
            myqueue = Queue()
            search_api = threading.Thread(target=gb.find_results, name='API-GBRESULTS',
                                          args=[' <ll> ' + authorname, myqueue])
            search_api.start()
        elif CONFIG.get_str('BOOK_API') == "GoodReads":
            gr = GoodReads(authorname)
            myqueue = Queue()
            search_api = threading.Thread(target=gr.find_results, name='API-GRRESULTS',
                                          args=[' <ll> ' + authorname, myqueue])
            search_api.start()
        elif CONFIG.get_str('BOOK_API') == "HardCover":
            hc = HardCover(authorname)
            myqueue = Queue()
            search_api = threading.Thread(target=hc.find_results, name='API-HCRESULTS',
                                          args=[' <ll> ' + authorname, myqueue])
            search_api.start()
        else:  # if lazylibrarian.CONFIG.get_str('BOOK_API') == "OpenLibrary":
            ol = OpenLibrary(authorname)
            myqueue = Queue()
            search_api = threading.Thread(target=ol.find_results, name='API-OLRESULTS',
                                          args=[' <ll> ' + authorname, myqueue])
            search_api.start()

        search_api.join()
        self.data = myqueue.get()

    def _findbook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return

        if CONFIG.get_str('BOOK_API') == "GoogleBooks":
            gb = GoogleBooks(kwargs['name'])
            myqueue = Queue()
            search_api = threading.Thread(target=gb.find_results, name='API-GBRESULTS',
                                          args=[kwargs['name'] + ' <ll> ', myqueue])
            search_api.start()
        elif CONFIG.get_str('BOOK_API') == "GoodReads":
            gr = GoodReads(kwargs['name'])
            myqueue = Queue()
            search_api = threading.Thread(target=gr.find_results, name='API-GRRESULTS',
                                          args=[kwargs['name'] + ' <ll> ', myqueue])
            search_api.start()
        elif CONFIG.get_str('BOOK_API') == "HardCover":
            hc = HardCover(kwargs['name'])
            myqueue = Queue()
            search_api = threading.Thread(target=hc.find_results, name='API-HCRESULTS',
                                          args=[kwargs['name'] + ' <ll> ', myqueue])
            search_api.start()
        else:  # if lazylibrarian.CONFIG.get_str('BOOK_API') == "OpenLibrary":
            ol = OpenLibrary(kwargs['name'])
            myqueue = Queue()
            search_api = threading.Thread(target=ol.find_results, name='API-OLRESULTS',
                                          args=[kwargs['name'] + ' <ll> ', myqueue])
            search_api.start()

        search_api.join()
        self.data = myqueue.get()

    def _addbook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return

        if CONFIG.get_str('BOOK_API') == "GoogleBooks":
            gb = GoogleBooks(kwargs['id'])
            threading.Thread(target=gb.find_book, name='API-GBRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()
        elif CONFIG.get_str('BOOK_API') == "GoodReads":
            gr = GoodReads(kwargs['id'])
            threading.Thread(target=gr.find_book, name='API-GRRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()
        elif CONFIG.get_str('BOOK_API') == "HardCover":
            hc = HardCover(kwargs['id'])
            threading.Thread(target=hc.find_book, name='API-HCRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()
        elif CONFIG.get_str('BOOK_API') == "OpenLibrary":
            ol = OpenLibrary(kwargs['id'])
            threading.Thread(target=ol.find_book, name='API-OLRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()

    def _movebook(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['id', 'toid']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        try:
            db = database.DBConnection()
            try:
                authordata = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['toid'],))
                if not authordata:
                    self.data = "No destination author [%s] in the database" % kwargs['toid']
                else:
                    bookdata = db.match('SELECT AuthorID, BookName from books where BookID=?', (kwargs['id'],))
                    if not bookdata:
                        self.data = "No bookid [%s] in the database" % kwargs['id']
                    else:
                        control_value_dict = {'BookID': kwargs['id']}
                        new_value_dict = {'AuthorID': kwargs['toid']}
                        db.upsert("books", new_value_dict, control_value_dict)
                        update_totals(bookdata[0])  # we moved from here
                        update_totals(kwargs['toid'])  # to here
                        self.data = "Moved book [%s] to [%s]" % (bookdata[1], authordata[0])
            finally:
                db.close()
            self.logger.debug(self.data)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _movebooks(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['fromname', 'toname']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        try:
            db = database.DBConnection()
            try:
                q = 'SELECT bookid,books.authorid from books,authors where books.AuthorID = authors.AuthorID'
                q += ' and authorname=?'
                fromhere = db.select(q, (kwargs['fromname'],))

                tohere = db.match('SELECT authorid from authors where authorname=?', (kwargs['toname'],))
                if not len(fromhere):
                    self.data = "No books by [%s] in the database" % kwargs['fromname']
                else:
                    if not tohere:
                        self.data = "No destination author [%s] in the database" % kwargs['toname']
                    else:
                        db.action('UPDATE books SET authorid=?, where authorname=?', (tohere[0], kwargs['fromname']))
                        self.data = "Moved %s books from %s to %s" % (len(fromhere), kwargs['fromname'],
                                                                      kwargs['toname'])
                        update_totals(fromhere[0][1])  # we moved from here
                        update_totals(tohere[0])  # to here
            finally:
                db.close()

            self.logger.debug(self.data)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _comicmeta(self, **kwargs):
        TELEMETRY.record_usage_data()
        name = kwargs.get('name')
        if not name:
            self.data = 'Missing parameter: name'
            return
        xml = 'xml' in kwargs
        self.data = comic_metadata(name, xml=xml)

    def _comicid(self, **kwargs):
        TELEMETRY.record_usage_data()
        name = kwargs.get('name')
        if not name:
            self.data = 'Missing parameter: name'
            return
        source = kwargs.get('source')
        if source not in ['cv', 'cx']:
            self.data = 'Invalid parameter: source'
            return
        best = 'best' in kwargs
        if source == 'cv':
            self.data = cv_identify(name, best=best)
        else:
            self.data = cx_identify(name, best=best)

    def _addauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        name = kwargs.get('name')
        if not name:
            self.data = 'Missing parameter: name'
            return
        books = True if kwargs.get('books') else False
        try:
            self.data = add_author_name_to_db(author=name, refresh=False, addbooks=books,
                                              reason="API add_author %s" % name)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _addauthorid(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
            return
        books = True if kwargs.get('books') else False
        try:
            self.data = add_author_to_db(refresh=False, authorid=self.id, addbooks=books,
                                         reason="API add_author_id %s" % self.id)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grfollowall(self):
        TELEMETRY.record_usage_data()
        db = database.DBConnection()
        try:
            cmd = ("SELECT AuthorName,AuthorID,GRfollow FROM authors where Status='Active' "
                   "or Status='Wanted' or Status='Loading'")
            authors = db.select(cmd)
            count = 0
            for author in authors:
                followid = check_int(author['GRfollow'], 0)
                if followid > 0:
                    self.logger.debug('%s is already followed' % author['AuthorName'])
                elif author['GRfollow'] == "0":
                    self.logger.debug('%s is manually unfollowed' % author['AuthorName'])
                else:
                    res = grfollow(author['AuthorID'], True)
                    if res.startswith('Unable'):
                        self.logger.warning(res)
                    try:
                        followid = res.split("followid=")[1]
                        self.logger.debug('%s marked followed' % author['AuthorName'])
                        count += 1
                    except IndexError:
                        followid = ''
                    db.action('UPDATE authors SET GRfollow=? WHERE AuthorID=?', (followid, author['AuthorID']))
        finally:
            db.close()
        self.data = "Added follow to %s %s" % (count, plural(count, "author"))

    def _hcsync(self, **kwargs):
        TELEMETRY.record_usage_data()
        library = kwargs.get('library', '')
        userid = kwargs.get('user', None)
        try:
            self.data = lazylibrarian.hc.hc_sync(library=library, userid=userid)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grsync(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['shelf', 'status']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        library = kwargs.get('library', 'eBook')
        reset = 'reset' in kwargs
        try:
            self.data = grsync(kwargs['status'], kwargs['shelf'], library, reset)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grfollow(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        try:
            self.data = grfollow(authorid=kwargs['id'], follow=True)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grunfollow(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        try:
            self.data = grfollow(authorid=kwargs['id'], follow=False)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _searchitem(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'item' not in kwargs:
            self.data = 'Missing parameter: item'
            return
        else:
            self.data = search_item(kwargs['item'])

    def _searchbook(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        books = [{"bookid": kwargs['id']}]
        library = kwargs.get('type')
        if CONFIG.use_any():
            if 'wait' in kwargs:
                search_book(books=books, library=library)
            else:
                threading.Thread(target=search_book, name='API-SEARCHBOOK', args=[books, library]).start()
        else:
            self.data = "No search methods set, check config"

    def _removeauthor(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
            return
        db = database.DBConnection()
        try:
            authorsearch = db.select('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
            if len(authorsearch):  # to stop error if try to remove an author while they are still loading
                author_name = authorsearch[0]['AuthorName']
                self.logger.debug("Removing all references to author: %s" % author_name)
                db.action('DELETE from authors WHERE AuthorID=?', (kwargs['id'],))
        finally:
            db.close()

    def _writecfg(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['name', 'value', 'group']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        try:
            item = CONFIG.get_item(kwargs['name'])
            if item:
                item.set_from_ui(kwargs['value'])
            CONFIG.save_config_and_backup_old(save_all=False, section=kwargs['group'])
        except Exception as e:
            self.data = 'Unable to update CFG entry for %s: %s, %s' % (kwargs['group'], kwargs['name'], str(e))

    def _readcfg(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['name', 'group']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        try:
            self.data = f"[{CONFIG.get_str(kwargs['name'])}]"
        except configparser.Error:
            self.data = 'No config entry for %s: %s' % (kwargs['group'], kwargs['name'])

    @staticmethod
    def _loadcfg():
        TELEMETRY.record_usage_data()
        # No need to reload the config
        pass

    def _getseriesauthors(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            count = get_series_authors(self.id)
            self.data = "Added %s" % count

    def _addseriesmembers(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = add_series_members(self.id)

    def _getseriesmembers(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_series_members(self.id)

    def _getbookauthors(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_book_authors(self.id)

    def _getworkseries(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        if not kwargs.get('source'):
            self.data = 'Missing parameter: source'
        else:
            self.data = get_work_series(self.id, kwargs.get('source'), reason="API get_work_series")

    def _getworkpage(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self.data = get_work_page(self.id)

    def _getbookcover(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            if 'src' in kwargs:
                self.data = get_book_cover(self.id, kwargs['src'])
            else:
                self.data = get_book_cover(self.id)

    def _getauthorimage(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
            return
        refresh = 'refresh' in kwargs
        max_num = kwargs.get('max', 1)
        self.data = get_author_image(self.id, refresh=refresh, max_num=max_num)

    def _lock(self, table, itemid, state):
        TELEMETRY.record_usage_data()
        db = database.DBConnection()
        try:
            dbentry = db.match('SELECT %sID from %ss WHERE %sID=%s' % (table, table, table, itemid))
            if dbentry:
                db.action("UPDATE %ss SET Manual='%s' WHERE %sID=%s" % (table, state, table, itemid))
            else:
                self.data = "%sID %s not found" % (table, itemid)
        finally:
            db.close()

    def _setauthorlock(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self._lock("author", kwargs['id'], "1")

    def _setauthorunlock(self, **kwargs):
        TELEMETRY.record_usage_data()
        self.id = kwargs.get('id')
        if not self.id:
            self.data = 'Missing parameter: id'
        else:
            self._lock("author", kwargs['id'], "0")

    def _setauthorimage(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['id', 'img']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        self._setimage("author", kwargs['id'], kwargs['img'])

    def _setbookimage(self, **kwargs):
        TELEMETRY.record_usage_data()
        for item in ['id', 'img']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        self._setimage("book", kwargs['id'], kwargs['img'])

    def _setimage(self, table, itemid, img):
        TELEMETRY.record_usage_data()
        msg = "%s Image [%s] rejected" % (table, img)
        # Cache file image
        if path_isfile(img):
            extn = os.path.splitext(img)[1].lower()
            if extn and extn in ['.jpg', '.jpeg', '.png']:
                destfile = os.path.join(DIRS.CACHEDIR, table, itemid + '.jpg')
                try:
                    shutil.copy(img, destfile)
                    setperm(destfile)
                    msg = ''
                except Exception as why:
                    msg += " Failed to copy file: %s %s" % (type(why).__name__, str(why))
            else:
                msg += " invalid extension"

        if img.startswith('http'):
            # cache image from url
            extn = os.path.splitext(img)[1].lower()
            if extn and extn in ['.jpg', '.jpeg', '.png']:
                _, success, _ = cache_img(ImageType(table), itemid, img)
                if success:
                    msg = ''
                else:
                    msg += " Failed to cache file"
            else:
                msg += " invalid extension"
        elif msg:
            msg += " Not found"

        if msg:
            self.data = msg
            return

        db = database.DBConnection()
        try:
            dbentry = db.match('SELECT %sID from %ss WHERE %sID=%s' % (table, table, table, itemid))
            if dbentry:
                db.action("UPDATE %ss SET %sImg='%s' WHERE %sID=%s" %
                          (table, table, 'cache' + os.path.sep + itemid + '.jpg', table, itemid))
            else:
                self.data = "%sID %s not found" % (table, itemid)
        finally:
            db.close()

    def _setbooklock(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        self._lock("book", kwargs['id'], "1")

    def _deduplicate(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        if 'wait' in kwargs:
            self.data = de_duplicate(kwargs['id'])
        else:
            threading.Thread(target=de_duplicate, name='API-DEDUPLICATE_%s' % kwargs['id'],
                             args=[kwargs['id']]).start()

    def _setbookunlock(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        self._lock("book", kwargs['id'], "0")

    @staticmethod
    def _restartjobs():
        TELEMETRY.record_usage_data()
        restart_jobs(command=SchedulerCommand.RESTART)

    @staticmethod
    def _checkrunningjobs():
        TELEMETRY.record_usage_data()
        check_running_jobs()

    def _showjobs(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'json' in kwargs:
            self.data = show_jobs(json=True)
        else:
            self.data = show_jobs()

    def _showstats(self, **kwargs):
        TELEMETRY.record_usage_data()
        if 'json' in kwargs:
            self.data = show_stats(json=True)
        else:
            self.data = show_stats()

    def _importalternate(self, **kwargs):
        TELEMETRY.record_usage_data()
        usedir = kwargs.get('dir', CONFIG.get_str('ALTERNATE_DIR'))
        library = kwargs.get('library', 'eBook')
        if 'wait' in kwargs:
            self.data = process_alternate(usedir, library)
        else:
            threading.Thread(target=process_alternate, name='API-IMPORTALT', args=[usedir, library]).start()

    def _includealternate(self, **kwargs):
        TELEMETRY.record_usage_data()
        startdir = kwargs.get('dir', CONFIG.get_str('ALTERNATE_DIR'))
        library = kwargs.get('library', 'eBook')
        if 'wait' in kwargs:
            self.data = library_scan(startdir, library, None, False)
        else:
            threading.Thread(target=library_scan, name='API-INCLUDEALT',
                             args=[startdir, library, None, False]).start()

    def _importcsvwishlist(self, **kwargs):
        TELEMETRY.record_usage_data()
        usedir = kwargs.get('dir', CONFIG.get_str('ALTERNATE_DIR'))
        status = kwargs.get('status', 'Wanted')
        library = kwargs.get('library', 'eBook')
        if 'wait' in kwargs:
            self.data = import_csv(usedir, status, library)
        else:
            threading.Thread(target=import_csv, name='API-IMPORTCSV', args=[usedir, status, library]).start()

    def _exportcsvwishlist(self, **kwargs):
        TELEMETRY.record_usage_data()
        usedir = kwargs.get('dir', CONFIG.get_str('ALTERNATE_DIR'))
        status = kwargs.get('status', 'Wanted')
        library = kwargs.get('library', 'eBook')
        if 'wait' in kwargs:
            self.data = export_csv(usedir, status, library)
        else:
            threading.Thread(target=export_csv, name='API-EXPORTCSV', args=[usedir, status, library]).start()

    def _telemetryshow(self):
        TELEMETRY.record_usage_data()
        self.data = TELEMETRY.get_data_for_ui_preview(send_usage=True, send_config=True)

    def _telemetrysend(self):
        TELEMETRY.record_usage_data()
        self.data = telemetry_send()
