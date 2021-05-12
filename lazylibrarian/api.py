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


import json
import os
import shutil
import sys
import threading

# noinspection PyUnresolvedReferences
from six.moves import configparser, queue
# noinspection PyUnresolvedReferences
from six.moves.urllib_parse import urlsplit, urlunsplit

import cherrypy
import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.bookrename import audio_rename, name_vars
from lazylibrarian.bookwork import set_work_pages, get_work_series, get_work_page, set_all_book_series, \
    get_series_members, get_series_authors, delete_empty_series, get_book_authors, set_all_book_authors, \
    set_work_id, get_gb_info, set_genres, genre_filter, get_book_pubdate, add_series_members
from lazylibrarian.cache import cache_img, clean_cache
from lazylibrarian.calibre import sync_calibre_list, calibre_list
from lazylibrarian.comicid import cv_identify, cx_identify, comic_metadata
from lazylibrarian.comicscan import comic_scan
from lazylibrarian.comicsearch import search_comics
from lazylibrarian.common import clear_log, restart_jobs, show_jobs, check_running_jobs, all_author_update, setperm, \
    log_header, author_update, show_stats, series_update, listdir, path_isfile, path_isdir, syspath, cpu_use
from lazylibrarian.csvfile import import_csv, export_csv, dump_table
from lazylibrarian.formatter import today, format_author_name, check_int, plural, replace_all
from lazylibrarian.gb import GoogleBooks
from lazylibrarian.gr import GoodReads
from lazylibrarian.ol import OpenLibrary
from lazylibrarian.grsync import grfollow, grsync
from lazylibrarian.images import get_author_image, get_author_images, get_book_cover, get_book_covers, \
    create_mag_covers, create_mag_cover, shrink_mag
from lazylibrarian.importer import add_author_to_db, add_author_name_to_db, update_totals
from lazylibrarian.librarysync import library_scan
from lazylibrarian.magazinescan import magazine_scan
from lazylibrarian.manualbook import search_item
from lazylibrarian.postprocess import process_dir, process_alternate, create_opf, process_img, \
    process_book_from_dir, process_mag_from_file
from lazylibrarian.preprocessor import preprocess_ebook, preprocess_audio, preprocess_magazine
from lazylibrarian.providers import get_capabilities
from lazylibrarian.rssfeed import gen_feed
from lazylibrarian.searchbook import search_book
from lazylibrarian.searchmag import search_magazines, get_issue_date
from lazylibrarian.searchrss import search_rss_book, search_wishlist
from six import PY2, string_types

cmd_dict = {'help': 'list available commands. ' +
                    'Time consuming commands take an optional &wait parameter if you want to wait for completion, ' +
                    'otherwise they return OK straight away and run in the background',
            'showMonths': 'List installed monthnames',
            'dumpMonths': 'Save installed monthnames to file',
            'saveTable': '&table= Save a database table to a file',
            'getIndex': 'list all authors',
            'getAuthor': '&id= get author by AuthorID and list their books',
            'getAuthorImage': '&id= [&refresh] [&max] get one or more images for this author',
            'setAuthorImage': '&id= &img= set a new image for this author',
            'setAuthorLock': '&id= lock author name/image/dates',
            'setAuthorUnlock': '&id= unlock author name/image/dates',
            'setBookLock': '&id= lock book details',
            'setBookUnlock': '&id= unlock book details',
            'setBookImage': '&id= &img= set a new image for this book',
            'shrinkMag': '&name= &size= shrink magazine size',
            'getAuthorImages': '[&wait] get images for all authors without one',
            'getWanted': 'list wanted books',
            'getRead': 'list read books for current user',
            'getReading': 'list currently-reading books for current user',
            'getToRead': 'list to-read books for current user',
            'getAbandoned': 'list abandoned books for current user',
            'getSnatched': 'list snatched books',
            'getHistory': 'list history',
            'getLogs': 'show current log',
            'getDebug': 'show debug log header',
            'getModules': 'show installed modules',
            'checkModules': 'Check using lazylibrarian library modules',
            'clearLogs': 'clear current log',
            'getMagazines': 'list magazines',
            'getIssues': '&name= list issues of named magazine',
            'getIssueName': '&name= get name of issue from path/filename',
            'createMagCovers': '[&wait] [&refresh] create covers for magazines, optionally refresh existing ones',
            'createMagCover': '&file= [&refresh] [&page=] create cover for magazine issue, optional page number',
            'forceMagSearch': '[&wait] search for all wanted magazines',
            'forceBookSearch': '[&wait] [&type=eBook/AudioBook] search for all wanted books',
            'forceRSSSearch': '[&wait] search all entries in rss feeds',
            'forceComicSearch': '[&wait] search for all wanted comics',
            'getRSSFeed': '&feed= [&limit=] show rss feed entries',
            'forceWishlistSearch': '[&wait] search all entries in wishlists',
            'forceProcess': '[&dir] [ignorekeepseeding] process books/mags in download or named dir',
            'pauseAuthor': '&id= pause author by AuthorID',
            'resumeAuthor': '&id= resume author by AuthorID',
            'ignoreAuthor': '&id= ignore author by AuthorID',
            'refreshAuthor': '&name= [&refresh] reload author (and their books) by name, optionally refresh cache',
            'authorUpdate': 'update the oldest author',
            'seriesUpdate': 'update the oldest series',
            'forceActiveAuthorsUpdate': '[&wait] [&refresh] reload all active authors and book data, refresh cache',
            'forceLibraryScan': '[&wait] [&remove] [&dir=] [&id=] rescan whole or part book library',
            'forceComicScan': '[&wait] [&id=] rescan whole or part comic library',
            'forceAudioBookScan': '[&wait] [&remove] [&dir=] [&id=] rescan whole or part audiobook library',
            'forceMagazineScan': '[&wait] [&title=] rescan whole or part magazine library',
            'getVersion': 'show lazylibrarian current/git version',
            'shutdown': 'stop lazylibrarian',
            'restart': 'restart lazylibrarian',
            'update': 'update lazylibrarian',
            'findAuthor': '&name= search goodreads/googlebooks for named author',
            'findAuthorID': '&name= find goodreads ID for named author',
            'findBook': '&name= search goodreads/googlebooks for named book',
            'addBook': '&id= add book details to the database',
            'moveBooks': '&fromname= &toname= move all books from one author to another by AuthorName',
            'moveBook': '&id= &toid= move one book to new author by BookID and AuthorID',
            'addAuthor': '&name= add author to database by name',
            'addAuthorID': '&id= add author to database by AuthorID',
            'removeAuthor': '&id= remove author from database by AuthorID',
            'addMagazine': '&name= add magazine to database by name',
            'removeMagazine': '&name= remove magazine and all of its issues from database by name',
            'queueBook': '&id= [&type=eBook/AudioBook] mark book as Wanted, default eBook',
            'unqueueBook': '&id= [&type=eBook/AudioBook] mark book as Skipped, default eBook',
            'readCFG': '&name=&group= read value of config variable "name" in section "group"',
            'writeCFG': '&name=&group=&value= set config variable "name" in section "group" to value',
            'loadCFG': 'reload config from file',
            'getBookCover': '&id= [&src=] fetch cover link from cache/cover/librarything/goodreads/google for BookID',
            'getAllBooks': 'list all books in the database',
            'listNoLang': 'list all books in the database with unknown language',
            'listNoDesc': 'list all books in the database with no description',
            'listNoISBN': 'list all books in the database with no isbn',
            'listNoGenre': 'list all books in the database with no genre',
            'listNoBooks': 'list all authors in the database with no books',
            'listDupeBooks': 'list all books in the database with more than one entry',
            'listDupeBookStatus': 'list all copies of books in the database with more than one entry',
            'removeNoBooks': 'delete all authors in the database with no books',
            'listIgnoredAuthors': 'list all authors in the database marked ignored',
            'listIgnoredBooks': 'list all books in the database marked ignored',
            'listIgnoredSeries': 'list all series in the database marked ignored',
            'listMissingWorkpages': 'list all books with errorpage or no workpage',
            'searchBook': '&id= [&wait] [&type=eBook/AudioBook] search for one book by BookID',
            'searchItem': '&item= get search results for an item (author, title, isbn)',
            'showStats': 'show database statistics',
            'showJobs': 'show status of running jobs',
            'restartJobs': 'restart background jobs',
            'showThreads': 'show threaded processes',
            'checkRunningJobs': 'ensure all needed jobs are running',
            'vacuum': 'vacuum the database',
            'getWorkSeries': '&id= Get series from Librarything BookWork using BookID or GoodReads using WorkID',
            'addSeriesMembers': '&id= add series members to database using SeriesID',
            'getSeriesMembers': '&id= Get list of series members using SeriesID',
            'getSeriesAuthors': '&id= Get all authors for a series and import them',
            'getWorkPage': '&id= Get url of Librarything BookWork using BookID',
            'getBookCovers': '[&wait] Check all books for cached cover and download one if missing',
            'getBookAuthors': '&id= Get list of authors associated with this book',
            'cleanCache': '[&wait] Clean unused and expired files from the LazyLibrarian caches',
            'deleteEmptySeries': 'Delete any book series that have no members',
            'setNoDesc': '[&refresh] Set descriptions for all books, include "No Description" entries on refresh',
            'setNoGenre': '[&refresh] Set book genre for all books without one, include "Unknown" entries on refresh',
            'setWork_Pages': '[&wait] Set the WorkPages links in the database',
            'setAllBookSeries': '[&wait] Set the series details from goodreads or librarything workpages',
            'setAllBookAuthors': '[&wait] Set all authors for all books from book workpages',
            'setWorkID': '[&wait] [&bookids] Set WorkID for all books that dont have one, or bookids',
            'importAlternate': '[&wait] [&dir=] [&library=] Import ebooks/audiobooks from named or alternate folder' +
                                ' and any subfolders',
            'includeAlternate': '[&wait] [&dir=] [&library=] Include links to ebooks/audiobooks from named or ' +
                                ' alternate folder and any subfolders',
            'importCSVwishlist': '[&wait] [&dir=] Import a CSV wishlist from named or alternate directory',
            'exportCSVwishlist': '[&wait] [&dir=] Export a CSV wishlist to named or alternate directory',
            'grSync': '&status= &shelf= [&library=] [&reset] Sync books with given status to a goodreads shelf, ' +
                      'or reset goodreads shelf to match lazylibrarian',
            'grFollow': '&id= Follow an author on goodreads',
            'grFollowAll': 'Follow all lazylibrarian authors on goodreads',
            'grUnfollow': '&id= Unfollow an author on goodreads',
            'writeOPF': '&id= [&refresh] write out an opf file for a bookid, optionally overwrite existing opf',
            'writeAllOPF': '[&refresh] write out opf files for all books, optionally overwrite existing opf',
            'renameAudio': '&id Rename an audiobook using configured pattern',
            'createPlaylist': '&id Create playlist for an audiobook',
            'nameVars': '&id Show the name variables that would be used for a bookid',
            'showCaps': '&provider= get a list of capabilities from a provider',
            'calibreList': '[&toread=] [&read=] get a list of books in calibre library',
            'syncCalibreList': '[&toread=] [&read=] sync list of read/toread books with calibre',
            'logMessage': '&level= &text=  send a message to lazylibrarian logger',
            'comicid': '&name= &source= [&best] try to identify comic from name',
            'comicmeta': '&name= [&xml] get metadata from comic archive, xml or dictionary',
            'getBookPubdate': '&id= get original publication date of a book by bookid',
            'gc_init': 'Initialise gc_before state',
            'gc_stats': 'Show difference since gc_init',
            'gc_collect': 'Run garbage collection & return how many items',
            'listNewAuthors': '[&limit=] List newest authors and show when added and reason for adding',
            'listNewBooks': '[&limit=] List newest books and show when added and reason for adding',
            'importBook': '[&library=] &id= &dir= add library [eBook|Audio] bookid from folder',
            'importMag': '&title= &num= &file= add magazine issue from file',
            'preprocessAudio': '&dir= &author= &title= [&id=] [&tag] [&merge] preprocess an audiobook folder',
            'preprocessBook': '&dir= preprocess an ebook folder',
            'preprocessMagazine': '&dir= &cover= preprocess a magazine folder',
            'memUse': 'memory usage of the program in kB',
            'cpuUse': 'recent cpu usage of the program',
            'nice': 'show current nice level',
            'nicer': 'make a little nicer',
            'subscribe': '&user= &feed= subscribe a user to a feed',
            'unsubscribe': '&user= &feed= remove a user from a feed',
            'listAlienAuthors': 'List authors not matching current book api',
            'listAlienBooks': 'List books not matching current book api',
            }


class Api(object):
    def __init__(self):

        self.apikey = None
        self.cmd = None
        self.id = None

        self.kwargs = None

        self.data = None

        self.callback = None

    def check_params(self, **kwargs):

        if not lazylibrarian.CONFIG['API_ENABLED']:
            self.data = 'API not enabled'
            return
        if not lazylibrarian.CONFIG['API_KEY']:
            self.data = 'API key not generated'
            return
        if len(lazylibrarian.CONFIG['API_KEY']) != 32:
            self.data = 'API key is invalid'
            return

        if 'apikey' not in kwargs:
            self.data = 'Missing api key'
            return

        if kwargs['apikey'] != lazylibrarian.CONFIG['API_KEY']:
            self.data = 'Incorrect API key'
            return
        else:
            self.apikey = kwargs.pop('apikey')

        if 'cmd' not in kwargs:
            self.data = 'Missing parameter: cmd, try cmd=help'
            return

        if kwargs['cmd'] not in cmd_dict:
            self.data = 'Unknown command: %s, try cmd=help' % kwargs['cmd']
            return
        else:
            self.cmd = kwargs.pop('cmd')

        self.kwargs = kwargs
        self.data = 'OK'

    @property
    def fetch_data(self):

        threading.currentThread().name = "API"

        if self.data == 'OK':
            remote_ip = cherrypy.request.headers.get('X-Forwarded-For')  # apache2
            if not remote_ip:
                remote_ip = cherrypy.request.headers.get('X-Host')  # lighthttpd
            if not remote_ip:
                remote_ip = cherrypy.request.headers.get('Remote-Addr')
            else:
                remote_ip = cherrypy.request.remote.ip
            logger.debug('Received API command from %s: %s %s' % (remote_ip, self.cmd, self.kwargs))
            method_to_call = getattr(self, "_" + self.cmd.lower())
            method_to_call(**self.kwargs)

            if 'callback' not in self.kwargs:
                if isinstance(self.data, string_types):
                    return self.data
                else:
                    return json.dumps(self.data)
            else:
                self.callback = self.kwargs['callback']
                self.data = json.dumps(self.data)
                self.data = self.callback + '(' + self.data + ');'
                return self.data
        else:
            return self.data

    @staticmethod
    def _dic_from_query(query):

        db = database.DBConnection()
        rows = db.select(query)

        rows_as_dic = []

        for row in rows:
            # noinspection PyTypeChecker
            row_as_dic = dict(list(zip(list(row.keys()), row)))
            rows_as_dic.append(row_as_dic)

        return rows_as_dic

    def _memuse(self):
        """ Current Memory usage in kB """

        with open('/proc/self/status') as f:
            memusage = f.read().split('VmRSS:')[1].split('\n')[0][:-3]
        self.data = memusage.strip()

    def _cpuuse(self):
        self.data = cpu_use()

    def _nice(self):
        self.data = os.nice(0)

    def _nicer(self):
        self.data = os.nice(1)

    @staticmethod
    def _gc_init():
        from collections import defaultdict
        from gc import get_objects
        lazylibrarian.GC_BEFORE = defaultdict(int)
        for i in get_objects():
            lazylibrarian.GC_BEFORE[type(i)] += 1

    def _gc_collect(self):
        from gc import collect
        self.data = collect()

    def _gc_stats(self):
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
                res = res + changed
        self.data = res

    def _getrssfeed(self, **kwargs):
        if 'feed' in kwargs:
            ftype = kwargs['feed']
        else:
            ftype = 'eBook'

        if 'limit' in kwargs:
            limit = kwargs['limit']
        else:
            limit = 10

        if 'authorid' in kwargs:
            authorid = kwargs['authorid']
        else:
            authorid = None

        # url might end in .xml
        if not str(limit).isdigit():
            try:
                limit = int(limit.split('.')[0])
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
        col1 = None
        col2 = None
        if 'toread' in kwargs:
            col2 = kwargs['toread']
        if 'read' in kwargs:
            col1 = kwargs['read']
        self.data = sync_calibre_list(col1, col2)

    def _subscribe(self, **kwargs):
        if 'user' not in kwargs:
            self.data = 'Missing parameter: user'
            return
        if 'feed' not in kwargs:
            self.data = 'Missing parameter: feed'
            return
        db = database.DBConnection()
        res = db.match('SELECT UserID from users WHERE userid=?', (kwargs['user'],))
        if not res:
            self.data = 'Invalid userid'
            return
        for provider in lazylibrarian.RSS_PROV:
            if provider['DISPNAME'] == kwargs['feed']:
                if lazylibrarian.wishlist_type(provider['HOST']):
                    db.action('INSERT into subscribers (UserID , Type, WantID ) VALUES (?, ?, ?)',
                              (kwargs['user'], 'feed', kwargs['feed']))
                    self.data = 'OK'
                    return
        self.data = 'Invalid feed'
        return

    def _unsubscribe(self, **kwargs):
        if 'user' not in kwargs:
            self.data = 'Missing parameter: user'
            return
        if 'feed' not in kwargs:
            self.data = 'Missing parameter: feed'
            return
        db = database.DBConnection()
        db.action('DELETE FROM subscribers WHERE UserID=? and Type=? and WantID=?',
                  (kwargs['user'], 'feed', kwargs['feed']))
        self.data = 'OK'
        return

    def _calibrelist(self, **kwargs):
        col1 = None
        col2 = None
        if 'toread' in kwargs:
            col2 = kwargs['toread']
        if 'read' in kwargs:
            col1 = kwargs['read']
        self.data = calibre_list(col1, col2)

    def _showcaps(self, **kwargs):
        if 'provider' not in kwargs:
            self.data = 'Missing parameter: provider'
            return

        prov = kwargs['provider']
        match = False
        for provider in lazylibrarian.NEWZNAB_PROV:
            if prov == provider['HOST']:
                prov = provider
                match = True
                break
        if not match:
            for provider in lazylibrarian.TORZNAB_PROV:
                if prov == provider['HOST']:
                    prov = provider
                    match = True
                    break
        if not match:
            self.data = 'Invalid parameter: provider'
            return
        self.data = get_capabilities(prov, True)

    def _help(self):
        res = ''
        for key in sorted(cmd_dict):
            res += "%s: %s<p>" % (key, cmd_dict[key])
        self.data = res

    def _listalienauthors(self):
        cmd = "SELECT AuthorID,AuthorName from authors WHERE AuthorID "
        if lazylibrarian.CONFIG['BOOK_API'] != 'OpenLibrary':
            cmd += 'NOT '
        cmd += 'LIKE "OL%A"'
        self.data = self._dic_from_query(cmd)

    def _listalienbooks(self):
        cmd = "SELECT BookID,BookName from books WHERE BookID "
        if lazylibrarian.CONFIG['BOOK_API'] != 'OpenLibrary':
            cmd += 'NOT '
        cmd += 'LIKE "OL%W"'
        self.data = self._dic_from_query(cmd)

    def _gethistory(self):
        self.data = self._dic_from_query(
            "SELECT * from wanted WHERE Status != 'Skipped' and Status != 'Ignored'")

    def _listnewauthors(self, **kwargs):
        if 'limit' in kwargs:
            limit = "limit %s" % kwargs['limit']
        else:
            limit = ''
        self.data = self._dic_from_query(
            "SELECT authorid,authorname,dateadded,reason,status from authors order by dateadded desc %s" % limit)

    def _listnewbooks(self, **kwargs):
        if 'limit' in kwargs:
            limit = "limit %s" % kwargs['limit']
        else:
            limit = ''
        self.data = self._dic_from_query(
            "SELECT bookid,bookname,bookadded,scanresult,status from books order by bookadded desc %s" % limit)

    def _showthreads(self):
        self.data = [n.name for n in [t for t in threading.enumerate()]]

    def _showmonths(self):
        self.data = lazylibrarian.MONTHNAMES

    def _renameaudio(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        self.data = audio_rename(kwargs['id'], rename=True)

    def _getbookpubdate(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        self.data = get_book_pubdate(kwargs['id'])

    def _createplaylist(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        self.data = audio_rename(kwargs['id'], playlist=True)

    def _preprocessaudio(self, **kwargs):
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
        if 'dir' not in kwargs:
            self.data = 'Missing parameter: dir'
            return
        preprocess_ebook(kwargs['dir'])
        self.data = 'OK'

    def _preprocessmagazine(self, **kwargs):
        for item in ['dir', 'cover']:
            if item not in kwargs:
                self.data = 'Missing parameter: %s' % item
                return
        preprocess_magazine(kwargs['dir'], check_int(kwargs['cover'], 0))
        self.data = 'OK'

    def _importbook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        if 'dir' not in kwargs:
            self.data = 'Missing parameter: dir'
            return
        if 'library' not in kwargs:
            library = 'eBook'
        else:
            library = kwargs['library']
        self.data = process_book_from_dir(kwargs['dir'], library, kwargs['id'])

    def _importmag(self, **kwargs):
        if 'title' not in kwargs:
            self.data = 'Missing parameter: title'
            return
        if 'num' not in kwargs:
            self.data = 'Missing parameter: num'
            return
        if 'file' not in kwargs:
            self.data = 'Missing parameter: file'
            return
        self.data = process_mag_from_file(kwargs['file'], kwargs['title'], kwargs['num'])

    def _namevars(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        self.data = name_vars(kwargs['id'])

    def _savetable(self, **kwargs):
        if 'table' not in kwargs:
            self.data = 'Missing parameter: table'
            return
        valid = ['users', 'magazines']
        if kwargs['table'] not in valid:
            self.data = 'Invalid table. Only %s' % str(valid)
            return
        self.data = "Saved %s" % dump_table(kwargs['table'], lazylibrarian.DATADIR)

    def _writeallopf(self, **kwargs):
        db = database.DBConnection()
        books = db.select('select BookID from books where BookFile is not null')
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
                    counter = counter
        self.data = 'Updated opf for %s %s' % (counter, plural(counter, "book"))

    def _writeopf(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
            db = database.DBConnection()
            cmd = 'SELECT AuthorName,BookID,BookName,BookDesc,BookIsbn,BookImg,BookDate,BookLang,BookPub,'
            cmd += 'BookFile,BookRate from books,authors WHERE BookID=? and books.AuthorID = authors.AuthorID'
            res = db.match(cmd, (kwargs['id'],))
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
        json_file = os.path.join(lazylibrarian.DATADIR, 'monthnames.json')
        with open(syspath(json_file), 'w') as f:
            json.dump(lazylibrarian.MONTHNAMES, f)

    def _getwanted(self):
        self.data = self._dic_from_query(
            "SELECT * from books WHERE Status='Wanted'")

    def _getread(self):
        userid = None
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if not userid:
            self.data = 'No userid'
        else:
            self.data = self._dic_from_query(
                "SELECT haveread from users WHERE userid='%s'" % userid)

    def _gettoread(self):
        userid = None
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if not userid:
            self.data = 'No userid'
        else:
            self.data = self._dic_from_query(
                "SELECT toread from users WHERE userid='%s'" % userid)

    def _getreading(self):
        userid = None
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if not userid:
            self.data = 'No userid'
        else:
            self.data = self._dic_from_query(
                "SELECT reading from users WHERE userid='%s'" % userid)

    def _getabandoned(self):
        userid = None
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if not userid:
            self.data = 'No userid'
        else:
            self.data = self._dic_from_query(
                "SELECT abandoned from users WHERE userid='%s'" % userid)

    def _vacuum(self):
        msg1 = self._dic_from_query("vacuum")
        msg2 = self._dic_from_query("pragma integrity_check")
        self.data = str(msg1) + str(msg2)

    def _getsnatched(self):
        cmd = "SELECT * from books,wanted WHERE books.bookid=wanted.bookid "
        cmd += "and books.Status='Snatched' or AudioStatus='Snatched'"
        self.data = self._dic_from_query(cmd)

    def _getlogs(self):
        self.data = lazylibrarian.LOGLIST

    def _logmessage(self, **kwargs):
        if 'level' not in kwargs:
            self.data = 'Missing parameter: level'
            return
        if 'text' not in kwargs:
            self.data = 'Missing parameter: text'
            return
        self.data = kwargs['text']
        if kwargs['level'].upper() == 'INFO':
            logger.info(self.data)
            return
        if kwargs['level'].upper() == 'WARN':
            logger.warn(self.data)
            return
        if kwargs['level'].upper() == 'ERROR':
            logger.error(self.data)
            return
        if kwargs['level'].upper() == 'DEBUG':
            logger.debug(self.data)
            return
        self.data = 'Invalid level: %s' % kwargs['level']
        return

    def _getdebug(self):
        self.data = log_header().replace('\n', '<br>')

    def _getmodules(self):
        lst = ''
        for item in sys.modules:
            lst = lst + "%s: %s<br>" % (item, str(sys.modules[item]).replace('<', '').replace('>', ''))
        self.data = lst

    def _checkmodules(self):
        lst = []
        for item in sys.modules:
            data = str(sys.modules[item]).replace('<', '').replace('>', '')
            for libname in ['apscheduler', 'bs4', 'deluge_client', 'feedparser', 'fuzzywuzzy', 'html5lib',
                            'httplib2', 'mobi', 'oauth2', 'pynma', 'pythontwitter', 'requests',
                            'unrar', 'six', 'webencodings']:
                if libname in data and 'dist-packages' in data:
                    lst.append("%s: %s" % (item, data))
        self.data = lst

    def _clearlogs(self):
        self.data = clear_log()

    def _getindex(self):
        self.data = self._dic_from_query(
            'SELECT * from authors order by AuthorName COLLATE NOCASE')

    def _listlolang(self):
        q = 'SELECT BookID,BookISBN,BookName,AuthorName from books,authors where '
        q += '(BookLang="Unknown" or BookLang="" or BookLang is NULL) and books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _listnogenre(self):
        q = 'SELECT BookID,BookName,AuthorName from books,authors where books.Status != "Ignored" and '
        q += '(BookGenre="Unknown" or BookGenre="" or BookGenre is NULL) and books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _listnodesc(self):
        q = 'SELECT BookID,BookName,AuthorName from books,authors where books.Status != "Ignored" and '
        q += '(BookDesc="" or BookDesc is NULL) and books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _setnodesc(self, **kwargs):
        if 'refresh' in kwargs:
            expire = True
            extra = ' or BookDesc="No Description"'
        else:
            expire = False
            extra = ''
        q = 'SELECT BookID,BookName,AuthorName,BookISBN from books,authors where books.Status != "Ignored" and '
        q += '(BookDesc="" or BookDesc is NULL' + extra + ') and books.AuthorID = authors.AuthorID'
        db = database.DBConnection()
        res = db.select(q)
        descs = 0
        cnt = 0
        logger.debug("Checking description for %s %s" % (len(res), plural(len(res), "book")))
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
                logger.debug("Updated description for %s:%s" % (auth, book))
                db.action('UPDATE books SET bookdesc=? WHERE bookid=?', (data['desc'], item['BookID']))
            elif data is None:  # error, see if it's because we are blocked
                for entry in lazylibrarian.PROVIDER_BLOCKLIST:
                    if entry["name"] == 'googleapis':
                        blocked = True
                        break
                if blocked:
                    break
        msg = "Scanned %d %s, found %d new %s from %d" % \
              (cnt, plural(cnt, "book"), descs, plural(descs, "description"), len(res))
        if blocked:
            msg += ': Access Blocked'
        self.data = msg
        logger.info(self.data)

    def _setnogenre(self, **kwargs):
        if 'refresh' in kwargs:
            expire = True
            extra = ' or BookGenre="Unknown"'
        else:
            expire = False
            extra = ''
        q = 'SELECT BookID,BookName,AuthorName,BookISBN from books,authors where books.Status != "Ignored" and '
        q += '(BookGenre="" or BookGenre is NULL' + extra + ') and books.AuthorID = authors.AuthorID'
        db = database.DBConnection()
        res = db.select(q)
        genre = 0
        cnt = 0
        logger.debug("Checking genre for %s %s" % (len(res), plural(len(res), "book")))
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
                logger.debug("Updated genre for %s:%s [%s]" % (auth, book, newgenre))
                set_genres([newgenre], item['BookID'])
            elif data is None:
                for entry in lazylibrarian.PROVIDER_BLOCKLIST:
                    if entry["name"] == 'googleapis':
                        blocked = True
                        break
                if blocked:
                    break
        msg = "Scanned %d %s, found %d new %s from %d" % (cnt, plural(cnt, "book"), genre,
                                                          plural(genre, "genre"), len(res))
        if blocked:
            msg += ': Access Blocked'
        self.data = msg
        logger.info(self.data)

    def _listnoisbn(self):
        q = 'SELECT BookID,BookName,AuthorName from books,authors where books.AuthorID = authors.AuthorID'
        q += ' and (BookISBN="" or BookISBN is NULL)'
        self.data = self._dic_from_query(q)

    def _listnobooks(self):
        q = 'select authorid,authorname,reason from authors where haveebooks+haveaudiobooks=0 and '
        q += 'reason not like "%Series%" except select authors.authorid,authorname,reason from books,authors where '
        q += 'books.authorid=authors.authorid and books.status=="Wanted";'
        self.data = self._dic_from_query(q)

    def _removenobooks(self):
        self._listnobooks()
        if self.data:
            db = database.DBConnection()
            for auth in self.data:
                logger.debug("Deleting %s" % auth['AuthorName'])
                db.action("DELETE from authors WHERE authorID=?", (auth['AuthorID'],))

    def _listignoredseries(self):
        q = 'SELECT SeriesID,SeriesName from series where Status="Ignored"'
        self.data = self._dic_from_query(q)

    def _listdupebooks(self):
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
        q = 'SELECT BookID,BookName from books where Status="Ignored"'
        self.data = self._dic_from_query(q)

    def _listignoredauthors(self):
        q = 'SELECT AuthorID,AuthorName from authors where Status="Ignored"'
        self.data = self._dic_from_query(q)

    def _listmissingworkpages(self):
        # first the ones with no workpage
        q = 'SELECT BookID from books where length(WorkPage) < 4'
        res = self._dic_from_query(q)
        # now the ones with an error page
        cache = os.path.join(lazylibrarian.CACHEDIR, "WorkCache")
        if path_isdir(cache):
            for cached_file in listdir(cache):
                target = os.path.join(cache, cached_file)
                if path_isfile(target):
                    if os.path.getsize(syspath(target)) < 500 and '.' in cached_file:
                        bookid = cached_file.split('.')[0]
                        res.append({"BookID": bookid})
        self.data = res

    def _getauthor(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']

        author = self._dic_from_query(
            'SELECT * from authors WHERE AuthorID="' + self.id + '"')
        books = self._dic_from_query(
            'SELECT * from books WHERE AuthorID="' + self.id + '"')

        self.data = {'author': author, 'books': books}

    def _getmagazines(self):
        self.data = self._dic_from_query('SELECT * from magazines order by Title COLLATE NOCASE')

    def _getallbooks(self):
        q = 'SELECT authors.AuthorID,AuthorName,AuthorLink,BookName,BookSub,BookGenre,BookIsbn,BookPub,'
        q += 'BookRate,BookImg,BookPages,BookLink,BookID,BookDate,BookLang,BookAdded,books.Status '
        q += 'from books,authors where books.AuthorID = authors.AuthorID'
        self.data = self._dic_from_query(q)

    def _getissues(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        self.id = kwargs['name']
        magazine = self._dic_from_query(
            'SELECT * from magazines WHERE Title="' + self.id + '"')
        issues = self._dic_from_query(
            'SELECT * from issues WHERE Title="' + self.id + '" order by IssueDate DESC')

        self.data = {'magazine': magazine, 'issues': issues}

    def _shrinkmag(self, **kwargs):
        for item in ['name', 'dpi']:
            if item not in kwargs:
                self.data = 'Missing parameter: ' + item
                return
        self.data = ''
        res = shrink_mag(kwargs['name'], check_int(kwargs['dpi'], 0))
        self.data = res

    def _getissuename(self, **kwargs):
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
                if '$Title' in lazylibrarian.CONFIG['MAG_DEST_FILE']:
                    fname = lazylibrarian.CONFIG['MAG_DEST_FILE'].replace('$IssueDate', issuedate).replace(
                        '$Title', title)
                else:
                    fname = lazylibrarian.CONFIG['MAG_DEST_FILE'].replace('$IssueDate', issuedate)
                self.data = os.path.join(dirname, fname + '.' + name_exploded[-1])
            else:
                self.data = "Regex %s [%s] %s" % (regex_pass, issuedate, year)
        else:
            self.data = "Regex %s [%s] %s" % (regex_pass, issuedate, year)

    def _createmagcovers(self, **kwargs):
        refresh = 'refresh' in kwargs
        if 'wait' in kwargs:
            self.data = create_mag_covers(refresh=refresh)
        else:
            threading.Thread(target=create_mag_covers, name='API-MAGCOVERS', args=[refresh]).start()

    def _createmagcover(self, **kwargs):
        if 'file' not in kwargs:
            self.data = 'Missing parameter: file'
            return
        refresh = 'refresh' in kwargs
        if 'page' in kwargs:
            self.data = create_mag_cover(issuefile=kwargs['file'], refresh=refresh, pagenum=kwargs['page'])
        else:
            self.data = create_mag_cover(issuefile=kwargs['file'], refresh=refresh)

    def _getbook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']

        book = self._dic_from_query('SELECT * from books WHERE BookID="' + self.id + '"')
        self.data = {'book': book}

    def _queuebook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            res = db.match('SELECT Status,AudioStatus from books WHERE BookID=?', (kwargs['id'],))
            if not res:
                self.data = "Invalid id: %s" % kwargs['id']
            else:
                if 'type' in kwargs and kwargs['type'] == 'AudioBook':
                    db.action('UPDATE books SET AudioStatus="Wanted" WHERE BookID=?', (kwargs['id'],))
                else:
                    db.action('UPDATE books SET Status="Wanted" WHERE BookID=?', (kwargs['id'],))
                self.data = 'OK'

    def _unqueuebook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            res = db.match('SELECT Status,AudioStatus from books WHERE BookID=?', (kwargs['id'],))
            if not res:
                self.data = "Invalid id: %s" % kwargs['id']
            else:
                if 'type' in kwargs and kwargs['type'] == 'AudioBook':
                    db.action('UPDATE books SET AudioStatus="Skipped" WHERE BookID=?', (kwargs['id'],))
                else:
                    db.action('UPDATE books SET Status="Skipped" WHERE BookID=?', (kwargs['id'],))
                self.data = 'OK'

    def _addmagazine(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        else:
            self.id = kwargs['name']

        db = database.DBConnection()
        control_value_dict = {"Title": self.id}
        new_value_dict = {
            "Regex": None,
            "Status": "Active",
            "MagazineAdded": today(),
            "IssueStatus": "Wanted",
            "Reject": None
        }
        db.upsert("magazines", new_value_dict, control_value_dict)

    def _removemagazine(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        else:
            self.id = kwargs['name']

        db = database.DBConnection()
        db.action('DELETE from magazines WHERE Title=?', (self.id,))
        db.action('DELETE from wanted WHERE BookID=?', (self.id,))

    def _pauseauthor(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
            if not res:
                self.data = "Invalid id: %s" % kwargs['id']
            else:
                db.action('UPDATE authors SET Status="Paused" WHERE AuthorID=?', (kwargs['id'],))
                self.data = 'OK'

    def _ignoreauthor(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
            if not res:
                self.data = "Invalid id: %s" % kwargs['id']
            else:
                db.action('UPDATE authors SET Status="Ignored" WHERE AuthorID=?', (kwargs['id'],))
                self.data = 'OK'

    def _resumeauthor(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            db = database.DBConnection()
            res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
            if not res:
                self.data = "Invalid id: %s" % kwargs['id']
            else:
                db.action('UPDATE authors SET Status="Active" WHERE AuthorID=?', (kwargs['id'],))
                self.data = 'OK'

    def _authorupdate(self):
        try:
            self.data = author_update(restart=False, only_overdue=False)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _seriesupdate(self):
        try:
            self.data = series_update(restart=False, only_overdue=False)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _refreshauthor(self, **kwargs):
        refresh = 'refresh' in kwargs
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        else:
            self.id = kwargs['name']

        try:
            add_author_to_db(self.id, refresh=refresh, reason="API refresh_author %s" % self.id)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _forceactiveauthorsupdate(self, **kwargs):
        refresh = 'refresh' in kwargs
        if 'wait' in kwargs:
            self.data = all_author_update(refresh=refresh)
        else:
            threading.Thread(target=all_author_update, name='API-AAUPDATE', args=[refresh]).start()

    def _forcemagsearch(self, **kwargs):
        if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_rss() or \
                lazylibrarian.use_direct() or lazylibrarian.use_irc():
            if 'wait' in kwargs:
                search_magazines(None, True)
            else:
                threading.Thread(target=search_magazines, name='API-SEARCHMAGS', args=[None, True]).start()
        else:
            self.data = 'No search methods set, check config'

    def _forcersssearch(self, **kwargs):
        if lazylibrarian.use_rss():
            if 'wait' in kwargs:
                search_rss_book()
            else:
                threading.Thread(target=search_rss_book, name='API-SEARCHRSS', args=[]).start()
        else:
            self.data = 'No rss feeds set, check config'

    def _forcecomicsearch(self, **kwargs):
        if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_rss() or \
                lazylibrarian.use_direct() or lazylibrarian.use_irc():
            if 'wait' in kwargs:
                search_comics()
            else:
                threading.Thread(target=search_comics, name='API-SEARCHCOMICS', args=[]).start()
        else:
            self.data = 'No search methods set, check config'

    def _forcewishlistsearch(self, **kwargs):
        if lazylibrarian.use_wishlist():
            if 'wait' in kwargs:
                search_wishlist()
            else:
                threading.Thread(target=search_wishlist, name='API-SEARCHWISHLIST', args=[]).start()
        else:
            self.data = 'No wishlists set, check config'

    def _forcebooksearch(self, **kwargs):
        if 'type' in kwargs:
            library = kwargs['type']
        else:
            library = None
        if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_rss() or \
                lazylibrarian.use_direct() or lazylibrarian.use_irc():
            if 'wait' in kwargs:
                search_book(library=library)
            else:
                threading.Thread(target=search_book, name='API-SEARCHALLBOOK', args=[None, library]).start()
        else:
            self.data = "No search methods set, check config"

    @staticmethod
    def _forceprocess(**kwargs):
        startdir = None
        if 'dir' in kwargs:
            startdir = kwargs['dir']
        ignoreclient = 'ignoreclient' in kwargs
        process_dir(startdir=startdir, ignoreclient=ignoreclient)

    @staticmethod
    def _forcelibraryscan(**kwargs):
        startdir = None
        authid = None
        remove = 'remove' in kwargs
        if 'dir' in kwargs:
            startdir = kwargs['dir']
        if 'id' in kwargs:
            authid = kwargs['id']
        if 'wait' in kwargs:
            library_scan(startdir=startdir, library='eBook', authid=authid, remove=remove)
        else:
            threading.Thread(target=library_scan, name='API-LIBRARYSCAN',
                             args=[startdir, 'eBook', authid, remove]).start()

    @staticmethod
    def _forcecomicscan(**kwargs):
        comicid = None
        if 'id' in kwargs:
            comicid = kwargs['id']
        if 'wait' in kwargs:
            comic_scan(comicid=comicid)
        else:
            threading.Thread(target=comic_scan, name='API-COMICSCAN',
                             args=[comicid]).start()

    @staticmethod
    def _forceaudiobookscan(**kwargs):
        startdir = None
        authid = None
        remove = 'remove' in kwargs
        if 'dir' in kwargs:
            startdir = kwargs['dir']
        if 'id' in kwargs:
            authid = kwargs['id']
        if 'wait' in kwargs:
            library_scan(startdir=startdir, library='AudioBook', authid=authid, remove=remove)
        else:
            threading.Thread(target=library_scan, name='API-LIBRARYSCAN',
                             args=[startdir, 'AudioBook', authid, remove]).start()

    @staticmethod
    def _forcemagazinescan(**kwargs):
        title = None
        if 'title' in kwargs:
            title = kwargs['title']
        if 'wait' in kwargs:
            magazine_scan(title)
        else:
            threading.Thread(target=magazine_scan, name='API-MAGSCAN', args=[title]).start()

    def _deleteemptyseries(self):
        self.data = delete_empty_series()

    def _cleancache(self, **kwargs):
        if 'wait' in kwargs:
            self.data = clean_cache()
        else:
            threading.Thread(target=clean_cache, name='API-CLEANCACHE', args=[]).start()

    def _setworkpages(self, **kwargs):
        if 'wait' in kwargs:
            self.data = set_work_pages()
        else:
            threading.Thread(target=set_work_pages, name='API-SETWORKPAGES', args=[]).start()

    def _setworkid(self, **kwargs):
        ids = None
        if 'bookids' in kwargs:
            ids = kwargs['bookids']
        if 'wait' in kwargs:
            self.data = set_work_id(ids)
        else:
            threading.Thread(target=set_work_id, name='API-SETWORKID', args=[ids]).start()

    def _setallbookseries(self, **kwargs):
        if 'wait' in kwargs:
            self.data = set_all_book_series()
        else:
            threading.Thread(target=set_all_book_series, name='API-SETALLBOOKSERIES', args=[]).start()

    def _setallbookauthors(self, **kwargs):
        if 'wait' in kwargs:
            self.data = set_all_book_authors()
        else:
            threading.Thread(target=set_all_book_authors, name='API-SETALLBOOKAUTHORS', args=[]).start()

    def _getbookcovers(self, **kwargs):
        if 'wait' in kwargs:
            self.data = get_book_covers()
        else:
            threading.Thread(target=get_book_covers, name='API-GETBOOKCOVERS', args=[]).start()

    def _getauthorimages(self, **kwargs):
        if 'wait' in kwargs:
            self.data = get_author_images()
        else:
            threading.Thread(target=get_author_images, name='API-GETAUTHORIMAGES', args=[]).start()

    def _getversion(self):
        self.data = {
            'install_type': lazylibrarian.CONFIG['INSTALL_TYPE'],
            'current_version': lazylibrarian.CONFIG['CURRENT_VERSION'],
            'latest_version': lazylibrarian.CONFIG['LATEST_VERSION'],
            'commits_behind': lazylibrarian.CONFIG['COMMITS_BEHIND'],
        }

    @staticmethod
    def _shutdown():
        lazylibrarian.SIGNAL = 'shutdown'

    @staticmethod
    def _restart():
        lazylibrarian.SIGNAL = 'restart'

    @staticmethod
    def _update():
        lazylibrarian.SIGNAL = 'update'

    def _findauthorid(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        authorname = format_author_name(kwargs['name'])
        gr = GoodReads(authorname)
        self.data = gr.find_author_id()

    def _findauthor(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return

        authorname = format_author_name(kwargs['name'])
        if lazylibrarian.CONFIG['BOOK_API'] == "GoogleBooks":
            gb = GoogleBooks(authorname)
            myqueue = queue.Queue()
            search_api = threading.Thread(target=gb.find_results, name='API-GBRESULTS', args=[authorname, myqueue])
            search_api.start()
        elif lazylibrarian.CONFIG['BOOK_API'] == "GoodReads":
            gr = GoodReads(authorname)
            myqueue = queue.Queue()
            search_api = threading.Thread(target=gr.find_results, name='API-GRRESULTS', args=[authorname, myqueue])
            search_api.start()
        else:  # if lazylibrarian.CONFIG['BOOK_API'] == "OpenLibrary":
            ol = OpenLibrary(authorname)
            myqueue = queue.Queue()
            search_api = threading.Thread(target=ol.find_results, name='API-OLRESULTS', args=[authorname, myqueue])
            search_api.start()

        search_api.join()
        self.data = myqueue.get()

    def _findbook(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return

        if lazylibrarian.CONFIG['BOOK_API'] == "GoogleBooks":
            gb = GoogleBooks(kwargs['name'])
            myqueue = queue.Queue()
            search_api = threading.Thread(target=gb.find_results, name='API-GBRESULTS', args=[kwargs['name'], myqueue])
            search_api.start()
        elif lazylibrarian.CONFIG['BOOK_API'] == "GoodReads":
            gr = GoodReads(kwargs['name'])
            myqueue = queue.Queue()
            search_api = threading.Thread(target=gr.find_results, name='API-GRRESULTS', args=[kwargs['name'], myqueue])
            search_api.start()
        else:  # if lazylibrarian.CONFIG['BOOK_API'] == "OpenLibrary":
            ol = OpenLibrary(kwargs['name'])
            myqueue = queue.Queue()
            search_api = threading.Thread(target=ol.find_results, name='API-OLRESULTS', args=[kwargs['name'], myqueue])
            search_api.start()

        search_api.join()
        self.data = myqueue.get()

    def _addbook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return

        if lazylibrarian.CONFIG['BOOK_API'] == "GoogleBooks":
            gb = GoogleBooks(kwargs['id'])
            threading.Thread(target=gb.find_book, name='API-GBRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()
        elif lazylibrarian.CONFIG['BOOK_API'] == "GoodReads":
            gr = GoodReads(kwargs['id'])
            threading.Thread(target=gr.find_book, name='API-GRRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()
        elif lazylibrarian.CONFIG['BOOK_API'] == "OpenLibrary":
            ol = OpenLibrary(kwargs['id'])
            threading.Thread(target=ol.find_book, name='API-OLRESULTS', args=[kwargs['id'],
                                                                              None, None, "Added by API"]).start()

    def _movebook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        if 'toid' not in kwargs:
            self.data = 'Missing parameter: toid'
            return
        try:
            db = database.DBConnection()
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
            logger.debug(self.data)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _movebooks(self, **kwargs):
        if 'fromname' not in kwargs:
            self.data = 'Missing parameter: fromname'
            return
        if 'toname' not in kwargs:
            self.data = 'Missing parameter: toname'
            return
        try:
            db = database.DBConnection()
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
                    self.data = "Moved %s books from %s to %s" % (len(fromhere), kwargs['fromname'], kwargs['toname'])
                    update_totals(fromhere[0][1])  # we moved from here
                    update_totals(tohere[0])  # to here

            logger.debug(self.data)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _comicmeta(self, **kwargs):
        if 'name' in kwargs:
            name = kwargs['name']
        else:
            self.data = 'Missing parameter: name'
            return
        xml = 'xml' in kwargs
        self.data = comic_metadata(name, xml=xml)

    def _comicid(self, **kwargs):
        if 'name' in kwargs:
            name = kwargs['name']
        else:
            self.data = 'Missing parameter: name'
            return
        if 'source' not in kwargs:
            self.data = 'Missing parameter: source'
            return
        else:
            source = kwargs['source']
            if source not in ['cv', 'cx']:
                self.data = 'Invalid parameter: source'
                return
        best = 'best' in kwargs
        if source == 'cv':
            self.data = cv_identify(name, best=best)
        else:
            self.data = cx_identify(name, best=best)

    def _addauthor(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        else:
            self.id = kwargs['name']
        try:
            self.data = add_author_name_to_db(author=self.id, refresh=False, reason="API add_author %s" % self.id)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _addauthorid(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        try:
            self.data = add_author_to_db(refresh=False, authorid=self.id, reason="API add_author_id %s" % self.id)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grfollowall(self):
        db = database.DBConnection()
        cmd = 'SELECT AuthorName,AuthorID,GRfollow FROM authors where '
        cmd += 'Status="Active" or Status="Wanted" or Status="Loading"'
        authors = db.select(cmd)
        count = 0
        for author in authors:
            followid = check_int(author['GRfollow'], 0)
            if followid > 0:
                logger.debug('%s is already followed' % author['AuthorName'])
            elif author['GRfollow'] == "0":
                logger.debug('%s is manually unfollowed' % author['AuthorName'])
            else:
                res = grfollow(author['AuthorID'], True)
                if res.startswith('Unable'):
                    logger.warn(res)
                try:
                    followid = res.split("followid=")[1]
                    logger.debug('%s marked followed' % author['AuthorName'])
                    count += 1
                except IndexError:
                    followid = ''
                db.action('UPDATE authors SET GRfollow=? WHERE AuthorID=?', (followid, author['AuthorID']))
        self.data = "Added follow to %s %s" % (count, plural(count, "author"))

    def _grsync(self, **kwargs):
        if 'shelf' not in kwargs:
            self.data = 'Missing parameter: shelf'
            return
        if 'status' not in kwargs:
            self.data = 'Missing parameter: status'
            return
        library = 'eBook'
        if 'library' in kwargs:
            library = kwargs['library']
        reset = 'reset' in kwargs
        try:
            self.data = grsync(kwargs['status'], kwargs['shelf'], library, reset)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grfollow(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        try:
            self.data = grfollow(authorid=kwargs['id'], follow=True)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _grunfollow(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        try:
            self.data = grfollow(authorid=kwargs['id'], follow=False)
        except Exception as e:
            self.data = "%s %s" % (type(e).__name__, str(e))

    def _searchitem(self, **kwargs):
        if 'item' not in kwargs:
            self.data = 'Missing parameter: item'
            return
        else:
            self.data = search_item(kwargs['item'])

    def _searchbook(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return

        books = [{"bookid": kwargs['id']}]
        if 'type' in kwargs:
            library = kwargs['type']
        else:
            library = None

        if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_rss() or \
                lazylibrarian.use_direct() or lazylibrarian.use_irc():
            if 'wait' in kwargs:
                search_book(books=books, library=library)
            else:
                threading.Thread(target=search_book, name='API-SEARCHBOOK', args=[books, library]).start()
        else:
            self.data = "No search methods set, check config"

    def _removeauthor(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']

        db = database.DBConnection()
        authorsearch = db.select('SELECT AuthorName from authors WHERE AuthorID=?', (kwargs['id'],))
        if len(authorsearch):  # to stop error if try to remove an author while they are still loading
            author_name = authorsearch[0]['AuthorName']
            logger.debug("Removing all references to author: %s" % author_name)
            db.action('DELETE from authors WHERE AuthorID=?', (kwargs['id'],))

    def _writecfg(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        if 'value' not in kwargs:
            self.data = 'Missing parameter: value'
            return
        if 'group' not in kwargs:
            self.data = 'Missing parameter: group'
            return
        try:
            self.data = '["%s"]' % lazylibrarian.CFG.get(kwargs['group'], kwargs['name'])
            lazylibrarian.CFG.set(kwargs['group'], kwargs['name'], kwargs['value'])
            if PY2:
                fmode = 'wb'
            else:
                fmode = 'w'
            with open(syspath(lazylibrarian.CONFIGFILE), fmode) as configfile:
                lazylibrarian.CFG.write(configfile)
            lazylibrarian.config_read(reloaded=True)
        except Exception as e:
            self.data = 'Unable to update CFG entry for %s: %s, %s' % (kwargs['group'], kwargs['name'], str(e))

    def _readcfg(self, **kwargs):
        if 'name' not in kwargs:
            self.data = 'Missing parameter: name'
            return
        if 'group' not in kwargs:
            self.data = 'Missing parameter: group'
            return
        try:
            self.data = '["%s"]' % lazylibrarian.CFG.get(kwargs['group'], kwargs['name'])
        except configparser.Error:
            self.data = 'No CFG entry for %s: %s' % (kwargs['group'], kwargs['name'])

    @staticmethod
    def _loadcfg():
        lazylibrarian.config_read(reloaded=True)

    def _getseriesauthors(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
        else:
            self.id = kwargs['id']
            count = get_series_authors(self.id)
            self.data = "Added %s" % count

    def _addseriesmembers(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        self.data = add_series_members(self.id)

    def _getseriesmembers(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        self.data = get_series_members(self.id)

    def _getbookauthors(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        self.data = get_book_authors(self.id)

    def _getworkseries(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        self.data = get_work_series(self.id, reason="API get_work_series")

    def _getworkpage(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        self.data = get_work_page(self.id)

    def _getbookcover(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        if 'src' in kwargs:
            self.data = get_book_cover(self.id, kwargs['src'])
        else:
            self.data = get_book_cover(self.id)

    def _getauthorimage(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        refresh = 'refresh' in kwargs
        if 'max' in kwargs:
            max_num = kwargs['max']
        else:
            max_num = 1
        self.data = get_author_image(self.id, refresh=refresh, max_num=max_num)

    def _lock(self, table, itemid, state):
        db = database.DBConnection()
        dbentry = db.match('SELECT %sID from %ss WHERE %sID=%s' % (table, table, table, itemid))
        if dbentry:
            db.action('UPDATE %ss SET Manual="%s" WHERE %sID=%s' % (table, state, table, itemid))
        else:
            self.data = "%sID %s not found" % (table, itemid)

    def _setauthorlock(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self._lock("author", kwargs['id'], "1")

    def _setauthorunlock(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self._lock("author", kwargs['id'], "0")

    def _setauthorimage(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        if 'img' not in kwargs:
            self.data = 'Missing parameter: img'
            return
        else:
            self._setimage("author", kwargs['id'], kwargs['img'])

    def _setbookimage(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self.id = kwargs['id']
        if 'img' not in kwargs:
            self.data = 'Missing parameter: img'
            return
        else:
            self._setimage("book", kwargs['id'], kwargs['img'])

    def _setimage(self, table, itemid, img):
        msg = "%s Image [%s] rejected" % (table, img)
        # Cache file image
        if path_isfile(img):
            extn = os.path.splitext(img)[1].lower()
            if extn and extn in ['.jpg', '.jpeg', '.png']:
                destfile = os.path.join(lazylibrarian.CACHEDIR, table, itemid + '.jpg')
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
                _, success, _ = cache_img(table, itemid, img)
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
        dbentry = db.match('SELECT %sID from %ss WHERE %sID=%s' % (table, table, table, itemid))
        if dbentry:
            db.action('UPDATE %ss SET %sImg="%s" WHERE %sID=%s' %
                      (table, table, 'cache' + os.path.sep + itemid + '.jpg', table, itemid))
        else:
            self.data = "%sID %s not found" % (table, itemid)

    def _setbooklock(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self._lock("book", kwargs['id'], "1")

    def _setbookunlock(self, **kwargs):
        if 'id' not in kwargs:
            self.data = 'Missing parameter: id'
            return
        else:
            self._lock("book", kwargs['id'], "0")

    @staticmethod
    def _restartjobs():
        restart_jobs(start='Restart')

    @staticmethod
    def _checkrunningjobs():
        check_running_jobs()

    def _showjobs(self):
        self.data = show_jobs()

    def _showstats(self):
        self.data = show_stats()

    def _importalternate(self, **kwargs):
        if 'dir' in kwargs:
            usedir = kwargs['dir']
        else:
            usedir = lazylibrarian.CONFIG['ALTERNATE_DIR']
        if 'library' in kwargs:
            library = kwargs['library']
        else:
            library = 'eBook'
        if 'wait' in kwargs:
            self.data = process_alternate(usedir, library)
        else:
            threading.Thread(target=process_alternate, name='API-IMPORTALT', args=[usedir, library]).start()

    def _includealternate(self, **kwargs):
        if 'dir' in kwargs:
            startdir = kwargs['dir']
        else:
            startdir = lazylibrarian.CONFIG['ALTERNATE_DIR']
        if 'library' in kwargs:
            library = kwargs['library']
        else:
            library = 'eBook'
        if 'wait' in kwargs:
            self.data = library_scan(startdir, library, None, False)
        else:
            threading.Thread(target=library_scan, name='API-INCLUDEALT',
                             args=[startdir, library, None, False]).start()

    def _importcsvwishlist(self, **kwargs):
        if 'dir' in kwargs:
            usedir = kwargs['dir']
        else:
            usedir = lazylibrarian.CONFIG['ALTERNATE_DIR']
        if 'wait' in kwargs:
            self.data = import_csv(usedir)
        else:
            threading.Thread(target=import_csv, name='API-IMPORTCSV', args=[usedir]).start()

    def _exportcsvwishlist(self, **kwargs):
        if 'dir' in kwargs:
            usedir = kwargs['dir']
        else:
            usedir = lazylibrarian.CONFIG['ALTERNATE_DIR']
        if 'wait' in kwargs:
            self.data = export_csv(usedir)
        else:
            threading.Thread(target=export_csv, name='API-EXPORTCSV', args=[usedir]).start()
