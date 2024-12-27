import json
import logging
import os
import platform
import re
import time
import traceback
import http.client
import cherrypy
import lazylibrarian
import requests
from lazylibrarian import database
from lazylibrarian.blockhandler import BLOCKHANDLER
from lazylibrarian.bookwork import get_status, isbn_from_words, thinglang
from lazylibrarian.common import get_readinglist, set_readinglist
from lazylibrarian.config2 import CONFIG
from lazylibrarian.filesystem import DIRS, path_isfile, syspath
from lazylibrarian.formatter import md5_utf8, make_unicode, is_valid_isbn, get_list, format_author_name, \
    date_format, thread_name, now, today, plural, unaccented, replace_all, check_year, check_int
from lazylibrarian.images import cache_bookimg, get_book_cover
try:
    from rapidfuzz import fuzz
except ModuleNotFoundError:
    from thefuzz import fuzz


def hc_api_sleep():
    time_now = time.time()
    delay = time_now - lazylibrarian.TIMERS['LAST_HC']
    if delay < 2.0:
        sleep_time = 2.0 - delay
        lazylibrarian.TIMERS['SLEEP_HC'] += sleep_time
        cachelogger = logging.getLogger('special.cache')
        cachelogger.debug("HardCover sleep %.3f, total %.3f" % (sleep_time, lazylibrarian.TIMERS['SLEEP_HC']))
        time.sleep(sleep_time)
    lazylibrarian.TIMERS['LAST_HC'] = time_now


def hc_sync(library='', userid=None):
    msg = ''
    # TODO currently this only syncs one user as hardcover doesn't yet allow access to other users lists
    if not userid:
        db = database.DBConnection()
        user = db.match("select distinct userid from sync where label like 'hc_%'")
        db.close()
        if not user or not user[0]:
            msg = 'No users with HardCover sync enabled, trying current userid'
            cookie = cherrypy.request.cookie
            if 'll_uid' in list(cookie.keys()):
                userid = cookie['ll_uid'].value
            else:
                userid = ''
                msg = 'No current userid'
        else:
            userid = user[0]
    if userid:
        hc = HardCover(userid)
        msg = hc.sync(library, userid)
    return msg


def validate_bookdict(bookdict):
    logger = logging.getLogger(__name__)
    if not bookdict.get('auth_name') or bookdict.get('auth_name') == 'Unknown':
        rejected = 'name', "Authorname Unknown"
        logger.debug('Rejecting %s, %s' % (bookdict.get('title'), rejected[1]))
        return rejected

    # these are reject reasons we might want to override, so optionally add to database as "ignored"
    ignorable = ['future', 'date', 'isbn', 'word', 'set']
    if CONFIG.get_bool('NO_LANG'):
        ignorable.append('lang')
    rejected = False

    db = database.DBConnection()
    try:
        wantedlanguages = get_list(CONFIG['IMP_PREFLANG'])
        if wantedlanguages and 'All' not in wantedlanguages:
            lang = ''
            languages = get_list(bookdict.get('languages'))
            if languages:
                for item in languages:
                    if item in wantedlanguages:
                        lang = item
                        break
                if not lang:
                    rejected = 'lang', 'Invalid language: %s' % str(languages)
            elif bookdict.get('isbn'):
                # Try to use shortcut of ISBN identifier codes described here...
                # http://en.wikipedia.org/wiki/List_of_ISBN_identifier_groups
                if len(bookdict['isbn']) == 10:
                    isbnhead = bookdict['isbn'][0:3]
                elif len(bookdict['isbn']) == 13:
                    isbnhead = bookdict['isbn'][3:6]
                else:
                    isbnhead = ''

                if isbnhead == '979':
                    for item in lazylibrarian.isbn_979_dict:
                        if isbnhead.startswith(item):
                            lang = lazylibrarian.isbn_979_dict[item]
                            break
                        if lang != "Unknown":
                            logger.debug("ISBN979 returned %s for %s" % (lang, isbnhead))
                elif isbnhead == '978':
                    for item in lazylibrarian.isbn_978_dict:
                        if isbnhead.startswith(item):
                            lang = lazylibrarian.isbn_978_dict[item]
                            break
                    if lang != "Unknown":
                        logger.debug("ISBN978 returned %s for %s" % (lang, isbnhead))
                    if lang == "Unknown" and isbnhead:
                        # Nothing in the isbn dictionary, try any cached results
                        match = db.match('SELECT lang FROM languages where isbn=?', (isbnhead,))
                        if match:
                            lang = match['lang']
                            logger.debug("Found cached language [%s] for %s [%s]" %
                                         (lang, bookdict['title'], isbnhead))
                    else:
                        lang = thinglang(bookdict['isbn'])
                        if lang:
                            db.action('insert into languages values (?, ?)', (isbnhead, lang))

                if lang and lang not in wantedlanguages:
                    rejected = 'lang', 'Invalid language: %s' % lang

                if not lang:
                    if "Unknown" not in wantedlanguages:
                        rejected = 'lang', 'No language'

            if not rejected and not bookdict['title']:
                rejected = 'name', 'No title'
            if not rejected and bookdict['publishers']:
                for bookpub in bookdict['publishers']:
                    if bookpub.lower() in get_list(CONFIG['REJECT_PUBLISHER']):
                        rejected = 'publisher', bookpub
                        break

            cmd = ("SELECT BookID,books.hc_id FROM books,authors WHERE books.AuthorID = authors.AuthorID and "
                   "BookName=? COLLATE NOCASE and AuthorName=? COLLATE NOCASE and books.Status != 'Ignored' "
                   "and AudioStatus != 'Ignored'")
            exists = db.match(cmd, (bookdict['title'], bookdict['auth_name']))
            if not exists:
                in_db = lazylibrarian.librarysync.find_book_in_db(bookdict['auth_name'], bookdict['title'],
                                                                  source='hc_id', ignored=False, library='eBook',
                                                                  reason='hc_get_author_books %s,%s' %
                                                                  (bookdict['auth_id'], bookdict['title']))
                if not in_db:
                    in_db = lazylibrarian.librarysync.find_book_in_db(bookdict['auth_name'], bookdict['title'],
                                                                      source='bookid', ignored=False, library='eBook',
                                                                      reason='hc_get_author_books %s,%s' %
                                                                      (bookdict['auth_id'], bookdict['title']))
                if in_db and in_db[0]:
                    cmd = "SELECT BookID,hc_id FROM books WHERE BookID=?"
                    exists = db.match(cmd, (in_db[0],))

            if exists and not rejected:
                # existing bookid might not still be listed at this source so won't refresh.
                # should we keep new bookid or existing one?
                # existing one might have been user edited, might be locked,
                # might have been merged from another authorid or inherited from goodreads?
                # Should probably use the one with the "best" info but since we don't know
                # which that is, keep the old one which is already linked to other db tables
                # but allow info (dates etc.) to be updated
                if bookdict['bookid'] != exists['BookID']:
                    rejected = 'dupe', 'Duplicate id (%s/%s)' % (bookdict['bookid'], exists['BookID'])
                    if not exists['hc_id']:
                        db.action("UPDATE books SET hc_id=? WHERE BookID=?", (bookdict['bookid'], exists['BookID']))

            if not rejected and bookdict['isbn'] and CONFIG.get_bool('ISBN_LOOKUP'):
                # try isbn lookup by name
                title = bookdict.get('title')
                if title:
                    try:
                        res = isbn_from_words(unaccented(title, only_ascii=False) + ' ' +
                                              unaccented(bookdict['auth_name'], only_ascii=False))
                    except Exception as e:
                        res = None
                        logger.warning("Error from isbn: %s" % e)
                    if res:
                        logger.debug("isbn found %s for %s" % (res, bookdict['bookid']))
                        if len(res) in [10, 13]:
                            bookdict['isbn'] = res

            if not rejected and not bookdict['isbn'] and CONFIG.get_bool('NO_ISBN'):
                rejected = 'isbn', 'No ISBN'

            if not rejected:
                dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '',
                       '[': ' ', ']': ' ', '#': '# ', ':': ' ', ';': ' '}
                name = replace_all(bookdict['title'], dic).strip()
                name = name.lower()
                # remove extra spaces if they're in a row
                name = " ".join(name.split())
                namewords = name.split(' ')
                badwords = get_list(CONFIG['REJECT_WORDS'], ',')
                for word in badwords:
                    if (' ' in word and word in name) or word in namewords:
                        rejected = 'word', 'Contains [%s]' % word
                        break

            if not rejected:
                book_name = unaccented(bookdict['title'], only_ascii=False)
                if CONFIG.get_bool('NO_SETS'):
                    # allow date ranges eg 1981-95
                    m = re.search(r'(\d+)-(\d+)', book_name)
                    if m:
                        if check_year(m.group(1), past=1800, future=0):
                            logger.debug("Allow %s, looks like a date range" % book_name)
                        else:
                            rejected = 'set', 'Set or Part %s' % m.group(0)
                    if re.search(r'\d+ of \d+', book_name) or \
                            re.search(r'\d+/\d+', book_name) and not re.search(r'\d+/\d+/\d+', book_name):
                        rejected = 'set', 'Set or Part'
                    elif re.search(r'\w+\s*/\s*\w+', book_name):
                        rejected = 'set', 'Set or Part'
                    if rejected:
                        logger.debug('Rejected %s, %s' % (book_name, rejected[1]))
                if rejected and rejected[0] not in ignorable:
                    logger.debug('Rejecting %s, %s' % (bookdict['title'], rejected[1]))
            elif rejected and not (rejected[0] in ignorable and CONFIG.get_bool('IMP_IGNORE')):
                logger.debug('Rejecting %s, %s' % (bookdict['title'], rejected[1]))
            else:
                logger.debug("Found title: %s" % bookdict['title'])
                if not rejected and CONFIG.get_bool('NO_FUTURE'):
                    publish_date = bookdict.get('publish_date')
                    if publish_date > today()[:len(publish_date)]:
                        rejected = 'future', 'Future publication date [%s]' % publish_date
                        if ignorable is None:
                            logger.debug('Rejecting %s, %s' % (bookdict['title'], rejected[1]))
                        else:
                            logger.debug("Not rejecting %s (future pub date %s) as %s" %
                                         (bookdict['title'], publish_date, ignorable))
                    if not rejected and CONFIG.get_bool('NO_PUBDATE'):
                        if not publish_date or publish_date == '0000':
                            rejected = 'date', 'No publication date'
                    if ignorable is None:
                        logger.debug('Rejecting %s, %s' % (bookdict['title'], rejected[1]))
                    else:
                        logger.debug("Not rejecting %s (no pub date) as %s" %
                                     (bookdict['title'], ignorable))
    except Exception:
        logger.error('Unhandled exception in validate_bookdict: %s' % traceback.format_exc())
        logger.error(f"{bookdict}")
    finally:
        db.close()
        return rejected


class HardCover:
    def __init__(self, name=''):

        self.hc_url = 'https://api.hardcover.app/'
        self.graphql_url = self.hc_url + 'v1/graphql'
        self.book_url = self.hc_url.replace('api.', '') + 'books/'
        self.auth_url = self.hc_url.replace('api.', '') + 'authors/'
        self.HC_WHOAMI = 'query whoami { me { id } }'

#       user_id = result of whoami/userid
#       status_id = 1 want-to-read, 2 currently_reading, 3 read, 4 owned, 5 dnf
        self.HC_USERBOOKS = '''
            query mybooks { user_books(order_by: {date_added: desc} where:
              {status_id: {_eq: [status]}, user_id: {_eq: [whoami]}})
              {
                id
                book {
                       id
                       title
                }
              }
            }
'''
        self.HC_FINDBOOK = '''
query FindBook { books([order] where: [where])
   {
    title
    id
    cached_image
    description
    rating
    ratings_count
    pages
    subtitle
    slug
    release_date
    release_year
    cached_tags
    contributions(order_by: {author: {}, contribution: desc}) {
      author {
        name
        id
      }
    }
    editions {
      isbn_10
      isbn_13
      publisher {
        name
      }
      language {
        language
      }
    }
    book_series {
      position
      series {
        name
        id
      }
    }
  }
}
'''

        self.HC_FINDAUTHORID = '''
query FindAuthorID {
  authors(order_by: {books_count: desc} where: {name: {_eq: "[authorname]"}}) {
    id
    name
    books_count
  }
}
'''
        self.HC_FINDAUTHORBYNAME = '''
query FindAuthorByName {
    search(query: "[authorname]", query_type: "author") {
    results
  }
}
'''
        self.HC_FINDBOOKBYNAME = '''
query FindBookByName {
    search(query: "[title]", query_type: "book") {
    results
  }
}
'''

        self.HC_ISBN13_BOOKS = self.HC_FINDBOOK.replace('[where]', '{isbn_13: {_eq: "[isbn]"}}'
                                                        ).replace('[order]', '')
        self.HC_ISBN10_BOOKS = self.HC_FINDBOOK.replace('[where]', '{isbn_10: {_eq: "[isbn]"}}'
                                                        ).replace('[order]', '')
        self.HC_BOOKID_BOOKS = self.HC_FINDBOOK.replace('[where]', '{id: {_eq: [bookid]}}'
                                                        ).replace('[order]', '')
        self.HC_AUTHORID_BOOKS = self.HC_FINDBOOK.replace('[where]',
                                                          '{contributions: {author: {id: {_eq: "[authorid]"}}}}'
                                                          ).replace('[order]', '')
        self.HC_BOOK_SERIES = '''
query FindSeries { book_series(where: {series_id: {_eq: [seriesid]}})
    {
      position
      series {
        name
        id
      }
      book {
        id
        title
        release_date
        release_year
        contributions {
          author {
            id
            name
          }
        }
        editions {
          language {
            language
          }
        }
      }
    }
}
'''
        self.HC_EDITIONS = '''
query FindEdition { editions(where: {book_id: {_eq: [bookid]}})
  {
    isbn_10
    isbn_13
    language {
      language
    }
    title
    publisher {
      name
    }
  }
}
'''
# cached_image in authors is a book image, not author??
        self.HC_AUTHORINFO = '''
query FindAuthor { authors(where: {id: {_eq: [authorid]}})
  {
    id
    name
    death_year
    death_date
    born_year
    born_date
    bio
  }
}'''

        self.HC_ADDUSERBOOK = '''
    mutation AddUserBook { insert_user_book (object: {book_id: [bookid], status_id: [status]}) { id, error }}
'''
        self.HC_DELUSERBOOK = '''
    mutation DelUserBook { delete_user_book (id: [bookid]) { id }}
'''

        self.name = make_unicode(name)
        self.title = ''
        if '<ll>' in self.name:
            self.name, self.title = self.name.split('<ll>')
        self.lt_cache = False
        self.logger = logging.getLogger(__name__)
        self.searchinglogger = logging.getLogger('special.searching')
        self.syncinglogger = logging.getLogger('special.grsync')
        self.matchinglogger = logging.getLogger('special.matching')
        self.cachelogger = logging.getLogger('special.cache')
        self.provider = "HardCover"
        self.user_agent = 'LazyLibrarian ('
        if CONFIG['CURRENT_VERSION']:
            self.user_agent += CONFIG['CURRENT_VERSION']
        else:
            self.user_agent += platform.system() + ' ' + platform.release()
        self.user_agent += ')'

    def is_in_cache(self, expiry: int, hashfilename: str, myhash: str) -> bool:
        if path_isfile(hashfilename):
            cache_modified_time = os.stat(hashfilename).st_mtime
            time_now = time.time()
            if expiry and cache_modified_time < time_now - expiry:
                # Cache entry is too old, delete it
                self.cachelogger.debug("Expiring %s" % myhash)
                os.remove(syspath(hashfilename))
                return False
            else:
                return True
        else:
            return False

    @staticmethod
    def read_from_cache(hashfilename: str) -> (str, bool):
        with open(syspath(hashfilename), "rb") as cachefile:
            source = cachefile.read()
        return source, True

    @staticmethod
    def get_hashed_filename(cache_location: str, url: str) -> (str, str):
        myhash = md5_utf8(url)
        hashfilename = os.path.join(cache_location, myhash[0], myhash[1], myhash + ".json")
        return hashfilename, myhash

    def result_from_cache(self, searchcmd: str, refresh=False) -> (str, bool):
        headers = {'Content-Type': 'application/json',
                   'User-Agent': self.user_agent,
                   'authorization': CONFIG.get_str('HC_API')
                   }
        query = {'query': searchcmd}
        cache_location = DIRS.get_cachedir('JSONCache')
        filename = self.graphql_url + '/' + str(query)
        hashfilename, myhash = self.get_hashed_filename(cache_location, filename)
        # CACHE_AGE is in days, so get it to seconds
        expire_older_than = CONFIG.get_int('CACHE_AGE') * 24 * 60 * 60
        valid_cache = self.is_in_cache(expire_older_than, hashfilename, myhash)
        if valid_cache and not refresh:
            lazylibrarian.CACHE_HIT += 1
            self.cachelogger.debug("CacheHandler: Returning CACHED response %s" % (hashfilename,))
            source, ok = self.read_from_cache(hashfilename)
            if ok:
                res = json.loads(source)
            else:
                res = {}
        else:
            lazylibrarian.CACHE_MISS += 1
            if BLOCKHANDLER.is_blocked(self.provider):
                return {}, False
            hc_api_sleep()
            try:
                http.client.HTTPConnection.debuglevel = 1 if lazylibrarian.REQUESTSLOG else 0
                r = requests.post(self.graphql_url, json=query, headers=headers)
                success = str(r.status_code).startswith('2')
            except requests.exceptions.ConnectionError as e:
                self.logger.error(str(e))
                success = False
                r = None
            if success:
                res = r.json()
                self.cachelogger.debug("CacheHandler: Storing %s %s" % ('json', myhash))
                with open(syspath(hashfilename), "w") as cachefile:
                    cachefile.write(json.dumps(res))
            else:
                res = {}
                self.logger.error('Access forbidden. Please wait a while before trying %s again.' % self.provider)
                try:
                    msg = str(r.status_code)
                except Exception:
                    msg = "Unknown reason"
                BLOCKHANDLER.block_provider(self.provider, msg, delay=10)
        return res, valid_cache

    def get_series_members(self, series_ident=None, series_title='', queue=None, refresh=False):
        resultlist = []
        resultdict = {}
        ser_name = ''
        series_id = ''
        author_name = ''
        api_hits = 0
        cache_hits = 0
        searchcmd = self.HC_BOOK_SERIES.replace('[seriesid]', str(series_ident)[2:])
        results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
        api_hits += not in_cache
        cache_hits += in_cache
        if 'errors' in results:
            self.logger.error(str(results['errors']))
        if 'data' in results and 'book_series' in results['data']:
            for entry in results['data']['book_series']:
                series_id = 'HC' + str(entry['series']['id'])
                if series_id != series_ident:
                    self.logger.debug("Series id mismatch for %s, %s" %
                                      (series_id, series_ident))
                else:
                    ser_name = entry['series']['name']
                    if not ser_name:
                        ser_name = series_title
                    if ser_name != series_title:
                        match = fuzz.partial_ratio(ser_name, series_title)
                        if match < 95:
                            self.logger.debug("Series name mismatch for %s, %s%% %s/%s" %
                                              (series_id, match, ser_name, series_title))
                        else:
                            ser_name = series_title

                if ser_name == series_title and series_id == series_ident:
                    position = entry['position']
                    if not position or str(position) == 'None':
                        position = 0
                    book_title = entry['book']['title']
                    workid = entry['book']['id']
                    authorname = entry['book']['contributions'][0]['author']['name']
                    authorlink = entry['book']['contributions'][0]['author']['id']
                    pubyear = entry['book']['release_year']
                    pubdate = entry['book']['release_date']
                    editions = entry['book']['editions']
                    languages = []
                    for lang in editions:
                        if lang.get('language'):
                            res = lang['language']
                            languages.append(res.get('language'))
                    languages = set(languages)

                    if not author_name:
                        author_name = authorname

                    if position and (position not in resultdict or resultdict[position][1] != author_name):
                        valid_lang = False
                        if not languages:
                            valid_lang = True
                        else:
                            wantedlanguages = get_list(CONFIG['IMP_PREFLANG'])
                            for lang in languages:
                                if lang in wantedlanguages:
                                    valid_lang = True
                                    break
                        if valid_lang:
                            resultdict[position] = [book_title, authorname, authorlink, workid, pubyear, pubdate]
            for item in resultdict:
                res = [item]
                res.extend(resultdict[item])
                resultlist.append(res)
            resultlist = sorted(resultlist)
            self.logger.debug("Found %s for series %s: %s" % (len(resultlist), series_id, ser_name))
            self.logger.debug("Used %s api hit, %s in cache" % (api_hits, cache_hits))

        if queue:
            queue.put(resultlist)
            return
        return resultlist

    def find_results(self, searchterm=None, queue=None, refresh=False):
        # noinspection PyBroadException
        try:
            resultlist = []
            resultcount = 0
            api_hits = 0
            cache_hits = 0
            searchtitle = ''
            searchauthorname = ''
            searchcmd = ''
            resultbooks = []
            authids = []
            if is_valid_isbn(searchterm):
                if len(searchterm) == 13:
                    searchcmd = self.HC_ISBN13_BOOKS.replace('[isbn]', searchterm)
                else:
                    searchcmd = self.HC_ISBN10_BOOKS.replace('[isbn]', searchterm)
                results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
                api_hits += not in_cache
                cache_hits += in_cache
                try:
                    resultbooks = results['data']['books']
                except (IndexError, KeyError):
                    pass

            if not searchcmd:  # not isbn search, could be author, title, both
                if ' <ll> ' in searchterm:  # special token separates title from author
                    searchtitle, searchauthorname = searchterm.split(' <ll> ')
                    searchterm = searchterm.replace(' <ll> ', ' ').strip()
                    searchtitle = searchtitle.split(' (')[0].strip()  # without any series info
                else:
                    # could be either... At the moment the HardCover book search covers both
                    # author and title, but in future we may need two searches
                    searchtitle = searchterm
                    searchauthorname = None

                if searchtitle:
                    searchcmd = self.HC_FINDBOOKBYNAME.replace('[title]', searchtitle)
                    bookresults, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
                    api_hits += not in_cache
                    cache_hits += in_cache
                    try:
                        for item in bookresults['data']['search']['results']['hits']:
                            resultbooks.append(item['document'])
                    except (IndexError, KeyError):
                        pass

                if searchauthorname:
                    searchcmd = self.HC_FINDAUTHORBYNAME.replace('[authorname]', searchauthorname)
                    authresults, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
                    api_hits += not in_cache
                    cache_hits += in_cache
                    try:
                        for item in authresults['data']['search']['results']['hits']:
                            authids.append(item['document']['id'])
                    except (IndexError, KeyError):
                        pass

                if authids:
                    for authid in authids:
                        searchcmd = self.HC_AUTHORID_BOOKS.replace('[authorid]', authid)
                        results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
                        api_hits += not in_cache
                        cache_hits += in_cache
                        if 'errors' in results:
                            self.logger.error(str(results['errors']))
                        if "data" in results:
                            books = results['data']['books']
                            for book in books:
                                if book not in resultbooks:
                                    resultbooks.append(book)

            if not resultbooks:
                if queue:
                    queue.put(resultlist)
                    return
                return resultlist

            for book_data in resultbooks:
                if 'users_count' in book_data:
                    # search results return a different layout to books
                    bookdict = self.get_searchdict(book_data)
                else:
                    bookdict = self.get_bookdict(book_data)

                if searchauthorname:
                    author_fuzz = fuzz.token_set_ratio(bookdict['auth_name'], searchauthorname)
                else:
                    author_fuzz = fuzz.token_set_ratio(bookdict['auth_name'], searchterm)
                book_title = bookdict['title']
                if searchtitle:
                    if book_title.endswith(')'):
                        book_title = book_title.rsplit(' (', 1)[0]
                    book_fuzz = fuzz.token_set_ratio(book_title, searchtitle)
                    # lose a point for each extra word in the fuzzy matches, so we get the closest match
                    words = len(get_list(book_title))
                    words -= len(get_list(searchtitle))
                    book_fuzz -= abs(words)
                else:
                    book_fuzz = fuzz.token_set_ratio(book_title, searchterm)
                    words = len(get_list(book_title))
                    words -= len(get_list(searchterm))
                    book_fuzz -= abs(words)
                isbn_fuzz = 0
                if is_valid_isbn(searchterm):
                    isbn_fuzz = 100
                    bookdict['isbn'] = searchterm

                highest_fuzz = max(author_fuzz + book_fuzz, isbn_fuzz)

                resultlist.append({
                        'authorname': bookdict['auth_name'],
                        'bookid': bookdict['bookid'],
                        'authorid': bookdict['auth_id'],
                        'bookname': bookdict['title'],
                        'booksub': bookdict['subtitle'],
                        'bookisbn': bookdict['isbn'],
                        'bookpub': bookdict['publishers'],
                        'bookdate': bookdict['publish_date'],
                        'booklang': bookdict['languages'],
                        'booklink': bookdict['link'],
                        'bookrate': bookdict['bookrate'],
                        'bookrate_count': bookdict['bookrate_count'],
                        'bookimg': bookdict['cover'],
                        'bookpages': bookdict['bookpages'],
                        'bookgenre': bookdict['genres'],
                        'bookdesc': bookdict['bookdesc'],
                        'workid': bookdict['bookid'],  # TODO should this be canonical id?
                        'author_fuzz': round(author_fuzz, 2),
                        'book_fuzz': round(book_fuzz, 2),
                        'isbn_fuzz': round(isbn_fuzz, 2),
                        'highest_fuzz': round(highest_fuzz, 2),
                        'source': "HardCover"
                    })
                resultcount += 1

            self.logger.debug("Used %s api hit, %s in cache" % (api_hits, cache_hits))
            queue.put(resultlist)

        except Exception:
            self.logger.error('Unhandled exception in HC.find_results: %s' % traceback.format_exc())

    def find_author_id(self, refresh=False):
        api_hits = 0
        authorname = self.name.replace('#', '').replace('/', '_')
        authorname = format_author_name(authorname, postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
        title = self.title

        if not title:
            # we only have an authorname. Return id of matching author with the most books
            self.logger.debug("Searching for author %s, refresh=%s" % (authorname, refresh))
            searchcmd = self.HC_FINDAUTHORBYNAME.replace('[authorname]', authorname)
            results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
            api_hits += not in_cache
            authorid = None
            matches = []
            if results:
                try:
                    for item in results['data']['search']['results']['hits']:
                        name = item['document']['name']
                        altnames = item['document']['alternate_names']
                        books_count = item['document']['books_count']
                        author_id = item['document']['id']
                        if authorname == name or authorname in altnames:
                            matches.append([books_count, author_id, name, altnames])
                    matches = sorted(matches, reverse=True)
                    authorid = matches[0][1]
                except (IndexError, KeyError):
                    pass
            if authorid:
                res = self.get_author_info(authorid)
                if res:
                    if res['authorname'] != authorname:
                        res['aka'] = authorname
                    self.logger.debug("Authorname search used %s api hit" % api_hits)
                    return res
        else:
            # search for the title and then check the authorname matches
            self.logger.debug("Searching for title %s, refresh=%s" % (title, refresh))
            searchcmd = self.HC_FINDBOOKBYNAME.replace('[title]', title)
            results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
            api_hits += not in_cache
            bookid = None
            if results:
                try:
                    for item in results['data']['search']['results']['hits']:
                        if authorname in item['document']['author_names']:
                            bookid = item['document']['id']
                            break
                except (IndexError, KeyError):
                    pass

            if bookid:
                url = None
                try:
                    for item in results['data']['search']['results']['hits']:
                        if 'cachedImage' in item['contributions'][0]['author']:
                            url = item['contributions'][0]['author']['cachedImage']['url']
                            break
                except (IndexError, KeyError):
                    pass

                if url:
                    # try to extract the authorid from the image url
                    parts = url.split('/')
                    if len(parts) == 6:
                        authorid = parts[4]
                        res = self.get_author_info(authorid)
                        if res:
                            if res['authorname'] != authorname:
                                res['aka'] = authorname
                            self.logger.debug("Title search used %s api hit" % api_hits)
                            return res

                # get the authorid from the book page as it's not in the title search results
                bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]', bookid)
                results, in_cache = self.result_from_cache(bookidcmd, refresh=refresh)
                api_hits += not in_cache
                if 'data' in results and results['data'].get('books'):
                    for book_data in results['data']['books']:
                        for author in book_data['contributions']:
                            # might be more than one author listed
                            author_name = author['author']['name']
                            authorid = str(author['author']['id'])
                            res = None
                            match = fuzz.ratio(author_name.lower(), authorname.lower())
                            if match >= CONFIG.get_int('NAME_RATIO'):
                                res = self.get_author_info(authorid)
                            if not res:
                                match = fuzz.partial_ratio(author_name.lower(), authorname.lower())
                                if match >= CONFIG.get_int('NAME_PARTNAME'):
                                    res = self.get_author_info(authorid)
                            if res:
                                if res['authorname'] != authorname:
                                    res['aka'] = authorname
                                self.logger.debug("Author/book search used %s api hit" % api_hits)
                                return res

        self.logger.debug("No results. Used %s api hit" % api_hits)
        return {}

    def get_author_info(self, authorid=None, refresh=False):
        author_name = ''
        author_born = ''
        author_died = ''
        author_link = ''
        about = ''
        totalbooks = 0
        api_hits = 0
        cache_hits = 0

        self.logger.debug("Getting author info for %s, refresh=%s" % (authorid, refresh))
        searchcmd = self.HC_AUTHORINFO.replace('[authorid]', str(authorid))
        results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
        api_hits += not in_cache
        cache_hits += in_cache
        if 'errors' in results:
            self.logger.error(str(results['errors']))
        if results and results.get('data'):
            for author in results['data']['authors']:
                if str(author['id']) == str(authorid):
                    author_name = author.get('name', '')
                    # hc sometimes returns multiple comma separated names, use the one we are looking for
                    if self.name and self.name in author_name:
                        author_name = self.name
                    author_born = author.get('born_date', '')
                    author_died = author.get('death_date', '')
                    totalbooks = author.get('books_count', 0)
                    about = author.get('bio', '')
                    # if 'cached_image' in author and author['cached_image'].get('url'):
                    #     author_img = author['cached_image']['url']
                    break

        if "," in author_name:
            postfix = get_list(CONFIG.get_csv('NAME_POSTFIX'))
            words = author_name.split(',')
            if len(words) == 2:
                if words[0].strip().strip('.').lower in postfix:
                    author_name = words[1].strip() + ' ' + words[0].strip()
                else:
                    author_name = author_name.split(',')[0]

        if not author_name:
            self.logger.warning("Rejecting authorid %s, no authorname" % authorid)
            return {}

        self.logger.debug("[%s] Returning HC info for authorID: %s" % (author_name, authorid))
        # return authorimg in this dict once we get a reliable one from hc
        # need to uncomment cached_image lines above
        author_dict = {
            'authorid': str(authorid),
            'authorlink': author_link,
            # 'authorimg': author_img,
            'authorborn': author_born,
            'authordeath': author_died,
            'about': about,
            'totalbooks': totalbooks,
            'authorname': format_author_name(author_name, postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
        }
        self.logger.debug("AuthorInfo used %s api hit, %s in cache" % (api_hits, cache_hits))
        return author_dict

    def get_bookdict(self, book_data):
        bookdict = {}
        if 'contributions' in book_data and len(book_data['contributions']):
            author = book_data['contributions'][0]
            bookdict['auth_name'] = " ".join(author['author']['name'].split())
            bookdict['auth_id'] = str(author['author']['id'])
        else:
            bookdict['auth_name'] = 'Unknown'
            bookdict['auth_id'] = '0'
        bookdict['title'] = book_data.get('title', '')
        bookdict['subtitle'] = book_data.get('subtitle', '')
        bookdict['cover'] = ""
        if 'cached_image' in book_data and book_data['cached_image'].get('url'):
            bookdict['cover'] = book_data['cached_image']['url']
        editions = book_data.get('editions', [])
        bookdict['isbn'] = ""
        for edition in editions:
            if edition.get('isbn_13'):
                bookdict['isbn'] = edition['isbn_13']
                break
            if edition.get('isbn_10'):
                bookdict['isbn'] = edition['isbn_10']
                break
        bookdict['series'] = []
        bookseries = book_data.get('book_series', [])
        for series in bookseries:
            bookdict['series'].append([series['series']['name'], series['series']['id'], series['position']])
        bookdict['link'] = book_data.get('slug', '')
        if bookdict['link']:
            bookdict['link'] = self.book_url + bookdict['link']
        bookdict['bookrate'] = book_data.get('rating', 0)
        bookdict['bookrate_count'] = book_data.get('ratings_count', 0)
        if bookdict['bookrate'] is None:
            bookdict['bookrate'] = 0

        bookdict['bookpages'] = book_data.get('pages', 0)
        if bookdict['bookpages'] is None:
            bookdict['bookpages'] = 0
        bookdict['bookdesc'] = book_data.get('description', '')
        bookdict['bookid'] = str(book_data.get('id', ''))
        bookdict['publish_date'] = book_data.get('release_date', '')
        if bookdict['publish_date']:
            bookdict['publish_date'] = date_format(bookdict['publish_date'],
                                                   context=f"{bookdict['auth_name']}/{bookdict['title']}")
        bookdict['first_publish_year'] = book_data.get('release_year', '')
        bookgenre = ''
        genres = []
        cached_tags = book_data['cached_tags']
        if 'Genre' in cached_tags:
            book_genres = cached_tags['Genre']
            for genre in book_genres:
                genres.append(genre['tag'])
        if genres:
            if lazylibrarian.GRGENRES:
                genre_limit = lazylibrarian.GRGENRES.get('genreLimit', 3)
            else:
                genre_limit = 3
            genres = list(set(genres))
            bookgenre = ', '.join(genres[:genre_limit])
        bookdict['genres'] = bookgenre
        bookdict['languages'] = ""
        langs = []
        for edition in editions:
            if edition.get('language'):
                lang = edition['language']['language']
                if lang:
                    langs.append(lang)
        if langs:
            bookdict['languages'] = ', '.join(set(langs))
        bookdict['publishers'] = ""
        pubs = []
        for edition in editions:
            if edition.get('publisher'):
                pub = edition['publisher']['name']
                if pub:
                    pubs.append(pub)
        if pubs:
            bookdict['publishers'] = ', '.join(set(pubs))
        bookdict['id_librarything'] = ""
        if not bookdict['cover']:
            bookdict['cover'] = 'images/nocover.png'
        return bookdict

    def get_searchdict(self, book_data):
        bookdict = {'auth_id': '0', 'auth_name': 'Unknown'}
        if 'contributions' in book_data and len(book_data['contributions']):
            author = book_data['contributions'][0]
            bookdict['auth_name'] = " ".join(author['author']['name'].split())
            try:
                url = author['author']['cachedImage']['url']
                if url:
                    # try to extract the authorid from the author image url
                    parts = url.split('/')
                    if len(parts) == 6:
                        bookdict['auth_id'] = str(parts[4])
            except (KeyError, IndexError):
                pass

        bookdict['title'] = book_data.get('title', '')
        bookdict['subtitle'] = book_data.get('subtitle', '')
        bookdict['cover'] = ""
        if 'image' in book_data and book_data['image'].get('url'):
            bookdict['cover'] = book_data['image']['url']
        isbns = book_data.get('isbns', [])
        bookdict['isbn'] = ""
        if isbns:
            bookdict['isbn'] = isbns[0]
        bookdict['series'] = []
        bookdict['link'] = book_data.get('slug', '')
        if bookdict['link']:
            bookdict['link'] = self.book_url + bookdict['link']
        bookdict['bookrate'] = book_data.get('rating', 0)
        bookdict['bookrate_count'] = book_data.get('ratings_count', 0)
        if bookdict['bookrate'] is None:
            bookdict['bookrate'] = 0

        bookdict['bookpages'] = book_data.get('pages', 0)
        if bookdict['bookpages'] is None:
            bookdict['bookpages'] = 0
        bookdict['bookdesc'] = book_data.get('description', '')
        bookdict['bookid'] = str(book_data.get('id', ''))
        bookdict['publish_date'] = book_data.get('release_date', '')
        if bookdict['publish_date']:
            bookdict['publish_date'] = date_format(bookdict['publish_date'],
                                                   context=f"{bookdict['auth_name']}/{bookdict['title']}")
        bookdict['first_publish_year'] = book_data.get('release_year', '')
        bookgenre = ''
        genres = []
        cached_tags = book_data['tags']
        if 'Genre' in cached_tags:
            book_genres = cached_tags['Genre']
            for genre in book_genres:
                genres.append(genre['tag'])
        if genres:
            if lazylibrarian.GRGENRES:
                genre_limit = lazylibrarian.GRGENRES.get('genreLimit', 3)
            else:
                genre_limit = 3
            genres = list(set(genres))
            bookgenre = ', '.join(genres[:genre_limit])
        bookdict['genres'] = bookgenre
        bookdict['languages'] = ""
        bookdict['publishers'] = ""
        bookdict['id_librarything'] = ""
        if not bookdict['cover']:
            bookdict['cover'] = 'images/nocover.png'
        return bookdict

    def get_author_books(self, authorid=None, authorname=None, bookstatus="Skipped", audiostatus='Skipped',
                         entrystatus='Active', refresh=False, reason='hc.get_author_books'):

        cache_hits = 0
        api_hits = 0
        book_ignore_count = 0
        bad_lang = 0
        added_count = 0
        entryreason = reason
        cover_time = 0
        cover_count = 0
        locked_count = 0
        new_authors = 0
        updated_count = 0
        removed_results = 0
        duplicates = 0
        auth_start = time.time()
        series_updates = []
        hc_id = ''
        entry_name = authorname

        # these are reject reasons we might want to override, so optionally add to database as "ignored"
        ignorable = ['future', 'date', 'isbn', 'word', 'set']
        if CONFIG.get_bool('NO_LANG'):
            ignorable.append('lang')

        db = database.DBConnection()
        try:
            match = db.match('SELECT authorid,hc_id FROM authors where authorid=? or hc_id=?', (authorid, authorid))
            if match:
                hc_id = match['hc_id']
                authorid = match['authorid']
            if not hc_id:
                hc_id = authorid

            # Artist is loading
            db.action("UPDATE authors SET Status='Loading' WHERE AuthorID=?", (authorid,))

            searchcmd = self.HC_AUTHORID_BOOKS.replace('[authorid]', hc_id)
            results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
            api_hits += not in_cache
            cache_hits += in_cache
            if 'errors' in results:
                self.logger.error(str(results['errors']))
            if not results or 'data' not in results:
                db.action("UPDATE authors SET Status=? WHERE AuthorID=?", (entrystatus, authorid))
                return

            self.logger.debug(f"HC found {len(results['data']['books'])} results")
            for book_data in results['data']['books']:
                bookdict = self.get_bookdict(book_data)
                if bookdict['auth_name'] != entry_name:
                    # not our author, might be a contributor to an anthology?
                    if 'contributions' in book_data and len(book_data['contributions']):
                        for author in book_data['contributions']:
                            if (fuzz.token_set_ratio(author['author']['name'], entry_name) >=
                                    CONFIG.get_int('NAME_RATIO')):
                                bookdict['auth_name'] = " ".join(author['author']['name'].split())
                                bookdict['auth_id'] = str(author['author']['id'])
                                break

                bookdict['book_status'] = bookstatus
                bookdict['audio_status'] = audiostatus
                rejected = validate_bookdict(bookdict)

                if rejected:
                    if rejected[0] in ignorable:
                        bookdict['book_status'] = 'Ignored'
                        bookdict['audio_status'] = 'Ignored'
                        book_ignore_count += 1
                        reason = "Ignored: %s" % rejected[1]
                        rejected = ''
                    elif rejected[0] == 'lang':
                        bad_lang += 1
                    elif rejected[0] == 'dupe':
                        duplicates += 1
                    elif rejected[0] in ['name', 'publisher']:
                        removed_results += 1

                elif 'author_update' in entryreason:
                    reason = 'Author: %s' % bookdict['auth_name']
                else:
                    reason = entryreason
                if rejected:
                    reason = rejected[1]
                else:
                    update_value_dict = {}
                    exists = db.match("SELECT * from books WHERE BookID=?", (bookdict['bookid'],))
                    if exists:
                        series = db.select('select seriesname from series,member where '
                                           'series.seriesid=member.seriesid and bookid=?', (exists['BookID'],))
                        serieslist = []
                        for n in series:
                            serieslist.append(n[0])

                        locked = exists['Manual']
                        if locked is None:
                            locked = False
                        elif locked.isdigit():
                            locked = bool(int(locked))
                    else:
                        serieslist = []
                        locked = False
                        self.logger.debug("Inserting new book [%s] for [%s]" % (bookdict['title'],
                                                                                bookdict['auth_name']))
                        if 'author_update' in entryreason:
                            reason = 'Author: %s' % bookdict['auth_name']
                        else:
                            reason = entryreason
                        reason = "[%s] %s" % (thread_name(), reason)
                        added_count += 1
                        if not bookdict['languages']:
                            bookdict['languages'] = 'Unknown'

                        cover_link = bookdict['cover']
                        if 'nocover' in cover_link or 'nophoto' in cover_link:
                            start = time.time()
                            cover_link, _ = get_book_cover(bookdict['bookid'])
                            cover_time += (time.time() - start)
                            cover_count += 1
                        elif cover_link and cover_link.startswith('http'):
                            cover_link = cache_bookimg(cover_link, bookdict['bookid'], 'hc')
                        if not cover_link:  # no results on search or failed to cache it
                            cover_link = 'images/nocover.png'

                        db.action('INSERT INTO books (AuthorID, BookName, BookImg, ' +
                                  'BookLink, BookID, BookDate, BookLang, BookAdded, Status, ' +
                                  'WorkPage, AudioStatus, ScanResult, OriginalPubDate, hc_id) ' +
                                  'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                  (authorid, bookdict['title'], cover_link, bookdict['link'],
                                   bookdict['bookid'], bookdict['publish_date'], bookdict['languages'], now(),
                                   bookdict['book_status'], '', bookdict['audio_status'], reason,
                                   bookdict['first_publish_year'], bookdict['bookid']))

                    # Leave alone if locked
                    if locked:
                        locked_count += 1
                    else:
                        if exists and exists['ScanResult'] and ' publication date' in exists['ScanResult'] \
                                and bookdict['publish_date'] and bookdict['publish_date'] != '0000' \
                                and bookdict['publish_date'] <= today()[:len(bookdict['publish_date'])]:
                            # was rejected on previous scan but bookdate has become valid
                            self.logger.debug("valid bookdate [%s] previous scanresult [%s]" %
                                              (bookdict['publish_date'], exists['ScanResult']))

                            update_value_dict["ScanResult"] = "bookdate %s is now valid" % bookdict['publish_date']
                        elif not exists:
                            update_value_dict["ScanResult"] = reason

                        if "ScanResult" in update_value_dict:
                            self.searchinglogger.debug("entry status %s %s,%s" % (entrystatus, bookstatus,
                                                                                  audiostatus))
                            book_status, audio_status = get_status(bookdict['bookid'], serieslist, bookstatus,
                                                                   audiostatus, entrystatus)
                            self.searchinglogger.debug("status is now %s,%s" % (book_status, audio_status))
                            update_value_dict["Status"] = book_status
                            update_value_dict["AudioStatus"] = audio_status

                    if update_value_dict:
                        control_value_dict = {"BookID": bookdict['bookid']}
                        db.upsert("books", update_value_dict, control_value_dict)

                    if not exists:
                        typ = 'Added'
                        added_count += 1
                    else:
                        typ = 'Updated'
                        updated_count += 1
                    msg = "[%s] %s book: %s [%s] status %s" % (bookdict['auth_name'], typ, bookdict['title'],
                                                               bookdict['languages'], bookstatus)
                    if CONFIG.get_bool('AUDIO_TAB'):
                        msg += " audio %s" % audiostatus
                    self.logger.debug(msg)

                    if CONFIG.get_bool('ADD_SERIES') and bookdict.get('series'):
                        for item in bookdict['series']:
                            ser_name = item[0]
                            ser_id = 'HC' + str(item[1])
                            if ser_id in series_updates:
                                self.logger.debug("Series %s:%s already updated" % (ser_id, ser_name))
                            else:
                                exists = db.match("SELECT * from series WHERE seriesid=?", (ser_id,))
                                if not exists:
                                    exists = db.match("SELECT * from series WHERE seriesname=? "
                                                      "and seriesid like 'HC%'", (ser_name,))
                                if not exists:
                                    self.logger.debug("New series: %s:%s" % (ser_id, ser_name))
                                    db.action('INSERT INTO series (SeriesID, SeriesName, Status, '
                                              'Updated, Reason) VALUES (?,?,?,?,?)',
                                              (ser_id, ser_name, 'Paused', time.time(), ser_name))
                                    db.commit()
                                    exists = {'Status': 'Paused'}

                                series_updates.append(ser_id)
                                if exists['Status'] in ['Paused', 'Ignored']:
                                    self.logger.debug("Not getting additional series members for %s, "
                                                      "status is %s" % (ser_name, exists['Status']))
                                else:
                                    seriesmembers = self.get_series_members(ser_id, ser_name)
                                    if len(seriesmembers) == 1:
                                        self.logger.debug("Found member %s for series %s" % (seriesmembers[0][1],
                                                                                             ser_name))
                                    else:
                                        self.logger.debug("Found %s members for series %s" % (len(seriesmembers),
                                                                                              ser_name))
                                    # position, book_title, author_name, author_id, book_id
                                    for member in seriesmembers:
                                        db.action("DELETE from member WHERE SeriesID=? AND SeriesNum=?",
                                                  (ser_id, member[0]))
                                        auth_name, exists = lazylibrarian.importer.get_preferred_author_name(member[2])
                                        if not exists:
                                            reason = "Series contributing author %s:%s" % (ser_name, member[1])
                                            lazylibrarian.importer.add_author_name_to_db(author=member[2],
                                                                                         refresh=False,
                                                                                         addbooks=False,
                                                                                         reason=reason
                                                                                         )
                                            auth_name, ex = lazylibrarian.importer.get_preferred_author_name(member[2])
                                            if not ex:
                                                self.logger.debug("Unable to add %s for %s, author not in database" %
                                                                  (member[2], member[1]))
                                                continue
                                        else:
                                            cmd = "SELECT * from authors WHERE authorname=?"
                                            exists = db.match(cmd, (auth_name,))
                                            if exists:
                                                auth_id = exists['AuthorID']
                                                if fuzz.ratio(auth_name.lower().replace('.', ''),
                                                              member[2].lower().replace('.', '')) < 95:
                                                    akas = get_list(exists['AKA'], ',')
                                                    if member[2] not in akas:
                                                        akas.append(member[2])
                                                        db.action("UPDATE authors SET AKA=? WHERE AuthorID=?",
                                                                  (', '.join(akas), auth_id))
                                                match = db.match('SELECT * from seriesauthors WHERE ' +
                                                                 'SeriesID=? and AuthorID=?',
                                                                 (ser_id, auth_id))
                                                if not match:
                                                    self.logger.debug("Adding %s as series author for %s" %
                                                                      (auth_name, ser_name))
                                                    new_authors += 1
                                                    db.action('INSERT INTO seriesauthors (SeriesID, '
                                                              'AuthorID) VALUES (?, ?)',
                                                              (ser_id, auth_id), suppress='UNIQUE')

                                                    self.logger.debug("Inserting new member [%s] for %s" %
                                                                      (member[0], ser_name))

                                                    cmd = "SELECT BookID FROM books WHERE BookID=?"
                                                    # make sure bookid is in database, if not, add it
                                                    match = db.match(cmd, (str(member[4]),))
                                                    if not match:
                                                        bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]',
                                                                                                 str(member[4]))
                                                        results, in_cache = self.result_from_cache(bookidcmd,
                                                                                                   refresh=refresh)
                                                        api_hits += not in_cache
                                                        cache_hits += in_cache
                                                        newbookdict = {}
                                                        if 'errors' in results:
                                                            self.logger.error(str(results['errors']))
                                                        if 'data' in results and results['data'].get('books'):
                                                            newbookdict = self.get_bookdict(results['data']['books'][0])
                                                        if newbookdict:
                                                            cover_link = newbookdict['cover']
                                                            if 'nocover' in cover_link or 'nophoto' in cover_link:
                                                                start = time.time()
                                                                cover_link, _ = get_book_cover(newbookdict['bookid'])
                                                                cover_time += (time.time() - start)
                                                                cover_count += 1
                                                            elif cover_link and cover_link.startswith('http'):
                                                                cover_link = cache_bookimg(cover_link,
                                                                                           newbookdict['bookid'], 'hc')
                                                            if not cover_link:  # no results or failed to cache it
                                                                cover_link = 'images/nocover.png'

                                                            cmd = ('INSERT INTO books (AuthorID, BookName, BookImg, '
                                                                   'BookLink, BookID, BookDate, BookLang, BookAdded, '
                                                                   'Status, WorkPage, AudioStatus, ScanResult, '
                                                                   'OriginalPubDate, hc_id) '
                                                                   'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
                                                            db.action(cmd, (authorid, newbookdict['title'],
                                                                            cover_link, newbookdict['link'],
                                                                            newbookdict['bookid'],
                                                                            newbookdict['publish_date'],
                                                                            newbookdict['languages'], now(),
                                                                            newbookdict['book_status'], '',
                                                                            newbookdict['audio_status'], reason,
                                                                            newbookdict['first_publish_year'],
                                                                            newbookdict['bookid']))

                                                            db.action('INSERT INTO member (SeriesID, BookID, '
                                                                      'SeriesNum) VALUES (?,?,?)',
                                                                      (ser_id, member[4], member[0]), suppress="UNIQUE")
                                                    ser = db.match("select count(*) as counter from member " +
                                                                   "where seriesid=?", (ser_id,))
                                                    if ser:
                                                        counter = check_int(ser['counter'], 0)
                                                        db.action("UPDATE series SET Total=? WHERE SeriesID=?",
                                                                  (counter, ser_id))

            # no more books to process, update summaries
            cmd = ("SELECT BookName, BookLink, BookDate, BookImg, BookID from books WHERE AuthorID=? and "
                   "Status != 'Ignored' order by BookDate DESC")
            lastbook = db.match(cmd, (authorid,))
            if lastbook:
                lastbookname = lastbook['BookName']
                lastbooklink = lastbook['BookLink']
                lastbookdate = lastbook['BookDate']
                lastbookid = lastbook['BookID']
                lastbookimg = lastbook['BookImg']
            else:
                lastbookname = ""
                lastbooklink = ""
                lastbookdate = ""
                lastbookid = ""
                lastbookimg = ""

            control_value_dict = {"AuthorID": authorid}
            new_value_dict = {
                "Status": entrystatus,
                "LastBook": lastbookname,
                "LastLink": lastbooklink,
                "LastDate": lastbookdate,
                "LastBookID": lastbookid,
                "LastBookImg": lastbookimg
            }
            db.upsert("authors", new_value_dict, control_value_dict)

            resultcount = added_count + updated_count
            self.logger.debug("Found %s locked %s" % (locked_count, plural(locked_count, "book")))
            self.logger.debug("Added %s new %s" % (new_authors, plural(new_authors, "author")))
            self.logger.debug("Removed %s unwanted language %s" % (bad_lang, plural(bad_lang, "result")))
            self.logger.debug("Removed %s incorrect/incomplete %s" % (removed_results, plural(removed_results,
                                                                                              "result")))
            self.logger.debug("Removed %s duplicate %s" % (duplicates, plural(duplicates, "result")))
            self.logger.debug("Ignored %s %s" % (book_ignore_count, plural(book_ignore_count, "book")))
            self.logger.debug("Imported/Updated %s %s in %d secs using %s api %s" %
                              (resultcount, plural(resultcount, "book"), int(time.time() - auth_start),
                               api_hits, plural(api_hits, "hit")))
            if cover_count:
                self.logger.debug("Fetched %s %s in %.2f sec" % (cover_count, plural(cover_count,
                                                                                     "cover"), cover_time))

            control_value_dict = {"authorname": entry_name.replace('"', '""')}
            new_value_dict = {
                              "GR_book_hits": 0,
                              "GR_lang_hits": 0,
                              "LT_lang_hits": 0,
                              "GB_lang_change": 0,
                              "cache_hits": cache_hits,
                              "bad_lang": bad_lang,
                              "bad_char": removed_results,
                              "uncached": api_hits,
                              "duplicates": duplicates
                              }
            db.upsert("stats", new_value_dict, control_value_dict)
        finally:
            db.close()

    def find_bookdict(self, bookid=None):
        bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]', str(bookid))
        results, in_cache = self.result_from_cache(bookidcmd, refresh=False)
        bookdict = {}
        if 'errors' in results:
            self.logger.error(str(results['errors']))
        if 'data' in results and results['data'].get('books'):
            bookdict = self.get_bookdict(results['data']['books'][0])
        return bookdict

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='hc.find_book'):
        bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]', str(bookid))
        results, in_cache = self.result_from_cache(bookidcmd, refresh=False)
        bookdict = {}
        if 'errors' in results:
            self.logger.error(str(results['errors']))
        if 'data' in results and results['data'].get('books'):
            bookdict = self.get_bookdict(results['data']['books'][0])
        if not bookstatus:
            bookstatus = CONFIG['NEWBOOK_STATUS']
            self.logger.debug("No bookstatus passed, using default %s" % bookstatus)
        if not audiostatus:
            audiostatus = CONFIG['NEWAUDIO_STATUS']
            self.logger.debug("No audiostatus passed, using default %s" % audiostatus)
        self.logger.debug("bookstatus=%s, audiostatus=%s" % (bookstatus, audiostatus))
        bookdict['book_status'] = bookstatus
        bookdict['audio_status'] = audiostatus
        rejected = validate_bookdict(bookdict)

        if rejected:
            if reason.startswith("Series:") or rejected[0] == 'name':
                return
            #
            # user has said they want this book, don't block for unwanted language etc.
            # Ignore book if adding as part of a series, else just warn and include it
            #
            title = bookdict['title']
            lang = bookdict['languages']
            bookdate = bookdict['publish_date']
            msg = ''
            if rejected[0] == 'name':
                msg = 'Book %s authorname invalid' % title
            elif rejected[0] == 'lang':
                msg = 'Book %s Language [%s] does not match preference' % (title, lang)

            elif rejected[0] in ['publisher']:
                msg = 'Book %s Publisher [%s] does not match preference' % (title, lang)

            elif CONFIG.get_bool('NO_PUBDATE'):
                if not bookdate or bookdate == '0000':
                    msg = 'Book %s Publication date [%s] does not match preference' % (title, bookdate)

            elif CONFIG.get_bool('NO_FUTURE'):
                # may have yyyy or yyyy-mm-dd
                if bookdate > today()[:len(bookdate)]:
                    msg = 'Book %s Future publication date [%s] does not match preference' % (title, bookdate)

            elif CONFIG.get_bool('NO_SETS'):
                if re.search(r'\d+ of \d+', title) or re.search(r'\d+/\d+', title):
                    msg = 'Book %s Set or Part' % title
                # allow date ranges eg 1981-95
                m = re.search(r'(\d+)-(\d+)', title)
                if m:
                    if check_year(m.group(1), past=1800, future=0):
                        self.logger.debug("Allow %s, looks like a date range" % m.group(1))
                        msg = ''
            if msg:
                self.logger.warning(msg + ' : adding anyway')

        auth_name, exists = lazylibrarian.importer.get_preferred_author_name(bookdict['auth_name'])
        if not exists:
            reason = "%s:%s" % (reason, bookdict['bookid'])
            lazylibrarian.importer.add_author_name_to_db(author=bookdict['auth_name'], refresh=False,
                                                         addbooks=False, reason=reason)
        auth_name, exists = lazylibrarian.importer.get_preferred_author_name(bookdict['auth_name'])
        if not exists:
            self.logger.debug("Unable to add %s for %s, author not found" %
                              (bookdict['auth_name'], bookdict['bookid']))
        else:
            db = database.DBConnection()
            cmd = "SELECT * from authors WHERE authorname=?"
            exists = db.match(cmd, (auth_name,))
            if not exists:
                self.logger.debug("Unable to add %s for %s, author not in database" % (bookdict['auth_name'],
                                                                                       bookdict['bookid']))
            else:
                auth_id = exists['AuthorID']
                cover_link = bookdict['cover']
                if 'nocover' in cover_link or 'nophoto' in cover_link:
                    cover_link, _ = get_book_cover(bookdict['bookid'])
                elif cover_link and cover_link.startswith('http'):
                    cover_link = cache_bookimg(cover_link, bookdict['bookid'], 'hc')
                if not cover_link:  # no results on search or failed to cache it
                    cover_link = 'images/nocover.png'
                db.action('INSERT INTO books (AuthorID, BookName, BookImg, ' +
                          'BookLink, BookID, BookDate, BookLang, BookAdded, Status, ' +
                          'WorkPage, AudioStatus, ScanResult, OriginalPubDate, hc_id) ' +
                          'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                          (auth_id, bookdict['title'], cover_link, bookdict['link'],
                           bookdict['bookid'], bookdict['publish_date'], bookdict['languages'], now(),
                           bookdict['book_status'], '', bookdict['audio_status'], reason,
                           bookdict['first_publish_year'], bookdict['bookid']))
            db.close()

        return

    def sync(self, library='', userid=None):
        # hc status_id = 1 want-to-read, 2 currently_reading, 3 read, 4 owned, 5 dnf
        # map to ll tables 'ToRead', 'Reading', 'HaveRead', 'Abandoned'
        # and status Have/Open = owned
        # library = eBook, AudioBook or leave empty for both
        msg = ''
        if not userid:
            if not CONFIG.get_bool('USER_ACCOUNTS'):
                msg = "User accounts are not enabled"
                self.logger.error(msg)

            cookie = cherrypy.request.cookie
            if 'll_uid' in list(cookie.keys()):
                userid = cookie['ll_uid'].value
            if not userid:
                msg = "No userid to sync"
                self.logger.error(msg)
        if msg:
            return msg

        thread_name('HCSync')
        db = database.DBConnection()
        try:
            # we currently don't use local hc_id as the apikey is linked to the hc_id
            # which is requested using a "whoami" command
            # but this will need to change so we can sync lists from multiple users
            res = db.match("SELECT hc_id from users where userid=?", (userid,))
            if res and not res['hc_id']:
                msg = f"No hc_id for user {userid}, first sync?"
                self.logger.warning(msg)

            self.logger.debug(f"HCsync starting for {userid}")
            db.upsert("jobs", {"Start": time.time()}, {"Name": "HCSYNC"})
            ll_haveread = get_readinglist('haveread', userid)
            self.syncinglogger.debug(f"ll have read contains {len(ll_haveread)}")
            ll_toread = get_readinglist('toread', userid)
            self.syncinglogger.debug(f"ll to read contains {len(ll_toread)}")
            ll_reading = get_readinglist('reading', userid)
            self.syncinglogger.debug(f"ll reading contains {len(ll_reading)}")
            ll_dnf = get_readinglist('dnf', userid)
            self.syncinglogger.debug(f"ll have dnf contains {len(ll_dnf)}")
            ll_owned = []
            if library == 'eBook':
                for item in db.select("SELECT bookid from books where status in ('Open', 'Have')"):
                    ll_owned.append(item['bookid'])
            elif library == 'AudioBook':
                for item in db.select("SELECT bookid from books where audiostatus in ('Open', 'Have')"):
                    ll_owned.append(item['bookid'])
            else:
                for item in db.select("SELECT bookid from books where status in ('Open', 'Have') "
                                      "or audiostatus in ('Open', 'Have')"):
                    ll_owned.append(item['bookid'])
            self.syncinglogger.debug(f"ll owned contains {len(ll_owned)}")
            searchcmd = self.HC_WHOAMI
            results, _ = self.result_from_cache(searchcmd, refresh=True)
            whoami = 0
            if 'errors' in results:
                self.logger.error(str(results['errors']))
            elif 'data' in results and 'me' in results['data']:
                # this is the hc_id tied to the api bearer token
                res = results['data']['me']
                whoami = res[0]['id']
                db.upsert("users", {'hc_id': whoami}, {'UserID': userid})
                if not whoami:
                    self.logger.error(f"No hc_id for user {userid}")
                    return msg

            hc_toread = []
            hc_reading = []
            hc_read = []
            hc_dnf = []
            hc_owned = []
            remapped = []
            sync_dict = {}
            hc_mapping = [[hc_dnf, 5, 'DNF'], [hc_reading, 2, 'Reading'], [hc_read, 3, 'Read'],
                          [hc_toread, 1, 'ToRead'], [hc_owned, 4, 'Owned']]
            for mapp in hc_mapping:
                searchcmd = self.HC_USERBOOKS.replace('[whoami]', str(whoami)).replace('[status]',
                                                                                       str(mapp[1]))
                results, _ = self.result_from_cache(searchcmd, refresh=True)
                if 'errors' in results:
                    self.logger.error(str(results['errors']))
                elif 'data' in results and 'user_books' in results['data']:
                    self.syncinglogger.debug(f"HardCover {mapp[2]} contains {len(results['data']['user_books'])}")
                    for item in results['data']['user_books']:
                        hc_id = item['book']['id']
                        res = db.match("SELECT bookid from books WHERE hc_id=?", (hc_id,))
                        if res and res['bookid']:
                            if res['bookid'] in remapped:
                                self.syncinglogger.debug(f"Duplicated entry {hc_id} in {mapp[2]} for {res['bookid']}")
                                delcmd = self.HC_DELUSERBOOK.replace('[bookid]', str(item['id']))
                                results, _ = self.result_from_cache(delcmd, refresh=True)
                                if 'errors' in results:
                                    self.logger.error(str(results['errors']))
                            else:
                                remapped.append(res['bookid'])
                                mapp[0].append(res['bookid'])
                                sync_dict[res['bookid']] = item['id']
                        else:
                            self.syncinglogger.warning(f"{mapp[2]} {hc_id} not found in database")
                            bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]', str(hc_id))
                            results, _ = self.result_from_cache(bookidcmd, refresh=False)
                            if 'errors' in results:
                                self.logger.error(str(results['errors']))
                            elif 'data' in results and results['data'].get('books'):
                                newbookdict = self.get_bookdict(results['data']['books'][0])
                                in_db = lazylibrarian.librarysync.find_book_in_db(newbookdict['auth_name'],
                                                                                  newbookdict['title'],
                                                                                  source='',
                                                                                  ignored=False,
                                                                                  library='eBook',
                                                                                  reason='hc_sync %s' % hc_id)
                                if in_db and in_db[0]:
                                    cmd = "SELECT BookID,hc_id,bookname FROM books WHERE BookID=?"
                                    exists = db.match(cmd, (in_db[0],))
                                    # hc_id in database doesn't match the one in hardcover user list,
                                    # assume new hardcover id is correct and amend book table to match
                                    self.syncinglogger.debug(f"Found {mapp[2]} {hc_id} for bookid {exists['BookID']}, "
                                                             f"current hc_id {exists['hc_id']}")
                                    if exists['BookID'] in remapped:
                                        self.syncinglogger.debug(f"Duplicated entry {hc_id} in {mapp[2]} "
                                                                 f"for {exists['BookID']}")
                                        delcmd = self.HC_DELUSERBOOK.replace('[bookid]', str(item['id']))
                                        results, _ = self.result_from_cache(delcmd, refresh=True)
                                        if 'errors' in results:
                                            self.logger.error(str(results['errors']))
                                    else:
                                        remapped.append(exists['BookID'])
                                        mapp[0].append(exists['BookID'])
                                        sync_dict[exists['BookID']] = item['id']
                                        db.action("UPDATE books SET hc_id=? WHERE bookid=?",
                                                  (str(hc_id), exists['BookID']))
                                else:
                                    self.syncinglogger.debug(f"Adding to library {hc_id} {newbookdict['auth_name']} "
                                                             f"{newbookdict['title']}")
                                    self.find_book(str(hc_id))
                            else:
                                self.syncinglogger.debug(results)
            last_toread = []
            last_reading = []
            last_read = []
            last_dnf = []
            res = db.match("select SyncList from sync where UserID=? and Label=?", (userid, "hc_toread"))
            if res:
                last_toread = get_list(res['SyncList'])
                self.syncinglogger.debug(f"last to_read contains {len(last_toread)}")
            res = db.match("select SyncList from sync where UserID=? and Label=?", (userid, "hc_reading"))
            if res:
                last_reading = get_list(res['SyncList'])
                self.syncinglogger.debug(f"last reading contains {len(last_reading)}")
            res = db.match("select SyncList from sync where UserID=? and Label=?", (userid, "hc_read"))
            if res:
                last_read = get_list(res['SyncList'])
                self.syncinglogger.debug(f"last read contains {len(last_read)}")
            res = db.match("select SyncList from sync where UserID=? and Label=?", (userid, "hc_dnf"))
            if res:
                last_dnf = get_list(res['SyncList'])
                self.syncinglogger.debug(f"last dnf contains {len(last_dnf)}")

            mapping = [[hc_toread, last_toread, ll_toread, 'toread'], [hc_read, last_read, ll_haveread, 'read'],
                       [hc_reading, last_reading, ll_reading, 'reading'], [hc_dnf, last_dnf, ll_dnf, 'dnf']]

            for mapp in mapping:
                added_to_shelf = list(set(mapp[0]) - set(mapp[1]))
                removed_from_shelf = list(set(mapp[1]) - set(mapp[0]))
                added_to_ll = list(set(mapp[2]) - set(mapp[0]))
                removed_from_ll = list(set(mapp[1]) - set(mapp[2]))
                self.syncinglogger.debug(f"added_to_shelf {mapp[3]} {len(added_to_shelf)}")
                self.syncinglogger.debug(f"removed_from_shelf {mapp[3]} {len(removed_from_shelf)}")
                self.syncinglogger.debug(f"added_to_ll {mapp[3]} {len(added_to_ll)}")
                self.syncinglogger.debug(f"removed_from_ll {mapp[3]} {len(removed_from_ll)}")
                additions = set(added_to_shelf + added_to_ll)
                removals = set(removed_from_shelf + removed_from_ll)
                cnt = 0
                for item in additions:
                    if item not in mapp[2]:
                        mapp[2].append(item)
                        cnt += 1
                msg += f"Added {cnt} to {mapp[3]}" + '\n'
                cnt = 0
                for item in removals:
                    if item in mapp[2]:
                        mapp[2].remove(item)
                        cnt += 1
                msg += f"Removed {cnt} from {mapp[3]}" + '\n'

            #
            # sync changes to HC
            #
            # Use readinglist for user as master list
            # new master list for HC is set(all books in readinglists for userid)
            # old masterlist was set(all books in hc_toread, hc_reading etc)
            # Anything in old set not in new set should be deleted from hc
            # Others in new_set may have changed status
            # Books in user lists have their own id for this, not the regular id,
            # but we need the regular id for adding new books to user lists

            cmd = f"SELECT books.bookid from readinglists,books WHERE books.bookid=readinglists.bookid and userid=?"
            res = db.select(cmd, (userid,))
            new_set = set()
            for item in res:
                new_set.add(item[0])
            old_set = set(hc_toread + hc_reading + hc_read + hc_dnf)
            deleted_items = old_set - new_set
            self.syncinglogger.debug(f"Deleting {len(deleted_items)} from HardCover")
            for item in deleted_items:
                book = db.match("SELECT hc_id from books WHERE bookid=?", (item,))
                if book and book[0] and item in sync_dict:
                    delcmd = self.HC_DELUSERBOOK.replace('[bookid]', str(sync_dict[item]))
                    results, _ = self.result_from_cache(delcmd, refresh=True)
                    if 'errors' in results:
                        self.logger.error(str(results['errors']))

            cmd = (f"SELECT hc_id,readinglists.status,bookname from readinglists,books WHERE "
                   f"books.bookid=readinglists.bookid and userid=? and books.bookid=?")
            cnt = 0
            status_ids = {1: 'want-to-read', 2: 'currently-reading', 3: 'read', 4: 'owned', 5: 'dnf'}
            for item in new_set:
                res = db.match(cmd, (userid, item))
                if res and res[0]:
                    old_status = 0
                    if item in hc_toread:
                        old_status = 1
                    if item in hc_reading:
                        old_status = 2
                    elif item in hc_read:
                        old_status = 3
                    elif item in hc_dnf:
                        old_status = 5
                    if res[1] != old_status:
                        if old_status:
                            self.syncinglogger.debug(f"Setting status of HardCover {res[0]} to {status_ids[res[1]]}, "
                                                     f"(was {status_ids[old_status]})")
                            sync_id = sync_dict[item]
                        else:
                            self.syncinglogger.debug(f"Adding new entry {res[0]} {res[2]} to HardCover, "
                                                     f"status {status_ids[res[1]]}")
                            sync_id = res[0]
                        addcmd = self.HC_ADDUSERBOOK.replace('[bookid]',
                                                             str(sync_id)).replace('[status]', str(res[1]))
                        results, _ = self.result_from_cache(addcmd, refresh=True)
                        if 'errors' in results:
                            self.logger.error(str(results['errors']))
                else:
                    self.syncinglogger.error(f"Unable to update bookid {item} ({res[2]}) at HardCover, no hc_id")
                    cnt += 1
                    for mapp in mapping:
                        if item in mapp[2]:
                            mapp[2].remove(item)

            msg += f"Unable to update {cnt} items at HardCover as no hc_id\n"

            for mapp in mapping:
                # mapp[2] is now the definitive list to store as last sync for status mapp[3]
                listmsg = f"HardCover {mapp[3]} contains {len(mapp[2])}"
                self.syncinglogger.debug(listmsg)
                msg += listmsg + '\n'
                set_readinglist(mapp[3], userid, mapp[2])
                label = 'hc_' + mapp[3]
                booklist = ','.join(mapp[2])
                db.upsert("sync", {'SyncList': booklist}, {'UserID': userid, 'Label': label})

            # maybe sync ll_owned to hc_owned, but only add/delete to ll using status HAVE, not OPEN
            # and use hc_id for admin user, not ll userid and not current user
        finally:
            db.upsert("jobs", {"Finish": time.time()}, {"Name": "HCSYNC"})
            db.close()
            self.logger.debug(f"HCsync completed for {userid}")
            thread_name('WEBSERVER')
            return msg
