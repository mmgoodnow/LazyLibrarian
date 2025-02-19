import http.client
import json
import logging
import os
import platform
import re
import threading
import time
import traceback

import cherrypy
import requests
from rapidfuzz import fuzz

import lazylibrarian
from lazylibrarian import database
from lazylibrarian.blockhandler import BLOCKHANDLER
from lazylibrarian.bookwork import get_status, isbn_from_words, isbnlang
from lazylibrarian.common import get_readinglist, set_readinglist
from lazylibrarian.config2 import CONFIG
from lazylibrarian.filesystem import DIRS, path_isfile, syspath
from lazylibrarian.formatter import md5_utf8, make_unicode, is_valid_isbn, get_list, format_author_name, \
    date_format, thread_name, now, today, plural, unaccented, replace_all, check_year, check_int
from lazylibrarian.images import cache_bookimg, get_book_cover


def test_auth(userid=None):
    msg = ''
    if not userid:
        userid, msg = get_current_userid()
    if userid:
        h_c = HardCover(userid)
        msg = h_c.hc_whoami()
    return msg


def hc_api_sleep(limit=1.1):  # official limit is 60 requests per minute. limit=1.1 gives us about 55
    time_now = time.time()
    delay = time_now - lazylibrarian.TIMERS['LAST_HC']
    if delay < limit:
        sleep_time = limit - delay
        lazylibrarian.TIMERS['SLEEP_HC'] += sleep_time
        cachelogger = logging.getLogger('special.cache')
        cachelogger.debug(f"HardCover sleep {sleep_time:.3f}, total {lazylibrarian.TIMERS['SLEEP_HC']:.3f}")
        time.sleep(sleep_time)
    lazylibrarian.TIMERS['LAST_HC'] = time_now


def get_current_userid():
    userid = ''
    msg = ''
    cookie = cherrypy.request.cookie
    if 'll_uid' in list(cookie.keys()):
        userid = cookie['ll_uid'].value
    else:
        msg = 'No current userid'
    return userid, msg


def hc_sync(library='', userid=None):
    msg = ''
    if not userid:
        userid, msg = get_current_userid()
    if userid:
        hc = HardCover(userid)
        msg = hc.sync(library, userid)
    return msg


def validate_bookdict(bookdict):
    logger = logging.getLogger(__name__)
    if not bookdict.get('auth_id') or not bookdict.get('auth_name'):
        rejected = 'name', "Authorname or ID not found"
        logger.debug(f"Rejecting {bookdict.get('title')}, {rejected[1]}")
        return rejected

    # these are reject reasons we might want to override, so optionally add to database as "ignored"
    ignorable = ['future', 'date', 'isbn', 'word', 'set']
    if CONFIG.get_bool('NO_LANG'):
        ignorable.append('lang')
    rejected = False

    db = database.DBConnection()
    try:
        wantedlanguages = get_list(CONFIG['IMP_PREFLANG'])
        if 'All' not in wantedlanguages:
            lang = ''
            languages = get_list(bookdict.get('languages'))
            if languages:
                for item in languages:
                    if item in wantedlanguages:
                        lang = item
                        break
            elif bookdict.get('isbn'):
                lang, _, _ = isbnlang(bookdict['isbn'])

            if not lang:
                lang = 'Unknown'

            if lang not in wantedlanguages:
                rejected = 'lang', f'Invalid language [{lang}]'

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
                                                                  reason=f"hc_get_author_books {bookdict['auth_id']},"
                                                                         f"{bookdict['title']}")
                if not in_db:
                    in_db = lazylibrarian.librarysync.find_book_in_db(bookdict['auth_name'], bookdict['title'],
                                                                      source='bookid', ignored=False, library='eBook',
                                                                      reason=f"hc_get_author_books "
                                                                             f"{bookdict['auth_id']},"
                                                                             f"{bookdict['title']}")
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
                    rejected = 'dupe', f"Duplicate id ({bookdict['bookid']}/{exists['BookID']})"
                    if not exists['hc_id']:
                        db.action("UPDATE books SET hc_id=? WHERE BookID=?", (bookdict['bookid'],
                                                                              exists['BookID']))

            if not rejected and bookdict['isbn'] and CONFIG.get_bool('ISBN_LOOKUP'):
                # try isbn lookup by name
                title = bookdict.get('title')
                if title:
                    try:
                        res = isbn_from_words(
                            f"{unaccented(title, only_ascii=False)} "
                            f"{unaccented(bookdict['auth_name'], only_ascii=False)}")
                    except Exception as e:
                        res = None
                        logger.warning(f"Error from isbn: {e}")
                    if res:
                        logger.debug(f"isbn found {res} for {bookdict['bookid']}")
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
                        rejected = 'word', f'Name contains [{word}]'
                        break

            if not rejected:
                book_name = unaccented(bookdict['title'], only_ascii=False)
                if CONFIG.get_bool('NO_SETS'):
                    # allow date ranges eg 1981-95
                    m = re.search(r'(\d+)-(\d+)', book_name)
                    if m:
                        if check_year(m.group(1), past=1800, future=0):
                            logger.debug(f"Allow {book_name}, looks like a date range")
                        else:
                            rejected = 'set', f'Set or Part {m.group(0)}'
                    if re.search(r'\d+ of \d+', book_name) or \
                            re.search(r'\d+/\d+', book_name) and not re.search(r'\d+/\d+/\d+', book_name):
                        rejected = 'set', 'Set or Part'
                    elif re.search(r'\w+\s*/\s*\w+', book_name):
                        rejected = 'set', 'Set or Part'
                    if rejected:
                        logger.debug(f'Rejected {book_name}, {rejected[1]}')
                if rejected and rejected[0] not in ignorable:
                    logger.debug(f"Rejecting {bookdict['title']}, {rejected[1]}")
            elif rejected and not (rejected[0] in ignorable and CONFIG.get_bool('IMP_IGNORE')):
                logger.debug(f"Rejecting {bookdict['title']}, {rejected[1]}")
            else:
                logger.debug(f"Found title: {bookdict['title']}")
                if not rejected and CONFIG.get_bool('NO_FUTURE'):
                    publish_date = bookdict.get('publish_date')
                    if publish_date > today()[:len(publish_date)]:
                        rejected = 'future', f'Future publication date [{publish_date}]'
                        if ignorable is None:
                            logger.debug(f"Rejecting {bookdict['title']}, {rejected[1]}")
                        else:
                            logger.debug(
                                f"Not rejecting {bookdict['title']} (future pub date {publish_date}) as {ignorable}")
                    if not rejected and CONFIG.get_bool('NO_PUBDATE'):
                        if not publish_date or publish_date == '0000':
                            rejected = 'date', 'No publication date'
                    if ignorable is None:
                        logger.debug(f"Rejecting {bookdict['title']}, {rejected[1]}")
                    else:
                        logger.debug(f"Not rejecting {bookdict['title']} (no pub date) as {ignorable}")
    except Exception:
        logger.error(f'Unhandled exception in validate_bookdict: {traceback.format_exc()}')
        logger.error(f"{bookdict}")
    finally:
        db.close()
        return rejected


class HardCover:
    def __init__(self, name=''):

        self.hc_url = 'https://api.hardcover.app/'
        self.graphql_url = f"{self.hc_url}v1/graphql"
        self.book_url = f"{self.hc_url.replace('api.', '')}books/"
        self.auth_url = f"{self.hc_url.replace('api.', '')}authors/"
        self.HC_WHOAMI = 'query whoami { me { id } }'
        # this will need changing when we get access to other users book lists
        self.apikey = CONFIG.get_str('HC_API')

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
    slug
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

        # queries using bookid, authorid, seriesid should be faster if we query using _by_pk e.g.
        # query books_by_pk rather than query books but at the moment that's not the case.

        self.HC_BOOKID_BOOKS = self.HC_FINDBOOK.replace('books([order] where: [where])', 'books_by_pk(id: [bookid])')

        self.HC_AUTHORID_BOOKS = self.HC_FINDBOOK.replace('[where]',
                                                          '{contributions: {author: {id: {_eq: "[authorid]"}}}}'
                                                          ).replace('[order]', '')

        self.HC_EDITION_BY_PK = '''
query EditionByPk {
  editions_by_pk(id: [editionid]) {
    language {
      language
    }
    title
    book_id
    contributions {
      author {
        id
        name
      }
    }
  }
}
'''
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

        # 5 deep because of default_physical_edition/language/language and contributions/author/id
        # when hardcover implement the 3 level deep limit we will have to get the additional data
        # for each series member using default_physical_edition_id to get edition_by_pk but it's much slower...
        self.HC_BOOK_SERIES_BY_PK = '''
query SeriesByPK {
  series_by_pk(id: "[seriesid]") {
    id
    name
    books_count
    primary_books_count
    book_series
        (where: {book: {book_status_id: {_eq: "1"}, compilation: {_eq: false}}}
        order_by: [{position: asc}, {book: {users_count: desc}}])
    {
      book_id
      position
      book {
        title
        release_date
        release_year
        default_physical_edition{
          language {
            language
          }
        }
        contributions {
          author {
            id
            name
          }
        }
        compilation
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
        self.HC_AUTHORINFO = '''
query FindAuthor { authors_by_pk(id: [authorid])
  {
    id
    name
    death_year
    death_date
    born_year
    born_date
    bio
    cached_image
    slug
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
            self.user_agent += f"{platform.system()} {platform.release()}"
        self.user_agent += ')'

    def is_in_cache(self, expiry: int, hashfilename: str, myhash: str) -> bool:
        if path_isfile(hashfilename):
            cache_modified_time = os.stat(hashfilename).st_mtime
            time_now = time.time()
            if expiry and cache_modified_time < time_now - expiry:
                # Cache entry is too old, delete it
                self.cachelogger.debug(f"Expiring {myhash}")
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
        hashfilename = os.path.join(cache_location, myhash[0], myhash[1], f"{myhash}.json")
        return hashfilename, myhash

    def result_from_cache(self, searchcmd: str, refresh=False) -> (str, bool):
        headers = {'Content-Type': 'application/json',
                   'User-Agent': self.user_agent,
                   'authorization': self.apikey
                   }
        query = {'query': searchcmd}
        cache_location = DIRS.get_cachedir('JSONCache')
        filename = f"{self.graphql_url}/{str(query)}"
        hashfilename, myhash = self.get_hashed_filename(cache_location, filename)
        # CACHE_AGE is in days, so get it to seconds
        expire_older_than = CONFIG.get_int('CACHE_AGE') * 24 * 60 * 60
        valid_cache = self.is_in_cache(expire_older_than, hashfilename, myhash)
        if valid_cache and not refresh:
            lazylibrarian.CACHE_HIT += 1
            self.cachelogger.debug(f"CacheHandler: Returning CACHED response {hashfilename}")
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
                self.cachelogger.debug(f"CacheHandler: Storing json {myhash}")
                with open(syspath(hashfilename), "w") as cachefile:
                    cachefile.write(json.dumps(res))
            else:
                # expected failure codes...
                # 401 expired or invalid api token
                # 403 blocked action (invalid query type, depth limit exceeded, multiple queries)
                # 429 rate limit of 60 per minute exceeded
                # 500 internal server error
                # On 429 error we should get headers
                # RateLimit-Limit 60 (requests per minute)
                # RateLimit-Remaining 0 (none left)
                # RateLimit-Reset 1735843440 (unix seconds count when reset)
                delay = 0
                if r.status_code == 429:
                    limit = r.headers.get('RateLimit-Limit', 'Unknown')
                    remaining = r.headers.get('RateLimit-Remaining', 'Unknown')
                    reset = r.headers.get('RateLimit-Reset', 'Unknown')
                    sleep_time = 0.0
                    if str(reset).isdigit():
                        sleep_time = reset - time.time()
                        reset = time.strftime("%H:%M:%S", time.localtime(reset))
                    self.logger.debug(f"429 error. Limit {limit}, Remaining {remaining}, Reset {reset}")
                    if sleep_time > 0.0:
                        if sleep_time < 5.0:  # short waits just sleep a bit
                            time.sleep(sleep_time)
                            lazylibrarian.TIMERS['SLEEP_HC'] += sleep_time
                        else:  # longer waits block provider and continue
                            delay = int(sleep_time)
                elif r.status_code in [401, 403]:
                    # allow time for user to update
                    delay = 24 * 3600
                elif r.status_code == 500:
                    # time for hardcover to fix error
                    delay = 2 * 3600
                else:
                    # unexpected error code, short delay
                    delay = 60
                try:
                    res = r.json()
                    msg = str(r.status_code)
                except Exception:
                    res = {}
                    msg = "Unknown reason"
                if 'error' in res:
                    msg = str(res['error'])
                    self.logger.error(msg)
                if delay:
                    BLOCKHANDLER.block_provider(self.provider, msg, delay=delay)
        return res, valid_cache

    def get_series_members(self, series_ident=None, series_title='', queue=None, refresh=False):
        resultlist = []
        resultdict = {}
        author_name = ''
        api_hits = 0
        cache_hits = 0
        searchcmd = self.HC_BOOK_SERIES_BY_PK.replace('[seriesid]', str(series_ident)[2:])
        results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
        api_hits += not in_cache
        cache_hits += in_cache
        if 'error' in results:
            self.logger.error(str(results['error']))
        if 'data' in results and 'series_by_pk' in results['data'] and results['data']['series_by_pk']:
            wantedlanguages = get_list(CONFIG['IMP_PREFLANG'])
            series_id = f"HC{str(results['data']['series_by_pk']['id'])}"
            if series_id != series_ident:
                self.logger.debug(f"Series id mismatch for {series_id}, {series_ident}")
            series_name = results['data']['series_by_pk']['name']
            # primary_books_count = results['data']['series_by_pk']['primary_books_count']
            if series_name != series_title:
                match = fuzz.partial_ratio(series_name, series_title)
                if match < 95:
                    self.logger.debug(f"Series name mismatch for {series_id}, {match}% {series_name}/{series_title}")

            for entry in results['data']['series_by_pk']['book_series']:
                # use HC_EDITION_BY_PK to get language, authorid, authorname
                # editionid = entry['book']['default_physical_edition_id']
                # searchcmd = self.HC_EDITION_BY_PK.replace('[editionid]', str(editionid))
                # editions, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
                # api_hits += not in_cache
                # cache_hits += in_cache
                # language = ''
                # authorname = ''
                # authorlink = ''
                # if 'errors' in editions:
                #    self.logger.error(str(editions['errors']))
                # elif 'data' in editions and 'editions_by_pk' in editions['data']:
                #    edition = editions['data']['editions_by_pk']
                #    if edition['language']:
                #        language = edition['language']['language']
                #    if edition['contributions']:
                #        authorname = edition['contributions'][0]['author']['name']
                #        authorlink = edition['contributions'][0]['author']['id']

                authorname = entry['book']['contributions'][0]['author']['name']
                authorlink = entry['book']['contributions'][0]['author']['id']
                edition = entry['book']['default_physical_edition']
                language = ''
                if edition and 'language' in edition and edition.get('language'):
                    language = edition['language']['language']

                book_id = entry['book_id']
                position = entry['position']
                if not position or str(position) == 'None':
                    position = 0
                book_title = entry['book']['title']
                pubyear = entry['book']['release_year']
                pubdate = entry['book']['release_date']
                compilation = entry['book']['compilation']

                if not author_name:
                    author_name = authorname
                # pick the first entry for each position that is non compilation and in a language we want
                if not compilation and position and (position not in resultdict or
                                                     resultdict[position][1] != author_name):
                    if not language:
                        language = 'Unknown'
                    if 'All' in wantedlanguages or language in wantedlanguages:
                        resultdict[position] = [book_title, authorname, authorlink, book_id, pubyear, pubdate]
            for item in resultdict:
                res = [item]
                res.extend(resultdict[item])
                resultlist.append(res)
            resultlist = sorted(resultlist)
            self.logger.debug(f"Found {len(resultlist)} for series {series_id}: {series_name}")
            self.logger.debug(f"Used {api_hits} api hit, {cache_hits} in cache")

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
                self.searchinglogger.debug(f"ISBN_BOOKS {searchterm}")
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
                    self.searchinglogger.debug(f"FINDBOOKBYNAME {searchtitle}")
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
                    self.searchinglogger.debug(f"FINDAUTHORBYNAME {searchauthorname}")
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
                        self.searchinglogger.debug(f"AUTHORID_BOOKS {authid}")
                        results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
                        api_hits += not in_cache
                        cache_hits += in_cache
                        if 'error' in results:
                            self.logger.error(str(results['error']))
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
                    bookdict = self.build_bookdict(book_data)

                if searchauthorname:
                    author_fuzz = fuzz.token_sort_ratio(bookdict['auth_name'], searchauthorname)
                else:
                    author_fuzz = fuzz.token_sort_ratio(bookdict['auth_name'], searchterm)
                book_title = bookdict['title']
                if book_title:
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

                    highest_fuzz = max((author_fuzz + book_fuzz) / 2, isbn_fuzz)

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

            self.logger.debug(f"Used {api_hits} api hit, {cache_hits} in cache")
            queue.put(resultlist)

        except Exception:
            self.logger.error(f'Unhandled exception in HC.find_results: {traceback.format_exc()}')

    def find_author_id(self, refresh=False):
        api_hits = 0
        authorname = self.name.replace('#', '').replace('/', '_')
        authorname = format_author_name(authorname, postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
        title = self.title

        if not title:
            # we only have an authorname. Return id of matching author with the most books
            self.logger.debug(f"Searching for author {authorname}, refresh={refresh}")
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
                    self.logger.debug(f"Authorname search used {api_hits} api hit")
                    return res
        else:
            # search for the title and then check the authorname matches
            self.logger.debug(f"Searching for title {title}, refresh={refresh}")
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
                            self.logger.debug(f"Title search used {api_hits} api hit")
                            return res

                # get the authorid from the book page as it's not in the title search results
                bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]', str(bookid))
                results, in_cache = self.result_from_cache(bookidcmd, refresh=refresh)
                api_hits += not in_cache
                if results and 'data' in results:
                    book_data = results['data'].get('books_by_pk', {})
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
                            self.logger.debug(f"Author/book search used {api_hits} api hit")
                            return res

        self.logger.debug(f"No results. Used {api_hits} api hit")
        return {}

    def get_author_info(self, authorid=None, refresh=False):
        author_name = ''
        author_born = ''
        author_died = ''
        author_link = ''
        author_img = ''
        about = ''
        totalbooks = 0
        api_hits = 0
        cache_hits = 0

        self.logger.debug(f"Getting HC author info for {authorid}, refresh={refresh}")
        searchcmd = self.HC_AUTHORINFO.replace('[authorid]', str(authorid))
        results, in_cache = self.result_from_cache(searchcmd, refresh=refresh)
        api_hits += not in_cache
        cache_hits += in_cache
        if 'error' in results:
            self.logger.error(str(results['error']))
        if results and 'data' in results:
            author = results['data'].get('authors_by_pk', {})
            if author and str(author['id']) == str(authorid):
                author_name = author.get('name', '')
                # hc sometimes returns multiple comma separated names, use the one we are looking for
                if self.name and self.name in author_name:
                    author_name = self.name
                author_born = author.get('born_date', '')
                author_died = author.get('death_date', '')
                totalbooks = author.get('books_count', 0)
                about = author.get('bio', '')
                author_link = author.get('slug', '')
                if author_link:
                    author_link = self.auth_url + author_link

                if 'cached_image' in author:
                    img = author['cached_image'].get('url', '')
                    if img and '/books/' not in img:
                        # hardcover image bug, sometimes gives us a book cover instead of author image
                        author_img = author['cached_image']['url']

        if "," in author_name:
            postfix = get_list(CONFIG.get_csv('NAME_POSTFIX'))
            words = author_name.split(',')
            if len(words) == 2:
                if words[0].strip().strip('.').lower in postfix:
                    author_name = f"{words[1].strip()} {words[0].strip()}"
                else:
                    author_name = author_name.split(',')[0]

        if not author_name:
            self.logger.warning(f"Rejecting authorid {authorid}, no authorname")
            return {}

        self.logger.debug(f"[{author_name}] Returning HC info for authorID: {authorid}")
        author_dict = {
            'authorid': str(authorid),
            'authorlink': author_link,
            'authorborn': author_born,
            'authordeath': author_died,
            'authorimg': author_img,
            'about': about,
            'totalbooks': totalbooks,
            'authorname': format_author_name(author_name, postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
        }
        self.logger.debug(f"AuthorInfo used {api_hits} api hit, {cache_hits} in cache")
        return author_dict

    def build_bookdict(self, book_data):
        bookdict = {'languages': '', 'publishers': '', 'auth_name': '', 'auth_id': '0',
                    'cover': '', 'isbn': '', 'series': []}
        if 'contributions' in book_data and len(book_data['contributions']):
            author = book_data['contributions'][0]
            bookdict['auth_name'] = " ".join(author['author']['name'].split())
            bookdict['auth_id'] = str(author['author']['id'])
        bookdict['title'] = book_data.get('title', '')
        bookdict['subtitle'] = book_data.get('subtitle', '')
        if 'cached_image' in book_data and book_data['cached_image'].get('url'):
            bookdict['cover'] = book_data['cached_image']['url']
        editions = book_data.get('editions', [])
        for edition in editions:
            if edition.get('isbn_13'):
                bookdict['isbn'] = edition['isbn_13']
                break
            if edition.get('isbn_10'):
                bookdict['isbn'] = edition['isbn_10']
                break
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
        langs = []
        for edition in editions:
            if edition.get('language'):
                lang = edition['language']['language']
                if lang:
                    langs.append(lang)
        if langs:
            bookdict['languages'] = ', '.join(set(langs))
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
        lt_lang_hits = 0
        book_ignore_count = 0
        bad_lang = 0
        added_count = 0
        not_cached = 0
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
            if 'error' in results:
                self.logger.error(str(results['error']))
            if not results or 'data' not in results:
                db.action("UPDATE authors SET Status=? WHERE AuthorID=?", (entrystatus, authorid))
                return

            self.logger.debug(f"HC found {len(results['data']['books'])} results")
            for book_data in results['data']['books']:
                bookdict = self.build_bookdict(book_data)
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
                        reason = f"Ignored: {rejected[1]}"
                        rejected = ''
                    elif rejected[0] == 'lang':
                        bad_lang += 1
                    elif rejected[0] == 'dupe':
                        duplicates += 1
                    elif rejected[0] in ['name', 'publisher']:
                        removed_results += 1

                elif 'author_update' in entryreason:
                    reason = f"Author: {bookdict['auth_name']}"
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
                        self.logger.debug(f"Inserting new book [{bookdict['title']}] for [{bookdict['auth_name']}]")
                        if 'author_update' in entryreason:
                            reason = f"Author: {bookdict['auth_name']}"
                        else:
                            reason = entryreason
                        reason = f"[{thread_name()}] {reason}"
                        added_count += 1
                        if not bookdict['languages']:
                            bookdict['languages'] = 'Unknown'
                            if bookdict['isbn']:
                                booklang, cache_hit, thing_hit = isbnlang(bookdict['isbn'])
                                if thing_hit:
                                    lt_lang_hits += 1
                                if booklang:
                                    bookdict['languages'] = booklang

                        cover_link = bookdict['cover']
                        if 'nocover' in cover_link or 'nophoto' in cover_link:
                            start = time.time()
                            cover_link, _ = get_book_cover(bookdict['bookid'], ignore='hardcover')
                            cover_time += (time.time() - start)
                            cover_count += 1
                        elif cover_link and cover_link.startswith('http'):
                            cover_link = cache_bookimg(cover_link, bookdict['bookid'], 'hc')
                        if not cover_link:  # no results on search or failed to cache it
                            cover_link = 'images/nocover.png'

                        db.action(
                            f"INSERT INTO books (AuthorID, BookName, BookImg, BookLink, BookID, BookDate, "
                            f"BookLang, BookAdded, Status, WorkPage, AudioStatus, ScanResult, OriginalPubDate, "
                            f"hc_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                            self.logger.debug(
                                f"valid bookdate [{bookdict['publish_date']}] previous scanresult "
                                f"[{exists['ScanResult']}]")

                            update_value_dict["ScanResult"] = f"bookdate {bookdict['publish_date']} is now valid"
                        elif not exists:
                            update_value_dict["ScanResult"] = reason

                        if "ScanResult" in update_value_dict:
                            self.searchinglogger.debug(f"entry status {entrystatus} {bookstatus},{audiostatus}")
                            book_status, audio_status = get_status(bookdict['bookid'], serieslist, bookstatus,
                                                                   audiostatus, entrystatus)
                            self.searchinglogger.debug(f"status is now {book_status},{audio_status}")
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
                    msg = (f"[{bookdict['auth_name']}] {typ} book: {bookdict['title']} [{bookdict['languages']}] "
                           f"status {bookstatus}")
                    if CONFIG.get_bool('AUDIO_TAB'):
                        msg += f" audio {audiostatus}"
                    self.logger.debug(msg)

                    if CONFIG.get_bool('ADD_SERIES') and bookdict.get('series'):
                        for item in bookdict['series']:
                            ser_name = item[0].strip()
                            ser_id = f"HC{str(item[1])}"
                            exists = db.match("SELECT * from series WHERE seriesid=?", (ser_id,))
                            if not exists:
                                exists = db.match("SELECT * from series WHERE seriesname=? "
                                                  "and instr(seriesid, 'HC') = 1", (ser_name,))
                            if not exists:
                                self.logger.debug(f"New series: {ser_id}:{ser_name}: {CONFIG['NEWSERIES_STATUS']}")
                                db.action('INSERT INTO series (SeriesID, SeriesName, Status, '
                                          'Updated, Reason) VALUES (?,?,?,?,?)',
                                          (ser_id, ser_name, CONFIG['NEWSERIES_STATUS'], time.time(), ser_name))
                                db.commit()
                                exists = {'Status': CONFIG['NEWSERIES_STATUS']}

                            # books in series might be by different authors
                            match = db.match(f"SELECT AuthorID from authors WHERE authorid=? or hc_id=?",
                                             (bookdict['auth_id'], bookdict['auth_id']))
                            if match:
                                auth_id = match['AuthorID']
                            else:
                                auth_id = authorid

                            authmatch = db.match(f"SELECT * from seriesauthors WHERE "
                                                 f"SeriesID=? and AuthorID=?", (ser_id, auth_id))
                            if not authmatch:
                                self.logger.debug(f"Adding {bookdict['auth_name']} as series author for {ser_name}")
                                db.action('INSERT INTO seriesauthors (SeriesID, AuthorID) VALUES (?, ?)',
                                          (ser_id, auth_id), suppress='UNIQUE')

                            match = db.match(f"SELECT * from member WHERE SeriesID=? AND BookID=?",
                                             (ser_id, bookdict['bookid']))
                            if not match:
                                self.logger.debug(f"Inserting new member [{item[2]}] for {ser_id}")
                                db.action(
                                    f"INSERT INTO member (SeriesID, BookID, WorkID, SeriesNum) VALUES (?,?,?,?)",
                                    (ser_id, bookdict['bookid'], '', item[2]), suppress='UNIQUE')
                            ser = db.match(
                                f"select count(*) as counter from member where seriesid=?",
                                (ser_id,))
                            if ser:
                                counter = check_int(ser['counter'], 0)
                                db.action("UPDATE series SET Total=? WHERE SeriesID=?",
                                          (counter, ser_id))

                            if exists['Status'] in ['Paused', 'Ignored']:
                                self.logger.debug(
                                    f"Not getting additional series members for {ser_name}, status is "
                                    f"{exists['Status']}")
                            elif ser_id in series_updates:
                                self.logger.debug(f"Series {ser_id}:{ser_name} already updated")
                            else:
                                seriesmembers = self.get_series_members(ser_id, ser_name)
                                series_updates.append(ser_id)
                                if len(seriesmembers) == 1:
                                    self.logger.debug(f"Found member {seriesmembers[0][1]} for series {ser_name}")
                                else:
                                    self.logger.debug(f"Found {len(seriesmembers)} members for series {ser_name}")
                                # position, book_title, author_name, hc_author_id, book_id
                                for member in seriesmembers:
                                    db.action("DELETE from member WHERE SeriesID=? AND SeriesNum=?",
                                              (ser_id, member[0]))
                                    auth_name, exists = lazylibrarian.importer.get_preferred_author_name(member[2])
                                    if not exists:
                                        reason = f"Series contributing author {ser_name}:{member[1]}"
                                        lazylibrarian.importer.add_author_name_to_db(author=member[2],
                                                                                     refresh=False,
                                                                                     addbooks=False,
                                                                                     reason=reason
                                                                                     )
                                        auth_name, exists = lazylibrarian.importer.get_preferred_author_name(member[2])
                                        if not exists:
                                            self.logger.debug(f"Unable to add {member[2]} for {member[1]}, "
                                                              f"author not in database")
                                            continue

                                    cmd = "SELECT * from authors WHERE authorname=? or hc_id=?"
                                    exists = db.match(cmd, (auth_name, member[3]))
                                    if exists:
                                        auth_id = exists['AuthorID']
                                        if fuzz.ratio(auth_name.lower().replace('.', ''),
                                                      member[2].lower().replace('.', '')) < 95:
                                            akas = get_list(exists['AKA'], ',')
                                            if member[2] not in akas:
                                                akas.append(member[2])
                                                db.action("UPDATE authors SET AKA=? WHERE AuthorID=?",
                                                          (', '.join(akas), auth_id))
                                        match = db.match(
                                            f"SELECT * from seriesauthors WHERE SeriesID=? and AuthorID=?", (ser_id, auth_id))
                                        if not match:
                                            self.logger.debug(f"Adding {auth_name} as series author for {ser_name}")
                                            new_authors += 1
                                            db.action('INSERT INTO seriesauthors (SeriesID, AuthorID) VALUES (?, ?)',
                                                      (ser_id, auth_id), suppress='UNIQUE')

                                    cmd = "SELECT BookID FROM books WHERE BookID=?"
                                    # make sure bookid is in database, if not, add it
                                    match = db.match(cmd, (str(member[4]),))
                                    if not match:
                                        newbookdict, in_cache = self.get_bookdict(str(member[4]))
                                        api_hits += not in_cache
                                        cache_hits += in_cache
                                        if not newbookdict:
                                            self.logger.debug(f"Unable to add bookid {member[4]} to database")
                                            continue

                                        cover_link = newbookdict['cover']
                                        if 'nocover' in cover_link or 'nophoto' in cover_link:
                                            start = time.time()
                                            cover_link, _ = get_book_cover(newbookdict['bookid'],
                                                                           ignore='hardcover')
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

                                        if (not newbookdict.get('book_status') or not
                                                newbookdict.get('audio_status')):
                                            newbookdict['book_status'], newbookdict['audio_status']\
                                                = get_status(bookdict['bookid'], serieslist,
                                                             bookstatus, audiostatus, entrystatus)
                                        db.action(cmd, (auth_id, newbookdict['title'],
                                                        cover_link, newbookdict['link'],
                                                        newbookdict['bookid'],
                                                        newbookdict['publish_date'],
                                                        newbookdict['languages'], now(),
                                                        newbookdict['book_status'], '',
                                                        newbookdict['audio_status'], reason,
                                                        newbookdict['first_publish_year'],
                                                        newbookdict['bookid']))

                                    self.logger.debug(
                                        f"Inserting new member [{member[0]}] for {ser_name}")
                                    db.action('INSERT INTO member (SeriesID, BookID, SeriesNum) VALUES (?,?,?)',
                                              (ser_id, member[4], member[0]), suppress="UNIQUE")

                                    ser = db.match(f"select count(*) as counter from member where seriesid=?",
                                                   (ser_id,))
                                    if ser:
                                        counter = check_int(ser['counter'], 0)
                                        db.action("UPDATE series SET Total=? WHERE SeriesID=?", (counter, ser_id))

                                    lazylibrarian.importer.update_totals(auth_id)

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
            self.logger.debug(f"Found {locked_count} locked {plural(locked_count, 'book')}")
            self.logger.debug(f"Added {new_authors} new {plural(new_authors, 'author')}")
            self.logger.debug(f"Removed {bad_lang} unwanted language {plural(bad_lang, 'result')}")
            self.logger.debug(f"Removed {removed_results} incorrect/incomplete {plural(removed_results, 'result')}")
            self.logger.debug(f"Removed {duplicates} duplicate {plural(duplicates, 'result')}")
            self.logger.debug(f"Ignored {book_ignore_count} {plural(book_ignore_count, 'book')}")
            self.logger.debug(
                f"Imported/Updated {resultcount} {plural(resultcount, 'book')} in "
                f"{int(time.time() - auth_start)} secs using {api_hits} api {plural(api_hits, 'hit')}")
            if cover_count:
                self.logger.debug(f"Fetched {cover_count} {plural(cover_count, 'cover')} in {cover_time:.2f} sec")

            control_value_dict = {"authorname": entry_name.replace('"', '""')}
            new_value_dict = {
                "GR_book_hits": api_hits,
                "GR_lang_hits": 0,
                "LT_lang_hits": lt_lang_hits,
                "GB_lang_change": 0,
                "cache_hits": cache_hits,
                "bad_lang": bad_lang,
                "bad_char": removed_results,
                "uncached": not_cached,
                "duplicates": duplicates
            }
            db.upsert("stats", new_value_dict, control_value_dict)
        finally:
            db.close()

    def get_bookdict(self, bookid=None):
        bookidcmd = self.HC_BOOKID_BOOKS.replace('[bookid]', str(bookid))
        results, in_cache = self.result_from_cache(bookidcmd, refresh=False)
        bookdict = {}
        if 'error' in results:
            self.logger.error(str(results['error']))
        if 'data' in results and results['data'].get('books_by_pk'):
            bookdict = self.build_bookdict(results['data']['books_by_pk'])
        return bookdict, in_cache

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='hc.find_book'):
        bookdict, _ = self.get_bookdict(bookid)
        if not bookstatus:
            bookstatus = CONFIG['NEWBOOK_STATUS']
            self.logger.debug(f"No bookstatus passed, using default {bookstatus}")
        if not audiostatus:
            audiostatus = CONFIG['NEWAUDIO_STATUS']
            self.logger.debug(f"No audiostatus passed, using default {audiostatus}")
        self.logger.debug(f"bookstatus={bookstatus}, audiostatus={audiostatus}")
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
                msg = f'Book {title} authorname invalid'
            elif rejected[0] == 'lang':
                msg = f'Book {title} Language [{lang}] does not match preference'

            elif rejected[0] in ['publisher']:
                msg = f'Book {title} Publisher [{lang}] does not match preference'

            elif CONFIG.get_bool('NO_PUBDATE'):
                if not bookdate or bookdate == '0000':
                    msg = f'Book {title} Publication date [{bookdate}] does not match preference'

            elif CONFIG.get_bool('NO_FUTURE'):
                # may have yyyy or yyyy-mm-dd
                if bookdate > today()[:len(bookdate)]:
                    msg = f'Book {title} Future publication date [{bookdate}] does not match preference'

            elif CONFIG.get_bool('NO_SETS'):
                if re.search(r'\d+ of \d+', title) or re.search(r'\d+/\d+', title):
                    msg = f'Book {title} Set or Part'
                # allow date ranges eg 1981-95
                m = re.search(r'(\d+)-(\d+)', title)
                if m:
                    if check_year(m.group(1), past=1800, future=0):
                        self.logger.debug(f"Allow {m.group(1)}, looks like a date range")
                        msg = ''
            if msg:
                self.logger.warning(f"{msg} : adding anyway")

        auth_name, exists = lazylibrarian.importer.get_preferred_author_name(bookdict['auth_name'])
        if not exists:
            reason = f"{reason}:{bookdict['bookid']}"
            lazylibrarian.importer.add_author_name_to_db(author=bookdict['auth_name'], refresh=False,
                                                         addbooks=False, reason=reason)
        auth_name, exists = lazylibrarian.importer.get_preferred_author_name(bookdict['auth_name'])
        if not exists:
            self.logger.debug(f"Unable to add {bookdict['auth_name']} for {bookdict['bookid']}, author not found")
        else:
            db = database.DBConnection()
            cmd = "SELECT * from authors WHERE authorname=?"
            exists = db.match(cmd, (auth_name,))
            if not exists:
                self.logger.debug(
                    f"Unable to add {bookdict['auth_name']} for {bookdict['bookid']}, author not in database")
            else:
                auth_id = exists['AuthorID']
                cover_link = bookdict['cover']
                if 'nocover' in cover_link or 'nophoto' in cover_link:
                    cover_link, _ = get_book_cover(bookdict['bookid'], ignore='hardcover')
                elif cover_link and cover_link.startswith('http'):
                    cover_link = cache_bookimg(cover_link, bookdict['bookid'], 'hc')
                if not cover_link:  # no results on search or failed to cache it
                    cover_link = 'images/nocover.png'
                db.action(
                    f"INSERT INTO books (AuthorID, BookName, BookImg, BookLink, BookID, BookDate, BookLang, "
                    f"BookAdded, Status, WorkPage, AudioStatus, ScanResult, OriginalPubDate, hc_id) "
                    f"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (auth_id, bookdict['title'], cover_link, bookdict['link'],
                     bookdict['bookid'], bookdict['publish_date'], bookdict['languages'], now(),
                     bookdict['book_status'], '', bookdict['audio_status'], reason,
                     bookdict['first_publish_year'], bookdict['bookid']))
            db.close()

        return

    def hc_whoami(self, userid=None):
        msg = ''
        if not userid:
            userid, msg = get_current_userid()
        if not userid:
            return msg

        #   Read the users bearer token here and pass to self.apikey

        searchcmd = self.HC_WHOAMI
        results, _ = self.result_from_cache(searchcmd, refresh=True)
        if 'error' in results:
            self.logger.error(str(results['error']))
            return str(results['error'])
        if 'data' in results and 'me' in results['data']:
            # this is the hc_id tied to the api bearer token
            res = results['data']['me']
            whoami = res[0]['id']
            if whoami:
                db = database.DBConnection()
                db.upsert("users", {'hc_id': whoami}, {'UserID': userid})
                return whoami
        return str(results)

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
        miss = []
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
            if 'error' in results:
                self.logger.error(str(results['error']))
            elif 'data' in results and 'me' in results['data']:
                # this is the hc_id tied to the api bearer token
                res = results['data']['me']
                whoami = res[0]['id']
                db.upsert("users", {'hc_id': whoami}, {'UserID': userid})
                if not whoami:
                    self.logger.error(f"No hc_id for user {userid}")
                    return msg

            self.syncinglogger.debug(f"whoami = {whoami}")
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
                if 'error' in results:
                    self.logger.error(str(results['error']))
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
                                if 'error' in results:
                                    self.logger.error(str(results['error']))
                            else:
                                remapped.append(res['bookid'])
                                mapp[0].append(res['bookid'])
                                sync_dict[res['bookid']] = item['id']
                        else:
                            self.syncinglogger.warning(f"{mapp[2]} {hc_id} not found in database")
                            newbookdict, in_cache = self.get_bookdict(str(hc_id))
                            if newbookdict:
                                in_db = lazylibrarian.librarysync.find_book_in_db(newbookdict['auth_name'],
                                                                                  newbookdict['title'],
                                                                                  source='',
                                                                                  ignored=False,
                                                                                  library='eBook',
                                                                                  reason=f'hc_sync {hc_id}')
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
                                        if 'error' in results:
                                            self.logger.error(str(results['error']))
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
                                self.syncinglogger.debug(f"No bookdict found for {hc_id}")
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
                msg += f"Added {cnt} to {mapp[3]}\n"
                cnt = 0
                for item in removals:
                    if item in mapp[2]:
                        mapp[2].remove(item)
                        cnt += 1
                msg += f"Removed {cnt} from {mapp[3]}\n"

            added_to_wanted = list(set(hc_toread) - set(last_toread))
            if added_to_wanted:
                ebook_wanted = []
                audio_wanted = []
                cmd = "select Status,AudioStatus,BookName from books where hc_id=?"
                for item in added_to_wanted:
                    res = db.match(cmd, (item,))
                    if not res:
                        self.syncinglogger.warning(f'Book {item} not found in database')
                    if res and CONFIG.get_bool('EBOOK_TAB') and CONFIG['NEWBOOK_STATUS'] not in ['Ignored']:
                        if res['Status'] not in ['Wanted', 'Have', 'Open']:
                            db.action("update books set status='Wanted' where bookid=?", (item,))
                            self.syncinglogger.debug(f"Marked ebook {item} wanted")
                            ebook_wanted.append({"bookid": item})
                        else:
                            self.syncinglogger.debug(f"ebook {item} already marked {res['Status']}")
                    if res and CONFIG.get_bool('AUDIO_TAB') and CONFIG['NEWAUDIO_STATUS'] not in ['Ignored']:
                        if res['AudioStatus'] not in ['Wanted', 'Have', 'Open']:
                            db.action("update books set audiostatus='Wanted' where bookid=?", (item,))
                            self.syncinglogger.debug(f"Marked audiobook {item} wanted")
                            audio_wanted.append({"bookid": item})
                        else:
                            self.syncinglogger.debug(f"audiobook {item} already marked {res['AudioStatus']}")
                if ebook_wanted:
                    self.syncinglogger.debug(f"Searching for {len(ebook_wanted)} {plural(len(ebook_wanted), 'ebook')}")
                    threading.Thread(target=lazylibrarian.searchrss.search_rss_book, name='HCSYNCRSSBOOKS',
                                     args=[ebook_wanted, 'eBook']).start()
                    threading.Thread(target=lazylibrarian.searchbook.search_book, name='HCSYNCBOOKS',
                                     args=[ebook_wanted, 'eBook']).start()
                if audio_wanted:
                    self.syncinglogger.debug(f"Searching for {len(audio_wanted)} "
                                             f"{plural(len(audio_wanted), 'audiobook')}")
                    threading.Thread(target=lazylibrarian.searchrss.search_rss_book, name='HCSYNCRSSAUDIO',
                                     args=[audio_wanted, 'AudioBook']).start()
                    threading.Thread(target=lazylibrarian.searchbook.search_book, name='HCSYNCAUDIO',
                                     args=[audio_wanted, 'AudioBook']).start()
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
                    if 'error' in results:
                        self.logger.error(str(results['error']))

            cmd = (f"SELECT hc_id,readinglists.status,bookname from readinglists,books WHERE "
                   f"books.bookid=readinglists.bookid and userid=? and books.bookid=?")

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
                        if 'error' in results:
                            self.logger.error(str(results['error']))
                else:
                    miss.append((item, res[2]))
                    for mapp in mapping:
                        if item in mapp[2]:
                            mapp[2].remove(item)
            if len(miss):
                msg += f"Unable to update {len(miss)} items at HardCover as no hc_id\n"

            for mapp in mapping:
                # mapp[2] is now the definitive list to store as last sync for status mapp[3]
                listmsg = f"HardCover {mapp[3]} contains {len(mapp[2])}"
                self.syncinglogger.debug(listmsg)
                msg += f"{listmsg}\n"
                set_readinglist(mapp[3], userid, mapp[2])
                label = f"hc_{mapp[3]}"
                booklist = ','.join(mapp[2])
                db.upsert("sync", {'SyncList': booklist}, {'UserID': userid, 'Label': label})

            # maybe sync ll_owned to hc_owned, but only add/delete to ll using status HAVE, not OPEN
            # and use hc_id for admin user, not ll userid and not current user
        finally:
            db.upsert("jobs", {"Finish": time.time()}, {"Name": "HCSYNC"})
            db.close()
            self.logger.debug(f"HCsync completed for {userid}")
            for missed in miss:
                self.syncinglogger.warning(f"Unable to add bookid {missed[0]} ({missed[1]}) at HardCover, no hc_id")
            thread_name('WEBSERVER')
            return msg
