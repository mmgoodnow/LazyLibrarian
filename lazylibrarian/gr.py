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

import re
import time
import traceback
import unicodedata

try:
    import urllib3
    import requests
except ImportError:
    import lib.requests as requests

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.bookwork import get_work_series, get_work_page, delete_empty_series, \
    set_series, get_status, isbn_from_words, thinglang, get_book_pubdate, get_gb_info, \
    get_gr_genres, set_genres, genre_filter
from lazylibrarian.images import get_book_cover
from lazylibrarian.cache import gr_xml_request, cache_img
from lazylibrarian.formatter import plural, today, replace_all, book_series, unaccented, split_title, get_list, \
    clean_name, is_valid_isbn, format_author_name, check_int, make_unicode, check_year, check_float, \
    make_utf8bytes, thread_name

from lib.thefuzz import fuzz
# noinspection PyUnresolvedReferences
from six.moves.urllib_parse import quote, quote_plus, urlencode


class GoodReads:
    # https://www.goodreads.com/api/

    def __init__(self, name=None):
        self.name = make_unicode(name)
        # self.type = type
        if not lazylibrarian.CONFIG['GR_API']:
            logger.warn('No Goodreads API key, check config')
        self.params = {"key": lazylibrarian.CONFIG['GR_API']}

    def find_results(self, searchterm=None, queue=None):
        # noinspection PyBroadException
        try:
            resultlist = []
            api_hits = 0
            searchtitle = ''
            searchauthorname = ''

            if ' <ll> ' in searchterm:  # special token separates title from author
                searchtitle, searchauthorname = searchterm.split(' <ll> ')
                searchterm = searchterm.replace(' <ll> ', ' ')
                searchtitle = searchtitle.split(' (')[0]  # without any series info

            url = quote_plus(make_utf8bytes(searchterm)[0])
            set_url = '/'.join([lazylibrarian.CONFIG['GR_URL'],
                                'search.xml?q=' + url + '&' + urlencode(self.params)])
            logger.debug('Now searching GoodReads API with searchterm: %s' % searchterm)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(set_url)

            resultcount = 0
            try:
                try:
                    rootxml, in_cache = gr_xml_request(set_url)
                except Exception as e:
                    logger.error("%s finding gr results: %s" % (type(e).__name__, str(e)))
                    queue.put(resultlist)
                    return
                if rootxml is None:
                    logger.debug("Error requesting results")
                    queue.put(resultlist)
                    return

                totalresults = check_int(rootxml.find('search/total-results').text, 0)

                resultxml = rootxml.iter('work')
                loop_count = 1
                while resultxml:
                    for author in resultxml:
                        try:
                            if author.find('original_publication_year').text is None:
                                bookdate = "0000"
                            elif check_year(author.find('original_publication_year').text, past=1800, future=0):
                                bookdate = author.find('original_publication_year').text
                                try:
                                    bookmonth = check_int(author.find('original_publication_month').text, 0)
                                    bookday = check_int(author.find('original_publication_day').text, 0)
                                    if bookmonth and bookday:
                                        bookdate = "%s-%02d-%02d" % (bookdate, bookmonth, bookday)
                                except (KeyError, AttributeError):
                                    pass
                            else:
                                bookdate = "0000"
                        except (KeyError, AttributeError):
                            bookdate = "0000"

                        try:
                            author_name_result = author.find('./best_book/author/name').text
                            # Goodreads sometimes puts extra whitespace in the author names!
                            author_name_result = ' '.join(author_name_result.split())
                        except (KeyError, AttributeError):
                            author_name_result = ""

                        booksub = ""
                        bookpub = ""
                        booklang = "Unknown"

                        try:
                            bookimg = author.find('./best_book/image_url').text
                            if not bookimg or 'nocover' in bookimg or 'nophoto' in bookimg:
                                bookimg = 'images/nocover.png'
                        except (KeyError, AttributeError):
                            bookimg = 'images/nocover.png'

                        try:
                            bookrate = check_float(author.find('average_rating').text, 0)
                        except KeyError:
                            bookrate = 0.0
                        try:
                            bookrate_count = check_int(author.find('ratings_count').text, 0)
                        except KeyError:
                            bookrate_count = 0

                        bookpages = '0'
                        bookgenre = ''
                        bookdesc = ''
                        bookisbn = ''
                        workid = ''

                        try:
                            booklink = '/'.join([lazylibrarian.CONFIG['GR_URL'],
                                                'book/show/' + author.find('./best_book/id').text])
                        except (KeyError, AttributeError):
                            booklink = ""

                        try:
                            authorid = author.find('./best_book/author/id').text
                        except (KeyError, AttributeError):
                            authorid = ""

                        try:
                            if author.find('./best_book/title').text is None:
                                book_title = ""
                            else:
                                book_title = author.find('./best_book/title').text
                        except (KeyError, AttributeError):
                            book_title = ""

                        if searchauthorname:
                            author_fuzz = fuzz.ratio(author_name_result, searchauthorname)
                        else:
                            author_fuzz = fuzz.ratio(author_name_result, searchterm)
                        if searchtitle:
                            if book_title.endswith(')'):
                                book_title = book_title.rsplit(' (', 1)[0]
                            book_fuzz = fuzz.token_set_ratio(book_title, searchtitle)
                            # lose a point for each extra word in the fuzzy matches so we get the closest match
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
                            bookisbn = searchterm

                        highest_fuzz = max((author_fuzz + book_fuzz) / 2, isbn_fuzz)

                        try:
                            bookid = author.find('./best_book/id').text
                        except (KeyError, AttributeError):
                            bookid = ""

                        # Don't query google for every book we find, it's too slow and too many
                        # api hits. Only query the ones we want to add to db later
                        # if not bookdesc:
                        #     bookdesc = get_book_desc(isbn=bookisbn, author=authorNameResult, title=bookTitle)
                        resultlist.append({
                            'authorname': author_name_result,
                            'bookid': bookid,
                            'authorid': authorid,
                            'bookname': book_title,
                            'booksub': booksub,
                            'bookisbn': bookisbn,
                            'bookpub': bookpub,
                            'bookdate': bookdate,
                            'booklang': booklang,
                            'booklink': booklink,
                            'bookrate': bookrate,
                            'bookrate_count': bookrate_count,
                            'bookimg': bookimg,
                            'bookpages': bookpages,
                            'bookgenre': bookgenre,
                            'bookdesc': bookdesc,
                            'workid': workid,
                            'author_fuzz': author_fuzz,
                            'book_fuzz': book_fuzz,
                            'isbn_fuzz': isbn_fuzz,
                            'highest_fuzz': highest_fuzz
                        })

                        resultcount += 1

                    loop_count += 1

                    if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < loop_count:
                        resultxml = None
                        logger.warn('Maximum results page search reached, still more results available')
                    elif totalresults and resultcount >= totalresults:
                        # fix for goodreads bug on isbn searches
                        resultxml = None
                    else:
                        url = set_url + '&page=' + str(loop_count)
                        resultxml = None
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                            logger.debug(set_url)
                        try:
                            rootxml, in_cache = gr_xml_request(url)
                            if rootxml is None:
                                logger.debug('Error requesting page %s of results' % loop_count)
                            else:
                                resultxml = rootxml.iter('work')
                                if not in_cache:
                                    api_hits += 1
                        except Exception as e:
                            resultxml = None
                            logger.error("%s finding page %s of results: %s" % (type(e).__name__, loop_count, str(e)))

                    if resultxml:
                        if all(False for _ in resultxml):  # returns True if iterator is empty
                            resultxml = None

            except Exception as err:
                # noinspection PyUnresolvedReferences
                if hasattr(err, 'code') and err.code == 404:
                    logger.error('Received a 404 error when searching for author')
                # noinspection PyUnresolvedReferences
                elif hasattr(err, 'code') and err.code == 403:
                    logger.warn('Access to api is denied 403: usage exceeded')
                else:
                    logger.error('An unexpected error has occurred when searching for an author: %s' % str(err))
                    logger.error('in GR.find_results: %s' % traceback.format_exc())

            logger.debug('Found %s %s with keyword: %s' % (resultcount, plural(resultcount, "result"), searchterm))
            logger.debug(
                'The GoodReads API was hit %s %s for keyword %s' % (api_hits, plural(api_hits, "time"), searchterm))

            queue.put(resultlist)

        except Exception:
            logger.error('Unhandled exception in GR.find_results: %s' % traceback.format_exc())

    def find_author_id(self, refresh=False):
        author = self.name
        author = format_author_name(unaccented(author, only_ascii='_'))
        # googlebooks gives us author names with long form unicode characters
        author = make_unicode(author)  # ensure it's unicode
        author = unicodedata.normalize('NFC', author)  # normalize to short form
        logger.debug("Searching for author with name: %s" % author)
        url = '/'.join([lazylibrarian.CONFIG['GR_URL'], 'api/author_url/'])
        try:
            url += quote(make_utf8bytes(author)[0]) + '?' + urlencode(self.params)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(url)
            rootxml, _ = gr_xml_request(url, use_cache=not refresh)
        except Exception as e:
            logger.error("%s finding authorid: %s, %s" % (type(e).__name__, url, str(e)))
            return {}
        if rootxml is None:
            logger.debug("Error requesting authorid")
            return {}

        resultxml = rootxml.iter('author')

        if resultxml is None:
            logger.warn('No authors found with name: %s' % author)
            return {}

        # In spite of how this looks, goodreads only returns one result, even if there are multiple matches
        # we just have to hope we get the right one. eg search for "James Lovelock" returns "James E. Lovelock"
        # who only has one book listed under googlebooks, the rest are under "James Lovelock"
        # goodreads has all his books under "James E. Lovelock". Can't come up with a good solution yet.
        # For now we'll have to let the user handle this by selecting/adding the author manually
        for res in resultxml:
            authorid = res.attrib.get("id")
            authorname = res.find('name').text
            authorname = format_author_name(unaccented(authorname, only_ascii=False))
            match = fuzz.ratio(author, authorname)
            if match >= lazylibrarian.CONFIG['NAME_RATIO']:
                return self.get_author_info(authorid)
            else:
                logger.debug("Fuzz failed: %s [%s][%s]" % (match, author, authorname))
        return {}

    def get_author_info(self, authorid=None):

        url = '/'.join([lazylibrarian.CONFIG['GR_URL'],
                        'author/show/' + authorid + '.xml?' + urlencode(self.params)])

        try:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(url)
            rootxml, _ = gr_xml_request(url)
        except Exception as e:
            logger.error("%s getting author info: %s" % (type(e).__name__, str(e)))
            return {}
        if rootxml is None:
            logger.debug("Error requesting author info")
            return {}

        resultxml = rootxml.find('author')
        if resultxml is None:
            logger.warn('No author found with ID: ' + authorid)
            return {}

        # added authorname to author_dict - this holds the intact name preferred by GR
        # except GR messes up names like "L. E. Modesitt, Jr." where it returns <name>Jr., L. E. Modesitt</name>
        authorname = format_author_name(resultxml[1].text)
        logger.debug("[%s] Processing info for authorID: %s" % (authorname, authorid))
        author_dict = {
            'authorid': resultxml[0].text,
            'authorlink': resultxml.find('link').text,
            'authorimg': resultxml.find('image_url').text,
            'authorborn': resultxml.find('born_at').text,
            'authordeath': resultxml.find('died_at').text,
            'about': resultxml.find('about').text,
            'totalbooks': resultxml.find('works_count').text,
            'authorname': authorname
        }
        return author_dict

    @staticmethod
    def get_bookdict(book):
        """ Return all the book info we need as a dictionary or default value if no key """
        mydict = {}
        for val, idx, default in [
            ('name', 'title', ''),
            ('shortname', 'title_without_series', ''),
            ('id', 'id', ''),
            ('desc', 'description', ''),
            ('pub', 'publisher', ''),
            ('link', 'link', ''),
            ('rate', 'average_rating', 0.0),
            ('pages', 'num_pages', 0),
            ('pub_year', 'publication_year', '0000'),
            ('pub_month', 'publication_month', '0'),
            ('pub_day', 'publication_day', '0'),
            ('workid', 'work/id', ''),
            ('isbn13', 'isbn13', ''),
            ('isbn10', 'isbn', ''),
            ('img', 'image_url', '')
        ]:

            value = default
            res = book.find(idx)
            if res is not None:
                value = res.text
            if value is None:
                value = default
            if idx == 'rate':
                value = check_float(value, 0.0)
            mydict[val] = value

        return mydict

    def get_author_books(self, authorid=None, authorname=None, bookstatus="Skipped", audiostatus='Skipped',
                         entrystatus='Active', refresh=False, reason='gr.get_author_books'):
        # noinspection PyBroadException
        try:
            entryreason = reason
            api_hits = 0
            gr_lang_hits = 0
            lt_lang_hits = 0
            gb_lang_change = 0
            cache_hits = 0
            not_cached = 0

            # Artist is loading
            db = database.DBConnection()
            control_value_dict = {"AuthorID": authorid}
            new_value_dict = {"Status": "Loading"}
            db.upsert("authors", new_value_dict, control_value_dict)

            gr_id = ''
            match = db.match('SELECT gr_id FROM authors where authorid=?', (authorid,))
            if match:
                gr_id = match['gr_id']
            if not gr_id:
                gr_id = authorid

            url = '/'.join([lazylibrarian.CONFIG['GR_URL'],
                            'author/list/' + gr_id + '.xml?' + urlencode(self.params)])

            try:
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                    logger.debug(url)
                rootxml, in_cache = gr_xml_request(url, use_cache=not refresh)
            except Exception as e:
                logger.error("%s fetching author books: %s" % (type(e).__name__, str(e)))
                return
            if rootxml is None:
                logger.debug("Error requesting author books")
                return
            if not in_cache:
                api_hits += 1

            resultxml = rootxml.iter('book')

            valid_langs = get_list(lazylibrarian.CONFIG['IMP_PREFLANG'])

            removed_results = 0
            duplicates = 0
            ignored = 0
            added_count = 0
            updated_count = 0
            book_ignore_count = 0
            total_count = 0
            locked_count = 0
            loop_count = 0
            cover_count = 0
            isbn_count = 0
            cover_time = 0
            isbn_time = 0
            auth_start = time.time()
            # these are reject reasons we might want to override, so optionally add to database as "ignored"
            ignorable = ['future', 'date', 'isbn', 'word', 'set']
            if lazylibrarian.CONFIG['NO_LANG']:
                ignorable.append('lang')

            if resultxml is None:
                logger.warn('[%s] No books found for author with ID: %s' % (authorname, gr_id))
            else:
                logger.debug("[%s] Now processing books with GoodReads API" % authorname)
                author_name_result = rootxml.find('./author/name').text
                # Goodreads sometimes puts extra whitespace in the author names!
                author_name_result = ' '.join(author_name_result.split())
                logger.debug("GoodReads author name [%s]" % author_name_result)
                loop_count = 1
                threadname = thread_name()
                while resultxml:
                    if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                        logger.debug("Aborting %s" % threadname)
                        break
                    for book in resultxml:
                        if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                            logger.debug("Aborting %s" % threadname)
                            break
                        total_count += 1
                        rejected = None
                        booksub = ''
                        series = ''
                        series_num = ''
                        book_language = "Unknown"
                        find_field = "id"
                        bookisbn = ""
                        isbnhead = ""
                        originalpubdate = ""
                        bookgenre = ''

                        bookdict = self.get_bookdict(book)

                        shortname = bookdict['shortname']
                        bookname = bookdict['name']
                        bookid = bookdict['id']
                        bookdesc = bookdict['desc']
                        bookpub = bookdict['pub']
                        booklink = bookdict['link']
                        bookrate = bookdict['rate']
                        bookpages = bookdict['pages']
                        bookimg = bookdict['img']
                        workid = bookdict['workid']
                        isbn13 = bookdict['isbn13']
                        isbn10 = bookdict['isbn10']
                        bookdate = bookdict['pub_year']
                        if check_year(bookdate, past=1800, future=0):
                            mn = check_int(bookdict['pub_month'], 0)
                            dy = check_int(bookdict['pub_day'], 0)
                            if mn and dy:
                                bookdate = "%s-%02d-%02d" % (bookdate, mn, dy)

                        if not bookname:
                            logger.debug('Rejecting bookid %s for %s, no bookname' %
                                         (bookid, author_name_result))
                            rejected = 'name', 'No bookname'

                        if bookpub:
                            if bookpub.lower() in get_list(lazylibrarian.CONFIG['REJECT_PUBLISHER']):
                                logger.warn("Ignoring %s: Publisher %s" % (bookname, bookpub))
                                rejected = 'publisher', bookpub

                        # bookname = replace_all(bookname, {':': ' ', '"': '', '\'': ''}).strip()

                        # if not rejected and re.match(r'[^\w-]', bookname):
                        # reject books with bad characters in title
                        # logger.debug("removed result [" + bookname + "] for bad characters")
                        # rejected = 'chars', 'Bad characters in bookname'

                        if not rejected:
                            if not bookimg or 'nocover' in bookimg or 'nophoto' in bookimg:
                                bookimg = 'images/nocover.png'

                            if isbn13:
                                find_field = "isbn13"
                                bookisbn = isbn13
                                isbnhead = bookisbn[3:6]
                            elif isbn10:
                                find_field = "isbn"
                                bookisbn = isbn10
                                isbnhead = bookisbn[0:3]

                            # Try to use shortcut of ISBN identifier codes described here...
                            # http://en.wikipedia.org/wiki/List_of_ISBN_identifier_groups
                            if isbnhead:
                                if find_field == "isbn13" and bookisbn.startswith('979'):
                                    for item in lazylibrarian.isbn_979_dict:
                                        if isbnhead.startswith(item):
                                            book_language = lazylibrarian.isbn_979_dict[item]
                                            break
                                    if book_language != "Unknown":
                                        logger.debug("ISBN979 returned %s for %s" % (book_language, isbnhead))
                                elif (find_field == "isbn") or (find_field == "isbn13" and
                                                                bookisbn.startswith('978')):
                                    for item in lazylibrarian.isbn_978_dict:
                                        if isbnhead.startswith(item):
                                            book_language = lazylibrarian.isbn_978_dict[item]
                                            break
                                    if book_language != "Unknown":
                                        logger.debug("ISBN978 returned %s for %s" % (book_language, isbnhead))

                            if book_language == "Unknown" and isbnhead:
                                # Nothing in the isbn dictionary, try any cached results
                                match = db.match('SELECT lang FROM languages where isbn=?', (isbnhead,))
                                if match:
                                    book_language = match['lang']
                                    cache_hits += 1
                                    logger.debug("Found cached language [%s] for %s [%s]" %
                                                 (book_language, find_field, isbnhead))
                                else:
                                    book_language = thinglang(bookisbn)
                                    lt_lang_hits += 1
                                    if book_language:
                                        db.action('insert into languages values (?, ?)', (isbnhead, book_language))

                            if not book_language or book_language == "Unknown":
                                # still  no earlier match, we'll have to search the goodreads api
                                try:
                                    if book.find(find_field).text:
                                        book_url = '/'.join([lazylibrarian.CONFIG['GR_URL'], 'book/show?id=' +
                                                             book.find(find_field).text + '&' +
                                                             urlencode(self.params)])
                                        logger.debug("Book URL: " + book_url)
                                        book_language = ""
                                        try:
                                            book_rootxml, in_cache = gr_xml_request(book_url)
                                            if book_rootxml is None:
                                                logger.debug('Error requesting book page')
                                            else:
                                                try:
                                                    book_language = book_rootxml.find('./book/language_code').text
                                                except Exception as e:
                                                    logger.error("%s finding language_code in book xml: %s" %
                                                                 (type(e).__name__, str(e)))
                                                # noinspection PyBroadException
                                                try:
                                                    res = book_rootxml.find('./book/isbn').text
                                                    isbnhead = res[0:3]
                                                except Exception:
                                                    # noinspection PyBroadException
                                                    try:
                                                        res = book_rootxml.find('./book/isbn13').text
                                                        isbnhead = res[3:6]
                                                    except Exception:
                                                        isbnhead = ''
                                                # if bookLanguage and not isbnhead:
                                                #     print(BOOK_URL)

                                                # might as well get the original publication date from here
                                                # noinspection PyBroadException
                                                try:
                                                    bookdate = book_rootxml.find(
                                                        './book/work/original_publication_year').text
                                                    if check_year(bookdate, past=1800, future=0):
                                                        try:
                                                            mn = check_int(book_rootxml.find(
                                                                './book/work/original_publication_month').text, 0)
                                                            dy = check_int(book_rootxml.find(
                                                                './book/work/original_publication_day').text, 0)
                                                            if mn and dy:
                                                                bookdate = "%s-%02d-%02d" % (bookdate, mn, dy)
                                                        except (KeyError, AttributeError):
                                                            logger.debug("No extended date info")
                                                            pass
                                                except Exception:
                                                    pass

                                        except Exception as e:
                                            logger.error("%s getting book xml: %s" % (type(e).__name__, str(e)))

                                        if not in_cache:
                                            gr_lang_hits += 1
                                        if not book_language:
                                            book_language = "Unknown"
                                        elif isbnhead:
                                            # if GR didn't give an isbn we can't cache it
                                            # just use language for this book
                                            control_value_dict = {"isbn": isbnhead}
                                            new_value_dict = {"lang": book_language}
                                            db.upsert("languages", new_value_dict, control_value_dict)
                                            logger.debug("GoodReads reports language [%s] for %s" %
                                                         (book_language, isbnhead))
                                        else:
                                            not_cached += 1

                                        logger.debug("GR language: " + book_language)
                                    else:
                                        logger.debug("No %s provided for [%s]" % (find_field, bookname))
                                        # continue

                                except Exception as e:
                                    logger.error("Goodreads language search failed: %s %s" %
                                                 (type(e).__name__, str(e)))

                            if not isbnhead and lazylibrarian.CONFIG['ISBN_LOOKUP']:
                                # try lookup by name
                                if bookname or shortname:
                                    if shortname:
                                        name = replace_all(shortname, {':': ' ', '"': '', '\'': ''}).strip()
                                    else:
                                        name = replace_all(bookname, {':': ' ', '"': '', '\'': ''}).strip()
                                    try:
                                        isbn_count += 1
                                        start = time.time()
                                        res = isbn_from_words(unaccented(name, only_ascii=False) + ' ' +
                                                              unaccented(author_name_result, only_ascii=False))
                                        isbn_time += (time.time() - start)
                                    except Exception as e:
                                        res = None
                                        logger.warn("Error from isbn: %s" % e)
                                    if res:
                                        logger.debug("isbn found %s for %s" % (res, bookid))
                                        bookisbn = res
                                        if len(res) == 13:
                                            isbnhead = res[3:6]
                                        else:
                                            isbnhead = res[0:3]

                            if not isbnhead and lazylibrarian.CONFIG['NO_ISBN']:
                                rejected = 'isbn', 'No ISBN'
                                logger.debug('Rejecting %s, %s' % (bookname, rejected[1]))

                            if "All" not in valid_langs:  # do we care about language
                                if book_language not in valid_langs:
                                    rejected = 'lang', 'Invalid language [%s]' % book_language
                                    logger.debug('Rejecting %s, %s' % (bookname, rejected[1]))
                                    ignored += 1

                        if not rejected:
                            dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '',
                                   '[': ' ', ']': ' ', '#': '# ', ':': ' ', ';': ' '}
                            name = replace_all(shortname, dic).strip()
                            if not name:
                                name = replace_all(bookname, dic).strip()
                            name = name.lower()
                            # remove extra spaces if they're in a row
                            name = " ".join(name.split())
                            namewords = name.split(' ')
                            badwords = get_list(lazylibrarian.CONFIG['REJECT_WORDS'], ',')
                            for word in badwords:
                                if (' ' in word and word in name) or word in namewords:
                                    rejected = 'word', 'Contains [%s]' % word
                                    logger.debug('Rejecting %s, %s' % (bookname, rejected[1]))
                                    break

                        if not rejected:
                            name = unaccented(bookname, only_ascii=False)
                            if lazylibrarian.CONFIG['NO_SETS']:
                                # allow date ranges eg 1981-95
                                m = re.search(r'(\d+)-(\d+)', name)
                                if m:
                                    if check_year(m.group(1), past=1800, future=0):
                                        logger.debug("Allow %s, looks like a date range" % bookname)
                                    else:
                                        rejected = 'set', 'Set or Part %s' % m.group(0)
                                if re.search(r'\d+ of \d+', name) or \
                                        re.search(r'\d+/\d+', name) and not re.search(r'\d+/\d+/\d+', name):
                                    rejected = 'set', 'Set or Part'
                                elif re.search(r'\w+\s*/\s*\w+', name):
                                    rejected = 'set', 'Set or Part'
                                if rejected:
                                    logger.debug('Rejected %s, %s' % (name, rejected[1]))

                        if not rejected:
                            oldbookname = bookname
                            bookname, booksub, bookseries = split_title(author_name_result, bookname)
                            if shortname:
                                sbookname, sbooksub, _ = split_title(author_name_result, shortname)
                                if sbookname != bookname:
                                    logger.warn('Different titles [%s][%s][%s]' % (oldbookname, sbookname, bookname))
                                    bookname = sbookname
                                if sbooksub != booksub:
                                    logger.warn('Different subtitles [%s][%s]' % (sbooksub, booksub))
                                    booksub = sbooksub
                            dic = {':': '.', '"': ''}  # do we need to strip apostrophes , '\'': ''}
                            bookname = replace_all(bookname, dic).strip()
                            booksub = replace_all(booksub, dic).strip()
                            bookseries = replace_all(bookseries, dic).strip()
                            if bookseries:
                                series, series_num = book_series(bookseries)
                            elif booksub:
                                series, series_num = book_series(booksub)
                            else:
                                series, series_num = book_series(bookname)

                            # 1. The author/list page only contains one author per book even if the book/show page
                            #    and html show multiple authors
                            # 2. The author/list page doesn't always include the publication date even if the
                            #    book/show page and html include it
                            # 3. The author/list page gives the publication date of the "best book" edition
                            #    and does not include the original publication date, though the book/show page
                            #    and html often show it
                            # We can't call book/show for every book because of api limits, and we can't scrape
                            # the html as it breaks goodreads terms of service
                            authors = book.find('authors')
                            anames = authors.iter('author')
                            amatch = False
                            alist = ''
                            role = ''
                            for aname in anames:
                                aid = aname.find('id').text
                                anm = aname.find('name').text
                                role = aname.find('role').text
                                if alist:
                                    alist += ', '
                                alist += anm
                                if aid == gr_id or anm == author_name_result:
                                    if aid != gr_id:
                                        logger.warn("Author %s has different authorid %s:%s" % (anm, aid, gr_id))
                                    if role is None or 'author' in role.lower() or \
                                            'writer' in role.lower() or \
                                            'creator' in role.lower() or \
                                            'pseudonym' in role.lower() or \
                                            'pen name' in role.lower():
                                        amatch = True
                                    else:
                                        logger.debug('Ignoring %s for %s, role is %s' % (anm, bookname, role))
                            if not amatch:
                                rejected = 'author', 'Wrong Author (got %s,%s)' % (alist, role)
                                logger.debug('Rejecting %s for %s, %s' %
                                             (bookname, author_name_result, rejected[1]))

                        cmd = 'SELECT AuthorName,BookName,AudioStatus,books.Status,ScanResult '
                        cmd += 'FROM books,authors WHERE authors.AuthorID = books.AuthorID AND BookID=?'
                        match = db.match(cmd, (bookid,))
                        not_rejectable = None
                        if match:
                            # we have a book with this bookid already
                            if author_name_result != match['AuthorName']:
                                rejected = 'author', 'Different author for this bookid [%s][%s]' % (
                                            author_name_result, match['AuthorName'])
                                logger.debug('Rejecting bookid %s, %s' % (bookid, rejected[1]))
                            elif bookname != match['BookName']:
                                # same bookid and author, assume goodreads fixed the title, use the new title
                                db.action("UPDATE books SET BookName=? WHERE BookID=?", (bookname, bookid))
                                logger.warn('Updated bookname [%s] to [%s]' % (match['BookName'], bookname))

                            msg = 'Bookid %s for [%s][%s] is in database marked %s' % (
                                   bookid, author_name_result, bookname, match['Status'])
                            if lazylibrarian.SHOW_AUDIO:
                                msg += ",%s" % match['AudioStatus']
                            msg += " %s" % match['ScanResult']
                            logger.debug(msg)

                            # Make sure we don't reject books we have already got or want
                            if match['Status'] not in ['Ignored', 'Skipped']:
                                not_rejectable = "Status: %s" % match['Status']
                            elif match['AudioStatus'] not in ['Ignored', 'Skipped']:
                                not_rejectable = "AudioStatus: %s" % match['AudioStatus']

                        if not match and not rejected:
                            cmd = 'SELECT BookID FROM books,authors WHERE books.AuthorID = authors.AuthorID'
                            cmd += ' and BookName=? COLLATE NOCASE and BookSub=? COLLATE NOCASE'
                            cmd += ' and AuthorName=? COLLATE NOCASE'
                            cmd += ' and books.Status != "Ignored" and AudioStatus != "Ignored"'
                            match = db.match(cmd, (bookname, booksub, author_name_result))

                            if not match:
                                in_db = lazylibrarian.librarysync.find_book_in_db(author_name_result, bookname,
                                                                                  ignored=False, library='eBook',
                                                                                  reason='gr_get_author_books')
                                if in_db and in_db[0]:
                                    cmd = 'SELECT AuthorName,BookName,BookID,AudioStatus,books.Status,ScanResult '
                                    cmd += 'FROM books,authors WHERE authors.AuthorID = books.AuthorID AND BookID=?'
                                    match = db.match(cmd, (in_db[0],))
                            if match:
                                if match['BookID'] != bookid:
                                    # we have a different bookid for this author/title already
                                    if not_rejectable:
                                        logger.debug("Not rejecting duplicate title %s (%s/%s) as %s" %
                                                     (bookname, bookid, match['BookID'], not_rejectable))
                                    else:
                                        duplicates += 1
                                        rejected = 'bookid', 'Got %s under bookid %s' % (bookid, match['BookID'])
                                        logger.debug('Rejecting bookid %s for [%s][%s] already got %s' %
                                                     (bookid, author_name_result, bookname, match['BookID']))

                        if rejected and rejected[0] not in ignorable:
                            removed_results += 1
                        if not rejected or (rejected and rejected[0] in ignorable and
                                            lazylibrarian.CONFIG['IMP_IGNORE']):
                            cmd = 'SELECT Status,AudioStatus,BookFile,AudioFile,Manual,BookAdded,BookName,'
                            cmd += 'OriginalPubDate,BookDesc,BookGenre,ScanResult FROM books WHERE BookID=?'
                            existing = db.match(cmd, (bookid,))
                            if existing:
                                book_status = existing['Status']
                                audio_status = existing['AudioStatus']
                                bookdesc = existing['BookDesc']
                                bookgenre = existing['BookGenre']
                                if lazylibrarian.CONFIG['FOUND_STATUS'] == 'Open':
                                    if book_status == 'Have' and existing['BookFile']:
                                        book_status = 'Open'
                                    if audio_status == 'Have' and existing['AudioFile']:
                                        audio_status = 'Open'
                                locked = existing['Manual']
                                added = existing['BookAdded']
                                if locked is None:
                                    locked = False
                                elif locked.isdigit():
                                    locked = bool(int(locked))
                                if not originalpubdate:
                                    originalpubdate = existing['OriginalPubDate']
                            else:
                                book_status = bookstatus  # new_book status, or new_author status
                                audio_status = audiostatus
                                added = today()
                                locked = False

                            if not originalpubdate or len(originalpubdate) < 5:
                                # already set with language code or existing book?
                                newdate, in_cache = get_book_pubdate(bookid)
                                if not originalpubdate:
                                    originalpubdate = newdate
                                elif originalpubdate < newdate:  # more detailed date
                                    originalpubdate = newdate
                                    logger.debug("Extended date info found: %s" % newdate)
                                if not in_cache:
                                    api_hits += 1

                            if originalpubdate:
                                bookdate = originalpubdate

                            if not rejected and lazylibrarian.CONFIG['NO_FUTURE']:
                                if bookdate > today()[:len(bookdate)]:
                                    if not_rejectable:
                                        logger.debug("Not rejecting %s (future pub date %s) as %s" %
                                                     (bookname, bookdate, not_rejectable))
                                    else:
                                        rejected = 'future', 'Future publication date [%s]' % bookdate
                                        logger.debug('Rejecting %s, %s' % (bookname, rejected[1]))

                            if not rejected and lazylibrarian.CONFIG['NO_PUBDATE']:
                                if not bookdate or bookdate == '0000':
                                    if not_rejectable:
                                        logger.debug("Not rejecting %s (no pub date) as %s" %
                                                     (bookname, not_rejectable))
                                    else:
                                        rejected = 'date', 'No publication date'
                                        logger.debug('Rejecting %s, %s' % (bookname, rejected[1]))

                            if rejected:
                                if rejected[0] in ignorable:
                                    book_status = 'Ignored'
                                    audio_status = 'Ignored'
                                    book_ignore_count += 1
                                    reason = "Ignored: %s" % rejected[1]
                                else:
                                    reason = "Rejected: %s" % rejected[1]
                            else:
                                if 'author_update' in entryreason:
                                    reason = 'Author: %s' % author_name_result
                                else:
                                    reason = entryreason

                            # Leave alone if locked
                            if locked:
                                locked_count += 1
                            else:
                                if not bookgenre:
                                    genres, _ = get_gr_genres(bookid)
                                    if genres:
                                        bookgenre = ', '.join(genres)
                                infodict = get_gb_info(isbn=bookisbn, author=author_name_result,
                                                       title=bookname, expire=False)
                                if infodict:  # None if api blocked
                                    gbupdate = []
                                    if not bookdesc and infodict['desc']:
                                        bookdesc = infodict['desc']
                                        gbupdate.append('Description')
                                    if not bookdate or bookdate == '0000' or len(infodict['date']) > len(bookdate):
                                        bookdate = infodict['date']
                                        gbupdate.append('Publication Date')
                                    if infodict['rate'] and not bookrate:
                                        bookrate = infodict['rate']
                                        gbupdate.append('Rating')
                                    if infodict['pub'] and not bookpub:
                                        bookpub = infodict['pub']
                                        gbupdate.append('Publisher')
                                    if infodict['pages'] and not bookpages:
                                        bookpages = infodict['pages']
                                        gbupdate.append('Pages')
                                    if not bookgenre and infodict['genre']:
                                        bookgenre = genre_filter(infodict['genre'])
                                        gbupdate.append('Genres')
                                    if gbupdate:
                                        logger.debug("Updated %s from googlebooks" % ', '.join(gbupdate))

                                threadname = thread_name()
                                reason = "[%s] %s" % (threadname, reason)
                                control_value_dict = {"BookID": bookid}
                                new_value_dict = {
                                    "AuthorID": authorid,
                                    "BookName": bookname,
                                    "BookSub": booksub,
                                    "BookDesc": bookdesc,
                                    "BookIsbn": bookisbn,
                                    "BookPub": bookpub,
                                    "BookGenre": bookgenre,
                                    "BookImg": bookimg,
                                    "BookLink": booklink,
                                    "BookRate": bookrate,
                                    "BookPages": bookpages,
                                    "BookDate": bookdate,
                                    "BookLang": book_language,
                                    "Status": book_status,
                                    "AudioStatus": audio_status,
                                    "BookAdded": added,
                                    "WorkID": workid,
                                    "gr_id": bookid,
                                    "ScanResult": reason,
                                    "OriginalPubDate": originalpubdate
                                }
                                db.upsert("books", new_value_dict, control_value_dict)

                                set_genres(get_list(bookgenre, ','), bookid)

                                update_value_dict = {}
                                # need to run get_work_series AFTER adding to book table (foreign key constraint)
                                serieslist = []
                                if series:
                                    serieslist = [('', series_num, clean_name(series, '&/'))]
                                if lazylibrarian.CONFIG['ADD_SERIES'] and "Ignored:" not in reason:
                                    newserieslist = get_work_series(workid, 'GR', reason=reason)
                                    if newserieslist:
                                        serieslist = newserieslist
                                        logger.debug('Updated series: %s [%s]' % (bookid, serieslist))
                                    _api_hits, pubdate = set_series(serieslist, bookid, authorid, workid, reason=reason)
                                    api_hits += _api_hits
                                    if pubdate and pubdate > originalpubdate:  # more detailed
                                        update_value_dict["OriginalPubDate"] = pubdate

                                if not rejected:
                                    if existing and existing['ScanResult'] and \
                                            ' publication date' in existing['ScanResult'] and \
                                            bookdate and bookdate != '0000' and \
                                            bookdate <= today()[:len(bookdate)]:
                                        # was rejected on previous scan but bookdate has become valid
                                        logger.debug("valid bookdate [%s] previous scanresult [%s]" %
                                                     (bookdate, existing['ScanResult']))
                                        update_value_dict["ScanResult"] = "bookdate %s is now valid" % bookdate
                                    elif not existing:
                                        update_value_dict["ScanResult"] = reason

                                    if "ScanResult" in update_value_dict:
                                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                            logger.debug("entry status %s %s,%s" % (entrystatus,
                                                                                    bookstatus,
                                                                                    audiostatus))
                                        book_status, audio_status = get_status(bookid, serieslist, bookstatus,
                                                                               audiostatus, entrystatus)
                                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                            logger.debug("status is now %s,%s" % (book_status,
                                                                                  audio_status))
                                        update_value_dict["Status"] = book_status
                                        update_value_dict["AudioStatus"] = audio_status

                                    if 'nocover' in bookimg or 'nophoto' in bookimg:
                                        # try to get a cover from another source
                                        start = time.time()
                                        workcover, source = get_book_cover(bookid)
                                        if source != 'cache':
                                            cover_count += 1
                                            cover_time += (time.time() - start)

                                        if workcover:
                                            logger.debug('Updated cover for %s using %s' % (bookname, source))
                                            update_value_dict["BookImg"] = workcover

                                    elif bookimg and bookimg.startswith('http'):
                                        start = time.time()
                                        link, success, was_already_cached = cache_img("book", bookid, bookimg)
                                        if not was_already_cached:
                                            cover_count += 1
                                            cover_time += (time.time() - start)
                                        if success:
                                            update_value_dict["BookImg"] = link
                                        else:
                                            logger.debug('Failed to cache image for %s' % bookimg)

                                    worklink = get_work_page(bookid)
                                    if worklink:
                                        update_value_dict["WorkPage"] = worklink

                                if update_value_dict:
                                    db.upsert("books", update_value_dict, control_value_dict)

                                if not existing:
                                    typ = 'Added'
                                    added_count += 1
                                else:
                                    typ = 'Updated'
                                    updated_count += 1
                                msg = "[%s] %s book: %s [%s] status %s" % (authorname, typ, bookname,
                                                                           book_language, book_status)
                                if lazylibrarian.SHOW_AUDIO:
                                    msg += " audio %s" % audio_status
                                logger.debug(msg)
                    loop_count += 1
                    if 0 < lazylibrarian.CONFIG['MAX_BOOKPAGES'] < loop_count:
                        resultxml = None
                    else:
                        url = '/'.join([lazylibrarian.CONFIG['GR_URL'], 'author/list/' + gr_id + '.xml?' +
                                        urlencode(self.params) + '&page=' + str(loop_count)])
                        resultxml = None
                        try:
                            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                logger.debug(url)
                            rootxml, in_cache = gr_xml_request(url, use_cache=not refresh)
                            if rootxml is None:
                                logger.debug('Error requesting next page of results')
                            else:
                                resultxml = rootxml.iter('book')
                                if not in_cache:
                                    api_hits += 1
                        except Exception as e:
                            resultxml = None
                            logger.error("%s finding next page of results: %s" % (type(e).__name__, str(e)))

                    if resultxml:
                        if all(False for _ in resultxml):  # returns True if iterator is empty
                            resultxml = None

            self.verify_ids(authorid)
            delete_empty_series()
            cmd = 'SELECT BookName, BookLink, BookDate, BookImg, BookID from books WHERE AuthorID=?'
            cmd += ' AND Status != "Ignored" order by BookDate DESC'
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
            loop_count -= 1
            logger.debug("Found %s %s in %s %s" % (total_count, plural(total_count, "result"),
                                                   loop_count, plural(loop_count, "page")))
            logger.debug("Found %s locked %s" % (locked_count, plural(locked_count, "book")))
            logger.debug("Removed %s unwanted language %s" % (ignored, plural(ignored, "result")))
            logger.debug("Removed %s incorrect/incomplete %s" % (removed_results, plural(removed_results, "result")))
            logger.debug("Removed %s duplicate %s" % (duplicates, plural(duplicates, "result")))
            logger.debug("Ignored %s %s" % (book_ignore_count, plural(book_ignore_count, "book")))
            logger.debug("Imported/Updated %s %s in %d secs using %s api %s" %
                         (resultcount, plural(resultcount, "book"), int(time.time() - auth_start),
                          api_hits, plural(api_hits, "hit")))
            if cover_count:
                logger.debug("Fetched %s %s in %.2f sec" % (cover_count, plural(cover_count, "cover"), cover_time))
            if isbn_count:
                logger.debug("Fetched %s ISBN in %.2f sec" % (isbn_count, isbn_time))

            control_value_dict = {"authorname": authorname.replace('"', '""')}
            new_value_dict = {
                            "GR_book_hits": api_hits,
                            "GR_lang_hits": gr_lang_hits,
                            "LT_lang_hits": lt_lang_hits,
                            "GB_lang_change": gb_lang_change,
                            "cache_hits": cache_hits,
                            "bad_lang": ignored,
                            "bad_char": removed_results,
                            "uncached": not_cached,
                            "duplicates": duplicates
                            }
            db.upsert("stats", new_value_dict, control_value_dict)

            if refresh:
                logger.info("[%s] Book processing complete: Added %s %s / Updated %s %s" %
                            (authorname, added_count, plural(added_count, "book"),
                             updated_count, plural(updated_count, "book")))
            else:
                logger.info("[%s] Book processing complete: Added %s %s to the database" %
                            (authorname, added_count, plural(added_count, "book")))

        except Exception:
            logger.error('Unhandled exception in GR.get_author_books: %s' % traceback.format_exc())

    def verify_ids(self, authorid):
        """ GoodReads occasionally consolidates bookids/workids and renumbers so check if changed... """
        db = database.DBConnection()
        cmd = "select BookID,gr_id,BookName from books WHERE AuthorID=? and gr_id is not NULL"
        books = db.select(cmd, (authorid,))
        counter = 0
        logger.debug('Checking BookID/WorkID for %s %s' % (len(books), plural(len(books), "book")))
        page = ''
        pages = []
        for book in books:
            bookid = book['gr_id']
            if not bookid:
                logger.warn("No gr_id for %s" % book['BookName'])
            else:
                if page:
                    page = page + ','
                page = page + bookid
                counter += 1
                if counter == 50:
                    counter = 0
                    pages.append(page)
                    page = ''
        if page:
            pages.append(page)

        found = 0
        differ = 0
        notfound = []
        pagecount = 0
        for page in pages:
            pagecount += 1
            url = '/'.join([lazylibrarian.CONFIG['GR_URL'], 'book/id_to_work_id/' + page + '?' +
                            urlencode(self.params)])
            try:
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                    logger.debug(url)
                rootxml, _ = gr_xml_request(url, use_cache=False)
                if rootxml is None:
                    logger.debug("Error requesting id_to_work_id page")
                else:
                    resultxml = rootxml.find('work-ids')
                    if len(resultxml):
                        ids = resultxml.iter('item')
                        books = get_list(page)
                        cnt = 0
                        for item in ids:
                            workid = item.text
                            if not workid:
                                notfound.append(books[cnt])
                                logger.debug("No workid returned for %s" % books[cnt])
                            else:
                                found += 1
                                res = db.match("SELECT WorkID from books WHERE bookid=?", (books[cnt],))
                                if res:
                                    if res['WorkID'] != workid:
                                        differ += 1
                                        logger.debug("Updating workid for %s from [%s] to [%s]" % (
                                                     books[cnt], res['WorkID'], workid))
                                        control_value_dict = {"gr_id": books[cnt]}
                                        new_value_dict = {"WorkID": workid}
                                        db.upsert("books", new_value_dict, control_value_dict)
                            cnt += 1

            except Exception as e:
                logger.error("%s parsing id_to_work_id page: %s" % (type(e).__name__, str(e)))
        logger.debug("BookID/WorkID Found %d, Differ %d, Missing %d" % (found, differ, len(notfound)))

        cnt = 0
        for bookid in notfound:
            res = db.match("SELECT BookName,Status,AudioStatus from books WHERE gr_id=?", (bookid,))
            if res:
                if lazylibrarian.CONFIG['FULL_SCAN']:
                    if res['Status'] in ['Wanted', 'Open', 'Have']:
                        logger.warn("Keeping unknown goodreads bookid %s: %s, Status is %s" %
                                    (bookid, res['BookName'], res['Status']))
                    elif res['AudioStatus'] in ['Wanted', 'Open', 'Have']:
                        logger.warn("Keeping unknown goodreads bookid %s: %s, AudioStatus is %s" %
                                    (bookid, res['BookName'], res['Status']))
                    else:
                        logger.debug("Deleting unknown goodreads bookid %s: %s" % (bookid, res['BookName']))
                        db.action("DELETE from books WHERE gr_id=?", (bookid,))
                        cnt += 1
                else:
                    logger.warn("Unknown goodreads bookid %s: %s" % (bookid, res['BookName']))
        if cnt:
            logger.warn("Deleted %s %s with unknown goodreads bookid" % (cnt, plural(cnt, 'entry')))

        # Check for any duplicate titles for this author in the library
        cmd = "select count('bookname'),bookname from books where authorid=? "
        cmd += "group by bookname having ( count(bookname) > 1 )"
        res = db.select(cmd, (authorid,))
        dupes = len(res)
        if dupes:
            for item in res:
                cmd = "select BookID,BookSub,Status,AudioStatus from books where bookname=? and authorid=?"
                dupe_books = db.select(cmd, (item['bookname'], authorid))
                cnt = len(dupe_books)
                booksubs = []
                for dupe in dupe_books:
                    cnt -= 1
                    if dupe['Status'] not in ['Ignored', 'Skipped'] \
                            or dupe['AudioStatus'] not in ['Ignored', 'Skipped']:
                        # this one is important (owned/wanted/snatched)
                        logger.debug("Keeping bookid %s (%s/%s)" %
                                     (dupe['BookID'], dupe['Status'], dupe['AudioStatus']))
                    elif dupe['BookSub'] not in booksubs:
                        booksubs.append(dupe['BookSub'])
                        logger.debug("Keeping bookid %s [%s][%s]" %
                                     (dupe['BookID'], item['bookname'], dupe['BookSub']))
                    elif cnt:
                        logger.debug("Removing bookid %s (%s/%s) %s" %
                                     (dupe['BookID'], dupe['Status'], dupe['AudioStatus'], item['bookname']))
                    else:
                        logger.debug("Not removing bookid %s (%s/%s) last entry for %s" %
                                     (dupe['BookID'], dupe['Status'], dupe['AudioStatus'], item['bookname']))

        # Warn about any remaining unignored dupes
        cmd = "select count('bookname'),bookname from books where authorid=? and "
        cmd += "( Status != 'Ignored' or AudioStatus != 'Ignored' ) group by bookname having ( count(bookname) > 1 )"
        res = db.select(cmd, (authorid,))
        dupes = len(res)
        if dupes:
            author = db.match("SELECT AuthorName from authors where AuthorID=?", (authorid,))
            logger.warn("There %s %s duplicate %s for %s" % (plural(dupes, 'is'), dupes, plural(dupes, 'title'),
                                                             author['AuthorName']))
            for item in res:
                logger.debug("%02d: %s" % (item[0], item[1]))

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='gr.find_book'):
        logger.debug("bookstatus=%s, audiostatus=%s" % (bookstatus, audiostatus))
        db = database.DBConnection()
        url = '/'.join([lazylibrarian.CONFIG['GR_URL'], 'book/show/' + bookid + '?' + urlencode(self.params)])
        try:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(url)
            rootxml, _ = gr_xml_request(url)
            if rootxml is None:
                logger.debug("Error requesting book")
                return
        except Exception as e:
            logger.error("%s finding book: %s" % (type(e).__name__, str(e)))
            return

        if not bookstatus:
            bookstatus = lazylibrarian.CONFIG['NEWBOOK_STATUS']
            logger.debug("No bookstatus passed, using default %s" % bookstatus)
        if not audiostatus:
            audiostatus = lazylibrarian.CONFIG['NEWAUDIO_STATUS']
            logger.debug("No audiostatus passed, using default %s" % audiostatus)
        logger.debug("bookstatus=%s, audiostatus=%s" % (bookstatus, audiostatus))
        book_language = rootxml.find('./book/language_code').text
        bookname = rootxml.find('./book/title').text

        if not book_language:
            book_language = "Unknown"
        #
        # user has said they want this book, don't block for unwanted language etc
        # Ignore book if adding as part of a series, else just warn and include it
        #
        valid_langs = get_list(lazylibrarian.CONFIG['IMP_PREFLANG'])
        if book_language not in valid_langs and 'All' not in valid_langs:
            msg = 'Book %s Language [%s] does not match preference' % (bookname, book_language)
            logger.warn(msg)
            if reason.startswith("Series:"):
                return

        if rootxml.find('./book/work/original_publication_year').text is None:
            originalpubdate = ''
            if rootxml.find('./book/publication_year').text is None:
                bookdate = "0000"
            else:
                bookdate = rootxml.find('./book/publication_year').text
                if check_year(bookdate, past=1800, future=0):
                    try:
                        mn = check_int(rootxml.find('./book/publication_month').text, 0)
                        dy = check_int(rootxml.find('./book/publication_day').text, 0)
                        if mn and dy:
                            bookdate = "%s-%02d-%02d" % (bookdate, mn, dy)
                    except (KeyError, AttributeError):
                        pass
        else:
            originalpubdate = rootxml.find('./book/work/original_publication_year').text
            if check_year(originalpubdate, past=1800, future=0):
                try:
                    mn = check_int(rootxml.find('./book/work/original_publication_month').text, 0)
                    dy = check_int(rootxml.find('./book/work/original_publication_day').text, 0)
                    if mn and dy:
                        originalpubdate = "%s-%02d-%02d" % (originalpubdate, mn, dy)
                except (KeyError, AttributeError):
                    pass
            bookdate = originalpubdate

        if lazylibrarian.CONFIG['NO_PUBDATE']:
            if not bookdate or bookdate == '0000':
                msg = 'Book %s Publication date [%s] does not match preference' % (bookname, bookdate)
                logger.warn(msg)
                if reason.startswith("Series:"):
                    return

        if lazylibrarian.CONFIG['NO_FUTURE']:
            # may have yyyy or yyyy-mm-dd
            if bookdate > today()[:len(bookdate)]:
                msg = 'Book %s Future publication date [%s] does not match preference' % (bookname, bookdate)
                logger.warn(msg)
                if reason.startswith("Series:"):
                    return

        if lazylibrarian.CONFIG['NO_SETS']:
            if re.search(r'\d+ of \d+', bookname) or re.search(r'\d+/\d+', bookname):
                msg = 'Book %s Set or Part' % bookname
                logger.warn(msg)
                if reason.startswith("Series:"):
                    return

            # allow date ranges eg 1981-95
            m = re.search(r'(\d+)-(\d+)', bookname)
            if m:
                if check_year(m.group(1), past=1800, future=0):
                    msg = "Allow %s, looks like a date range" % m.group(1)
                    logger.debug(msg)
                else:
                    msg = 'Set or Part %s' % bookname
                    logger.warn(msg)
                    if reason.startswith("Series:"):
                        return
            elif re.search(r'\w+\s*/\s*\w+', bookname):
                msg = 'Set or Part %s' % bookname
                logger.warn(msg)
                if reason.startswith("Series:"):
                    return
        try:
            bookimg = rootxml.find('./book/img_url').text
            if not bookimg or 'nocover' in bookimg or 'nophoto' in bookimg:
                bookimg = 'images/nocover.png'
        except (KeyError, AttributeError):
            bookimg = 'images/nocover.png'

        authorname = rootxml.find('./book/authors/author/name').text
        authorid = rootxml.find('./book/authors/author/id').text
        bookdesc = rootxml.find('./book/description').text
        bookisbn = rootxml.find('./book/isbn13').text
        if not bookisbn:
            bookisbn = rootxml.find('./book/isbn').text
        bookpub = rootxml.find('./book/publisher').text
        booklink = rootxml.find('./book/link').text
        bookrate = check_float(rootxml.find('./book/average_rating').text, 0)
        bookpages = rootxml.find('.book/num_pages').text
        workid = rootxml.find('.book/work/id').text

        match = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
        if match:
            author = {'authorid': authorid, 'authorname': match['AuthorName']}
        else:
            match = db.match('SELECT AuthorID from authors WHERE AuthorName=?', (authorname,))
            if match:
                logger.debug('%s: Changing authorid from %s to %s' %
                             (authorname, authorid, match['AuthorID']))
                author = {'authorid': match['AuthorID'], 'authorname': authorname}
            else:
                gr = GoodReads(authorname)
                author = gr.find_author_id()
        if author:
            author_id = author['authorid']
            match = db.match('SELECT * from authors WHERE AuthorID=?', (author_id,))
            if not match:
                # no author but request to add book, add author with newauthor status
                # User hit "add book" button from a search, or a wishlist import, or api call
                newauthor_status = 'Active'
                if lazylibrarian.CONFIG['NEWAUTHOR_STATUS'] in ['Skipped', 'Ignored']:
                    newauthor_status = 'Paused'
                # also pause author if adding as a series contributor/wishlist/grsync
                if reason.startswith("Series:") or "grsync" in reason or "wishlist" in reason:
                    newauthor_status = 'Paused'
                control_value_dict = {"AuthorID": author_id}
                new_value_dict = {
                    "AuthorName": author['authorname'],
                    "AuthorImg": author['authorimg'],
                    "AuthorLink": author['authorlink'],
                    "AuthorBorn": author['authorborn'],
                    "AuthorDeath": author['authordeath'],
                    "DateAdded": today(),
                    "Updated": int(time.time()),
                    "Status": newauthor_status,
                    "Reason": reason
                }
                logger.debug("Adding author %s %s, %s" % (author_id, author['authorname'], newauthor_status))
                # cmd = 'insert into authors (AuthorID, AuthorName, AuthorImg, AuthorLink, AuthorBorn,'
                # cmd += ' AuthorDeath, DateAdded, Updated, Status, Reason) values (?,?,?,?,?,?,?,?,?,?)'
                # db.action(cmd, (AuthorID, author['authorname'], author['authorimg'], author['authorlink'],
                #                   author['authorborn'], author['authordeath'], today(), int(time.time()),
                #                   newauthor_status, reason))

                db.upsert("authors", new_value_dict, control_value_dict)
                db.commit()  # shouldn't really be necessary as context manager commits?
                authorname = author['authorname']
                if lazylibrarian.CONFIG['NEWAUTHOR_BOOKS'] and newauthor_status != 'Paused':
                    self.get_author_books(author_id, entrystatus=lazylibrarian.CONFIG['NEWAUTHOR_STATUS'],
                                          reason=reason)
        else:
            logger.warn("No AuthorID for %s, unable to add book %s" % (authorname, bookname))
            return

        # bookname = unaccented(bookname, only_ascii=False)
        bookname, booksub, bookseries = split_title(authorname, bookname)
        dic = {':': '.', '"': ''}
        bookname = replace_all(bookname, dic).strip()
        booksub = replace_all(booksub, dic).strip()
        if bookseries:
            series, series_num = book_series(bookseries)
        elif booksub:
            series, series_num = book_series(booksub)
        else:
            series, series_num = book_series(bookname)

        if not bookisbn:
            try:
                res = isbn_from_words(bookname + ' ' + unaccented(authorname, only_ascii=False))
            except Exception as e:
                res = None
                logger.warn("Error from isbn: %s" % e)
            if res:
                logger.debug("isbn found %s for %s" % (res, bookname))
                bookisbn = res

        bookgenre = ''
        genres, _ = get_gr_genres(bookid)
        if genres:
            bookgenre = ', '.join(genres)
        if not bookdesc:
            infodict = get_gb_info(isbn=bookisbn, author=authorname, title=bookname, expire=False)
            if infodict is not None:  # None if api blocked
                if infodict and infodict['desc']:
                    bookdesc = infodict['desc']
                else:
                    bookdesc = 'No Description'
                if not bookgenre:
                    if infodict and infodict['genre']:
                        bookgenre = genre_filter(infodict['genre'])
                    else:
                        bookgenre = 'Unknown'

        threadname = thread_name()
        reason = "[%s] %s" % (threadname, reason)
        match = db.match("SELECT * from authors where AuthorID=?", (author_id,))
        if not match:
            logger.warn("Authorid %s not found in database, unable to add %s" % (author_id, bookname))
        else:
            control_value_dict = {"BookID": bookid}
            new_value_dict = {
                "AuthorID": author_id,
                "BookName": bookname,
                "BookSub": booksub,
                "BookDesc": bookdesc,
                "BookIsbn": bookisbn,
                "BookPub": bookpub,
                "BookGenre": bookgenre,
                "BookImg": bookimg,
                "BookLink": booklink,
                "BookRate": bookrate,
                "BookPages": bookpages,
                "BookDate": bookdate,
                "BookLang": book_language,
                "Status": bookstatus,
                "AudioStatus": audiostatus,
                "BookAdded": today(),
                "WorkID": workid,
                "gr_id": bookid,
                "ScanResult": reason,
                "OriginalPubDate": originalpubdate
            }

            db.upsert("books", new_value_dict, control_value_dict)
            logger.info("%s by %s added to the books database, %s/%s" % (bookname, authorname, bookstatus, audiostatus))

            if 'nocover' in bookimg or 'nophoto' in bookimg:
                # try to get a cover from another source
                workcover, source = get_book_cover(bookid)
                if workcover:
                    logger.debug('Updated cover for %s using %s' % (bookname, source))
                    control_value_dict = {"BookID": bookid}
                    new_value_dict = {"BookImg": workcover}
                    db.upsert("books", new_value_dict, control_value_dict)

            elif bookimg and bookimg.startswith('http'):
                link, success, _ = cache_img("book", bookid, bookimg)
                if success:
                    control_value_dict = {"BookID": bookid}
                    new_value_dict = {"BookImg": link}
                    db.upsert("books", new_value_dict, control_value_dict)
                else:
                    logger.debug('Failed to cache image for %s' % bookimg)

            serieslist = []
            if series:
                serieslist = [('', series_num, clean_name(series, '&/'))]
            if lazylibrarian.CONFIG['ADD_SERIES'] and "Ignored:" not in reason:
                newserieslist = get_work_series(workid, 'GR', reason=reason)
                if newserieslist:
                    serieslist = newserieslist
                    logger.debug('Updated series: %s [%s]' % (bookid, serieslist))
                set_series(serieslist, bookid, reason=reason)

            set_genres(get_list(bookgenre, ','), bookid)

            worklink = get_work_page(bookid)
            if worklink:
                control_value_dict = {"BookID": bookid}
                new_value_dict = {"WorkPage": worklink}
                db.upsert("books", new_value_dict, control_value_dict)
