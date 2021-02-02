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
import threading
import time
import traceback
import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.cache import json_request, html_request, cache_img
from lazylibrarian.formatter import check_float, check_int, now, is_valid_isbn, makeUnicode, formatAuthorName, \
    getList, makeUTF8bytes, plural, unaccented, replace_all, check_year, today, dateFormat
from lazylibrarian.bookwork import librarything_wait, isbn_from_words, get_gb_info, genreFilter, getStatus, \
    thingLang
from six import PY2
try:
    import html5lib
    from bs4 import BeautifulSoup
except ImportError:
    if PY2:
        from lib.bs4 import BeautifulSoup
    else:
        from lib3.bs4 import BeautifulSoup

# noinspection PyUnresolvedReferences
from six.moves.urllib_parse import quote_plus

from lazylibrarian.images import getBookCover

try:
    from fuzzywuzzy import fuzz
except ImportError:
    from lib.fuzzywuzzy import fuzz


class OpenLibrary:
    # https://openlibrary.org/developers/api

    def __init__(self, name=None):
        self.OL_SEARCH = "https://openlibrary.org/search.json?"
        self.OL_AUTHOR = "https://openlibrary.org/authors/"
        self.OL_ISBN = "https://openlibrary.org/isbn/"
        self.OL_WORK = "https://openlibrary.org/works/"
        self.OL_BOOK = "https://openlibrary.org/books/"
        self.LT_SERIES = 'http://www.librarything.com/nseries/'
        self.LT_WORK = "https://www.librarything.com/work/"
        self.name = makeUnicode(name)
        self.lt_cache = False

    def find_results(self, searchterm=None, queue=None):
        # noinspection PyBroadException
        try:
            resultlist = []
            resultcount = 0
            api_hits = 0
            searchtitle = ''
            searchauthorname = ''
            offset = 0
            next_page = True
            loopCount = 1

            if ' <ll> ' in searchterm:  # special token separates title from author
                searchtitle, searchauthorname = searchterm.split(' <ll> ')
                searchterm = searchterm.replace(' <ll> ', ' ')
                searchtitle = searchtitle.split(' (')[0]  # without any series info

            logger.debug('Now searching OpenLibrary API with searchterm: %s' % searchterm)
            searchbytes, _ = makeUTF8bytes(searchterm)
            searchbytes = searchbytes.replace(b'#', b'').replace(b'/', b'_')
            baseurl = self.OL_SEARCH + 'q=' + quote_plus(searchbytes)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(baseurl)

            while next_page:
                url = baseurl
                if offset:
                    url += "&offset=%s" % offset
                results, in_cache = json_request(url)
                if not in_cache:
                    api_hits += 1
                if results and 'numFound' in results:
                    logger.debug("Found %s results for searchterm, page %s" % (results['numFound'], offset))
                else:
                    break

                for book in results['docs']:
                    authorName = book.get('author_name')
                    if authorName:
                        authorName = authorName[0]
                    booklink = book.get('key')
                    bookid = ''
                    if booklink:
                        bookid = booklink.split('/')[-1]
                    authorid = book.get('author_key')
                    if authorid:
                        authorid = authorid[0]
                    bookTitle = book.get('title')
                    booksub = ''
                    bookisbn = book.get('isbn')
                    if bookisbn:
                        bookisbn = bookisbn[0]
                    bookpub = book.get('first_publish_year')
                    booklang = book.get('lang')
                    bookdate = ''
                    if booklang:
                        booklang = ', '.join(booklang)
                    bookrate = 0
                    bookrate_count = 0
                    cover = book.get('cover_i')
                    if not cover:
                        cover = 'images/nocover.png'
                    else:
                        cover = 'http://covers.openlibrary.org/b/id/%s-S.jpg' % cover
                    bookpages = 0
                    bookgenre = ''
                    bookdesc = ''
                    workid = book.get('id_librarything')
                    if workid:
                        workid = workid[0]

                    if searchauthorname:
                        author_fuzz = fuzz.token_set_ratio(authorName, searchauthorname)
                    else:
                        author_fuzz = fuzz.token_set_ratio(authorName, searchterm)
                    if searchtitle:
                        if bookTitle.endswith(')'):
                            bookTitle = bookTitle.rsplit(' (', 1)[0]
                        book_fuzz = fuzz.token_set_ratio(bookTitle, searchtitle)
                        # lose a point for each extra word in the fuzzy matches so we get the closest match
                        words = len(getList(bookTitle))
                        words -= len(getList(searchtitle))
                        book_fuzz -= abs(words)
                    else:
                        book_fuzz = fuzz.token_set_ratio(bookTitle, searchterm)
                        words = len(getList(bookTitle))
                        words -= len(getList(searchterm))
                        book_fuzz -= abs(words)
                    isbn_fuzz = 0
                    if is_valid_isbn(searchterm):
                        isbn_fuzz = 100
                        bookisbn = searchterm

                    highest_fuzz = max((author_fuzz + book_fuzz) / 2, isbn_fuzz)

                    if bookid and authorid:
                        resultlist.append({
                            'authorname': authorName,
                            'bookid': bookid,
                            'authorid': authorid,
                            'bookname': bookTitle,
                            'booksub': booksub,
                            'bookisbn': bookisbn,
                            'bookpub': bookpub,
                            'bookdate': bookdate,
                            'booklang': booklang,
                            'booklink': booklink,
                            'bookrate': bookrate,
                            'bookrate_count': bookrate_count,
                            'bookimg': cover,
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

                loopCount += 1
                if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < loopCount:
                    logger.warn('Maximum results page search reached, still more results available')
                    next_page = False

                offset += len(results['docs'])
                if offset >= check_int(results["numFound"], 0):
                    next_page = False

            logger.debug('Found %s %s with keyword: %s' % (resultcount, plural(resultcount, "result"), searchterm))
            logger.debug('The OpenLibrary API was hit %s %s for keyword %s' %
                         (api_hits, plural(api_hits, "time"), searchterm))

            queue.put(resultlist)

        except Exception:
            logger.error('Unhandled exception in OL.find_results: %s' % traceback.format_exc())

    def find_author_id(self, refresh=False):
        authorname = self.name.replace('#', '').replace('/', '_')
        title = ''
        if '<ll>' in authorname:
            authorname, title = authorname.split('<ll>')
        authorname = formatAuthorName(authorname)
        if title:
            authorbooks, in_cache = json_request(self.OL_SEARCH + "author=" + quote_plus(authorname) +
                                                 "&title=" + quote_plus(title), useCache=not refresh)
        else:
            authorbooks, in_cache = json_request(self.OL_SEARCH + "author=" + quote_plus(authorname),
                                                 useCache=not refresh)

        if authorbooks and authorbooks["numFound"]:
            for book in authorbooks['docs']:
                author_name = formatAuthorName(book.get('author_name')[0])
                if fuzz.token_set_ratio(author_name, authorname) >= lazylibrarian.CONFIG['NAME_RATIO']:
                    key = book.get('author_key')[0]
                    if key:
                        key = key.split('/')[-1]
                    res = self.get_author_info(key)
                    if res and res['authorname'] != authorname:
                        res['aka'] = authorname
                    return res

        if title:  # no results using author/title, try author only
            authorbooks, in_cache = json_request(self.OL_SEARCH + "author=" + quote_plus(authorname),
                                                 useCache=not refresh)
            if not authorbooks or not authorbooks["numFound"]:
                logger.debug("No books found for %s" % authorname)
                return None
            for book in authorbooks['docs']:
                author_name = formatAuthorName(book.get('author_name')[0])
                if fuzz.token_set_ratio(author_name, authorname) >= lazylibrarian.CONFIG['NAME_RATIO']:
                    key = book.get('author_key')[0]
                    if key:
                        key = key.split('/')[-1]
                    res = self.get_author_info(key)
                    if res and res['authorname'] != authorname:
                        res['aka'] = authorname
                    return res
        return {}

    def get_author_info(self, authorid=None):
        logger.debug("Getting author info for %s" % authorid)
        authorinfo, in_cache = json_request(self.OL_AUTHOR + authorid + '.json')
        if not authorinfo:
            logger.debug("No info found for %s" % authorid)
            return None

        bio = authorinfo.get('bio', '')
        if bio and isinstance(bio, dict):
            about = bio.get('value', '')
        else:
            about = ''

        photos = authorinfo.get('photos', '')
        if photos and isinstance(photos, list):
            author_img = 'http://covers.openlibrary.org/a/id/%s-M.jpg' % photos[0]
        else:
            author_img = 'images/nophoto.png'

        author_link = self.OL_AUTHOR + authorid
        author_name = authorinfo.get('name', '')
        author_born = authorinfo.get('birth_date', '')
        author_died = authorinfo.get('death_date', '')

        if "," in author_name:
            postfix = getList(lazylibrarian.CONFIG['NAME_POSTFIX'])
            words = author_name.split(',')
            if len(words) == 2:
                if words[0].strip().strip('.').lower in postfix:
                    author_name = words[1].strip() + ' ' + words[0].strip()

        if not author_name:
            logger.warn("Rejecting authorid %s, no authorname" % authorid)
            return None

        logger.debug("[%s] Processing info for authorID: %s" % (author_name, authorid))
        author_dict = {
            'authorid': authorid,
            'authorlink': author_link,
            'authorimg': author_img,
            'authorborn': author_born,
            'authordeath': author_died,
            'about': about,
            'totalbooks': '0',
            'authorname': formatAuthorName(author_name)
        }
        return author_dict

    def get_series_members(self, series_id):
        if not self.lt_cache:
            librarything_wait()
        data, self.lt_cache = html_request(self.LT_SERIES + series_id)
        results = []
        if data:
            try:
                table = data.split(b'>Core<')[1].split(b'<table>')[1].split(b'</table>')[0]
                rows = table.split(b'<tr>')
                for row in rows:
                    row = row.decode('utf-8')
                    if 'href=' in row:
                        try:
                            workid = row.split('data-workid="')[1].split('"')[0].split('/')[-1]
                        except IndexError:
                            workid = ''
                        try:
                            bookname = row.split('data-title="')[1].split('>')[1].split('<')[0]
                        except IndexError:
                            bookname = ''
                        try:
                            authorlink = row.split('href="')[2].split('">')[0]
                            authorname = row.split('href="')[2].split('">')[1].split('<')[0]
                        except IndexError:
                            authorlink = ''
                            authorname = ''
                        try:
                            order = row.split('<td class="right">')[1].split('</label>')[1].split('<')[0]
                        except IndexError:
                            order = ''
                        onum = check_float(order, 0)
                        if onum == int(onum):  # drop the ".0"
                            onum = int(onum)
                        if onum:  # might not be numeric
                            order = onum
                        results.append([order, bookname, authorname, authorlink, workid])

            except IndexError:
                if b'>Core<' in data:  # error parsing, or just no series data available?
                    logger.debug('Error parsing series table for %s' % series_id)
            finally:
                return results

    def lt_workinfo(self, lt_id):
        if not self.lt_cache:
            librarything_wait()
        result, self.lt_cache = html_request(self.LT_WORK + lt_id)
        if not result:
            return -1, [], []

        soup = BeautifulSoup(result, "html5lib")
        try:
            rating = check_float(result.split(b'<span class="dark_hint">(', 1)[1].split(b')</span>', 1)[0], 0)
            if rating == int(rating):  # drop the ".0"
                rating = int(rating)
        except IndexError:
            rating = 0

        genrelist = []
        tags = soup.find_all("div", class_="tags")
        if tags:
            taglines = tags[0].text
            for lyne in taglines.split('\n'):
                if lyne:
                    try:
                        name, count = lyne.rsplit('(', 1)
                        count = check_int(count.split(')', 1)[0], 0)
                        name = name.strip()
                        accept = True
                        if lazylibrarian.GRGENRES:
                            if name in lazylibrarian.GRGENRES.get('genreExclude', []):
                                accept = False
                            if accept:
                                for item in lazylibrarian.GRGENRES.get('genreExcludeParts', []):
                                    if item in name:
                                        accept = False
                                        break
                            if accept:
                                for item in lazylibrarian.GRGENRES.get('genreReplace', []):
                                    if name == item:
                                        name = lazylibrarian.GRGENRES['genreReplace'][item]
                                        break
                        if accept and count > 1:
                            genrelist.append([name, count])
                    except (IndexError, ValueError):
                        logger.error("Split genre error [%s]" % lyne)
                        pass
            if genrelist:
                genrelist.sort(key=lambda x: x[1], reverse=True)
                limit = lazylibrarian.GRGENRES.get('genreLimit', 0)
                if limit:
                    genrelist = genrelist[:limit]

        serieslist = []
        try:
            seriesinfo = result.split(b"<h2>Belongs to Series</h2><div>")[1].split(b"</div>")[0]
            seriesinfo = seriesinfo.split(b'href="')[1:]
            for item in seriesinfo:
                name, count = item.split(b"</a>")
                count = count.split(b'(')[1].split(b')')[0]
                seriesid, name = name.split(b'">')
                name = name.split(b'<')[0]
                count = check_float(count, 0)
                if count == int(count):  # drop the ".0"
                    count = int(count)
                name = name.decode('utf-8')
                seriesid = seriesid.decode('utf-8').split('/')[2]
                serieslist.append([name, count, seriesid])
        except IndexError:
            pass

        return rating, genrelist, serieslist

    def get_author_books(self, authorid=None, authorname=None, bookstatus="Skipped", audiostatus='Skipped',
                         entrystatus='Active', refresh=False, reason='ol.get_author_books'):
        myDB = database.DBConnection()
        offset = 0
        next_page = True
        entryreason = reason
        removedResults = 0
        duplicates = 0
        ignored = 0
        added_count = 0
        updated_count = 0
        book_ignore_count = 0
        total_count = 0
        locked_count = 0
        loopCount = 0
        cover_count = 0
        isbn_count = 0
        cover_time = 0
        isbn_time = 0
        api_hits = 0
        gr_lang_hits = 0
        lt_lang_hits = 0
        gb_lang_change = 0
        auth_start = time.time()

        # these are reject reasons we might want to override, so optionally add to database as "ignored"
        ignorable = ['future', 'date', 'isbn', 'word', 'set']
        if lazylibrarian.CONFIG['NO_LANG']:
            ignorable.append('lang')

        while next_page:
            url = self.OL_SEARCH + "author=" + authorid
            if offset:
                url += "&offset=%s" % offset
            authorbooks, in_cache = json_request(url, useCache=not refresh)
            api_hits += not in_cache
            cache_hits = in_cache
            if not authorbooks or not authorbooks["numFound"]:
                logger.debug("No books found for key %s" % authorid)
                next_page = False
            docs = authorbooks.get('docs', [])
            for book in docs:
                book_status = bookstatus
                audio_status = audiostatus
                auth_name = book.get('author_name')[0]
                title = book.get('title')
                cover = book.get('cover_i')
                isbns = book.get('isbn')
                link = book.get('key')
                bookpages = 0
                bookdesc = ''
                key = book.get('key').split('/')[-1]
                first_publish_year = book.get('first_publish_year')
                auth_key = book.get('author_key')[0]
                languages = book.get('language')
                publish_date = book.get('publish_date', '')
                publishers = book.get('publisher')
                id_librarything = book.get('id_librarything')
                if publish_date:
                    publish_date = dateFormat(publish_date[0])
                if languages:
                    lang = ', '.join(languages)
                else:
                    lang = ''
                if isbns:
                    isbn = isbns[0]
                    if len(isbn) == 10:
                        isbnhead = isbn[0:3]
                    elif len(isbn) == 13:
                        isbnhead = isbn[3:6]
                    else:
                        isbnhead = ''
                else:
                    isbn = ''
                    isbnhead = ''

                rejected = False
                wantedlanguages = ['eng']
                if wantedlanguages and 'All' not in wantedlanguages:
                    if languages:
                        valid_lang = all(item in languages for item in wantedlanguages)
                        if not valid_lang:
                            rejected = 'lang', 'Invalid language: %s' % str(languages)
                    else:

                        # Try to use shortcut of ISBN identifier codes described here...
                        # http://en.wikipedia.org/wiki/List_of_ISBN_identifier_groups
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
                            match = myDB.match('SELECT lang FROM languages where isbn=?', (isbnhead,))
                            if match:
                                lang = match['lang']
                                cache_hits += 1
                                logger.debug("Found cached language [%s] for %s [%s]" %
                                             (lang, title, isbnhead))
                            else:
                                lang = thingLang(isbn)
                                lt_lang_hits += 1
                                if lang:
                                    myDB.action('insert into languages values (?, ?)', (isbnhead, lang))

                        if lang and lang not in wantedlanguages:
                            rejected = 'lang', 'Invalid language: %s' % lang

                        if not lang:
                            rejected = 'lang', 'No language'

                if not rejected and not title:
                    rejected = 'name', 'No title'

                exists = myDB.match("SELECT * from books WHERE BookName=? COLLATE NOCASE", (title,))
                if exists and not rejected:
                    rejected = 'name', 'Duplicate title'

                if not rejected and publishers:
                    for bookpub in publishers:
                        if bookpub.lower() in getList(lazylibrarian.CONFIG['REJECT_PUBLISHER']):
                            rejected = 'publisher', bookpub
                            break
                if publishers and not rejected:
                    publishers = ', '.join(publishers)

                if not rejected and not isbnhead and lazylibrarian.CONFIG['ISBN_LOOKUP']:
                    # try lookup by name
                    if title:
                        try:
                            res = isbn_from_words(unaccented(title, only_ascii=False) + ' ' +
                                                  unaccented(auth_name, only_ascii=False))
                            isbn_count += 1
                        except Exception as e:
                            res = None
                            logger.warn("Error from isbn: %s" % e)
                        if res:
                            logger.debug("isbn found %s for %s" % (res, key))
                            isbn = res
                            if len(res) == 13:
                                isbnhead = res[3:6]
                            else:
                                isbnhead = res[0:3]

                if not isbnhead and lazylibrarian.CONFIG['NO_ISBN']:
                    rejected = 'isbn', 'No ISBN'

                if not rejected:
                    dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '',
                           '[': ' ', ']': ' ', '#': '# ', ':': ' ', ';': ' '}
                    name = replace_all(title, dic).strip()
                    name = name.lower()
                    # remove extra spaces if they're in a row
                    name = " ".join(name.split())
                    namewords = name.split(' ')
                    badwords = getList(lazylibrarian.CONFIG['REJECT_WORDS'], ',')
                    for word in badwords:
                        if (' ' in word and word in name) or word in namewords:
                            rejected = 'word', 'Contains [%s]' % word
                            break

                if not rejected:
                    bookname = unaccented(title, only_ascii=False)
                    if lazylibrarian.CONFIG['NO_SETS']:
                        # allow date ranges eg 1981-95
                        m = re.search(r'(\d+)-(\d+)', bookname)
                        if m:
                            if check_year(m.group(1), past=1800, future=0):
                                logger.debug("Allow %s, looks like a date range" % bookname)
                            else:
                                rejected = 'set', 'Set or Part %s' % m.group(0)
                        elif re.search(r'\d+ of \d+', bookname) or \
                                re.search(r'\d+/\d+', bookname):
                            rejected = 'set', 'Set or Part'

                if rejected and rejected[0] not in ignorable:
                    logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                elif rejected and not (rejected[0] in ignorable and lazylibrarian.CONFIG['IMP_IGNORE']):
                    logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                else:
                    logger.debug("Found title: %s %s" % (title, id_librarything))
                    if not rejected and lazylibrarian.CONFIG['NO_FUTURE']:
                        if publish_date > today()[:len(publish_date)]:
                            if ignorable is None:
                                rejected = 'future', 'Future publication date [%s]' % publish_date
                                logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                            else:
                                logger.debug("Not rejecting %s (future pub date %s) as %s" %
                                             (title, publish_date, ignorable))

                    if not rejected and lazylibrarian.CONFIG['NO_PUBDATE']:
                        if not publish_date or publish_date == '0000':
                            if ignorable is None:
                                rejected = 'date', 'No publication date'
                                logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                            else:
                                logger.debug("Not rejecting %s (no pub date) as %s" %
                                             (title, ignorable))

                    if rejected:
                        if rejected[0] in ignorable:
                            book_status = 'Ignored'
                            audio_status = 'Ignored'
                            book_ignore_count += 1
                            reason = "Ignored: %s" % rejected[1]
                        else:
                            reason = "Rejected: %s" % rejected[1]
                    else:
                        if 'authorUpdate' in entryreason:
                            reason = 'Author: %s' % auth_name
                        else:
                            reason = entryreason

                    if not cover:
                        cover = 'images/nocover.png'
                    else:
                        cover = 'http://covers.openlibrary.org/b/id/%s-M.jpg' % cover
                    rating = 0
                    seriesdisplay = ''
                    if id_librarything:
                        id_librarything = id_librarything[0]
                        rating, genrelist, serieslist = self.lt_workinfo(id_librarything)
                        if rating >= 0:
                            genrenames = []
                            for item in genrelist:
                                genrenames.append(genreFilter(item[0]))
                            genres = ', '.join(set(genrenames))
                            updateValueDict = {}
                            exists = myDB.match("SELECT * from books WHERE LT_Workid=?", (id_librarything,))
                            if exists:
                                locked = exists['Manual']
                                if locked is None:
                                    locked = False
                                elif locked.isdigit():
                                    locked = bool(int(locked))
                            else:
                                locked = False
                                bookdate = publish_date
                                bookrate = rating
                                infodict = get_gb_info(isbn=isbn, author=auth_name, title=title, expire=False)
                                if infodict:
                                    gbupdate = []
                                    if infodict['desc']:
                                        bookdesc = infodict['desc']
                                        gbupdate.append("Description")
                                    else:
                                        bookdesc = 'No Description'
                                    if not genres and infodict['genre']:
                                        genres = genreFilter(infodict['genre'])
                                        gbupdate.append('Genres')
                                    if not bookdate or bookdate == '0000' or len(infodict['date']) > len(bookdate):
                                        bookdate = infodict['date']
                                        gbupdate.append('Publication Date')
                                    if infodict['pub'] and not publishers:
                                        publishers = infodict['pub']
                                        gbupdate.append('Publisher')
                                    if infodict['rate'] and not bookrate:
                                        bookrate = infodict['rate']
                                        gbupdate.append('Rating')
                                    if infodict['pages'] and not bookpages:
                                        bookpages = infodict['pages']
                                        gbupdate.append('Pages')
                                    if gbupdate:
                                        logger.debug("Updated %s from googlebooks" % ', '.join(gbupdate))

                                if 'authorUpdate' in entryreason:
                                    reason = 'Author: %s' % auth_name
                                elif not reason:
                                    reason = entryreason
                                threadname = threading.currentThread().getName()
                                reason = "[%s] %s" % (threadname, reason)
                                myDB.action('INSERT INTO books (AuthorID, BookName, BookDesc, BookGenre, ' +
                                            'BookIsbn, BookPub, BookRate, BookImg, BookLink, BookID, BookDate, ' +
                                            'BookLang, BookAdded, Status, WorkPage, AudioStatus, LT_WorkID, ' +
                                            'ScanResult, OriginalPubDate, BookPages) ' +
                                            'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                            (authorid, title, bookdesc, genres, isbn, publishers, bookrate, cover,
                                             link, key, bookdate, lang, now(), book_status, '', audio_status,
                                             id_librarything, reason, first_publish_year, bookpages))

                            # Leave alone if locked
                            if locked:
                                locked_count += 1
                            else:
                                if exists and exists['ScanResult'] and ' publication date' in exists['ScanResult'] \
                                        and publish_date and publish_date != '0000' and \
                                        publish_date <= today()[:len(publish_date)]:
                                    # was rejected on previous scan but bookdate has become valid
                                    logger.debug("valid bookdate [%s] previous scanresult [%s]" %
                                                 (publish_date, exists['ScanResult']))

                                    updateValueDict["ScanResult"] = "bookdate %s is now valid" % publish_date

                                if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                    logger.debug("entry status %s %s,%s" % (entrystatus, bookstatus, audiostatus))
                                book_status, audio_status = getStatus(key, serieslist, bookstatus,
                                                                      audiostatus, entrystatus)
                                if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                    logger.debug("status is now %s,%s" % (book_status, audio_status))
                                updateValueDict["Status"] = book_status
                                updateValueDict["AudioStatus"] = audio_status
                                controlValueDict = {"LT_WorkID": id_librarything}
                                myDB.upsert("books", updateValueDict, controlValueDict)

                            if not exists:
                                typ = 'Added'
                                added_count += 1
                            else:
                                typ = 'Updated'
                                updated_count += 1
                            msg = "[%s] %s book: %s [%s] status %s" % (auth_name, typ, title,
                                                                       lang, bookstatus)
                            if lazylibrarian.SHOW_AUDIO:
                                msg += " audio %s" % audiostatus
                            logger.debug(msg)

                            if lazylibrarian.CONFIG['ADD_SERIES']:
                                for series in serieslist:
                                    newseries = "%s %s" % (series[0], series[1])
                                    newseries.strip()
                                    if seriesdisplay and newseries:
                                        seriesdisplay += '<br>'
                                    seriesdisplay += newseries
                                    exists = myDB.match("SELECT * from series WHERE seriesid=?", (series[2],))
                                    if not exists:
                                        logger.debug("New series: %s" % series[0])
                                        myDB.action('INSERT INTO series (SeriesID, SeriesName, Status, Updated,' +
                                                    ' Reason) VALUES (?,?,?,?,?)',
                                                    (series[2], series[0], 'Paused', time.time(), id_librarything))
                                    seriesmembers = self.get_series_members(series[2])
                                    if not seriesmembers:
                                        logger.warn("Series %s (%s) has no members at librarything" % (
                                                    series[0], series[2]))
                                        seriesmembers = [[series[1], title, auth_name, auth_key, id_librarything]]
                                    if seriesmembers:
                                        logger.debug("Found %s members for series %s" % (len(seriesmembers),
                                                                                         series[0]))
                                        for member in seriesmembers:
                                            # member[order, bookname, authorname, authorlink, workid]
                                            auth_name, exists = lazylibrarian.importer.getPreferredAuthorName(member[2])
                                            if not exists:
                                                lazylibrarian.importer.addAuthorNameToDB(author=member[2],
                                                                                         refresh=False, addbooks=False,
                                                                                         reason="Series author %s" %
                                                                                                series[0])
                                                auth_name, exists = \
                                                    lazylibrarian.importer.getPreferredAuthorName(member[2])
                                                if exists:
                                                    auth_name = member[2]
                                                else:
                                                    logger.debug("Unable to add %s for %s, author not in database" %
                                                                 (member[2], member[1]))
                                                break
                                            else:
                                                cmd = "SELECT * from authors WHERE authorname=?"
                                                exists = myDB.match(cmd, (auth_name,))
                                                if exists:
                                                    auth_key = exists['AuthorID']
                                                    if fuzz.ratio(auth_name.lower().replace('.', ''),
                                                                  member[2].lower().replace('.', '')) < 95:
                                                        akas = getList(exists['AKA'], ',')
                                                        if member[2] not in akas:
                                                            akas.append(member[2])
                                                            myDB.action("UPDATE authors SET AKA=? WHERE AuthorID=?",
                                                                        (', '.join(akas), auth_key))
                                                    match = myDB.match('SELECT * from seriesauthors WHERE ' +
                                                                       'SeriesID=? and AuthorID=?',
                                                                       (series[2], auth_key))
                                                    if not match:
                                                        logger.debug("Adding %s as series author for %s" %
                                                                     (auth_name, series[0]))
                                                        myDB.action('INSERT INTO seriesauthors ("SeriesID", ' +
                                                                    '"AuthorID") VALUES (?, ?)',
                                                                    (series[2], auth_key), suppress='UNIQUE')

                                            # if book not in library, use librarything workid to get an isbn
                                            # use that to get openlibrary workid
                                            # add book to library, then add seriesmember
                                            exists = myDB.match("SELECT * from books WHERE LT_Workid=?",
                                                                (member[4],))
                                            if exists:
                                                match = myDB.match("SELECT * from member WHERE " +
                                                                   "SeriesID=? AND BookID=?",
                                                                   (series[2], exists['BookID']))
                                                if not match:
                                                    logger.debug("Inserting new member [%s] for %s" %
                                                                 (member[0], series[0]))
                                                    myDB.action('INSERT INTO member (SeriesID, BookID, ' +
                                                                'WorkID, SeriesNum) VALUES (?,?,?,?)',
                                                                (series[2], exists['BookID'], member[4], member[0]))
                                                ser = myDB.match('select count(*) as counter from member ' +
                                                                 'where seriesid=?', (series[2],))
                                                if ser:
                                                    counter = check_int(ser['counter'], 0)
                                                    myDB.action("UPDATE series SET Total=? WHERE SeriesID=?",
                                                                (counter, series[2]))
                                            else:
                                                if not self.lt_cache:
                                                    librarything_wait()
                                                bookresult, self.lt_cache = html_request(self.LT_WORK + member[4])
                                                if bookresult:
                                                    isbns = bookresult.split(b'ISBN:')
                                                    isbnlist = []
                                                    for item in isbns:
                                                        item = item.decode('utf-8')
                                                        isbn = item.split('&', 1)[0].split(',', 1)[0]
                                                        if is_valid_isbn(isbn):
                                                            isbnlist.append(isbn)
                                                    workid = None
                                                    worklink = None
                                                    for isbn in isbnlist:
                                                        bookinfo, in_cache = json_request(self.OL_ISBN + isbn +
                                                                                          '.json')
                                                        api_hits += not in_cache
                                                        cache_hits = in_cache
                                                        if bookinfo and bookinfo.get('works'):
                                                            try:
                                                                for work in bookinfo.get('works'):
                                                                    worklink = work.get('key')
                                                                    workid = work.get('key').split('/')[-1]
                                                            except IndexError:
                                                                workid = None
                                                            if workid:
                                                                break
                                                    if workid:
                                                        workinfo, in_cache = json_request(self.OL_WORK +
                                                                                          workid + '.json')
                                                        api_hits += not in_cache
                                                        cache_hits = in_cache
                                                        if workinfo and 'title' in workinfo:
                                                            title = workinfo.get('title')
                                                            covers = workinfo.get('covers')
                                                            if covers:
                                                                cover = 'http://covers.openlibrary.org/b/id/'
                                                                cover += '%s-M.jpg' % covers[0]
                                                            else:
                                                                cover = 'images/nocover.png'
                                                            publish_date = dateFormat(workinfo.get('publish_date', ''))
                                                            rating, genrelist, _ = self.lt_workinfo(member[4])
                                                            genrenames = []
                                                            for item in genrelist:
                                                                genrenames.append(item[0])
                                                            genres = ', '.join(genrenames)
                                                            lang = ''
                                                            match = myDB.match('SELECT * from authors ' +
                                                                               'WHERE AuthorName=? COLLATE NOCASE',
                                                                               (auth_name,))
                                                            if match:
                                                                bauth_key = match['AuthorID']
                                                            else:
                                                                reason = "Series author %s" % series[0]
                                                                lazylibrarian.importer.addAuthorNameToDB(
                                                                    author=auth_name, refresh=False,
                                                                    addbooks=False, reason=reason)
                                                                match = myDB.match('SELECT * from authors ' +
                                                                                   'WHERE AuthorName=? COLLATE NOCASE',
                                                                                   (auth_name,))
                                                                if match:
                                                                    bauth_key = match['AuthorID']
                                                                else:
                                                                    msg = "Unable to add %s for %s" % (auth_name, title)
                                                                    msg += ", author not in database"
                                                                    logger.debug(msg)
                                                                    break

                                                            match = myDB.match('SELECT * from books ' +
                                                                               'WHERE BookID=?', (workid,))
                                                            if not match:
                                                                logger.debug("Inserting new member [%s] for %s" %
                                                                             (member[0], series[0]))

                                                                reason = "Member %s of series %s" % (member[0],
                                                                                                     series[0])
                                                                threadname = threading.currentThread().getName()
                                                                reason = "[%s] %s" % (threadname, reason)
                                                                added_count += 1
                                                                myDB.action('INSERT INTO books (AuthorID, ' +
                                                                            'BookName, BookDesc, BookGenre, ' +
                                                                            'BookIsbn, BookPub, BookRate, ' +
                                                                            'BookImg, BookLink, BookID, ' +
                                                                            'BookDate, BookLang, BookAdded, ' +
                                                                            'Status, WorkPage, AudioStatus, ' +
                                                                            'LT_WorkID, ScanResult, OriginalPubDate)' +
                                                                            ' VALUES ' +
                                                                            '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                                                            (bauth_key, title, '', genres, '', '',
                                                                             rating, cover, worklink, workid,
                                                                             bookstatus, '', publish_date, lang,
                                                                             '', audiostatus, member[4],
                                                                             reason, publish_date))

                                                                if 'nocover' in cover or 'nophoto' in cover:
                                                                    cover = get_cover(workid, title)
                                                                    cover_count += 1
                                                                if cover and cover.startswith('http'):
                                                                    cache_cover(workid, cover)

                                                            match = myDB.match("SELECT * from seriesauthors WHERE " +
                                                                               "SeriesID=? AND AuthorID=?",
                                                                               (series[2], bauth_key))
                                                            if not match:
                                                                logger.debug("Adding %s as series author for %s" %
                                                                             (auth_name, series[0]))
                                                                myDB.action('INSERT INTO seriesauthors ("SeriesID", ' +
                                                                            '"AuthorID") VALUES (?, ?)',
                                                                            (series[2], bauth_key), suppress='UNIQUE')

                                                            match = myDB.match("SELECT * from member WHERE " +
                                                                               "SeriesID=? AND BookID=?",
                                                                               (series[2], workid))
                                                            if not match:
                                                                myDB.action('INSERT INTO member ' +
                                                                            '(SeriesID, BookID, WorkID, SeriesNum)  ' +
                                                                            'VALUES (?,?,?,?)',
                                                                            (series[2], workid, member[4], member[0]))
                                                                ser = myDB.match('select count(*) as counter ' +
                                                                                 'from member where seriesid=?',
                                                                                 (series[2],))
                                                                if ser:
                                                                    counter = check_int(ser['counter'], 0)
                                                                    myDB.action("UPDATE series SET Total=? " +
                                                                                "WHERE SeriesID=?",
                                                                                (counter, series[2]))
                    if rating == 0:
                        logger.debug("No additional librarything info")
                        exists = myDB.match("SELECT * from books WHERE BookID=?", (key,))
                        if not exists:
                            logger.debug("Inserting new book for %s" % authorid)
                            if 'authorUpdate' in entryreason:
                                reason = 'Author: %s' % auth_name
                            else:
                                reason = entryreason
                            threadname = threading.currentThread().getName()
                            reason = "[%s] %s" % (threadname, reason)
                            added_count += 1
                            myDB.action('INSERT INTO books (AuthorID, BookName, BookImg, ' +
                                        'BookLink, BookID, BookDate, BookLang, BookAdded, Status, ' +
                                        'WorkPage, AudioStatus, ScanResult, OriginalPubDate) ' +
                                        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                        (authorid, title, cover, link, key, publish_date,
                                         lang, now(), bookstatus, '', audiostatus, reason, first_publish_year))
                            if 'nocover' in cover or 'nophoto' in cover:
                                cover = get_cover(key, title)
                                cover_count += 1
                            if cover and cover.startswith('http'):
                                cache_cover(key, cover)

                    if seriesdisplay:
                        myDB.action("UPDATE books SET SeriesDisplay=? WHERE BookID=?", (seriesdisplay, key))
                    added_count += 1

            offset += len(authorbooks['docs'])
            if offset >= check_int(authorbooks["numFound"], 0):
                next_page = False

            resultcount = added_count + updated_count
            loopCount -= 1
            logger.debug("Found %s %s in %s %s" % (total_count, plural(total_count, "result"),
                                                   loopCount, plural(loopCount, "page")))
            logger.debug("Found %s locked %s" % (locked_count, plural(locked_count, "book")))
            logger.debug("Removed %s unwanted language %s" % (ignored, plural(ignored, "result")))
            logger.debug("Removed %s incorrect/incomplete %s" % (removedResults, plural(removedResults, "result")))
            logger.debug("Removed %s duplicate %s" % (duplicates, plural(duplicates, "result")))
            logger.debug("Ignored %s %s" % (book_ignore_count, plural(book_ignore_count, "book")))
            logger.debug("Imported/Updated %s %s in %d secs using %s api %s" %
                         (resultcount, plural(resultcount, "book"), int(time.time() - auth_start),
                          api_hits, plural(api_hits, "hit")))
            if cover_count:
                logger.debug("Fetched %s %s in %.2f sec" % (cover_count, plural(cover_count, "cover"), cover_time))
            if isbn_count:
                logger.debug("Fetched %s ISBN in %.2f sec" % (isbn_count, isbn_time))

            controlValueDict = {"authorname": authorname.replace('"', '""')}
            newValueDict = {
                            "GR_book_hits": api_hits,
                            "GR_lang_hits": gr_lang_hits,
                            "LT_lang_hits": lt_lang_hits,
                            "GB_lang_change": gb_lang_change,
                            "cache_hits": cache_hits,
                            "bad_lang": ignored,
                            "bad_char": removedResults,
                            "uncached": api_hits,
                            "duplicates": duplicates
                            }
            myDB.upsert("stats", newValueDict, controlValueDict)

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='ol.find_book'):
        logger.debug("bookstatus=%s, audiostatus=%s" % (bookstatus, audiostatus))
        myDB = database.DBConnection()
        URL = self.OL_WORK + bookid + '.json'
        try:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(URL)
            workinfo, in_cache = json_request(URL)
            if not workinfo:
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

        if workinfo:
            title = workinfo.get('title', '')
            if not title:
                logger.warn("No title for %s, unable to add book" % bookid)
                return
            covers = workinfo.get('covers', '')
            if covers:
                cover = 'http://covers.openlibrary.org/b/id/'
                cover += '%s-M.jpg' % covers[0]
            else:
                cover = 'images/nocover.png'
            publish_date = dateFormat(workinfo.get('publish_date', ''))
            lang = "Unknown"
            #
            # user has said they want this book, don't block for unwanted language etc
            # Ignore book if adding as part of a series, else just warn and include it
            #
            valid_langs = getList(lazylibrarian.CONFIG['IMP_PREFLANG'])
            if lang not in valid_langs and 'All' not in valid_langs:
                msg = 'Book %s Language [%s] does not match preference' % (title, lang)
                logger.warn(msg)
                if reason.startswith("Series:"):
                    return
            originalpubdate = ''
            if publish_date:
                bookdate = publish_date
            else:
                bookdate = "0000"
            if lazylibrarian.CONFIG['NO_PUBDATE']:
                if not bookdate or bookdate == '0000':
                    msg = 'Book %s Publication date [%s] does not match preference' % (title, bookdate)
                    logger.warn(msg)
                    if reason.startswith("Series:"):
                        return

            if lazylibrarian.CONFIG['NO_FUTURE']:
                # may have yyyy or yyyy-mm-dd
                if bookdate > today()[:len(bookdate)]:
                    msg = 'Book %s Future publication date [%s] does not match preference' % (title, bookdate)
                    logger.warn(msg)
                    if reason.startswith("Series:"):
                        return

            if lazylibrarian.CONFIG['NO_SETS']:
                if re.search(r'\d+ of \d+', title) or re.search(r'\d+/\d+', title):
                    msg = 'Book %s Set or Part' % title
                    logger.warn(msg)
                    if reason.startswith("Series:"):
                        return

            # allow date ranges eg 1981-95
            m = re.search(r'(\d+)-(\d+)', title)
            if m:
                if check_year(m.group(1), past=1800, future=0):
                    msg = "Allow %s, looks like a date range" % m.group(1)
                    logger.debug(msg)
                else:
                    msg = 'Set or Part %s' % title
                    logger.warn(msg)
                    if reason.startswith("Series:"):
                        return

            authorname = ''
            authors = workinfo.get('authors')
            if authors:
                try:
                    authorid = authors[0]['author']['key']
                    authorid = authorid.split('/')[-1]
                except KeyError:
                    authorid = ''
            else:
                authorid = ''
            if not authorid:
                logger.warn("No AuthorID for %s, unable to add book" % title)
                return
            bookdesc = ''
            bookpub = ''
            booklink = workinfo.get('key')
            bookrate = 0
            bookpages = 0
            workid = ''
            bookisbn = ''
            bookgenre = ''
            match = myDB.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            if match:
                authorname = match['AuthorName']
            else:
                lazylibrarian.importer.addAuthorToDB(authorid=authorid, refresh=False, addbooks=False,
                                                     reason="ol.find_book %s" % bookid)
                match = myDB.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
                if match:
                    authorname = match['AuthorName']
            if not authorname:
                logger.warn("No AuthorName for %s, unable to add book %s" % (authorid, title))
                return
            try:
                res = isbn_from_words(title + ' ' + unaccented(authorname, only_ascii=False))
            except Exception as e:
                res = None
                logger.warn("Error from isbn: %s" % e)
            if res:
                logger.debug("isbn found %s for %s" % (res, title))
                bookisbn = res

            infodict = get_gb_info(isbn=bookisbn, author=authorname, title=title, expire=False)
            if infodict is not None:  # None if api blocked
                if infodict and infodict['desc']:
                    bookdesc = infodict['desc']
                else:
                    bookdesc = 'No Description'
                if not bookgenre:
                    if infodict and infodict['genre']:
                        bookgenre = genreFilter(infodict['genre'])
                    else:
                        bookgenre = 'Unknown'

            threadname = threading.currentThread().getName()
            reason = "[%s] %s" % (threadname, reason)
            controlValueDict = {"BookID": bookid}
            newValueDict = {
                "AuthorID": authorid,
                "BookName": title,
                "BookSub": '',
                "BookDesc": bookdesc,
                "BookIsbn": bookisbn,
                "BookPub": bookpub,
                "BookGenre": bookgenre,
                "BookImg": cover,
                "BookLink": booklink,
                "BookRate": bookrate,
                "BookPages": bookpages,
                "BookDate": bookdate,
                "BookLang": lang,
                "Status": bookstatus,
                "AudioStatus": audiostatus,
                "BookAdded": today(),
                "WorkID": workid,
                "ScanResult": reason,
                "OriginalPubDate": originalpubdate
            }

            myDB.upsert("books", newValueDict, controlValueDict)
            logger.info("%s by %s added to the books database, %s/%s" % (title, authorname, bookstatus, audiostatus))

            if 'nocover' in cover or 'nophoto' in cover:
                cover = get_cover(bookid, title)
            if cover and cover.startswith('http'):
                cache_cover(bookid, cover)


def get_cover(bookid, title):
    workcover, source = getBookCover(bookid)
    if workcover:
        myDB = database.DBConnection()
        logger.debug('Updated cover for %s using %s' % (title, source))
        controlValueDict = {"BookID": bookid}
        newValueDict = {"BookImg": workcover}
        myDB.upsert("books", newValueDict, controlValueDict)
    return workcover


def cache_cover(bookid, cover):
    link, success, _ = cache_img("book", bookid, cover)
    if success:
        myDB = database.DBConnection()
        controlValueDict = {"BookID": bookid}
        newValueDict = {"BookImg": link}
        myDB.upsert("books", newValueDict, controlValueDict)
    else:
        logger.debug('Failed to cache image for %s (%s)' % (cover, link))





