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
import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.cache import json_request, html_request, cache_img
from lazylibrarian.formatter import check_float, check_int, now, is_valid_isbn, make_unicode, format_author_name, \
    get_list, make_utf8bytes, plural, unaccented, replace_all, check_year, today, date_format, thread_name
from lazylibrarian.bookwork import librarything_wait, isbn_from_words, get_gb_info, genre_filter, get_status, \
    thinglang

import html5lib
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
from lazylibrarian.images import get_book_cover
from lib.thefuzz import fuzz


class OpenLibrary:
    # https://openlibrary.org/developers/api
    def __init__(self, name=''):
        self.OL_SEARCH = '/'.join([lazylibrarian.CONFIG['OL_URL'], "search.json?"])
        self.OL_AUTHOR = '/'.join([lazylibrarian.CONFIG['OL_URL'], "authors/"])
        self.OL_ISBN = '/'.join([lazylibrarian.CONFIG['OL_URL'], "isbn/"])
        self.OL_WORK = '/'.join([lazylibrarian.CONFIG['OL_URL'], "works/"])
        self.OL_BOOK = '/'.join([lazylibrarian.CONFIG['OL_URL'], "books/"])
        self.LT_NSERIES = '/'.join([lazylibrarian.CONFIG['LT_URL'], 'nseries/'])
        self.LT_SERIES = '/'.join([lazylibrarian.CONFIG['LT_URL'], 'series/'])
        self.LT_WORK = '/'.join([lazylibrarian.CONFIG['LT_URL'], "work/"])
        self.name = make_unicode(name)
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
            loop_count = 1

            if ' <ll> ' in searchterm:  # special token separates title from author
                searchtitle, searchauthorname = searchterm.split(' <ll> ')
                searchterm = searchterm.replace(' <ll> ', ' ')
                searchtitle = searchtitle.split(' (')[0]  # without any series info

            logger.debug('Now searching OpenLibrary API with searchterm: %s' % searchterm)
            searchbytes, _ = make_utf8bytes(searchterm)
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
                    logger.debug("Found %s results for searchterm, page %s" % (results['numFound'], loop_count - 1))
                else:
                    break

                for book in results['docs']:
                    author_name = book.get('author_name')
                    if author_name:
                        author_name = author_name[0]
                    booklink = book.get('key')
                    bookid = ''
                    if booklink:
                        bookid = booklink.split('/')[-1]
                        booklink = '/'.join([lazylibrarian.CONFIG['OL_URL'], booklink])
                    authorid = book.get('author_key')
                    if authorid:
                        authorid = authorid[0]
                    book_title = book.get('title')
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
                        author_fuzz = fuzz.token_set_ratio(author_name, searchauthorname)
                    else:
                        author_fuzz = fuzz.token_set_ratio(author_name, searchterm)
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

                    if bookid and authorid:
                        resultlist.append({
                            'authorname': author_name,
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

                loop_count += 1
                if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < loop_count:
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
        logger.debug("Getting author id for %s, refresh=%s" % (authorname, refresh))
        title = ''
        if '<ll>' in authorname:
            authorname, title = authorname.split('<ll>')
        authorname = format_author_name(authorname)
        if title:
            authorbooks, in_cache = json_request(self.OL_SEARCH + "author=" + quote_plus(authorname) +
                                                 "&title=" + quote_plus(title), use_cache=not refresh)
        else:
            authorbooks, in_cache = json_request(self.OL_SEARCH + "author=" + quote_plus(authorname),
                                                 use_cache=not refresh)

        if authorbooks and authorbooks["docs"]:
            for book in authorbooks['docs']:
                author_name = format_author_name(book.get('author_name')[0])
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
                                                 use_cache=not refresh)
            if not authorbooks or not authorbooks["docs"]:
                logger.debug("No books found for %s" % authorname)
                return {}
            for book in authorbooks['docs']:
                author_name = format_author_name(book.get('author_name')[0])
                if fuzz.token_set_ratio(author_name, authorname) >= lazylibrarian.CONFIG['NAME_RATIO']:
                    key = book.get('author_key')[0]
                    if key:
                        key = key.split('/')[-1]
                    res = self.get_author_info(key, refresh=refresh)
                    if res and res['authorname'] != authorname:
                        res['aka'] = authorname
                    return res
        return {}

    def get_author_info(self, authorid=None, refresh=False):
        logger.debug("Getting author info for %s, refresh=%s" % (authorid, refresh))
        authorinfo, in_cache = json_request(self.OL_AUTHOR + authorid + '.json', use_cache=not refresh)
        if not authorinfo:
            logger.debug("No info found for %s" % authorid)
            return {}

        try:
            if authorinfo['type']['key'] == '/type/redirect':
                newauthorid = authorinfo['location'].rsplit('/', 1)[1]
                logger.debug("Authorid %s redirected to %s" % (authorid, newauthorid))
                authorid = newauthorid
                authorinfo, in_cache = json_request(self.OL_AUTHOR + authorid + '.json', use_cache=not refresh)
                if not authorinfo:
                    logger.debug("No info found for redirect %s" % authorid)
                    return {}
        except (IndexError, KeyError):
            pass

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
            postfix = get_list(lazylibrarian.CONFIG['NAME_POSTFIX'])
            words = author_name.split(',')
            if len(words) == 2:
                if words[0].strip().strip('.').lower in postfix:
                    author_name = words[1].strip() + ' ' + words[0].strip()

        if not author_name:
            logger.warn("Rejecting authorid %s, no authorname" % authorid)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_matching:
                logger.debug(str(authorinfo))
            return {}

        logger.debug("[%s] Processing info for authorID: %s" % (author_name, authorid))
        author_dict = {
            'authorid': authorid,
            'authorlink': author_link,
            'authorimg': author_img,
            'authorborn': author_born,
            'authordeath': author_died,
            'about': about,
            'totalbooks': '0',
            'authorname': format_author_name(author_name)
        }
        return author_dict

    def get_series_members(self, series_id, series_name=''):
        if not self.lt_cache:
            librarything_wait()

        data, self.lt_cache = html_request(self.LT_NSERIES + str(series_id))
        results = []
        if data:
            try:
                try:
                    seriesname = data.split(b'Series: ')[1].split(b'</')[0].decode('utf-8')
                except IndexError:
                    try:
                        seriesname = data.split(b'<h1')[1].split(b'</h1>')[0].split(b'>')[-1].decode('utf-8')
                    except IndexError:
                        seriesname = ''
                if seriesname:
                    match = fuzz.partial_ratio(seriesname, series_name)
                    if match < 95:
                        logger.debug("Series name mismatch for %s, %s%% %s/%s" %
                                     (series_id, match, seriesname, series_name))
                    else:
                        table = data.split(b'<table>')[1].split(b'</table>')[0]
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
                        logger.debug("Found %s for nseries %s" % (len(results), series_id))

            except IndexError:
                if b'<table>' in data:  # error parsing, or just no series data available?
                    logger.debug('Error parsing series table for %s' % series_id)
                else:
                    logger.debug("SeriesID %s not found at librarything" % series_id)
            finally:
                if results:
                    return results

        db = database.DBConnection()
        res = db.match("SELECT SeriesName from series where SeriesID=?", (series_id,))
        if res:
            series_name = res['SeriesName']
            data, self.lt_cache = html_request(self.LT_SERIES + series_name)
            if data:
                try:
                    try:
                        res = data.split(b'Works (')[1].split(b')')[0]
                        works = check_int(res, 0)
                    except IndexError:
                        works = 0
                    logger.debug("Found %s for %s" % (works, series_name))
                    if works:
                        try:
                            seriesid = data.split(b'/nseries/')[1].split(b'/')[0].decode('utf-8')
                        except IndexError:
                            seriesid = ''
                        if seriesid and seriesid != series_id:
                            logger.debug("SeriesID mismatch %s/%s for %s" % (seriesid, series_id, series_name))
                        table = data.split(b'class="worksinseries"')[1].split(b'</table>')[0]
                        rows = table.split(b'<tr')
                        for row in rows:
                            row = row.decode('utf-8')
                            if 'href=' in row:
                                booklink = row.split('href="')[1]
                                bookname = booklink.split('">')[1].split('<')[0]
                                booklink = booklink.split('"')[0]
                                workid = booklink.split('/')[-1]
                                try:
                                    authorlink = row.split('href="')[2]
                                    authorname = authorlink.split('">')[1].split('<')[0]
                                    authorlink = authorlink.split('">')[0]
                                    order = row.split('class="order">')[1].split('<')[0]
                                    results.append([order, bookname, authorname, authorlink, workid])
                                except IndexError:
                                    logger.debug('Incomplete data in series table for series %s' % series_id)
                except IndexError:
                    if b'class="worksinseries"' in data:  # error parsing, or just no series data available?
                        logger.debug('Error in series table for series %s' % series_id)
        else:
            logger.debug("SeriesID %s not found in database")
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
        db = database.DBConnection()
        offset = 0
        next_page = True
        entryreason = reason
        removed_results = 0
        duplicates = 0
        bad_lang = 0
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
        api_hits = 0
        gr_lang_hits = 0
        lt_lang_hits = 0
        gb_lang_change = 0
        auth_start = time.time()
        series_updates = []
        cache_hits = 0

        # these are reject reasons we might want to override, so optionally add to database as "ignored"
        ignorable = ['future', 'date', 'isbn', 'word', 'set']
        if lazylibrarian.CONFIG['NO_LANG']:
            ignorable.append('lang')

        ol_id = ''
        match = db.match('SELECT ol_id FROM authors where authorid=?', (authorid,))
        if match:
            ol_id = match['ol_id']
        if not ol_id:
            ol_id = authorid

        # Artist is loading
        control_value_dict = {"AuthorID": authorid}
        new_value_dict = {"Status": "Loading"}
        db.upsert("authors", new_value_dict, control_value_dict)

        while next_page:
            loop_count += 1
            url = self.OL_SEARCH + "author=" + ol_id
            if offset:
                url += "&offset=%s" % offset
            authorbooks, in_cache = json_request(url, use_cache=not refresh)
            api_hits += not in_cache
            cache_hits += in_cache
            if not authorbooks or not authorbooks["docs"]:
                logger.debug("No books found for key %s" % ol_id)
                next_page = False
                docs = []
            else:
                docs = authorbooks.get('docs', [])
            hit = 0
            miss = 0
            for book in docs:
                lt_id = book.get('id_librarything')
                if lt_id and lt_id[0]:
                    hit += 1
                else:
                    miss += 1
            logger.debug("%s books on page, %s with LT_ID, %s without" % (hit + miss, hit, miss))
            total_count += hit + miss
            for book in docs:
                book_status = bookstatus
                audio_status = audiostatus
                auth_name = book.get('author_name')[0]
                auth_id = book.get('author_key')[0]
                title = book.get('title')
                cover = book.get('cover_i')
                isbns = book.get('isbn')
                link = book.get('key')
                bookpages = 0
                bookdesc = ''
                if title.endswith(')') and '(' not in title:  # openlibrary oddity, "book title )"
                    title = title.rstrip(')').strip()
                key = book.get('key').split('/')[-1]
                first_publish_year = book.get('first_publish_year')
                auth_key = book.get('author_key')[0]
                languages = book.get('language')
                publish_date = book.get('publish_date', '')
                publishers = book.get('publisher')
                id_librarything = book.get('id_librarything')
                if id_librarything:
                    id_librarything = id_librarything[0]
                if publish_date:
                    publish_date = date_format(publish_date[0])
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
                wantedlanguages = get_list(lazylibrarian.CONFIG['IMP_PREFLANG'])
                if wantedlanguages and 'All' not in wantedlanguages:
                    if languages:
                        for item in languages:
                            if item in wantedlanguages:
                                lang = item
                                break
                        if not lang:
                            rejected = 'lang', 'Invalid language: %s' % str(languages)
                            bad_lang += 1
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
                            match = db.match('SELECT lang FROM languages where isbn=?', (isbnhead,))
                            if match:
                                lang = match['lang']
                                cache_hits += 1
                                logger.debug("Found cached language [%s] for %s [%s]" %
                                             (lang, title, isbnhead))
                            else:
                                lang = thinglang(isbn)
                                lt_lang_hits += 1
                                if lang:
                                    db.action('insert into languages values (?, ?)', (isbnhead, lang))

                        if lang and lang not in wantedlanguages:
                            rejected = 'lang', 'Invalid language: %s' % lang
                            bad_lang += 1

                        if not lang:
                            if "Unknown" not in wantedlanguages:
                                rejected = 'lang', 'No language'
                                bad_lang += 1
                            else:
                                lang = "Unknown"

                if not rejected and not title:
                    rejected = 'name', 'No title'

                cmd = 'SELECT BookID,LT_WorkID FROM books,authors WHERE books.AuthorID = authors.AuthorID'
                cmd += ' and BookName=? COLLATE NOCASE and AuthorName=? COLLATE NOCASE'
                cmd += ' and books.Status != "Ignored" and AudioStatus != "Ignored"'
                exists = db.match(cmd, (title, auth_name))
                if not exists:
                    if auth_id != authorid:
                        rejected = 'name', 'Different author (%s/%s/%s)' % (authorid, auth_id, auth_name)
                    else:
                        in_db = lazylibrarian.librarysync.find_book_in_db(auth_name, title,
                                                                          ignored=False, library='eBook',
                                                                          reason='ol_get_author_books %s,%s' %
                                                                          (authorid, title))
                        if in_db and in_db[0]:
                            cmd = 'SELECT BookID,LT_WorkID FROM books WHERE BookID=?'
                            exists = db.match(cmd, (in_db[0],))

                if exists and id_librarything and not exists['LT_WorkID']:
                    db.action("UPDATE books SET LT_WorkID=? WHERE BookID=?",
                              (id_librarything, exists['BookID']))
                if exists and not rejected:
                    # existing bookid might not still be listed at openlibrary so won't refresh.
                    # should we keep new bookid or existing one?
                    # existing one might have been user edited, might be locked,
                    # might have been merged from another authorid or inherited from goodreads?
                    # Should probably use the one with the "best" info but since we don't know
                    # which that is, keep the old one which is already linked to other db tables
                    # but allow info (dates etc) to be updated
                    if key != exists['BookID']:
                        logger.debug('Rejecting bookid %s for [%s][%s] already got %s' %
                                     (key, auth_name, title, exists['BookID']))
                        duplicates += 1
                        rejected = 'name', 'Duplicate id (%s/%s)' % (key, exists['BookID'])
                if not rejected:
                    exists = db.match("SELECT * from books WHERE LT_WorkID=? and BookName !=? COLLATE NOCASE",
                                      (id_librarything, title))
                    if exists:
                        rejected = 'name', 'Duplicate LT_ID (%s/%s)' % (title, exists['BookName'])
                if not rejected and publishers:
                    for bookpub in publishers:
                        if bookpub.lower() in get_list(lazylibrarian.CONFIG['REJECT_PUBLISHER']):
                            rejected = 'publisher', bookpub
                            break

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

                if not rejected and isbnhead and lazylibrarian.CONFIG['NO_ISBN']:
                    rejected = 'isbn', 'No ISBN'

                if not rejected:
                    dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '',
                           '[': ' ', ']': ' ', '#': '# ', ':': ' ', ';': ' '}
                    name = replace_all(title, dic).strip()
                    name = name.lower()
                    # remove extra spaces if they're in a row
                    name = " ".join(name.split())
                    namewords = name.split(' ')
                    badwords = get_list(lazylibrarian.CONFIG['REJECT_WORDS'], ',')
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
                        if re.search(r'\d+ of \d+', bookname) or \
                                re.search(r'\d+/\d+', bookname) and not re.search(r'\d+/\d+/\d+', bookname):
                            rejected = 'set', 'Set or Part'
                        elif re.search(r'\w+\s*/\s*\w+', bookname):
                            rejected = 'set', 'Set or Part'
                        if rejected:
                            logger.debug('Rejected %s, %s' % (bookname, rejected[1]))

                if rejected and rejected[0] not in ignorable:
                    logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                elif rejected and not (rejected[0] in ignorable and lazylibrarian.CONFIG['IMP_IGNORE']):
                    logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                else:
                    logger.debug("Found title: %s LT:%s" % (title, id_librarything))
                    if not rejected and lazylibrarian.CONFIG['NO_FUTURE']:
                        if publish_date > today()[:len(publish_date)]:
                            rejected = 'future', 'Future publication date [%s]' % publish_date
                            if ignorable is None:
                                logger.debug('Rejecting %s, %s' % (title, rejected[1]))
                            else:
                                logger.debug("Not rejecting %s (future pub date %s) as %s" %
                                             (title, publish_date, ignorable))

                    if not rejected and lazylibrarian.CONFIG['NO_PUBDATE']:
                        if not publish_date or publish_date == '0000':
                            rejected = 'date', 'No publication date'
                            if ignorable is None:
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
                            continue  # next book in docs
                    else:
                        if 'author_update' in entryreason:
                            reason = 'Author: %s' % auth_name
                        else:
                            reason = entryreason

                    if not cover:
                        cover = 'images/nocover.png'
                    else:
                        cover = 'http://covers.openlibrary.org/b/id/%s-M.jpg' % cover
                    rating = 0
                    # If we have a librarything ID we can look up series info as openlibrary doesn't
                    # include any. Sadly librarything have disabled whatwork and thingtitle apis
                    # so we can't look up missing IDs and their web pages are not scrapable for the info
                    if id_librarything:
                        rating, genrelist, serieslist = self.lt_workinfo(id_librarything)
                        if rating >= 0:
                            genrenames = []
                            for item in genrelist:
                                genrenames.append(genre_filter(item[0]))
                            genres = ', '.join(set(genrenames))
                            update_value_dict = {}
                            exists = db.match("SELECT * from books WHERE BookID=?", (key,))
                            if not exists:
                                exists = db.match("SELECT * from books WHERE LT_Workid=?", (id_librarything,))
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
                                if 'Invalid language: ' in reason:
                                    try:
                                        lang = reason.split('Invalid language: ')[1].split("'")[1]
                                    except IndexError:
                                        pass
                                infodict = get_gb_info(isbn=isbn, author=auth_name, title=title, expire=False)
                                if infodict:
                                    gbupdate = []
                                    if infodict['desc']:
                                        bookdesc = infodict['desc']
                                        gbupdate.append("Description")
                                    else:
                                        bookdesc = 'No Description'
                                    if not genres and infodict['genre']:
                                        genres = genre_filter(infodict['genre'])
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
                                    if infodict['lang'] and not lang:
                                        lang = infodict['lang']
                                        gbupdate.append('Language')
                                    if gbupdate:
                                        logger.debug("Updated %s from googlebooks" % ', '.join(gbupdate))
                                        gb_lang_change += 1

                                reason = "[%s] %s" % (thread_name(), reason)
                                if not lang:
                                    lang = 'Unknown'
                                if isinstance(publishers, list):
                                    publishers = ', '.join(publishers)
                                db.action('INSERT INTO books (AuthorID, BookName, BookDesc, BookGenre, ' +
                                          'BookIsbn, BookPub, BookRate, BookImg, BookLink, BookID, BookDate, ' +
                                          'BookLang, BookAdded, Status, WorkPage, AudioStatus, LT_WorkID, ' +
                                          'ScanResult, OriginalPubDate, BookPages) ' +
                                          'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                          (authorid, title, bookdesc, genres, isbn, publishers, bookrate, cover,
                                           link, key, bookdate, lang, now(), book_status, '', audio_status,
                                           id_librarything, reason, first_publish_year, bookpages))

                                if 'nocover' in cover or 'nophoto' in cover:
                                    cover = get_cover(key, title)
                                    cover_count += 1
                                if cover and cover.startswith('http'):
                                    cache_cover(key, cover)
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

                                    update_value_dict["ScanResult"] = "bookdate %s is now valid" % publish_date
                                elif not exists:
                                    update_value_dict["ScanResult"] = reason

                                if "ScanResult" in update_value_dict:
                                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                        logger.debug("entry status %s %s,%s" % (entrystatus, bookstatus, audiostatus))
                                    book_status, audio_status = get_status(key, serieslist, bookstatus,
                                                                           audiostatus, entrystatus)
                                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                                        logger.debug("status is now %s,%s" % (book_status, audio_status))
                                    update_value_dict["Status"] = book_status
                                    update_value_dict["AudioStatus"] = audio_status

                            if update_value_dict:
                                control_value_dict = {"LT_WorkID": id_librarything}
                                db.upsert("books", update_value_dict, control_value_dict)

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
                                    seriesid = series[2]
                                    exists = db.match("SELECT * from series WHERE seriesid=?", (seriesid,))
                                    if not exists:
                                        exists = db.match("SELECT * from series WHERE seriesname=?", (series[0],))
                                        if exists:
                                            db.action('PRAGMA foreign_keys = OFF')
                                            for table in ['series', 'member', 'seriesauthors']:
                                                cmd = "UPDATE " + table + " SET SeriesID=? WHERE SeriesID=?"
                                                db.action(cmd, (seriesid, exists['SeriesID']))
                                            db.action('PRAGMA foreign_keys = ON')
                                            db.commit()
                                    if not exists:
                                        logger.debug("New series: %s" % series[0])
                                        db.action('INSERT INTO series (SeriesID, SeriesName, Status, Updated,' +
                                                  ' Reason) VALUES (?,?,?,?,?)',
                                                  (seriesid, series[0], 'Paused', time.time(), id_librarything))
                                        db.commit()
                                        exists = {'Status': 'Paused'}
                                    seriesmembers = None
                                    cmd = "SELECT * from member WHERE seriesid=? AND WorkID=?"
                                    if not db.match(cmd, (seriesid, id_librarything)):
                                        seriesmembers = [[series[1], title, auth_name, auth_key, id_librarything]]
                                        if seriesid in series_updates:
                                            logger.debug("Series %s already updated" % seriesid)
                                        elif exists['Status'] in ['Paused', 'Ignored']:
                                            logger.debug("Not getting additional series members for %s, status is %s" %
                                                         (series[0], exists['Status']))
                                        else:
                                            seriesmembers = self.get_series_members(seriesid, series[0])
                                            series_updates.append(seriesid)
                                            if not seriesmembers:
                                                logger.warn("Series %s (%s) has no members at librarything" % (
                                                            series[0], seriesid))
                                    if seriesmembers:
                                        if len(seriesmembers) == 1:
                                            logger.debug("Found member %s for series %s" % (series[1], series[0]))
                                        else:
                                            logger.debug("Found %s members for series %s" % (len(seriesmembers),
                                                                                             series[0]))
                                        for member in seriesmembers:
                                            # member[order, bookname, authorname, authorlink, workid]
                                            # remove any old entries for this series member
                                            db.action("DELETE from member WHERE seriesid=? AND seriesnum=?",
                                                      (seriesid, member[0]))
                                            auth_name, exists = lazylibrarian.importer.get_preferred_author_name(
                                                member[2])
                                            if not exists:
                                                reason = "Series author %s:%s" % (series[0], member[1])
                                                lazylibrarian.importer.add_author_name_to_db(author=member[2],
                                                                                             refresh=False,
                                                                                             addbooks=False,
                                                                                             reason=reason
                                                                                             )
                                                auth_name, exists = \
                                                    lazylibrarian.importer.get_preferred_author_name(member[2])
                                                if exists:
                                                    auth_name = member[2]
                                                else:
                                                    logger.debug("Unable to add %s for %s, author not in database" %
                                                                 (member[2], member[1]))
                                                    continue
                                            else:
                                                cmd = "SELECT * from authors WHERE authorname=?"
                                                exists = db.match(cmd, (auth_name,))
                                                if exists:
                                                    auth_key = exists['AuthorID']
                                                    if fuzz.ratio(auth_name.lower().replace('.', ''),
                                                                  member[2].lower().replace('.', '')) < 95:
                                                        akas = get_list(exists['AKA'], ',')
                                                        if member[2] not in akas:
                                                            akas.append(member[2])
                                                            db.action("UPDATE authors SET AKA=? WHERE AuthorID=?",
                                                                      (', '.join(akas), auth_key))
                                                    match = db.match('SELECT * from seriesauthors WHERE ' +
                                                                     'SeriesID=? and AuthorID=?',
                                                                     (seriesid, auth_key))
                                                    if not match:
                                                        logger.debug("Adding %s as series author for %s" %
                                                                     (auth_name, series[0]))
                                                        db.action('INSERT INTO seriesauthors ("SeriesID", ' +
                                                                  '"AuthorID") VALUES (?, ?)',
                                                                  (seriesid, auth_key), suppress='UNIQUE')

                                            # if book not in library, use librarything workid to get an isbn
                                            # use that to get openlibrary workid
                                            # add book to library, then add seriesmember
                                            exists = db.match("SELECT * from books WHERE LT_Workid=?",
                                                              (member[4],))
                                            if exists:
                                                match = db.match("SELECT * from member WHERE " +
                                                                 "SeriesID=? AND BookID=?",
                                                                 (seriesid, exists['BookID']))
                                                if not match:
                                                    logger.debug("Inserting new member [%s] for %s" %
                                                                 (member[0], series[0]))
                                                    db.action('INSERT INTO member (SeriesID, BookID, ' +
                                                              'WorkID, SeriesNum) VALUES (?,?,?,?)',
                                                              (seriesid, exists['BookID'], member[4], member[0]))
                                                ser = db.match('select count(*) as counter from member ' +
                                                               'where seriesid=?', (seriesid,))
                                                if ser:
                                                    counter = check_int(ser['counter'], 0)
                                                    db.action("UPDATE series SET Total=? WHERE SeriesID=?",
                                                              (counter, seriesid))
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
                                                        cache_hits += in_cache
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
                                                        cache_hits += in_cache
                                                        if workinfo and 'title' in workinfo:
                                                            title = workinfo.get('title')
                                                            covers = workinfo.get('covers')
                                                            if covers:
                                                                cover = 'http://covers.openlibrary.org/b/id/'
                                                                cover += '%s-M.jpg' % covers[0]
                                                            else:
                                                                cover = 'images/nocover.png'
                                                            publish_date = date_format(workinfo.get('publish_date', ''))
                                                            rating, genrelist, _ = self.lt_workinfo(member[4])
                                                            genrenames = []
                                                            for item in genrelist:
                                                                genrenames.append(item[0])
                                                            genres = ', '.join(genrenames)
                                                            lang = ''
                                                            match = db.match('SELECT * from authors ' +
                                                                             'WHERE AuthorName=? COLLATE NOCASE',
                                                                             (auth_name,))
                                                            if match:
                                                                bauth_key = match['AuthorID']
                                                            else:
                                                                reason = "Series author %s:%s" % (series[0], member[1])
                                                                lazylibrarian.importer.add_author_name_to_db(
                                                                    author=auth_name, refresh=False,
                                                                    addbooks=False, reason=reason)
                                                                match = db.match('SELECT * from authors ' +
                                                                                 'WHERE AuthorName=? COLLATE NOCASE',
                                                                                 (auth_name,))
                                                                if match:
                                                                    bauth_key = match['AuthorID']
                                                                else:
                                                                    msg = "Unable to add %s for %s" % (auth_name, title)
                                                                    msg += ", author not in database"
                                                                    logger.debug(msg)
                                                                    continue

                                                            match = db.match('SELECT * from books ' +
                                                                             'WHERE BookID=?', (workid,))
                                                            if not match:
                                                                logger.debug("Inserting new member [%s] for %s" %
                                                                             (member[0], series[0]))

                                                                reason = "Member %s of series %s" % (member[0],
                                                                                                     series[0])
                                                                reason = "[%s] %s" % (thread_name(), reason)
                                                                added_count += 1
                                                                if not lang:
                                                                    lang = 'Unknown'
                                                                db.action('INSERT INTO books (AuthorID, ' +
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
                                                                           publish_date, lang, '', bookstatus,
                                                                           '', audiostatus, member[4],
                                                                           reason, publish_date))

                                                                if 'nocover' in cover or 'nophoto' in cover:
                                                                    cover = get_cover(workid, title)
                                                                    cover_count += 1
                                                                if cover and cover.startswith('http'):
                                                                    cache_cover(workid, cover)

                                                            match = db.match("SELECT * from seriesauthors WHERE " +
                                                                             "SeriesID=? AND AuthorID=?",
                                                                             (seriesid, bauth_key))
                                                            if not match:
                                                                logger.debug("Adding %s as series author for %s" %
                                                                             (auth_name, series[0]))
                                                                db.action('INSERT INTO seriesauthors ("SeriesID", ' +
                                                                          '"AuthorID") VALUES (?, ?)',
                                                                          (seriesid, bauth_key), suppress='UNIQUE')

                                                            match = db.match("SELECT * from member WHERE " +
                                                                             "SeriesID=? AND BookID=?",
                                                                             (seriesid, workid))
                                                            if not match:
                                                                db.action('INSERT INTO member ' +
                                                                          '(SeriesID, BookID, WorkID, SeriesNum)  ' +
                                                                          'VALUES (?,?,?,?)',
                                                                          (seriesid, workid, member[4], member[0]))
                                                                ser = db.match('select count(*) as counter ' +
                                                                               'from member where seriesid=?',
                                                                               (seriesid,))
                                                                if ser:
                                                                    counter = check_int(ser['counter'], 0)
                                                                    db.action("UPDATE series SET Total=? " +
                                                                              "WHERE SeriesID=?",
                                                                              (counter, seriesid))
                    if rating == 0:
                        logger.debug("No additional librarything info")
                        exists = db.match("SELECT * from books WHERE BookID=?", (key,))
                        if not exists:
                            logger.debug("Inserting new book for %s" % authorid)
                            if 'author_update' in entryreason:
                                reason = 'Author: %s' % auth_name
                            else:
                                reason = entryreason
                            reason = "[%s] %s" % (thread_name(), reason)
                            added_count += 1
                            if not lang:
                                lang = 'Unknown'
                            db.action('INSERT INTO books (AuthorID, BookName, BookImg, ' +
                                      'BookLink, BookID, BookDate, BookLang, BookAdded, Status, ' +
                                      'WorkPage, AudioStatus, ScanResult, OriginalPubDate) ' +
                                      'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                      (authorid, title, cover, link, key, publish_date,
                                       lang, now(), book_status, '', audio_status, reason, first_publish_year))
                            if 'nocover' in cover or 'nophoto' in cover:
                                cover = get_cover(key, title)
                                cover_count += 1
                            if cover and cover.startswith('http'):
                                cache_cover(key, cover)

                    added_count += 1

            if authorbooks and authorbooks.get("docs"):
                offset += len(authorbooks['docs'])
                if offset >= check_int(authorbooks["numFound"], 0):
                    next_page = False
            else:
                next_page = False

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
        logger.debug("Found %s %s in %s %s" % (total_count, plural(total_count, "result"),
                                               loop_count, plural(loop_count, "page")))
        logger.debug("Found %s locked %s" % (locked_count, plural(locked_count, "book")))
        logger.debug("Removed %s unwanted language %s" % (bad_lang, plural(bad_lang, "result")))
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
                          "bad_lang": bad_lang,
                          "bad_char": removed_results,
                          "uncached": api_hits,
                          "duplicates": duplicates
                          }
        db.upsert("stats", new_value_dict, control_value_dict)

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='ol.find_book'):
        logger.debug("bookstatus=%s, audiostatus=%s" % (bookstatus, audiostatus))
        db = database.DBConnection()
        url = self.OL_WORK + bookid + '.json'
        try:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                logger.debug(url)
            workinfo, in_cache = json_request(url)
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
            publish_date = date_format(workinfo.get('publish_date', ''))
            lang = "Unknown"
            #
            # user has said they want this book, don't block for unwanted language etc
            # Ignore book if adding as part of a series, else just warn and include it
            #
            valid_langs = get_list(lazylibrarian.CONFIG['IMP_PREFLANG'])
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
            match = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            if match:
                authorname = match['AuthorName']
            else:
                # ol does not give us authorname in work page
                auth_id = lazylibrarian.importer.add_author_to_db(authorid=authorid, refresh=False,
                                                                  addbooks=False,
                                                                  reason="ol.find_book %s" % bookid)
                # authorid may have changed on importing
                if authorid != auth_id and auth_id.startswith('OL'):
                    authorid = auth_id
                match = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
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
            if infodict:
                if infodict.get('desc'):
                    bookdesc = infodict['desc']
                else:
                    bookdesc = 'No Description'
                if not bookgenre and infodict.get('genre'):
                    bookgenre = genre_filter(infodict['genre'])
                else:
                    bookgenre = 'Unknown'

            reason = "[%s] %s" % (thread_name(), reason)
            control_value_dict = {"BookID": bookid}
            new_value_dict = {
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

            db.upsert("books", new_value_dict, control_value_dict)
            logger.info("%s by %s added to the books database, %s/%s" % (title, authorname, bookstatus, audiostatus))

            if 'nocover' in cover or 'nophoto' in cover:
                cover = get_cover(bookid, title)
            if cover and cover.startswith('http'):
                cache_cover(bookid, cover)


def get_cover(bookid, title):
    workcover, source = get_book_cover(bookid)
    if workcover:
        db = database.DBConnection()
        logger.debug('Updated cover for %s using %s' % (title, source))
        control_value_dict = {"BookID": bookid}
        new_value_dict = {"BookImg": workcover}
        db.upsert("books", new_value_dict, control_value_dict)
    return workcover


def cache_cover(bookid, cover):
    link, success, _ = cache_img("book", bookid, cover)
    if success:
        db = database.DBConnection()
        control_value_dict = {"BookID": bookid}
        new_value_dict = {"BookImg": link}
        db.upsert("books", new_value_dict, control_value_dict)
    else:
        logger.debug('Failed to cache image for %s (%s)' % (cover, link))
