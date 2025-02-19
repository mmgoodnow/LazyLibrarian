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


# example
# https://www.googleapis.com/books/v1/volumes?q=+inauthor:george+martin+intitle:song+ice+fire

import logging
import re
import time
import traceback
from urllib.parse import quote, quote_plus, urlencode

from rapidfuzz import fuzz

import lazylibrarian
from lazylibrarian import database
from lazylibrarian.bookwork import get_work_series, get_work_page, delete_empty_series, \
    set_series, get_status, google_book_dict, isbnlang
from lazylibrarian.cache import json_request
from lazylibrarian.config2 import CONFIG
from lazylibrarian.formatter import plural, today, replace_all, unaccented, is_valid_isbn, \
    get_list, clean_name, make_unicode, make_utf8bytes, replace_quotes_with, check_year, thread_name
from lazylibrarian.hc import HardCover
from lazylibrarian.images import cache_bookimg
from lazylibrarian.images import get_book_cover
from lazylibrarian.ol import OpenLibrary


class GoogleBooks:
    def __init__(self, name=None):
        self.name = make_unicode(name)
        self.logger = logging.getLogger(__name__)
        self.url = '/'.join([CONFIG['GB_URL'], 'books/v1/volumes?q='])
        self.params = {
            'maxResults': 40,
            'printType': 'books',
        }

        if CONFIG['GB_API']:
            self.params['key'] = CONFIG['GB_API']

    # noinspection PyBroadException
    def find_results(self, searchterm=None, queue=None):
        """ GoogleBooks performs much better if we search for author OR title
            not both at once, so if searchterm is not isbn, two searches needed.
            Lazylibrarian searches use <ll> to separate title from author in searchterm
            If this token isn't present, it's an isbn or searchterm as supplied by user
        """
        try:
            resultlist = []
            # See if we should check ISBN field, otherwise ignore it
            api_strings = ['inauthor:', 'intitle:']
            if is_valid_isbn(searchterm):
                api_strings = ['isbn:']

            api_hits = 0

            ignored = 0
            total_count = 0
            no_author_count = 0
            title = ''
            authorname = ''

            if ' <ll> ' in searchterm:  # special token separates title from author
                title, authorname = searchterm.split(' <ll> ')

            fullterm = searchterm.replace(' <ll> ', ' ')
            self.logger.debug(f'Now searching Google Books API with searchterm: {fullterm}')

            for api_value in api_strings:
                set_url = self.url
                if api_value == "isbn:":
                    set_url += quote(api_value + searchterm)
                elif api_value == 'intitle:':
                    searchterm = fullterm
                    if title:  # just search for title
                        title = title.split(' (')[0]  # without any series info
                        searchterm = title
                    # strip all ascii and non-ascii quotes/apostrophes
                    searchterm = replace_quotes_with(searchterm, '')
                    set_url += quote(make_utf8bytes(f"{api_value}\"{searchterm}\"")[0])
                elif api_value == 'inauthor:':
                    searchterm = fullterm
                    if authorname:
                        searchterm = authorname  # just search for author
                    searchterm = searchterm.strip()
                    set_url += quote_plus(make_utf8bytes(f"{api_value}\"{searchterm}\"")[0])

                startindex = 0
                resultcount = 0
                ignored = 0
                number_results = 1
                total_count = 0
                no_author_count = 0
                try:
                    while startindex < number_results:

                        self.params['startIndex'] = startindex
                        url = f"{set_url}&{urlencode(self.params)}"

                        try:
                            jsonresults, in_cache = json_request(url)
                            if not jsonresults:
                                number_results = 0
                            else:
                                if not in_cache:
                                    api_hits += 1
                                number_results = jsonresults['totalItems']
                                self.logger.debug(f"Searching url: {url}")
                            if number_results == 0:
                                self.logger.warning(f'Found no results for {api_value} with value: {searchterm}')
                                break
                            else:
                                pass
                        except Exception as err:
                            errmsg = str(err)
                            self.logger.warning(
                                f'Google Books API Error [{errmsg}]: Check your API key or wait a while')
                            break

                        startindex += 40

                        for item in jsonresults['items']:
                            total_count += 1

                            book = google_book_dict(item)
                            if not book['author']:
                                self.logger.debug('Skipped a result without authorfield.')
                                no_author_count += 1
                                continue

                            if not book['name']:
                                self.logger.debug('Skipped a result without title.')
                                continue

                            valid_langs = get_list(CONFIG['IMP_PREFLANG'])
                            if "All" not in valid_langs:  # don't care about languages, accept all
                                try:
                                    # skip if language is not in valid list -
                                    booklang = book['lang']
                                    if not booklang:
                                        booklang = 'Unknown'
                                    if booklang not in valid_langs:
                                        self.logger.debug(
                                            f"Skipped {book['name']} with language {booklang}")
                                        ignored += 1
                                        continue
                                except KeyError:
                                    ignored += 1
                                    self.logger.debug(f"Skipped {book['name']} where no language is found")
                                    continue

                            if authorname:
                                author_fuzz = fuzz.token_sort_ratio(book['author'], authorname)
                            else:
                                author_fuzz = fuzz.token_sort_ratio(book['author'], fullterm)

                            if title:
                                if title.endswith(')'):
                                    title = title.rsplit('(', 1)[0]
                                book_fuzz = fuzz.token_set_ratio(book['name'], title)
                                # lose a point for each extra word in the fuzzy matches so we get the closest match
                                words = len(get_list(book['name']))
                                words -= len(get_list(title))
                                book_fuzz -= abs(words)
                            else:
                                book_fuzz = fuzz.token_set_ratio(book['name'], fullterm)

                            isbn_fuzz = 0
                            if is_valid_isbn(fullterm):
                                isbn_fuzz = 100

                            highest_fuzz = max((author_fuzz + book_fuzz) / 2, isbn_fuzz)

                            dic = {':': '.', '"': '', '\'': ''}
                            bookname = replace_all(book['name'], dic)

                            bookname = unaccented(bookname, only_ascii=False)

                            author_id = ''
                            if book['author']:
                                db = database.DBConnection()
                                try:
                                    match = db.match(
                                        'SELECT AuthorID FROM authors WHERE AuthorName=?', (book['author'],))
                                    if match:
                                        author_id = match['AuthorID']
                                finally:
                                    db.close()

                            resultlist.append({
                                'authorname': book['author'],
                                'authorid': author_id,
                                'bookid': item['id'],
                                'bookname': bookname,
                                'booksub': book['sub'],
                                'bookisbn': book['isbn'],
                                'bookpub': book['pub'],
                                'bookdate': book['date'],
                                'booklang': book['lang'],
                                'booklink': book['link'],
                                'bookrate': float(book['rate']),
                                'bookrate_count': book['rate_count'],
                                'bookimg': book['img'],
                                'bookpages': book['pages'],
                                'bookgenre': book['genre'],
                                'bookdesc': book['desc'],
                                'author_fuzz': author_fuzz,
                                'book_fuzz': book_fuzz,
                                'isbn_fuzz': isbn_fuzz,
                                'highest_fuzz': highest_fuzz,
                                'source': 'GoogleBooks'
                            })

                            resultcount += 1

                except KeyError:
                    break

                self.logger.debug(
                    f"Returning {resultcount} {plural(resultcount, 'result')} for ({api_value}) "
                    f"with keyword: {searchterm}")

            self.logger.debug(f"Found {total_count} {plural(total_count, 'result')}")
            self.logger.debug(f"Removed {ignored} unwanted language {plural(ignored, 'result')}")
            self.logger.debug(f"Removed {no_author_count} {plural(no_author_count, 'book')} with no author")
            self.logger.debug(
                f"The Google Books API was hit {api_hits} {plural(api_hits, 'time')} for searchterm: {fullterm}")
            queue.put(resultlist)

        except Exception:
            self.logger.error(f'Unhandled exception in GB.find_results: {traceback.format_exc()}')

    def get_author_books(self, authorid=None, authorname=None, bookstatus="Skipped",
                         audiostatus="Skipped", entrystatus='Active', refresh=False, reason='gb.get_author_books'):
        # noinspection PyBroadException
        self.logger.debug(f'[{authorname}] Now processing books with Google Books API')
        db = database.DBConnection()
        try:
            # google doesnt like accents in author names
            set_url = self.url + quote(f'inauthor:"{unaccented(authorname, only_ascii=False)}"')
            entryreason = reason
            api_hits = 0
            gr_lang_hits = 0
            lt_lang_hits = 0
            gb_lang_change = 0
            cache_hits = 0
            not_cached = 0
            startindex = 0
            removed_results = 0
            duplicates = 0
            ignored = 0
            added_count = 0
            updated_count = 0
            locked_count = 0
            book_ignore_count = 0
            total_count = 0
            number_results = 1

            valid_langs = get_list(CONFIG['IMP_PREFLANG'])
            # Artist is loading
            db.action("UPDATE authors SET Status='Loading' WHERE AuthorID=?", (authorid,))

            try:
                threadname = thread_name()
                while startindex < number_results:
                    if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                        self.logger.debug(f"Aborting {threadname}")
                        break
                    self.params['startIndex'] = startindex
                    url = f"{set_url}&{urlencode(self.params)}"

                    try:
                        jsonresults, in_cache = json_request(url, use_cache=not refresh)
                        if not jsonresults:
                            number_results = 0
                        else:
                            if not in_cache:
                                api_hits += 1
                            number_results = jsonresults['totalItems']
                    except Exception as err:
                        errmsg = str(err)
                        self.logger.warning(f'Google Books API Error [{errmsg}]: Check your API key or wait a while')
                        break

                    if number_results == 0:
                        self.logger.warning(f'Found no results for {authorname}')
                        break
                    else:
                        self.logger.debug(f"Found {number_results} {plural(number_results, 'result')} for {authorname}")

                    startindex += 40

                    for item in jsonresults['items']:
                        if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                            self.logger.debug(f"Aborting {threadname}")
                            break
                        total_count += 1
                        book = google_book_dict(item)
                        # skip if no author, no author is no book.
                        if not book['author']:
                            self.logger.debug('Skipped a result without authorfield.')
                            continue

                        booklang = book['lang']
                        # do we care about language?
                        if "All" not in valid_langs:
                            if book['isbn']:
                                # seems google lies to us, sometimes tells us books are in english when they are not
                                if booklang == "Unknown" or booklang == "en":
                                    googlelang = booklang
                                    match = False
                                    if book['isbn']:
                                        booklang, cache_hit, thing_hit = isbnlang(book['isbn'])
                                        if thing_hit:
                                            lt_lang_hits += 1
                                        if booklang:
                                            match = True
                                    if match:
                                        # We found a better language match
                                        if googlelang == "en" and booklang not in ["en-US", "en-GB", "eng"]:
                                            # these are all english, may need to expand this list
                                            self.logger.debug(
                                                f"{book['name']} Google thinks [{googlelang}], we think [{booklang}]")
                                            gb_lang_change += 1
                                    else:  # No match anywhere, accept google language
                                        booklang = googlelang

                            if not booklang:
                                booklang = 'Unknown'
                            if booklang not in valid_langs:
                                self.logger.debug(f"Skipped [{book['name']}] with language {booklang}")
                                ignored += 1
                                continue

                        ignorable = ['future', 'date', 'isbn']
                        if CONFIG.get_bool('NO_LANG'):
                            ignorable.append('lang')
                        rejected = None
                        check_status = False
                        existing_book = None
                        bookname = book['name']
                        bookid = item['id']
                        if not bookname:
                            self.logger.debug(f'Rejecting bookid {bookid} for {authorname}, no bookname')
                            rejected = 'name', 'No bookname'
                        else:
                            bookname = replace_all(bookname, {':': ' ', '"': '', '\'': ''}).strip()
                            # if re.match(r'[^\w-]', bookname):  # remove books with bad characters in title
                            # self.logger.debug("[%s] removed book for bad characters" % bookname)
                            # rejected = 'chars', 'Bad characters in bookname'

                        if not rejected and CONFIG.get_bool('NO_FUTURE'):
                            # googlebooks sometimes gives yyyy, sometimes yyyy-mm, sometimes yyyy-mm-dd
                            if book['date'] > today()[:len(book['date'])]:
                                self.logger.debug(f"Rejecting {bookname}, future publication date {book['date']}")
                                rejected = 'future', f"Future publication date [{book['date']}]"

                        if not rejected and CONFIG.get_bool('NO_PUBDATE'):
                            if not book['date']:
                                self.logger.debug(f'Rejecting {bookname}, no publication date')
                                rejected = 'date', 'No publication date'

                        if not rejected and CONFIG.get_bool('NO_ISBN'):
                            if not book['isbn']:
                                self.logger.debug(f'Rejecting {bookname}, no isbn')
                                rejected = 'isbn', 'No ISBN'

                        if not rejected:
                            cmd = ("SELECT BookID,gb_id FROM books,authors WHERE books.AuthorID = authors.AuthorID and "
                                   "BookName=? COLLATE NOCASE and AuthorName=? COLLATE NOCASE and "
                                   "books.Status != 'Ignored' and AudioStatus != 'Ignored'")
                            match = db.match(cmd, (bookname, authorname))
                            if not match:
                                in_db = lazylibrarian.librarysync.find_book_in_db(authorname, bookname, source='gb_id',
                                                                                  ignored=False, library='eBook',
                                                                                  reason='gb_get_author_books')
                                if in_db and in_db[0]:
                                    cmd = "SELECT BookID,gb_id FROM books WHERE BookID=?"
                                    match = db.match(cmd, (in_db[0],))
                            if match:
                                if match['BookID'] != bookid:  # we have a different book with this author/title already
                                    self.logger.debug(f'Rejecting bookid {bookid} for [{authorname}][{bookname}]'
                                                      f' already got {match["BookID"]}')
                                    rejected = 'bookid', f'Got under different bookid {bookid}'
                                    if not match['gb_id']:
                                        db.action("UPDATE books SET gb_id=? WHERE BookID=?", (bookid, match['BookID']))
                                    duplicates += 1

                        cmd = ("SELECT AuthorName,BookName,AudioStatus,books.Status,ScanResult,gb_id,BookID "
                               "FROM books,authors WHERE authors.AuthorID = books.AuthorID AND BookID=?")
                        match = db.match(cmd, (bookid,))
                        if match:  # we have a book with this bookid already
                            if bookname != match['BookName'] or authorname != match['AuthorName']:
                                self.logger.debug(
                                    f"Rejecting bookid {bookid} for [{authorname}][{bookname}] already got "
                                    f"bookid for [{match['AuthorName']}][{match['BookName']}]")
                                if not match['gb_id']:
                                    db.action("UPDATE books SET gb_id=? WHERE BookID=?",
                                              (bookid, match['BookID']))
                                duplicates += 1
                                rejected = 'got', 'Already got this bookid in database'
                            else:
                                msg = (f"Bookid {bookid} for [{authorname}][{bookname}] is in database "
                                       f"marked {match['Status']}")
                                if CONFIG.get_bool('AUDIO_TAB'):
                                    msg += f",{match['AudioStatus']}"
                                msg += f" {match['ScanResult']}"
                                self.logger.debug(msg)
                                check_status = True

                            # Make sure we don't reject books we have got
                            if match['Status'] in ['Open', 'Have'] or match['AudioStatus'] in ['Open', 'Have']:
                                rejected = None

                        if rejected and rejected[0] not in ignorable:
                            removed_results += 1
                        if check_status or rejected is None or (
                                CONFIG.get_bool('IMP_IGNORE') and rejected[0] in ignorable):  # dates, isbn

                            cmd = ("SELECT Status,AudioStatus,BookFile,AudioFile,Manual,BookAdded,BookName,ScanResult "
                                   "FROM books WHERE BookID=?")
                            existing = db.match(cmd, (bookid,))
                            if existing:
                                book_status = existing['Status']
                                audio_status = existing['AudioStatus']
                                if CONFIG['FOUND_STATUS'] == 'Open':
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
                            else:
                                book_status = bookstatus  # new_book status, or new_author status
                                audio_status = audiostatus
                                added = today()
                                locked = False

                            if rejected:
                                if rejected[0] in ignorable:
                                    book_status = 'Ignored'
                                    audio_status = 'Ignored'
                                    book_ignore_count += 1
                                    reason = f"Ignored: {rejected[1]}"
                                else:
                                    reason = f"Rejected: {rejected[1]}"
                            else:
                                if 'author_update' in entryreason:
                                    reason = f'Author: {authorname}'
                                else:
                                    reason = entryreason

                            if locked:
                                locked_count += 1
                            else:
                                reason = f"[{thread_name()}] {reason}"
                                control_value_dict = {"BookID": bookid}
                                new_value_dict = {
                                    "AuthorID": authorid,
                                    "BookName": bookname,
                                    "BookSub": book['sub'],
                                    "BookDesc": book['desc'],
                                    "BookIsbn": book['isbn'],
                                    "BookPub": book['pub'],
                                    "BookGenre": book['genre'],
                                    "BookImg": book['img'],
                                    "BookLink": book['link'],
                                    "BookRate": float(book['rate']),
                                    "BookPages": book['pages'],
                                    "BookDate": book['date'],
                                    "BookLang": booklang,
                                    "Status": book_status,
                                    "AudioStatus": audio_status,
                                    "BookAdded": added,
                                    "WorkID": '',
                                    "ScanResult": reason,
                                    "gb_id": bookid
                                }

                                if 'nocover' in book['img'] or 'nophoto' in book['img']:
                                    # try to get a cover from another source
                                    link, _ = get_book_cover(bookid, ignore='googleapis')
                                    if link:
                                        new_value_dict["BookImg"] = link
                                    elif book['img'] and book['img'].startswith('http'):
                                        link = cache_bookimg(book['img'], bookid, 'gb')
                                        new_value_dict["BookImg"] = link

                                db.upsert("books", new_value_dict, control_value_dict)
                                self.logger.debug(f"Book found: {bookname} {book['date']}")

                                serieslist = []
                                if book['series']:
                                    serieslist = [('', book['seriesNum'], clean_name(book['series'], '&/'))]
                                if CONFIG.get_bool('ADD_SERIES') and "Ignored:" not in reason:
                                    newserieslist = get_work_series(bookid, 'LT', reason=reason)
                                    if newserieslist:
                                        serieslist = newserieslist
                                        self.logger.debug(f'Updated series: {bookid} [{serieslist}]')
                                    set_series(serieslist, bookid, reason=reason)

                                update_value_dict = {}
                                control_value_dict = {"BookID": bookid}
                                if not existing or (existing['ScanResult'] and
                                                    ' publication date' in existing['ScanResult'] and
                                                    book['date'] and book['date'] != '0000' and
                                                    book['date'] <= today()[:len(book['date'])]):
                                    # was rejected on previous scan but bookdate is now valid
                                    book_status, audio_status = get_status(bookid, serieslist, bookstatus, audiostatus,
                                                                           entrystatus)
                                    update_value_dict["Status"] = book_status
                                    update_value_dict["AudioStatus"] = audio_status

                                    if existing:
                                        # was rejected on previous scan but bookdate has become valid
                                        self.logger.debug(
                                            f"valid bookdate [{book['date']}] previous scanresult "
                                            f"[{existing['ScanResult']}]")
                                        update_value_dict["ScanResult"] = f"bookdate {book['date']} is now valid"

                                worklink = get_work_page(bookid)
                                if worklink:
                                    update_value_dict["WorkPage"] = worklink

                                if update_value_dict:
                                    db.upsert("books", update_value_dict, control_value_dict)

                                if not existing_book:
                                    typ = 'Added'
                                    added_count += 1
                                else:
                                    typ = 'Updated'
                                    updated_count += 1
                                msg = f"[{authorname}] {typ} book: {bookname} [{booklang}] status {book_status}"
                                if CONFIG.get_bool('AUDIO_TAB'):
                                    msg += f" audio {audio_status}"
                                self.logger.debug(msg)
            except KeyError:
                pass

            delete_empty_series()
            self.logger.debug(
                f"[{authorname}] The Google Books API was hit {api_hits} {plural(api_hits, 'time')}"
                f" to populate book list")
            cmd = ("SELECT BookName, BookLink, BookDate, BookImg, BookID from books WHERE AuthorID=? AND "
                   "Status != 'Ignored' order by BookDate DESC")
            lastbook = db.match(cmd, (authorid,))

            if lastbook:  # maybe there are no books [remaining] for this author
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
            self.logger.debug(f"Found {total_count} total {plural(total_count, 'book')} for author")
            self.logger.debug(f"Found {locked_count} locked {plural(locked_count, 'book')}")
            self.logger.debug(f"Removed {ignored} unwanted language {plural(ignored, 'result')}")
            self.logger.debug(f"Removed {removed_results} incorrect/incomplete {plural(removed_results, 'result')}")
            self.logger.debug(f"Removed {duplicates} duplicate {plural(duplicates, 'result')}")
            self.logger.debug(f"Ignored {book_ignore_count} {plural(book_ignore_count, 'book')}")
            self.logger.debug(f"Imported/Updated {resultcount} {plural(resultcount, 'book')} for author")

            db.action('insert into stats values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                      (authorname, api_hits, gr_lang_hits, lt_lang_hits, gb_lang_change,
                       cache_hits, ignored, removed_results, not_cached, duplicates))

            if refresh:
                self.logger.info(
                    f"[{authorname}] Book processing complete: Added {added_count} "
                    f"{plural(added_count, 'book')} / Updated {updated_count} "
                    f"{plural(updated_count, 'book')}")
            else:
                self.logger.info(
                    f"[{authorname}] Book processing complete: Added {added_count} "
                    f"{plural(added_count, 'book')} to the database")

        except Exception:
            self.logger.error(f'Unhandled exception in GB.get_author_books: {traceback.format_exc()}')
        finally:
            db.close()

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='gb.find_book'):
        if not CONFIG['GB_API']:
            self.logger.warning('No GoogleBooks API key, check config')
            return
        url = '/'.join([CONFIG['GB_URL'], f"books/v1/volumes/{str(bookid)}?key={CONFIG['GB_API']}"])
        jsonresults, _ = json_request(url)

        if not jsonresults:
            self.logger.debug(f'No results found for {bookid}')
            return

        if not bookstatus:
            bookstatus = CONFIG['NEWBOOK_STATUS']
        if not audiostatus:
            audiostatus = CONFIG['NEWAUDIO_STATUS']

        book = google_book_dict(jsonresults)
        dic = {':': '.', '"': ''}
        bookname = replace_all(book['name'], dic).strip()

        if not book['author']:
            self.logger.debug(f'Book {bookname} does not contain author field, skipping')
            return
        # warn if language is in ignore list, but user said they wanted this book
        valid_langs = get_list(CONFIG['IMP_PREFLANG'])
        if book['lang'] not in valid_langs and 'All' not in valid_langs:
            msg = f"Book {bookname} googlebooks language does not match preference, {book['lang']}"
            self.logger.warning(msg)
            if reason.startswith("Series:"):
                return

        if CONFIG.get_bool('NO_PUBDATE'):
            if not book['date'] or book['date'] == '0000':
                msg = f"Book {bookname} Publication date does not match preference, {book['date']}"
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return

        if CONFIG.get_bool('NO_FUTURE'):
            if book['date'] > today()[:4]:
                msg = f"Book {bookname} Future publication date does not match preference, {book['date']}"
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return

        if CONFIG.get_bool('NO_SETS'):
            # allow date ranges eg 1981-95
            m = re.search(r'(\d+)-(\d+)', bookname)
            if m:
                if check_year(m.group(1), past=1800, future=0):
                    self.logger.debug(f"Allow {bookname}, looks like a date range")
                else:
                    msg = f"Book {bookname} Set or Part"
                    self.logger.warning(msg)
                    if reason.startswith("Series:"):
                        return
            # book 1 of 3 or 1/3 but not dates 01/02/21
            if re.search(r'\d+ of \d+', bookname) or \
                    re.search(r'\d+/\d+', bookname) and not re.search(r'\d+/\d+/\d+', bookname):
                msg = f"Book {bookname} Set or Part"
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return
            # book title / another titla
            elif re.search(r'\w+\s*/\s*\w+', bookname):
                msg = f"Book {bookname} Set or Part"
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return

        db = database.DBConnection()
        try:
            authorname = book['author']
            if CONFIG['BOOK_API'] == "HardCover":
                hc = HardCover(f"{authorname}<ll>{bookname}")
                author = hc.find_author_id()
            else:
                ol = OpenLibrary(f"{authorname}<ll>{bookname}")
                author = ol.find_author_id()
            if author:
                author_id = author['authorid']
                match = db.match('SELECT AuthorID from authors WHERE AuthorID=?', (author_id,))
                if not match:
                    match = db.match('SELECT AuthorID from authors WHERE AuthorName=?', (author['authorname'],))
                    if match:
                        self.logger.debug(
                            f"{author['authorname']}: Changing authorid from {author_id} to {match['AuthorID']}")
                        author_id = match['AuthorID']  # we have a different authorid for that authorname
                    else:  # no author but request to add book, add author with newauthor status
                        # User hit "add book" button from a search or a wishlist import
                        newauthor_status = 'Active'
                        if CONFIG['NEWAUTHOR_STATUS'] in ['Skipped', 'Ignored']:
                            newauthor_status = 'Paused'
                        # also set paused if adding author as a series contributor
                        if reason.startswith('Series:'):
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
                        if CONFIG['BOOK_API'] == "HardCover":
                            new_value_dict['hc_id'] = author_id
                        else:
                            new_value_dict['ol_id'] = author_id
                        authorname = author['authorname']
                        db.upsert("authors", new_value_dict, control_value_dict)
                        if CONFIG.get_bool('NEWAUTHOR_BOOKS') and newauthor_status != 'Paused':
                            self.get_author_books(author_id, entrystatus=CONFIG['NEWAUTHOR_STATUS'],
                                                  reason=reason)
            else:
                self.logger.warning(f"No AuthorID for {book['author']}, unable to add book {bookname}")
                return

            reason = f"[{thread_name()}] {reason}"
            control_value_dict = {"BookID": bookid}
            new_value_dict = {
                "AuthorID": author_id,
                "BookName": bookname,
                "BookSub": book['sub'],
                "BookDesc": book['desc'],
                "BookIsbn": book['isbn'],
                "BookPub": book['pub'],
                "BookGenre": book['genre'],
                "BookImg": book['img'],
                "BookLink": book['link'],
                "BookRate": float(book['rate']),
                "BookPages": book['pages'],
                "BookDate": book['date'],
                "BookLang": book['lang'],
                "Status": bookstatus,
                "AudioStatus": audiostatus,
                "ScanResult": reason,
                "BookAdded": today(),
                "gb_id": bookid
            }

            if 'nocover' in book['img'] or 'nophoto' in book['img']:
                # try to get a cover from another source
                link, _ = get_book_cover(bookid, ignore='googleapis')
                if link:
                    new_value_dict["BookImg"] = link
                elif book['img'] and book['img'].startswith('http'):
                    link = cache_bookimg(book['img'], bookid, 'gb')
                    new_value_dict["BookImg"] = link

            db.upsert("books", new_value_dict, control_value_dict)
            self.logger.info(f"{bookname} by {authorname} added to the books database, {bookstatus}/{audiostatus}")
            serieslist = []
            if book['series']:
                serieslist = [('', book['seriesNum'], clean_name(book['series'], '&/'))]
            if CONFIG.get_bool('ADD_SERIES') and "Ignored:" not in reason:
                newserieslist = get_work_series(bookid, 'LT', reason=reason)
                if newserieslist:
                    serieslist = newserieslist
                    self.logger.debug(f'Updated series: {bookid} [{serieslist}]')
                set_series(serieslist, bookid, reason=reason)

            worklink = get_work_page(bookid)
            if worklink:
                control_value_dict = {"BookID": bookid}
                new_value_dict = {"WorkPage": worklink}
                db.upsert("books", new_value_dict, control_value_dict)
        finally:
            db.close()
