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

import lazylibrarian
from lazylibrarian.images import cache_bookimg
from lazylibrarian import database
from lazylibrarian.bookwork import get_work_series, get_work_page, delete_empty_series, \
    set_series, get_status, thinglang, google_book_dict
from lazylibrarian.cache import json_request
from lazylibrarian.config2 import CONFIG
from lazylibrarian.formatter import plural, today, replace_all, unaccented, is_valid_isbn, \
    get_list, clean_name, make_unicode, make_utf8bytes, replace_quotes_with, check_year, thread_name
from lazylibrarian.hc import HardCover
from lazylibrarian.images import get_book_cover
from lazylibrarian.ol import OpenLibrary
from thefuzz import fuzz


class GoogleBooks:
    def __init__(self, name=None):
        self.name = make_unicode(name)
        self.logger = logging.getLogger(__name__)
        if not CONFIG['GB_API']:
            self.logger.warning('No GoogleBooks API key, check config')
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
            self.logger.debug('Now searching Google Books API with searchterm: %s' % fullterm)

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
                    set_url += quote(make_utf8bytes(api_value + '"' + searchterm + '"')[0])
                elif api_value == 'inauthor:':
                    searchterm = fullterm
                    if authorname:
                        searchterm = authorname  # just search for author
                    searchterm = searchterm.strip()
                    set_url += quote_plus(make_utf8bytes(api_value + '"' + searchterm + '"')[0])

                startindex = 0
                resultcount = 0
                ignored = 0
                number_results = 1
                total_count = 0
                no_author_count = 0
                try:
                    while startindex < number_results:

                        self.params['startIndex'] = startindex
                        url = set_url + '&' + urlencode(self.params)

                        try:
                            jsonresults, in_cache = json_request(url)
                            if not jsonresults:
                                number_results = 0
                            else:
                                if not in_cache:
                                    api_hits += 1
                                number_results = jsonresults['totalItems']
                                self.logger.debug('Searching url: ' + url)
                            if number_results == 0:
                                self.logger.warning('Found no results for %s with value: %s' % (api_value, searchterm))
                                break
                            else:
                                pass
                        except Exception as err:
                            errmsg = str(err)
                            self.logger.warning(
                                'Google Books API Error [%s]: Check your API key or wait a while' % errmsg)
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
                                    if booklang not in valid_langs:
                                        self.logger.debug(
                                            'Skipped %s with language %s' % (book['name'], booklang))
                                        ignored += 1
                                        continue
                                except KeyError:
                                    ignored += 1
                                    self.logger.debug('Skipped %s where no language is found' % book['name'])
                                    continue

                            if authorname:
                                author_fuzz = fuzz.ratio(book['author'], authorname)
                            else:
                                author_fuzz = fuzz.ratio(book['author'], fullterm)

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

                self.logger.debug("Returning %s %s for (%s) with keyword: %s" %
                                  (resultcount, plural(resultcount, "result"), api_value, searchterm))

            self.logger.debug("Found %s %s" % (total_count, plural(total_count, "result")))
            self.logger.debug("Removed %s unwanted language %s" % (ignored, plural(ignored, "result")))
            self.logger.debug("Removed %s %s with no author" % (no_author_count, plural(no_author_count, "book")))
            self.logger.debug('The Google Books API was hit %s %s for searchterm: %s' %
                              (api_hits, plural(api_hits, "time"), fullterm))
            queue.put(resultlist)

        except Exception:
            self.logger.error('Unhandled exception in GB.find_results: %s' % traceback.format_exc())

    def get_author_books(self, authorid=None, authorname=None, bookstatus="Skipped",
                         audiostatus="Skipped", entrystatus='Active', refresh=False, reason='gb.get_author_books'):
        # noinspection PyBroadException
        self.logger.debug('[%s] Now processing books with Google Books API' % authorname)
        db = database.DBConnection()
        try:
            # google doesnt like accents in author names
            set_url = self.url + quote('inauthor:"%s"' % unaccented(authorname, only_ascii=False))
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
                        self.logger.debug("Aborting %s" % threadname)
                        break
                    self.params['startIndex'] = startindex
                    url = set_url + '&' + urlencode(self.params)

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
                        self.logger.warning('Google Books API Error [%s]: Check your API key or wait a while' % errmsg)
                        break

                    if number_results == 0:
                        self.logger.warning('Found no results for %s' % authorname)
                        break
                    else:
                        self.logger.debug('Found %s %s for %s' % (number_results, plural(number_results, "result"),
                                                                  authorname))

                    startindex += 40

                    for item in jsonresults['items']:
                        if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                            self.logger.debug("Aborting %s" % threadname)
                            break
                        total_count += 1
                        book = google_book_dict(item)
                        # skip if no author, no author is no book.
                        if not book['author']:
                            self.logger.debug('Skipped a result without authorfield.')
                            continue

                        isbnhead = ""
                        if len(book['isbn']) == 10:
                            isbnhead = book['isbn'][0:3]
                        elif len(book['isbn']) == 13:
                            isbnhead = book['isbn'][3:6]

                        booklang = book['lang']
                        # do we care about language?
                        if "All" not in valid_langs:
                            if book['isbn']:
                                # seems google lies to us, sometimes tells us books are in english when they are not
                                if booklang == "Unknown" or booklang == "en":
                                    googlelang = booklang
                                    match = False
                                    lang = db.match('SELECT lang FROM languages where isbn=?', (isbnhead,))
                                    if lang:
                                        booklang = lang['lang']
                                        cache_hits += 1
                                        self.logger.debug("Found cached language [%s] for [%s]" % (booklang, isbnhead))
                                        match = True
                                    if not match:  # no match in cache, try lookup dict
                                        if isbnhead:
                                            if len(book['isbn']) == 13 and book['isbn'].startswith('979'):
                                                for lang in lazylibrarian.isbn_979_dict:
                                                    if isbnhead.startswith(lang):
                                                        booklang = lazylibrarian.isbn_979_dict[lang]
                                                        self.logger.debug("ISBN979 returned %s for %s" % (booklang,
                                                                                                          isbnhead))
                                                        match = True
                                                        break
                                            elif (len(book['isbn']) == 10) or \
                                                    (len(book['isbn']) == 13 and book['isbn'].startswith('978')):
                                                for lang in lazylibrarian.isbn_978_dict:
                                                    if isbnhead.startswith(lang):
                                                        booklang = lazylibrarian.isbn_978_dict[lang]
                                                        self.logger.debug("ISBN979 returned %s for %s" %
                                                                          (booklang, isbnhead))
                                                        match = True
                                                        break
                                            if match:
                                                control_value_dict = {"isbn": isbnhead}
                                                new_value_dict = {"lang": booklang}
                                                db.upsert("languages", new_value_dict, control_value_dict)

                                    if not match:
                                        booklang = thinglang(book['isbn'])
                                        lt_lang_hits += 1
                                        if booklang:
                                            match = True
                                            db.action('insert into languages values (?, ?)', (isbnhead, booklang))

                                    if match:
                                        # We found a better language match
                                        if googlelang == "en" and booklang not in ["en-US", "en-GB", "eng"]:
                                            # these are all english, may need to expand this list
                                            self.logger.debug("%s Google thinks [%s], we think [%s]" %
                                                              (book['name'], googlelang, booklang))
                                            gb_lang_change += 1
                                    else:  # No match anywhere, accept google language
                                        booklang = googlelang

                            # skip if language is in ignore list
                            if booklang not in valid_langs:
                                self.logger.debug('Skipped [%s] with language %s' % (book['name'], booklang))
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
                            self.logger.debug('Rejecting bookid %s for %s, no bookname' % (bookid, authorname))
                            rejected = 'name', 'No bookname'
                        else:
                            bookname = replace_all(bookname, {':': ' ', '"': '', '\'': ''}).strip()
                            # if re.match(r'[^\w-]', bookname):  # remove books with bad characters in title
                            # self.logger.debug("[%s] removed book for bad characters" % bookname)
                            # rejected = 'chars', 'Bad characters in bookname'

                        if not rejected and CONFIG.get_bool('NO_FUTURE'):
                            # googlebooks sometimes gives yyyy, sometimes yyyy-mm, sometimes yyyy-mm-dd
                            if book['date'] > today()[:len(book['date'])]:
                                self.logger.debug('Rejecting %s, future publication date %s' % (bookname, book['date']))
                                rejected = 'future', 'Future publication date [%s]' % book['date']

                        if not rejected and CONFIG.get_bool('NO_PUBDATE'):
                            if not book['date']:
                                self.logger.debug('Rejecting %s, no publication date' % bookname)
                                rejected = 'date', 'No publication date'

                        if not rejected and CONFIG.get_bool('NO_ISBN'):
                            if not isbnhead:
                                self.logger.debug('Rejecting %s, no isbn' % bookname)
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
                                    rejected = 'bookid', 'Got under different bookid %s' % bookid
                                    if not match['gb_id']:
                                        db.action("UPDATE books SET gb_id=? WHERE BookID=?", (bookid, match['BookID']))
                                    duplicates += 1

                        cmd = ("SELECT AuthorName,BookName,AudioStatus,books.Status,ScanResult,gb_id,BookID "
                               "FROM books,authors WHERE authors.AuthorID = books.AuthorID AND BookID=?")
                        match = db.match(cmd, (bookid,))
                        if match:  # we have a book with this bookid already
                            if bookname != match['BookName'] or authorname != match['AuthorName']:
                                self.logger.debug('Rejecting bookid %s for [%s][%s] already got bookid for [%s][%s]' %
                                                  (bookid, authorname, bookname, match['AuthorName'],
                                                   match['BookName']))
                                if not match['gb_id']:
                                    db.action("UPDATE books SET gb_id=? WHERE BookID=?", (bookid, match['BookID']))
                                duplicates += 1
                                rejected = 'got', 'Already got this bookid in database'
                            else:
                                msg = 'Bookid %s for [%s][%s] is in database marked %s' % (
                                    bookid, authorname, bookname, match['Status'])
                                if CONFIG.get_bool('AUDIO_TAB'):
                                    msg += ",%s" % match['AudioStatus']
                                msg += " %s" % match['ScanResult']
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
                                    reason = "Ignored: %s" % rejected[1]
                                else:
                                    reason = "Rejected: %s" % rejected[1]
                            else:
                                if 'author_update' in entryreason:
                                    reason = 'Author: %s' % authorname
                                else:
                                    reason = entryreason

                            if locked:
                                locked_count += 1
                            else:
                                reason = "[%s] %s" % (thread_name(), reason)
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
                                    link, _ = get_book_cover(bookid)
                                    if link:
                                        new_value_dict = {"BookImg": link}
                                    elif book['img'] and book['img'].startswith('http'):
                                        link = cache_bookimg(book['img'], bookid, 'gb')
                                        new_value_dict = {"BookImg": link}

                                db.upsert("books", new_value_dict, control_value_dict)
                                self.logger.debug("Book found: " + bookname + " " + book['date'])

                                serieslist = []
                                if book['series']:
                                    serieslist = [('', book['seriesNum'], clean_name(book['series'], '&/'))]
                                if CONFIG.get_bool('ADD_SERIES') and "Ignored:" not in reason:
                                    newserieslist = get_work_series(bookid, 'LT', reason=reason)
                                    if newserieslist:
                                        serieslist = newserieslist
                                        self.logger.debug('Updated series: %s [%s]' % (bookid, serieslist))
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
                                        self.logger.debug("valid bookdate [%s] previous scanresult [%s]" %
                                                          (book['date'], existing['ScanResult']))
                                        update_value_dict["ScanResult"] = "bookdate %s is now valid" % book['date']

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
                                msg = "[%s] %s book: %s [%s] status %s" % (authorname, typ, bookname,
                                                                           booklang, book_status)
                                if CONFIG.get_bool('AUDIO_TAB'):
                                    msg += " audio %s" % audio_status
                                self.logger.debug(msg)
            except KeyError:
                pass

            delete_empty_series()
            self.logger.debug('[%s] The Google Books API was hit %s %s to populate book list' %
                              (authorname, api_hits, plural(api_hits, "time")))
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
            self.logger.debug("Found %s total %s for author" % (total_count, plural(total_count, "book")))
            self.logger.debug("Found %s locked %s" % (locked_count, plural(locked_count, "book")))
            self.logger.debug("Removed %s unwanted language %s" % (ignored, plural(ignored, "result")))
            self.logger.debug("Removed %s incorrect/incomplete %s" % (removed_results, plural(removed_results,
                                                                                              "result")))
            self.logger.debug("Removed %s duplicate %s" % (duplicates, plural(duplicates, "result")))
            self.logger.debug("Ignored %s %s" % (book_ignore_count, plural(book_ignore_count, "book")))
            self.logger.debug("Imported/Updated %s %s for author" % (resultcount, plural(resultcount, "book")))

            db.action('insert into stats values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                      (authorname, api_hits, gr_lang_hits, lt_lang_hits, gb_lang_change,
                       cache_hits, ignored, removed_results, not_cached, duplicates))

            if refresh:
                self.logger.info("[%s] Book processing complete: Added %s %s / Updated %s %s" %
                                 (authorname, added_count, plural(added_count, "book"),
                                  updated_count, plural(updated_count, "book")))
            else:
                self.logger.info("[%s] Book processing complete: Added %s %s to the database" %
                                 (authorname, added_count, plural(added_count, "book")))

        except Exception:
            self.logger.error('Unhandled exception in GB.get_author_books: %s' % traceback.format_exc())
        finally:
            db.close()

    def find_book(self, bookid=None, bookstatus=None, audiostatus=None, reason='gb.find_book'):
        if not CONFIG['GB_API']:
            self.logger.warning('No GoogleBooks API key, check config')
            return
        url = '/'.join([CONFIG['GB_URL'], 'books/v1/volumes/' +
                        str(bookid) + "?key=" + CONFIG['GB_API']])
        jsonresults, _ = json_request(url)

        if not jsonresults:
            self.logger.debug('No results found for %s' % bookid)
            return

        if not bookstatus:
            bookstatus = CONFIG['NEWBOOK_STATUS']
        if not audiostatus:
            audiostatus = CONFIG['NEWAUDIO_STATUS']

        book = google_book_dict(jsonresults)
        dic = {':': '.', '"': ''}
        bookname = replace_all(book['name'], dic).strip()

        if not book['author']:
            self.logger.debug('Book %s does not contain author field, skipping' % bookname)
            return
        # warn if language is in ignore list, but user said they wanted this book
        valid_langs = get_list(CONFIG['IMP_PREFLANG'])
        if book['lang'] not in valid_langs and 'All' not in valid_langs:
            msg = 'Book %s googlebooks language does not match preference, %s' % (bookname, book['lang'])
            self.logger.warning(msg)
            if reason.startswith("Series:"):
                return

        if CONFIG.get_bool('NO_PUBDATE'):
            if not book['date'] or book['date'] == '0000':
                msg = 'Book %s Publication date does not match preference, %s' % (bookname, book['date'])
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return

        if CONFIG.get_bool('NO_FUTURE'):
            if book['date'] > today()[:4]:
                msg = 'Book %s Future publication date does not match preference, %s' % (bookname, book['date'])
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return

        if CONFIG.get_bool('NO_SETS'):
            # allow date ranges eg 1981-95
            m = re.search(r'(\d+)-(\d+)', bookname)
            if m:
                if check_year(m.group(1), past=1800, future=0):
                    self.logger.debug("Allow %s, looks like a date range" % bookname)
                else:
                    msg = 'Book %s Set or Part'
                    self.logger.warning(msg)
                    if reason.startswith("Series:"):
                        return
            # book 1 of 3 or 1/3 but not dates 01/02/21
            if re.search(r'\d+ of \d+', bookname) or \
                    re.search(r'\d+/\d+', bookname) and not re.search(r'\d+/\d+/\d+', bookname):
                msg = 'Book %s Set or Part'
                self.logger.warning(msg)
                if reason.startswith("Series:"):
                    return
            # book title / another titla
            elif re.search(r'\w+\s*/\s*\w+', bookname):
                msg = 'Book %s Set or Part'
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
                        self.logger.debug('%s: Changing authorid from %s to %s' %
                                          (author['authorname'], author_id, match['AuthorID']))
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
                self.logger.warning("No AuthorID for %s, unable to add book %s" % (book['author'], bookname))
                return

            reason = "[%s] %s" % (thread_name(), reason)
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
                link, _ = get_book_cover(bookid)
                if link:
                    new_value_dict = {"BookImg": link}
                elif book['img'] and book['img'].startswith('http'):
                    link = cache_bookimg(book['img'], bookid, 'gb')
                    new_value_dict = {"BookImg": link}

            db.upsert("books", new_value_dict, control_value_dict)
            self.logger.info("%s by %s added to the books database, %s/%s" % (bookname, authorname,
                                                                              bookstatus, audiostatus))
            serieslist = []
            if book['series']:
                serieslist = [('', book['seriesNum'], clean_name(book['series'], '&/'))]
            if CONFIG.get_bool('ADD_SERIES') and "Ignored:" not in reason:
                newserieslist = get_work_series(bookid, 'LT', reason=reason)
                if newserieslist:
                    serieslist = newserieslist
                    self.logger.debug('Updated series: %s [%s]' % (bookid, serieslist))
                set_series(serieslist, bookid, reason=reason)

            worklink = get_work_page(bookid)
            if worklink:
                control_value_dict = {"BookID": bookid}
                new_value_dict = {"WorkPage": worklink}
                db.upsert("books", new_value_dict, control_value_dict)
        finally:
            db.close()
