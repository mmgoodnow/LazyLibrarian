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

import logging
import threading
import time
import traceback

import lazylibrarian
from lazylibrarian import database
from lazylibrarian.config2 import CONFIG
from lazylibrarian.csvfile import finditem
from lazylibrarian.formatter import plural, unaccented, format_author_name, split_title, thread_name, get_list
from lazylibrarian.importer import import_book, search_for, add_author_name_to_db
from lazylibrarian.providers import iterate_over_rss_sites, iterate_over_wishlists
from lazylibrarian.resultlist import process_result_list
from lazylibrarian.scheduling import schedule_job, SchedulerCommand
from lazylibrarian.telemetry import TELEMETRY


def cron_search_rss_book():
    logger = logging.getLogger(__name__)
    if 'SEARCHALLRSS' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_rss_book()
    else:
        logger.debug("SEARCHALLRSS is already running")


def cron_search_wishlist():
    logger = logging.getLogger(__name__)
    if 'SEARCHWISHLIST' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_wishlist()
    else:
        logger.debug("SEARCHWISHLIST is already running")


def want_existing(bookmatch, book, search_start, ebook_status, audio_status):
    logger = logging.getLogger(__name__)
    want_book = False
    want_audio = False
    db = database.DBConnection()
    try:
        bookid = bookmatch['BookID']
        authorname = bookmatch['AuthorName']
        bookname = bookmatch['BookName']
        cmd = "SELECT authors.Status,Updated from authors,books WHERE authors.authorid=books.authorid and bookid=?"
        auth_res = db.match(cmd, (bookid,))
        if auth_res:
            auth_status = auth_res['Status']
        else:
            auth_status = 'Unknown'
        cmd = "SELECT SeriesName,Status from series,member where series.SeriesID=member.SeriesID and member.BookID=?"
        series = db.select(cmd, (bookid,))
        reject_series = None
        for ser in series:
            if ser['Status'] in ['Paused', 'Ignored']:
                reject_series = {"Name": ser['SeriesName'], "Status": ser['Status']}
                break
        if bookmatch['Status'] in ['Open', 'Wanted', 'Have']:
            logger.info(
                f"Found book {bookname} by {authorname}, already marked as \"{bookmatch['Status']}\"")
            if bookmatch["Requester"]:  # Already on a wishlist
                if book["dispname"] not in bookmatch["Requester"]:
                    new_value_dict = {"Requester": f"{bookmatch['Requester'] + book['dispname']} "}
                    control_value_dict = {"BookID": bookid}
                    db.upsert("books", new_value_dict, control_value_dict)
            else:
                new_value_dict = {"Requester": f"{book['dispname']} "}
                control_value_dict = {"BookID": bookid}
                db.upsert("books", new_value_dict, control_value_dict)
        elif auth_status in ['Ignored'] and auth_res['Updated'] < search_start:
            logger.info(f'Found book {bookname}, but author is "{auth_status}"')
        elif reject_series and auth_res['Updated'] < search_start:
            logger.info(f"Found book {bookname}, but series \"{reject_series['Name']}\" is {reject_series['Status']}")
        elif ebook_status == 'Wanted':
            logger.info(f'Found book {bookname} by {authorname}, marking as "Wanted"')
            control_value_dict = {"BookID": bookid}
            new_value_dict = {"Status": "Wanted"}
            db.upsert("books", new_value_dict, control_value_dict)
            if bookmatch["Requester"]:  # Already on a wishlist
                if book["dispname"] not in bookmatch["Requester"]:
                    new_value_dict = {"Requester": f"{bookmatch['Requester'] + book['dispname']} "}
                    control_value_dict = {"BookID": bookid}
                    db.upsert("books", new_value_dict, control_value_dict)
            else:
                new_value_dict = {"Requester": f"{book['dispname']} "}
                control_value_dict = {"BookID": bookid}
                db.upsert("books", new_value_dict, control_value_dict)
        if bookmatch['AudioStatus'] in ['Open', 'Wanted', 'Have']:
            logger.info(f"Found audiobook {bookname} by {authorname}, already marked as \"{bookmatch['AudioStatus']}\"")
            if bookmatch["AudioRequester"]:  # Already on a wishlist
                if book["dispname"] not in bookmatch["AudioRequester"]:
                    new_value_dict = {"AudioRequester": f"{bookmatch['AudioRequester'] + book['dispname']} "}
                    control_value_dict = {"BookID": bookid}
                    db.upsert("books", new_value_dict, control_value_dict)
            else:
                new_value_dict = {"AudioRequester": f"{book['dispname']} "}
                control_value_dict = {"BookID": bookid}
                db.upsert("books", new_value_dict, control_value_dict)
        elif auth_status in ['Ignored'] and auth_res['Updated'] < search_start:
            logger.info(f'Found book {bookname}, but author is "{auth_status}"')
        elif reject_series and auth_res['Updated'] < search_start:
            logger.info(f"Found book {bookname}, but series \"{reject_series['Name']}\" is {reject_series['Status']}")
        elif audio_status == 'Wanted':  # skipped/ignored
            logger.info(f'Found audiobook {bookname} by {authorname}, marking as "Wanted"')
            control_value_dict = {"BookID": bookid}
            new_value_dict = {"AudioStatus": "Wanted"}
            db.upsert("books", new_value_dict, control_value_dict)
            if bookmatch["AudioRequester"]:  # Already on a wishlist
                if book["dispname"] not in bookmatch["AudioRequester"]:
                    new_value_dict = {"AudioRequester": f"{bookmatch['AudioRequester'] + book['dispname']} "}
                    control_value_dict = {"BookID": bookid}
                    db.upsert("books", new_value_dict, control_value_dict)
            else:
                new_value_dict = {"AudioRequester": f"{book['dispname']} "}
                control_value_dict = {"BookID": bookid}
                db.upsert("books", new_value_dict, control_value_dict)
    finally:
        db.close()

    return want_book, want_audio


# noinspection PyBroadException
def search_wishlist():
    TELEMETRY.record_usage_data('Search/Wishlist')
    logger = logging.getLogger(__name__)
    thread_name("SEARCHWISHLIST")
    new_books = []
    new_audio = []
    search_start = time.time()
    db = database.DBConnection()
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        try:
            resultlist, wishproviders = iterate_over_wishlists()
            if not wishproviders:
                logger.debug('No wishlists are set')
                schedule_job(action=SchedulerCommand.STOP, target='search_wishlist')
                return  # No point in continuing

            # for each item in resultlist, add to database if necessary, and mark as wanted
            logger.debug(f"Processing {len(resultlist)} {plural(len(resultlist), 'item')} in wishlists")
            for book in resultlist:
                # we get rss_author, rss_title, maybe rss_isbn, rss_bookid (goodreads bookid)
                # we can just use bookid if goodreads, or try isbn and name matching on author/title if not
                # eg NYTimes wishlist
                if lazylibrarian.STOPTHREADS and thread_name() == "SEARCHWISHLIST":
                    logger.debug("Aborting SEARCHWISHLIST")
                    break

                if 'E' in book['types']:
                    ebook_status = "Wanted"
                else:
                    ebook_status = "Skipped"
                if 'A' in book['types']:
                    audio_status = "Wanted"
                else:
                    audio_status = "Skipped"

                item = {'Title': book['rss_title']}
                if book.get('rss_bookid'):
                    item['BookID'] = book['rss_bookid']
                if book.get('rss_isbn'):
                    item['ISBN'] = book['rss_isbn']

                bookmatch = finditem(item, book['rss_author'], reason=f"wishlist: {book['dispname']}")
                if bookmatch:  # it's in the database
                    want_book, want_audio = want_existing(bookmatch, book, search_start, ebook_status, audio_status)
                    if want_book:
                        new_books.append({"bookid": bookmatch['BookID']})
                    if want_audio:
                        new_audio.append({"bookid": bookmatch['BookID']})
                else:  # not in database yet
                    results = []
                    authorname = format_author_name(book['rss_author'],
                                                    postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
                    authmatch = db.match('SELECT * FROM authors where AuthorName=?', (authorname,))
                    if authmatch:
                        logger.debug(f"Author {authorname} found in database, {authmatch['Status']}")
                        if authmatch['Status'] == 'Ignored':
                            authorname = ''
                    else:
                        logger.debug(f"Author {authorname} not found")
                        newauthor, _, _ = add_author_name_to_db(author=authorname, addbooks=False,
                                                                reason=f"wishlist: {book['rss_title']}",
                                                                title=book['rss_title'])
                        if newauthor and newauthor != authorname:
                            logger.debug(f"Preferred authorname changed from [{authorname}] to [{newauthor}]")
                            authorname = newauthor
                        if not newauthor:
                            logger.warning(f"Authorname {authorname} not added to database")
                            authorname = ''

                    if authorname and book['rss_isbn']:
                        results = search_for(book['rss_isbn'])
                        for result in results:
                            if result['isbn_fuzz'] > CONFIG.get_int('MATCH_RATIO'):
                                logger.info(
                                    f"Found {result['bookid']} ({result['isbn_fuzz']}%) {result['authorname']}: "
                                    f"{result['bookname']}")
                                if result['authorname'] != authorname:
                                    logger.debug(f"isbn authorname mismatch {result['authorname']}:{authorname}")
                                    authorname = result['authorname']
                                    bookmatch = finditem(item, result['authorname'],
                                                         reason=f"wishlist: {book['dispname']}")
                                    if bookmatch:  # it's in the database under isbn authorname
                                        want_book, want_audio = want_existing(bookmatch, book, search_start,
                                                                              ebook_status, audio_status)
                                        if want_book:
                                            new_books.append({"bookid": bookmatch['BookID']})
                                        if want_audio:
                                            new_audio.append({"bookid": bookmatch['BookID']})
                                        authorname = None  # to skip adding it again
                                else:
                                    bookmatch = result
                                break

                    if authorname and not bookmatch:
                        searchterm = f"{book['rss_title']} <ll> {authorname}"
                        results = search_for(unaccented(searchterm, only_ascii=False))
                        for result in results:
                            if result['author_fuzz'] > CONFIG.get_int('MATCH_RATIO') \
                                    and result['book_fuzz'] > CONFIG.get_int('MATCH_RATIO'):
                                logger.info(
                                    f"Found {result['bookid']} ({result['author_fuzz']}% {result['book_fuzz']}%) "
                                    f"{result['authorname']}: {result['bookname']}")
                                bookmatch = result
                                break

                    if authorname and not bookmatch:
                        # no match on full searchterm, try splitting out subtitle and series
                        newtitle, _, _ = split_title(authorname, book['rss_title'])
                        if newtitle != book['rss_title']:
                            title = newtitle
                            searchterm = f"{title} <ll> {authorname}"
                            results = search_for(unaccented(searchterm, only_ascii=False))
                            for result in results:
                                if result['author_fuzz'] > CONFIG.get_int('MATCH_RATIO') \
                                        and result['book_fuzz'] > CONFIG.get_int('MATCH_RATIO'):
                                    logger.info(
                                        f"Found {result['bookid']} ({result['author_fuzz']}% {result['book_fuzz']}%) "
                                        f"{result['authorname']}: {result['bookname']}")
                                    bookmatch = result
                                    break

                    if authorname and bookmatch:
                        import_book(bookmatch['bookid'], ebook_status, audio_status,
                                    reason=f"Added from wishlist {book['dispname']}")
                        if ebook_status == 'Wanted':
                            new_books.append({"bookid": bookmatch['bookid']})
                        if audio_status == 'Wanted':
                            new_audio.append({"bookid": bookmatch['bookid']})
                        new_value_dict = {"Requester": f"{book['dispname']} ", "AudioRequester": f"{book['dispname']} "}
                        control_value_dict = {"BookID": bookmatch['bookid']}
                        db.upsert("books", new_value_dict, control_value_dict)

                    elif authorname is not None:
                        msg = f"Skipping book {book['rss_title']} by {book['rss_author']}"
                        if not results:
                            msg += ', No results returned'
                            logger.warning(msg)
                        else:
                            msg += ', No match found'
                            logger.warning(msg)
                            logger.warning(
                                f"Closest match ({results[0]['author_fuzz']}% {results[0]['book_fuzz']}%) "
                                f"{results[0]['authorname']}: {results[0]['bookname']}")
            if new_books or new_audio:
                tot = len(new_books) + len(new_audio)
                logger.info(f"Wishlist marked {tot} {plural(tot, 'item')} as Wanted")
            else:
                logger.debug("Wishlist marked no new items as Wanted")
        finally:
            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
    except Exception:
        logger.error(f'Unhandled exception in search_wishlist: {traceback.format_exc()}')
    finally:
        db.close()
        if new_books:
            threading.Thread(target=search_rss_book, name='WISHLISTRSSBOOKS',
                             args=[new_books, 'eBook']).start()
            threading.Thread(target=lazylibrarian.searchbook.search_book, name='WISHLISTBOOKS',
                             args=[new_books, 'eBook']).start()
        if new_audio:
            threading.Thread(target=search_rss_book, name='WISHLISTRSSAUDIO',
                             args=[new_audio, 'AudioBook']).start()
            threading.Thread(target=lazylibrarian.searchbook.search_book, name='WISHLISTAUDIO',
                             args=[new_audio, 'AudioBook']).start()
        thread_name("WEBSERVER")


# noinspection PyBroadException
def search_rss_book(books=None, library=None):
    """
    books is a list of new books to add, or None for backlog search
    library is "eBook" or "AudioBook" or None to search all book types
    """
    TELEMETRY.record_usage_data('Search/Book/RSS')
    logger = logging.getLogger(__name__)
    if not (CONFIG.use_rss()):
        logger.warning('rss search is disabled')
        schedule_job(action=SchedulerCommand.STOP, target='search_rss_book')
        return
    threadname = thread_name()
    if "Thread" in threadname:
        if not books:
            thread_name("SEARCHALLRSS")
        else:
            thread_name("SEARCHRSS")

    db = database.DBConnection()
    try:
        searchbooks = []
        if not books:
            # We are performing a backlog search
            cmd = ("SELECT BookID, AuthorName, Bookname, BookSub, BookAdded, books.Status, AudioStatus from "
                   "books,authors WHERE (books.Status='Wanted' OR AudioStatus='Wanted') and "
                   "books.AuthorID = authors.AuthorID order by BookAdded desc")
            results = db.select(cmd)
            for terms in results:
                searchbooks.append(terms)
        else:
            # The user has added a new book
            for book in books:
                cmd = ("SELECT BookID, AuthorName, BookName, BookSub, books.Status, AudioStatus from books,authors "
                       "WHERE BookID=? AND books.AuthorID = authors.AuthorID")
                results = db.select(cmd, (book['bookid'],))
                for terms in results:
                    searchbooks.append(terms)

        if len(searchbooks) == 0:
            logger.debug("SearchRSS - No books to search for")
            return

        resultlist, nproviders, _ = iterate_over_rss_sites()
        if not nproviders:
            logger.warning('No rss providers are available')
            schedule_job(action=SchedulerCommand.STOP, target='search_rss_book')
            return  # No point in continuing

        logger.info(f"rss Searching for {len(searchbooks)} {plural(len(searchbooks), 'book')}")

        searchlist = []
        for searchbook in searchbooks:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLRSS":
                logger.debug(f"Aborting {threadname}")
                break

            # searchterm is only used for display purposes
            searchterm = f"{searchbook['AuthorName']} {searchbook['BookName']}"
            if searchbook['BookSub']:
                searchterm = f"{searchterm}: {searchbook['BookSub']}"

            if library is None or library == 'eBook':
                if searchbook['Status'] == "Wanted":
                    cmd = "SELECT BookID from wanted WHERE BookID=? and AuxInfo='eBook' and Status='Snatched'"
                    snatched = db.match(cmd, (searchbook["BookID"],))
                    if snatched:
                        logger.warning(
                            f"eBook {searchbook['AuthorName']} {searchbook['BookName']} already marked "
                            f"snatched in wanted table")
                    else:
                        searchlist.append(
                            {"bookid": searchbook['BookID'],
                             "bookName": searchbook['BookName'],
                             "bookSub": searchbook['BookSub'],
                             "authorName": searchbook['AuthorName'],
                             "library": "eBook",
                             "searchterm": searchterm})

            if library is None or library == 'AudioBook':
                if searchbook['AudioStatus'] == "Wanted":
                    cmd = "SELECT BookID from wanted WHERE BookID=? and AuxInfo='AudioBook' and Status='Snatched'"
                    snatched = db.match(cmd, (searchbook["BookID"],))
                    if snatched:
                        logger.warning(
                            f"AudioBook {searchbook['AuthorName']} {searchbook['BookName']} already marked "
                            f"snatched in wanted table")
                    else:
                        searchlist.append(
                            {"bookid": searchbook['BookID'],
                             "bookName": searchbook['BookName'],
                             "bookSub": searchbook['BookSub'],
                             "authorName": searchbook['AuthorName'],
                             "library": "AudioBook",
                             "searchterm": searchterm})

        rss_count = 0
        for book in searchlist:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLRSS":
                logger.debug(f"Aborting {threadname}")
                break
            if book['library'] == 'AudioBook':
                searchtype = 'audio'
            else:
                searchtype = 'book'
            found = process_result_list(resultlist, book, searchtype, 'rss')

            # if you can't find the book, try title without any "(extended details, series etc)"
            if not found and '(' in book['bookName']:  # anything to shorten?
                searchtype = f"short{searchtype}"
                found = process_result_list(resultlist, book, searchtype, 'rss')

            if not found:
                logger.info(f"rss Searches for {book['library']} {book['searchterm']} returned no results.")
            if found > 1:
                rss_count += 1

        logger.info(f"rss Search for Wanted items complete, found {rss_count} {plural(rss_count, 'book')}")
        db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})

    except Exception:
        logger.error(f'Unhandled exception in search_rss_book: {traceback.format_exc()}')
    finally:
        db.close()
        thread_name("WEBSERVER")
