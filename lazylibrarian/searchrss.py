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

import time
import threading
import traceback

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.common import scheduleJob
from lazylibrarian.csvfile import finditem
from lazylibrarian.formatter import plural, unaccented, formatAuthorName, check_int, split_title
from lazylibrarian.importer import import_book, search_for, addAuthorNameToDB
from lazylibrarian.providers import IterateOverRSSSites, IterateOverWishLists
from lazylibrarian.resultlist import processResultList


def cron_search_rss_book():
    if 'SEARCHALLRSS' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_rss_book()
    else:
        logger.debug("SEARCHALLRSS is already running")


def cron_search_wishlist():
    if 'SEARCHWISHLIST' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_wishlist()
    else:
        logger.debug("SEARCHWISHLIST is already running")


# noinspection PyBroadException
def search_wishlist():
    try:
        threadname = threading.currentThread().name
        if "Thread-" in threadname:
            threading.currentThread().name = "SEARCHWISHLIST"

        myDB = database.DBConnection()

        resultlist, wishproviders = IterateOverWishLists()
        new_books = 0
        if not wishproviders:
            logger.debug('No wishlists are set')
            scheduleJob(action='Stop', target='search_wishlist')
            return  # No point in continuing

        # for each item in resultlist, add to database if necessary, and mark as wanted
        logger.debug('Processing %s item%s in wishlists' % (len(resultlist), plural(len(resultlist))))
        for book in resultlist:
            # we get rss_author, rss_title, maybe rss_isbn, rss_bookid (goodreads bookid)
            # we can just use bookid if goodreads, or try isbn and name matching on author/title if not
            # eg NYTimes wishlist
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

            bookmatch = finditem(item, book['rss_author'], reason="wishlist: %s" % book['dispname'])
            if bookmatch:  # it's already in the database
                bookid = bookmatch['BookID']
                authorname = bookmatch['AuthorName']
                bookname = bookmatch['BookName']
                cmd = 'SELECT authors.Status from authors,books '
                cmd += 'WHERE authors.authorid=books.authorid and bookid=?'
                auth_res = myDB.match(cmd, (bookid,))
                if auth_res:
                    auth_status = auth_res['Status']
                else:
                    auth_status = 'Unknown'
                cmd = 'SELECT SeriesName,Status from series,member '
                cmd += 'where series.SeriesID=member.SeriesID and member.BookID=?'
                series = myDB.select(cmd, (bookid,))
                reject_series = None
                for ser in series:
                    if ser['Status'] in ['Paused', 'Ignored']:
                        reject_series = {"Name": ser['SeriesName'], "Status": ser['Status']}
                        break
                if bookmatch['Status'] in ['Open', 'Wanted', 'Have']:
                    logger.info(
                        'Found book %s by %s, already marked as "%s"' % (bookname, authorname, bookmatch['Status']))
                    if bookmatch["Requester"]:  # Already on a wishlist
                        if book["dispname"] not in bookmatch["Requester"]:
                            newValueDict = {"Requester": bookmatch["Requester"] + book["dispname"] + ' '}
                            controlValueDict = {"BookID": bookid}
                            myDB.upsert("books", newValueDict, controlValueDict)
                    else:
                        newValueDict = {"Requester": book["dispname"] + ' '}
                        controlValueDict = {"BookID": bookid}
                        myDB.upsert("books", newValueDict, controlValueDict)
                elif auth_status in ['Ignored']:
                    logger.info('Found book %s, but author is "%s"' % (bookname, auth_status))
                elif reject_series:
                    logger.info('Found book %s, but series "%s" is %s' %
                                (bookname, reject_series['Name'], reject_series['Status']))
                elif ebook_status == 'Wanted':  # skipped/ignored
                    logger.info('Found book %s by %s, marking as "Wanted"' % (bookname, authorname))
                    controlValueDict = {"BookID": bookid}
                    newValueDict = {"Status": "Wanted"}
                    myDB.upsert("books", newValueDict, controlValueDict)
                    new_books += 1
                    if bookmatch["Requester"]:  # Already on a wishlist
                        if book["dispname"] not in bookmatch["Requester"]:
                            newValueDict = {"Requester": bookmatch["Requester"] + book["dispname"] + ' '}
                            controlValueDict = {"BookID": bookid}
                            myDB.upsert("books", newValueDict, controlValueDict)
                    else:
                        newValueDict = {"Requester": book["dispname"] + ' '}
                        controlValueDict = {"BookID": bookid}
                        myDB.upsert("books", newValueDict, controlValueDict)
                if bookmatch['AudioStatus'] in ['Open', 'Wanted', 'Have']:
                    logger.info('Found audiobook %s by %s, already marked as "%s"' %
                                (bookname, authorname, bookmatch['AudioStatus']))
                    if bookmatch["AudioRequester"]:  # Already on a wishlist
                        if book["dispname"] not in bookmatch["AudioRequester"]:
                            newValueDict = {"AudioRequester": bookmatch["AudioRequester"] + book["dispname"] + ' '}
                            controlValueDict = {"BookID": bookid}
                            myDB.upsert("books", newValueDict, controlValueDict)
                    else:
                        newValueDict = {"AudioRequester": book["dispname"] + ' '}
                        controlValueDict = {"BookID": bookid}
                        myDB.upsert("books", newValueDict, controlValueDict)
                elif auth_status in ['Ignored']:
                    logger.info('Found book %s, but author is "%s"' % (bookname, auth_status))
                elif reject_series:
                    logger.info('Found book %s, but series "%s" is %s' %
                                (bookname, reject_series['Name'], reject_series['Status']))
                elif audio_status == 'Wanted':  # skipped/ignored
                    logger.info('Found audiobook %s by %s, marking as "Wanted"' % (bookname, authorname))
                    controlValueDict = {"BookID": bookid}
                    newValueDict = {"AudioStatus": "Wanted"}
                    myDB.upsert("books", newValueDict, controlValueDict)
                    new_books += 1
                    if bookmatch["AudioRequester"]:  # Already on a wishlist
                        if book["dispname"] not in bookmatch["AudioRequester"]:
                            newValueDict = {"AudioRequester": bookmatch["AudioRequester"] + book["dispname"] + ' '}
                            controlValueDict = {"BookID": bookid}
                            myDB.upsert("books", newValueDict, controlValueDict)
                    else:
                        newValueDict = {"AudioRequester": book["dispname"] + ' '}
                        controlValueDict = {"BookID": bookid}
                        myDB.upsert("books", newValueDict, controlValueDict)

            else:  # not in database yet
                results = []
                authorname = formatAuthorName(book['rss_author'])
                authmatch = myDB.match('SELECT * FROM authors where AuthorName=?', (authorname,))
                if authmatch:
                    logger.debug("Author %s found in database" % authorname)
                else:
                    logger.debug("Author %s not found" % authorname)
                    newauthor, _, _ = addAuthorNameToDB(author=authorname,
                                                        addbooks=lazylibrarian.CONFIG['NEWAUTHOR_BOOKS'],
                                                        reason="wishlist: %s" % book['rss_title'])
                    if len(newauthor) and newauthor != authorname:
                        logger.debug("Preferred authorname changed from [%s] to [%s]" % (authorname, newauthor))
                        authorname = newauthor

                if book['rss_isbn']:
                    results = search_for(book['rss_isbn'])
                    for result in results:
                        if result['isbn_fuzz'] > check_int(lazylibrarian.CONFIG['MATCH_RATIO'], 90):
                            logger.info("Found (%s%%) %s: %s" %
                                        (result['isbn_fuzz'], result['authorname'], result['bookname']))
                            bookmatch = result
                            break
                if not bookmatch:
                    searchterm = "%s <ll> %s" % (book['rss_title'], authorname)
                    results = search_for(unaccented(searchterm))
                    for result in results:
                        if result['author_fuzz'] > check_int(lazylibrarian.CONFIG['MATCH_RATIO'], 90) \
                                and result['book_fuzz'] > check_int(lazylibrarian.CONFIG['MATCH_RATIO'], 90):
                            logger.info("Found (%s%% %s%%) %s: %s" % (result['author_fuzz'], result['book_fuzz'],
                                                                      result['authorname'], result['bookname']))
                            bookmatch = result
                            break
                if not bookmatch:  # no match on full searchterm, try splitting out subtitle
                    newtitle, _ = split_title(authorname, book['rss_title'])
                    if newtitle != book['rss_title']:
                        title = newtitle
                        searchterm = "%s <ll> %s" % (title, authorname)
                        results = search_for(unaccented(searchterm))
                        for result in results:
                            if result['author_fuzz'] > check_int(lazylibrarian.CONFIG['MATCH_RATIO'], 90) \
                                    and result['book_fuzz'] > check_int(lazylibrarian.CONFIG['MATCH_RATIO'], 90):
                                logger.info("Found (%s%% %s%%) %s: %s" % (result['author_fuzz'], result['book_fuzz'],
                                                                          result['authorname'], result['bookname']))
                                bookmatch = result
                                break
                if bookmatch:
                    import_book(bookmatch['bookid'], ebook_status, audio_status,
                                reason="Added from wishlist %s" % book["dispname"])
                    new_books += 1
                    newValueDict = {"Requester": book["dispname"] + ' ', "AudioRequester": book["dispname"] + ' '}
                    controlValueDict = {"BookID": bookmatch['bookid']}
                    myDB.upsert("books", newValueDict, controlValueDict)
                else:
                    msg = "Skipping book %s by %s" % (book['rss_title'], book['rss_author'])
                    if not results:
                        msg += ', No results returned'
                        logger.warn(msg)
                    else:
                        msg += ', No match found'
                        logger.warn(msg)
                        logger.warn("Closest match (%s%% %s%%) %s: %s" % (results[0]['author_fuzz'],
                                                                          results[0]['book_fuzz'],
                                                                          results[0]['authorname'],
                                                                          results[0]['bookname']))
        if new_books:
            logger.info("Wishlist marked %s book%s as Wanted" % (new_books, plural(new_books)))
        else:
            logger.debug("Wishlist marked no new books as Wanted")
        myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})

    except Exception:
        logger.error('Unhandled exception in search_wishlist: %s' % traceback.format_exc())
    finally:
        threading.currentThread().name = "WEBSERVER"


# noinspection PyBroadException
def search_rss_book(books=None, library=None):
    """
    books is a list of new books to add, or None for backlog search
    library is "eBook" or "AudioBook" or None to search all book types
    """
    if not (lazylibrarian.USE_RSS()):
        logger.warn('RSS search is disabled')
        scheduleJob(action='Stop', target='search_rss_book')
        return
    try:
        threadname = threading.currentThread().name
        if "Thread-" in threadname:
            if not books:
                threading.currentThread().name = "SEARCHALLRSS"
            else:
                threading.currentThread().name = "SEARCHRSS"

        myDB = database.DBConnection()

        searchbooks = []
        if not books:
            # We are performing a backlog search
            cmd = 'SELECT BookID, AuthorName, Bookname, BookSub, BookAdded, books.Status, AudioStatus '
            cmd += 'from books,authors WHERE (books.Status="Wanted" OR AudioStatus="Wanted") '
            cmd += 'and books.AuthorID = authors.AuthorID order by BookAdded desc'
            results = myDB.select(cmd)
            for terms in results:
                searchbooks.append(terms)
        else:
            # The user has added a new book
            for book in books:
                cmd = 'SELECT BookID, AuthorName, BookName, BookSub, books.Status, AudioStatus '
                cmd += 'from books,authors WHERE BookID=? AND books.AuthorID = authors.AuthorID'
                results = myDB.select(cmd, (book['bookid'],))
                for terms in results:
                    searchbooks.append(terms)

        if len(searchbooks) == 0:
            logger.debug("SearchRSS - No books to search for")
            return

        resultlist, nproviders, _ = IterateOverRSSSites()
        if not nproviders:
            logger.warn('No rss providers are available')
            scheduleJob(action='Stop', target='search_rss_book')
            return  # No point in continuing

        logger.info('RSS Searching for %i book%s' % (len(searchbooks), plural(len(searchbooks))))

        searchlist = []
        for searchbook in searchbooks:
            # searchterm is only used for display purposes
            searchterm = searchbook['AuthorName'] + ' ' + searchbook['BookName']
            if searchbook['BookSub']:
                searchterm = searchterm + ': ' + searchbook['BookSub']

            if library is None or library == 'eBook':
                if searchbook['Status'] == "Wanted":
                    cmd = 'SELECT BookID from wanted WHERE BookID=? and AuxInfo="eBook" and Status="Snatched"'
                    snatched = myDB.match(cmd, (searchbook["BookID"],))
                    if snatched:
                        logger.warn('eBook %s %s already marked snatched in wanted table' %
                                    (searchbook['AuthorName'], searchbook['BookName']))
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
                    cmd = 'SELECT BookID from wanted WHERE BookID=? and AuxInfo="AudioBook" and Status="Snatched"'
                    snatched = myDB.match(cmd, (searchbook["BookID"],))
                    if snatched:
                        logger.warn('AudioBook %s %s already marked snatched in wanted table' %
                                    (searchbook['AuthorName'], searchbook['BookName']))
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
            if book['library'] == 'AudioBook':
                searchtype = 'audio'
            else:
                searchtype = 'book'
            found = processResultList(resultlist, book, searchtype, 'rss')

            # if you can't find the book, try title without any "(extended details, series etc)"
            if not found and '(' in book['bookName']:  # anything to shorten?
                searchtype = 'short' + searchtype
                found = processResultList(resultlist, book, searchtype, 'rss')

            if not found:
                logger.info("RSS Searches for %s %s returned no results." % (book['library'], book['searchterm']))
            if found > 1:
                rss_count += 1

        logger.info("RSS Search for Wanted items complete, found %s book%s" % (rss_count, plural(rss_count)))
        myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})

    except Exception:
        logger.error('Unhandled exception in search_rss_book: %s' % traceback.format_exc())
    finally:
        threading.currentThread().name = "WEBSERVER"
