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


import threading
import traceback
import time
import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.formatter import plural, check_int, thread_name
from lazylibrarian.providers import iterate_over_newznab_sites, iterate_over_torrent_sites, iterate_over_rss_sites, \
    iterate_over_direct_sites, iterate_over_irc_sites
from lazylibrarian.resultlist import find_best_result, download_result


def cron_search_book():
    if 'SEARCHALLBOOKS' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_book()
    else:
        logger.debug("SEARCHALLBOOKS is already running")


def good_enough(match):
    if match and int(match[0]) >= lazylibrarian.CONFIG.get_int('MATCH_RATIO'):
        return True
    return False


def warn_mode(mode):
    # don't nag. Show warning messages no more than every 20 mins
    timenow = int(time.time())
    if mode == 'rss':
        if check_int(lazylibrarian.TIMERS['NO_RSS_MSG'], 0) + 1200 < timenow:
            lazylibrarian.TIMERS['NO_RSS_MSG'] = timenow
        else:
            return
    elif mode == 'nzb':
        if check_int(lazylibrarian.TIMERS['NO_NZB_MSG'], 0) + 1200 < timenow:
            lazylibrarian.TIMERS['NO_NZB_MSG'] = timenow
        else:
            return
    elif mode == 'tor':
        if check_int(lazylibrarian.TIMERS['NO_TOR_MSG'], 0) + 1200 < timenow:
            lazylibrarian.TIMERS['NO_TOR_MSG'] = timenow
        else:
            return
    elif mode == 'irc':
        if check_int(lazylibrarian.TIMERS['NO_IRC_MSG'], 0) + 1200 < timenow:
            lazylibrarian.TIMERS['NO_IRC_MSG'] = timenow
        else:
            return
    elif mode == 'direct':
        if check_int(lazylibrarian.TIMERS['NO_DIRECT_MSG'], 0) + 1200 < timenow:
            lazylibrarian.TIMERS['NO_DIRECT_MSG'] = timenow
        else:
            return
    else:
        return
    logger.warn('No %s providers are available. Check config and blocklist' % mode)


def search_book(books=None, library=None):
    """
    books is a list of new books to add, or None for backlog search
    library is "eBook" or "AudioBook" or None to search all book types
    """
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        threadname = thread_name()
        if 'SEARCHALL' in threadname or 'API-SEARCH' in threadname or 'FORCE-SEARCH' in threadname:
            force = True
        else:
            force = False

        if "Thread-" in threadname:
            if not books:
                thread_name("SEARCHALLBOOKS")
                threadname = "SEARCHALLBOOKS"
            else:
                thread_name("SEARCHBOOKS")

        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        searchlist = []
        searchbooks = []

        if not books:
            # We are performing a backlog search
            cmd = 'SELECT BookID, AuthorName, Bookname, BookSub, BookAdded, books.Status, AudioStatus '
            cmd += 'from books,authors WHERE (books.Status="Wanted" OR AudioStatus="Wanted") '
            cmd += 'and books.AuthorID = authors.AuthorID order by BookAdded desc'
            results = db.select(cmd)
            for terms in results:
                searchbooks.append(terms)
        else:
            # The user has added new books
            if library:
                logger.debug("Searching for %s %s" % (len(books), plural(len(books), library)))
            for book in books:
                if not book['bookid'] in ['booklang', 'library', 'ignored']:
                    cmd = 'SELECT BookID, AuthorName, BookName, BookSub, books.Status, AudioStatus '
                    cmd += 'from books,authors WHERE BookID=? AND books.AuthorID = authors.AuthorID'
                    results = db.select(cmd, (book['bookid'],))
                    if results:
                        for terms in results:
                            searchbooks.append(terms)
                    else:
                        logger.debug("SearchBooks - BookID %s is not in the database" % book['bookid'])

        if len(searchbooks) == 0:
            logger.debug("No books to search for")
            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            return

        nprov = lazylibrarian.CONFIG.total_active_providers()
        if nprov == 0:
            msg = "SearchBooks - No providers to search"
            blocked = len(lazylibrarian.PROVIDER_BLOCKLIST)
            if blocked:
                msg += " (there %s %s in blocklist)" % (plural(blocked, "is"), blocked)
            else:
                msg += " (check you have some enabled)"
            logger.debug(msg)
            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            return

        modelist = []
        if lazylibrarian.CONFIG.use_nzb():
            modelist.append('nzb')
        if lazylibrarian.CONFIG.use_tor():
            modelist.append('tor')
        if lazylibrarian.CONFIG.use_direct():
            modelist.append('direct')
        if lazylibrarian.CONFIG.use_rss():
            modelist.append('rss')
        if lazylibrarian.CONFIG.use_irc():
            modelist.append('irc')

        logger.info('Searching %s %s %s for %i %s' %
                    (nprov, plural(nprov, "provider"), str(modelist), len(searchbooks),
                     plural(len(searchbooks), library)))
        logger.info("Provider Blocklist contains %s %s" % (len(lazylibrarian.PROVIDER_BLOCKLIST),
                                                           plural(len(lazylibrarian.PROVIDER_BLOCKLIST), 'entry')))

        for searchbook in searchbooks:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLBOOKS":
                logger.debug("Aborting %s" % threadname)
                break

            # searchterm is only used for display purposes
            searchterm = ''
            if searchbook['AuthorName']:
                searchterm = searchbook['AuthorName']
            else:
                logger.warn("No AuthorName for %s" % searchbook['BookID'])

            if searchbook['BookName']:
                if len(searchterm):
                    searchterm += ' '
                searchterm += searchbook['BookName']
            else:
                logger.warn("No BookName for %s" % searchbook['BookID'])

            if searchbook['BookSub']:
                if len(searchterm):
                    searchterm += ': '
                searchterm += searchbook['BookSub']

            if searchbook['Status'] == "Wanted":
                cmd = 'SELECT BookID from wanted WHERE BookID=? and AuxInfo="eBook" and Status="Snatched"'
                snatched = db.match(cmd, (searchbook["BookID"],))
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

            if searchbook['AudioStatus'] == "Wanted":
                cmd = 'SELECT BookID from wanted WHERE BookID=? and AuxInfo="AudioBook" and Status="Snatched"'
                snatched = db.match(cmd, (searchbook["BookID"],))
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

        # only get rss results once per run, as they are not search specific
        rss_resultlist = None
        if lazylibrarian.CONFIG.use_rss():
            rss_resultlist, nprov, dltypes = iterate_over_rss_sites()
            if not nprov or (library == 'Audiobook' and 'A' not in dltypes) or \
                            (library == 'eBook' and 'E' not in dltypes) or \
                            (library is None and ('E' in dltypes or 'A' in dltypes)):
                warn_mode('rss')

        book_count = 0
        for book in searchlist:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLBOOKS":
                logger.debug("Aborting %s" % threadname)
                break
            do_search = True
            if lazylibrarian.CONFIG.get_bool('DELAYSEARCH') and not force:
                res = db.match('SELECT * FROM failedsearch WHERE BookID=? AND Library=?',
                               (book['bookid'], book['library']))
                if not res:
                    logger.debug("SearchDelay: %s %s has not failed before" % (book['library'], book['bookid']))
                else:
                    skipped = check_int(res['Count'], 0)
                    interval = check_int(res['Interval'], 0)
                    if skipped < interval:
                        logger.debug("SearchDelay: %s %s not due (%d/%d)" %
                                     (book['library'], book['bookid'], skipped, interval))
                        db.action("UPDATE failedsearch SET Count=? WHERE BookID=? AND Library=?",
                                  (skipped + 1, book['bookid'], book['library']))
                        do_search = False
                    else:
                        logger.debug("SearchDelay: %s %s due this time (%d/%d)" %
                                     (book['library'], book['bookid'], skipped, interval))

            matches = []
            if do_search:
                # first attempt, try author/title in category "book"
                if book['library'] == 'AudioBook':
                    searchtype = 'audio'
                else:
                    searchtype = 'book'

                if lazylibrarian.CONFIG.use_nzb():
                    resultlist, nprov = iterate_over_newznab_sites(book, searchtype)
                    if not nprov:
                        warn_mode('nzb')
                    elif resultlist:
                        match = find_best_result(resultlist, book, searchtype, 'nzb')
                        if not good_enough(match):
                            logger.info("NZB search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found NZB result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                if lazylibrarian.CONFIG.use_tor():
                    resultlist, nprov = iterate_over_torrent_sites(book, searchtype)
                    if not nprov:
                        warn_mode('tor')
                    elif resultlist:
                        match = find_best_result(resultlist, book, searchtype, 'tor')
                        if not good_enough(match):
                            logger.info("Torrent search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found Torrent result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                if lazylibrarian.CONFIG.use_direct():
                    resultlist, nprov = iterate_over_direct_sites(book, searchtype)
                    if not nprov:
                        warn_mode('direct')
                    elif resultlist:
                        match = find_best_result(resultlist, book, searchtype, 'direct')
                        if not good_enough(match):
                            logger.info("Direct search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found Direct result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                if lazylibrarian.CONFIG.use_irc():
                    resultlist, nprov = iterate_over_irc_sites(book, searchtype)
                    if not nprov:
                        warn_mode('irc')
                    elif resultlist:
                        match = find_best_result(resultlist, book, searchtype, 'irc')
                        if not good_enough(match):
                            logger.info("IRC search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found IRC result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                if lazylibrarian.CONFIG.use_rss() and rss_resultlist:
                    match = find_best_result(rss_resultlist, book, searchtype, 'rss')
                    if not good_enough(match):
                        logger.info("RSS search for %s %s returned no results." %
                                    (book['library'], book['searchterm']))
                    else:
                        logger.info("Found RSS result: %s %s%%, %s priority %s" %
                                    (searchtype, match[0], match[1]['NZBprov'], match[3]))
                        matches.append(match)

                # if you can't find the book, try author/title without any "(extended details, series etc)"
                if not matches and '(' in book['bookName']:
                    if lazylibrarian.CONFIG.use_nzb():
                        resultlist, nprov = iterate_over_newznab_sites(book, 'short' + searchtype)
                        if not nprov:
                            warn_mode('nzb')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'nzb')
                            if not good_enough(match):
                                logger.info("NZB short search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found NZB result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_tor():
                        resultlist, nprov = iterate_over_torrent_sites(book, 'short' + searchtype)
                        if not nprov:
                            warn_mode('tor')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'tor')
                            if not good_enough(match):
                                logger.info("Torrent short search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found Torrent result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_direct():
                        resultlist, nprov = iterate_over_direct_sites(book, 'short' + searchtype)
                        if not nprov:
                            warn_mode('direct')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'direct')
                            if not good_enough(match):
                                logger.info("Direct short search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found Direct result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_irc():
                        resultlist, nprov = iterate_over_irc_sites(book, 'short' + searchtype)
                        if not nprov:
                            warn_mode('irc')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'irc')
                            if not good_enough(match):
                                logger.info("IRC short search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found IRC result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_rss() and rss_resultlist:
                        match = find_best_result(rss_resultlist, book, searchtype, 'rss')
                        if not good_enough(match):
                            logger.info("RSS short search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found RSS result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                # if you can't find the book under "books", you might find under general search
                # general search is the same as booksearch for torrents, irc and rss, no need to check again
                if not matches and lazylibrarian.CONFIG.use_nzb():
                    resultlist, nprov = iterate_over_newznab_sites(book, 'general' + searchtype)
                    if not nprov:
                        warn_mode('nzb')
                    elif resultlist:
                        match = find_best_result(resultlist, book, searchtype, 'nzb')
                        if not good_enough(match):
                            logger.info("NZB general search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found NZB result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                # if still not found, try general search again without any "(extended details, series etc)"
                # shortgeneral is the same as shortbook for torrents, irc and rss, no need to check again
                if not matches and lazylibrarian.CONFIG.use_nzb() and '(' in book['searchterm']:
                    resultlist, nprov = iterate_over_newznab_sites(book, 'shortgeneral' + searchtype)
                    if not nprov:
                        warn_mode('nzb')
                    elif resultlist:
                        match = find_best_result(resultlist, book, searchtype, 'nzb')
                        if not good_enough(match):
                            logger.info("NZB shortgeneral search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found NZB result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

                # if still not found, try general search again with title only
                if not matches:
                    if lazylibrarian.CONFIG.use_nzb():
                        resultlist, nprov = iterate_over_newznab_sites(book, 'title' + searchtype)
                        if not nprov:
                            warn_mode('nzb')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'nzb')
                            if not good_enough(match):
                                logger.info("NZB title search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found NZB result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_tor():
                        resultlist, nprov = iterate_over_torrent_sites(book, 'title' + searchtype)
                        if not nprov:
                            warn_mode('tor')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'tor')
                            if not good_enough(match):
                                logger.info("Torrent title search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found Torrent result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_direct():
                        resultlist, nprov = iterate_over_direct_sites(book, 'title' + searchtype)
                        if not nprov:
                            warn_mode('direct')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'direct')
                            if not good_enough(match):
                                logger.info("Direct title search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found Direct result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    # irchighway says search results without both author and title will be
                    # silently rejected but that doesn't seem to be actioned...
                    if lazylibrarian.CONFIG.use_irc():
                        resultlist, nprov = iterate_over_irc_sites(book, 'title' + searchtype)
                        if not nprov:
                            warn_mode('irc')
                        elif resultlist:
                            match = find_best_result(resultlist, book, searchtype, 'irc')
                            if not good_enough(match):
                                logger.info("IRC title search for %s %s returned no results." %
                                            (book['library'], book['searchterm']))
                            else:
                                logger.info("Found IRC result: %s %s%%, %s priority %s" %
                                            (searchtype, match[0], match[1]['NZBprov'], match[3]))
                                matches.append(match)

                    if lazylibrarian.CONFIG.use_rss():
                        match = find_best_result(rss_resultlist, book, searchtype, 'rss')
                        if not good_enough(match):
                            logger.info("RSS title search for %s %s returned no results." %
                                        (book['library'], book['searchterm']))
                        else:
                            logger.info("Found RSS result: %s %s%%, %s priority %s" %
                                        (searchtype, match[0], match[1]['NZBprov'], match[3]))
                            matches.append(match)

            if matches:
                highest = max(matches, key=lambda s: (s[0], s[3]))  # sort on percentage and priority
                logger.info("Requesting %s download: %s%% %s: %s" %
                            (book['library'], highest[0], highest[1]['NZBprov'], highest[1]['NZBtitle']))
                if download_result(highest, book) > 1:
                    book_count += 1  # we found it
                db.action("DELETE from failedsearch WHERE BookID=? AND Library=?", (book['bookid'], book['library']))
            elif lazylibrarian.CONFIG.get_bool('DELAYSEARCH') and not force and do_search and len(modelist):
                res = db.match('SELECT * FROM failedsearch WHERE BookID=? AND Library=?',
                               (book['bookid'], book['library']))
                if res:
                    interval = check_int(res['Interval'], 0)
                else:
                    interval = 0

                db.upsert("failedsearch",
                          {'Count': 0, 'Interval': interval + 1, 'Time': time.time()},
                          {'BookID': book['bookid'], 'Library': book['library']})

            time.sleep(lazylibrarian.CONFIG.get_int('SEARCH_RATELIMIT'))

        logger.info("Search for Wanted items complete, found %s %s" % (book_count, plural(book_count, "book")))

    except Exception:
        logger.error('Unhandled exception in search_book: %s' % traceback.format_exc())
    finally:
        db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
        thread_name("WEBSERVER")
