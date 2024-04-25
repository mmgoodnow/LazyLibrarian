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
import string
import threading
import time
import traceback
from operator import itemgetter
from queue import Queue

import lazylibrarian
from lazylibrarian import database
from lazylibrarian.cache import cache_img, ImageType
from lazylibrarian.config2 import CONFIG
from lazylibrarian.formatter import today, unaccented, format_author_name, make_unicode, \
    get_list, check_int, thread_name, plural
from lazylibrarian.gb import GoogleBooks
from lazylibrarian.gr import GoodReads
from lazylibrarian.grsync import grfollow
from lazylibrarian.images import get_author_image
from lazylibrarian.ol import OpenLibrary
from lazylibrarian.processcontrol import get_info_on_caller
from thefuzz import fuzz


def is_valid_authorid(authorid: str, api=None) -> bool:
    if not authorid or not isinstance(authorid, str):
        return False  # Reject blank, or non-string
    if api is None:
        api = CONFIG['BOOK_API']
    # GoogleBooks doesn't provide authorid so we use one of the other sources
    if authorid.isdigit() and api in ['GoodReads', 'GoogleBooks']:
        return True
    if authorid.startswith('OL') and authorid.endswith('A') and api in ['OpenLibrary', 'GoogleBooks']:
        return True
    return False


def get_preferred_author_name(author: str) -> (str, bool):
    # Look up an authorname in the database, if not found try fuzzy match
    # Return possibly changed authorname and whether found in library
    logger = logging.getLogger(__name__)
    author = format_author_name(author, postfix=get_list(CONFIG.get_csv('NAME_POSTFIX')))
    match = False
    db = database.DBConnection()
    try:
        check_exist_author = db.match('SELECT * FROM authors where AuthorName=?', (author,))
        if check_exist_author:
            match = True
        else:  # If no exact match, look for a close fuzzy match to handle misspellings, accents or AKA
            match_name = author.lower().replace('.', '')
            res = db.action('select AuthorID,AuthorName,AKA from authors')
            for item in res:
                aname = item['AuthorName']
                if aname:
                    match_fuzz = fuzz.ratio(aname.lower().replace('.', ''), match_name)
                    if match_fuzz >= CONFIG.get_int('NAME_RATIO'):
                        logger.debug("Fuzzy match [%s] %s%% for [%s]" % (item['AuthorName'], match_fuzz, author))
                        author = item['AuthorName']
                        match = True
                        break
                aka = item['AKA']
                if aka:
                    match_fuzz = fuzz.ratio(aka.lower().replace('.', ''), match_name)
                    if match_fuzz >= CONFIG.get_int('NAME_RATIO'):
                        logger.debug("Fuzzy match [%s] %s%% for [%s]" % (item['AKA'], match_fuzz, author))
                        author = item['AuthorName']
                        match = True
                        break
    finally:
        db.close()
    return author, match


def add_author_name_to_db(author=None, refresh=False, addbooks=None, reason=None, title=None):
    # get authors name in a consistent format, look them up in the database
    # if not in database, try to import them.
    # return authorname,authorid,new where new=False if author already in db, new=True if added
    # authorname returned is our preferred name, or empty string if not found or unable to add
    logger = logging.getLogger(__name__)
    if not reason:
        program, method, lineno = get_info_on_caller(depth=1)
        if lineno > 0:
            reason = "%s:%s:%s" % (program, method, lineno)
        else:
            reason = 'Unknown reason in add_author_name_to_db'

    if addbooks is None:  # we get passed True/False or None
        addbooks = CONFIG.get_bool('NEWAUTHOR_BOOKS')

    new = False
    author_info = {}
    if not author or len(author) < 2 or 'unknown' in author.lower() or 'anonymous' in author.lower():
        logger.debug('Invalid Author Name [%s]' % author)
        return "", "", False

    db = database.DBConnection()
    try:
        # Check if the author exists, and import the author if not,
        author, exists = get_preferred_author_name(author)
        if exists:
            check_exist_author = db.match('SELECT * FROM authors where AuthorName=?', (author,))
        else:
            check_exist_author = None
        if not exists and (CONFIG.get_bool('ADD_AUTHOR') or reason.startswith('API')):
            logger.debug('Author %s not found in database, trying to add' % author)
            # no match for supplied author, but we're allowed to add new ones
            if CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
                if title:
                    ol = OpenLibrary(author + '<ll>' + title)
                else:
                    ol = OpenLibrary(author)
                try:
                    author_info = ol.find_author_id()
                except Exception as e:
                    logger.warning("%s finding author id for [%s] %s" % (type(e).__name__, author, str(e)))
                    return "", "", False
            else:
                gr = GoodReads(author)
                try:
                    author_info = gr.find_author_id()
                except Exception as e:
                    logger.warning("%s finding author id for [%s] %s" % (type(e).__name__, author, str(e)))
                    return "", "", False

            # only try to add if data matches found author data
            if author_info:
                authorname = author_info['authorname']
                # "J.R.R. Tolkien" is the same person as "J. R. R. Tolkien" and "J R R Tolkien"
                match_auth = author.replace('.', ' ')
                match_auth = ' '.join(match_auth.split())

                match_name = authorname.replace('.', ' ')
                match_name = ' '.join(match_name.split())

                match_name = unaccented(match_name, only_ascii=False)
                match_auth = unaccented(match_auth, only_ascii=False)

                # allow a degree of fuzziness to cater for different accented character handling.
                # filename may have the accented or un-accented version of the character
                # We stored GoodReads/OpenLibrary author name in author_info, so store in LL db under that
                # fuzz.ratio doesn't lowercase for us
                match_fuzz = fuzz.ratio(match_auth.lower(), match_name.lower())
                if match_fuzz < CONFIG.get_int('NAME_RATIO'):
                    logger.debug("Failed to match author [%s] to authorname [%s] fuzz [%d]" %
                                 (author, match_name, match_fuzz))

                # To save loading hundreds of books by unknown authors at GR or GB, ignore unknown
                if "unknown" not in author.lower() and 'anonymous' not in author.lower() and \
                        match_fuzz >= CONFIG.get_int('NAME_RATIO'):
                    # use "intact" name for author that we stored in
                    # author_dict, not one of the various mangled versions
                    # otherwise the books appear to be by a different author!
                    author = author_info['authorname']
                    authorid = author_info['authorid']
                    # this new authorname may already be in the
                    # database, so check again
                    check_exist_author = db.match('SELECT * FROM authors where AuthorID=?', (authorid,))
                    if check_exist_author:
                        logger.debug('Found authorname %s in database' % author)
                        new = False
                    else:
                        logger.info("Adding new author [%s] %s addbooks=%s" % (author, reason, addbooks))
                        try:
                            add_author_to_db(authorname=author, refresh=refresh, authorid=authorid, addbooks=addbooks,
                                             reason=reason)
                            check_exist_author = db.match('SELECT * FROM authors where AuthorID=?', (authorid,))
                            if check_exist_author:
                                new = True
                        except Exception as e:
                            logger.error('Failed to add author [%s] to db: %s %s' % (author, type(e).__name__, str(e)))

        # check author exists in db, either newly loaded or already there
        if check_exist_author:
            aka = author_info.get('aka', '')
            akas = get_list(check_exist_author['AKA'], ',')
            if aka and aka not in akas:
                akas.append(aka)
                db.action("UPDATE authors SET AKA=? WHERE AuthorID=?",
                          (', '.join(akas), check_exist_author['AuthorID']))
        else:
            logger.debug("Failed to match author [%s] in database" % author)
            return "", "", False
    finally:
        db.close()
    author = make_unicode(author)
    return author, check_exist_author['AuthorID'], new


def get_all_author_details(authorid=None, authorname=None):
    # fetch as much data as you can on an author using all configured sources
    #
    logger = logging.getLogger(__name__)
    author = {}
    ol_id = None
    gr_id = None
    gr_name = ''
    ol_name = ''
    ol_author = {}
    gr_author = {}

    if authorid.startswith('OL'):
        ol_id = authorid
        ol = OpenLibrary(ol_id)
        ol_author = ol.get_author_info(authorid=ol_id)
        if not authorname:
            authorname = ol_author['authorname']
    elif CONFIG['GR_API']:
        gr_id = authorid
        gr = GoodReads(gr_id)
        gr_author = gr.get_author_info(authorid=gr_id)
        if not authorname:
            authorname = gr_author['authorname']

    if not ol_id and 'unknown' not in authorname and 'anonymous' not in authorname:
        ol = OpenLibrary(authorname)
        ol_author = ol.find_author_id()
        if ol_author:
            ol_id = ol_author['authorid']
            ol_author = ol.get_author_info(authorid=ol_id)

    if not gr_id and CONFIG['GR_API'] and 'unknown' not in authorname and 'anonymous' not in authorname:
        gr = GoodReads(authorname)
        gr_author = gr.find_author_id()
        if gr_author:
            gr_id = gr_author['authorid']
            gr_author = gr.get_author_info(authorid=gr_id)

    if ol_author:
        ol_author['ol_id'] = ol_author['authorid']
        ol_name = ol_author['authorname']
        for item in ol_author:
            if not author.get(item):  # if key doesn't exist or value empty
                author[item] = ol_author[item]
    if gr_author:
        gr_author['gr_id'] = gr_author['authorid']
        gr_name = gr_author['authorname']
        for item in gr_author:
            if not author.get(item):
                author[item] = gr_author[item]

    # which id do we prefer
    if ol_author and CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
        author['authorid'] = author['ol_id']
        author['authorname'] = ol_name
    elif gr_author:
        author['authorid'] = author['gr_id']
        author['authorname'] = gr_name
    else:
        author = {}

    if author:
        akas = []
        if gr_name and ol_name and gr_name != ol_name:
            if author.get('AKA'):
                akas = get_list(author.get('AKA', ''), ',')
            if author['authorname'] != gr_name and gr_name not in akas:
                logger.warning("Conflicting goodreads authorname for %s [%s][%s] setting AKA" %
                               (author['authorid'], author['authorname'], gr_name))
                akas.append(gr_name)
            if author['authorname'] != ol_name and ol_name not in akas:
                logger.warning("Conflicting openlibrary authorname for %s [%s][%s] setting AKA" %
                               (author['authorid'], author['authorname'], ol_name))
                akas.append(ol_name)
            author['AKA'] = ', '.join(akas)
    return author


def add_author_to_db(authorname=None, refresh=False, authorid=None, addbooks=True, reason=None):
    """
    Add an author to the database by name or id, and optionally get a list of all their books
    If author already exists in database, refresh their details and optionally booklist
    Returns the author ID
    """
    logger = logging.getLogger(__name__)
    if not reason:
        program, method, lineno = get_info_on_caller(depth=1)
        if lineno > 0:
            reason = "%s:%s:%s" % (program, method, lineno)
        else:
            reason = "Unknown reason in add_author_to_db"

    threadname = thread_name()
    if "Thread-" in threadname:
        thread_name("AddAuthorToDB")
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        new_author = True
        dbauthor = db.match("SELECT * from authors WHERE AuthorID=?", (authorid,))
        if dbauthor:
            new_author = False
        elif authorname and 'unknown' not in authorname and 'anonymous' not in authorname:
            dbauthor = db.match("SELECT * from authors WHERE AuthorName=?", (authorname,))
            if dbauthor:
                new_author = False

        if new_author or refresh:
            current_author = get_all_author_details(authorid, authorname)
        else:
            current_author = dict(dbauthor)

        if new_author:
            current_author['manual'] = False
            current_author['status'] = CONFIG['NEWAUTHOR_STATUS']
        else:
            current_author['manual'] = dbauthor['manual']
            current_author['status'] = dbauthor['status']

        if not current_author or not current_author.get('authorid'):
            msg = "No author info for %s:%s" % (authorid, authorname)
            # goodreads sometimes changes authorid
            # maybe change of provider or no reply from provider
            logger.error(msg)
            return msg

        if authorid and current_author['authorid'] != authorid:
            logger.warning("Conflicting authorid for %s (new:%s old:%s) Changing to new authorid" %
                           (current_author['authorname'], current_author['authorid'], authorid))
            db.action("PRAGMA foreign_keys = OFF")
            db.action('UPDATE books SET AuthorID=? WHERE AuthorID=?',
                      (current_author['authorid'], authorid))
            db.action('UPDATE seriesauthors SET AuthorID=? WHERE AuthorID=?',
                      (current_author['authorid'], authorid), suppress='UNIQUE')
            if current_author['authorid'].startswith('OL'):
                db.action('UPDATE authors SET AuthorID=?,ol_id=? WHERE AuthorID=?',
                          (current_author['authorid'], current_author['ol_id'], authorid), suppress='UNIQUE')
            else:
                db.action('UPDATE authors SET AuthorID=?,gr_id=? WHERE AuthorID=?',
                          (current_author['authorid'], current_author['gr_id'], authorid), suppress='UNIQUE')
            db.action("PRAGMA foreign_keys = ON")

        if authorname and current_author['authorname'] != authorname:
            dbauthor = db.match("SELECT * from authors WHERE AuthorName=?", (current_author['authorname'],))
            if dbauthor:
                logger.warning("Authorname %s already exists with id %s" % (current_author['authorname'],
                                                                            dbauthor['authorID']))
                current_author['authorid'] = dbauthor['authorid']
            else:
                logger.warning("Updating authorname for %s (new:%s old:%s)" % (current_author['authorid'],
                                                                               current_author['authorname'],
                                                                               authorname))
                db.action('UPDATE authors SET AuthorName=? WHERE AuthorID=?',
                          (current_author['authorname'], current_author['authorid']))

        control_value_dict = {"AuthorID": current_author['authorid']}
        if new_author or not current_author['manual']:
            db.upsert("authors", current_author, control_value_dict)

        entry_status = current_author['status']
        new_value_dict = {
                            "Status": "Loading",
                            "Updated": int(time.time())
                        }
        if new_author:
            new_value_dict["AuthorImg"] = "images/nophoto.png"
            new_value_dict['Reason'] = reason
            new_value_dict['DateAdded'] = today()
            refresh = True
            logger.debug("Adding new author id %s (%s) to database %s, Addbooks=%s" %
                         (current_author['authorid'], current_author['authorname'], reason, addbooks))
        else:
            logger.debug("Updating author %s (%s) %s" % (current_author['authorid'],
                                                         current_author['authorname'], entry_status))
        db.upsert("authors", new_value_dict, control_value_dict)

        # if author is set to manual, should we allow replacing 'nophoto' ?
        new_img = False
        authorimg = current_author.get('authorimg')
        if authorimg and 'nophoto' in authorimg:
            newimg = get_author_image(current_author['authorid'])
            if newimg:
                authorimg = newimg
                new_img = True

            # allow caching new image
            if authorimg and authorimg.startswith('http'):
                newimg, success, _ = cache_img(ImageType.AUTHOR, authorid, authorimg, refresh=refresh)
                if success:
                    authorimg = newimg
                    new_img = True
                else:
                    logger.debug('Failed to cache image for %s (%s)' % (authorimg, newimg))

            if new_img:
                db.action("UPDATE authors SET AuthorIMG=? WHERE AuthorID=?", (authorimg, current_author['authorid']))

        if not current_author['manual'] and addbooks:
            if new_author:
                bookstatus = CONFIG['NEWAUTHOR_STATUS']
                audiostatus = CONFIG['NEWAUTHOR_AUDIO']
            else:
                bookstatus = CONFIG['NEWBOOK_STATUS']
                audiostatus = CONFIG['NEWAUDIO_STATUS']

            if entry_status not in ['Active', 'Wanted', 'Ignored', 'Paused']:
                entry_status = 'Active'  # default for invalid/unknown or "loading"
            if entry_status not in ['Ignored', 'Paused']:
                # process books
                if CONFIG['BOOK_API'] == "GoogleBooks" and CONFIG['GB_API']:
                    book_api = GoogleBooks()
                    book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                              bookstatus=bookstatus,
                                              audiostatus=audiostatus, entrystatus=entry_status,
                                              refresh=refresh, reason=reason)
                    if CONFIG.get_bool('MULTI_SOURCE'):
                        book_api = OpenLibrary()
                        book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                                  bookstatus=bookstatus,
                                                  audiostatus=audiostatus, entrystatus=entry_status,
                                                  refresh=refresh, reason=reason)
                        if CONFIG['GR_API']:
                            book_api = GoodReads(current_author['authorname'])
                            book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                                      bookstatus=bookstatus,
                                                      audiostatus=audiostatus, entrystatus=entry_status,
                                                      refresh=refresh, reason=reason)
                elif CONFIG['BOOK_API'] == "GoodReads" and CONFIG['GR_API']:
                    book_api = GoodReads(current_author['authorname'])
                    book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                              bookstatus=bookstatus,
                                              audiostatus=audiostatus, entrystatus=entry_status,
                                              refresh=refresh, reason=reason)
                    if CONFIG.get_bool('MULTI_SOURCE'):
                        book_api = OpenLibrary()
                        book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                                  bookstatus=bookstatus,
                                                  audiostatus=audiostatus, entrystatus=entry_status,
                                                  refresh=refresh, reason=reason)
                        if CONFIG['GB_API']:
                            book_api = GoogleBooks()
                            book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                                      bookstatus=bookstatus,
                                                      audiostatus=audiostatus, entrystatus=entry_status,
                                                      refresh=refresh, reason=reason)
                elif CONFIG['BOOK_API'] == "OpenLibrary":
                    book_api = OpenLibrary(current_author['authorname'])
                    book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                              bookstatus=bookstatus,
                                              audiostatus=audiostatus, entrystatus=entry_status,
                                              refresh=refresh, reason=reason)
                    if CONFIG.get_bool('MULTI_SOURCE'):
                        if CONFIG['GR_API']:
                            book_api = GoodReads()
                            book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                                      bookstatus=bookstatus,
                                                      audiostatus=audiostatus, entrystatus=entry_status,
                                                      refresh=refresh, reason=reason)
                        if CONFIG['GB_API']:
                            book_api = GoogleBooks()
                            book_api.get_author_books(current_author['authorid'], current_author['authorname'],
                                                      bookstatus=bookstatus,
                                                      audiostatus=audiostatus, entrystatus=entry_status,
                                                      refresh=refresh, reason=reason)
                de_duplicate(authorid)

            if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                msg = "[%s] Author update aborted, status %s" % (current_author['authorname'], entry_status)
                logger.debug(msg)
                return msg

            if new_author and CONFIG['GR_FOLLOWNEW']:
                res = grfollow(current_author['authorid'], True)
                if res.startswith('Unable'):
                    logger.warning(res)
                try:
                    followid = res.split("followid=")[1]
                    logger.debug('%s marked followed' % current_author['authorname'])
                except IndexError:
                    followid = ''
                db.action('UPDATE authors SET GRfollow=? WHERE AuthorID=?', (followid, current_author['authorid']))
        else:
            # if we're not loading any books and it's a new author,
            # mark author as paused in case it's a wishlist or a series contributor
            if new_author and not addbooks:
                entry_status = 'Paused'

        if current_author:
            update_totals(current_author['authorid'])
            db.action("UPDATE authors SET Status=? WHERE AuthorID=?", (entry_status,
                                                                       current_author['authorid']))
            msg = "%s [%s] Author update complete, status %s" % (current_author['authorid'],
                                                                 current_author['authorname'], entry_status)
            logger.info(msg)
            return current_author['authorid']
        else:
            msg = "Authorid %s (%s) not found in database" % (authorid, authorname)
            logger.warning(msg)
            return msg

    except Exception:
        msg = 'Unhandled exception in add_author_to_db: %s' % traceback.format_exc()
        logger.error(msg)
        return msg
    finally:
        db.close()


def collate_nopunctuation(string1, string2):
    # strip all punctuation so things like "it's" matches "its"
    str1 = string1.lower().translate(str.maketrans('', '', string.punctuation))
    str2 = string2.lower().translate(str.maketrans('', '', string.punctuation))
    if str1 < str2:
        return -1
    elif str1 > str2:
        return 1
    return 0


def de_duplicate(authorid):
    logger = logging.getLogger(__name__)
    db = database.DBConnection()
    db.connection.create_collation('nopunctuation', collate_nopunctuation)
    total = 0
    try:
        # check/delete any duplicate titles - exact match only
        cmd = ("select count('bookname'),bookname from books where authorid=? and "
               "( Status != 'Ignored' or AudioStatus != 'Ignored' ) group by bookname COLLATE NOPUNCTUATION "
               "having ( count(bookname) > 1 )")
        res = db.select(cmd, (authorid,))
        dupes = len(res)
        author = db.match("SELECT AuthorName from authors where AuthorID=?", (authorid,))
        if not dupes:
            logger.debug("No duplicates to merge")
        else:
            logger.warning("There %s %s duplicate %s for %s" % (plural(dupes, 'is'), dupes, plural(dupes, 'title'),
                                                                author['AuthorName']))
            for item in res:
                favourite = ''
                copies = db.select("SELECT * from books where AuthorID=? and BookName=? COLLATE NOPUNCTUATION",
                                   (authorid, item[1]))
                for copy in copies:
                    if (copy['Status'] in ['Open', 'Have', 'Wanted'] or
                            copy['AudioStatus'] in ['Open', 'Have', 'Wanted']):
                        favourite = copy
                        break
                if not favourite:
                    for copy in copies:
                        if copy['Status'] not in ['Ignored'] and copy['AudioStatus'] not in ['Ignored']:
                            favourite = copy
                            break
                if not favourite:
                    favourite = copies[0]
                logger.debug("Favourite %s %s %s %s" % (favourite['BookID'], favourite['BookName'], favourite['Status'],
                                                        favourite['AudioStatus']))
                for copy in copies:
                    if copy['BookID'] != favourite['BookID']:
                        for key in ['BookSub', 'BookDesc', 'BookGenre', 'BookIsbn', 'BookPub', 'BookRate',
                                    'BookImg', 'BookPages', 'BookLink', 'BookFile', 'BookDate', 'BookLang',
                                    'BookAdded', 'WorkPage', 'Manual', 'SeriesDisplay', 'BookLibrary',
                                    'AudioFile', 'AudioLibrary', 'WorkID', 'ScanResult',
                                    'OriginalPubDate', 'Requester', 'AudioRequester', 'LT_WorkID', 'Narrator']:
                            if not favourite[key] and copy[key]:
                                cmd = "UPDATE books SET %s=? WHERE BookID=?" % key
                                logger.debug("Copy %s from %s" % (key, copy['BookID']))
                                db.action(cmd, (copy[key], favourite['BookID']))
                                if key == 'BookFile' and favourite['Status'] not in ['Open', 'Have']:
                                    logger.debug("Copy Status from %s" % copy['BookID'])
                                    db.action('UPDATE books SET Status=? WHERE BookID=?',
                                              (copy['Status'], favourite['BookID']))
                                if key == 'AudioFile' and favourite['AudioStatus'] not in ['Open', 'Have']:
                                    logger.debug("Copy AudioStatus from %s" % copy['BookID'])
                                    db.action('UPDATE books SET AudioStatus=? WHERE BookID=?',
                                              (copy['AudioStatus'], favourite['BookID']))

                        logger.debug("Delete %s keeping %s" % (copy['BookID'], favourite['BookID']))
                        db.action('DELETE from books WHERE BookID=?', (copy['BookID'],))
                        total += 1
    finally:
        db.close()
    logger.info("Deleted %s duplicate %s for %s" % (total, plural(dupes, 'title'), author['AuthorName']))


def update_totals(authorid):
    logger = logging.getLogger(__name__)
    db = database.DBConnection()
    try:
        # author totals needs to be updated every time a book is marked differently
        match = db.select('SELECT AuthorID from authors WHERE AuthorID=?', (authorid,))
        if not match:
            logger.debug('Update_totals - authorid [%s] not found' % authorid)
            return

        cmd = ("SELECT BookName, BookLink, BookDate, BookID from books WHERE AuthorID=? and Status != 'Ignored' "
               "order by BookDate DESC")
        lastbook = db.match(cmd, (authorid,))

        cmd = ("select sum(case status when 'Ignored' then 0 else 1 end) as unignored,sum(case when status == 'Have' "
               "then 1 when status == 'Open' then 1 else 0 end) as EHave, sum(case when audiostatus == 'Have' "
               "then 1 when audiostatus == 'Open' then 1 else 0 end) as AHave, sum(case when status == 'Have' "
               "then 1 when status == 'Open' then 1 when audiostatus == 'Have' then 1 when audiostatus == 'Open' "
               "then 1 else 0 end) as Have, count(*) as total from books where authorid=?")
        totals = db.match(cmd, (authorid,))

        control_value_dict = {"AuthorID": authorid}
        new_value_dict = {
            "TotalBooks": check_int(totals['total'], 0),
            "UnignoredBooks": check_int(totals['unignored'], 0),
            "HaveBooks": check_int(totals['Have'], 0),
            "HaveEBooks": check_int(totals['EHave'], 0),
            "HaveAudioBooks": check_int(totals['AHave'], 0),
            "LastBook": lastbook['BookName'] if lastbook else None,
            "LastLink": lastbook['BookLink'] if lastbook else None,
            "LastBookID": lastbook['BookID'] if lastbook else None,
            "LastDate": lastbook['BookDate'] if lastbook else None
        }
        db.upsert("authors", new_value_dict, control_value_dict)

        cmd = ("select series.seriesid as Series,sum(case books.status when 'Ignored' then 0 else 1 end) "
               "as Total,sum(case when books.status == 'Have' then 1 when books.status == 'Open' then 1 "
               "when books.audiostatus == 'Have' then 1 when books.audiostatus == 'Open' then 1 else 0 end) "
               "as Have from books,member,series,seriesauthors where member.bookid=books.bookid and "
               "member.seriesid = series.seriesid and seriesauthors.seriesid = series.seriesid and "
               "seriesauthors.authorid=? group by series.seriesid")
        res = db.select(cmd, (authorid,))
        if len(res):
            for series in res:
                db.action('UPDATE series SET Have=?, Total=? WHERE SeriesID=?',
                          (check_int(series['Have'], 0), check_int(series['Total'], 0), series['Series']))

        res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
    finally:
        db.close()
    logger.debug('Updated totals for [%s] %s/%s' % (res['AuthorName'], new_value_dict['HaveBooks'],
                                                    new_value_dict['TotalBooks']))


def import_book(bookid, ebook=None, audio=None, wait=False, reason='importer.import_book'):
    """ search goodreads or googlebooks for a bookid and import the book
        ebook/audio=None makes find_book use configured default """
    logger = logging.getLogger(__name__)
    if CONFIG['BOOK_API'] == "GoogleBooks":
        gb = GoogleBooks(bookid)
        if not wait:
            threading.Thread(target=gb.find_book, name='GB-IMPORT', args=[bookid, ebook, audio, reason]).start()
        else:
            gb.find_book(bookid, ebook, audio, reason)
    elif CONFIG['BOOK_API'] == 'OpenLibrary':
        ol = OpenLibrary(bookid)
        logger.debug("bookstatus=%s, audiostatus=%s" % (ebook, audio))
        if not wait:
            threading.Thread(target=ol.find_book, name='OL-IMPORT', args=[bookid, ebook, audio, reason]).start()
        else:
            ol.find_book(bookid, ebook, audio, reason)
    else:
        gr = GoodReads(bookid)
        logger.debug("bookstatus=%s, audiostatus=%s" % (ebook, audio))
        if not wait:
            threading.Thread(target=gr.find_book, name='GR-IMPORT', args=[bookid, ebook, audio, reason]).start()
        else:
            gr.find_book(bookid, ebook, audio, reason)


def search_for(searchterm, source=None):
    """
        search openlibrary/goodreads/googlebooks for a searchterm, return a list of results
    """
    loggersearching = logging.getLogger('special.searching')
    if not source:
        source = CONFIG['BOOK_API']
    loggersearching.debug("%s %s" % (source, searchterm))
    if source == "GoogleBooks" and CONFIG['GB_API']:
        gb = GoogleBooks(searchterm)
        myqueue = Queue()
        search_api = threading.Thread(target=gb.find_results, name='GB-RESULTS', args=[searchterm, myqueue])
        search_api.start()
    elif source == "GoodReads" and CONFIG['GR_API']:
        myqueue = Queue()
        gr = GoodReads(searchterm)
        search_api = threading.Thread(target=gr.find_results, name='GR-RESULTS', args=[searchterm, myqueue])
        search_api.start()
    elif source == "OpenLibrary":
        myqueue = Queue()
        ol = OpenLibrary(searchterm)
        search_api = threading.Thread(target=ol.find_results, name='OL-RESULTS', args=[searchterm, myqueue])
        search_api.start()
    else:
        search_api = None
        myqueue = None

    if search_api:
        search_api.join()
        searchresults = myqueue.get()
        sortedlist = sorted(searchresults, key=itemgetter('highest_fuzz', 'bookrate_count'), reverse=True)
        loggersearching.debug(str(sortedlist))
        return sortedlist
    return []
