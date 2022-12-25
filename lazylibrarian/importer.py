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

import inspect
import os
import threading
import time
import traceback
from operator import itemgetter

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.cache import cache_img
from lazylibrarian.formatter import today, unaccented, format_author_name, make_unicode, \
    unaccented_bytes, get_list, check_int, thread_name
from lazylibrarian.gb import GoogleBooks
from lazylibrarian.gr import GoodReads
from lazylibrarian.grsync import grfollow
from lazylibrarian.images import get_author_image
from lazylibrarian.ol import OpenLibrary

from thefuzz import fuzz
from queue import Queue


def is_valid_authorid(authorid):
    if not authorid or not isinstance(authorid, str):
        return False # Reject blank, or non-string
    # GoogleBooks doesn't provide authorid so we use one of the other sources
    if authorid.isdigit() and lazylibrarian.CONFIG['BOOK_API'] in ['GoodReads', 'GoogleBooks']:
        return True
    if authorid.startswith('OL') and authorid.endswith('A') and \
            lazylibrarian.CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
        return True
    return False


def get_preferred_author_name(author):
    # Look up an authorname in the database, if not found try fuzzy match
    # Return possibly changed authorname and whether found in library
    author = format_author_name(author)
    match = False
    db = database.DBConnection()
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
                if match_fuzz >= lazylibrarian.CONFIG.get_int('NAME_RATIO'):
                    logger.debug("Fuzzy match [%s] %s%% for [%s]" % (item['AuthorName'], match_fuzz, author))
                    author = item['AuthorName']
                    match = True
                    break
            aka = item['AKA']
            if aka:
                match_fuzz = fuzz.ratio(aka.lower().replace('.', ''), match_name)
                if match_fuzz >= lazylibrarian.CONFIG.get_int('NAME_RATIO'):
                    logger.debug("Fuzzy match [%s] %s%% for [%s]" % (item['AKA'], match_fuzz, author))
                    author = item['AuthorName']
                    match = True
                    break
    return author, match


def add_author_name_to_db(author=None, refresh=False, addbooks=None, reason=None, title=None):
    # get authors name in a consistent format, look them up in the database
    # if not in database, try to import them.
    # return authorname,authorid,new where new=False if author already in db, new=True if added
    # authorname returned is our preferred name, or empty string if not found or unable to add
    if not reason:
        if len(inspect.stack()) > 2:
            frame = inspect.getframeinfo(inspect.stack()[2][0])
            program = os.path.basename(frame.filename)
            method = frame.function
            lineno = frame.lineno
            reason = "%s:%s:%s" % (program, method, lineno)
        else:
            reason = 'Unknown reason in add_author_name_to_db'

    if addbooks is None:  # we get passed True/False or None
        addbooks = lazylibrarian.CONFIG.get_bool('NEWAUTHOR_BOOKS')

    new = False
    author_info = {}
    if not author or len(author) < 2 or 'unknown' in author.lower() or 'anonymous' in author.lower():
        logger.debug('Invalid Author Name [%s]' % author)
        return "", "", False

    db = database.DBConnection()
    # Check if the author exists, and import the author if not,
    author, exists = get_preferred_author_name(author)
    if exists:
        check_exist_author = db.match('SELECT * FROM authors where AuthorName=?', (author,))
    else:
        check_exist_author = None
    if not exists and (lazylibrarian.CONFIG.get_bool('ADD_AUTHOR') or reason.startswith('API')):
        logger.debug('Author %s not found in database, trying to add' % author)
        # no match for supplied author, but we're allowed to add new ones
        if lazylibrarian.CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
            if title:
                ol = OpenLibrary(author + '<ll>' + title)
            else:
                ol = OpenLibrary(author)
            try:
                author_info = ol.find_author_id()
            except Exception as e:
                logger.warn("%s finding author id for [%s] %s" % (type(e).__name__, author, str(e)))
                return "", "", False
        else:
            gr = GoodReads(author)
            try:
                author_info = gr.find_author_id()
            except Exception as e:
                logger.warn("%s finding author id for [%s] %s" % (type(e).__name__, author, str(e)))
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
            if match_fuzz < lazylibrarian.CONFIG.get_int('NAME_RATIO'):
                logger.debug("Failed to match author [%s] to authorname [%s] fuzz [%d]" %
                             (author, match_name, match_fuzz))

            # To save loading hundreds of books by unknown authors at GR or GB, ignore unknown
            if "unknown" not in author.lower() and 'anonymous' not in author.lower() and \
                    match_fuzz >= lazylibrarian.CONFIG.get_int('NAME_RATIO'):
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
    author = make_unicode(author)
    return author, check_exist_author['AuthorID'], new


def add_author_to_db(authorname=None, refresh=False, authorid=None, addbooks=True, reason=None):
    """
    Add an author to the database by name or id, and optionally get a list of all their books
    If author already exists in database, refresh their details and optionally booklist
    Returns the author ID
    """
    if not reason:
        if len(inspect.stack()) > 2:
            frame = inspect.getframeinfo(inspect.stack()[2][0])
            program = os.path.basename(frame.filename)
            method = frame.function
            lineno = frame.lineno
            reason = "%s:%s:%s" % (program, method, lineno)
        else:
            reason = "Unknown reason in add_author_to_db"

    threadname = thread_name()
    if "Thread-" in threadname:
        thread_name("AddAuthorToDB")
    # noinspection PyBroadException
    try:
        db = database.DBConnection()
        match = False
        authorimg = ''
        new_author = not refresh
        entry_status = 'Active'

        if is_valid_authorid(authorid):
            dbauthor = db.match("SELECT * from authors WHERE AuthorID=?", (authorid,))
            if not dbauthor and authorname and 'unknown' not in authorname and 'anonymous' not in authorname:
                dbauthor = db.match("SELECT * from authors WHERE AuthorName=?", (authorname,))
                if dbauthor:
                    logger.warn("Conflicting authorid for %s (new:%s old:%s) Using new authorid" %
                                (authorname, authorid, dbauthor['AuthorID']))
                    db.action("PRAGMA foreign_keys = OFF")
                    db.action('UPDATE books SET AuthorID=? WHERE AuthorID=?',
                              (authorid, dbauthor['authorid']))
                    db.action('UPDATE seriesauthors SET AuthorID=? WHERE AuthorID=?',
                              (authorid, dbauthor['authorid']), suppress='UNIQUE')
                    if authorid.startswith('OL'):
                        db.action('UPDATE authors SET AuthorID=?,ol_id=? WHERE AuthorID=?',
                                  (authorid, authorid, dbauthor['authorid']), suppress='UNIQUE')
                    else:
                        db.action('UPDATE authors SET AuthorID=?,gr_id=? WHERE AuthorID=?',
                                  (authorid, authorid, dbauthor['authorid']), suppress='UNIQUE')
                    db.action("PRAGMA foreign_keys = ON")
                    entry_status = dbauthor['Status']
                    authorid = dbauthor['authorid']
            if not dbauthor:
                authorname = 'unknown author %s' % authorid
            else:
                entry_status = dbauthor['Status']
                authorname = dbauthor['authorname']

            control_value_dict = {"AuthorID": authorid}
            new_value_dict = {"Status": "Loading"}
            if new_author and not dbauthor:
                new_value_dict["AuthorName"] = authorname
                new_value_dict["AuthorImg"] = "images/nophoto.png"
                new_value_dict['Reason'] = reason

            db.upsert("authors", new_value_dict, control_value_dict)

            author = {}
            ol_id = ''
            gr_id = ''
            gr_name = ''
            ol_name = ''
            ol_author = {}
            gr_author = {}

            if dbauthor:
                ol_id = dbauthor['ol_id']
                gr_id = dbauthor['gr_id']
            elif authorid.startswith('OL'):
                ol_id = authorid
            elif authorid:
                gr_id = authorid

            if not ol_id and 'unknown' not in authorname and 'anonymous' not in authorname:
                ol = OpenLibrary(authorname)
                ol_author = ol.find_author_id()
                if ol_author:
                    ol_id = ol_author['authorid']
                else:
                    ol_id = authorid
            if not gr_id and 'unknown' not in authorname and 'anonymous' not in authorname:
                gr = GoodReads(authorname)
                gr_author = gr.find_author_id()
                if gr_author:
                    gr_id = gr_author['authorid']
                else:
                    gr_id = authorid

            if ol_id:
                ol = OpenLibrary(ol_id)
                ol_author = ol.get_author_info(authorid=ol_id, refresh=refresh)
            if gr_id:
                gr = GoodReads(gr_id)
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
            if author.get('ol_id') and lazylibrarian.CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
                author['authorid'] = author['ol_id']
            elif author.get('gr_id'):
                author['authorid'] = author['gr_id']
            else:
                author = {}

            if author:
                authorname = author['authorname']
                akas = []
                if gr_name and ol_name and gr_name != ol_name:
                    if author.get('AKA'):
                        akas = get_list(author.get('AKA', ''), ',')
                    if authorname != gr_name and gr_name not in akas:
                        logger.warn("Conflicting goodreads authorname for %s [%s][%s] setting AKA" %
                                    (authorid, authorname, gr_name))
                        akas.append(gr_name)
                    if authorname != ol_name and ol_name not in akas:
                        logger.warn("Conflicting openlibrary authorname for %s [%s][%s] setting AKA" %
                                    (authorid, authorname, ol_name))
                        akas.append(ol_name)
                    author['AKA'] = ', '.join(akas)

                authorid = author['authorid']
                if not dbauthor:
                    dbauthor = db.match("SELECT * from authors WHERE AuthorName=?", (authorname,))
                if dbauthor:
                    if dbauthor['AuthorID'] != authorid:
                        logger.warn("Conflicting authorid for %s (new:%s old:%s) Using new authorid" %
                                    (authorname, authorid, dbauthor['AuthorID']))
                        db.action("PRAGMA foreign_keys = OFF")
                        db.action('UPDATE books SET AuthorID=? WHERE AuthorID=?',
                                  (authorid, dbauthor['authorid']))
                        db.action('UPDATE seriesauthors SET AuthorID=? WHERE AuthorID=?',
                                  (authorid, dbauthor['authorid']), suppress='UNIQUE')
                        if authorid.startswith('OL'):
                            logger.debug("Changing authorid from GR to OL")
                            db.action('UPDATE authors SET AuthorID=?,gr_id=? WHERE AuthorID=?',
                                      (authorid, dbauthor['authorid'], dbauthor['authorid']), suppress='UNIQUE')
                        else:
                            logger.debug("Changing authorid from OL to GR")
                            db.action('UPDATE authors SET AuthorID=?,ol_id=? WHERE AuthorID=?',
                                      (authorid, dbauthor['authorid'], dbauthor['authorid']), suppress='UNIQUE')
                        db.action("PRAGMA foreign_keys = ON")
                        entry_status = dbauthor['Status']
                        dbauthor = db.match("SELECT * from authors WHERE AuthorName=?", (authorname,))
                    logger.debug("Updating author %s (%s) %s" % (authorid, authorname, entry_status))
                    new_author = False
                else:
                    new_author = True
                    logger.debug("Adding new author id %s (%s) to database %s, Addbooks=%s" %
                                 (authorid, authorname, reason, addbooks))

                authorimg = author['authorimg']
                control_value_dict = {"AuthorID": authorid}
                new_value_dict = {
                    "Updated": int(time.time())
                }
                if new_author:
                    new_value_dict['Reason'] = reason
                    new_value_dict["DateAdded"] = today()
                    new_value_dict["AuthorImg"] = authorimg
                if new_author or (dbauthor and not dbauthor['manual']):
                    new_value_dict["AuthorBorn"] = author['authorborn']
                    new_value_dict["AuthorDeath"] = author['authordeath']
                    new_value_dict["AuthorLink"] = author['authorlink']
                    new_value_dict["gr_id"] = author.get('gr_id', '')
                    new_value_dict["ol_id"] = author.get('ol_id', '')
                    new_value_dict["AKA"] = author.get('AKA', '')
                    if author.get('about', ''):
                        new_value_dict['About'] = author['about']
                if dbauthor and dbauthor['authorname'] != authorname:
                    if 'unknown' not in dbauthor['authorname'] and 'anonymous' not in dbauthor['authorname']:
                        if unaccented_bytes(dbauthor['authorname']) != unaccented_bytes(authorname):
                            authorname = dbauthor['authorname']
                            logger.warn("Authorname mismatch for %s [%s][%s]" %
                                        (authorid, dbauthor['authorname'], author['authorname']))

                new_value_dict["AuthorName"] = authorname

                db.upsert("authors", new_value_dict, control_value_dict)
                match = True
            else:
                logger.warn("No author info for %s:%s" % (authorid, authorname))
                # goodreads sometimes changes authorid
                # maybe change of provider or no reply from provider

        if not match and authorname and 'unknown' not in authorname.lower() and 'anonymous' not in authorname.lower():
            authorname = ' '.join(authorname.split())  # ensure no extra whitespace
            ol = OpenLibrary(authorname)
            ol_author = ol.find_author_id(refresh=refresh)
            gr = GoodReads(authorname)
            gr_author = gr.find_author_id(refresh=refresh)

            author = {}
            # which do we prefer
            if lazylibrarian.CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
                if ol_author.get('authorid'):
                    ol_author['ol_id'] = ol_author['authorid']
                    for item in ol_author:
                        if not author.get(item):  # if key doesn't exist or value empty
                            author[item] = ol_author[item]
                if gr_author.get('authorid'):
                    gr_author['gr_id'] = gr_author['authorid']
                    for item in gr_author:
                        if not author.get(item):
                            author[item] = gr_author[item]
            else:
                if gr_author.get('authorid'):
                    gr_author['gr_id'] = gr_author['authorid']
                    for item in gr_author:
                        if not author.get(item):
                            author[item] = gr_author[item]
                if ol_author.get('authorid'):
                    ol_author['ol_id'] = ol_author['authorid']
                    for item in ol_author:
                        if not author.get(item):  # if key doesn't exist or value empty
                            author[item] = ol_author[item]

            if author.get('authorid'):
                authorid = author['authorid']
                dbauthor = db.match("SELECT * from authors WHERE AuthorID=?", (author['authorid'],))
                if not dbauthor:
                    dbauthor = db.match("SELECT * from authors WHERE AuthorName=?", (authorname,))
                    if dbauthor:
                        authorid = dbauthor['AuthorID']

                if not dbauthor:
                    authorimg = author['authorimg']
                    control_value_dict = {"AuthorID": authorid}
                    new_value_dict = {
                        "AuthorName": authorname,
                        "AuthorLink": author['authorlink'],
                        "Updated": int(time.time()),
                        'Reason': reason,
                        'DateAdded': today(),
                        "Status": "Loading"
                    }
                    logger.debug("Adding new author: %s (%s) %s" % (authorid, authorname, reason))
                    entry_status = 'Active'
                    new_author = True
                else:
                    authorimg = dbauthor['authorimg']
                    authorid = dbauthor['authorid']
                    authorname = dbauthor['AuthorName']
                    control_value_dict = {"AuthorID": authorid}
                    new_value_dict = {
                        "Updated": int(time.time()),
                        "Status": "Loading",
                        "gr_id": author.get('gr_id', ''),
                        "ol_id": author.get('ol_id', '')
                    }
                    logger.debug("Updating author: %s (%s)" % (authorid, authorname))
                    entry_status = dbauthor['Status']
                    new_author = False

                    if author['authorname'] != dbauthor['authorname']:
                        akas = get_list(dbauthor['AKA'], ',')
                        if author['authorname'] not in akas:
                            logger.warn("Conflicting authorname for %s [%s][%s] setting AKA" %
                                        (authorid, author['authorname'], dbauthor['authorname']))
                            akas.append(author['authorname'])
                            db.action("UPDATE authors SET AKA=? WHERE AuthorID=?", (', '.join(akas), authorid))
                        authorname = dbauthor['authorname']
                    if author['authorid'] != authorid:
                        # GoodReads may have altered authorid?
                        logger.warn("Conflicting authorid for %s (%s:%s) Moving to new authorid" %
                                    (authorname, author['authorid'], authorid))
                        db.action("PRAGMA foreign_keys = OFF")
                        db.action('UPDATE books SET AuthorID=? WHERE AuthorID=?',
                                  (author['authorid'], authorid))
                        db.action('UPDATE seriesauthors SET AuthorID=? WHERE AuthorID=?',
                                  (author['authorid'], authorid), suppress='UNIQUE')
                        db.action('DELETE from authors WHERE AuthorID=?', (authorid,))
                        authorid = author['authorid']
                        dbauthor = None

                if not dbauthor or (dbauthor and not dbauthor['manual']):
                    new_value_dict["AuthorImg"] = author['authorimg']
                    new_value_dict["AuthorBorn"] = author['authorborn']
                    new_value_dict["AuthorDeath"] = author['authordeath']
                    new_value_dict["gr_id"] = author.get('gr_id', '')
                    new_value_dict["ol_id"] = author.get('ol_id', '')

                db.upsert("authors", new_value_dict, control_value_dict)

                if dbauthor is None:
                    db.action("PRAGMA foreign_keys = ON")
                match = True
            else:
                msg = "No authorID found for %s" % authorname
                logger.error(msg)
                # name not found at provider or no reply from provider
                # don't keep trying the same one...
                db.action("UPDATE authors SET Updated=? WHERE AuthorName=?", (int(time.time()), authorname))
                return msg
        if not match:
            msg = "No matching result for %s:%s" % (authorid, authorname)
            logger.error(msg)
            db.action("UPDATE authors SET Updated=? WHERE AuthorID=?", (int(time.time()), authorid))
            return msg

        # if author is set to manual, should we allow replacing 'nophoto' ?
        new_img = False
        match = db.match("SELECT Manual from authors WHERE AuthorID=?", (authorid,))
        if match and not match['Manual']:
            if authorimg and 'nophoto' in authorimg:
                newimg = get_author_image(authorid)
                if newimg:
                    authorimg = newimg
                    new_img = True

            # allow caching new image
            if authorimg and authorimg.startswith('http'):
                newimg, success, _ = cache_img("author", authorid, authorimg, refresh=refresh)
                if success:
                    authorimg = newimg
                    new_img = True
                else:
                    logger.debug('Failed to cache image for %s (%s)' % (authorimg, newimg))

            if new_img:
                db.action("UPDATE authors SET AuthorIMG=? WHERE AuthorID=?", (authorimg, authorid))

        if match and addbooks:
            if new_author:
                bookstatus = lazylibrarian.CONFIG['NEWAUTHOR_STATUS']
                audiostatus = lazylibrarian.CONFIG['NEWAUTHOR_AUDIO']
            else:
                bookstatus = lazylibrarian.CONFIG['NEWBOOK_STATUS']
                audiostatus = lazylibrarian.CONFIG['NEWAUDIO_STATUS']

            if entry_status not in ['Active', 'Wanted', 'Ignored', 'Paused']:
                entry_status = 'Active'  # default for invalid/unknown or "loading"
            # process books
            if lazylibrarian.CONFIG['BOOK_API'] == "GoogleBooks" and lazylibrarian.CONFIG['GB_API']:
                book_api = GoogleBooks()
                book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                          audiostatus=audiostatus, entrystatus=entry_status,
                                          refresh=refresh, reason=reason)
                if lazylibrarian.CONFIG.get_bool('MULTI_SOURCE'):
                    book_api = OpenLibrary()
                    book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                              audiostatus=audiostatus, entrystatus=entry_status,
                                              refresh=refresh, reason=reason)
                    if lazylibrarian.CONFIG['GR_API']:
                        book_api = GoodReads(authorname)
                        book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                                  audiostatus=audiostatus, entrystatus=entry_status,
                                                  refresh=refresh, reason=reason)
            elif lazylibrarian.CONFIG['BOOK_API'] == "GoodReads" and lazylibrarian.CONFIG['GR_API']:
                book_api = GoodReads(authorname)
                book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                          audiostatus=audiostatus, entrystatus=entry_status,
                                          refresh=refresh, reason=reason)
                if lazylibrarian.CONFIG.get_bool('MULTI_SOURCE'):
                    book_api = OpenLibrary()
                    book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                              audiostatus=audiostatus, entrystatus=entry_status,
                                              refresh=refresh, reason=reason)
                    if lazylibrarian.CONFIG['GB_API']:
                        book_api = GoogleBooks()
                        book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                                  audiostatus=audiostatus, entrystatus=entry_status,
                                                  refresh=refresh, reason=reason)
            elif lazylibrarian.CONFIG['BOOK_API'] == "OpenLibrary":
                book_api = OpenLibrary(authorname)
                book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                          audiostatus=audiostatus, entrystatus=entry_status,
                                          refresh=refresh, reason=reason)
                if lazylibrarian.CONFIG.get_bool('MULTI_SOURCE'):
                    if lazylibrarian.CONFIG['GR_API']:
                        book_api = GoodReads()
                        book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                                  audiostatus=audiostatus, entrystatus=entry_status,
                                                  refresh=refresh, reason=reason)
                    if lazylibrarian.CONFIG['GB_API']:
                        book_api = GoogleBooks()
                        book_api.get_author_books(authorid, authorname, bookstatus=bookstatus,
                                                  audiostatus=audiostatus, entrystatus=entry_status,
                                                  refresh=refresh, reason=reason)
            if lazylibrarian.STOPTHREADS and threadname == "AUTHORUPDATE":
                msg = "[%s] Author update aborted, status %s" % (authorname, entry_status)
                logger.debug(msg)
                return msg

            if new_author and lazylibrarian.CONFIG['GR_FOLLOWNEW']:
                res = grfollow(authorid, True)
                if res.startswith('Unable'):
                    logger.warn(res)
                try:
                    followid = res.split("followid=")[1]
                    logger.debug('%s marked followed' % authorname)
                except IndexError:
                    followid = ''
                db.action('UPDATE authors SET GRfollow=? WHERE AuthorID=?', (followid, authorid))
        else:
            # if we're not loading any books and it's a new author,
            # mark author as paused in case it's a wishlist or a series contributor
            if new_author and not addbooks:
                entry_status = 'Paused'

        if match:
            update_totals(authorid)
            db.action("UPDATE authors SET Status=? WHERE AuthorID=?", (entry_status, authorid))
            msg = "%s [%s] Author update complete, status %s" % (authorid, authorname, entry_status)
            logger.info(msg)
            return authorid
        else:
            msg = "Authorid %s (%s) not found in database" % (authorid, authorname)
            logger.warn(msg)
            return msg

    except Exception:
        msg = 'Unhandled exception in add_author_to_db: %s' % traceback.format_exc()
        logger.error(msg)
        return msg


def update_totals(authorid):
    db = database.DBConnection()
    # author totals needs to be updated every time a book is marked differently
    match = db.select('SELECT AuthorID from authors WHERE AuthorID=?', (authorid,))
    if not match:
        logger.debug('Update_totals - authorid [%s] not found' % authorid)
        return

    cmd = 'SELECT BookName, BookLink, BookDate, BookID from books WHERE AuthorID=?'
    cmd += ' AND Status != "Ignored" order by BookDate DESC'
    lastbook = db.match(cmd, (authorid,))

    cmd = "select sum(case status when 'Ignored' then 0 else 1 end) as unignored,"
    cmd += "sum(case when status == 'Have' then 1 when status == 'Open' then 1 else 0 end) as EHave, "
    cmd += "sum(case when audiostatus == 'Have' then 1 when audiostatus == 'Open' then 1 "
    cmd += "else 0 end) as AHave, sum(case when status == 'Have' then 1 when status == 'Open' then 1 "
    cmd += "when audiostatus == 'Have' then 1 when audiostatus == 'Open' then 1 else 0 end) as Have, "
    cmd += "count(*) as total from books where authorid=?"
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

    cmd = "select series.seriesid as Series,sum(case books.status when 'Ignored' then 0 else 1 end) as Total,"
    cmd += "sum(case when books.status == 'Have' then 1 when books.status == 'Open' then 1 "
    cmd += "when books.audiostatus == 'Have' then 1 when books.audiostatus == 'Open' then 1 "
    cmd += "else 0 end) as Have from books,member,series,seriesauthors where member.bookid=books.bookid "
    cmd += "and member.seriesid = series.seriesid and seriesauthors.seriesid = series.seriesid "
    cmd += "and seriesauthors.authorid=? group by series.seriesid"
    res = db.select(cmd, (authorid,))
    if len(res):
        for series in res:
            db.action('UPDATE series SET Have=?, Total=? WHERE SeriesID=?',
                      (check_int(series['Have'], 0), check_int(series['Total'], 0), series['Series']))

    res = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
    logger.debug('Updated totals for [%s] %s/%s' % (res['AuthorName'], new_value_dict['HaveBooks'],
                                                    new_value_dict['TotalBooks']))


def import_book(bookid, ebook=None, audio=None, wait=False, reason='importer.import_book'):
    """ search goodreads or googlebooks for a bookid and import the book
        ebook/audio=None makes find_book use configured default """
    if lazylibrarian.CONFIG['BOOK_API'] == "GoogleBooks":
        gb = GoogleBooks(bookid)
        if not wait:
            threading.Thread(target=gb.find_book, name='GB-IMPORT', args=[bookid, ebook, audio, reason]).start()
        else:
            gb.find_book(bookid, ebook, audio, reason)
    elif lazylibrarian.CONFIG['BOOK_API'] == 'OpenLibrary':
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


def search_for(searchterm):
    """ search goodreads or googlebooks for a searchterm, return a list of results
    """
    if lazylibrarian.CONFIG['BOOK_API'] == "GoogleBooks":
        gb = GoogleBooks(searchterm)
        myqueue = Queue()
        search_api = threading.Thread(target=gb.find_results, name='GB-RESULTS', args=[searchterm, myqueue])
        search_api.start()
    elif lazylibrarian.CONFIG['BOOK_API'] == "GoodReads":
        myqueue = Queue()
        gr = GoodReads(searchterm)
        search_api = threading.Thread(target=gr.find_results, name='GR-RESULTS', args=[searchterm, myqueue])
        search_api.start()
    elif lazylibrarian.CONFIG['BOOK_API'] == "OpenLibrary":
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
        return sortedlist
    return []
