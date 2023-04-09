#  This file is part of Lazylibrarian.
#
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import json
import os
import re
import logging
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import traceback
import uuid
import zipfile

import lazylibrarian
from lazylibrarian.config2 import CONFIG
from lazylibrarian.telemetry import TELEMETRY
from lazylibrarian.gb import GoogleBooks
from lazylibrarian.gr import GoodReads
from lazylibrarian.ol import OpenLibrary

from lazylibrarian import database, utorrent, transmission, qbittorrent, \
    deluge, rtorrent, synology, sabnzbd, nzbget
from lazylibrarian.bookrename import name_vars, audio_rename, stripspaces, id3read
from lazylibrarian.cache import cache_img, ImageType
from lazylibrarian.calibre import calibredb
from lazylibrarian.common import run_script, multibook, calibre_prg
from lazylibrarian.filesystem import DIRS, path_isfile, path_isdir, syspath, path_exists, remove_file, listdir, \
    setperm, make_dirs, safe_move, safe_copy, opf_file, bts_file, jpg_file, book_file, get_directory, walk
from lazylibrarian.formatter import unaccented, plural, now, today, \
    replace_all, get_list, surname_first, make_unicode, check_int, is_valid_type, split_title, \
    make_utf8bytes, sanitize, thread_name
from lazylibrarian.images import createthumbs
from lazylibrarian.importer import add_author_to_db, add_author_name_to_db, update_totals, search_for, import_book
from lazylibrarian.librarysync import get_book_info, find_book_in_db, library_scan, get_book_meta
from lazylibrarian.magazinescan import create_id
from lazylibrarian.mailinglist import mailing_list
from lazylibrarian.images import create_mag_cover
from lazylibrarian.preprocessor import preprocess_ebook, preprocess_audio, preprocess_magazine
from lazylibrarian.notifiers import notify_download, custom_notify_download, notify_snatch,  custom_notify_snatch
from lazylibrarian.scheduling import schedule_job, SchedulerCommand
from deluge_client import DelugeRPCClient
from thefuzz import fuzz


def update_downloads(provider):
    db = database.DBConnection()
    try:
        entry = db.match('SELECT Count FROM downloads where Provider=?', (provider,))
        if entry:
            counter = int(entry['Count'])
            db.action('UPDATE downloads SET Count=? WHERE Provider=?', (counter + 1, provider))
        else:
            db.action('INSERT into downloads (Count, Provider) VALUES  (?, ?)', (1, provider))
    finally:
        db.close()


def process_mag_from_file(source_file=None, title=None, issuenum=None):
    # import a magazine issue by title/num
    # Assumes the source file is the correct file for the issue and renames it to match
    # Adds the magazine id to the database if not already there
    logger = logging.getLogger(__name__)
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        if not source_file or not path_isfile(source_file):
            logger.warning("%s is not a file" % source_file)
            return False
        _, extn = os.path.splitext(source_file)
        extn = extn.lstrip('.')
        if not extn or extn not in get_list(CONFIG['MAG_TYPE']):
            logger.warning("%s is not a valid issue file" % source_file)
            return False
        title = unaccented(sanitize(title), only_ascii=False)
        if not title:
            logger.warning("No title for %s, rejecting" % source_file)
            return False

        TELEMETRY.record_usage_data('Process/Magazine/FromFile')
        entry = db.match('SELECT * FROM magazines where Title=?', (title,))
        if not entry:
            logger.debug("Magazine title [%s] not found, adding it" % title)
            control_value_dict = {"Title": title}
            new_value_dict = {"LastAcquired": today(),
                              "IssueStatus": CONFIG['FOUND_STATUS'],
                              "IssueDate": "",
                              "LatestCover": ""}
            db.upsert("magazines", new_value_dict, control_value_dict)
        # rename issuefile to match pattern
        # update magazine lastissue/cover as required
        entry = db.match('SELECT * FROM magazines where Title=?', (title,))
        mostrecentissue = entry['IssueDate']
        dest_path = CONFIG['MAG_DEST_FOLDER'].replace(
            '$IssueDate', issuenum).replace('$Title', title)

        if CONFIG.get_bool('MAG_RELATIVE'):
            dest_dir = get_directory('eBook')
            dest_path = stripspaces(os.path.join(dest_dir, dest_path))
            dest_path = make_utf8bytes(dest_path)[0]
        else:
            dest_path = make_utf8bytes(dest_path)[0]

        if not dest_path or not make_dirs(dest_path):
            logger.error('Unable to create destination directory %s' % dest_path)
            return False

        if '$IssueDate' in CONFIG['MAG_DEST_FILE']:
            global_name = CONFIG['MAG_DEST_FILE'].replace(
                '$IssueDate', issuenum).replace('$Title', title)
        else:
            global_name = "%s %s" % (title, issuenum)
        global_name = unaccented(global_name, only_ascii=False)
        global_name = sanitize(global_name)
        tempdir = tempfile.mkdtemp()
        _ = safe_copy(source_file, os.path.join(tempdir, os.path.basename(source_file)))
        data = {"IssueDate": issuenum, "Title": title}
        success, dest_file, pp_path = process_destination(tempdir, dest_path, global_name, data, "magazine")
        shutil.rmtree(tempdir, ignore_errors=True)
        if not success:
            logger.error("Unable to import %s: %s" % (source_file, dest_file))
            return False

        remove_file(source_file)
        if mostrecentissue:
            if mostrecentissue.isdigit() and str(issuenum).isdigit():
                older = (int(mostrecentissue) > int(issuenum))  # issuenumber
            else:
                older = (mostrecentissue > issuenum)  # YYYY-MM-DD
        else:
            older = False

        maginfo = db.match("SELECT CoverPage from magazines WHERE Title=?", (title,))
        # create a thumbnail cover for the new issue
        coverfile = create_mag_cover(dest_file, pagenum=check_int(maginfo['CoverPage'], 1))
        if coverfile:
            myhash = uuid.uuid4().hex
            hashname = os.path.join(DIRS.CACHEDIR, 'magazine', '%s.jpg' % myhash)
            shutil.copyfile(coverfile, hashname)
            setperm(hashname)
            coverfile = 'cache/magazine/%s.jpg' % myhash
            createthumbs(hashname)

        issueid = create_id("%s %s" % (title, issuenum))
        control_value_dict = {"Title": title, "IssueDate": issuenum}
        new_value_dict = {"IssueAcquired": today(),
                          "IssueFile": dest_file,
                          "IssueID": issueid,
                          "Cover": coverfile
                          }
        db.upsert("issues", new_value_dict, control_value_dict)

        control_value_dict = {"Title": title}
        if older:  # check this in case processing issues arriving out of order
            new_value_dict = {"LastAcquired": today(),
                              "IssueStatus": CONFIG['FOUND_STATUS']}
        else:
            new_value_dict = {"LastAcquired": today(),
                              "IssueStatus": CONFIG['FOUND_STATUS'],
                              "IssueDate": issuenum,
                              "LatestCover": coverfile}
        db.upsert("magazines", new_value_dict, control_value_dict)

        if not CONFIG.get_bool('IMP_MAGOPF'):
            logger.debug('create_mag_opf is disabled')
        else:
            if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                authors = title
            else:
                authors = 'magazines'
            _ = create_mag_opf(dest_file, authors, title, issuenum, issueid, overwrite=True)
        if CONFIG['IMP_AUTOADDMAG']:
            dest_path = os.path.dirname(dest_file)
            process_auto_add(dest_path, booktype='mag')
        return True

    except Exception:
        logger.error('Unhandled exception in import_mag: %s' % traceback.format_exc())
        return False
    finally:
        db.close()


def process_book_from_dir(source_dir=None, library='eBook', bookid=None, automerge=False):
    # import a book by id from a directory
    # Assumes the book is the correct file for the id and renames it to match
    # Adds the id to the database if not already there
    logger = logging.getLogger(__name__)
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        if not source_dir or not path_isdir(source_dir):
            logger.warning("%s is not a directory" % source_dir)
            return False
        if source_dir.startswith(get_directory(library)):
            logger.warning('Source directory must not be the same as or inside library')
            return False

        TELEMETRY.record_usage_data('Process/Book/FromDir')
        reject = multibook(source_dir)
        if reject:
            logger.debug("Not processing %s, found multiple %s" % (source_dir, reject))
            return False

        if library in ['eBook', 'Audio']:
            logger.debug('Processing %s directory %s' % (library, source_dir))
            book = db.match('SELECT * from books where BookID=?', (bookid,))
            if not book:
                logger.warning("Bookid [%s] not found in database, trying to add..." % (bookid,))
                if CONFIG['BOOK_API'] == "GoodReads":
                    gr_id = GoodReads(bookid)
                    gr_id.find_book(bookid, None, None, "Added by import_book %s" % source_dir)
                elif CONFIG['BOOK_API'] == "GoogleBooks":
                    gb_id = GoogleBooks(bookid)
                    gb_id.find_book(bookid, None, None, "Added by import_book %s" % source_dir)
                elif CONFIG['BOOK_API'] == "OpenLibrary":
                    ol_id = OpenLibrary(bookid)
                    ol_id.find_book(bookid, None, None, "Added by import_book %s" % source_dir)
                # see if it's there now...
                book = db.match('SELECT * from books where BookID=?', (bookid,))
            if not book:
                logger.debug("Unable to add bookid %s to database" % bookid)
                return False
            return process_book(source_dir, bookid, library, automerge=automerge)
        else:
            logger.error("import_book not implemented for %s" % library)
            return False
    except Exception:
        logger.error('Unhandled exception in import_book: %s' % traceback.format_exc())
        return False
    finally:
        db.close()


def process_issues(source_dir=None, title=''):
    # import magazine issues for a given title from an alternate directory
    # noinspection PyBroadException
    logger = logging.getLogger(__name__)
    loggermatching = logging.getLogger('special.matching')
    try:
        if not source_dir:
            logger.warning("Alternate Directory not configured")
            return False
        if not path_isdir(source_dir):
            logger.warning("%s is not a directory" % source_dir)
            return False

        TELEMETRY.record_usage_data('Process/Issues')
        logger.debug('Looking for %s issues in %s' % (title, source_dir))
        # first, recursively process any items in subdirectories
        flist = listdir(source_dir)
        for fname in flist:
            subdir = os.path.join(source_dir, fname)
            if path_isdir(subdir):
                process_issues(subdir, title)

        dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '', '[': ' ', ']': ' ', '#': '# '}
        db = database.DBConnection()
        try:
            res = db.match('SELECT Reject,DateType from magazines WHERE Title=?', (title,))
        finally:
            db.close()
        if not res:
            logger.error("%s not found in database" % title)
            return False

        rejects = get_list(res['Reject'])
        title_words = replace_all(title.lower(), dic).split()

        # import any files in this directory that match the title, are a magazine file, and have a parseable date
        for f in listdir(source_dir):
            _, extn = os.path.splitext(f)
            extn = extn.lstrip('.')
            if not extn or extn.lower() not in get_list(CONFIG['MAG_TYPE']):
                continue

            loggermatching.debug('Trying to match %s' % f)
            filename_words = replace_all(f.lower(), dic).split()
            found_title = True
            for word in title_words:
                if word not in filename_words:
                    loggermatching.debug('[%s] not found in %s' % (word, f))
                    found_title = False
                    break

            if found_title:
                for item in rejects:
                    if item in filename_words:
                        loggermatching.debug('Rejecting %s, contains %s' % (f, item))
                        found_title = False
                        break

            if found_title:
                if '*' in rejects:  # strict rejection mode, no extraneous words
                    nouns = get_list(CONFIG['ISSUE_NOUNS'])
                    nouns.extend(get_list(CONFIG['VOLUME_NOUNS']))
                    nouns.extend(get_list(CONFIG['MAG_NOUNS']))
                    nouns.extend(get_list(CONFIG['MAG_TYPE']))
                    valid = True
                    for word in filename_words:
                        if word.islower():  # contains ANY lowercase letters
                            if word not in title_words and word not in nouns:
                                valid = False
                                for month in range(1, 13):
                                    if word in lazylibrarian.MONTHNAMES[month]:
                                        valid = True
                                        break
                                if not valid:
                                    logger.debug("Rejecting %s, strict, contains %s" %
                                                 (f, word))
                                    break
                    if not valid:
                        found_title = False

            if found_title:
                regex_pass, issuedate, year = lazylibrarian.searchmag.get_issue_date(filename_words, res['DateType'])
                if regex_pass:
                    # suppress the "-01" day on monthly magazines
                    # if re.match(r'\d+-\d\d-01', str(issuedate)):
                    #    issuedate = issuedate[:-3]

                    if process_mag_from_file(os.path.join(source_dir, f), title, issuedate):
                        logger.debug('Processed %s issue %s' % (title, issuedate))
                    else:
                        logger.warning('Failed to process %s' % f)
                else:
                    loggermatching.debug('regex failed for %s' % f)
        return True

    except Exception:
        logger.error('Unhandled exception in process_issues: %s' % traceback.format_exc())
        return False


def process_alternate(source_dir=None, library='eBook', automerge=False):
    # import a book/audiobook from an alternate directory
    # noinspection PyBroadException
    logger = logging.getLogger(__name__)
    try:
        if not source_dir:
            logger.warning("Alternate Directory not configured")
            return False
        if not path_isdir(source_dir):
            logger.warning("%s is not a directory" % source_dir)
            return False
        if source_dir.startswith(get_directory(library)):
            logger.warning('Alternate directory must not be the same as or inside Destination')
            return False

        TELEMETRY.record_usage_data('Process/Alternate')
        logger.debug('Processing %s directory %s' % (library, source_dir))
        # first, recursively process any books in subdirectories
        flist = listdir(source_dir)
        for fname in flist:
            subdir = os.path.join(source_dir, fname)
            if path_isdir(subdir):
                process_alternate(subdir, library=library, automerge=automerge)

        metadata = {}
        if "LL.(" in source_dir:
            bookid = source_dir.split("LL.(")[1].split(")")[0]
            db = database.DBConnection()
            res = db.match("SELECT BookName,AuthorName from books,authors WHERE books.AuthorID = authors.AuthorID AND BookID=?", (bookid,))
            if res:
                metadata = {"title": res['BookName'], "creator": res['AuthorName']}
                logger.debug("Importing %s bookid %s for %s %s" % (library, bookid, res['AuthorName'], res['BookName']))
            else:
                logger.warning("Failed to find LL bookid %s in database" % bookid)
            db.close()

        if library == 'eBook':
            # only import one book from each alternate (sub)directory, this is because
            # the importer may delete the directory after importing a book,
            # depending on lazylibrarian.CONFIG['DESTINATION_COPY'] setting
            # also if multiple books in a folder and only a "metadata.opf"
            # or "cover.jpg" which book is it for?
            reject = multibook(source_dir)
            if reject:
                logger.debug("Not processing %s, found multiple %s" % (source_dir, reject))
                return False

            new_book = book_file(source_dir, booktype='ebook', config=CONFIG)
            if not new_book:
                # check if an archive in this directory
                for f in listdir(source_dir):
                    if not is_valid_type(f, extensions=CONFIG.get_all_types_list()):
                        # Is file an archive, if so look inside and extract to new dir
                        res = unpack_archive(os.path.join(source_dir, f), source_dir, f)
                        if res:
                            source_dir = res
                            break
                new_book = book_file(source_dir, booktype='ebook', config=CONFIG)
            if not new_book:
                logger.warning("No book file found in %s" % source_dir)
                return False

            if not metadata:
                # if we haven't already got metadata from an LL.num
                # see if there is a metadata file in this folder with the info we need
                # try book_name.opf first, or fall back to any filename.opf
                metafile = os.path.splitext(new_book)[0] + '.opf'
                if not path_isfile(metafile):
                    metafile = opf_file(source_dir)
                if metafile and path_isfile(metafile):
                    try:
                        metadata = get_book_info(metafile)
                    except Exception as e:
                        logger.warning('Failed to read metadata from %s, %s %s' % (metafile, type(e).__name__, str(e)))
                else:
                    logger.debug('No metadata file found for %s' % new_book)

            if 'title' not in metadata or 'creator' not in metadata:
                # if not got both, try to get metadata from the book file
                extn = os.path.splitext(new_book)[1]
                if extn.lower() in [".epub", ".mobi"]:
                    try:
                        metadata = get_book_info(new_book)
                    except Exception as e:
                        logger.warning('No metadata found in %s, %s %s' % (new_book, type(e).__name__, str(e)))
        else:
            new_book = book_file(source_dir, booktype='audiobook', config=CONFIG)
            if not new_book:
                logger.warning("No audiobook file found in %s" % source_dir)
                return False
            if not metadata:
                id3r = id3read(new_book)
                author = id3r['author']
                book = id3r['title']

                if author and book:
                    metadata['creator'] = author
                    metadata['title'] = book
                    metadata['narrator'] = id3r['narrator']

        if 'title' not in metadata or 'creator' not in metadata:
            author, book = get_book_meta(source_dir, "postprocess")
            if author and book:
                metadata['creator'] = author
                metadata['title'] = book

        if 'title' in metadata and 'creator' in metadata:
            authorname = metadata['creator']
            bookname = metadata['title']
            db = database.DBConnection()
            try:
                authorid = ''
                bookid = ''
                results = []
                authmatch = db.match('SELECT * FROM authors where AuthorName=?', (authorname,))

                if not authmatch:
                    # try goodreads/openlibrary preferred authorname
                    if CONFIG['BOOK_API'] in ['OpenLibrary', 'GoogleBooks']:
                        logger.debug("Checking OpenLibrary for [%s]" % authorname)
                        ol = OpenLibrary(authorname)
                        try:
                            author_gr = ol.find_author_id()
                        except Exception as e:
                            author_gr = {}
                            logger.warning("No author id for [%s] %s" % (authorname, type(e).__name__))
                    else:
                        logger.debug("Checking GoodReads for [%s]" % authorname)
                        gr = GoodReads(authorname)
                        try:
                            author_gr = gr.find_author_id()
                        except Exception as e:
                            author_gr = {}
                            logger.warning("No author id for [%s] %s" % (authorname, type(e).__name__))
                    if author_gr:
                        grauthorname = author_gr['authorname']
                        authorid = author_gr['authorid']
                        logger.debug("Found [%s] for [%s]" % (grauthorname, authorname))
                        authorname = grauthorname
                        authmatch = db.match('SELECT * FROM authors where AuthorID=?', (authorid,))

                    if authmatch:
                        logger.debug("Author %s found in database" % authorname)
                        authorid = authmatch['authorid']
                    else:
                        logger.debug("Author %s not found, adding to database" % authorname)
                        if authorid:
                            add_author_to_db(authorid=authorid, addbooks=CONFIG.get_bool('NEWAUTHOR_BOOKS'),
                                             reason="process_alternate: %s" % bookname)
                        else:
                            aname, authorid, _ = add_author_name_to_db(author=authorname,
                                                                       reason="process_alternate: %s" % bookname,
                                                                       title=bookname)
                            if aname and aname != authorname:
                                authorname = aname
                            if not aname:
                                authorid = ''

                    if authorid:
                        bookid, _ = find_book_in_db(authorname, bookname, ignored=False, library=library,
                                                    reason="process_alternate: %s" % bookname)

                    if authorid and not bookid:
                        # new book, or new author where we didn't want to load their back catalog
                        searchterm = "%s <ll> %s" % (unaccented(bookname, only_ascii=False),
                                                     unaccented(authorname, only_ascii=False))
                        match = {}
                        results = search_for(searchterm)
                        for result in results:
                            if result['book_fuzz'] >= CONFIG.get_int('MATCH_RATIO') \
                                    and result['authorid'] == authorid:
                                match = result
                                break
                        if not match:  # no match on full searchterm, try splitting out subtitle and series
                            newtitle, _, _ = split_title(authorname, bookname)
                            if newtitle != bookname:
                                bookname = newtitle
                                searchterm = "%s <ll> %s" % (unaccented(bookname, only_ascii=False),
                                                             unaccented(authorname, only_ascii=False))
                                results = search_for(searchterm)
                                for result in results:
                                    if result['book_fuzz'] >= CONFIG.get_int('MATCH_RATIO') \
                                            and result['authorid'] == authorid:
                                        match = result
                                        break
                        if match:
                            logger.info("Found (%s%%) %s: %s for %s: %s" %
                                        (match['book_fuzz'], match['authorname'], match['bookname'],
                                         authorname, bookname))
                            import_book(match['bookid'], ebook="Skipped", audio="Skipped", wait=True,
                                        reason="Added from alternate dir")
                            imported = db.match('select * from books where BookID=?', (match['bookid'],))
                            if imported:
                                bookid = match['bookid']
                                update_totals(authorid)
            finally:
                db.close()

            if not bookid:
                msg = "%s %s by %s not found in database" % (library, bookname, authorname)
                if not results:
                    msg += ', No results returned'
                    logger.warning(msg)
                else:
                    msg += ', No match found'
                    logger.warning(msg)
                    msg = "Closest match (%s%% %s%%) %s: %s" % (results[0]['author_fuzz'], results[0]['book_fuzz'],
                                                                results[0]['authorname'], results[0]['bookname'])
                    if results[0]['authorid'] != authorid:
                        msg += ' wrong authorid'
                    logger.warning(msg)
                return False

            db = database.DBConnection()
            if library == 'eBook':
                res = db.match("SELECT Status from books WHERE BookID=?", (bookid,))
                if res and res['Status'] == 'Ignored':
                    logger.warning("%s %s by %s is marked Ignored in database, importing anyway" %
                                   (library, bookname, authorname))
            else:
                res = db.match("SELECT AudioStatus,Narrator from books WHERE BookID=?", (bookid,))
                if metadata.get('narrator', '') and res and not res['Narrator']:
                    db.action("update books set narrator=? where bookid=?", (metadata['narrator'], bookid))
                if res and res['AudioStatus'] == 'Ignored':
                    logger.warning("%s %s by %s is marked Ignored in database, importing anyway" %
                                   (library, bookname, authorname))
            db.close()
            return process_book(source_dir, bookid, library, automerge=automerge)

        else:
            logger.warning('%s %s has no metadata' % (library, new_book))
            res = check_residual(source_dir)
            if not res:
                logger.warning('%s has no book with LL.number' % source_dir)
                return False

    except Exception:
        logger.error('Unhandled exception in process_alternate: %s' % traceback.format_exc())
        return False


def move_into_subdir(sourcedir, targetdir, fname, move='move'):
    # move the book and any related files too, other book formats, or opf, jpg with same title
    # (files begin with fname) from sourcedir to new targetdir
    # can't move metadata.opf or cover.jpg or similar as can't be sure they are ours
    # return how many files you moved
    logger = logging.getLogger(__name__)
    cnt = 0
    list_dir = listdir(sourcedir)
    valid_extensions = CONFIG.get_all_types_list()
    for ourfile in list_dir:
        if ourfile.startswith(fname):  # or is_valid_booktype(ourfile, booktype="audiobook"):
            if is_valid_type(ourfile, extensions=valid_extensions):
                try:
                    srcfile = os.path.join(sourcedir, ourfile)
                    dstfile = os.path.join(targetdir, ourfile)
                    if CONFIG.get_bool('DESTINATION_COPY') or move == 'copy':
                        dstfile = safe_copy(srcfile, dstfile)
                        setperm(dstfile)
                        logger.debug("copy_into_subdir %s" % ourfile)
                        cnt += 1
                    else:
                        dstfile = safe_move(srcfile, dstfile)
                        setperm(dstfile)
                        logger.debug("move_into_subdir %s" % ourfile)
                        cnt += 1
                except Exception as why:
                    logger.warning("Failed to copy/move file %s to [%s], %s %s" %
                                   (ourfile, targetdir, type(why).__name__, str(why)))
                    continue
    return cnt


def unpack_multipart(source_dir, download_dir, title):
    """ unpack multipart zip/rar files into one directory
        returns new directory in download_dir with book in it, or empty string
    """
    logger = logging.getLogger(__name__)
    # loggerpostprocess = logging.getLogger('special.postprocess')
    TELEMETRY.record_usage_data('Process/MultiPart')
    # noinspection PyBroadException
    try:
        targetdir = os.path.join(download_dir, title + '.unpack')
        if not make_dirs(targetdir, new=True):
            logger.error("Failed to create target dir %s" % targetdir)
            return ''
        for f in listdir(source_dir):
            archivename = os.path.join(source_dir, f)
            if zipfile.is_zipfile(archivename):
                try:
                    z = zipfile.ZipFile(archivename)
                    for item in z.namelist():
                        if not item.endswith('/'):
                            # not if it's a directory
                            logger.debug('Extracting %s to %s' % (item, targetdir))
                            if os.path.__name__ == 'ntpath':
                                dst = os.path.join(targetdir, item.replace('/', '\\'))
                            else:
                                dst = os.path.join(targetdir, item)
                            with open(syspath(dst), "wb") as d:
                                d.write(z.read(item))
                except Exception as e:
                    logger.error("Failed to unzip %s: %s" % (archivename, e))
                    return ''
        for f in listdir(targetdir):
            if f.endswith('.rar'):
                resultdir = unpack_archive(os.path.join(targetdir, f), targetdir, title, targetdir=targetdir)
                if resultdir != targetdir:
                    for d in listdir(resultdir):
                        shutil.move(os.path.join(resultdir, d), os.path.join(targetdir, d))
                break
        return targetdir
    except Exception:
        logger.error('Unhandled exception in unpack_multipart: %s' % traceback.format_exc())
        return ''


def unpack_archive(archivename, download_dir, title, targetdir=''):
    """ See if archivename is an archive containing a book
        returns new directory in download_dir with book in it, or empty string
    """
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    archivename = make_unicode(archivename)
    if not path_isfile(archivename):  # regular files only
        return ''

    # noinspection PyBroadException
    try:
        if zipfile.is_zipfile(archivename):
            TELEMETRY.record_usage_data('Process/Archive')
            loggerpostprocess.debug('%s is a zip file' % archivename)
            try:
                z = zipfile.ZipFile(archivename)
            except Exception as e:
                logger.error("Failed to unzip %s: %s" % (archivename, e))
                return ''
            if not targetdir:
                targetdir = os.path.join(download_dir, title + '.unpack')
            if not make_dirs(targetdir, new=True):
                logger.error("Failed to create target dir %s" % targetdir)
                return ''

            logger.debug("Created target %s" % targetdir)
            # Look for any wanted files (inc jpg for cbr/cbz)
            for item in z.namelist():
                if is_valid_type(item, extensions=CONFIG.get_all_types_list()) and not item.endswith('/'):
                    # not if it's a directory
                    logger.debug('Extracting %s to %s' % (item, targetdir))
                    if os.path.__name__ == 'ntpath':
                        dst = os.path.join(targetdir, item.replace('/', '\\'))
                    else:
                        dst = os.path.join(targetdir, item)
                    dstdir = os.path.dirname(dst)
                    if not make_dirs(dstdir):
                        logger.error("Failed to create directory %s" % dstdir)
                        return ''
                    with open(syspath(dst), "wb") as f:
                        f.write(z.read(item))

        elif tarfile.is_tarfile(archivename):
            loggerpostprocess.debug('%s is a tar file' % archivename)
            try:
                z = tarfile.TarFile(archivename)
            except Exception as e:
                logger.error("Failed to untar %s: %s" % (archivename, e))
                return ''

            targetdir = os.path.join(download_dir, title + '.unpack')
            if not make_dirs(targetdir, new=True):
                logger.error("Failed to create target dir %s" % targetdir)
                return ''

            logger.debug("Created target %s" % targetdir)
            for item in z.getnames():
                if is_valid_type(item, extensions=CONFIG.get_all_types_list()) and not item.endswith('/'):
                    # not if it's a directory
                    logger.debug('Extracting %s to %s' % (item, targetdir))
                    if os.path.__name__ == 'ntpath':
                        dst = os.path.join(targetdir, item.replace('/', '\\'))
                    else:
                        dst = os.path.join(targetdir, item)
                    dstdir = os.path.dirname(dst)
                    if not make_dirs(dstdir):
                        logger.error("Failed to create directory %s" % dstdir)
                        return ''
                    with open(syspath(dst), "wb") as f:
                        f.write(z.extractfile(item).read())

        elif lazylibrarian.UNRARLIB == 1 and lazylibrarian.RARFILE.is_rarfile(archivename):
            loggerpostprocess.debug('%s is a rar file' % archivename)
            try:
                z = lazylibrarian.RARFILE.RarFile(archivename)
            except Exception as e:
                logger.error("Failed to unrar %s: %s" % (archivename, e))
                return ''

            targetdir = os.path.join(download_dir, title + '.unpack')
            if not make_dirs(targetdir, new=True):
                logger.error("Failed to create target dir %s" % targetdir)
                return ''

            logger.debug("Created target %s" % targetdir)
            for item in z.namelist():
                if is_valid_type(item, extensions=CONFIG.get_all_types_list()) and not item.endswith('/'):
                    # not if it's a directory
                    logger.debug('Extracting %s to %s' % (item, targetdir))
                    if os.path.__name__ == 'ntpath':
                        dst = os.path.join(targetdir, item.replace('/', '\\'))
                    else:
                        dst = os.path.join(targetdir, item)
                    dstdir = os.path.dirname(dst)
                    if not make_dirs(dstdir):
                        logger.error("Failed to create directory %s" % dstdir)
                        return ''
                    with open(syspath(dst), "wb") as f:
                        f.write(z.read(item))

        elif lazylibrarian.UNRARLIB == 2:
            # noinspection PyBroadException
            try:
                z = lazylibrarian.RARFILE(archivename)
                loggerpostprocess.debug('%s is a rar file' % archivename)
            except Exception as e:
                if archivename.endswith('.rar'):
                    logger.debug(str(e))
                z = None  # not a rar archive

            if z:
                targetdir = os.path.join(download_dir, title + '.unpack')
                if not make_dirs(targetdir, new=True):
                    logger.error("Failed to create target dir %s" % targetdir)
                    return ''

                logger.debug("Created target %s" % targetdir)
                wanted_files = []
                for item in z.infoiter():
                    if not item.isdir and is_valid_type(item.filename, extensions=CONFIG.get_all_types_list()):
                        wanted_files.append(item.filename)

                data = z.read_files("*")
                for entry in data:
                    for item in wanted_files:
                        if entry[0].filename.endswith(item):
                            logger.debug('Extracting %s to %s' % (item, targetdir))
                            if os.path.__name__ == 'ntpath':
                                dst = os.path.join(targetdir, item.replace('/', '\\'))
                            else:
                                dst = os.path.join(targetdir, item)
                            dstdir = os.path.dirname(dst)
                            if not make_dirs(dstdir):
                                logger.error("Failed to create directory %s" % dstdir)
                            else:
                                with open(syspath(dst), "wb") as f:
                                    f.write(entry[1])
                            break
        if not targetdir:
            loggerpostprocess.debug("[%s] doesn't look like an archive we can unpack" % archivename)
            return ''

        return targetdir

    except Exception:
        logger.error('Unhandled exception in unpack_archive: %s' % traceback.format_exc())
        return ''


def PostProcessor():  # was cron_process_dir
    if lazylibrarian.STOPTHREADS:
        logger = logging.getLogger(__name__)
        logger.debug("STOPTHREADS is set, not starting postprocessor")
        schedule_job(SchedulerCommand.STOP, target='PostProcessor')
    else:
        process_dir()


def book_type(book):
    booktype = book['AuxInfo']
    if booktype not in ['AudioBook', 'eBook', 'comic']:
        if not booktype:
            booktype = 'eBook'
        else:
            booktype = 'Magazine'
    return booktype


def process_dir(reset=False, startdir=None, ignoreclient=False, downloadid=None):
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    loggerfuzz = logging.getLogger('special.fuzz')
    status = {'status': 'failed'}
    count = 0
    for threadname in [n.name for n in [t for t in threading.enumerate()]]:
        if threadname == 'POSTPROCESSOR':
            count += 1

    threadname = thread_name()
    if threadname == 'POSTPROCESSOR':
        count -= 1
    if count:
        logger.debug("POSTPROCESSOR is already running")
        status['status'] = 'running'
        return status

    thread_name("POSTPROCESS")
    db = database.DBConnection()
    # noinspection PyBroadException,PyStatementEffect
    try:
        ppcount = 0
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        skipped_extensions = get_list(CONFIG['SKIPPED_EXT'])
        if startdir:
            templist = [startdir]
        else:
            templist = get_list(CONFIG['DOWNLOAD_DIR'], ',')
            if len(templist) and get_directory("Download") != templist[0]:
                templist.insert(0, get_directory("Download"))
        dirlist = []
        for item in templist:
            if path_isdir(item):
                dirlist.append(item)
            else:
                logger.debug("[%s] is not a directory" % item)

        if not dirlist:
            logger.error("No download directories are configured")
        if downloadid:
            snatched = db.select('SELECT * from wanted WHERE DownloadID=? AND Status="Snatched"', (downloadid,))
        else:
            snatched = db.select('SELECT * from wanted WHERE Status="Snatched"')
        logger.debug('Found %s %s marked "Snatched"' % (len(snatched), plural(len(snatched), "file")))
        if len(snatched):
            TELEMETRY.record_usage_data('Process/Snatched')
            for book in snatched:
                # see if we can get current status from the downloader as the name
                # may have been changed once magnet resolved, or download started or completed
                # depending on torrent downloader. Usenet doesn't change the name. We like usenet.
                matchtitle = unaccented(book['NZBtitle'], only_ascii=False)
                dlname = get_download_name(matchtitle, book['Source'], book['DownloadID'])

                if dlname and dlname != matchtitle:
                    if book['Source'] == 'SABNZBD':
                        logger.warning("%s unexpected change [%s] to [%s]" % (book['Source'], matchtitle, dlname))
                    logger.debug("%s Changing [%s] to [%s]" % (book['Source'], matchtitle, dlname))
                    # should we check against reject word list again as the name has changed?
                    db.action('UPDATE wanted SET NZBtitle=? WHERE NZBurl=?', (dlname, book['NZBurl']))
                    matchtitle = dlname

                booktype = book_type(book)

                # here we could also check percentage downloaded or eta or status?
                # If downloader says it hasn't completed, no need to look for it.
                rejected = check_contents(book['Source'], book['DownloadID'], booktype, matchtitle)
                if rejected:
                    # change status to "Failed", and ask downloader to delete task and files
                    # Only reset book status to wanted if still snatched in case another download task succeeded
                    if book['BookID'] != 'unknown':
                        cmd = ''
                        if booktype == 'eBook':
                            cmd = 'UPDATE books SET status="Wanted" WHERE status="Snatched" and BookID=?'
                        elif booktype == 'AudioBook':
                            cmd = 'UPDATE books SET audiostatus="Wanted" WHERE audiostatus="Snatched" and BookID=?'
                        if cmd:
                            db.action(cmd, (book['BookID'],))
                        db.action('UPDATE wanted SET Status="Failed",DLResult=? WHERE BookID=?',
                                  (rejected, book['BookID']))
                        if CONFIG.get_bool('DEL_FAILED'):
                            delete_task(book['Source'], book['DownloadID'], True)
                else:
                    _ = get_download_progress(book['Source'], book['DownloadID'])  # set completion time
                    dlfolder = get_download_folder(book['Source'], book['DownloadID'])
                    if dlfolder:
                        match = False
                        for download_dir in dirlist:
                            if dlfolder.startswith(download_dir):
                                match = True
                                break
                        if not match:
                            logger.debug("%s is downloading to [%s]" % (book['Source'], dlfolder))

        for download_dir in dirlist:
            try:
                downloads = listdir(download_dir)
            except OSError as why:
                logger.error('Could not access directory [%s] %s' % (download_dir, why.strerror))
                thread_name("WEBSERVER")
                return status

            logger.debug('Found %s %s in %s' % (len(downloads), plural(len(downloads), "file"), download_dir))

            # any books left to look for...

            if downloadid:
                snatched = db.select('SELECT * from wanted WHERE DownloadID=? AND Status="Snatched"', (downloadid,))
            else:
                snatched = db.select('SELECT * from wanted WHERE Status="Snatched"')
            if len(snatched):
                for book in snatched:
                    # check if we need to wait awhile before processing, might be copying/unpacking/moving
                    delay = CONFIG.get_int('PP_DELAY')
                    if delay:
                        completion = time.time() - check_int(book['Completed'], 0)
                        completion = int(-(-completion // 1))  # round up to int
                        if completion < delay:
                            logger.warning("Ignoring %s as completion was %s %s ago" %
                                           (book['NZBtitle'], completion, plural(completion, 'second')))
                            continue
                        elif check_int(book['Completed'], 0):
                            logger.debug("%s was completed %s %s ago" %
                                         (book['NZBtitle'], completion, plural(completion, 'second')))

                    book = dict(book)  # so we can modify values later
                    booktype = book_type(book)
                    # remove accents and convert not-ascii apostrophes
                    matchtitle = unaccented(book['NZBtitle'], only_ascii=False)
                    # torrent names might have words_separated_by_underscores
                    matchtitle = matchtitle.split(' LL.(')[0].replace('_', ' ')
                    # strip noise characters
                    matchtitle = sanitize(matchtitle).strip()
                    matches = []
                    logger.debug('Looking for %s %s in %s' % (booktype, matchtitle, download_dir))

                    for fname in downloads:
                        # skip if failed before or incomplete torrents, or incomplete btsync etc
                        loggerpostprocess.debug("Checking extn on %s" % fname)
                        extn = os.path.splitext(fname)[1]
                        if not extn or extn.strip('.') not in skipped_extensions:
                            # This is to get round differences in torrent filenames.
                            # Usenet is ok, but Torrents aren't always returned with the name we searched for
                            # We ask the torrent downloader for the torrent name, but don't always get an answer,
                            # so we try to do a "best match" on the name, there might be a better way...
                            matchname = unaccented(fname, only_ascii=False)
                            matchname = matchname.split(' LL.(')[0].replace('_', ' ')
                            matchname = sanitize(matchname)
                            match = fuzz.token_set_ratio(matchtitle, matchname)
                            pp_path = ''
                            loggerfuzz.debug("%s%% match %s : %s" % (match, matchtitle, matchname))
                            if match >= CONFIG.get_int('DLOAD_RATIO'):
                                # matching file or folder name
                                pp_path = os.path.join(download_dir, fname)
                            elif path_isdir(os.path.join(download_dir, fname)):
                                # obfuscated folder might contain our file
                                for f in listdir(os.path.join(download_dir, fname)):
                                    if is_valid_type(f, extensions=CONFIG.get_all_types_list(), extras='cbr, cbz'):
                                        matchname = unaccented(f, only_ascii=False)
                                        matchname = matchname.split(' LL.(')[0].replace('_', ' ')
                                        matchname = sanitize(matchname)
                                        match = fuzz.token_set_ratio(matchtitle, matchname)
                                        loggerfuzz.debug("%s%% match %s : %s" % (match, matchtitle, matchname))
                                        if match >= CONFIG.get_int('DLOAD_RATIO'):
                                            # found matching file in this folder
                                            pp_path = os.path.join(download_dir, fname)
                                            break

                            if match >= CONFIG.get_int('DLOAD_RATIO'):
                                loggerpostprocess.debug("process_dir found %s %s" % (type(pp_path), repr(pp_path)))

                                if path_isfile(pp_path):
                                    # Check for single file downloads first. Book/mag file in download root.
                                    # move the file into its own subdirectory, so we don't move/delete
                                    # things that aren't ours
                                    # note that epub are zipfiles so check booktype first
                                    # and don't unpack cbr/cbz comics'
                                    if is_valid_type(fname, extensions=CONFIG.get_all_types_list(), extras='cbr, cbz'):
                                        loggerpostprocess.debug('file [%s] is a valid book/mag' % fname)
                                        if bts_file(download_dir):
                                            logger.debug("Skipping %s, found a .bts file" % download_dir)
                                        else:
                                            aname = os.path.splitext(fname)[0]
                                            while aname[-1] in '_. ':
                                                aname = aname[:-1]

                                            if CONFIG.get_bool('DESTINATION_COPY') or \
                                                    (book['NZBmode'] in ['torrent', 'magnet', 'torznab'] and
                                                     CONFIG.get_bool('KEEP_SEEDING')):
                                                move = 'copy'
                                            else:
                                                move = 'move'

                                            targetdir = os.path.join(download_dir, aname + '.unpack')
                                            if make_dirs(targetdir, new=True):
                                                logger.debug("Created target %s" % targetdir)
                                                cnt = move_into_subdir(download_dir, targetdir, aname, move=move)
                                                if cnt:
                                                    pp_path = targetdir
                                                else:
                                                    try:
                                                        os.rmdir(targetdir)
                                                    except OSError as why:
                                                        logger.warning("Unable to delete %s: %s" %
                                                                       (targetdir, why.strerror))
                                            else:
                                                logger.debug("Unable to make directory %s" % targetdir)
                                    else:
                                        # Is file an archive, if so look inside and extract to new dir
                                        res = unpack_archive(pp_path, download_dir, matchtitle)
                                        if res:
                                            pp_path = res
                                        else:
                                            logger.debug('Skipping unhandled file %s' % fname)

                                if path_isdir(pp_path):
                                    logger.debug('Found folder (%s%%) [%s] for %s %s' %
                                                 (match, pp_path, booktype, matchtitle))
                                    # some magazines are packed as multipart zip files, each zip contains a rar file
                                    # and the rar files need assembling into the final magazine
                                    zipfiles = 0
                                    for f in listdir(pp_path):
                                        archivename = os.path.join(pp_path, f)
                                        if zipfile.is_zipfile(archivename):
                                            zipfiles += 1
                                    if zipfiles > 1:
                                        pp_path = unpack_multipart(pp_path, download_dir, matchtitle)

                                    # folder name matches, look in subdirectories for a filename of a valid type
                                    for r, _, f in walk(pp_path):
                                        for item in f:
                                            if is_valid_type(item, extensions=CONFIG.get_all_types_list(), extras='cbr, cbz'):
                                                pp_path = os.path.dirname(os.path.join(r, item))
                                                break

                                    skipped = False

                                    if booktype == 'eBook':
                                        # Might be multiple books in the download, could be a collection?
                                        # If so, should we process all the books recursively? we can maybe use
                                        # process_alternate(pp_path) but that currently only does ebooks,
                                        # not audio or mag, or should we just try to find and extract
                                        # the one wanted item from the collection?
                                        # For now, try to find best match and only copy that book
                                        mult = multibook(pp_path, recurse=True)
                                        if mult:
                                            skipped = True
                                            found_file = None
                                            found_score = 0
                                            # find the best match
                                            for f in listdir(pp_path):
                                                if CONFIG.is_valid_booktype(f, booktype="book"):
                                                    bookmatch = fuzz.token_set_ratio(matchtitle, f)
                                                    loggerfuzz.debug("%s%% match %s : %s" % (bookmatch,
                                                                                             matchtitle, f))
                                                    if bookmatch > found_score:
                                                        found_file = f
                                                        found_score = bookmatch

                                            if found_score >= CONFIG.get_int('DLOAD_RATIO'):
                                                # found a matching book file in this folder
                                                targetdir = os.path.join(download_dir, matchtitle + '.unpack')
                                                if not make_dirs(targetdir, new=True):
                                                    logger.error("Failed to create target dir %s" % targetdir)
                                                else:
                                                    logger.debug("Found %s (%s%%) for %s" % (found_file,
                                                                                             found_score,
                                                                                             matchtitle))
                                                    found_file, _ = os.path.splitext(found_file)
                                                    # copy all valid types of this title, plus opf, jpg
                                                    for f in listdir(pp_path):
                                                        base, extn = os.path.splitext(f)
                                                        if base == found_file:
                                                            if CONFIG.is_valid_booktype(f, booktype="book") or \
                                                                    extn in ['.opf', '.jpg']:
                                                                shutil.copyfile(os.path.join(pp_path, f),
                                                                                os.path.join(targetdir, f))
                                                    pp_path = targetdir
                                                    skipped = False
                                            if skipped:
                                                logger.debug("Skipping %s, found multiple %s with no good match" %
                                                             (pp_path, mult))
                                        else:
                                            result = book_file(pp_path, 'ebook', recurse=True, config=CONFIG)
                                            if result:
                                                pp_path = os.path.dirname(result)
                                            else:
                                                logger.debug("Skipping %s, no ebook found" % pp_path)
                                                skipped = True
                                    elif booktype == 'AudioBook':
                                        result = book_file(pp_path, 'audiobook', recurse=True, config=CONFIG)
                                        if result:
                                            pp_path = os.path.dirname(result)
                                        else:
                                            logger.debug("Skipping %s, no audiobook found" % pp_path)
                                            skipped = True
                                    elif booktype == 'Magazine':
                                        result = book_file(pp_path, 'mag', recurse=True, config=CONFIG)
                                        if result:
                                            pp_path = os.path.dirname(result)
                                        else:
                                            logger.debug("Skipping %s, no magazine found" % pp_path)
                                            skipped = True
                                    if not listdir(pp_path):
                                        logger.debug("Skipping %s, folder is empty" % pp_path)
                                        skipped = True
                                    elif bts_file(pp_path):
                                        logger.debug("Skipping %s, found a .bts file" % pp_path)
                                        skipped = True
                                    if not skipped:
                                        matches.append([match, pp_path, book])
                                        if match == 100:  # no point looking any further
                                            break
                            else:
                                pp_path = os.path.join(download_dir, fname)
                                matches.append([match, pp_path, book])  # so we can report the closest match
                        else:
                            logger.debug('Skipping %s' % fname)

                    match = 0
                    pp_path = ''
                    dest_path = ''
                    bookname = ''
                    global_name = ''
                    mostrecentissue = ''
                    data = None
                    if matches:
                        highest = max(matches, key=lambda x: x[0])
                        match = highest[0]
                        pp_path = highest[1]
                        book = highest[2]  # type: dict
                    if match and match >= CONFIG.get_int('DLOAD_RATIO'):
                        logger.debug('Found match (%s%%): %s for %s %s' % (
                            match, repr(pp_path), booktype, repr(book['NZBtitle'])))
                        cmd = 'SELECT AuthorName,BookName from books,authors WHERE BookID=?'
                        cmd += ' and books.AuthorID = authors.AuthorID'
                        data = db.match(cmd, (book['BookID'],))
                        if data:  # it's ebook/audiobook
                            logger.debug('Processing %s %s' % (booktype, book['BookID']))
                            bookname = data['BookName']
                            authorname = data['AuthorName']
                            # CFG2DO: Verify that this path handling code is no longer necessary
                            # if os.name == 'nt':
                            #     if '/' in lazylibrarian.CONFIG['EBOOK_DEST_FOLDER']:
                            #         logger.warning('Please check your EBOOK_DEST_FOLDER setting')
                            #         lazylibrarian.CONFIG['EBOOK_DEST_FOLDER'] = lazylibrarian.CONFIG[
                            #             'EBOOK_DEST_FOLDER'].replace('/', '\\')
                            #     if '/' in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER']:
                            #         logger.warning('Please check your AUDIOBOOK_DEST_FOLDER setting')
                            #         lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER'] = lazylibrarian.CONFIG[
                            #             'AUDIOBOOK_DEST_FOLDER'].replace('/', '\\')
                            # Default destination path, should be allowed change per config file.
                            namevars = name_vars(book['BookID'])
                            if booktype == 'AudioBook' and get_directory('Audio'):
                                dest_path = namevars['AudioFolderName']
                                dest_dir = get_directory('Audio')
                            else:
                                dest_path = namevars['FolderName']
                                dest_dir = get_directory('eBook')

                            dest_path = stripspaces(os.path.join(dest_dir, dest_path))
                            dest_path = make_utf8bytes(dest_path)[0]
                            global_name = namevars['BookFile']
                            global_name = sanitize(global_name)
                            data = {'AuthorName': authorname, 'BookName': bookname, 'BookID': book['BookID']}
                        else:
                            data = db.match('SELECT * from magazines WHERE Title=?', (book['BookID'],))
                            if data:  # it's a magazine
                                booktype = 'magazine'
                                logger.debug('Processing magazine %s' % book['BookID'])
                                # AuxInfo was added for magazine release date, normally housed in 'magazines'
                                # but if multiple files are downloading, there will be an error in post-processing
                                # trying to go to the same directory.
                                mostrecentissue = data['IssueDate']  # keep for processing issues arriving out of order
                                mag_name = unaccented(sanitize(book['BookID']), only_ascii=False)
                                # book auxinfo is a cleaned date, e.g. 2015-01-01
                                iss_date = book['AuxInfo']
                                if iss_date == '1970-01-01':
                                    logger.debug("Looks like an invalid or missing date, retrying %s" %
                                                 book['NZBtitle'])
                                    dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '',
                                           '[': ' ', ']': ' ', '#': '# '}
                                    regex_pass, issuedate, year = lazylibrarian.searchmag.get_issue_date(
                                        replace_all(book['NZBtitle'].lower(), dic).split(), data['DateType'])
                                    if regex_pass:
                                        if regex_pass in [10, 12, 13] and year:
                                            iss_date = "%s%04d" % (year, int(issuedate))
                                        elif str(issuedate).isdigit():
                                            iss_date = str(issuedate).zfill(4)
                                        else:
                                            iss_date = issuedate
                                # suppress the "-01" day on monthly magazines
                                # if re.match(r'\d+-\d\d-01', str(iss_date)):
                                #    iss_date = iss_date[:-3]
                                book['AuxInfo'] = iss_date
                                dest_path = CONFIG['MAG_DEST_FOLDER'].replace(
                                    '$IssueDate', iss_date).replace('$Title', mag_name)

                                if CONFIG.get_bool('MAG_RELATIVE'):
                                    dest_dir = get_directory('eBook')
                                    dest_path = stripspaces(os.path.join(dest_dir, dest_path))
                                    dest_path = make_utf8bytes(dest_path)[0]
                                else:
                                    dest_path = make_utf8bytes(dest_path)[0]

                                if not dest_path or not make_dirs(dest_path):
                                    logger.warning('Unable to create directory %s' % dest_path)

                                if '$IssueDate' in CONFIG['MAG_DEST_FILE']:
                                    global_name = CONFIG['MAG_DEST_FILE'].replace(
                                        '$IssueDate', iss_date).replace('$Title', mag_name)
                                else:
                                    global_name = "%s %s" % (mag_name, iss_date)
                                global_name = unaccented(global_name, only_ascii=False)
                                global_name = sanitize(global_name)
                                data = {'Title': mag_name, 'IssueDate': iss_date, 'BookID': book['BookID']}
                            else:
                                if book['BookID'] and '_' in book['BookID']:
                                    comicid, issueid = book['BookID'].split('_')
                                    data = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
                                else:
                                    comicid = ''
                                    issueid = 0
                                    data = None
                                if data:  # it's a comic
                                    booktype = 'comic'
                                    logger.debug('Processing %s issue %s' % (data['Title'], issueid))
                                    mostrecentissue = data['LatestIssue']
                                    comic_name = unaccented(sanitize(data['Title']), only_ascii=False)
                                    dest_path = CONFIG['COMIC_DEST_FOLDER'].replace(
                                        '$Issue', issueid).replace(
                                        '$Publisher', data['Publisher']).replace(
                                        '$Title', comic_name)

                                    global_name = "%s %s" % (comic_name, issueid)
                                    global_name = unaccented(global_name, only_ascii=False)
                                    global_name = sanitize(global_name)
                                    data = {'Title': comic_name, 'IssueDate': issueid, 'BookID': comicid}

                                    if CONFIG.get_bool('COMIC_RELATIVE'):
                                        dest_dir = get_directory('eBook')
                                        dest_path = stripspaces(os.path.join(dest_dir, dest_path))
                                        dest_path = make_utf8bytes(dest_path)[0]
                                    else:
                                        dest_path = make_utf8bytes(dest_path)[0]

                                    if not make_dirs(dest_path):
                                        logger.warning('Unable to create directory %s' % dest_path)

                                else:  # not recognised, maybe deleted
                                    emsg = 'Nothing in database matching "%s"' % book['BookID']
                                    logger.debug(emsg)
                                    control_value_dict = {"BookID": book['BookID'], "Status": "Snatched"}
                                    new_value_dict = {"Status": "Failed", "NZBDate": now(), "DLResult": emsg}
                                    db.upsert("wanted", new_value_dict, control_value_dict)
                                    data = None
                    else:
                        logger.debug("Snatched %s %s is not in download directory" %
                                     (book['NZBmode'], book['NZBtitle']))
                        if match:
                            logger.debug('Closest match (%s%%): %s' % (match, pp_path))
                            for match in matches:
                                loggerfuzz.debug('Match: %s%%  %s' % (match[0], match[1]))

                    if not dest_path:
                        continue

                    data['NZBmode'] = book['NZBmode']
                    success, dest_file, pp_path = process_destination(pp_path, dest_path, global_name, data, booktype)
                    if success:
                        logger.debug("Processed %s (%s): %s, %s" % (book['NZBmode'], pp_path, global_name,
                                                                    book['NZBurl']))
                        dest_file = make_unicode(dest_file)
                        # only update the snatched ones in case some already marked failed/processed in history
                        control_value_dict = {"NZBurl": book['NZBurl'], "Status": "Snatched"}
                        new_value_dict = {"Status": "Processed", "NZBDate": now(), "DLResult": dest_file}
                        db.upsert("wanted", new_value_dict, control_value_dict)
                        status['status'] = 'success'
                        issueid = 0
                        if bookname and dest_file:  # it's ebook or audiobook, and we know the location
                            process_extras(dest_file, global_name, book['BookID'], booktype)
                        elif booktype == 'comic':
                            comicid = data.get('BookID', '')
                            issueid = data.get('IssueDate', 0)
                            if comicid:
                                if mostrecentissue:
                                    older = (int(mostrecentissue) > int(issueid))
                                else:
                                    older = False

                                coverfile = create_mag_cover(dest_file, refresh=True)
                                if coverfile:
                                    myhash = uuid.uuid4().hex
                                    hashname = os.path.join(DIRS.CACHEDIR, 'comic', '%s.jpg' % myhash)
                                    shutil.copyfile(coverfile, hashname)
                                    setperm(hashname)
                                    coverfile = 'cache/comic/%s.jpg' % myhash
                                    createthumbs(hashname)

                                control_value_dict = {"ComicID": comicid}
                                if older:  # check this in case processing issues arriving out of order
                                    new_value_dict = {"LastAcquired": today(),
                                                      "IssueStatus": CONFIG['FOUND_STATUS']}
                                else:
                                    new_value_dict = {"LatestIssue": issueid, "LastAcquired": today(),
                                                      "LatestCover": coverfile,
                                                      "IssueStatus": CONFIG['FOUND_STATUS']}
                                db.upsert("comics", new_value_dict, control_value_dict)
                                control_value_dict = {"ComicID": comicid, "IssueID": issueid}
                                new_value_dict = {"IssueAcquired": today(),
                                                  "IssueFile": dest_file,
                                                  "Cover": coverfile
                                                  }
                                db.upsert("comicissues", new_value_dict, control_value_dict)
                        elif not bookname:  # magazine
                            if mostrecentissue:
                                if mostrecentissue.isdigit() and str(book['AuxInfo']).isdigit():
                                    older = (int(mostrecentissue) > int(book['AuxInfo']))  # issuenumber
                                else:
                                    older = (mostrecentissue > book['AuxInfo'])  # YYYY-MM-DD
                            else:
                                older = False

                            maginfo = db.match("SELECT CoverPage from magazines WHERE Title=?", (book['BookID'],))
                            # create a thumbnail cover for the new issue
                            if CONFIG.get_bool('SWAP_COVERPAGE'):
                                coverpage = 1
                            else:
                                coverpage = check_int(maginfo['CoverPage'], 1)
                            coverfile = create_mag_cover(dest_file, pagenum=coverpage)
                            if coverfile:
                                myhash = uuid.uuid4().hex
                                hashname = os.path.join(DIRS.CACHEDIR, 'magazine', '%s.jpg' % myhash)
                                shutil.copyfile(coverfile, hashname)
                                setperm(hashname)
                                coverfile = 'cache/magazine/%s.jpg' % myhash
                                createthumbs(hashname)

                            issueid = create_id("%s %s" % (book['BookID'], book['AuxInfo']))
                            control_value_dict = {"Title": book['BookID'], "IssueDate": book['AuxInfo']}
                            new_value_dict = {"IssueAcquired": today(),
                                              "IssueFile": dest_file,
                                              "IssueID": issueid,
                                              "Cover": coverfile
                                              }
                            db.upsert("issues", new_value_dict, control_value_dict)

                            control_value_dict = {"Title": book['BookID']}
                            if older:  # check this in case processing issues arriving out of order
                                new_value_dict = {"LastAcquired": today(),
                                                  "IssueStatus": CONFIG['FOUND_STATUS']}
                            else:
                                new_value_dict = {"LastAcquired": today(),
                                                  "IssueStatus": CONFIG['FOUND_STATUS'],
                                                  "IssueDate": book['AuxInfo'],
                                                  "LatestCover": coverfile
                                                  }
                            db.upsert("magazines", new_value_dict, control_value_dict)

                            if not CONFIG.get_bool('IMP_MAGOPF'):
                                logger.debug('create_mag_opf is disabled')
                            else:
                                if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                                    authors = book['BookID']
                                else:
                                    authors = 'magazines'
                                _ = create_mag_opf(dest_file, authors, book['BookID'], book['AuxInfo'], issueid,
                                                   overwrite=True)
                            if CONFIG['IMP_AUTOADDMAG']:
                                dest_path = os.path.dirname(dest_file)
                                process_auto_add(dest_path, booktype='mag')

                        # calibre or ll copied/moved the files we want, now delete source files
                        logger.debug("Copied %s %s %s" % (pp_path, ignoreclient, book['NZBmode']))
                        to_delete = True
                        # Only delete torrents if seeding is complete - examples from radarr
                        # DELUGE CanBeRemoved = (torrent.IsAutoManaged && torrent.StopAtRatio &&
                        # torrent.Ratio >= torrent.StopRatio && torrent.State == DelugeTorrentStatus.Paused);
                        # TRANSMISSION CanBeRemoved = torrent.Status == TransmissionTorrentStatus.Stopped;
                        # RTORRENT No stop ratio data is present, so do not delete CanBeRemoved = false;
                        # UTORRENT CanBeRemoved = (!torrent.Status.HasFlag(UTorrentTorrentStatus.Queued) &&
                        # !torrent.Status.HasFlag(UTorrentTorrentStatus.Started));
                        # DOWNLOADSTATION CanBeRemoved = DownloadStationTaskStatus.Finished;
                        # QBITTORRENT CanBeRemoved = (!config.MaxRatioEnabled || config.MaxRatio <= torrent.Ratio) &&
                        # torrent.State == "pausedUP";

                        if ignoreclient is False and to_delete:
                            # ask downloader to delete the torrent, but not the files
                            # we may delete them later, depending on other settings
                            if not book['Source']:
                                logger.warning("Unable to remove %s, no source" % book['NZBtitle'])
                            elif not book['DownloadID'] or book['DownloadID'] == "unknown":
                                logger.warning("Unable to remove %s from %s, no DownloadID" %
                                               (book['NZBtitle'], book['Source']))
                            elif book['Source'] != 'DIRECT':
                                progress, finished = get_download_progress(book['Source'], book['DownloadID'])
                                logger.debug("Progress for %s %s/%s" % (book['NZBtitle'], progress, finished))
                                if progress == 100 and finished:
                                    if book['NZBmode'] in ['torrent', 'magnet', 'torznab'] and \
                                            CONFIG.get_bool('KEEP_SEEDING'):
                                        cmd = 'UPDATE wanted SET Status="Seeding" WHERE NZBurl=? and Status="Processed"'
                                        db.action(cmd, (book['NZBurl'],))
                                        logger.debug('%s still seeding at %s' % (book['NZBtitle'], book['Source']))
                                    elif CONFIG.get_bool('DEL_COMPLETED'):
                                        logger.debug('Deleting completed %s from %s' % (book['NZBtitle'],
                                                                                        book['Source']))
                                        delete_task(book['Source'], book['DownloadID'], False)
                                elif progress < 0:
                                    logger.debug('%s not found at %s' % (book['NZBtitle'], book['Source']))

                        # only delete the files if not in download root dir and DESTINATION_COPY not set
                        if '.unpack' in pp_path:  # always delete files we unpacked
                            pp_path = pp_path.split('.unpack')[0] + '.unpack'
                            to_delete = True
                        elif CONFIG.get_bool('DESTINATION_COPY'):
                            to_delete = False
                        if pp_path == download_dir.rstrip(os.sep):
                            to_delete = False
                        logger.debug("To Delete: %s %s" % (pp_path, to_delete))
                        if to_delete:
                            # walk up any subdirectories
                            if pp_path.startswith(download_dir) and '.unpack' not in pp_path:
                                logger.debug("[%s][%s]" % (pp_path, download_dir))
                                while os.path.dirname(pp_path) != download_dir.rstrip(os.sep):
                                    pp_path = os.path.dirname(pp_path)
                            try:
                                shutil.rmtree(pp_path, ignore_errors=True)
                                logger.debug('Deleted %s for %s, %s from %s' %
                                             (pp_path, book['NZBtitle'], book['NZBmode'],
                                              book['Source']))
                            except Exception as why:
                                logger.warning("Unable to remove %s, %s %s" %
                                               (pp_path, type(why).__name__, str(why)))
                        else:
                            if CONFIG.get_bool('DESTINATION_COPY'):
                                logger.debug("Not removing %s as Keep Files is set" % pp_path)
                            else:
                                logger.debug("Not removing %s as in download root" % pp_path)

                        logger.info('Successfully processed: %s' % global_name)

                        ppcount += 1
                        dispname = CONFIG.disp_name(book['NZBprov'])
                        if CONFIG.get_bool('NOTIFY_WITH_TITLE'):
                            dispname = "%s: %s" % (dispname, book['NZBtitle'])
                        if CONFIG.get_bool('NOTIFY_WITH_URL'):
                            dispname = "%s: %s" % (dispname, book['NZBurl'])
                        if bookname:
                            custom_notify_download("%s %s" % (book['BookID'], booktype))
                            notify_download("%s %s from %s at %s" %
                                            (booktype, global_name, dispname, now()), book['BookID'])
                            mailing_list(booktype, global_name, book['BookID'])
                        else:
                            custom_notify_download("%s %s" % (book['BookID'], book['NZBurl']))
                            notify_download("%s %s from %s at %s" %
                                            (booktype, global_name, dispname, now()), issueid)
                            mailing_list(booktype, global_name, issueid)

                        update_downloads(book['NZBprov'])
                    else:
                        logger.error('Postprocessing for %s has failed: %s' % (repr(global_name), repr(dest_file)))
                        dispname = CONFIG.disp_name(book['NZBprov'])
                        custom_notify_snatch("%s %s" % (book['BookID'], booktype), fail=True)
                        notify_snatch("%s %s from %s at %s" %
                                      (booktype, global_name, dispname, now()), fail=True)
                        control_value_dict = {"NZBurl": book['NZBurl'], "Status": "Snatched"}
                        new_value_dict = {"Status": "Failed", "DLResult": make_unicode(dest_file), "NZBDate": now()}
                        db.upsert("wanted", new_value_dict, control_value_dict)
                        # if it's a book, reset status, so we try for a different version
                        # if it's a magazine, user can select a different one from pastissues table
                        if booktype == 'eBook':
                            db.action('UPDATE books SET status="Wanted" WHERE BookID=?', (book['BookID'],))
                        elif booktype == 'AudioBook':
                            db.action('UPDATE books SET audiostatus="Wanted" WHERE BookID=?', (book['BookID'],))

                        # at this point, as it failed we should move it, or it will get postprocessed
                        # again (and fail again)
                        if CONFIG.get_bool('DEL_DOWNLOADFAILED'):
                            logger.debug('Deleting %s' % pp_path)
                            shutil.rmtree(pp_path, ignore_errors=True)
                        else:
                            shutil.rmtree(pp_path + '.fail', ignore_errors=True)
                            try:
                                _ = safe_move(pp_path, pp_path + '.fail')
                                logger.warning('Residual files remain in %s.fail' % pp_path)
                            except Exception as why:
                                logger.error("Unable to rename %s, %s %s" %
                                             (repr(pp_path), type(why).__name__, str(why)))
                                if not os.access(syspath(pp_path), os.R_OK):
                                    logger.error("%s is not readable" % repr(pp_path))
                                if not os.access(syspath(pp_path), os.W_OK):
                                    logger.error("%s is not writeable" % repr(pp_path))
                                if not os.access(syspath(pp_path), os.X_OK):
                                    logger.error("%s is not executable" % repr(pp_path))
                                parent = os.path.dirname(pp_path)
                                try:
                                    with open(syspath(os.path.join(parent, 'll_temp')), 'w', encoding='utf-8') as f:
                                        f.write(u'test')
                                    remove_file(os.path.join(parent, 'll_temp'))
                                except Exception as why:
                                    logger.error("Parent Directory %s is not writeable: %s" % (parent, why))
                                logger.warning('Residual files remain in %s' % pp_path)

            ppcount += check_residual(download_dir)

        logger.info('%s %s processed.' % (ppcount, plural(ppcount, "download")))

        # Now check for any that are still marked snatched, seeding, or any aborted...
        cmd = 'SELECT * from wanted WHERE Status IN ("Snatched", "Aborted", "Seeding")'
        snatched = db.select(cmd)
        logger.info("Found %s unprocessed" % len(snatched))
        for book in snatched:
            booktype = book_type(book)
            abort = False
            hours = 0
            mins = 0
            progress = 'Unknown'
            finished = False
            loggerpostprocess.debug("%s %s %s" % (book['Status'], book['Source'], book['NZBtitle']))
            if book['Status'] == "Aborted":
                abort = True
            else:
                progress, finished = get_download_progress(book['Source'], book['DownloadID'])

            if book['Status'] == "Seeding":
                loggerpostprocess.debug("Progress:%s Finished:%s Waiting:%s" % (progress, finished,
                                                                                CONFIG.get_bool('SEED_WAIT')))
                if not CONFIG.get_bool('KEEP_SEEDING') and (finished or progress < 0
                                                            and not CONFIG.get_bool('SEED_WAIT')):
                    if finished:
                        logger.debug('%s finished seeding at %s' % (book['NZBtitle'], book['Source']))
                    else:
                        logger.debug('%s not seeding at %s' % (book['NZBtitle'], book['Source']))
                    if CONFIG.get_bool('DEL_COMPLETED'):
                        logger.debug("Removing seeding completed %s from %s" % (book['NZBtitle'], book['Source']))
                        if CONFIG.get_bool('DESTINATION_COPY'):
                            delfiles = False
                        else:
                            delfiles = True
                        delete_task(book['Source'], book['DownloadID'], delfiles)
                    if book['BookID'] != 'unknown':
                        cmd = 'UPDATE wanted SET status="Processed",NZBDate=? WHERE status="Seeding" and BookID=?'
                        db.action(cmd, (now(), book['BookID']))
                        abort = False
                    # only delete the files if not in download root dir and DESTINATION_COPY not set
                    # This is for downloaders (rtorrent) that don't let us tell them to delete files
                    # NOTE it will silently fail if the torrent client downloadfolder is not local
                    # e.g. in a docker or on a remote machine
                    pp_path = get_download_folder(book['Source'], book['DownloadID'])
                    if CONFIG.get_bool('DESTINATION_COPY'):
                        logger.debug("Not removing original files as Keep Files is set")
                    elif pp_path in get_list(CONFIG['DOWNLOAD_DIR']):
                        logger.debug("Not removing original files as in download root")
                    else:
                        shutil.rmtree(pp_path, ignore_errors=True)
                        logger.debug('Deleted %s for %s, %s from %s' % (pp_path, book['NZBtitle'],
                                                                        book['NZBmode'], book['Source']))
                else:
                    logger.debug('%s still seeding at %s' % (book['NZBtitle'], book['Source']))

            if book['Status'] == "Snatched":
                try:
                    when_snatched = datetime.datetime.strptime(book['NZBdate'], '%Y-%m-%d %H:%M:%S')
                    timenow = datetime.datetime.now()
                    td = timenow - when_snatched
                    diff = td.total_seconds()  # time difference in seconds
                except ValueError:
                    diff = 0
                hours = int(diff / 3600)
                mins = int(diff / 60)

                # has it been aborted (wait a short while before checking)
                if mins > 5 and progress < 0:
                    abort = True

                if CONFIG.get_int('TASK_AGE') and hours >= CONFIG.get_int('TASK_AGE'):
                    # SAB can report 100% (or more) and not finished if missing blocks and needs repair
                    if check_int(progress, 0) < 95:
                        abort = True
                    # allow a little more time for repair or if nearly finished
                    elif hours >= CONFIG.get_int('TASK_AGE') + 1:
                        abort = True
            if abort:
                dlresult = ''
                if book['Source'] and book['Source'] != 'DIRECT':
                    if book['Status'] == "Snatched":
                        progress = "%s" % progress
                        if progress.isdigit():  # could be "Unknown" or -1
                            progress += '%'
                        dlresult = '%s was  sent to %s %s hours ago. Progress: %s' % (book['NZBtitle'],
                                                                                      book['Source'],
                                                                                      hours, progress)
                    else:
                        dlresult = '%s was aborted by %s' % (book['NZBtitle'], book['Source'])

                custom_notify_snatch("%s %s" % (book['BookID'], book['Source']), fail=True)
                notify_snatch("%s from %s at %s" %
                              (book['NZBtitle'], book['Source'], now()), fail=True)

                # change status to "Failed", and ask downloader to delete task and files
                # Only reset book status to wanted if still snatched in case another download task succeeded
                if book['BookID'] != 'unknown':
                    cmd = ''
                    if booktype == 'eBook':
                        cmd = 'UPDATE books SET status="Wanted" WHERE status="Snatched" and BookID=?'
                    elif booktype == 'AudioBook':
                        cmd = 'UPDATE books SET audiostatus="Wanted" WHERE audiostatus="Snatched" and BookID=?'
                    if cmd:
                        db.action(cmd, (book['BookID'],))

                    # use url and status for identifier because magazine id isn't unique
                    if book['Status'] == "Snatched":
                        q = 'UPDATE wanted SET Status="Failed",DLResult=? WHERE NZBurl=? and Status="Snatched"'
                        db.action(q, (dlresult, book['NZBurl']))
                    else:  # don't overwrite dlresult reason for the abort
                        q = 'UPDATE wanted SET Status="Failed" WHERE NZBurl=? and Status="Aborted"'
                        db.action(q, (book['NZBurl'],))

                    if CONFIG.get_bool('DEL_FAILED'):
                        logger.warning('%s, deleting failed task' % dlresult)
                        delete_task(book['Source'], book['DownloadID'], True)
            elif mins:
                if book['Source']:
                    logger.debug('%s was sent to %s %s %s ago. Progress %s' %
                                 (book['NZBtitle'], book['Source'], mins, plural(mins, 'minute'), progress))
                else:
                    logger.debug('%s was sent somewhere?? %s minutes ago ' % (book['NZBtitle'], mins))

        db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
        # Check if postprocessor needs to run again
        snatched = db.select('SELECT * from wanted WHERE Status="Snatched"')
        seeding = db.select('SELECT * from wanted WHERE Status="Seeding"')
        from lazylibrarian.scheduling import schedule_job
        if not len(snatched) and not len(seeding):
            logger.info('Nothing marked as snatched or seeding. Stopping postprocessor.')
            schedule_job(SchedulerCommand.STOP, target='PostProcessor')
            status['status'] = 'idle'
            status['action'] = 'Stopped'
        elif len(seeding):
            logger.info('Seeding %s' % len(seeding))
            schedule_job(SchedulerCommand.RESTART, target='PostProcessor')
            status['status'] = 'seeding'
            status['action'] = 'Restarted'
        elif reset:
            schedule_job(SchedulerCommand.RESTART, target='PostProcessor')
            status['action'] = 'Restarted'
    except Exception:
        logger.error('Unhandled exception in process_dir: %s' % traceback.format_exc())
    finally:
        db.close()
        logger.debug('Returning %s' % status)
        thread_name(threadname)
        return status


def check_contents(source, downloadid, booktype, title):
    """ Check contents list of a download against various reject criteria
        name, size, filetype, banned words
        Return empty string if ok, or error message if rejected
        Error message gets logged and then passed back to history table
    """
    logger = logging.getLogger(__name__)
    rejected = ''
    banned_extensions = get_list(CONFIG['BANNED_EXT'])
    if booktype.lower() == 'ebook':
        maxsize = CONFIG.get_int('REJECT_MAXSIZE')
        minsize = CONFIG.get_int('REJECT_MINSIZE')
        filetypes = CONFIG['EBOOK_TYPE']
        banwords = CONFIG['REJECT_WORDS']
    elif booktype.lower() == 'audiobook':
        maxsize = CONFIG.get_int('REJECT_MAXAUDIO')
        # minsize = lazylibrarian.CONFIG['REJECT_MINAUDIO']
        minsize = 0  # individual audiobook chapters can be quite small
        filetypes = CONFIG['AUDIOBOOK_TYPE']
        banwords = CONFIG['REJECT_AUDIO']
    elif booktype.lower() == 'magazine':
        maxsize = CONFIG.get_int('REJECT_MAGSIZE')
        minsize = CONFIG.get_int('REJECT_MAGMIN')
        filetypes = CONFIG['MAG_TYPE']
        banwords = CONFIG['REJECT_MAGS']
    else:  # comics
        maxsize = CONFIG.get_int('REJECT_MAXCOMIC')
        minsize = CONFIG.get_int('REJECT_MINCOMIC')
        filetypes = CONFIG['COMIC_TYPE']
        banwords = CONFIG['REJECT_COMIC']

    if banwords:
        banlist = get_list(banwords, ',')
    else:
        banlist = []

    downloadfiles = get_download_files(source, downloadid)

    # Downloaders return varying amounts of info using varying names
    if not downloadfiles:  # empty
        if source not in ['DIRECT', 'NZBGET', 'SABNZBD']:  # these don't give us a contents list
            logger.debug("No filenames returned by %s for %s" % (source, title))
    else:
        logger.debug("Checking files in %s" % title)
        for entry in downloadfiles:
            fname = ''
            fsize = 0
            if 'path' in entry:  # deluge, rtorrent
                fname = entry['path']
            if 'name' in entry:  # transmission, qbittorrent
                fname = entry['name']
            if 'filename' in entry:  # utorrent, synology
                fname = entry['filename']
            if 'size' in entry:  # deluge, qbittorrent, synology, rtorrent
                fsize = entry['size']
            if 'filesize' in entry:  # utorrent
                fsize = entry['filesize']
            if 'length' in entry:  # transmission
                fsize = entry['length']
            extn = os.path.splitext(fname)[1].lstrip('.').lower()
            if extn and extn in banned_extensions:
                rejected = "%s extension %s" % (title, extn)
                logger.warning("%s. Rejecting download" % rejected)
                break

            if not rejected and banlist:
                wordlist = get_list(fname.lower().replace(os.sep, ' ').replace('.', ' '))
                for word in wordlist:
                    if word in banlist:
                        rejected = "%s contains %s" % (fname, word)
                        logger.warning("%s. Rejecting download" % rejected)
                        break

            # only check size on right types of file
            # e.g. don't reject cos jpg is smaller than min file size for a book
            # need to check if we have a size in K M G or just a number. If K M G could be a float.
            unit = ''
            if not rejected and filetypes:
                if extn in filetypes and fsize:
                    try:
                        if 'G' in str(fsize):
                            fsize = int(float(fsize.split('G')[0].strip()) * 1073741824)
                        elif 'M' in str(fsize):
                            fsize = int(float(fsize.split('M')[0].strip()) * 1048576)
                        elif 'K' in str(fsize):
                            fsize = int(float(fsize.split('K')[0].strip() * 1024))
                        fsize = round(check_int(fsize, 0) / 1048576.0, 2)  # float to 2dp in Mb
                        unit = 'Mb'
                    except ValueError:
                        fsize = 0
                    if fsize:
                        if maxsize and fsize > maxsize:
                            rejected = "%s is too large (%s%s)" % (fname, fsize, unit)
                            logger.warning("%s. Rejecting download" % rejected)
                            break
                        if minsize and fsize < minsize:
                            rejected = "%s is too small (%s%s)" % (fname, fsize, unit)
                            logger.warning("%s. Rejecting download" % rejected)
                            break
            if not rejected:
                logger.debug("%s: (%s%s) is wanted" % (fname, fsize, unit))
    if not rejected:
        logger.debug("%s accepted" % title)
    else:
        logger.debug("%s: %s" % (title, rejected))
    return rejected


def check_residual(download_dir):
    # Import any books in download that weren't marked as snatched, but have a LL.(bookid)
    # don't process any we've already got as we might not want to delete originals
    # NOTE: we currently only import ebook OR audiobook from a single folder, not both
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    db = database.DBConnection()
    try:
        skipped_extensions = get_list(CONFIG['SKIPPED_EXT'])
        ppcount = 0
        downloads = listdir(download_dir)
        loggerpostprocess.debug("Scanning %s %s in %s for LL.(num)" % (len(downloads),
                                                                       plural(len(downloads), 'entry'), download_dir))
        TELEMETRY.record_usage_data('Process/Residual')
        for entry in downloads:
            if "LL.(" in entry:
                _, extn = os.path.splitext(entry)
                if not extn or extn.strip('.') not in skipped_extensions:
                    book_id = entry.split("LL.(")[1].split(")")[0]
                    logger.debug("Book with id: %s found in download directory" % book_id)
                    pp_path = os.path.join(download_dir, entry)
                    # At this point we don't know if we want audio or ebook or both since it wasn't snatched
                    is_audio = (book_file(pp_path, "audiobook", config=CONFIG) != '')
                    is_ebook = (book_file(pp_path, "ebook", config=CONFIG) != '')
                    logger.debug("Contains ebook=%s audio=%s" % (is_ebook, is_audio))
                    data = db.match('SELECT BookFile,AudioFile from books WHERE BookID=?', (book_id,))
                    have_ebook = (data and data['BookFile'] and path_isfile(data['BookFile']))
                    have_audio = (data and data['AudioFile'] and path_isfile(data['AudioFile']))
                    logger.debug("Already have ebook=%s audio=%s" % (have_ebook, have_audio))

                    if have_ebook and have_audio:
                        exists = True
                    elif have_ebook and not CONFIG.get_bool('AUDIO_TAB'):
                        exists = True
                    else:
                        exists = False

                    if exists:
                        logger.debug('Skipping BookID %s, already exists' % book_id)
                    else:
                        loggerpostprocess.debug("Checking type of %s" % pp_path)

                        if path_isfile(pp_path):
                            loggerpostprocess.debug("%s is a file" % pp_path)
                            pp_path = os.path.join(download_dir)

                        if path_isdir(pp_path):
                            loggerpostprocess.debug("%s is a dir" % pp_path)
                            if process_book(pp_path, book_id):
                                loggerpostprocess.debug("Imported %s" % pp_path)
                                ppcount += 1
                else:
                    loggerpostprocess.debug("Skipping extn %s" % entry)
            else:
                loggerpostprocess.debug("Skipping (no LL bookid) %s" % entry)
    finally:
        db.close()
    return ppcount


def get_download_name(title, source, downloadid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    dlname = None
    try:
        logger.debug("%s was sent to %s" % (title, source))
        if source == 'TRANSMISSION':
            dlname = transmission.get_torrent_name(downloadid)
        elif source == 'QBITTORRENT':
            dlname = qbittorrent.get_name(downloadid)
        elif source == 'UTORRENT':
            dlname = utorrent.name_torrent(downloadid)
        elif source == 'RTORRENT':
            dlname = rtorrent.get_name(downloadid)
        elif source == 'SYNOLOGY_TOR':
            dlname = synology.get_name(downloadid)
        elif source == 'DELUGEWEBUI':
            dlname = deluge.get_torrent_name(downloadid)
        elif source == 'DELUGERPC':
            client = DelugeRPCClient(CONFIG['DELUGE_HOST'], int(
                CONFIG['DELUGE_PORT']),
                                     CONFIG['DELUGE_USER'], CONFIG['DELUGE_PASS'],
                                     decode_utf8=True)
            try:
                client.connect()
                result = client.call('core.get_torrent_status', downloadid, {})
                loggerdlcomms.debug("Deluge RPC Status [%s]" % str(result))
                if 'name' in result:
                    dlname = unaccented(result['name'], only_ascii=False)
            except Exception as e:
                logger.error('DelugeRPC failed %s %s' % (type(e).__name__, str(e)))
        elif source == 'SABNZBD':
            data = {}
            if not lazylibrarian.SAB_VER[0]:
                _ = sabnzbd.check_link()
            if lazylibrarian.SAB_VER > (3, 2, 0):
                # we can filter on nzo_ids
                res, _ = sabnzbd.sab_nzbd(nzburl='queue', nzo_ids=downloadid)
            else:
                db = database.DBConnection()
                try:
                    cmd = 'SELECT * from wanted WHERE DownloadID=? and Source=?'
                    data = db.match(cmd, (downloadid, source))
                finally:
                    db.close()
                if data and data['NZBtitle']:
                    res, _ = sabnzbd.sab_nzbd(nzburl='queue', search=data['NZBtitle'])
                else:
                    res, _ = sabnzbd.sab_nzbd(nzburl='queue')

            if res and 'queue' in res:
                logger.debug("SAB queue returned %s for %s" % (len(res['queue']['slots']), downloadid))
                for item in res['queue']['slots']:
                    if item['nzo_id'] == downloadid:
                        dlname = item['filename']
                        break

            if not dlname:  # not in queue, try history in case completed or error
                if lazylibrarian.SAB_VER > (3, 2, 0):
                    res, _ = sabnzbd.sab_nzbd(nzburl='history', nzo_ids=downloadid)
                elif data and data['NZBtitle']:
                    res, _ = sabnzbd.sab_nzbd(nzburl='history', search=data['NZBtitle'])
                else:
                    res, _ = sabnzbd.sab_nzbd(nzburl='history')

                if res and 'history' in res:
                    logger.debug("SAB history returned %s for %s" % (len(res['history']['slots']), downloadid))
                    for item in res['history']['slots']:
                        if item['nzo_id'] == downloadid:
                            # logger.debug(str(item))
                            dlname = item['name']
                            break
        return dlname

    except Exception as e:
        logger.error("Failed to get filename from %s for %s: %s %s" %
                     (source, downloadid, type(e).__name__, str(e)))
        return None


def get_download_files(source, downloadid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    dlfiles = None
    TELEMETRY.record_usage_data('Get/DownloadFiles')
    try:
        if source == 'TRANSMISSION':
            dlfiles = transmission.get_torrent_files(downloadid)
        elif source == 'UTORRENT':
            dlfiles = utorrent.list_torrent(downloadid)
        elif source == 'RTORRENT':
            dlfiles = rtorrent.get_files(downloadid)
        elif source == 'SYNOLOGY_TOR':
            dlfiles = synology.get_files(downloadid)
        elif source == 'QBITTORRENT':
            dlfiles = qbittorrent.get_files(downloadid)
        elif source == 'DELUGEWEBUI':
            dlfiles = deluge.get_torrent_files(downloadid)
        elif source == 'DELUGERPC':
            client = DelugeRPCClient(CONFIG['DELUGE_HOST'], int(
                CONFIG['DELUGE_PORT']),
                                     CONFIG['DELUGE_USER'], CONFIG['DELUGE_PASS'],
                                     decode_utf8=True)
            try:
                client.connect()
                result = client.call('core.get_torrent_status', downloadid, {})
                loggerdlcomms.debug("Deluge RPC Status [%s]" % str(result))
                if 'files' in result:
                    dlfiles = result['files']
            except Exception as e:
                logger.error('DelugeRPC failed %s %s' % (type(e).__name__, str(e)))
        else:
            loggerdlcomms.debug("Unable to get file list from %s (not implemented)" % source)
        return dlfiles

    except Exception as e:
        logger.error("Failed to get list of files from %s for %s: %s %s" %
                     (source, downloadid, type(e).__name__, str(e)))
        return None


def get_download_folder(source, downloadid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    dlfolder = None
    # noinspection PyBroadException
    TELEMETRY.record_usage_data('Get/DownloadFolder')
    try:
        if source == 'TRANSMISSION':
            dlfolder = transmission.get_torrent_folder(downloadid)
        elif source == 'UTORRENT':
            dlfolder = utorrent.dir_torrent(downloadid)
        elif source == 'RTORRENT':
            dlfolder = rtorrent.get_folder(downloadid)
        elif source == 'SYNOLOGY_TOR':
            dlfolder = synology.get_folder(downloadid)
        elif source == 'QBITTORRENT':
            dlfolder = qbittorrent.get_folder(downloadid)
        elif source == 'DELUGEWEBUI':
            dlfolder = deluge.get_torrent_folder(downloadid)
        elif source == 'DELUGERPC':
            client = DelugeRPCClient(CONFIG['DELUGE_HOST'], int(
                CONFIG['DELUGE_PORT']),
                                     CONFIG['DELUGE_USER'], CONFIG['DELUGE_PASS'],
                                     decode_utf8=True)
            try:
                client.connect()
                result = client.call('core.get_torrent_status', downloadid, {})
                loggerdlcomms.debug("Deluge RPC Status [%s]" % str(result))
                if 'save_path' in result:
                    dlfolder = result['save_path']
            except Exception as e:
                logger.error('DelugeRPC failed %s %s' % (type(e).__name__, str(e)))

        elif source == 'SABNZBD':
            data = {}
            if not lazylibrarian.SAB_VER[0]:
                _ = sabnzbd.check_link()
            if lazylibrarian.SAB_VER > (3, 2, 0):
                # we can filter on nzo_ids
                res, _ = sabnzbd.sab_nzbd(nzburl='queue', nzo_ids=downloadid)
            else:
                db = database.DBConnection()
                try:
                    cmd = 'SELECT * from wanted WHERE DownloadID=? and Source=?'
                    data = db.match(cmd, (downloadid, source))
                finally:
                    db.close()
                if data and data['NZBtitle']:
                    res, _ = sabnzbd.sab_nzbd(nzburl='queue', search=data['NZBtitle'])
                else:
                    res, _ = sabnzbd.sab_nzbd(nzburl='queue')
            if res and 'queue' in res:
                logger.debug("SAB queue returned %s for %s" % (len(res['queue']['slots']), downloadid))
                for item in res['queue']['slots']:
                    if item['nzo_id'] == downloadid:
                        dlfolder = None  # still in queue, not unpacked
                        break
            if not dlfolder:  # not in queue, try history
                if lazylibrarian.SAB_VER > (3, 2, 0):
                    res, _ = sabnzbd.sab_nzbd(nzburl='history', nzo_ids=downloadid)
                elif data and data['NZBtitle']:
                    res, _ = sabnzbd.sab_nzbd(nzburl='history', search=data['NZBtitle'])
                else:
                    res, _ = sabnzbd.sab_nzbd(nzburl='history')

                if res and 'history' in res:
                    logger.debug("SAB history returned %s for %s" % (len(res['history']['slots']), downloadid))
                    for item in res['history']['slots']:
                        if item['nzo_id'] == downloadid:
                            # logger.debug(str(item))
                            dlfolder = item.get('storage')
                            break

        elif source == 'NZBGET':
            res, _ = nzbget.send_nzb(cmd='listgroups')
            loggerdlcomms.debug(str(res))
            if res:
                for item in res:
                    if item['NZBID'] == check_int(downloadid, 0):
                        dlfolder = item.get('DestDir')
                        break
            if not dlfolder:  # not in queue, try history
                res, _ = nzbget.send_nzb(cmd='history')
                loggerdlcomms.debug(str(res))
                if res:
                    for item in res:
                        if item['NZBID'] == check_int(downloadid, 0):
                            dlfolder = item.get('DestDir')
                            break
        return dlfolder

    except Exception:
        logger.warning("Failed to get folder from %s for %s" % (source, downloadid))
        logger.error('Unhandled exception in get_download_folder: %s' % traceback.format_exc())
        return None


def get_download_progress(source, downloadid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    progress = 0
    finished = False
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        if source == 'TRANSMISSION':
            progress, errorstring, finished = transmission.get_torrent_progress(downloadid)
            if errorstring:
                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                db.action(cmd, (errorstring, downloadid, source))
                progress = -1

        elif source == 'DIRECT':
            cmd = 'SELECT * from wanted WHERE DownloadID=? and Source=?'
            data = db.match(cmd, (downloadid, source))
            if data:
                progress = 100
                finished = True
            else:
                progress = 0

        elif str(source).startswith('IRC'):
            cmd = 'SELECT * from wanted WHERE DownloadID=? and Source=?'
            data = db.match(cmd, (downloadid, source))
            if data:
                progress = 100
                finished = True
            else:
                progress = 0

        elif source == 'SABNZBD':
            data = {}
            if not lazylibrarian.SAB_VER[0]:
                _ = sabnzbd.check_link()
            if lazylibrarian.SAB_VER > (3, 2, 0):
                res, _ = sabnzbd.sab_nzbd(nzburl='queue', nzo_ids=downloadid)
            else:
                cmd = 'SELECT * from wanted WHERE DownloadID=? and Source=?'
                data = db.match(cmd, (downloadid, source))
                if data and data['NZBtitle']:
                    res, _ = sabnzbd.sab_nzbd(nzburl='queue', search=data['NZBtitle'])
                else:
                    res, _ = sabnzbd.sab_nzbd(nzburl='queue')

            found = False
            if not res or 'queue' not in res:
                progress = 0
            else:
                logger.debug("SAB queue returned %s for %s" % (len(res['queue']['slots']), downloadid))
                for item in res['queue']['slots']:
                    if item['nzo_id'] == downloadid:
                        found = True
                        # logger.debug(str(item))
                        progress = item['percentage']
                        break
            if not found:  # not in queue, try history in case completed or error
                if lazylibrarian.SAB_VER > (3, 2, 0):
                    res, _ = sabnzbd.sab_nzbd(nzburl='history', nzo_ids=downloadid)
                elif data and data['NZBtitle']:
                    res, _ = sabnzbd.sab_nzbd(nzburl='history', search=data['NZBtitle'])
                else:
                    res, _ = sabnzbd.sab_nzbd(nzburl='history')

                if not res or 'history' not in res:
                    progress = 0
                else:
                    logger.debug("SAB history returned %s for %s" % (len(res['history']['slots']), downloadid))
                    for item in res['history']['slots']:
                        if item['nzo_id'] == downloadid:
                            found = True
                            # logger.debug(str(item))
                            # 100% if completed, 99% if still extracting or repairing, -1 if not found or failed
                            if item['status'] == 'Completed' and not item['fail_message']:
                                progress = 100
                                finished = True
                            elif item['status'] in ['Extracting', 'Fetching']:
                                progress = 99
                            elif item['status'] == 'Failed' or item['fail_message']:
                                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                                db.action(cmd, (item['fail_message'], downloadid, source))
                                progress = -1
                            break
            if not found:
                logger.debug('%s not found at %s' % (downloadid, source))
                progress = 0

        elif source == 'NZBGET':
            res, _ = nzbget.send_nzb(cmd='listgroups')
            loggerdlcomms.debug(str(res))
            found = False
            if res:
                for item in res:
                    # nzbget NZBIDs are integers
                    if item['NZBID'] == check_int(downloadid, 0):
                        found = True
                        logger.debug("NZBID %s status %s" % (item['NZBID'], item['Status']))
                        total = item['FileSizeHi'] << 32 + item['FileSizeLo']
                        if total:
                            remaining = item['RemainingSizeHi'] << 32 + item['RemainingSizeLo']
                            done = total - remaining
                            progress = int(done * 100 / total)
                            if progress == 100:
                                finished = True
                        break
            if not found:  # not in queue, try history in case completed or error
                res, _ = nzbget.send_nzb(cmd='history')
                loggerdlcomms.debug(str(res))
                if res:
                    for item in res:
                        if item['NZBID'] == check_int(downloadid, 0):
                            found = True
                            logger.debug("NZBID %s status %s" % (item['NZBID'], item['Status']))
                            # 100% if completed, -1 if not found or failed
                            if 'SUCCESS' in item['Status']:
                                progress = 100
                                finished = True
                            elif 'WARNING' in item['Status'] or 'FAILURE' in item['Status']:
                                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? '
                                cmd += 'WHERE DownloadID=? and Source=?'
                                db.action(cmd, (item['Status'], downloadid, source))
                                progress = -1
                            break
            if not found:
                logger.debug('%s not found at %s' % (downloadid, source))
                progress = 0

        elif source == 'QBITTORRENT':
            progress, status, finished = qbittorrent.get_progress(downloadid)
            if progress == -1:
                logger.debug('%s not found at %s' % (downloadid, source))
                progress = 0
            if status == 'error':
                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                db.action(cmd, ("QBITTORRENT returned error", downloadid, source))
                progress = -1

        elif source == 'UTORRENT':
            progress, status, finished = utorrent.progress_torrent(downloadid)
            if progress == -1:
                logger.debug('%s not found at %s' % (downloadid, source))
                progress = 0
            if status & 16:  # Error
                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                db.action(cmd, ("UTORRENT returned error status %d" % status, downloadid, source))
                progress = -1

        elif source == 'RTORRENT':
            progress, status = rtorrent.get_progress(downloadid)
            if progress == -1:
                logger.debug('%s not found at %s' % (downloadid, source))
                progress = 0
            if status == 'finished':
                progress = 100
                finished = True
            elif status == 'error':
                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                db.action(cmd, ("rTorrent returned error", downloadid, source))
                progress = -1

        elif source == 'SYNOLOGY_TOR':
            progress, status, finished = synology.get_progress(downloadid)
            if status == 'finished':
                progress = 100
            elif status == 'error':
                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                db.action(cmd, ("Synology returned error", downloadid, source))
                progress = -1

        elif source == 'DELUGEWEBUI':
            progress, message, finished = deluge.get_torrent_progress(downloadid)
            if message and message != 'OK':
                cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                db.action(cmd, (message, downloadid, source))
                progress = -1

        elif source == 'DELUGERPC':
            client = DelugeRPCClient(CONFIG['DELUGE_HOST'], int(
                CONFIG['DELUGE_PORT']),
                                     CONFIG['DELUGE_USER'], CONFIG['DELUGE_PASS'],
                                     decode_utf8=True)
            try:
                client.connect()
                result = client.call('core.get_torrent_status', downloadid, {})
                loggerdlcomms.debug("Deluge RPC Status [%s]" % str(result))

                if 'progress' in result:
                    progress = result['progress']
                    try:
                        finished = result['is_auto_managed'] and result['stop_at_ratio'] and \
                                   result['state'].lower() == 'paused' and result['ratio'] >= result['stop_ratio']
                    except (KeyError, AttributeError):
                        finished = False
                else:
                    progress = -1
                    finished = False
                if 'message' in result and result['message'] != 'OK':
                    cmd = 'UPDATE wanted SET Status="Aborted",DLResult=? WHERE DownloadID=? and Source=?'
                    db.action(cmd, (result['message'], downloadid, source))
                    progress = -1
            except Exception as e:
                logger.error('DelugeRPC failed %s %s' % (type(e).__name__, str(e)))
                progress = 0

        else:
            loggerdlcomms.debug("Unable to get progress from %s (not implemented)" % source)
            progress = 0
        try:
            progress = int(progress)
        except ValueError:
            logger.debug("Progress value error %s [%s] %s" % (source, progress, downloadid))
            progress = 0

        if finished:  # store when we noticed it was completed (can ask some downloaders, but not all)
            res = db.match('SELECT Completed from wanted WHERE DownloadID=? and Source=?', (downloadid, source))
            if res and not res['Completed']:
                db.action('UPDATE wanted SET Completed=? WHERE DownloadID=? and Source=?',
                          (int(time.time()), downloadid, source))
        return progress, finished

    except Exception:
        logger.warning("Failed to get download progress from %s for %s" % (source, downloadid))
        logger.error('Unhandled exception in get_download_progress: %s' % traceback.format_exc())
        return 0, False
    finally:
        db.close()


def delete_task(source, download_id, remove_data):
    logger = logging.getLogger(__name__)
    try:
        if source == "BLACKHOLE":
            logger.warning("Download %s has not been processed from blackhole" % download_id)
        elif source == "SABNZBD":
            if CONFIG.get_bool('SAB_DELETE'):
                sabnzbd.sab_nzbd(download_id, 'delete', remove_data)
                sabnzbd.sab_nzbd(download_id, 'delhistory', remove_data)
        elif source == "NZBGET":
            nzbget.delete_nzb(download_id, remove_data)
        elif source == "UTORRENT":
            utorrent.remove_torrent(download_id, remove_data)
        elif source == "RTORRENT":
            rtorrent.remove_torrent(download_id, remove_data)
        elif source == "QBITTORRENT":
            qbittorrent.remove_torrent(download_id, remove_data)
        elif source == "TRANSMISSION":
            transmission.remove_torrent(download_id, remove_data)
        elif source == "SYNOLOGY_TOR" or source == "SYNOLOGY_NZB":
            synology.remove_torrent(download_id, remove_data)
        elif source == "DELUGEWEBUI":
            deluge.remove_torrent(download_id, remove_data)
        elif source == "DELUGERPC":
            client = DelugeRPCClient(CONFIG['DELUGE_HOST'],
                                     int(CONFIG['DELUGE_PORT']),
                                     CONFIG['DELUGE_USER'],
                                     CONFIG['DELUGE_PASS'],
                                     decode_utf8=True)
            try:
                client.connect()
                client.call('core.remove_torrent', download_id, remove_data)
            except Exception as e:
                logger.error('DelugeRPC failed %s %s' % (type(e).__name__, str(e)))
        elif source == 'DIRECT' or source.startswith('IRC'):
            return True
        else:
            logger.debug("Unknown source [%s] in delete_task" % source)
            return False
        return True

    except Exception as e:
        logger.warning("Failed to delete task %s from %s: %s %s" % (download_id, source, type(e).__name__, str(e)))
        return False


def process_book(pp_path=None, bookid=None, library=None, automerge=False):
    TELEMETRY.record_usage_data('Process/Book')
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        # Move a book into LL folder structure given just the folder and bookID, returns True or False
        # Called from "import_alternate" or if we find a "LL.(xxx)" folder that doesn't match a snatched book/mag
        loggerpostprocess.debug("process_book %s" % pp_path)
        is_audio = (book_file(pp_path, "audiobook", config=CONFIG) != '')
        is_ebook = (book_file(pp_path, "ebook", config=CONFIG) != '')

        cmd = 'SELECT AuthorName,BookName,BookID,books.Status,AudioStatus from books,authors '
        cmd += 'WHERE BookID=? and books.AuthorID = authors.AuthorID'
        data = db.match(cmd, (bookid,))
        if data:
            authorname = data['AuthorName']
            bookname = data['BookName']
            cmd = 'SELECT BookID, NZBprov, NZBmode,AuxInfo FROM wanted WHERE BookID=? and Status="Snatched"'
            # we may have wanted to snatch an ebook and audiobook of the same title/id
            was_snatched = db.select(cmd, (bookid,))
            want_audio = False
            want_ebook = False
            booktype = None
            if data['Status'] in ['Wanted', 'Snatched'] or library == 'eBook':
                want_ebook = True
            if data['AudioStatus'] in ['Wanted', 'Snatched'] or library == 'Audio':
                want_audio = True
            for item in was_snatched:
                if item['AuxInfo'] == 'AudioBook':
                    want_audio = True
                elif item['AuxInfo'] == 'eBook' or not item['AuxInfo']:
                    want_ebook = True
            if not is_audio and not is_ebook:
                logger.debug('Bookid %s, failed to find valid booktype' % bookid)
            elif want_audio and is_audio:
                booktype = "AudioBook"
            elif want_ebook and is_ebook:
                booktype = "eBook"
            elif not was_snatched:
                loggerpostprocess.debug('Bookid %s was not snatched so cannot check type, contains ebook:%s audio:%s' %
                                        (bookid, is_ebook, is_audio))

                if is_audio and not CONFIG.get_bool('AUDIO_TAB'):
                    is_audio = False
                if is_audio:
                    booktype = "AudioBook"
                elif is_ebook:
                    booktype = "eBook"
            if not booktype:
                logger.debug('Bookid %s, failed to find valid booktype, contains ebook:%s audio:%s' %
                             (bookid, is_ebook, is_audio))
                return False

            if booktype == "AudioBook":
                dest_dir = get_directory('Audio')
            else:
                dest_dir = get_directory('eBook')

            # CFG2DO Check that path handling no longer necessary
            # if os.name == 'nt':
            #     if '/' in lazylibrarian.CONFIG['EBOOK_DEST_FOLDER']:
            #         logger.warning('Please check your EBOOK_DEST_FOLDER setting')
            #         lazylibrarian.CONFIG['EBOOK_DEST_FOLDER'] = lazylibrarian.CONFIG[
            #             'EBOOK_DEST_FOLDER'].replace('/', '\\')
            #     if '/' in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER']:
            #         logger.warning('Please check your AUDIOBOOK_DEST_FOLDER setting')
            #         lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER'] = lazylibrarian.CONFIG[
            #             'AUDIOBOOK_DEST_FOLDER'].replace('/', '\\')

            namevars = name_vars(bookid)
            # global_name is only used for ebooks to ensure book/cover/opf all have the same basename
            # audiobooks are usually multipart so can't be renamed this way
            global_name = namevars['BookFile']
            global_name = sanitize(global_name)
            if booktype == "AudioBook":
                dest_path = stripspaces(os.path.join(dest_dir, namevars['AudioFolderName']))
            else:
                dest_path = stripspaces(os.path.join(dest_dir, namevars['FolderName']))
            dest_path = make_utf8bytes(dest_path)[0]

            data = {'AuthorName': authorname, 'BookName': bookname, 'BookID': bookid}
            success, dest_file, pp_path = process_destination(pp_path, dest_path, global_name, data,
                                                              booktype, automerge=automerge)
            if success:
                # update nzbs
                dest_file = make_unicode(dest_file)
                if was_snatched:
                    snatched_from = CONFIG.disp_name(was_snatched[0]['NZBprov'])
                    loggerpostprocess.debug("%s was snatched from %s" % (global_name, snatched_from))
                    control_value_dict = {"BookID": bookid}
                    new_value_dict = {"Status": "Processed", "NZBDate": now(), "DLResult": dest_file}
                    db.upsert("wanted", new_value_dict, control_value_dict)
                else:
                    control_value_dict = {"BookID": bookid}
                    new_value_dict = {"AuxInfo": booktype, "NZBDate": now(), "DLResult": dest_file}
                    db.upsert("wanted", new_value_dict, control_value_dict)
                    snatched_from = "manually added"
                    loggerpostprocess.debug("%s %s was %s" % (booktype, global_name, snatched_from))

                if dest_file:  # do we know the location (not calibre already exists)
                    process_extras(dest_file, global_name, bookid, booktype)

                if '.unpack' in pp_path:
                    pp_path = pp_path.split('.unpack')[0] + '.unpack'

                if '.unpack' in pp_path or not CONFIG.get_bool('DESTINATION_COPY') and pp_path != dest_dir:
                    if path_isdir(pp_path):
                        # calibre might have already deleted it?
                        logger.debug("Deleting %s" % pp_path)
                        shutil.rmtree(pp_path, ignore_errors=True)
                else:
                    if CONFIG.get_bool('DESTINATION_COPY'):
                        logger.debug("Not removing %s as Keep Files is set" % pp_path)
                    else:
                        logger.debug("Not removing %s as in download root" % pp_path)

                logger.info('Successfully processed: %s' % global_name)
                custom_notify_download("%s %s" % (bookid, booktype))
                if snatched_from == "manually added":
                    frm = ''
                else:
                    frm = 'from '

                notify_download("%s %s %s%s at %s" % (booktype, global_name, frm, snatched_from, now()), bookid)
                mailing_list(booktype, global_name, bookid)
                if was_snatched:
                    update_downloads(CONFIG.disp_name(was_snatched[0]['NZBprov']))
                else:
                    update_downloads("manually added")
                return True
            else:
                logger.error('Postprocessing for %s has failed: %s' % (repr(global_name), repr(dest_file)))
                shutil.rmtree(pp_path + '.fail', ignore_errors=True)
                try:
                    _ = safe_move(pp_path, pp_path + '.fail')
                    logger.warning('Residual files remain in %s.fail' % pp_path)
                except Exception as e:
                    logger.error("Unable to rename %s, %s %s" %
                                 (repr(pp_path), type(e).__name__, str(e)))
                    if not os.access(syspath(pp_path), os.R_OK):
                        logger.error("%s is not readable" % repr(pp_path))
                    if not os.access(syspath(pp_path), os.W_OK):
                        logger.error("%s is not writeable" % repr(pp_path))
                    parent = os.path.dirname(pp_path)
                    try:
                        with open(syspath(os.path.join(parent, 'll_temp')), 'w', encoding='utf-8') as f:
                            f.write(u'test')
                        remove_file(os.path.join(parent, 'll_temp'))
                    except Exception as why:
                        logger.error("Directory %s is not writeable: %s" % (parent, why))
                    logger.warning('Residual files remain in %s' % pp_path)

                was_snatched = db.match('SELECT NZBurl FROM wanted WHERE BookID=? and Status="Snatched"', (bookid,))
                if was_snatched:
                    control_value_dict = {"NZBurl": was_snatched['NZBurl']}
                    new_value_dict = {"Status": "Failed", "NZBDate": now(), "DLResult": dest_file}
                    db.upsert("wanted", new_value_dict, control_value_dict)
                # reset status so we try for a different version
                if booktype == 'AudioBook':
                    db.action('UPDATE books SET audiostatus="Wanted" WHERE BookID=?', (bookid,))
                else:
                    db.action('UPDATE books SET status="Wanted" WHERE BookID=?', (bookid,))
        return False
    except Exception:
        logger.error('Unhandled exception in process_book: %s' % traceback.format_exc())
        return False
    finally:
        db.close()


def process_extras(dest_file=None, global_name=None, bookid=None, booktype="eBook"):
    # given bookid, handle author count, calibre autoadd, book image, opf

    logger = logging.getLogger(__name__)
    if not bookid:
        logger.error('No bookid supplied')
        return
    if not dest_file:
        logger.error('No dest_file supplied')
        return

    TELEMETRY.record_usage_data('Process/Extras')
    db = database.DBConnection()
    try:
        control_value_dict = {"BookID": bookid}
        if booktype == 'AudioBook':
            new_value_dict = {"AudioFile": dest_file, "AudioStatus": CONFIG['FOUND_STATUS'],
                              "AudioLibrary": now()}
            db.upsert("books", new_value_dict, control_value_dict)
            if CONFIG['AUDIOBOOK_DEST_FILE']:
                book_filename = audio_rename(bookid, rename=True, playlist=True)
                if dest_file != book_filename:
                    db.action('UPDATE books set AudioFile=? where BookID=?', (book_filename, bookid))
        else:
            new_value_dict = {"Status": CONFIG['FOUND_STATUS'], "BookFile": dest_file, "BookLibrary": now()}
            db.upsert("books", new_value_dict, control_value_dict)

        # update authors book counts
        match = db.match('SELECT AuthorID FROM books WHERE BookID=?', (bookid,))
        if match:
            update_totals(match['AuthorID'])

        elif booktype != 'eBook':  # only do autoadd/img/opf for ebooks
            return

        cmd = 'SELECT AuthorName,BookID,BookName,BookDesc,BookIsbn,BookImg,BookDate,BookLang,BookPub,BookRate,'
        cmd += 'Narrator from books,authors WHERE BookID=? and books.AuthorID = authors.AuthorID'
        data = db.match(cmd, (bookid,))
    finally:
        db.close()
    if not data:
        logger.error('No data found for bookid %s' % bookid)
        return

    dest_path = os.path.dirname(dest_file)

    # download and cache image if http link
    process_img(dest_path, data['BookID'], data['BookImg'], global_name, ImageType.BOOK)

    # do we want to create metadata - there may already be one in pp_path, but it was downloaded and might
    # not contain our choice of authorname/title/identifier, so if autoadding we ignore it and write our own
    if not CONFIG.get_bool('IMP_AUTOADD_BOOKONLY'):
        _ = create_opf(dest_path, data, global_name, overwrite=True)
    else:
        _ = create_opf(dest_path, data, global_name, overwrite=False)
    # if our_opf:
    #     write_meta(dest_path, opf_file)  # write metadata from opf to all ebook types in dest folder

    # If you use auto add by Calibre you need the book in a single directory, not nested
    # So take the files you Copied/Moved to Dest_path and copy/move into Calibre auto add folder.
    if CONFIG['IMP_AUTOADD']:
        process_auto_add(dest_path)


# noinspection PyBroadException
def process_destination(pp_path=None, dest_path=None, global_name=None, data=None, booktype='', automerge=False,
                        preprocess=True):
    """ Copy/move book/mag and associated files into target directory
        Return True, full_path_to_book, pp_path (which may have changed)  or False, error_message"""

    TELEMETRY.record_usage_data('Process/Destination')
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    logger.debug("%s [%s] %s" % (booktype, global_name, str(data)))
    booktype = booktype.lower()
    pp_path = make_unicode(pp_path)
    bestmatch = ''
    cover = ''
    comicid = data.get('BookID', '')
    issueid = data.get('IssueDate', '')
    authorname = data.get('AuthorName', '')
    bookname = data.get('BookName', '')
    bookid = data.get('BookID', '')
    title = data.get('Title', '')
    issuedate = data.get('IssueDate', '')
    mode = data.get('NZBmode', '')

    if booktype == 'ebook' and CONFIG.get_bool('ONE_FORMAT'):
        booktype_list = get_list(CONFIG['EBOOK_TYPE'])
        for btype in booktype_list:
            if not bestmatch:
                for fname in listdir(pp_path):
                    extn = os.path.splitext(fname)[1].lstrip('.')
                    if extn and extn.lower() == btype:
                        bestmatch = btype
                        break
    if bestmatch:
        match = bestmatch
        logger.debug('One format import, best match = %s' % bestmatch)
    else:  # mag, comic or audiobook or multi-format book
        match = False
        for fname in listdir(pp_path):
            if CONFIG.is_valid_booktype(fname, booktype=booktype):
                match = True
                break

    if not match:
        # no book/mag found in a format we wanted. Leave for the user to delete or convert manually
        return False, 'Unable to locate a valid filetype (%s) in %s, leaving for manual processing' % (
            booktype, pp_path), pp_path

    if not pp_path.endswith('.unpack') and (CONFIG.get_bool('DESTINATION_COPY') or
                                            (mode in ['torrent', 'magnet', 'torznab'] and
                                             CONFIG.get_bool('KEEP_SEEDING'))):
        logger.debug("Copying to target %s" % pp_path + '.unpack')
        shutil.copytree(pp_path, pp_path + '.unpack')
        pp_path += '.unpack'

    if preprocess:
        logger.debug("preprocess (%s) %s" % (booktype, pp_path))
        if booktype == 'ebook':
            preprocess_ebook(pp_path)
        elif 'audio' in booktype:
            preprocess_audio(pp_path, bookid, authorname, bookname)
        elif booktype == 'magazine':
            db = database.DBConnection()
            try:
                res = db.match("SELECT CoverPage from magazines WHERE Title=?", (bookid,))
            finally:
                db.close()
            cover = 0
            if res:
                cover = check_int(res['CoverPage'], 0)
            preprocess_magazine(pp_path, cover=cover)

        # run custom pre-processing, for example remove unwanted formats
        # or force format conversion before sending to calibre
        if len(CONFIG['EXT_PREPROCESS']):
            logger.debug("Running external PreProcessor: %s %s %s %s" % (booktype, pp_path, authorname, bookname))
            params = [CONFIG['EXT_PREPROCESS'], booktype, pp_path, authorname, bookname]
            rc, res, err = run_script(params)
            if rc:
                return False, "Preprocessor returned %s: res[%s] err[%s]" % (rc, res, err), pp_path
            logger.debug("PreProcessor: %s" % res), pp_path

    # If ebook, magazine or comic, do we want calibre to import it for us
    newbookfile = ''
    if (CONFIG['IMP_CALIBREDB'] and
            (booktype == 'ebook' and CONFIG.get_bool('IMP_CALIBRE_EBOOK')) or
            (booktype == 'magazine' and CONFIG.get_bool('IMP_CALIBRE_MAGAZINE')) or
            (booktype == 'comic' and CONFIG.get_bool('IMP_CALIBRE_COMIC'))):
        try:
            logger.debug('Importing %s %s into calibre library' % (booktype, global_name))
            # calibre may ignore metadata.opf and book_name.opf depending on calibre settings,
            # and ignores opf data if there is data embedded in the book file,
            # so we send separate "set_metadata" commands after the import
            for fname in listdir(pp_path):
                extn = os.path.splitext(fname)[1]
                srcfile = os.path.join(pp_path, fname)
                if CONFIG.is_valid_booktype(fname, booktype=booktype) or extn in ['.opf', '.jpg']:
                    if bestmatch and not fname.endswith(bestmatch) and extn not in ['.opf', '.jpg']:
                        logger.debug("Removing %s as not %s" % (fname, bestmatch))
                        remove_file(srcfile)
                    else:
                        dstfile = os.path.join(pp_path, global_name.replace('"', '_') + extn)
                        # calibre does not like quotes in author names
                        _ = safe_move(srcfile, dstfile)
                else:
                    logger.debug('Removing %s as not wanted' % fname)
                    if path_isfile(srcfile):
                        remove_file(srcfile)
                    elif path_isdir(srcfile):
                        shutil.rmtree(srcfile)

            identifier = ''
            if booktype in ['ebook', 'audiobook']:
                if bookid.isdigit():
                    identifier = "goodreads:%s" % bookid
                elif bookid.startswith('OL'):
                    identifier = "OpenLibrary:%s" % bookid
                else:
                    identifier = "google:%s" % bookid
            elif booktype == 'comic':
                if comicid.startswith('CV'):
                    identifier = "ComicVine:%s" % comicid[2:]
                elif comicid.startswith('CX'):
                    identifier = "Comixology:%s" % comicid[2:]

            if booktype == 'magazine':
                issueid = create_id("%s %s" % (title, issuedate))
                identifier = "lazylibrarian:%s" % issueid
                magfile = book_file(pp_path, "magazine", config=CONFIG)
                jpgfile = os.path.splitext(magfile)[0] + '.jpg'
                if path_isfile(jpgfile):
                    # calibre likes "cover.jpg"
                    coverfile = os.path.basename(jpgfile)
                    jpgfile = safe_copy(jpgfile, jpgfile.replace(coverfile, 'cover.jpg'))
                elif magfile:
                    if cover == 0:
                        cover = 1  # if not set, default to page 1
                    jpgfile = create_mag_cover(magfile, pagenum=cover, refresh=False)
                    if jpgfile:
                        coverfile = os.path.basename(jpgfile)
                        jpgfile = safe_copy(jpgfile, jpgfile.replace(coverfile, 'cover.jpg'))

                if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                    authors = title
                    global_name = issuedate
                else:
                    authors = 'magazines'
                    if '$IssueDate' in CONFIG['MAG_DEST_FILE']:
                        global_name = CONFIG['MAG_DEST_FILE'].replace(
                            '$IssueDate', issuedate).replace('$Title', title)
                    else:
                        global_name = "%s - %s" % (title, issuedate)

                params = [magfile, '--duplicates', '--authors', authors, '--series', title,
                          '--title', global_name, '--tags', 'Magazine']
                if jpgfile:
                    image = ['--cover', jpgfile]
                    params.extend(image)
                res, err, rc = calibredb('add', params)
            else:
                if automerge:
                    res, err, rc = calibredb('add', ['-1', '--automerge', 'overwrite'], [pp_path])
                else:
                    res, err, rc = calibredb('add', ['-1'], [pp_path])

            if rc:
                return False, 'calibredb rc %s from %s' % (rc, CONFIG['IMP_CALIBREDB']), pp_path
            elif booktype == "ebook" and (' --duplicates' in res or ' --duplicates' in err):
                logger.warning('Calibre failed to import %s %s, already exists, marking book as "Have"' %
                               (authorname, bookname))
                db = database.DBConnection()
                try:
                    control_value_dict = {"BookID": bookid}
                    new_value_dict = {"Status": "Have"}
                    db.upsert("books", new_value_dict, control_value_dict)
                finally:
                    db.close()
                return True, '', pp_path
            # Answer should look like "Added book ids : bookID" (string may be translated!)
            try:
                calibre_id = res.rsplit(": ", 1)[1].split("\n", 1)[0].strip()
            except IndexError:
                return False, 'Calibre failed to import %s %s, no added bookids' % (authorname, bookname), pp_path

            if calibre_id.isdigit():
                logger.debug('Calibre ID: [%s]' % calibre_id)
            else:
                logger.warning('Calibre ID looks invalid: [%s]' % calibre_id)

            our_opf = False
            rc = 0
            if not CONFIG.get_bool('IMP_AUTOADD_BOOKONLY'):
                # we can pass an opf with all the info, and a cover image
                db = database.DBConnection()
                try:
                    if booktype in ['ebook', 'audiobook']:
                        cmd = 'SELECT AuthorName,BookID,BookName,BookDesc,BookIsbn,BookImg,BookDate,BookLang,'
                        cmd += 'BookPub,BookRate,Requester,AudioRequester,BookGenre,Narrator from books,authors '
                        cmd += 'WHERE BookID=? and books.AuthorID = authors.AuthorID'
                        data = db.match(cmd, (bookid,))
                    elif booktype == 'comic':
                        cmd = 'SELECT Title,comicissues.ComicID,IssueID,IssueAcquired,IssueFile,'
                        cmd += 'comicissues.Cover,Publisher,Contributors from comics,comicissues WHERE '
                        cmd += 'comics.ComicID = comicissues.ComicID and IssueID=? and comicissues.ComicID=?'
                        data = db.match(cmd, (issueid, comicid))
                        bookid = "%s_%s" % (comicid, issueid)
                finally:
                    db.close()

                if not data:
                    logger.error('No data found for bookid %s' % bookid)
                else:
                    opfpath = ''
                    if booktype in ['ebook', 'audiobook']:
                        process_img(pp_path, bookid, data['BookImg'], global_name, ImageType.BOOK)
                        opfpath, our_opf = create_opf(pp_path, data, global_name, True)
                        # if we send an opf, does calibre update the book-meta as well?
                    elif booktype == 'comic':
                        if data.get('Cover'):
                            process_img(pp_path, bookid, data['Cover'], global_name, ImageType.COMIC)
                        if not CONFIG.get_bool('IMP_COMICOPF'):
                            logger.debug('create_comic_opf is disabled')
                        else:
                            opfpath, our_opf = create_comic_opf(pp_path, data, global_name, True)
                    else:
                        if not CONFIG.get_bool('IMP_MAGOPF'):
                            logger.debug('create_mag_opf is disabled')
                        else:
                            if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                                authors = title
                            else:
                                authors = 'magazines'
                            opfpath, our_opf = create_mag_opf(pp_path, authors, title, issuedate, issueid,
                                                              overwrite=True)
                            # calibre likes "metadata.opf"
                            opffile = os.path.basename(opfpath)
                            if opffile != 'metadata.opf':
                                opfpath = safe_copy(opfpath, opfpath.replace(opffile, 'metadata.opf'))

                    if opfpath:
                        _, _, rc = calibredb('set_metadata', None, [calibre_id, opfpath])
                        if rc:
                            logger.warning("calibredb unable to set opf")

                    tags = ''
                    if CONFIG.get_bool('OPF_TAGS'):
                        if booktype == 'magazine':
                            tags = 'Magazine'
                        if booktype == 'ebook':
                            if CONFIG.get_bool('GENRE_TAGS') and data['BookGenre']:
                                tags = data['BookGenre']
                            if CONFIG.get_bool('WISHLIST_TAGS'):
                                if data['Requester'] is not None:
                                    tag = data['Requester'].replace(" ", ",")
                                    if tag not in tags:
                                        if tags:
                                            tags += ', '
                                        tags += tag
                                elif data['AudioRequester'] is not None:
                                    tag = data['AudioRequester'].replace(" ", ",")
                                    if tag not in tags:
                                        if tags:
                                            tags += ', '
                                        tags += tag
                    if tags:
                        _, _, rc = calibredb('set_metadata', ['--field', 'tags:%s' % tags], [calibre_id])
                        if rc:
                            logger.warning("calibredb unable to set tags")

            if not our_opf and not rc:  # pre-existing opf might not have our preferred authorname/title/identifier
                if booktype == 'magazine':
                    if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                        authorname = title
                        global_name = issuedate
                    else:
                        authorname = 'magazines'
                        if '$IssueDate' in CONFIG['MAG_DEST_FILE']:
                            global_name = CONFIG['MAG_DEST_FILE'].replace(
                                '$IssueDate', issuedate).replace('$Title', title)
                        else:
                            global_name = "%s - %s" % (title, issuedate)

                    _, _, rc = calibredb('set_metadata', ['--field', 'pubdate:%s' % issuedate], [calibre_id])
                    if rc:
                        logger.warning("calibredb unable to set pubdate")
                _, _, rc = calibredb('set_metadata', ['--field', 'authors:%s' % unaccented(
                                     authorname, only_ascii=False)], [calibre_id])
                if rc:
                    logger.warning("calibredb unable to set author")
                _, _, rc = calibredb('set_metadata', ['--field',
                                                      'title:%s' % unaccented(global_name, only_ascii=False)],
                                     [calibre_id])
                if rc:
                    logger.warning("calibredb unable to set title")

                _, _, rc = calibredb('set_metadata', ['--field', 'identifiers:%s' % identifier], [calibre_id])
                if rc:
                    logger.warning("calibredb unable to set identifier")

            if booktype == 'comic':  # for now assume calibredb worked, and didn't move the file
                return True, data['IssueFile'], pp_path

            # Ask calibre for the author/title, so we can construct the likely location
            target_dir = ''
            calibre_authorname = ''
            dest_dir = get_directory('eBook')
            res, err, rc = calibredb('list', ['--fields', 'title,authors', '--search', 'id:%s' % calibre_id],
                                     ['--for-machine'])
            if not rc:
                try:
                    res = '{ ' + res.split('{')[1].split('}')[0] + ' }'
                    res = json.loads(res)
                    if booktype == 'magazine':
                        dest_dir = CONFIG['MAG_DEST_FOLDER']
                        if CONFIG.get_bool('MAG_RELATIVE'):
                            dest_dir = os.path.join(get_directory('eBook'), dest_dir)
                    elif booktype == 'comic':
                        dest_dir = CONFIG['COMIC_DEST_FOLDER']
                        if CONFIG.get_bool('COMIC_RELATIVE'):
                            dest_dir = os.path.join(get_directory('eBook'), dest_dir)

                    while '$' in dest_dir:
                        dest_dir = os.path.dirname(dest_dir)

                    logger.debug('[%s][%s][%s][%s]' % (dest_dir, res['authors'], res['title'], res['id']))
                    target_dir = os.path.join(dest_dir, res['authors'], "%s (%d)" % (res['title'], res['id']))
                    logger.debug("Calibre target: %s" % target_dir)
                    calibre_authorname = res['authors']
                    calibre_id = res['id']
                except Exception as e:
                    logger.debug("Unable to read json response; %s" % str(e))
                    target_dir = ''

                if not target_dir or not path_isdir(target_dir) and calibre_authorname:
                    author_dir = os.path.join(dest_dir, calibre_authorname)
                    if path_isdir(author_dir):  # assumed author directory
                        our_id = '(%s)' % calibre_id
                        entries = listdir(author_dir)
                        for entry in entries:
                            if entry.endswith(our_id):
                                target_dir = os.path.join(author_dir, entry)
                                break

                        if not target_dir or not path_isdir(target_dir):
                            logger.debug('Failed to locate calibre folder with id %s in %s' % (our_id, author_dir))
                    else:
                        logger.debug('Failed to locate calibre author folder %s' % author_dir)

            if not target_dir or not path_isdir(target_dir):
                # calibre does not like accents or quotes in names
                if authorname.endswith('.'):  # calibre replaces trailing dot with underscore e.g. Jr. becomes Jr_
                    authorname = authorname[:-1] + '_'
                author_dir = os.path.join(dest_dir, unaccented(authorname.replace('"', '_'), only_ascii=False), '')
                if path_isdir(author_dir):  # assumed author directory
                    our_id = '(%s)' % calibre_id
                    entries = listdir(author_dir)
                    for entry in entries:
                        if entry.endswith(our_id):
                            target_dir = os.path.join(author_dir, entry)
                            break

                    if not target_dir or not path_isdir(target_dir):
                        return False, 'Failed to locate folder with calibre_id %s in %s' % (our_id, author_dir), pp_path
                else:
                    return False, 'Failed to locate author folder %s' % author_dir, pp_path

            if booktype == 'ebook':
                remv = bool(CONFIG.get_bool('FULL_SCAN'))
                logger.debug('Scanning directory [%s]' % target_dir)
                _ = library_scan(target_dir, remove=remv)

            newbookfile = book_file(target_dir, booktype=booktype, config=CONFIG)
            # should we be setting permissions on calibres directories and files?
            if newbookfile:
                setperm(target_dir)
                if booktype in ['magazine', 'comic']:
                    ignorefile = os.path.join(target_dir, '.ll_ignore')
                    try:
                        with open(syspath(ignorefile), 'w', encoding='utf-8') as f:
                            f.write(make_unicode(booktype))
                    except IOError as e:
                        logger.warning("Unable to create/write to ignorefile: %s" % str(e))

                for fname in listdir(target_dir):
                    setperm(os.path.join(target_dir, fname))

                # clear up any residual non-calibre folder
                shutil.rmtree(target_dir.rsplit('(', 1)[0].strip(), ignore_errors=True)
                return True, newbookfile, pp_path
            return False, "Failed to find a valid %s in [%s]" % (booktype, target_dir), pp_path
        except Exception as e:
            logger.error('Unhandled exception importing to calibre: %s' % traceback.format_exc())
            return False, 'calibredb import failed, %s %s' % (type(e).__name__, str(e)), pp_path
    else:
        # we are copying the files ourselves
        loggerpostprocess.debug("BookType: %s, calibredb: [%s]" % (booktype, CONFIG['IMP_CALIBREDB']))
        loggerpostprocess.debug("Source Path: %s" % (repr(pp_path)))
        loggerpostprocess.debug("Dest Path: %s" % (repr(dest_path)))
        dest_path, encoding = make_utf8bytes(dest_path)
        if encoding:
            loggerpostprocess.debug("dest_path was %s" % encoding)
        if not path_exists(dest_path):
            logger.debug('%s does not exist, so it\'s safe to create it' % dest_path)
        elif not path_isdir(dest_path):
            logger.debug('%s exists but is not a directory, deleting it' % dest_path)
            try:
                remove_file(dest_path)
            except OSError as why:
                return False, 'Unable to delete %s: %s' % (dest_path, why.strerror), pp_path
        if path_isdir(dest_path):
            setperm(dest_path)
        elif not make_dirs(dest_path):
            return False, 'Unable to create directory %s' % dest_path, pp_path

        udest_path = make_unicode(dest_path)  # we can't mix unicode and bytes in log messages or joins
        global_name, encoding = make_utf8bytes(global_name)
        if encoding:
            loggerpostprocess.debug("global_name was %s" % encoding)

        # ok, we've got a target directory, try to copy only the files we want, renaming them on the fly.
        firstfile = ''  # try to keep track of "preferred" ebook type or the first part of multipart audiobooks
        for fname in listdir(pp_path):
            if bestmatch and CONFIG.is_valid_booktype(fname, booktype=booktype) and not fname.endswith(bestmatch):
                logger.debug("Ignoring %s as not %s" % (fname, bestmatch))
            else:
                if CONFIG.is_valid_booktype(fname, booktype=booktype) or \
                        (fname.lower().endswith(".jpg") or fname.lower().endswith(".opf")):
                    srcfile = os.path.join(pp_path, fname)
                    if booktype in ['audiobook', 'comic']:
                        if fname.lower().endswith(".jpg") or fname.lower().endswith(".opf"):
                            destfile = os.path.join(udest_path, make_unicode(global_name) + os.path.splitext(fname)[1])
                        else:
                            destfile = os.path.join(udest_path, fname)  # don't rename audio or comic files, just copy
                    else:
                        destfile = os.path.join(udest_path, make_unicode(global_name) + os.path.splitext(fname)[1])
                    try:
                        logger.debug('Copying %s to directory %s' % (fname, udest_path))
                        destfile = safe_copy(srcfile, destfile)
                        setperm(destfile)
                        if CONFIG.is_valid_booktype(make_unicode(destfile), booktype=booktype):
                            newbookfile = destfile
                    except Exception as why:
                        # extra debugging to see if we can figure out a windows encoding issue
                        parent = os.path.dirname(destfile)
                        try:
                            with open(syspath(os.path.join(parent, 'll_temp')), 'w', encoding='utf-8') as f:
                                f.write(u'test')
                            remove_file(os.path.join(parent, 'll_temp'))
                        except Exception as w:
                            logger.error("Destination Directory [%s] is not writeable: %s" % (parent, w))
                        return False, "Unable to copy file %s to %s: %s %s" % (srcfile, destfile,
                                                                               type(why).__name__, str(why)), pp_path
                else:
                    logger.debug('Ignoring unwanted file: %s' % fname)

        if booktype in ['ebook', 'audiobook']:
            cmd = 'SELECT AuthorName,BookID,BookName,BookDesc,BookIsbn,BookImg,BookDate,BookLang,'
            cmd += 'BookPub,BookRate,Requester,AudioRequester,BookGenre,Narrator from books,authors '
            cmd += 'WHERE BookID=? and books.AuthorID = authors.AuthorID'
            db = database.DBConnection()
            try:
                data = db.match(cmd, (bookid,))
            finally:
                db.close()
            process_img(pp_path, bookid, data['BookImg'], make_unicode(global_name), ImageType.BOOK)
            _ = create_opf(pp_path, data, make_unicode(global_name), True)

        # for ebooks, prefer the first booktype found in ebook_type list
        if booktype == 'ebook':
            book_basename = os.path.join(dest_path, global_name)
            booktype_list = get_list(CONFIG['EBOOK_TYPE'])
            for booktype in booktype_list:
                preferred_type = "%s.%s" % (make_unicode(book_basename), booktype)
                if path_exists(preferred_type):
                    logger.debug("Link to preferred type %s, %s" % (booktype, preferred_type))
                    firstfile = preferred_type
                    break

        # link to the first part of multipart audiobooks unless there is a whole-book file
        elif booktype == 'audiobook':
            tokmatch = ''
            for f in listdir(dest_path):
                # if no number_period or number_space in filename assume its whole-book
                if not re.findall(r'\d+\b', f) and CONFIG.is_valid_booktype(f, booktype='audiobook'):
                    firstfile = os.path.join(udest_path, f)
                    tokmatch = 'whole'
                    break
            for token in [' 001.', ' 01.', ' 1.', ' 001 ', ' 01 ', ' 1 ', '001', '01']:
                if tokmatch:
                    break
                for f in listdir(dest_path):
                    if CONFIG.is_valid_booktype(f, booktype='audiobook'):
                        if not firstfile:
                            firstfile = os.path.join(udest_path, f)
                            logger.debug("Primary link to %s" % f)
                        if token in f:
                            firstfile = os.path.join(udest_path, f)
                            logger.debug("Link to first part [%s], %s" % (token, f))
                            tokmatch = token
                            break

        elif booktype in ['magazine', 'comic']:
            try:
                ignorefile = os.path.join(dest_path, b'.ll_ignore')
                with open(syspath(ignorefile), 'w') as f:
                    f.write(make_unicode(booktype))
            except (IOError, TypeError) as e:
                logger.warning("Unable to create/write to ignorefile: %s" % str(e))

            if booktype == 'comic':
                cmd = 'SELECT Title,comicissues.ComicID,IssueID,IssueAcquired,IssueFile,'
                cmd += 'comicissues.Cover,Publisher,Contributors from comics,comicissues WHERE '
                cmd += 'comics.ComicID = comicissues.ComicID and IssueID=? and comicissues.ComicID=?'
                db = database.DBConnection()
                try:
                    data = db.match(cmd, (issueid, comicid))
                finally:
                    db.close()
                bookid = "%s_%s" % (comicid, issueid)
                if data:
                    process_img(pp_path, bookid, data['Cover'], global_name, ImageType.COMIC)
                    if not CONFIG.get_bool('IMP_COMICOPF'):
                        logger.debug('create_comic_opf is disabled')
                    else:
                        _, _ = create_comic_opf(pp_path, data, global_name, True)
                else:
                    logger.debug('No data found for %s_%s' % (comicid, issueid))
            else:
                if not CONFIG.get_bool('IMP_MAGOPF'):
                    logger.debug('create_mag_opf is disabled')
                else:
                    if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                        authors = title
                    else:
                        authors = 'magazines'
                    _, _ = create_mag_opf(pp_path, authors, title, issuedate, issueid, overwrite=True)

        if firstfile:
            newbookfile = firstfile
    return True, newbookfile, pp_path


def process_auto_add(src_path=None, booktype='book'):
    # Called to copy/move the book files to an auto add directory for the likes of Calibre which can't do nested dirs
    logger = logging.getLogger(__name__)
    autoadddir = CONFIG['IMP_AUTOADD']
    savefiles = CONFIG.get_bool('IMP_AUTOADD_COPY')
    if booktype == 'mag':
        autoadddir = CONFIG['IMP_AUTOADDMAG']
        savefiles = CONFIG.get_bool('IMP_AUTOADDMAG_COPY')

    if not path_exists(autoadddir):
        logger.error('AutoAdd directory for %s [%s] is missing or not set - cannot perform autoadd' % (
            booktype, autoadddir))
        return False
    TELEMETRY.record_usage_data('Process/Autoadd')
    # Now try and copy all the book files into a single dir.
    try:
        names = listdir(src_path)
        # files jpg, opf & book(s) should have same name
        # Caution - book may be pdf, mobi, epub or all 3.
        # for now simply copy all files, and let the autoadder sort it out
        #
        # Update - seems Calibre will only use the jpeg if named same as book, not cover.jpg
        # and only imports one format of each ebook, treats the others as duplicates, might be configable in calibre?
        # ignores author/title data in opf file if there is any embedded in book

        match = False
        if booktype == 'book' and CONFIG.get_bool('ONE_FORMAT'):
            booktype_list = get_list(CONFIG['EBOOK_TYPE'])
            for booktype in booktype_list:
                while not match:
                    for name in names:
                        extn = os.path.splitext(name)[1].lstrip('.')
                        if extn and extn.lower() == booktype:
                            match = booktype
                            break
        copied = False
        for name in names:
            if match and CONFIG.is_valid_booktype(name, booktype=booktype) and not name.endswith(match):
                logger.debug('Skipping %s' % os.path.splitext(name)[1])
            elif booktype == 'book' and CONFIG.get_bool('IMP_AUTOADD_BOOKONLY') and not \
                    CONFIG.is_valid_booktype(name, booktype="book"):
                logger.debug('Skipping %s' % name)
            elif booktype == 'mag' and CONFIG.get_bool('IMP_AUTOADD_MAGONLY') and not \
                    CONFIG.is_valid_booktype(name, booktype="mag"):
                logger.debug('Skipping %s' % name)
            else:
                srcname = os.path.join(src_path, name)
                dstname = os.path.join(autoadddir, name)
                try:
                    if savefiles:
                        logger.debug('AutoAdd Copying file [%s] from [%s] to [%s]' % (name, srcname, dstname))
                        dstname = safe_copy(srcname, dstname)
                    else:
                        logger.debug('AutoAdd Moving file [%s] from [%s] to [%s]' % (name, srcname, dstname))
                        dstname = safe_move(srcname, dstname)
                    copied = True
                except Exception as why:
                    logger.error('AutoAdd - Failed to copy/move file [%s] %s [%s] ' %
                                 (name, type(why).__name__, str(why)))
                    return False
                try:
                    os.chmod(syspath(dstname), 0o666)  # make rw for calibre
                except OSError as why:
                    logger.warning("Could not set permission of %s because [%s]" % (dstname, why.strerror))
                    # permissions might not be fatal, continue

        if copied and not savefiles:  # do we want to keep the library files?
            logger.debug('Removing %s' % src_path)
            shutil.rmtree(src_path)

    except OSError as why:
        logger.error('AutoAdd - Failed because [%s]' % why.strerror)
        return False

    logger.info('Auto Add completed for [%s]' % src_path)
    return True


def process_img(dest_path=None, bookid=None, bookimg=None, global_name=None, cache=ImageType.BOOK, overwrite=False):
    """ cache the bookimg from url or filename, and optionally copy it to bookdir """
    # if lazylibrarian.CONFIG['IMP_AUTOADD_BOOKONLY']:
    #     logger.debug('Not creating coverfile, bookonly is set')
    #     return

    logger = logging.getLogger(__name__)
    coverfile = jpg_file(dest_path)
    if not overwrite and coverfile:
        logger.debug('Cover %s already exists' % coverfile)
        return

    TELEMETRY.record_usage_data('Process/Image')
    if bookimg.startswith('cache/'):
        img = bookimg.replace('cache/', '')
        if os.path.__name__ == 'ntpath':
            img = img.replace('/', '\\')
        cachefile = os.path.join(DIRS.CACHEDIR, img)
    else:
        link, success, _ = cache_img(cache, bookid, bookimg, False)
        if not success:
            logger.error('Error caching cover from %s, %s' % (bookimg, link))
            return
        cachefile = os.path.join(DIRS.CACHEDIR, cache.value, bookid + '.jpg')

    try:
        coverfile = os.path.join(dest_path, global_name + '.jpg')
        coverfile = safe_copy(cachefile, coverfile)
        setperm(coverfile)
    except Exception as e:
        logger.error("Error copying image %s to %s, %s %s" % (bookimg,
                                                              coverfile, type(e).__name__, str(e)))
        return


def create_comic_opf(pp_path, data, global_name, overwrite=False):
    """ Needs calibre to be configured to read metadata from file contents, not filename """
    title = data['Title']
    issue = data['IssueID']
    contributors = data.get('Contributors', '')
    issue_id = "%s_%s" % (data['ComicID'], data['IssueID'])
    iname = "%s: %s" % (data['Title'], data['IssueID'])
    publisher = data['Publisher']
    mtime = os.path.getmtime(data['IssueFile'])
    iss_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))

    data = {
        'AuthorName': title,
        'BookID': issue_id,
        'BookName': iname,
        'FileAs': iname,
        'BookDesc': '',
        'BookIsbn': '',
        'BookDate': iss_acquired,
        'BookLang': '',
        'BookImg': global_name + '.jpg',
        'BookPub': publisher,
        'Series': title,
        'Series_index': issue
    }  # type: dict
    if contributors:
        data['Contributors'] = contributors
    # noinspection PyTypeChecker
    return create_opf(pp_path, data, global_name, overwrite=overwrite)


def create_mag_opf(issuefile, authors, title, issue, issue_id, overwrite=False):
    """ Needs calibre to be configured to read metadata from file contents, not filename """
    logger = logging.getLogger(__name__)
    logger.debug("Creating opf with file:%s authors:%s title:%s issue:%s issueid:%s overwrite:%s" %
                 (issuefile, authors, title, issue, issue_id, overwrite))
    dest_path, global_name = os.path.split(issuefile)
    global_name = os.path.splitext(global_name)[0]

    if CONFIG.get_bool('IMP_CALIBRE_MAGISSUE'):
        iname = issue
    elif issue and len(issue) == 10 and issue[8:] == '01' and issue[4] == '-' and issue[7] == '-':  # yyyy-mm-01
        yr = issue[0:4]
        mn = issue[5:7]
        month = lazylibrarian.MONTHNAMES[int(mn)][0]
        iname = "%s - %s%s %s" % (title, month[0].upper(), month[1:], yr)  # The Magpi - January 2017
    elif title in issue:
        iname = issue  # 0063 - Android Magazine -> 0063
    else:
        iname = "%s - %s" % (title, issue)  # Android Magazine - 0063

    mtime = os.path.getmtime(issuefile)
    iss_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))

    data = {
        'AuthorName': authors,
        'BookID': issue_id,
        'BookName': iname,
        'FileAs': authors,
        'BookDesc': '',
        'BookIsbn': '',
        'BookDate': iss_acquired,
        'BookLang': 'eng',
        'BookImg': global_name + '.jpg',
        'BookPub': '',
        'Series': title,
        'Series_index': issue,
        'Scheme': 'lazylibrarian',
    }  # type: dict
    # noinspection PyTypeChecker
    return create_opf(dest_path, data, global_name, overwrite=overwrite)


def create_opf(dest_path=None, data=None, global_name=None, overwrite=False):
    logger = logging.getLogger(__name__)
    opfpath = os.path.join(dest_path, global_name + '.opf')
    if not overwrite and path_exists(opfpath):
        logger.debug('%s already exists. Did not create one.' % opfpath)
        setperm(opfpath)
        return opfpath, False

    data = dict(data)

    bookid = data['BookID']
    if bookid.startswith('CV'):
        scheme = "COMICVINE"
    elif bookid.startswith('CX'):
        scheme = "COMIXOLOGY"
    elif 'Scheme' in data:
        scheme = data['Scheme']
    elif bookid.isdigit():
        scheme = 'goodreads'
    elif bookid.startswith('OL'):
        scheme = 'OpenLibrary'
    else:
        scheme = 'GoogleBooks'

    seriesname = ''
    seriesnum = ''
    if 'Series_index' not in data:
        # no series details passed in data dictionary, look them up in db
        db = database.DBConnection()
        try:
            res = {}
            if 'LT_WorkID' in data and data['LT_WorkID']:
                cmd = 'SELECT SeriesID,SeriesNum from member WHERE workid=?'
                res = db.match(cmd, (data['LT_WorkID'],))
            if not res and 'WorkID' in data and data['WorkID']:
                cmd = 'SELECT SeriesID,SeriesNum from member WHERE workid=?'
                res = db.match(cmd, (data['WorkID'],))
            if not res:
                cmd = 'SELECT SeriesID,SeriesNum from member WHERE bookid=?'
                res = db.match(cmd, (bookid,))
            if res:
                seriesid = res['SeriesID']
                serieslist = get_list(res['SeriesNum'])
                # might be "Book 3.5" or similar, just get the numeric part
                while serieslist:
                    seriesnum = serieslist.pop()
                    try:
                        _ = float(seriesnum)
                        break
                    except ValueError:
                        seriesnum = ''
                        pass

                if not seriesnum:
                    # couldn't figure out number, keep everything we got, could be something like "Book Two"
                    serieslist = res['SeriesNum']

                cmd = 'SELECT SeriesName from series WHERE seriesid=?'
                res = db.match(cmd, (seriesid,))
                if res:
                    seriesname = res['SeriesName']
                    if not seriesnum:
                        # add what we got to series name and set seriesnum to 1 so user can sort it out manually
                        seriesname = "%s %s" % (seriesname, serieslist)
                        seriesnum = 1
        finally:
            db.close()

    opfinfo = '<?xml version="1.0"  encoding="UTF-8"?>\n\
<package version="2.0" xmlns="http://www.idpf.org/2007/opf" >\n\
    <metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">\n\
        <dc:title>%s</dc:title>\n\
        <dc:language>%s</dc:language>\n' % (data.get('BookName', ''), data.get('BookLang', ''))

    opfinfo += '        <dc:identifier opf:scheme="%s">%s</dc:identifier>\n' % (scheme, bookid)

    if "Contributors" in data:
        # what calibre does is split into individuals and add a line for each, e.g.
        # <dc:creator opf:file-as="Pastoras, Das &amp; Ribic, Esad &amp; Aaron, Jason"
        # opf:role="aut">Das Pastoras</dc:creator>
        # <dc:creator opf:file-as="Pastoras, Das &amp; Ribic, Esad &amp; Aaron, Jason"
        # opf:role="aut">Esad Ribic</dc:creator>
        # <dc:creator opf:file-as="Pastoras, Das &amp; Ribic, Esad &amp; Aaron, Jason"
        # opf:role="aut">Jason Aaron</dc:creator>
        #
        entries = []
        names = ''
        for contributor in get_list(data['Contributors'], ','):
            if ':' in contributor:
                role, name = contributor.split(':', 1)
            else:
                name = contributor
                role = 'Unknown'
            if name and role:
                entries.append([name.strip(), role.strip()])
                if names:
                    names += ' &amp; '
                names += surname_first(name, postfixes=CONFIG.get_list('NAME_POSTFIX'))
        for entry in entries:
            opfinfo += '        <dc:creator opf:file-as="%s" opf:role="%s">%s</dc:creator>\n' % \
                       (names, entry[1], entry[0])
    elif data.get("FileAs", ''):
        opfinfo += '        <dc:creator opf:file-as="%s" opf:role="aut">%s</dc:creator>\n' % \
                   (data['FileAs'], data['FileAs'])
    else:
        opfinfo += '        <dc:creator opf:file-as="%s" opf:role="aut">%s</dc:creator>\n' % \
                   (surname_first(data['AuthorName'], postfixes=CONFIG.get_list('NAME_POSTFIX')), data['AuthorName'])
    if data.get('BookIsbn', ''):
        opfinfo += '        <dc:identifier opf:scheme="ISBN">%s</dc:identifier>\n' % data['BookIsbn']

    if data.get('BookPub', ''):
        opfinfo += '        <dc:publisher>%s</dc:publisher>\n' % data['BookPub']

    if data.get('BookDate', ''):
        opfinfo += '        <dc:date>%s</dc:date>\n' % data['BookDate']

    if data.get('BookDesc', ''):
        opfinfo += '        <dc:description>%s</dc:description>\n' % data['BookDesc']

    if CONFIG.get_bool('GENRE_TAGS') and data.get("BookGenre", ''):
        for genre in get_list(data['BookGenre'], ','):
            opfinfo += '        <dc:subject>%s</dc:subject>\n' % genre

    if data.get('BookRate', ''):
        rate = check_int(data['BookRate'], 0)
        rate = int(round(rate * 2))  # calibre uses 0-10, goodreads 0-5
        opfinfo += '        <meta content="%s" name="calibre:rating"/>\n' % rate

    if seriesname:
        opfinfo += '        <meta content="%s" name="calibre:series"/>\n' % seriesname
    elif 'Series' in data:
        opfinfo += '        <meta content="%s" name="calibre:series"/>\n' % data['Series']

    if seriesnum:
        opfinfo += '        <meta content="%s" name="calibre:series_index"/>\n' % seriesnum
    elif 'Series_index' in data:
        opfinfo += '        <meta content="%s" name="calibre:series_index"/>\n' % data['Series_index']
    if data.get('Narrator', ''):
        opfinfo += '        <meta content="%s" name="lazylibrarian:narrator"/>\n' % data['Narrator']

    coverfile = jpg_file(dest_path)
    if coverfile:
        coverfile = os.path.basename(coverfile)
    else:
        coverfile = 'cover.jpg'

    opfinfo += '        <guide>\n\
            <reference href="%s" type="cover" title="Cover"/>\n\
        </guide>\n\
    </metadata>\n\
</package>' % coverfile  # file in current directory, not full path

    dic = {'...': '', ' & ': ' ', ' = ': ' ', '$': 's', ' + ': ' ', '*': ''}
    opfinfo = make_unicode(replace_all(opfinfo, dic))
    try:
        with open(syspath(opfpath), 'w', encoding='utf-8') as opf:
            opf.write(opfinfo)
        logger.debug('Saved metadata to: ' + opfpath)
        setperm(opfpath)
        return opfpath, True
    except Exception as e:
        logger.error("Error creating opf %s, %s %s" % (opfpath, type(e).__name__, str(e)))
        return '', False


def write_meta(book_folder, opf):
    logger = logging.getLogger(__name__)
    if not path_exists(opf):
        logger.error("No opf file [%s]" % opf)
        return

    ebook_meta = calibre_prg('ebook-meta')
    if not ebook_meta:
        logger.debug("No ebook-meta found")
        return

    flist = listdir(book_folder)
    for fname in flist:
        if CONFIG.is_valid_booktype(fname, booktype='ebook'):
            book = os.path.join(book_folder, fname)
            params = [ebook_meta, book, "--write_meta", opf]
            logger.debug("Writing metadata to [%s]" % fname)
            try:
                if os.name != 'nt':
                    _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                stderr=subprocess.STDOUT)
                else:
                    _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
                logger.debug("Metadata written from %s" % opf)
            except Exception as e:
                logger.error(str(e))
