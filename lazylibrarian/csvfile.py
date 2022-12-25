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

import os
import shutil
import traceback

import lazylibrarian
from lazylibrarian import database, logger
from lazylibrarian.common import csv_file
from lazylibrarian.filesystem import DIRS, path_isdir, syspath, remove_file, safe_move
from lazylibrarian.formatter import plural, is_valid_isbn, now, unaccented, format_author_name, \
    make_unicode, split_title
from lazylibrarian.importer import search_for, import_book, add_author_name_to_db, update_totals
from lazylibrarian.librarysync import find_book_in_db

try:
    from csv import writer, reader, QUOTE_MINIMAL
except ImportError:
    from lib.csv import writer, reader, QUOTE_MINIMAL


# noinspection PyArgumentList
def dump_table(table, savedir=None, status=None):
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        columns = db.select('PRAGMA table_info(%s)' % table)
        if not columns:  # no such table
            logger.warn("No such table [%s]" % table)
            return 0

        if not path_isdir(savedir):
            savedir = DIRS.DATADIR
        else:
            savedir = str(savedir)

        headers = ''
        for item in columns:
            if headers:
                headers += ','
            headers += item[1]
        if status:
            cmd = 'SELECT %s from %s WHERE status="%s"' % (headers, table, status)
        else:
            cmd = 'SELECT %s from %s' % (headers, table)
        data = db.select(cmd)
        count = 0
        if data is not None:
            label = table
            if status:
                label += '_%s' % status
            csvfile = os.path.join(savedir, "%s.csv" % label)
            headers = headers.split(',')
            with open(syspath(csvfile), 'w', encoding='utf-8', newline='') as outfile:
                # noinspection PyTypeChecker
                csvwrite = writer(outfile, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)
                csvwrite.writerow(headers)
                for item in data:
                    csvwrite.writerow([str(s) if s else '' for s in item])
                    count += 1
            msg = "Exported %s %s to %s" % (count, plural(count, "item"), csvfile)
            logger.info(msg)
        return count

    except Exception:
        msg = 'Unhandled exception in dump_table: %s' % traceback.format_exc()
        logger.error(msg)
        return 0


def restore_table(table, savedir=None, status=None):
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        columns = db.select('PRAGMA table_info(%s)' % table)
        if not columns:  # no such table
            logger.warn("No such table [%s]" % table)
            return 0

        if not path_isdir(savedir):
            savedir = DIRS.DATADIR
        else:
            savedir = str(savedir)

        headers = ''

        label = table
        if status:
            label += '_%s' % status
        csvfile = os.path.join(savedir, "%s.csv" % label)

        logger.debug('Reading file %s' % csvfile)
        csvreader = reader(open(csvfile, 'r', encoding='utf-8', newline=''))
        count = 0
        for row in csvreader:
            if csvreader.line_num == 1:
                headers = row
            else:
                item = dict(list(zip(headers, row)))

                if table == 'magazines':
                    control_value_dict = {"Title": make_unicode(item['Title'])}
                    new_value_dict = {"Regex": make_unicode(item['Regex']),
                                      "Reject": make_unicode(item['Reject']),
                                      "Status": item['Status'],
                                      "MagazineAdded": item['MagazineAdded'],
                                      "IssueStatus": item['IssueStatus'],
                                      "CoverPage": item['CoverPage']}
                    db.upsert("magazines", new_value_dict, control_value_dict)
                    count += 1

                elif table == 'users':
                    control_value_dict = {"UserID": item['UserID']}
                    new_value_dict = {"UserName": item['UserName'],
                                      "Password": item['Password'],
                                      "Email": item['Email'],
                                      "Name": item['Name'],
                                      "Perms": item['Perms'],
                                      "HaveRead": item['HaveRead'],
                                      "ToRead": item['ToRead'],
                                      "CalibreRead": item['CalibreRead'],
                                      "CalibreToRead": item['CalibreToRead'],
                                      "BookType": item['BookType']
                                      }
                    db.upsert("users", new_value_dict, control_value_dict)
                    count += 1
                else:
                    logger.error("Invalid table [%s]" % table)
                    return 0
        msg = "Imported %s %s from %s" % (count, plural(count, "item"), csvfile)
        logger.info(msg)
        return count

    except Exception:
        msg = 'Unhandled exception in restore_table: %s' % traceback.format_exc()
        logger.error(msg)
        return 0


def export_csv(search_dir=None, status="Wanted", library=''):
    """ Write a csv file to the search_dir containing all books marked as "Wanted" """
    msg = 'Export CSV'
    if not library:
        if lazylibrarian.SHOW_AUDIO:
            library = 'AudioBook'
        else:
            library = 'eBook'
    # noinspection PyBroadException
    try:
        if not search_dir:
            msg = "Alternate Directory not configured"
            logger.warn(msg)
            return msg
        elif not path_isdir(search_dir):
            msg = "Alternate Directory [%s] not found" % search_dir
            logger.warn(msg)
            return msg
        elif not os.access(syspath(search_dir), os.W_OK | os.X_OK):
            msg = "Alternate Directory [%s] not writable" % search_dir
            logger.warn(msg)
            return msg

        csvfile = os.path.join(search_dir, "%s %s - %s.csv" % (status, library, now().replace(':', '-')))

        db = database.DBConnection()

        cmd = 'SELECT BookID,AuthorName,BookName,BookIsbn,books.AuthorID FROM books,authors '
        if library == 'eBook':
            cmd += 'WHERE books.Status=? and books.AuthorID = authors.AuthorID'
        else:
            cmd += 'WHERE AudioStatus=? and books.AuthorID = authors.AuthorID'
        find_status = db.select(cmd, (status,))

        if not find_status:
            msg = "No %s marked as %s" % (library, status)
            logger.warn(msg)
            return msg
        count = 0
        # noinspection PyArgumentList
        with open(syspath(csvfile), 'w', encoding='utf-8', newline='') as outfile:
            # noinspection PyTypeChecker
            csvwrite = writer(outfile, delimiter=',', quotechar='"', quoting=QUOTE_MINIMAL)

            # write headers, change AuthorName BookName BookIsbn to match import csv names
            csvwrite.writerow(['BookID', 'Author', 'Title', 'ISBN', 'AuthorID'])

            for resulted in find_status:
                logger.debug("Exported CSV for %s %s" % (library, resulted['BookName']))
                row = ([resulted['BookID'], resulted['AuthorName'], resulted['BookName'],
                        resulted['BookIsbn'], resulted['AuthorID']])
                csvwrite.writerow([("%s" % s) for s in row])
                count += 1
        msg = "CSV exported %s %s to %s" % (count, plural(count, library), csvfile)
        logger.info(msg)
    except Exception:
        msg = 'Unhandled exception in export_csv: %s' % traceback.format_exc()
        logger.error(msg)
    finally:
        return msg


def finditem(item, preferred_authorname, library='eBook', reason='csv.finditem'):
    """
    Try to find book matching the csv item in the database
    Return database entry, or False if not found
    """
    db = database.DBConnection()
    bookmatch = ""
    isbn10 = ""
    isbn13 = ""
    bookid = ""
    bookname = item['Title']

    bookname = make_unicode(bookname)
    if 'ISBN' in item:
        isbn10 = item['ISBN']
    if 'ISBN13' in item:
        isbn13 = item['ISBN13']
    if 'BookID' in item:
        bookid = item['BookID']

    # try to find book in our database using bookid or isbn, or if that fails, name matching
    cmd = 'SELECT AuthorName,BookName,BookID,books.Status,AudioStatus,Requester,'
    cmd += 'AudioRequester FROM books,authors where books.AuthorID = authors.AuthorID '
    if bookid:
        fullcmd = cmd + 'and BookID=?'
        bookmatch = db.match(fullcmd, (bookid,))
    if not bookmatch:
        if is_valid_isbn(isbn10):
            fullcmd = cmd + 'and BookIsbn=?'
            bookmatch = db.match(fullcmd, (isbn10,))
    if not bookmatch:
        if is_valid_isbn(isbn13):
            fullcmd = cmd + 'and BookIsbn=?'
            bookmatch = db.match(fullcmd, (isbn13,))
    if not bookmatch:
        bookid, _ = find_book_in_db(preferred_authorname, bookname, ignored=False, library=library,
                                    reason=reason)
        if bookid:
            fullcmd = cmd + 'and BookID=?'
            bookmatch = db.match(fullcmd, (bookid,))
    return bookmatch


def import_csv(search_dir=None, status='Wanted', library=''):
    """ Find a csv file in the search_dir and process all the books in it,
        adding authors to the database if not found
        and marking the books as "Wanted"
        Optionally delete the file on successful completion
    """
    msg = 'Import CSV'
    if not library:
        library = 'audio' if lazylibrarian.SHOW_AUDIO else 'eBook'
    # noinspection PyBroadException
    try:
        if not search_dir:
            msg = "Alternate Directory not configured"
            logger.warn(msg)
            return msg
        elif not path_isdir(search_dir):
            msg = "Alternate Directory [%s] not found" % search_dir
            logger.warn(msg)
            return msg

        csvfile = csv_file(search_dir, library=library)

        headers = None

        db = database.DBConnection()
        bookcount = 0
        authcount = 0
        skipcount = 0
        total = 0
        existing = 0

        if not csvfile:
            msg = "No %s CSV file found in %s" % (library, search_dir)
            logger.warn(msg)
            return msg

        logger.debug('Reading file %s' % csvfile)
        csvreader = reader(open(csvfile, 'r', encoding='utf-8', newline=''))
        for row in csvreader:
            if csvreader.line_num == 1:
                # If we are on the first line, create the headers list from the first row
                headers = row
                if 'Author' not in headers or 'Title' not in headers:
                    msg = 'Invalid CSV file found %s' % csvfile
                    logger.warn(msg)
                    return msg
            elif row:
                total += 1
                item = dict(list(zip(headers, row)))
                authorname = format_author_name(item['Author'])
                title = make_unicode(item['Title'])

                authmatch = db.match('SELECT * FROM authors where AuthorName=?', (authorname,))

                if authmatch:
                    logger.debug("CSV: Author %s found in database" % authorname)
                    authorid = authmatch['authorid']
                else:
                    logger.debug("CSV: Author %s not found" % authorname)
                    newauthor, authorid, new = add_author_name_to_db(author=authorname, addbooks=False,
                                                                     reason="import_csv %s" % csvfile,
                                                                     title=title)
                    if newauthor and newauthor != authorname:
                        logger.debug("Preferred authorname changed from [%s] to [%s]" % (authorname, newauthor))
                        authorname = newauthor
                    if new:
                        authcount += 1
                    if not authorid:
                        logger.warn("Authorname %s not added to database" % authorname)

                if authorid:
                    bookmatch = finditem(item, authorname, library=library, reason='import_csv: %s' % csvfile)
                else:
                    bookmatch = {}

                imported = False
                results = []
                if bookmatch:
                    authorname = bookmatch['AuthorName']
                    bookname = bookmatch['BookName']
                    bookid = bookmatch['BookID']
                    bookstatus = bookmatch['Status'] if library == 'eBook' else bookmatch['AudioStatus']
                    if bookstatus in ['Open', 'Wanted', 'Have']:
                        existing += 1
                        logger.info('Found %s %s by %s, already marked as "%s"' %
                                    (library, bookname, authorname, bookstatus))
                        imported = True
                    else:  # skipped/ignored
                        logger.info('Found %s %s by %s, marking as "%s"' % (library, bookname, authorname, status))
                        control_value_dict = {"BookID": bookid}
                        new_value_dict = {"Status": status} if library == 'eBook' else {"AudioStatus": status}
                        db.upsert("books", new_value_dict, control_value_dict)
                        bookcount += 1
                elif authorid:
                    searchterm = "%s <ll> %s" % (title, authorname)
                    results = search_for(unaccented(searchterm, only_ascii=False))
                    for result in results:
                        if result['book_fuzz'] >= lazylibrarian.CONFIG.get_int('MATCH_RATIO') \
                                and result['authorid'] == authorid:
                            bookmatch = result
                            break
                    if not bookmatch:  # no match on full searchterm, try splitting out subtitle and series
                        newtitle, _, _ = split_title(authorname, title)
                        if newtitle != title:
                            title = newtitle
                            searchterm = "%s <ll> %s" % (title, authorname)
                            results = search_for(unaccented(searchterm, only_ascii=False))
                            for result in results:
                                if result['book_fuzz'] >= lazylibrarian.CONFIG.get_int('MATCH_RATIO') \
                                        and result['authorid'] == authorid:
                                    bookmatch = result
                                    break
                    if bookmatch:
                        logger.info("Found (%s%%) %s: %s for %s: %s" %
                                    (bookmatch['book_fuzz'], bookmatch['authorname'], bookmatch['bookname'],
                                     authorname, title))
                        if library == 'eBook':
                            import_book(bookmatch['bookid'], ebook=status, wait=True,
                                        reason="Added by import_csv %s" % csvfile)
                        else:
                            import_book(bookmatch['bookid'], audio=status, wait=True,
                                        reason="Added by import_csv %s" % csvfile)
                        imported = db.match('select * from books where BookID=?', (bookmatch['bookid'],))
                        if imported:
                            bookcount += 1

                if bookmatch and imported:
                    update_totals(authorid)
                else:
                    msg = "Skipping book %s by %s" % (title, authorname)
                    if not results:
                        msg += ', No results found'
                        logger.warn(msg)
                    elif bookmatch and not imported:
                        msg += ', Failed to import %s' % bookmatch['bookid']
                        logger.warn(msg)
                    else:
                        msg += ', No match found'
                        logger.warn(msg)
                        msg = "Closest match (%s%% %s%%) %s: %s" % (results[0]['author_fuzz'],
                                                                    results[0]['book_fuzz'],
                                                                    results[0]['authorname'],
                                                                    results[0]['bookname'])
                        if results[0]['authorid'] != authorid:
                            msg += ' wrong authorid'
                        logger.warn(msg)
                    skipcount += 1

        msg = "Found %i %s%s in csv file, %i already existing or Wanted" % (total, library,
                                                                            plural(total),
                                                                            existing)
        logger.info(msg)
        msg = "Added %i new %s, marked %i %s as '%s', %i %s not found" % \
              (authcount, plural(authcount, "author"), bookcount, plural(bookcount, library),
               status, skipcount, plural(skipcount, library))
        logger.info(msg)
        if lazylibrarian.CONFIG.get_bool('DELETE_CSV'):
            if skipcount == 0:
                logger.info("Deleting %s on successful completion" % csvfile)
                try:
                    remove_file(csvfile)
                except OSError as why:
                    logger.warn('Unable to delete %s: %s' % (csvfile, why.strerror))
            else:
                logger.warn("Not deleting %s as not all books found" % csvfile)
                if path_isdir(csvfile + '.fail'):
                    try:
                        shutil.rmtree(csvfile + '.fail')
                    except Exception as why:
                        logger.warn("Unable to remove %s, %s %s" % (csvfile + '.fail',
                                                                    type(why).__name__, str(why)))
                try:
                    _ = safe_move(csvfile, csvfile + '.fail')
                except Exception as e:
                    logger.error("Unable to rename %s, %s %s" %
                                 (csvfile, type(e).__name__, str(e)))
                    if not os.access(syspath(csvfile), os.R_OK):
                        logger.error("%s is not readable" % csvfile)
                    if not os.access(syspath(csvfile), os.W_OK):
                        logger.error("%s is not writeable" % csvfile)
                    parent = os.path.dirname(csvfile)
                    try:
                        with open(syspath(os.path.join(parent, 'll_temp')), 'w') as f:
                            f.write('test')
                        remove_file(os.path.join(parent, 'll_temp'))
                    except Exception as why:
                        logger.error("Directory %s is not writeable: %s" % (parent, why))
    except Exception:
        msg = 'Unhandled exception in import_csv: %s' % traceback.format_exc()
        logger.error(msg)
    finally:
        return msg
