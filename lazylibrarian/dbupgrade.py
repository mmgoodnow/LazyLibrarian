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

from __future__ import with_statement

import datetime
import os
import time
import traceback
import uuid
from shutil import copyfile

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.bookwork import setGenres
from lazylibrarian.common import restartJobs, pwd_generator, setperm, syspath
from lazylibrarian.formatter import plural, md5_utf8, getList, check_int
from lazylibrarian.importer import update_totals
from lazylibrarian.common import path_exists

# database version history:
# 0 original version or new empty database
# 1 changes up to June 2016
# 2 removed " MB" from nzbsize field in wanted table
# 3 removed SeriesOrder column from books table as redundant
# 4 added duplicates column to stats table
# 5 issue numbers padded to 4 digits with leading zeros
# 6 added Manual field to books table for user editing
# 7 added Source and DownloadID to wanted table for download monitoring
# 8 move image cache from data/images/cache into datadir
# 9 add regex to magazine table
# 10 check for missing columns in pastissues table
# 11 Keep most recent book image in author table
# 12 Keep latest issue cover in magazine table
# 13 add Manual column to author table for user editing
# 14 separate book and author images in case id numbers collide
# 15 move series and seriesnum into separate tables so book can appear in multiple series
# 16 remove series, authorlink, authorname columns from book table, only in series/author tables now
# 17 remove authorid from series table, new seriesauthor table to allow multiple authors per series
# 18 Added unique constraint to seriesauthors table
# 19 add seriesdisplay to book table
# 20 add booklibrary date to book table
# 21 add audiofile audiolibrary date and audiostatus to books table
# 22 add goodreads "follow" to author table
# 23 add user accounts
# 24 add HaveRead and ToRead to user accounts
# 25 add index for magazine issues (title) for new dbchanges
# 26 create Sync table
# 27 add indexes for book/author/wanted status
# 28 add CalibreRead and CalibreToRead columns to user table
# 29 add goodreads workid to books table
# 30 add BookType to users table
# 31 add DateType to magazines table
# 32 add counters to series table
# 33 add DLResult to wanted table
# 34 add ScanResult to books table, and new isbn table
# 35 add OriginalPubDate to books table
# 36 create failedsearch table
# 37 add delete cascade to tables
# 38 change series count and total to integers
# 39 add LastBookID to author table
# 40 add CoverPage to magazines table
# 41 add Requester and AudioRequester to books table
# 42 add SendTo to users table
# 43 remove foreign key constraint on wanted table
# 44 move hosting to gitlab
# ---------- Removed support for upgrades earlier than this ----------------
# ----------   v45+ lets us migrate last dobytang version   ------------
# 45 update local git repo to new origin
# 46 remove pastissues table and rebuild to ensure no foreign key
# 47 genres and genrebooks tables
# 48 ensure magazine table schema is current
# 49 ensure author table schema is current
# 50 add comics and comicissues tables
# 51 add aka to comics table
# 52 add updated to series table
# 53 add jobs table
# 54 separated author added date from updated timestamp
# 55 Add Reason to author table
# 56 Add Cover column to comicissues and issues tables
# 57 Add Description to comics and Description/Contributors to comicissues
# 58 Ensure Link was added to comicissues (was missing for new installs in v57)
# 59 Added per provider seeders instead of global
# 60 Moved preprocessor into main program and disabled old preprocessor
# --------- Database upgrades from this point are checked at every startup ------------
# 61 Add reason to series table
# 62 Add About to author table
# 63 Add Start to jobs table, rename LastRun to Finish
# 64 Add Added time to pastissues table
# 65 Add Reading and Abandoned to users table
# 66 Add subscribers table
# 67 Add prefs to user table
# 68 Add completed time to wanted table
# 69 Add LT_WorkID to books table and AKA to authors

db_current_version = 69


def upgrade_needed():
    """
    Check if database needs upgrading
    Return zero if up-to-date
    Return current version if needs upgrade
    """

    myDB = database.DBConnection()
    # Had a report of "index out of range", can't replicate it.
    # Maybe on some versions of sqlite an unset user_version
    # or unsupported pragma gives an empty result?
    db_version = 0
    result = myDB.match('PRAGMA user_version')
    if result and result[0]:
        value = str(result[0])
        if value.isdigit():
            db_version = int(value)

    if db_version < db_current_version:
        return db_current_version
    return 0


def has_column(myDB, table, column):
    columns = myDB.select('PRAGMA table_info(%s)' % table)
    if not columns:  # no such table
        return False
    for item in columns:
        if item[1] == column:
            return True
    # no such column
    return False


def dbupgrade(current_version):
    with open(syspath(os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'dbupgrade.log')), 'a') as upgradelog:
        # noinspection PyBroadException
        try:
            myDB = database.DBConnection()
            db_version = 0
            result = myDB.match('PRAGMA user_version')
            if result and result[0]:
                value = str(result[0])
                if value.isdigit():
                    db_version = int(value)

            check = myDB.match('PRAGMA integrity_check')
            if check and check[0]:
                result = check[0]
                if result == 'ok':
                    logger.debug('Database integrity check: %s' % result)
                else:
                    logger.error('Database integrity check: %s' % result)
                    # should probably abort now if result is not "ok"

            if db_version < current_version:
                myDB = database.DBConnection()
                if db_version:
                    lazylibrarian.UPDATE_MSG = 'Updating database to version %s, current version is %s' % (
                        current_version, db_version)
                    logger.info(lazylibrarian.UPDATE_MSG)
                    upgradelog.write("%s v0: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
                else:
                    # it's a new database. Create v60 tables and then upgrade as required
                    db_version = 60
                    lazylibrarian.UPDATE_MSG = 'Creating new database, version %s' % db_current_version
                    upgradelog.write("%s v%s: %s\n" % (time.ctime(), db_version, lazylibrarian.UPDATE_MSG))
                    logger.info(lazylibrarian.UPDATE_MSG)
                    # sanity check for incomplete initialisations
                    res = myDB.select("select name from sqlite_master where type is 'table'")
                    for item in res:
                        myDB.action("DROP TABLE IF EXISTS %s" % item['name'])

                    # new v60 set of database tables
                    myDB.action('CREATE TABLE authors (AuthorID TEXT UNIQUE, AuthorName TEXT UNIQUE, ' +
                                'AuthorImg TEXT, AuthorLink TEXT, DateAdded TEXT, Status TEXT, LastBook TEXT, ' +
                                'LastBookImg TEXT, LastLink TEXT, LastDate TEXT, HaveBooks INTEGER DEFAULT 0, ' +
                                'TotalBooks INTEGER DEFAULT 0, AuthorBorn TEXT, AuthorDeath TEXT, ' +
                                'UnignoredBooks INTEGER DEFAULT 0, Manual TEXT, GRfollow TEXT, ' +
                                'LastBookID TEXT, Updated INTEGER DEFAULT 0, Reason TEXT, About TEXT, AKA TEXT)')
                    myDB.action('CREATE TABLE wanted (BookID TEXT, NZBurl TEXT, NZBtitle TEXT, NZBdate TEXT, ' +
                                'NZBprov TEXT, Status TEXT, NZBsize TEXT, AuxInfo TEXT, NZBmode TEXT, ' +
                                'Source TEXT, DownloadID TEXT, DLResult TEXT)')
                    myDB.action('CREATE TABLE magazines (Title TEXT UNIQUE, Regex TEXT, Status TEXT, ' +
                                'MagazineAdded TEXT, LastAcquired TEXT, IssueDate TEXT, IssueStatus TEXT, ' +
                                'Reject TEXT, LatestCover TEXT, DateType TEXT, CoverPage INTEGER DEFAULT 1)')
                    myDB.action('CREATE TABLE languages (isbn TEXT, lang TEXT)')
                    myDB.action('CREATE TABLE stats (authorname text, GR_book_hits int, GR_lang_hits int, ' +
                                'LT_lang_hits int, GB_lang_change, cache_hits int, bad_lang int, bad_char int, ' +
                                'uncached int, duplicates int)')
                    myDB.action('CREATE TABLE series (SeriesID INTEGER UNIQUE, SeriesName TEXT, Status TEXT, ' +
                                'Have INTEGER DEFAULT 0, Total INTEGER DEFAULT 0, Updated INTEGER DEFAULT 0, ' +
                                'Reason TEXT)')
                    myDB.action('CREATE TABLE downloads (Count INTEGER DEFAULT 0, Provider TEXT)')
                    myDB.action('CREATE TABLE users (UserID TEXT UNIQUE, UserName TEXT UNIQUE, Password TEXT, ' +
                                'Email TEXT, Name TEXT, Perms INTEGER DEFAULT 0, HaveRead TEXT, ToRead TEXT, ' +
                                'CalibreRead TEXT, CalibreToRead TEXT, BookType TEXT, SendTo TEXT)')
                    myDB.action('CREATE TABLE isbn (Words TEXT, ISBN TEXT)')
                    myDB.action('CREATE TABLE genres (GenreID INTEGER PRIMARY KEY AUTOINCREMENT, ' +
                                'GenreName TEXT UNIQUE)')
                    myDB.action('CREATE TABLE comics (ComicID TEXT UNIQUE, Title TEXT, Status TEXT, ' +
                                'Added TEXT, LastAcquired TEXT, Updated TEXT, LatestIssue TEXT, IssueStatus TEXT, ' +
                                'LatestCover TEXT, SearchTerm TEXT, Start TEXT, First INTEGER, Last INTEGER, ' +
                                'Publisher TEXT, Link TEXT, aka TEXT, Description TEXT)')
                    myDB.action('CREATE TABLE jobs (Name TEXT, Finish INTEGER DEFAULT 0, Start INTEGER DEFAULT 0)')

                    myDB.action('CREATE TABLE books (AuthorID TEXT REFERENCES authors (AuthorID) ' +
                                'ON DELETE CASCADE, BookName TEXT, BookSub TEXT, BookDesc TEXT, ' +
                                'BookGenre TEXT, BookIsbn TEXT, BookPub TEXT, BookRate INTEGER DEFAULT 0, ' +
                                'BookImg TEXT, BookPages INTEGER DEFAULT 0, BookLink TEXT, BookID TEXT UNIQUE, ' +
                                'BookFile TEXT, BookDate TEXT, BookLang TEXT, BookAdded TEXT, Status TEXT, ' +
                                'WorkPage TEXT, Manual TEXT, SeriesDisplay TEXT, BookLibrary TEXT, ' +
                                'AudioFile TEXT, AudioLibrary TEXT, AudioStatus TEXT, WorkID TEXT, ' +
                                'ScanResult TEXT, OriginalPubDate TEXT, Requester TEXT, AudioRequester TEXT, ' +
                                'LT_WorkID TEXT)')
                    myDB.action('CREATE TABLE issues (Title TEXT REFERENCES magazines (Title) ' +
                                'ON DELETE CASCADE, IssueID TEXT UNIQUE, IssueAcquired TEXT, IssueDate TEXT, ' +
                                'IssueFile TEXT, Cover TEXT)')
                    myDB.action('CREATE TABLE member (SeriesID INTEGER REFERENCES series (SeriesID) ' +
                                'ON DELETE CASCADE, BookID TEXT REFERENCES books (BookID) ON DELETE CASCADE, ' +
                                'WorkID TEXT, SeriesNum TEXT)')
                    myDB.action('CREATE TABLE seriesauthors (SeriesID INTEGER, ' +
                                'AuthorID TEXT REFERENCES authors (AuthorID) ON DELETE CASCADE, ' +
                                'UNIQUE (SeriesID,AuthorID))')
                    myDB.action('CREATE TABLE sync (UserID TEXT REFERENCES users (UserID) ON DELETE CASCADE, ' +
                                'Label TEXT, Date TEXT, SyncList TEXT)')
                    myDB.action('CREATE TABLE failedsearch (BookID TEXT REFERENCES books (BookID) ' +
                                'ON DELETE CASCADE, Library TEXT, Time TEXT, Interval INTEGER DEFAULT 0, ' +
                                'Count INTEGER DEFAULT 0)')
                    myDB.action('CREATE TABLE genrebooks (GenreID INTEGER REFERENCES genres (GenreID) ' +
                                'ON DELETE CASCADE, BookID TEXT REFERENCES books (BookID) ON DELETE CASCADE, ' +
                                'UNIQUE (GenreID,BookID))')
                    myDB.action('CREATE TABLE comicissues (ComicID TEXT REFERENCES comics (ComicID) ' +
                                'ON DELETE CASCADE, IssueID TEXT, IssueAcquired TEXT, IssueFile TEXT, ' +
                                'Cover TEXT, Description TEXT, Link TEXT, Contributors TEXT, ' +
                                'UNIQUE (ComicID, IssueID))')

                    # pastissues table has same layout as wanted table, code below is to save typos if columns change
                    res = myDB.match("SELECT sql FROM sqlite_master WHERE type='table' AND name='wanted'")
                    myDB.action(res['sql'].replace('wanted', 'pastissues'))
                    myDB.action('ALTER TABLE pastissues ADD COLUMN Added INTEGER DEFAULT 0')

                    cmd = 'INSERT into users (UserID, UserName, Name, Password, Perms) VALUES (?, ?, ?, ?, ?)'
                    myDB.action(cmd, (pwd_generator(), 'admin', 'admin', md5_utf8('admin'), 65535))
                    logger.debug('Added admin user')

                    myDB.action('CREATE INDEX issues_Title_index ON issues (Title)')
                    myDB.action('CREATE INDEX books_index_authorid ON books(AuthorID)')
                    myDB.action('CREATE INDEX books_index_status ON books(Status)')
                    myDB.action('CREATE INDEX authors_index_status ON authors(Status)')
                    myDB.action('CREATE INDEX wanted_index_status ON wanted(Status)')

                if db_version < 45:
                    msg = 'Your database is too old. Unable to upgrade database from v%s.' % db_version
                    upgradelog.write("%s: %s\n" % (time.ctime(), msg))
                    logger.error(msg)
                    lazylibrarian.UPDATE_MSG = msg

                db_changes = 0
                index = db_version + 1
                while 'db_v%s' % index in globals():
                    db_changes += 1
                    upgrade_function = getattr(lazylibrarian.dbupgrade, 'db_v%s' % index)
                    upgrade_function(myDB, upgradelog)
                    index += 1

            # a few quick sanity checks and a schema update if needed...
            lazylibrarian.UPDATE_MSG = 'Checking Database'
            db_changes += check_db(upgradelog=upgradelog)

            if db_changes:
                myDB.action('PRAGMA user_version=%s' % current_version)
                lazylibrarian.UPDATE_MSG = 'Cleaning Database'
                upgradelog.write("%s: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
                myDB.action('vacuum')
                lazylibrarian.UPDATE_MSG = 'Database updated to version %s' % current_version
                logger.info(lazylibrarian.UPDATE_MSG)
                upgradelog.write("%s: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))

            restartJobs(start='Start')
            lazylibrarian.UPDATE_MSG = ''

        except Exception:
            msg = 'Unhandled exception in database upgrade: %s' % traceback.format_exc()
            upgradelog.write("%s: %s\n" % (time.ctime(), msg))
            logger.error(msg)
            lazylibrarian.UPDATE_MSG = ''


def check_db(upgradelog=None):
    myDB = database.DBConnection()
    cnt = 0

    if not upgradelog:
        upgradelog = open(syspath(os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'dbupgrade.log')), 'a')
    db_changes = update_schema(myDB, upgradelog)

    lazylibrarian.UPDATE_MSG = 'Checking unique authors'
    unique = False
    indexes = myDB.select("PRAGMA index_list('authors')")
    for item in indexes:
        data = list(item)
        if data[2] == 1:  # unique index
            res = myDB.match("PRAGMA index_info('%s')" % data[1])
            data = list(res)
            if data[2] == 'AuthorID':
                unique = True
                break
    if not unique:
        res = myDB.match('select count(distinct authorid) as d,count(authorid) as c from authors')
        if res['d'] == res['c']:
            logger.warn("Adding unique index to AuthorID")
            myDB.action("CREATE UNIQUE INDEX unique_authorid ON authors('AuthorID')")
        else:
            msg = 'Unable to create unique index on AuthorID: %i vs %i' % (res['d'], res['c'])
            logger.error(msg)
        cnt = 1

    try:
        # correct any invalid/unpadded dates
        lazylibrarian.UPDATE_MSG = 'Checking dates'
        cmd = 'SELECT BookID,BookDate from books WHERE  BookDate LIKE "%-_-%" or BookDate LIKE "%-_"'
        res = myDB.select(cmd)
        tot = len(res)
        if tot:
            cnt += tot
            msg = 'Updating %s %s with invalid/unpadded bookdate' % (tot, plural(tot, "book"))
            logger.warn(msg)
            for item in res:
                parts = item['BookDate'].split('-')
                if len(parts) == 3:
                    mn = check_int(parts[1], 0)
                    dy = check_int(parts[2], 0)
                    if mn and dy:
                        bookdate = "%s-%02d-%02d" % (parts[0], mn, dy)
                        myDB.action("UPDATE books SET BookDate=? WHERE BookID=?", (bookdate, item['BookID']))
                    else:
                        logger.warn("Invalid Month/Day (%s) for %s" % (item['BookDate'], item['BookID']))
                else:
                    logger.warn("Invalid BookDate (%s) for %s" % (item['BookDate'], item['BookID']))
                    myDB.action("UPDATE books SET BookDate=? WHERE BookID=?", ("0000", item['BookID']))

        # update any series "Skipped" to series "Paused"
        res = myDB.match('SELECT count(*) as counter from series WHERE Status="Skipped"')
        tot = res['counter']
        if tot:
            cnt += tot
            logger.warn("Found %s series marked Skipped, updating to Paused" % tot)
            myDB.action('UPDATE series SET Status="Paused" WHERE Status="Skipped"')

        # replace faulty/html language results with Unknown
        lazylibrarian.UPDATE_MSG = 'Checking languages'
        filt = 'BookLang is NULL or BookLang LIKE "%<%" or BookLang LIKE "%invalid%"'
        cmd = 'SELECT count(*) as counter from books WHERE ' + filt
        res = myDB.match(cmd)
        tot = res['counter']
        if tot:
            cnt += tot
            msg = 'Updating %s %s with no language to "Unknown"' % (tot, plural(tot, "book"))
            logger.warn(msg)
            myDB.action('UPDATE books SET BookLang="Unknown" WHERE ' + filt)

        # delete html error pages
        filt = 'length(lang) > 30'
        cmd = 'SELECT count(*) as counter from languages WHERE ' + filt
        res = myDB.match(cmd)
        tot = res['counter']
        if tot:
            cnt += tot
            msg = 'Updating %s %s with bad data' % (tot, plural(tot, "language"))
            logger.warn(msg)
            cmd = 'DELETE from languages WHERE ' + filt
            myDB.action(cmd)

        # suppress duplicate entries in language table
        lazylibrarian.UPDATE_MSG = 'Checking unique languages'
        filt = 'rowid not in (select max(rowid) from languages group by isbn)'
        cmd = 'SELECT count(*) as counter from languages WHERE ' + filt
        res = myDB.match(cmd)
        tot = res['counter']
        if tot:
            cnt += tot
            msg = 'Deleting %s duplicate %s' % (tot, plural(tot, "language"))
            logger.warn(msg)
            cmd = 'DELETE from languages WHERE ' + filt
            myDB.action(cmd)

        #  remove books with no bookid
        lazylibrarian.UPDATE_MSG = 'Removing books with no bookid'
        books = myDB.select('SELECT * FROM books WHERE BookID is NULL or BookID=""')
        if books:
            cnt += len(books)
            msg = 'Removing %s %s with no bookid' % (len(books), plural(len(books), "book"))
            logger.warn(msg)
            myDB.action('DELETE from books WHERE BookID is NULL or BookID=""')

        #  remove books with no authorid
        lazylibrarian.UPDATE_MSG = 'Removing books with no authorid'
        books = myDB.select('SELECT BookID FROM books WHERE AuthorID is NULL or AuthorID=""')
        if books:
            cnt += len(books)
            msg = 'Removing %s %s with no authorid' % (len(books), plural(len(books), "book"))
            logger.warn(msg)
            for book in books:
                myDB.action('DELETE from books WHERE BookID=?', (book["BookID"],))

        # remove authors with no authorid
        lazylibrarian.UPDATE_MSG = 'Removing authors with no authorid'
        authors = myDB.select('SELECT * FROM authors WHERE AuthorID IS NULL or AuthorID=""')
        if authors:
            cnt += len(authors)
            msg = 'Removing %s %s with no authorid' % (len(authors), plural(len(authors), "author"))
            logger.warn(msg)
            myDB.action('DELETE from authors WHERE AuthorID is NULL or AuthorID=""')

        # remove authors with no name
        lazylibrarian.UPDATE_MSG = 'Removing authors with no name'
        authors = myDB.select('SELECT AuthorID FROM authors WHERE AuthorName IS NULL or AuthorName = ""')
        if authors:
            cnt += len(authors)
            msg = 'Removing %s %s with no name' % (len(authors), plural(len(authors), "author"))
            logger.warn(msg)
            for author in authors:
                myDB.action('DELETE from authors WHERE AuthorID=?', (author["AuthorID"],))

        # remove magazines with no name
        lazylibrarian.UPDATE_MSG = 'Removing magazines with no name'
        mags = myDB.select('SELECT Title FROM magazines WHERE Title IS NULL or Title = ""')
        if mags:
            cnt += len(mags)
            msg = 'Removing %s %s with no name' % (len(mags), plural(len(mags), "magazine"))
            logger.warn(msg)
            myDB.action('DELETE from magazines WHERE Title IS NULL or Title = ""')

        # remove authors with no books
        lazylibrarian.UPDATE_MSG = 'Removing authors with no books'
        authors = myDB.select('SELECT AuthorID FROM authors WHERE TotalBooks=0')
        if authors:
            for author in authors:  # check we haven't mis-counted
                update_totals(author['authorid'])
            authors = myDB.select('SELECT AuthorID FROM authors WHERE TotalBooks=0')
            if authors:
                cnt += len(authors)
                msg = 'Removing %s %s with no books' % (len(authors), plural(len(authors), "author"))
                logger.warn(msg)
                for author in authors:
                    myDB.action('DELETE from authors WHERE AuthorID=?', (author["AuthorID"],))

        # remove series with no members
        lazylibrarian.UPDATE_MSG = 'Removing series with no members'
        series = myDB.select('SELECT SeriesID,SeriesName FROM series WHERE Total=0')
        if series:
            for ser in series:  # check we haven't mis-counted
                res = myDB.match('select count(*) as counter from member where seriesid=?', (ser['SeriesID'],))
                if res:
                    counter = check_int(res['counter'], 0)
                    if counter:
                        myDB.action("UPDATE series SET Total=? WHERE SeriesID=?", (counter, ser['SeriesID']))
            series = myDB.select('SELECT SeriesID,SeriesName FROM series WHERE Total=0')
            if series:
                cnt += len(series)
                msg = 'Removing %s series with no members' % len(series)
                logger.warn(msg)
                for item in series:
                    logger.warn("Removing series %s:%s" % (item['SeriesID'], item['SeriesName']))
                    myDB.action('DELETE from series WHERE SeriesID=?', (item["SeriesID"],))

        # check if genre exclusions/translations have altered
        lazylibrarian.UPDATE_MSG = 'Checking for invalid genres'
        if lazylibrarian.GRGENRES:
            for item in lazylibrarian.GRGENRES.get('genreExclude', []):
                match = myDB.match('SELECT GenreID from genres where GenreName=? COLLATE NOCASE', (item,))
                if match:
                    cnt += 1
                    msg = 'Removing excluded genre [%s]' % item
                    logger.warn(msg)
                    myDB.action('DELETE from genrebooks WHERE GenreID=?', (match['GenreID'],))
                    myDB.action('DELETE from genres WHERE GenreID=?', (match['GenreID'],))
            for item in lazylibrarian.GRGENRES.get('genreExcludeParts', []):
                cmd = 'SELECT GenreID,GenreName from genres where GenreName like "%' + item + '%" COLLATE NOCASE'
                matches = myDB.select(cmd)
                if matches:
                    cnt += len(matches)
                    for itm in matches:
                        msg = 'Removing excluded genre [%s]' % itm['GenreName']
                        logger.warn(msg)
                        myDB.action('DELETE from genrebooks WHERE GenreID=?', (itm['GenreID'],))
                        myDB.action('DELETE from genres WHERE GenreID=?', (itm['GenreID'],))
            for item in lazylibrarian.GRGENRES.get('genreReplace', {}):
                match = myDB.match('SELECT GenreID from genres where GenreName=? COLLATE NOCASE', (item,))
                if match:
                    newitem = lazylibrarian.GRGENRES['genreReplace'][item]
                    newmatch = myDB.match('SELECT GenreID from genres where GenreName=? COLLATE NOCASE', (newitem,))
                    cnt += 1
                    msg = 'Replacing genre [%s] with [%s]' % (item, newitem)
                    logger.warn(msg)
                    if not newmatch:
                        myDB.action('INSERT into genres (GenreName) VALUES (?)', (newitem,))
                    res = myDB.select('SELECT bookid from genrebooks where genreid=?', (match['GenreID'],))
                    for bk in res:
                        cmd = 'select genrename from genres,genrebooks,books where genres.genreid=genrebooks.genreid '
                        cmd += ' and books.bookid=genrebooks.bookid and books.bookid=?'
                        bkgenres = myDB.select(cmd, (bk['bookid'],))
                        lst = []
                        for gnr in bkgenres:
                            lst.append(gnr['genrename'])
                        if item in lst:
                            lst.remove(item)
                        if newitem not in lst:
                            lst.append(newitem)
                        setGenres(lst, bk['bookid'])
        # remove genres with no books
        lazylibrarian.UPDATE_MSG = 'Removing genres with no books'
        cmd = 'select GenreID, (select count(*) as counter from genrebooks where genres.genreid = genrebooks.genreid)'
        cmd += ' as cnt from genres where cnt = 0'
        genres = myDB.select(cmd)
        if genres:
            cnt += len(genres)
            msg = 'Removing %s empty %s' % (len(genres), plural(len(genres), "genre"))
            logger.warn(msg)
            for item in genres:
                myDB.action('DELETE from genres WHERE GenreID=?', (item["GenreID"],))

        # remove any orphan entries (shouldnt happen with foreign key active)
        lazylibrarian.UPDATE_MSG = 'Removing orphans'
        for entry in [
                        ['authorid', 'books', 'authors'],
                        ['seriesid', 'member', 'series'],
                        ['seriesid', 'seriesauthors', 'series'],
                        ['seriesid', 'series', 'seriesauthors'],
                        ['authorid', 'seriesauthors', 'authors'],
                        ['title', 'issues', 'magazines'],
                        ['genreid', 'genrebooks', 'genres'],
                        ['comicid', 'comicissues', 'comics'],
                        ['userid', 'subscribers', 'users'],
                     ]:
            orphans = myDB.select('select %s from %s except select %s from %s' %
                                  (entry[0], entry[1], entry[0], entry[2]))
            if orphans:
                cnt += len(orphans)
                msg = 'Found %s orphan %s in %s' % (len(orphans), entry[0], entry[1])
                logger.warn(msg)
                for orphan in orphans:
                    myDB.action('DELETE from %s WHERE %s="%s"' % (entry[1], entry[0], orphan[0]))

        # reset any snatched entries in books table that don't match history/wanted
        lazylibrarian.UPDATE_MSG = 'Syncing Snatched entries'
        cmd = 'select bookid from books where status="Snatched" '
        cmd += 'except select bookid from wanted where status="Snatched" and auxinfo="eBook"'
        snatches = myDB.select(cmd)
        if snatches:
            cnt += len(snatches)
            msg = 'Found %s snatched ebook not snatched in wanted' % len(snatches)
            logger.warn(msg)
            for orphan in snatches:
                myDB.action('UPDATE books SET status="Skipped" WHERE bookid=?', (orphan[0],))

        cmd = 'select bookid from books where audiostatus="Snatched" '
        cmd += 'except select bookid from wanted where status="Snatched" and auxinfo="AudioBook"'
        snatches = myDB.select(cmd)
        if snatches:
            cnt += len(snatches)
            msg = 'Found %s snatched audiobook not snatched in wanted' % len(snatches)
            logger.warn(msg)
            for orphan in snatches:
                myDB.action('UPDATE books SET audiostatus="Skipped" WHERE bookid=?', (orphan[0],))

        # all authors with no books in the library and no books marked wanted unless series contributor
        cmd = 'select authorid from authors where havebooks=0 and Reason not like "%Series%" except '
        cmd += 'select authorid from wanted,books where books.bookid=wanted.bookid and books.status=="Wanted";'
        authors = myDB.select(cmd)
        if authors:
            cnt += len(authors)
            msg = 'Found %s %s with no books in the library or marked wanted' % (len(authors),
                                                                                 plural(len(authors), "author"))
            logger.warn(msg)
            # for author in authors:
            # name = myDB.match("SELECT authorname from authors where authorid=?", (author[0],))
            # logger.warn("%s %s" % (author[0], name[0]))
            # myDB.action('DELETE from authors where authorid=?', (author[0],))

        # update empty bookdate to "0000"
        lazylibrarian.UPDATE_MSG = 'Updating books with no bookdate'
        books = myDB.select('SELECT * FROM books WHERE BookDate is NULL or BookDate=""')
        if books:
            cnt += len(books)
            msg = 'Found %s %s with no bookdate' % (len(books), plural(len(books), "book"))
            logger.warn(msg)
            myDB.action('UPDATE books SET BookDate="0000" WHERE BookDate is NULL or BookDate=""')

    except Exception as e:
        msg = 'Error: %s %s' % (type(e).__name__, str(e))
        logger.error(msg)

    logger.info("Database check found %s %s" % (cnt, plural(cnt, "error")))
    lazylibrarian.UPDATE_MSG = ''
    return db_changes


def calc_eta(start_time, start_count, done):
    percent_done = done * 100 / start_count
    if not percent_done:
        secs_left = start_count * 1.5
    else:
        time_elapsed = time.time() - start_time
        secs_per_percent = time_elapsed / percent_done
        percent_left = 100 - percent_done
        secs_left = percent_left * secs_per_percent

    eta = int(secs_left / 60) + (secs_left % 60 > 0)
    if eta < 2:
        return "Completed %s%% eta %s minute" % (int(percent_done), eta)
    if eta < 120:
        return "Completed %s%% eta %s minutes" % (int(percent_done), eta)
    else:
        eta = int(secs_left / 3600) + (secs_left % 3600 > 0)
        return "Completed %s%% eta %s hours" % (int(percent_done), eta)


def db_v46(myDB, upgradelog):
    upgradelog.write("%s v46: %s\n" % (time.ctime(), "Re-creating past issues table"))
    myDB.action('DROP TABLE pastissues')
    res = myDB.match("SELECT sql FROM sqlite_master WHERE type='table' AND name='wanted'")
    myDB.action(res['sql'].replace('wanted', 'pastissues'))
    upgradelog.write("%s v46: complete\n" % time.ctime())


def db_v47(myDB, upgradelog):
    upgradelog.write("%s v47: %s\n" % (time.ctime(), "Creating genre tables"))
    if not has_column(myDB, "genres", "GenreID"):
        myDB.action('CREATE TABLE genres (GenreID INTEGER PRIMARY KEY AUTOINCREMENT, GenreName TEXT UNIQUE)')
        myDB.action('CREATE TABLE genrebooks (GenreID INTEGER REFERENCES genres (GenreID) ON DELETE CASCADE, ' +
                    'BookID TEXT REFERENCES books (BookID) ON DELETE CASCADE, ' +
                    'UNIQUE (GenreID,BookID))')
    res = myDB.select('SELECT bookid,bookgenre FROM books WHERE (Status="Open" or AudioStatus="Open")')
    tot = len(res)
    if tot:
        upgradelog.write("%s v47: Upgrading %s genres\n" % (time.ctime(), tot))
        cnt = 0
        for book in res:
            cnt += 1
            myDB.action('DELETE from genrebooks WHERE BookID=?', (book['bookid'],))
            lazylibrarian.UPDATE_MSG = "Updating genres %s of %s" % (cnt, tot)
            for item in getList(book['bookgenre'], ','):
                match = myDB.match('SELECT GenreID from genres where GenreName=? COLLATE NOCASE', (item,))
                if not match:
                    myDB.action('INSERT into genres (GenreName) VALUES (?)', (item,))
                    match = myDB.match('SELECT GenreID from genres where GenreName=?', (item,))
                myDB.action('INSERT into genrebooks (GenreID, BookID) VALUES (?,?)',
                            (match['GenreID'], book['bookid']), suppress='UNIQUE')
    upgradelog.write("%s v47: complete\n" % time.ctime())


def db_v48(myDB, upgradelog):
    upgradelog.write("%s v48: %s\n" % (time.ctime(), "Checking magazines table"))
    res = myDB.action("SELECT sql FROM sqlite_master WHERE type='table' AND name='magazines'")
    if 'Title TEXT UNIQUE' not in res:
        res = myDB.match('SELECT count(*) as cnt from magazines')
        upgradelog.write("%s v48: updating %s magazines\n" % (time.ctime(), res['cnt']))
        myDB.action('PRAGMA foreign_keys = OFF')
        myDB.action('DROP TABLE IF EXISTS temp')
        myDB.action('ALTER TABLE magazines RENAME to temp')
        myDB.action('CREATE TABLE magazines (Title TEXT UNIQUE, Regex TEXT, Status TEXT, MagazineAdded TEXT, ' +
                    'LastAcquired TEXT, IssueDate TEXT, IssueStatus TEXT, Reject TEXT, LatestCover TEXT, ' +
                    'DateType TEXT, CoverPage INTEGER DEFAULT 1)')
        myDB.action('INSERT INTO magazines SELECT Title,Regex,Status,MagazineAdded,LastAcquired,IssueDate,' +
                    'IssueStatus,Reject,LatestCover,DateType,CoverPage FROM temp')
        myDB.action('DROP TABLE temp')
        myDB.action('PRAGMA foreign_keys = ON')
    upgradelog.write("%s v48: complete\n" % time.ctime())


def db_v49(myDB, upgradelog):
    upgradelog.write("%s v49: %s\n" % (time.ctime(), "Checking authors table"))
    res = myDB.action("SELECT sql FROM sqlite_master WHERE type='table' AND name='authors'")
    if 'AuthorID TEXT UNIQUE' not in res or 'AuthorName TEXT UNIQUE' not in res:
        res = myDB.match('SELECT count(*) as cnt from authors')
        upgradelog.write("%s v49: updating %s authors\n" % (time.ctime(), res['cnt']))
        myDB.action('PRAGMA foreign_keys = OFF')
        myDB.action('DROP TABLE IF EXISTS temp')
        myDB.action('ALTER TABLE authors RENAME to temp')
        myDB.action('CREATE TABLE authors (AuthorID TEXT UNIQUE, AuthorName TEXT UNIQUE, ' +
                    'AuthorImg TEXT, AuthorLink TEXT, DateAdded TEXT, Status TEXT, LastBook TEXT, ' +
                    'LastBookImg TEXT, LastLink TEXT, LastDate TEXT, HaveBooks INTEGER DEFAULT 0, ' +
                    'TotalBooks INTEGER DEFAULT 0, AuthorBorn TEXT, AuthorDeath TEXT, ' +
                    'UnignoredBooks INTEGER DEFAULT 0, Manual TEXT, GRfollow TEXT, LastBookID TEXT)')
        myDB.action('INSERT INTO authors SELECT AuthorID,AuthorName,AuthorImg,AuthorLink,DateAdded,Status,' +
                    'LastBook,LastBookImg,LastLink,LastDate,HaveBooks,TotalBooks,AuthorBorn,AuthorDeath,' +
                    'UnignoredBooks,Manual,GRfollow,LastBookID FROM temp')
        myDB.action('DROP TABLE temp')
        myDB.action('PRAGMA foreign_keys = ON')
    upgradelog.write("%s v49: complete\n" % time.ctime())


def db_v50(myDB, upgradelog):
    upgradelog.write("%s v50: %s\n" % (time.ctime(), "Creating comics tables"))
    if not has_column(myDB, "comics", "ComicID"):
        myDB.action('CREATE TABLE comics (ComicID TEXT UNIQUE, Title TEXT, Status TEXT, ' +
                    'Added TEXT, LastAcquired TEXT, Updated TEXT, LatestIssue TEXT, IssueStatus TEXT, ' +
                    'LatestCover TEXT, SearchTerm TEXT, Start TEXT, First INTEGER, Last INTEGER, ' +
                    'Publisher TEXT, Link TEXT)')
        myDB.action('CREATE TABLE comicissues (ComicID TEXT REFERENCES comics (ComicID) ' +
                    'ON DELETE CASCADE, IssueID TEXT, IssueAcquired TEXT, IssueFile TEXT, ' +
                    'UNIQUE (ComicID, IssueID))')


def db_v51(myDB, upgradelog):
    if not has_column(myDB, "comics", "aka"):
        lazylibrarian.UPDATE_MSG = 'Adding aka to comics table'
        upgradelog.write("%s v51: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE comics ADD COLUMN aka TEXT')
    upgradelog.write("%s v51: complete\n" % time.ctime())


def db_v52(myDB, upgradelog):
    if not has_column(myDB, "series", "Updated"):
        lazylibrarian.UPDATE_MSG = 'Adding Updated column to series table'
        upgradelog.write("%s v52: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE series ADD COLUMN Updated INTEGER DEFAULT 0')
    upgradelog.write("%s v52: complete\n" % time.ctime())


def db_v53(myDB, upgradelog):
    if not has_column(myDB, "jobs", "Name"):
        lazylibrarian.UPDATE_MSG = 'Creating jobs table'
        upgradelog.write("%s v53: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('CREATE TABLE jobs (Name TEXT, LastRun INTEGER DEFAULT 0)')
    upgradelog.write("%s v53: complete\n" % time.ctime())


def db_v54(myDB, upgradelog):
    if not has_column(myDB, "authors", "Updated"):
        lazylibrarian.UPDATE_MSG = 'Separating dates in authors table'
        upgradelog.write("%s v54: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE authors ADD COLUMN Updated INTEGER DEFAULT 0')
        lazylibrarian.UPDATE_MSG = 'Updating author dates'
        upgradelog.write("%s v54: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        authors = myDB.select('SELECT AuthorID,AuthorImg,DateAdded from authors')
        cnt = 0
        if authors:
            tot = len(authors)
            for author in authors:
                cnt += 1
                lazylibrarian.UPDATE_MSG = "Updating Author dates: %s of %s" % (cnt, tot)
                updated = 0
                # noinspection PyBroadException
                try:
                    updated = int(time.mktime(datetime.datetime.strptime(author['DateAdded'],
                                                                         "%Y-%m-%d").timetuple()))
                except Exception:
                    upgradelog.write("%s v54: Error getting date from [%s] %s\n" %
                                     (time.ctime(), author['DateAdded'], author['AuthorID']))
                finally:
                    myDB.action('UPDATE authors SET Updated=? WHERE AuthorID=?',
                                (updated, author['AuthorID']))
            upgradelog.write("%s v54: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
    upgradelog.write("%s v54: complete\n" % time.ctime())


def db_v55(myDB, upgradelog):
    if not has_column(myDB, "authors", "Reason"):
        lazylibrarian.UPDATE_MSG = 'Adding Reason column to authors table'
        upgradelog.write("%s v55: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE authors ADD COLUMN Reason TEXT')
    upgradelog.write("%s v55: complete\n" % time.ctime())


def db_v56(myDB, upgradelog):
    if not has_column(myDB, "issues", "Cover"):
        lazylibrarian.UPDATE_MSG = 'Adding Cover column to issues table'
        upgradelog.write("%s v56: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE issues ADD COLUMN Cover TEXT')

        issues = myDB.select('SELECT IssueFile from issues')
        tot = len(issues)
        start_time = time.time()
        cnt = 0
        for issue in issues:
            cnt += 1
            lazylibrarian.UPDATE_MSG = 'Updating issue cover for %s: %s' % (issue['IssueFile'],
                                                                            calc_eta(start_time, tot, cnt))
            coverfile = os.path.splitext(issue['IssueFile'])[0] + '.jpg'
            if not path_exists(coverfile):
                coverfile = os.path.join(lazylibrarian.PROG_DIR, 'data', 'images', 'nocover.jpg')
            myhash = uuid.uuid4().hex
            hashname = os.path.join(lazylibrarian.CACHEDIR, 'magazine', '%s.jpg' % myhash)
            cachefile = 'cache/magazine/%s.jpg' % myhash
            copyfile(coverfile, hashname)
            setperm(hashname)
            myDB.action('UPDATE issues SET Cover=? WHERE IssueFile=?', (cachefile, issue['IssueFile']))

    if not has_column(myDB, "comicissues", "Cover"):
        lazylibrarian.UPDATE_MSG = 'Adding Cover column to comicissues table'
        upgradelog.write("%s v56: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE comicissues ADD COLUMN Cover TEXT')

        issues = myDB.select('SELECT * from comicissues')
        tot = len(issues)
        start_time = time.time()
        cnt = 0
        for issue in issues:
            cnt += 1
            lazylibrarian.UPDATE_MSG = 'Updating comicissue cover for %s: %s' % (issue['IssueFile'],
                                                                                 calc_eta(start_time,
                                                                                          tot, cnt))
            coverfile = os.path.splitext(issue['IssueFile'])[0] + '.jpg'
            if not path_exists(coverfile):
                coverfile = os.path.join(lazylibrarian.PROG_DIR, 'data', 'images', 'nocover.jpg')
            myhash = uuid.uuid4().hex
            hashname = os.path.join(lazylibrarian.CACHEDIR, 'comic', '%s.jpg' % myhash)
            cachefile = 'cache/comic/%s.jpg' % myhash
            copyfile(coverfile, hashname)
            setperm(hashname)
            myDB.action('UPDATE comicissues SET Cover=? WHERE IssueFile=?', (cachefile, issue['IssueFile']))

    upgradelog.write("%s v56: complete\n" % time.ctime())


def db_v57(myDB, upgradelog):
    if not has_column(myDB, "comicissues", "Description"):
        lazylibrarian.UPDATE_MSG = 'Adding Description, Link and Contributors columns to comicissues table'
        upgradelog.write("%s v57: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE comicissues ADD COLUMN Description TEXT')
        myDB.action('ALTER TABLE comicissues ADD COLUMN Link TEXT')
        myDB.action('ALTER TABLE comicissues ADD COLUMN Contributors TEXT')
    if not has_column(myDB, "comics", "Description"):
        lazylibrarian.UPDATE_MSG = 'Adding Description column to comics table'
        upgradelog.write("%s v57: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE comics ADD COLUMN Description TEXT')
    upgradelog.write("%s v57: complete\n" % time.ctime())


def db_v58(myDB, upgradelog):
    if not has_column(myDB, "comicissues", "Link"):
        lazylibrarian.UPDATE_MSG = 'Adding Link column to comicissues table'
        upgradelog.write("%s v58: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE comicissues ADD COLUMN Link TEXT')
    upgradelog.write("%s v58: complete\n" % time.ctime())


# noinspection PyUnusedLocal
def db_v59(myDB, upgradelog):
    seeders = lazylibrarian.CONFIG.get('NUMBEROFSEEDERS', 0)
    if seeders:
        lazylibrarian.UPDATE_MSG = 'Setting up SEEDERS'
        upgradelog.write("%s v58: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        for entry in lazylibrarian.TORZNAB_PROV:
            entry['SEEDERS'] = seeders
        for item in ['KAT_SEEDERS', 'WWT_SEEDERS', 'TPB_SEEDERS', 'ZOO_SEEDERS', 'TRF_SEEDERS',
                     'TDL_SEEDERS', 'LIME_SEEDERS']:
            lazylibrarian.CONFIG[item] = seeders
    lazylibrarian.CONFIG['NUMBEROFSEEDERS'] = 0
    lazylibrarian.config_write()
    upgradelog.write("%s v59: complete\n" % time.ctime())


# noinspection PyUnusedLocal
def db_v60(myDB, upgradelog):
    lazylibrarian.UPDATE_MSG = '<b>The old example_preprocessor is deprecated</b>'
    lazylibrarian.UPDATE_MSG += '<br>it\'s functions are now included in the main program'
    lazylibrarian.UPDATE_MSG += '<br>See new config options in "processing" tab'
    time.sleep(30)
    upgradelog.write("%s v60: complete\n" % time.ctime())


def update_schema(myDB, upgradelog):
    db_version = 0
    changes = 0

    result = myDB.match('PRAGMA user_version')
    if result and result[0]:
        value = str(result[0])
        if value.isdigit():
            db_version = int(value)
    logger.debug("Schema check v%s, database is v%s" % (db_current_version, db_version))
    if db_current_version != db_version:
        changes += 1

    if not has_column(myDB, "series", "Reason"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding Reason column to series table'
        upgradelog.write("%s v61: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE series ADD COLUMN Reason TEXT')
        myDB.action('UPDATE series SET Reason="Historic"')

    if not has_column(myDB, "authors", "About"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding About column to authors table'
        upgradelog.write("%s v62: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE authors ADD COLUMN About TEXT')

    if not has_column(myDB, "jobs", "Start"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Replacing jobs table'
        upgradelog.write("%s v63: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('DROP TABLE IF EXISTS temp')
        myDB.action('ALTER TABLE jobs RENAME to temp')
        myDB.action('CREATE TABLE jobs (Name TEXT, Finish INTEGER DEFAULT 0, Start INTEGER DEFAULT 0)')
        myDB.action('INSERT INTO jobs SELECT Name,LastRun as Start,LastRun as Finish FROM temp')
        myDB.action('DROP TABLE temp')

    if not has_column(myDB, "pastissues", "Added"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding Added column to pastissues table'
        upgradelog.write("%s v64: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE pastissues ADD COLUMN Added INTEGER DEFAULT 0')
        myDB.action('UPDATE pastissues SET Added=? WHERE Added=0', (int(time.time()),))

    if not has_column(myDB, "users", "Abandoned"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding Reading,Abandoned columns to users table'
        upgradelog.write("%s v65: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE users ADD COLUMN Reading TEXT')
        myDB.action('ALTER TABLE users ADD COLUMN Abandoned TEXT')

    if not has_column(myDB, "subscribers", "UserID"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Creating subscribers table'
        upgradelog.write("%s v66: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        act = 'CREATE TABLE subscribers (UserID TEXT REFERENCES users (UserID) ON DELETE CASCADE,'
        act += ' Type TEXT, WantID Text)'
        myDB.action(act)

    if not has_column(myDB, "users", "Prefs"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding Prefs to users table'
        upgradelog.write("%s v67: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE users ADD COLUMN Prefs INTEGER DEFAULT 0')

    if not has_column(myDB, "wanted", "Completed"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding Completed to wanted table'
        upgradelog.write("%s v68: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE wanted ADD COLUMN Completed INTEGER DEFAULT 0')

    if not has_column(myDB, "authors", "AKA"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding AKA to authors table'
        upgradelog.write("%s v69: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE authors ADD COLUMN AKA TEXT')

    if not has_column(myDB, "books", "LT_WorkID"):
        changes += 1
        lazylibrarian.UPDATE_MSG = 'Adding LT_WorkID to books table'
        upgradelog.write("%s v69: %s\n" % (time.ctime(), lazylibrarian.UPDATE_MSG))
        myDB.action('ALTER TABLE books ADD COLUMN LT_WorkID TEXT')

    if changes:
        upgradelog.write("%s Changed: %s\n" % (time.ctime(), changes))
    logger.debug("Schema changes: %s" % changes)
    return changes
