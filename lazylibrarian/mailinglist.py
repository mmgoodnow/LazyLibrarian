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
import lazylibrarian
from lazylibrarian import database, logger
from lazylibrarian.formatter import plural, getList, check_int
from lazylibrarian.notifiers import email_notifier


def mailing_list(book_type, global_name, book_id):
    if not lazylibrarian.CONFIG['USER_ACCOUNTS']:
        return
    myDB = database.DBConnection()
    columns = myDB.select('PRAGMA table_info(subscribers)')
    if not columns:  # no such table
        return

    # if book check users who subscribe to author or series
    # if from a wishlist or rss check which users are subscribed to the feed
    # if magazine or comic issue check which users subscribe to the title
    booktype = book_type.lower()
    if booktype in ['ebook', 'audiobook']:
        if booktype == 'ebook':
            data = myDB.match("SELECT Requester,AuthorID,BookFile as filename from books where bookid=?", (book_id,))
            feeds = getList(data['Requester'])
        else:
            data = myDB.match("SELECT AudioRequester,AuthorID,AudioFile as filename from books where bookid=?",
                              (book_id,))
            feeds = getList(data['AudioRequester'])

        users = myDB.select('SELECT UserID from subscribers WHERE Type="author" and WantID=?', (data['AuthorID'],))
        cnt = 0
        for user in users:
            myDB.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                        (user['UserID'], booktype, book_id))
            cnt += 1
        if cnt:
            logger.debug("%s wanted by %s %s to author %s" % (book_id, cnt, plural(cnt, 'subscriber'), data['Author']))

        series = myDB.select('SELECT SeriesID from member WHERE BookID=?', (book_id,))
        for item in series:
            users = myDB.select('SELECT UserID  from subscribers WHERE Type="series" and WantID=?', (item['SeriesID'],))
            cnt = 0
            for user in users:
                myDB.action('INSERT into subscribers (UserID , Type, WantID) VALUES (?, ?, ?)',
                            (user['UserID'], booktype, book_id))
                cnt += 1
            if cnt:
                logger.debug("%s wanted by %s %s to series %s" % (book_id, cnt, plural(cnt, 'subscriber'),
                                                                  item['SeriesID']))

        for item in feeds:
            users = myDB.select('SELECT UserID from subscribers WHERE Type="feed" and WantID=?', (item,))
            cnt = 0
            for user in users:
                myDB.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                            (user['UserID'], booktype, book_id))
                cnt += 1
            if cnt:
                logger.debug("%s wanted by %s %s to feed %s" % (book_id, cnt, plural(cnt, 'subscriber'), item))

    elif booktype == 'magazine':
        data = myDB.match("SELECT Title,IssueFile as filename from issues where IssueID=?", (book_id,))
        if not data:
            logger.error('Invalid issueid [%s]' % book_id)
            return
        users = myDB.select('SELECT UserID from subscribers WHERE Type="magazine" and WantID=?', (data['Title'],))
        cnt = 0
        for user in users:
            myDB.action('INSERT into subscribers (UserID, type, WantID) VALUES (?, ?, ?)',
                        (user['UserID'], booktype, book_id))
            cnt += 1
        if cnt:
            logger.debug("%s wanted by %s %s to magazine %s" % (book_id, cnt, plural(cnt, 'subscriber'), data['Title']))
    elif booktype == 'comic':
        try:
            comicid, issueid = book_id.split('_')
        except ValueError:
            logger.error("Invalid comicid/issueid [%s]" % book_id)
            return
        data = myDB.match("SELECT IssueFile as filename from comicissues where comicid=? and issueid=?",
                          (comicid, issueid))
        users = myDB.select('SELECT UserID from subscribers WHERE Type="comic" and WantID=?', (comicid,))
        cnt = 0
        for user in users:
            myDB.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                        (user['UserID'], booktype, book_id))
            cnt += 1
        if cnt:
            logger.debug("%s wanted by %s %s to comic %s" % (book_id, cnt, plural(cnt, 'subscriber'), comicid))
    else:
        logger.error("Invalid book_type [%s]" % book_type)
        return

    # now send to all users requesting it
    users = myDB.select('SELECT UserID from subscribers WHERE Type=? and WantID=?', (booktype, book_id))
    userlist = []
    for user in users:
        userlist.append(user['UserID'])

    userlist = set(userlist)  # eg in case subscribed to author and series or book in multiple wishlist

    if not len(userlist):
        logger.debug("%s %s not wanted by any users" % (book_type, global_name))
        return
    else:
        logger.debug("%s %s wanted by %s %s" % (book_type, global_name, len(userlist), plural(len(userlist), 'user')))

    if not data or not data['filename'] or not os.path.exists(data['filename']):
        logger.error("Unable to locate %s %s" % (booktype, book_id))
        return

    filename = data['filename']
    fsize = check_int(os.path.getsize(filename), 0)
    limit = check_int(lazylibrarian.CONFIG['EMAIL_LIMIT'], 0)
    if limit and fsize > limit * 1024 * 1024:
        msg = '%s is too large (%s) to email' % (os.path.split(filename)[1], fsize)
        logger.debug(msg)
        filename = ''

    count = 0
    for user in userlist:
        res = myDB.match('SELECT SendTo,BookType from users where UserID=?', (user,))
        if res and res['SendTo']:
            if booktype == 'ebook':
                pref = res['BookType']
                basename, extn = os.path.splitext(filename)
                prefname = "%s.%s" % (basename, pref)
                if os.path.exists(prefname):
                    filename = prefname

            if filename:
                logger.debug("Emailing %s to %s" % (filename, res['SendTo']))
                if global_name:
                    msg = global_name + ' is attached'
                else:
                    msg = ''
                result = email_notifier.email_file(subject="Message from LazyLibrarian",
                                                   message=msg, to_addr=res['SendTo'], files=[filename])
            else:
                if global_name:
                    logger.debug("Notifying %s to %s" % (global_name, res['SendTo']))
                    msg = global_name + ' is available for download'
                    result = email_notifier.email_file(subject="Message from LazyLibrarian",
                                                       message=msg, to_addr=res['SendTo'], files=[])

            if result:
                count += 1
                myDB.action("DELETE from subscribers WHERE UserID=? and Type=? and WantID=?",
                            (user, booktype, book_id))
            else:
                # should we also delete from mailing list if email failed?
                msg = "Failed to email file %s to %s" % (os.path.split(filename)[1], res['SendTo'])
                logger.error(msg)

    logger.debug("Emailed %s %s to %s %s" % (book_type, global_name, count, plural(count, 'user')))
