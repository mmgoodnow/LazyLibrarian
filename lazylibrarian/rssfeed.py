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

import os
import time
import datetime
import traceback

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.common import mimeType

# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_parse import unquote_plus

try:
    from rfeed import Item, Guid, Feed, iTunes, iTunesItem, iTunesOwner, iTunesCategory, Enclosure
except ImportError:
    from lib.rfeed import Item, Guid, Feed, iTunes, iTunesItem, iTunesOwner, iTunesCategory, Enclosure
try:
    from lib.tinytag import TinyTag
except ImportError:
    TinyTag = None


def genFeed(ftype, limit=10, user=0, baseurl='', authorid=None, onetitle=None):
    res = ''
    if not lazylibrarian.CONFIG['RSS_ENABLED']:
        return res
    # noinspection PyBroadException
    try:
        podcast = False
        if ftype == 'eBook':
            cmd = "select AuthorName,BookName,BookDesc,BookLibrary,BookFile,BookID from books,authors where "
            if authorid:
                cmd += 'books.AuthorID = ? and '
            cmd += "BookLibrary != '' and books.AuthorID = authors.AuthorID order by BookLibrary desc limit ?"
            baselink = baseurl + '/bookWall&have=1'
        elif ftype == 'AudioBook':
            podcast = lazylibrarian.CONFIG['RSS_PODCAST']
            cmd = "select AuthorName,BookName,BookSub,BookDesc,AudioLibrary,AudioFile,BookID "
            cmd += "from books,authors where "
            if authorid:
                cmd += 'books.AuthorID = ? and '
            cmd += "AudioLibrary != '' and books.AuthorID = authors.AuthorID "
            cmd += "order by AudioLibrary desc limit ?"
            baselink = baseurl + '/audioWall'
        elif ftype == 'Magazine':
            cmd = "select Title,IssueDate,IssueAcquired,IssueFile,IssueID from issues "
            if onetitle:
                cmd += 'where Title = ? '
            cmd += "order by IssueAcquired desc limit ?"
            baselink = baseurl + '/magWall'
        elif ftype == 'Comic':
            cmd = "select Title,Publisher,comics.ComicID,IssueAcquired,IssueFile,IssueID from comics,comicissues where"
            if onetitle:
                cmd += 'Title = ? and '
            cmd += "comics.comicid=comicissues.comicid order by IssueAcquired desc limit ?"
            baselink = baseurl + '/comicWall'
        else:
            logger.debug("Invalid feed type")
            return res

        myDB = database.DBConnection()
        if authorid:
            results = myDB.select(cmd, (authorid, limit))
        elif onetitle:
            results = myDB.select(cmd, (unquote_plus(onetitle), limit))
        else:
            results = myDB.select(cmd, (limit,))
        items = []
        logger.debug("Found %s %s results" % (len(results), ftype))

        if not results:
            podcast = False

        for res in results:
            link = ''
            itunes_item = ''
            if ftype == 'eBook':
                pubdate = datetime.datetime.strptime(res['BookLibrary'], '%Y-%m-%d %H:%M:%S')
                title = res['BookName']
                author = res['AuthorName']
                description = res['BookDesc']
                bookid = res['BookID']
                extn = os.path.splitext(res['BookFile'])[1]
                if user:
                    link = '%s/serveBook/%s%s%s' % (baseurl, user, res['BookID'], extn)

            elif ftype == 'AudioBook':
                pubdate = datetime.datetime.strptime(res['AudioLibrary'], '%Y-%m-%d %H:%M:%S')
                title = res['BookName']
                author = res['AuthorName']
                description = res['BookDesc']
                bookid = res['BookID']
                extn = os.path.splitext(res['AudioFile'])[1]
                if user:
                    link = '%s/serveAudio/%s%s%s' % (baseurl, user, res['BookID'], extn)

                if TinyTag:
                    id3r = TinyTag.get(res['AudioFile'])
                    secs = id3r.duration
                    duration = time.strftime('%H:%M:%S', time.gmtime(secs))
                else:
                    duration = "01:11:02"  # any value as default

                itunes_item = iTunesItem(
                    author=res['AuthorName'],
                    image='%s/serveImg/%s%s.jpg' % (baseurl, user, res['BookID']),
                    duration=duration,
                    explicit="clean",
                    subtitle=res['BookSub'],
                    summary=res['BookDesc'])

            elif ftype == 'Magazine':
                pubdate = datetime.datetime.strptime(res['IssueAcquired'], '%Y-%m-%d')
                title = "%s (%s)" % (res['Title'], res['IssueDate'])
                author = res['Title']
                description = title
                bookid = res['IssueID']
                extn = os.path.splitext(res['IssueFile'])[1]
                if user:
                    link = '%s/serveIssue/%s%s%s' % (baseurl, user, res['IssueID'], extn)

            else:  # if ftype == 'Comic':
                pubdate = datetime.datetime.strptime(res['IssueAcquired'], '%Y-%m-%d')
                title = res['Title']
                author = res['Publisher']
                description = title
                bookid = res['IssueID']
                extn = os.path.splitext(res['IssueFile'])[1]
                if user:
                    link = '%s/serveComic/%s%s_%s%s' % (baseurl, user, res['ComicID'], res['IssueID'], extn)

            if podcast:
                item = Item(
                    title=title,
                    link=link,
                    description=description,
                    author=author,
                    guid=Guid(bookid),
                    pubDate=pubdate,
                    enclosure=Enclosure(url=link, length=0, type=mimeType(res['AudioFile'])),
                    extensions=[itunes_item]
                )
            else:
                item = Item(
                    title=title,
                    link=link,
                    description=description,
                    author=author,
                    guid=Guid(bookid),
                    pubDate=pubdate
                )
            items.append(item)

        itunes = iTunes(
            author="LazyLibrarian",
            subtitle="Podcast of recent audiobooks",
            summary="Audiobooks in the library",
            image='%s/serveImg/%s%s.png' % (baseurl, user, ''),
            explicit="clean",
            categories=iTunesCategory(name='AudioBooks', subcategory='Recent AudioBooks'),
            owner=iTunesOwner(name='LazyLibrarian', email=lazylibrarian.CONFIG['ADMIN_EMAIL']))

        title = "%s Recent Downloads" % ftype
        if authorid and results:
            title = "%s %s Recent Downloads" % (results[0]['AuthorName'], ftype)
        elif onetitle and results:
            title = "%s %s Recent Downloads" % (onetitle, ftype)

        if podcast:
            feed = Feed(
                title="Podcast RSS Feed",
                link=baselink,
                description="LazyLibrarian %s" % title,
                language="en-US",
                lastBuildDate=datetime.datetime.now(),
                items=items,
                extensions=[itunes])
        else:
            feed = Feed(
                title=title,
                link=baselink,
                description="LazyLibrarian %s" % title,
                language="en-US",
                lastBuildDate=datetime.datetime.now(),
                items=items)

        logger.debug("Returning %s %s" % (len(items), ftype))

        res = feed.rss()
    except Exception:
        logger.error('Unhandled exception in rssfeed: %s' % traceback.format_exc())
    return res
