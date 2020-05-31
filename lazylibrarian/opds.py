#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  This file is part of LazyLibrarian.
#
#  LazyLibrarian is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  LazyLibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with LazyLibrarian.  If not, see <http://www.gnu.org/licenses/>.
#  Adapted for LazyLibrarian from Mylar

import datetime
import os

# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_parse import quote_plus, urlsplit

import cherrypy
import lazylibrarian
from cherrypy.lib.static import serve_file
from lazylibrarian import logger, database
from lazylibrarian.cache import cache_img
from lazylibrarian.common import mimeType, zipAudio, path_isfile
from lazylibrarian.formatter import makeUnicode, check_int, plural, getList
from lib.six import text_type, string_types

searchable = ['Authors', 'Magazines', 'Series', 'Author', 'RecentBooks', 'RecentAudio', 'RecentMags',
              'RatedBooks', 'RatedAudio', 'ReadBooks', 'ToReadBooks', 'Genre', 'Genres', 'Comics',
              'Comic', 'RecentComics']

cmd_list = searchable + ['root', 'Serve', 'search', 'Members', 'Magazine']


class OPDS(object):

    def __init__(self):
        self.cmd = None
        self.img = None
        self.filepath = None
        self.filename = None
        self.kwargs = None
        self.data = None
        self.user_agent = ''
        self.reader = ''

        self.PAGE_SIZE = check_int(lazylibrarian.CONFIG['OPDS_PAGE'], 0)

        if lazylibrarian.CONFIG['HTTP_ROOT'] is None:
            self.opdsroot = '/opds'
        elif lazylibrarian.CONFIG['HTTP_ROOT'].endswith('/'):
            self.opdsroot = lazylibrarian.CONFIG['HTTP_ROOT'] + 'opds'
        else:
            if lazylibrarian.CONFIG['HTTP_ROOT'] != '/':
                self.opdsroot = lazylibrarian.CONFIG['HTTP_ROOT'] + '/opds'
            else:
                self.opdsroot = lazylibrarian.CONFIG['HTTP_ROOT'] + 'opds'
        """
        my_ip = cherrypy.request.headers.get('X-Forwarded-Host')
        if not my_ip:
            my_ip = cherrypy.request.headers.get('Host')

        self.opdsroot = '%s://%s%s' % (cherrypy.request.scheme, my_ip, self.opdsroot)
        """
        self.searchroot = self.opdsroot.replace('/opds', '')

    def checkParams(self, **kwargs):
        if 'cmd' not in kwargs:
            self.cmd = 'root'

        if not lazylibrarian.CONFIG['OPDS_ENABLED']:
            self.data = self._error_with_message('OPDS not enabled')
            return

        if not self.cmd:
            if kwargs['cmd'] not in cmd_list:
                self.data = self._error_with_message('Unknown command: %s' % kwargs['cmd'])
                return
            else:
                self.cmd = kwargs.pop('cmd')

        self.kwargs = kwargs
        self.data = 'OK'

    def fetchData(self):
        if self.data == 'OK':
            remote_ip = cherrypy.request.headers.get('X-Forwarded-For')  # apache2
            if not remote_ip:
                remote_ip = cherrypy.request.headers.get('X-Host')  # lighthttpd
            if not remote_ip:
                remote_ip = cherrypy.request.headers.get('Remote-Addr')
            if not remote_ip:
                remote_ip = cherrypy.request.remote.ip

            self.user_agent = cherrypy.request.headers.get('User-Agent')
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug(self.user_agent)

            # NOTE Moon+ identifies as Aldiko/Moon+  so check for Moon+ first
            # at the moment we only need to identify Aldiko as it doesn't paginate properly
            IDs = ['Moon+', 'FBReader', 'Aldiko']
            for item in IDs:
                if item in self.user_agent:
                    self.reader = ' (' + item + ')'
                    break

            logger.debug('Received OPDS command from %s%s %s %s' % (remote_ip, self.reader, self.cmd, self.kwargs))

            if self.cmd == 'search':
                if 't' in self.kwargs and self.kwargs['t'] in searchable:
                    self.cmd = self.kwargs['t']
                else:
                    self.cmd = 'RecentBooks'
            methodToCall = getattr(self, "_" + self.cmd)
            try:
                _ = methodToCall(**self.kwargs)
                if self.img:
                    return serve_file(self.img, content_type='image/jpeg')
                if self.filepath and self.filename:
                    logger.debug('Downloading %s: %s' % (self.filename, self.filepath))
                    return serve_file(self.filepath, mimeType(self.filename), 'attachment', name=self.filename)
                if isinstance(self.data, string_types):
                    return self.data
                else:
                    cherrypy.response.headers['Content-Type'] = "text/xml"
                    logger.debug('Returning %s: %s entries' % (self.data['title'], len(self.data['entries'])))
                    # noinspection PyUnresolvedReferences
                    return lazylibrarian.webServe.serve_template(templatename="opds.html",
                                                                 title=self.data['title'], opds=self.data)
            except Exception as e:
                logger.error("Unhandled OPDS %s error: %s" % (self.cmd, e))
        else:
            return self.data

    def multiLink(self, bookfile, bookid):
        types = []
        multi = ''
        basename, _ = os.path.splitext(bookfile)
        if not isinstance(basename, text_type):
            basename = basename.decode('utf-8')
        for item in getList(lazylibrarian.CONFIG['EBOOK_TYPE']):
            target = basename + '.' + item
            if path_isfile(target):
                types.append(item)
        if len(types) > 1:
            for fmt in types:
                multi += '<link href="'
                multi += '%s?cmd=Serve&amp;bookid=%s&amp;fmt=%s' % (self.opdsroot, quote_plus(bookid), fmt)
                multi += '" rel="http://opds-spec.org/acquisition" type="' + mimeType('.' + fmt) + '"/>'
        return multi

    @staticmethod
    def _error_with_message(message):
        error = '<feed><error>%s</error></feed>' % message
        cherrypy.response.headers['Content-Type'] = "text/xml"
        return error

    def _root(self, **kwargs):
        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS', 'id': 'OPDSRoot', 'updated': now()}
        links = []
        entries = []

        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='self'))
        links.append(getLink(href='%s/opensearchbooks.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Books'))

        res = myDB.match("select count(*) as counter from books where Status='Open'")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Recent eBooks (%i)' % res['counter'],
                    'id': 'RecentBooks',
                    'updated': now(),
                    'content': 'Recently Added eBooks',
                    'href': '%s?cmd=RecentBooks%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("select count(*) as counter from books where AudioStatus='Open'")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Recent AudioBooks (%i)' % res['counter'],
                    'id': 'RecentAudio',
                    'updated': now(),
                    'content': 'Recently Added AudioBooks',
                    'href': '%s?cmd=RecentAudio%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("SELECT count(*) as counter from comics WHERE LastAcquired != ''")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Recent Comics (%i)' % res['counter'],
                    'id': 'RecentComics',
                    'updated': now(),
                    'content': 'Recently Added Comic Issues',
                    'href': '%s?cmd=RecentComics%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("select count(*) as counter from issues where IssueFile != ''")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Recent Magazine Issues (%i)' % res['counter'],
                    'id': 'RecentMags',
                    'updated': now(),
                    'content': 'Recently Added Magazine Issues',
                    'href': '%s?cmd=RecentMags%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("select count(*) as counter from books where Status='Open' and CAST(BookRate AS INTEGER) > 0")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Best Rated eBooks (%i)' % res['counter'],
                    'id': 'RatedBooks',
                    'updated': now(),
                    'content': 'Best Rated eBooks',
                    'href': '%s?cmd=RatedBooks%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        cmd = "select count(*) as counter from books"
        cmd += " where AudioStatus='Open' and CAST(BookRate AS INTEGER) > 0"
        res = myDB.match(cmd)
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Best Rated AudioBooks (%i)' % res['counter'],
                    'id': 'RatedAudio',
                    'updated': now(),
                    'content': 'Best Rated AudioBooks',
                    'href': '%s?cmd=RatedAudio%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        if userid:
            res = myDB.match('SELECT HaveRead,ToRead from users where userid=?', (kwargs['user'],))
            if res:
                readfilter = getList(res['HaveRead'])
            else:
                readfilter = []

            if len(readfilter) > 0:
                entries.append(
                    {
                        'title': 'Read Books (%i)' % len(readfilter),
                        'id': 'ReadBooks',
                        'updated': now(),
                        'content': 'Books marked as Read',
                        'href': '%s?cmd=ReadBooks%s' % (self.opdsroot, userid),
                        'kind': 'acquisition',
                        'rel': 'subsection',
                    }
                )

            if res:
                readfilter = getList(res['ToRead'])
            else:
                readfilter = []

            if len(readfilter) > 0:
                entries.append(
                    {
                        'title': 'To Read Books (%i)' % len(readfilter),
                        'id': 'ToReadBooks',
                        'updated': now(),
                        'content': 'Books marked as To-Read',
                        'href': '%s?cmd=ToReadBooks%s' % (self.opdsroot, userid),
                        'kind': 'acquisition',
                        'rel': 'subsection',
                    }
                )

        cmd = "SELECT count(*) as counter from authors WHERE Status != 'Ignored' and CAST(HaveBooks AS INTEGER) > 0"
        res = myDB.match(cmd)
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Authors (%i)' % res['counter'],
                    'id': 'Authors',
                    'updated': now(),
                    'content': 'List of Authors',
                    'href': '%s?cmd=Authors%s' % (self.opdsroot, userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("SELECT count(*) as counter from series WHERE CAST(Have AS INTEGER) > 0")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Series (%i)' % res['counter'],
                    'id': 'Series',
                    'updated': now(),
                    'content': 'List of Series',
                    'href': '%s?cmd=Series%s' % (self.opdsroot, userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }
            )
        cmd = 'select genrename,(select count(*) as counter from genrebooks,books where '
        cmd += 'genrebooks.genreid = genres.genreid and books.status="Open" '
        cmd += 'and books.bookid=genrebooks.bookid) as cnt from genres where cnt > 0'
        # cmd = "select distinct BookGenre from books where Status='Open' and BookGenre != '' and BookGenre !='Unknown'"
        res = myDB.select(cmd)
        if res and len(res) > 0:
            entries.append(
                {
                    'title': 'Genres (%i)' % len(res),
                    'id': 'Genres',
                    'updated': now(),
                    'content': 'Genres',
                    'href': '%s?cmd=Genres%s' % (self.opdsroot, userid),
                    'kind': 'acquisition',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("SELECT count(*) as counter from magazines WHERE LastAcquired != ''")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Magazines (%i)' % res['counter'],
                    'id': 'Magazines',
                    'updated': now(),
                    'content': 'List of Magazines',
                    'href': '%s?cmd=Magazines%s' % (self.opdsroot, userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }
            )

        res = myDB.match("SELECT count(*) as counter from comics WHERE LastAcquired != ''")
        if res['counter'] > 0:
            entries.append(
                {
                    'title': 'Comics (%i)' % res['counter'],
                    'id': 'Comics',
                    'updated': now(),
                    'content': 'List of Comics',
                    'href': '%s?cmd=Comics%s' % (self.opdsroot, userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }
            )

        feed['links'] = links
        feed['entries'] = entries
        self.data = feed
        return

    def _Genres(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'Aldiko' in self.reader:  # Aldiko doesn't paginate long lists
            limit = 0

        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Genres', 'id': 'Genres', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Genres%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchgenres.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Genre'))

        cmd = 'select genrename,(select count(*) as counter from genrebooks,books where '
        cmd += 'genrebooks.genreid = genres.genreid and books.status="Open" '
        cmd += 'and books.bookid=genrebooks.bookid) as cnt from genres where cnt > 0'
        if 'query' in kwargs:
            cmd += " and genrename LIKE '%" + kwargs['query'] + "%'"
        cmd += ' order by cnt DESC,genrename ASC'
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for genre in page:
            totalbooks = genre['cnt']
            name = makeUnicode(genre['genrename'])
            entry = {
                    'title': escape('%s (%s)' % (name, totalbooks)),
                    'id': escape('genre:%s' % genre['genrename']),
                    'updated': now(),
                    'content': escape('%s (%s)' % (name, totalbooks)),
                    'href': '%s?cmd=Genre&amp;genre=%s%s' % (self.opdsroot, quote_plus(genre['genrename']), userid),
                    'author': escape('%s' % name),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }

            entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Genres&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Genres&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "genre"), index + 1,
                                                            fin, len(results)))
        self.data = feed
        return

    def _Genre(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        if 'genre' not in kwargs:
            self.data = self._error_with_message('No Genre Provided')
            return
        links = []
        entries = []

        cmd = "SELECT BookName,BookDate,BookAdded,BookDesc,BookImg,BookFile,AudioFile,books.BookID "
        cmd += "from genrebooks,genres,books WHERE (books.Status='Open' or books.AudioStatus='Open') "
        cmd += "AND books.Bookid=genrebooks.BookID AND genrebooks.genreid=genres.genreid AND genrename=?"
        cmd += "order by BookName"
        results = myDB.select(cmd, (kwargs['genre'],))
        if not len(results):
            self.data = self._error_with_message('No results for Genre "%s"' % kwargs['genre'])
            return

        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for book in page:
            mime_type = None
            rel = 'file'
            if book['BookFile']:
                mime_type = self.multiLink(book['BookFile'], book['BookID'])
                if mime_type:
                    rel = 'multi'
                else:
                    mime_type = mimeType(book['BookFile'])

            elif book['AudioFile']:
                mime_type = mimeType(book['AudioFile'])
            if mime_type:
                cmd = 'SELECT AuthorName from authors,books WHERE authors.authorid = books.authorid AND '
                cmd += 'books.bookid=?'
                res = myDB.match(cmd, (book['BookID'],))
                author = res['AuthorName']
                entry = {'title': escape('%s' % book['BookName']),
                         'id': escape('book:%s' % book['BookID']),
                         'updated': opdstime(book['BookAdded']),
                         'href': '%s?cmd=Serve&amp;bookid=%s%s' % (self.opdsroot, book['BookID'], userid),
                         'kind': 'acquisition',
                         'rel': rel,
                         'author': escape("%s" % author),
                         'type': mime_type}

                if lazylibrarian.CONFIG['OPDS_METAINFO']:
                    entry['image'] = self.searchroot + '/' + book['BookImg']
                    entry['thumbnail'] = entry['image']
                    entry['content'] = escape('%s %s' % (book['BookName'], book['BookDesc']))
                else:
                    entry['content'] = escape('%s %s' % (book['BookName'], book['BookAdded']))
                entries.append(entry)

        feed = {'title': 'LazyLibrarian OPDS - Genre %s' % escape(kwargs['genre']),
                'id': 'genre:%s' % escape(kwargs['genre']), 'updated': now()}
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Genres%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchbooks.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Books'))

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Genre&amp;genre=%s&amp;index=%s%s' % (self.opdsroot,
                        quote_plus(kwargs['genre']), index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Genre&amp;genre=%s&amp;index=%s%s' % (self.opdsroot,
                        quote_plus(kwargs['genre']), index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s %s, %s to %s from %s" % (len(entries), escape(kwargs['genre']),
                                                               plural(len(entries), "book"), index + 1,
                                                               fin, len(results)))
        self.data = feed
        return

    def _Authors(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'Aldiko' in self.reader:  # Aldiko doesn't paginate long lists
            limit = 0

        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Authors', 'id': 'Authors', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Authors%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchauthors.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Authors'))
        cmd = "SELECT AuthorName,AuthorID,HaveBooks,TotalBooks,Updated,AuthorImg from Authors WHERE "
        if 'query' in kwargs:
            cmd += "AuthorName LIKE '%" + kwargs['query'] + "%' AND "
        cmd += "CAST(HaveBooks AS INTEGER) > 0 order by AuthorName"
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for author in page:
            totalbooks = check_int(author['TotalBooks'], 0)
            havebooks = check_int(author['HaveBooks'], 0)
            lastupdated = datetime.datetime.utcfromtimestamp(author['Updated']).strftime("%Y-%m-%d")
            name = makeUnicode(author['AuthorName'])
            entry = {
                    'title': escape('%s (%s/%s)' % (name, havebooks, totalbooks)),
                    'id': escape('author:%s' % author['AuthorID']),
                    'updated': opdstime(lastupdated),
                    'content': escape('%s (%s)' % (name, havebooks)),
                    'href': '%s?cmd=Author&amp;authorid=%s%s' % (self.opdsroot, author['AuthorID'], userid),
                    'author': escape('%s' % name),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }

            if lazylibrarian.CONFIG['OPDS_METAINFO']:
                entry['thumbnail'] = '/' + author['AuthorImg']
            entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Authors&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Authors&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "author"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _Comics(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'Aldiko' in self.reader:
            limit = 0

        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Comics', 'id': 'Comics', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Comics%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchcomics.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Comics'))
        cmd = 'select comics.*,(select count(*) as counter from comicissues where '
        cmd += 'comics.ComicID = comicissues.ComicID) as Iss_Cnt from comics '
        if 'query' in kwargs:
            cmd += "WHERE comics.title LIKE '%" + kwargs['query'] + "%' "
        cmd += 'order by comics.title'
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for mag in page:
            if mag['Iss_Cnt'] > 0:
                title = makeUnicode(mag['Title'])
                entry = {
                    'title': escape('%s %s (%s)' % (title, mag['Start'], mag['Iss_Cnt'])),
                    'id': escape('comic:%s' % mag['ComicID']),
                    'updated': opdstime(mag['LastAcquired']),
                    'content': escape('%s' % title),
                    'href': '%s?cmd=Comic&amp;magid=%s%s' % (self.opdsroot, mag['ComicID'], userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }

                # if lazylibrarian.CONFIG['OPDS_METAINFO']:
                #     res = cache_img('magazine', md5_utf8(mag['LatestCover']), mag['LatestCover'], refresh=True)
                #     entry['thumbnail'] = '/' + res[0]
                entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Comics&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Comics&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "comic"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _Magazines(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'Aldiko' in self.reader:
            limit = 0

        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Magazines', 'id': 'Magazines', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Magazines%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchmagazines.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Magazines'))
        cmd = 'select magazines.*,(select count(*) as counter from issues where magazines.title = issues.title)'
        cmd += ' as Iss_Cnt from magazines '
        if 'query' in kwargs:
            cmd += "WHERE magazines.title LIKE '%" + kwargs['query'] + "%' "
        cmd += 'order by magazines.title'
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for mag in page:
            if mag['Iss_Cnt'] > 0:
                title = makeUnicode(mag['Title'])
                entry = {
                    'title': escape('%s (%s)' % (title, mag['Iss_Cnt'])),
                    'id': escape('magazine:%s' % title),
                    'updated': opdstime(mag['LastAcquired']),
                    'content': escape('%s' % title),
                    'href': '%s?cmd=Magazine&amp;magid=%s%s' % (self.opdsroot, quote_plus(title), userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }

                # if lazylibrarian.CONFIG['OPDS_METAINFO']:
                #     res = cache_img('magazine', md5_utf8(mag['LatestCover']), mag['LatestCover'], refresh=True)
                #     entry['thumbnail'] = '/' + res[0]
                entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Magazines&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Magazines&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "magazine"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _Series(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'Aldiko' in self.reader:
            limit = 0

        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Series', 'id': 'Series', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Series%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchseries.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Series'))
        cmd = "SELECT SeriesName,SeriesID,Have,Total from Series WHERE CAST(Have AS INTEGER) > 0 "
        if 'query' in kwargs:
            cmd += "AND SeriesName LIKE '%" + kwargs['query'] + "%' "
        cmd += "order by SeriesName"
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for series in page:
            cmd = "SELECT books.BookID,SeriesNum from books,member where SeriesID=? "
            cmd += "and books.bookid = member.bookid order by CAST(SeriesNum AS INTEGER)"
            firstbook = myDB.match(cmd, (series['SeriesID'],))
            if firstbook:
                cmd = 'SELECT AuthorName from authors,books WHERE authors.authorid = books.authorid AND books.bookid=?'
                res = myDB.match(cmd, (firstbook['BookID'],))
                author = res['AuthorName']
            else:
                author = 'Unknown'
            totalbooks = check_int(series['Total'], 0)
            havebooks = check_int(series['Have'], 0)
            sername = makeUnicode(series['SeriesName'])
            entries.append(
                {
                    'title': escape('%s (%s/%s) %s' % (sername, havebooks, totalbooks, author)),
                    'id': escape('series:%s' % series['SeriesID']),
                    'updated': now(),
                    'content': escape('%s (%s)' % (sername, havebooks)),
                    'href': '%s?cmd=Members&amp;seriesid=%s%s' % (self.opdsroot, series['SeriesID'], userid),
                    'kind': 'navigation',
                    'rel': 'subsection',
                }
            )

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Series&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Series&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s series, %s to %s from %s" % (len(entries), index + 1, fin, len(results)))
        self.data = feed
        return

    def _Comic(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        if 'magid' not in kwargs:
            self.data = self._error_with_message('No ComicID Provided')
            return
        links = []
        entries = []
        title = ''
        comic = myDB.match("SELECT Title from comics WHERE ComicID=?", (kwargs['magid'],))
        if comic:
            title = makeUnicode(comic['Title'])
        cmd = "SELECT IssueID,IssueAcquired,IssueFile from comicissues "
        cmd += "WHERE ComicID=? order by IssueID DESC"
        results = myDB.select(cmd, (kwargs['magid'],))
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for issue in page:
            issueid = '%s_%s' % (kwargs['magid'], issue['IssueID'])
            entry = {'title': escape('%s (%s)' % (title, issue['IssueID'])),
                     'id': 'issue:%s' % issueid,
                     'updated': opdstime(issue['IssueAcquired']),
                     'content': escape('%s - %s' % (title, issue['IssueID'])),
                     'href': '%s?cmd=Serve&amp;comicissueid=%s%s' % (self.opdsroot, issueid, userid),
                     'kind': 'acquisition',
                     'rel': 'file',
                     'type': mimeType(issue['IssueFile'])}
            if lazylibrarian.CONFIG['OPDS_METAINFO']:
                fname = os.path.splitext(issue['IssueFile'])[0]
                res = cache_img('comic', issueid, fname + '.jpg')
                entry['image'] = self.searchroot + '/' + res[0]
                entry['thumbnail'] = entry['image']
            entries.append(entry)

        feed = {}
        title = '%s (%s)' % (escape(title), len(entries))
        feed['title'] = 'LazyLibrarian OPDS - %s' % title
        feed['id'] = 'comic:%s' % kwargs['magid']
        feed['updated'] = now()
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Comic&amp;magid=%s%s' % (self.opdsroot, kwargs['magid'], userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Comic&amp;magid=%s&amp;index=%s%s' % (self.opdsroot,
                        kwargs['magid'], index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Comic&amp;magid=%s&amp;index=%s%s' % (self.opdsroot,
                        kwargs['magid'], index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "issue"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _Magazine(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        if 'magid' not in kwargs:
            self.data = self._error_with_message('No Magazine Provided')
            return
        links = []
        entries = []
        title = ''
        cmd = "SELECT Title,IssueID,IssueDate,IssueAcquired,IssueFile from issues "
        cmd += "WHERE Title='%s' order by IssueDate DESC"
        results = myDB.select(cmd % kwargs['magid'])
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for issue in page:
            title = makeUnicode(issue['Title'])
            entry = {'title': escape('%s (%s)' % (title, issue['IssueDate'])),
                     'id': escape('issue:%s' % issue['IssueID']),
                     'updated': opdstime(issue['IssueAcquired']),
                     'content': escape('%s - %s' % (title, issue['IssueDate'])),
                     'href': '%s?cmd=Serve&amp;issueid=%s%s' % (self.opdsroot, quote_plus(issue['IssueID']), userid),
                     'kind': 'acquisition',
                     'rel': 'file',
                     'type': mimeType(issue['IssueFile'])}
            if lazylibrarian.CONFIG['OPDS_METAINFO']:
                fname = os.path.splitext(issue['IssueFile'])[0]
                res = cache_img('magazine', issue['IssueID'], fname + '.jpg')
                entry['image'] = self.searchroot + '/' + res[0]
                entry['thumbnail'] = entry['image']
            entries.append(entry)

        feed = {}
        title = '%s (%s)' % (escape(title), len(entries))
        feed['title'] = 'LazyLibrarian OPDS - %s' % title
        feed['id'] = 'magazine:%s' % escape(kwargs['magid'])
        feed['updated'] = now()
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Magazine&amp;magid=%s%s' % (self.opdsroot, quote_plus(kwargs['magid']),
                             userid), ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Magazine&amp;magid=%s&amp;index=%s%s' % (self.opdsroot,
                        quote_plus(kwargs['magid']), index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Magazine&amp;magid=%s&amp;index=%s%s' % (self.opdsroot,
                        quote_plus(kwargs['magid']), index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "issue"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _Author(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        if 'authorid' not in kwargs:
            self.data = self._error_with_message('No Author Provided')
            return
        links = []
        entries = []
        links.append(getLink(href='%s/opensearchbooks.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Books'))
        author = myDB.match("SELECT AuthorName from authors WHERE AuthorID=?", (kwargs['authorid'],))
        author = makeUnicode(author['AuthorName'])
        cmd = "SELECT BookName,BookDate,BookID,BookAdded,BookDesc,BookImg,BookFile,AudioFile from books WHERE "
        if 'query' in kwargs:
            cmd += "BookName LIKE '%" + kwargs['query'] + "%' AND "
        cmd += "(Status='Open' or AudioStatus='Open') and AuthorID=? order by BookDate DESC"
        results = myDB.select(cmd, (kwargs['authorid'],))
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for book in page:
            mime_type = None
            rel = 'file'
            if book['BookFile']:
                mime_type = self.multiLink(book['BookFile'], book['BookID'])
                if mime_type:
                    rel = 'multi'
                else:
                    mime_type = mimeType(book['BookFile'])

            elif book['AudioFile']:
                mime_type = mimeType(book['AudioFile'])

            if mime_type:
                if book['BookDate'] and book['BookDate'] != '0000':
                    disptitle = escape('%s (%s)' % (book['BookName'], book['BookDate']))
                else:
                    disptitle = escape('%s' % book['BookName'])
                entry = {'title': disptitle,
                         'id': escape('book:%s' % book['BookID']),
                         'updated': opdstime(book['BookAdded']),
                         'href': '%s?cmd=Serve&amp;bookid=%s%s' % (self.opdsroot, book['BookID'], userid),
                         'kind': 'acquisition',
                         'rel': rel,
                         'type': mime_type}
                if lazylibrarian.CONFIG['OPDS_METAINFO']:
                    entry['image'] = self.searchroot + '/' + book['BookImg']
                    entry['thumbnail'] = entry['image']
                    entry['content'] = escape('%s - %s' % (book['BookName'], book['BookDesc']))
                    entry['author'] = escape('%s' % author)
                else:
                    entry['content'] = escape('%s (%s)' % (book['BookName'], book['BookAdded']))
                entries.append(entry)

        feed = {}
        authorname = '%s (%s)' % (escape(author), len(entries))
        feed['title'] = 'LazyLibrarian OPDS - %s' % authorname
        feed['id'] = 'author:%s' % escape(kwargs['authorid'])
        feed['updated'] = now()
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Authors%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Author&amp;authorid=%s&amp;index=%s%s' % (self.opdsroot,
                        quote_plus(kwargs['authorid']), index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Author&amp;authorid=%s&amp;index=%s%s' % (self.opdsroot,
                        quote_plus(kwargs['authorid']), index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))
        feed['links'] = links
        feed['entries'] = entries
        self.data = feed
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "book"),
                                                            index + 1, fin, len(results)))
        return

    def _Members(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        if 'seriesid' not in kwargs:
            self.data = self._error_with_message('No Series Provided')
            return
        links = []
        entries = []
        series = myDB.match("SELECT SeriesName from Series WHERE SeriesID=?", (kwargs['seriesid'],))
        cmd = "SELECT BookName,BookDate,BookAdded,BookDesc,BookImg,BookFile,AudioFile,books.BookID,SeriesNum "
        cmd += "from books,member where (Status='Open' or AudioStatus='Open') and SeriesID=? "
        cmd += "and books.bookid = member.bookid order by CAST(SeriesNum AS INTEGER)"
        results = myDB.select(cmd, (kwargs['seriesid'],))
        cmd = 'SELECT AuthorName from authors,books WHERE authors.authorid = books.authorid AND '
        cmd += 'books.bookid=?'
        res = myDB.match(cmd, (results[0]['BookID'],))
        author = res['AuthorName']
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for book in page:
            mime_type = None
            rel = 'file'
            if book['BookFile']:
                mime_type = self.multiLink(book['BookFile'], book['BookID'])
                if mime_type:
                    rel = 'multi'
                else:
                    mime_type = mimeType(book['BookFile'])

            elif book['AudioFile']:
                mime_type = mimeType(book['AudioFile'])
            if mime_type:
                if book['SeriesNum']:
                    snum = ' (%s)' % book['SeriesNum']
                else:
                    snum = ''
                entry = {'title': escape('%s%s' % (book['BookName'], snum)),
                         'id': escape('book:%s' % book['BookID']),
                         'updated': opdstime(book['BookAdded']),
                         'href': '%s?cmd=Serve&amp;bookid=%s%s' % (self.opdsroot, book['BookID'], userid),
                         'kind': 'acquisition',
                         'rel': rel,
                         'author': escape("%s" % author),
                         'type': mime_type}

                if lazylibrarian.CONFIG['OPDS_METAINFO']:
                    entry['image'] = self.searchroot + '/' + book['BookImg']
                    entry['thumbnail'] = entry['image']
                    entry['content'] = escape('%s (%s %s) %s' % (book['BookName'], series['SeriesName'],
                                                                 book['SeriesNum'], book['BookDesc']))
                else:
                    entry['content'] = escape('%s (%s %s) %s' % (book['BookName'], series['SeriesName'],
                                                                 book['SeriesNum'], book['BookAdded']))
                entries.append(entry)

        feed = {}
        seriesname = '%s (%s) %s' % (escape(series['SeriesName']), len(entries), author)
        feed['title'] = 'LazyLibrarian OPDS - %s' % seriesname
        feed['id'] = 'series:%s' % escape(kwargs['seriesid'])
        feed['updated'] = now()
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=Series%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=Members&amp;seriesid=%s&amp;index=%s%s' % (self.opdsroot,
                        kwargs['seriesid'], index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=Members&amp;seriesid=%s&amp;index=%s%s' % (self.opdsroot,
                        kwargs['seriesid'], index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "book"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _RecentMags(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Recent Magazines', 'id': 'Recent Magazines', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=RecentMags%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchmagazines.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Magazines'))
        cmd = "select Title,IssueID,IssueAcquired,IssueDate,IssueFile,Cover from issues "
        cmd += "where IssueFile != '' "
        if 'query' in kwargs:
            cmd += "AND Title LIKE '%" + kwargs['query'] + "%' "
        cmd += "order by IssueAcquired DESC"
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for mag in page:
            title = makeUnicode(mag['Title'])
            entry = {'title': escape('%s' % mag['IssueDate']),
                     'id': escape('issue:%s' % mag['IssueID']),
                     'updated': opdstime(mag['IssueAcquired']),
                     'content': escape('%s - %s' % (title, mag['IssueDate'])),
                     'href': '%s?cmd=Serve&amp;issueid=%s' % (self.opdsroot, quote_plus(mag['IssueID'])),
                     'kind': 'acquisition',
                     'rel': 'file',
                     'author': escape(title),
                     'type': mimeType(mag['IssueFile'])}
            if lazylibrarian.CONFIG['OPDS_METAINFO']:
                entry['image'] = self.searchroot + '/' + mag['Cover']
                entry['thumbnail'] = entry['image']
            entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=RecentMags&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=RecentMags&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "issue"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _RecentComics(self, **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - Recent Comics', 'id': 'Recent Comics', 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=RecentComics%s' % (self.opdsroot, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchcomics.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Comics'))
        cmd = "select comics.ComicID,Title,IssueID,IssueAcquired,IssueFile,Start from comics,comicissues "
        cmd += "where comics.ComicID = comicissues.ComicID and IssueFile != '' "
        if 'query' in kwargs:
            cmd += "AND Title LIKE '%" + kwargs['query'] + "%' "
        cmd += "order by IssueAcquired DESC"
        results = myDB.select(cmd)
        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for mag in page:
            title = makeUnicode(mag['Title'])
            issueid = '%s_%s' % (mag['ComicID'], mag['IssueID'])
            entry = {'title': escape('%s %s' % (title, mag['Start'])),
                     'id': escape('issue:%s' % issueid),
                     'updated': opdstime(mag['IssueAcquired']),
                     'content': escape('%s - %s' % (title, mag['IssueID'])),
                     'href': '%s?cmd=Serve&amp;comicissueid=%s' % (self.opdsroot, issueid),
                     'kind': 'acquisition',
                     'rel': 'file',
                     'author': escape(title),
                     'type': mimeType(mag['IssueFile'])}
            if lazylibrarian.CONFIG['OPDS_METAINFO']:
                fname = os.path.splitext(mag['IssueFile'])[0]
                res = cache_img('comic', issueid, fname + '.jpg')
                entry['image'] = self.searchroot + '/' + res[0]
                entry['thumbnail'] = entry['image']
            entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=RecentComics&amp;index=%s%s' % (self.opdsroot, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=RecentComics&amp;index=%s%s' % (self.opdsroot, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "issue"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _ReadBooks(self, sorder='Read', **kwargs):
        if 'user' not in kwargs:
            sorder = ''
        return self._Books(sorder, **kwargs)

    def _ToReadBooks(self, sorder='ToRead', **kwargs):
        if 'user' not in kwargs:
            sorder = ''
        return self._Books(sorder, **kwargs)

    def _RatedBooks(self, sorder='Rated', **kwargs):
        return self._Books(sorder, **kwargs)

    def _RecentBooks(self, sorder='Recent', **kwargs):
        return self._Books(sorder, **kwargs)

    def _Books(self, sorder='Recent', **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - %s Books' % sorder, 'id': '%s Books' % sorder, 'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=%sBooks%s' % (self.opdsroot, sorder, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchbooks.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Books'))
        cmd = "select BookName,BookID,BookLibrary,BookDate,BookImg,BookDesc,BookRate,BookAdded,BookFile,AuthorID "
        cmd += "from books where Status='Open' "
        if 'query' in kwargs:
            cmd += "AND BookName LIKE '%" + kwargs['query'] + "%' "
        if sorder == 'Recent':
            cmd += "order by BookLibrary DESC, BookName ASC"
        if sorder == 'Rated':
            cmd += "and CAST(BookRate AS INTEGER) > 0 order by BookRate DESC, BookDate DESC"

        results = myDB.select(cmd)
        if results and lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug("Initial select found %s" % len(results))

        readfilter = None
        if sorder == 'Read' and 'user' in kwargs:
            res = myDB.match('SELECT HaveRead from users where userid=?', (kwargs['user'],))
            if res:
                readfilter = getList(res['HaveRead'])
            else:
                readfilter = []
        if sorder == 'ToRead' and 'user' in kwargs:
            res = myDB.match('SELECT ToRead from users where userid=?', (kwargs['user'],))
            if res:
                readfilter = getList(res['ToRead'])
            else:
                readfilter = []

        if readfilter is not None:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug("Filter length %s" % len(readfilter))
            filtered = []
            for res in results:
                if res['BookID'] in readfilter:
                    filtered.append(res)
            results = filtered
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug("Filter matches %s" % len(results))

        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for book in page:
            mime_type = None
            rel = 'file'
            if book['BookFile']:
                mime_type = self.multiLink(book['BookFile'], book['BookID'])
                if mime_type:
                    rel = 'multi'
                else:
                    mime_type = mimeType(book['BookFile'])

            elif book['AudioFile']:
                mime_type = mimeType(book['AudioFile'])
            if mime_type:
                title = makeUnicode(book['BookName'])
                if sorder == 'Rated':
                    dispname = escape("%s (%s)" % (title, book['BookRate']))
                else:
                    dispname = escape(title)
                entry = {'title': dispname,
                         'id': escape('book:%s' % book['BookID']),
                         'updated': opdstime(book['BookLibrary']),
                         'href': '%s?cmd=Serve&amp;bookid=%s%s' % (self.opdsroot, quote_plus(book['BookID']), userid),
                         'kind': 'acquisition',
                         'rel': rel,
                         'type': mime_type}

                if lazylibrarian.CONFIG['OPDS_METAINFO']:
                    auth = myDB.match("SELECT AuthorName from authors WHERE AuthorID=?", (book['AuthorID'],))
                    if auth:
                        author = makeUnicode(auth['AuthorName'])
                        entry['image'] = self.searchroot + '/' + book['BookImg']
                        entry['thumbnail'] = entry['image']
                        entry['content'] = escape('%s - %s' % (title, book['BookDesc']))
                        entry['author'] = escape('%s' % author)
                else:
                    entry['content'] = escape('%s (%s)' % (title, book['BookAdded']))
                entries.append(entry)

            """
                <link type="application/epub+zip" rel="http://opds-spec.org/acquisition"
                title="EPUB (no images)" length="18552" href="//www.gutenberg.org/ebooks/57490.epub.noimages"/>
                <link type="application/x-mobipocket-ebook" rel="http://opds-spec.org/acquisition"
                title="Kindle (no images)" length="110360" href="//www.gutenberg.org/ebooks/57490.kindle.noimages"/>
            """

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=%sBooks&amp;index=%s%s' % (self.opdsroot, sorder, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=%sBooks&amp;index=%s%s' % (self.opdsroot, sorder, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "book"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _RatedAudio(self, sorder='Rated', **kwargs):
        return self._Audio(sorder, **kwargs)

    def _RecentAudio(self, sorder='Recent', **kwargs):
        return self._Audio(sorder, **kwargs)

    def _Audio(self, sorder='Recent', **kwargs):
        index = 0
        limit = self.PAGE_SIZE
        if 'index' in kwargs:
            index = check_int(kwargs['index'], 0)
        userid = ''
        if 'user' in kwargs:
            userid = '&amp;user=%s' % kwargs['user']

        myDB = database.DBConnection()
        feed = {'title': 'LazyLibrarian OPDS - %s AudioBooks' % sorder, 'id': '%s AudioBooks' % sorder,
                'updated': now()}
        links = []
        entries = []
        links.append(getLink(href=self.opdsroot, ftype='application/atom+xml; profile=opds-catalog; kind=navigation',
                             rel='start', title='Home'))
        links.append(getLink(href='%s?cmd=%sAudio%s' % (self.opdsroot, sorder, userid),
                             ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='self'))
        links.append(getLink(href='%s/opensearchbooks.xml' % self.searchroot,
                             ftype='application/opensearchdescription+xml', rel='search', title='Search Books'))

        cmd = "select BookName,BookID,AudioLibrary,BookDate,BookImg,BookDesc,BookRate,BookAdded,AuthorID"
        cmd += " from books WHERE "
        if 'query' in kwargs:
            cmd += "BookName LIKE '%" + kwargs['query'] + "%' AND "
        cmd += "AudioStatus='Open'"
        if sorder == 'Recent':
            cmd += " order by AudioLibrary DESC, BookName ASC"
        if sorder == 'Rated':
            cmd += " order by BookRate DESC, BookDate DESC"
        results = myDB.select(cmd)

        if limit:
            page = results[index:(index + limit)]
        else:
            page = results
            limit = len(page)
        for book in page:
            title = makeUnicode(book['BookName'])
            if sorder == 'Rated':
                dispname = escape("%s (%s)" % (title, book['BookRate']))
            else:
                dispname = escape(title)
            entry = {'title': dispname,
                     'id': escape('audio:%s' % book['BookID']),
                     'updated': opdstime(book['AudioLibrary']),
                     'href': '%s?cmd=Serve&amp;audioid=%s%s' % (self.opdsroot, quote_plus(book['BookID']), userid),
                     'kind': 'acquisition',
                     'rel': 'file',
                     'type': mimeType("we_send.zip")}
            if lazylibrarian.CONFIG['OPDS_METAINFO']:
                auth = myDB.match("SELECT AuthorName from authors WHERE AuthorID=?", (book['AuthorID'],))
                if auth:
                    author = makeUnicode(auth['AuthorName'])
                    entry['image'] = self.searchroot + '/' + book['BookImg']
                    entry['thumbnail'] = entry['image']
                    entry['content'] = escape('%s - %s' % (title, book['BookDesc']))
                    entry['author'] = escape('%s' % author)
            else:
                entry['content'] = escape('%s (%s)' % (title, book['BookAdded']))
            entries.append(entry)

        if len(results) > (index + limit):
            links.append(
                getLink(href='%s?cmd=%sAudio&amp;index=%s%s' % (self.opdsroot, sorder, index + limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='next'))
        if index >= limit:
            links.append(
                getLink(href='%s?cmd=%sAudio&amp;index=%s%s' % (self.opdsroot, sorder, index - limit, userid),
                        ftype='application/atom+xml; profile=opds-catalog; kind=navigation', rel='previous'))

        feed['links'] = links
        feed['entries'] = entries
        fin = index + limit
        if fin > len(results):
            fin = len(results)
        logger.debug("Returning %s %s, %s to %s from %s" % (len(entries), plural(len(entries), "audiobook"),
                                                            index + 1, fin, len(results)))
        self.data = feed
        return

    def _Serve(self, **kwargs):
        if 'bookid' in kwargs:
            if 'fmt' in kwargs:
                fmt = kwargs['fmt']
            else:
                fmt = ''
            myid = kwargs['bookid']
            myDB = database.DBConnection()
            res = myDB.match('SELECT BookFile,BookName from books where bookid=?', (myid,))
            bookfile = res['BookFile']
            if fmt:
                bookfile = os.path.splitext(bookfile)[0] + '.' + fmt
            self.filepath = bookfile
            self.filename = os.path.split(bookfile)[1]
            return
        elif 'issueid' in kwargs:
            myid = kwargs['issueid']
            myDB = database.DBConnection()
            res = myDB.match('SELECT IssueFile from issues where issueid=?', (myid,))
            self.filepath = res['IssueFile']
            self.filename = os.path.split(res['IssueFile'])[1]
            return
        elif 'comicissueid' in kwargs:
            myid = kwargs['comicissueid']
            myDB = database.DBConnection()
            try:
                comicid, issueid = myid.split('_')
            except ValueError:
                return
            res = myDB.match('SELECT IssueFile from comicissues where comicid=? and issueid=?',
                             (comicid, issueid))
            self.filepath = res['IssueFile']
            self.filename = os.path.split(res['IssueFile'])[1]
            return
        elif 'audioid' in kwargs:
            myid = kwargs['audioid']
            myDB = database.DBConnection()
            res = myDB.match('SELECT AudioFile,BookName from books where BookID=?', (myid,))
            basefile = res['AudioFile']
            # zip up all the audiobook parts
            if basefile and path_isfile(basefile):
                target = zipAudio(os.path.dirname(basefile), res['BookName'])
                self.filepath = target
                self.filename = res['BookName'] + '.zip'
            return


def getLink(href=None, ftype=None, rel=None, title=None):
    link = {}
    if href:
        link['href'] = href
    if ftype:
        link['type'] = ftype
    if rel:
        link['rel'] = rel
    if title:
        link['title'] = title
    return link


def escape(data):
    """Escape &, <, and > in a string of data.
    """
    # must do ampersand first
    data = data.replace("&", "&amp;")
    data = data.replace(">", "&gt;")
    data = data.replace("<", "&lt;")
    return data


def now():
    dtnow = datetime.datetime.now()
    return dtnow.strftime("%Y-%m-%dT%H:%M:%SZ")


def opdstime(datestr):
    # YYYY-MM-DDTHH:MM:SSZ
    if not datestr:
        return now()
    if len(datestr) == 10:
        return "%s%s" % (datestr, 'T00:00:00Z')
    elif len(datestr) == 19:
        return "%sT%sZ" % (datestr[:10], datestr[11:])
    return now()
