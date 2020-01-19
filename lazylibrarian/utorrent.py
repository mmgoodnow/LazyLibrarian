#  This file is part of LazyLibrarian.
#  LazyLibrarian is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  LazyLibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with LazyLibrarian.  If not, see <http://www.gnu.org/licenses/>.


import json
import re

# noinspection PyUnresolvedReferences
from lib.six.moves import http_cookiejar
# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_error import HTTPError
# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_parse import urljoin, urlencode
# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_request import HTTPCookieProcessor, HTTPBasicAuthHandler, \
    build_opener, install_opener, Request

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.common import getUserAgent
from lazylibrarian.formatter import check_int, getList
from lib.six import PY2


class utorrentclient(object):
    TOKEN_REGEX = b"<div id='token' style='display:none;'>([^<>]+)</div>"

    # noinspection PyUnusedLocal
    def __init__(self, base_url='',  # lazylibrarian.CONFIG['UTORRENT_HOST'],
                 username='',  # lazylibrarian.CONFIG['UTORRENT_USER'],
                 password='',):  # lazylibrarian.CONFIG['UTORRENT_PASS']):

        host = lazylibrarian.CONFIG['UTORRENT_HOST']
        port = check_int(lazylibrarian.CONFIG['UTORRENT_PORT'], 0)
        if not host or not port:
            logger.error('Invalid Utorrent host or port, check your config')

        if not host.startswith("http://") and not host.startswith("https://"):
            host = 'http://' + host

        if host.endswith('/'):
            host = host[:-1]

        if host.endswith('/gui'):
            host = host[:-4]

        if lazylibrarian.CONFIG['UTORRENT_BASE']:
            host = "%s:%s/%s" % (host, port, lazylibrarian.CONFIG['UTORRENT_BASE'].strip('/'))
        else:
            host = "%s:%s" % (host, port)

        self.base_url = host
        self.username = lazylibrarian.CONFIG['UTORRENT_USER']
        self.password = lazylibrarian.CONFIG['UTORRENT_PASS']
        self.opener = self._make_opener('uTorrent', self.base_url, self.username, self.password)
        self.token = self._get_token()
        if not PY2 and self.token is not None:
            self.token = self.token.decode('utf-8')
        # TODO refresh token, when necessary

    @staticmethod
    def _make_opener(realm, base_url, username, password):
        """uTorrent API need HTTP Basic Auth and cookie support for token verify."""
        auth = HTTPBasicAuthHandler()
        auth.add_password(realm=realm, uri=base_url, user=username, passwd=password)
        opener = build_opener(auth)
        install_opener(opener)

        cookie_jar = http_cookiejar.CookieJar()
        cookie_handler = HTTPCookieProcessor(cookie_jar)

        handlers = [auth, cookie_handler]
        opener = build_opener(*handlers)
        return opener

    def _get_token(self):
        url = urljoin(self.base_url, 'gui/token.html')
        try:
            response = self.opener.open(url)
        except Exception as err:
            logger.error('%s getting Token. uTorrent responded with: %s' % (type(err).__name__, str(err)))
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('URL: %s' % url)
            return None
        match = re.search(utorrentclient.TOKEN_REGEX, response.read())
        return match.group(1)

    def list(self, **kwargs):
        params = [('list', '1')]
        params += list(kwargs.items())
        # HASH (string),
        # STATUS* (integer),
        #   1 = Started
        #   2 = Checking
        #   4 = Start after check
        #   8 = Checked
        #   16 = Error
        #   32 = Paused
        #   64 = Queued
        #   128 = Loaded
        # NAME (string),
        # SIZE (integer in bytes),
        # PERCENT PROGRESS (integer in per mils),
        # DOWNLOADED (integer in bytes),
        # UPLOADED (integer in bytes),
        # RATIO (integer in per mils),
        # UPLOAD SPEED (integer in bytes per second),
        # DOWNLOAD SPEED (integer in bytes per second),
        # ETA (integer in seconds),
        # LABEL (string),
        # PEERS CONNECTED (integer),
        # PEERS IN SWARM (integer),
        # SEEDS CONNECTED (integer),
        # SEEDS IN SWARM (integer),
        # AVAILABILITY (integer in 1/65535ths),
        # TORRENT QUEUE ORDER (integer),
        # REMAINING (integer in bytes)
        return self._action(params)

    def add_url(self, url):
        # can recieve magnet or normal .torrent link
        params = [('action', 'add-url'), ('s', url)]
        return self._action(params)

    def start(self, *hashes):
        params = [('action', 'start'), ]
        for hashid in hashes:
            params.append(('hash', hashid))
        return self._action(params)

    def stop(self, *hashes):
        params = [('action', 'stop'), ]
        for hashid in hashes:
            params.append(('hash', hashid))
        return self._action(params)

    def pause(self, *hashes):
        params = [('action', 'pause'), ]
        for hashid in hashes:
            params.append(('hash', hashid))
        return self._action(params)

    def forcestart(self, *hashes):
        params = [('action', 'forcestart'), ]
        for hashid in hashes:
            params.append(('hash', hashid))
        return self._action(params)

    def getfiles(self, hashid):
        params = [('action', 'getfiles'), ('hash', hashid)]
        res = self._action(params)
        files = res[1].get('files')
        if not files:
            return []
        flist = []
        for entry in files[1]:
            flist.append({"filename": entry[0], "filesize": entry[1]})
        return flist

    def getprops(self, hashid):
        params = [('action', 'getprops'), ('hash', hashid)]
        return self._action(params)

    def removedata(self, hashid):
        params = [('action', 'removedata'), ('hash', hashid)]
        return self._action(params)

    def remove(self, hashid):
        params = [('action', 'remove'), ('hash', hashid)]
        return self._action(params)

    def setprops(self, hashid, s, v):
        params = [('action', 'setprops'), ('hash', hashid), ("s", s), ("v", v)]
        return self._action(params)

    def setprio(self, hashid, priority, *files):
        params = [('action', 'setprio'), ('hash', hashid), ('p', str(priority))]
        for file_index in files:
            params.append(('f', str(file_index)))
        return self._action(params)

    def _action(self, params, body=None, content_type=None):
        url = "%s/gui/?token=%s&%s" % (self.base_url, self.token, urlencode(params))
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug("uTorrent params %s" % str(params))
        request = Request(url)
        if lazylibrarian.CONFIG['PROXY_HOST']:
            for item in getList(lazylibrarian.CONFIG['PROXY_TYPE']):
                request.set_proxy(lazylibrarian.CONFIG['PROXY_HOST'], item)
        request.add_header('User-Agent', getUserAgent())

        if body:
            if PY2:
                request.add_data(body)
            else:
                request.data(body)
            request.add_header('Content-length', len(body))
        if content_type:
            request.add_header('Content-type', content_type)

        try:
            response = self.opener.open(request)
            res = response.code
            js = json.loads(response.read())
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug("uTorrent response code %s" % res)
                logger.debug(str(js))
            return res, js
        except Exception as err:
            logger.debug('URL: %s' % url)
            logger.debug('uTorrent webUI raised the following error: ' + str(err))
            return 0, str(err)


def checkLink():
    """ Check we can talk to utorrent"""
    try:
        client = utorrentclient()
        if client.token is not None:
            try:
                _ = client.list()
            except Exception as err:
                return "uTorrent list FAILED: %s %s" % (type(err).__name__, str(err))

            # we would also like to check lazylibrarian.utorrent_label
            # but uTorrent only sends us a list of labels that have active torrents
            # so we can't tell if our label is known, or does it get created anyway?
            if lazylibrarian.CONFIG['UTORRENT_LABEL']:
                return "uTorrent login successful, label not checked"
            return "uTorrent login successful"
        return "uTorrent login FAILED\nCheck debug log"
    except Exception as err:
        return "uTorrent login FAILED: %s %s" % (type(err).__name__, str(err))


def labelTorrent(hashid):
    label = lazylibrarian.CONFIG['UTORRENT_LABEL']
    uTorrentClient = utorrentclient()
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            uTorrentClient.setprops(torrent[0], 'label', label)
            return True
    return False


def dirTorrent(hashid):
    uTorrentClient = utorrentclient()
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            return torrent[26]
    return False


def nameTorrent(hashid):
    uTorrentClient = utorrentclient()
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            return torrent[2]
    return ""


def progressTorrent(hashid):
    uTorrentClient = utorrentclient()
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            return check_int(torrent[4], 0) // 10, torrent[1], \
                             (torrent[1] & 65 == 0)  # status not started or queued
    return -1, '', False


def listTorrent(hashid):
    uTorrentClient = utorrentclient()
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            return uTorrentClient.getfiles(torrent[0])
    return []


def removeTorrent(hashid, remove_data=False):
    uTorrentClient = utorrentclient()
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            if remove_data:
                uTorrentClient.removedata(torrent[0])
            else:
                uTorrentClient.remove(torrent[0])
            return True
    return False


def addTorrent(link, hashid):
    uTorrentClient = utorrentclient()
    uTorrentClient.add_url(link)
    label = lazylibrarian.CONFIG['UTORRENT_LABEL']
    torrentList = uTorrentClient.list()
    for torrent in torrentList[1].get('torrents'):
        if torrent[0].lower() == hashid:
            if label:
                uTorrentClient.setprops(torrent[0], 'label', label)
            return hashid, ''
    return False, 'uTorrent failed to locate hashid'
