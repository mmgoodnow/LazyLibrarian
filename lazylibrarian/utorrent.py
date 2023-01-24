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
import logging
import re
import time

from http.cookiejar import CookieJar
from urllib.parse import urljoin, urlencode
from urllib.request import HTTPCookieProcessor, HTTPBasicAuthHandler, \
    build_opener, install_opener, Request

from lazylibrarian.config2 import CONFIG
from lazylibrarian.common import get_user_agent
from lazylibrarian.formatter import check_int, get_list


class UtorrentClient(object):
    TOKEN_REGEX = b"<div id='token' style='display:none;'>([^<>]+)</div>"

    # noinspection PyUnusedLocal
    def __init__(self, base_url='',  # lazylibrarian.CONFIG['UTORRENT_HOST'],
                 username='',  # lazylibrarian.CONFIG['UTORRENT_USER'],
                 password='',):  # lazylibrarian.CONFIG['UTORRENT_PASS']):
        self.logger = logging.getLogger(__name__)
        self.loggerdlcomms = logging.getLogger('special.dlcomms')

        host = CONFIG['UTORRENT_HOST']
        port = CONFIG.get_int('UTORRENT_PORT')
        if not host or not port:
            self.logger.error('Invalid Utorrent host or port, check your config')

        if not host.startswith("http://") and not host.startswith("https://"):
            host = 'http://' + host

        host = host.rstrip('/')

        if host.endswith('/gui'):
            host = host[:-4]

        if CONFIG['UTORRENT_BASE']:
            host = "%s:%s/%s" % (host, port, CONFIG['UTORRENT_BASE'].strip('/'))
        else:
            host = "%s:%s" % (host, port)

        self.base_url = host
        self.username = CONFIG['UTORRENT_USER']
        self.password = CONFIG['UTORRENT_PASS']
        self.opener = self._make_opener('uTorrent', self.base_url, self.username, self.password)
        self.token = self._get_token()
        if self.token is not None:
            self.token = self.token.decode('utf-8')
        # TODO refresh token, when necessary

    @staticmethod
    def _make_opener(realm, base_url, username, password):
        """uTorrent API need HTTP Basic Auth and cookie support for token verify."""
        auth = HTTPBasicAuthHandler()
        auth.add_password(realm=realm, uri=base_url, user=username, passwd=password)
        opener = build_opener(auth)
        install_opener(opener)

        cookie_jar = CookieJar()
        cookie_handler = HTTPCookieProcessor(cookie_jar)

        handlers = [auth, cookie_handler]
        opener = build_opener(*handlers)
        return opener

    def _get_token(self):
        url = urljoin(self.base_url, 'gui/token.html')
        try:
            response = self.opener.open(url)
        except Exception as err:
            self.logger.error('%s getting Token. uTorrent responded with: %s' % (type(err).__name__, str(err)))
            self.loggerdlcomms.debug('URL: %s' % url)
            return None
        match = re.search(UtorrentClient.TOKEN_REGEX, response.read())
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
        # noinspection PyUnresolvedReferences
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
        self.loggerdlcomms.debug("uTorrent params %s" % str(params))
        request = Request(url)
        if CONFIG['PROXY_HOST']:
            for item in get_list(CONFIG['PROXY_TYPE']):
                request.set_proxy(CONFIG['PROXY_HOST'], item)
        request.add_header('User-Agent', get_user_agent())

        if body:
            request.data(body)
            request.add_header('Content-length', str(len(body)))
        if content_type:
            request.add_header('Content-type', content_type)

        try:
            response = self.opener.open(request)
            res = response.code
            js = json.loads(response.read())
            self.loggerdlcomms.debug("uTorrent response code %s" % res)
            self.loggerdlcomms.debug(str(js))
            return res, js
        except Exception as err:
            self.logger.debug('URL: %s' % url)
            self.logger.debug('uTorrent webUI raised the following error: ' + str(err))
            return 0, str(err)


def check_link():
    """ Check we can talk to utorrent"""
    try:
        client = UtorrentClient()
        if client.token is not None:
            try:
                _ = client.list()
            except Exception as err:
                return "uTorrent list FAILED: %s %s" % (type(err).__name__, str(err))

            # we would also like to check lazylibrarian.utorrent_label
            # but uTorrent only sends us a list of labels that have active torrents
            # so we can't tell if our label is known, or does it get created anyway?
            if CONFIG['UTORRENT_LABEL']:
                return "uTorrent login successful, label not checked"
            return "uTorrent login successful"
        return "uTorrent login FAILED\nCheck debug log"
    except Exception as err:
        return "uTorrent login FAILED: %s %s" % (type(err).__name__, str(err))


# noinspection PyUnresolvedReferences
def label_torrent(hashid, label):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("set label %s for %s" % (label, hashid))
    uclient = UtorrentClient()
    torrent_list = uclient.list()
    for torrent in torrent_list[1].get('torrents'):
        if torrent[0].lower() == hashid.lower():
            uclient.setprops(torrent[0], 'label', label)
            return True
    return False


def dir_torrent(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("get directory for %s" % hashid)
    uclient = UtorrentClient()
    torrentlist = uclient.list()
    # noinspection PyUnresolvedReferences
    for torrent in torrentlist[1].get('torrents'):
        if torrent[0].lower() == hashid.lower():
            return torrent[26]
    return False


def name_torrent(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("get name for %s" % hashid)
    uclient = UtorrentClient()
    torrentlist = uclient.list()
    # noinspection PyUnresolvedReferences
    for torrent in torrentlist[1].get('torrents'):
        if torrent[0].lower() == hashid.lower():
            return torrent[2]
    return ""


def pause_torrent(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("pause %s" % hashid)
    uclient = UtorrentClient()
    return uclient.pause(hashid)


def progress_torrent(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("get progress for %s" % hashid)
    uclient = UtorrentClient()
    torrentlist = uclient.list()
    # noinspection PyUnresolvedReferences
    for torrent in torrentlist[1].get('torrents'):
        if torrent[0].lower() == hashid.lower():
            return check_int(torrent[4], 0) // 10, torrent[1], \
                             (torrent[1] & 65 == 0)  # status not started or queued
    return -1, '', False


def list_torrent(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("get file list for %s" % hashid)
    uclient = UtorrentClient()
    torrentlist = uclient.list()
    # noinspection PyUnresolvedReferences
    for torrent in torrentlist[1].get('torrents'):
        if torrent[0].lower() == hashid.lower():
            return uclient.getfiles(torrent[0])
    return []


def remove_torrent(hashid, remove_data=False):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("remove torrent %s remove_data=%s" % (hashid, remove_data))
    uclient = UtorrentClient()
    torrentlist = uclient.list()
    # noinspection PyUnresolvedReferences
    for torrent in torrentlist[1].get('torrents'):
        if torrent[0].lower() == hashid.lower():
            if remove_data:
                uclient.removedata(torrent[0])
            else:
                uclient.remove(torrent[0])
            return True
    return False


def add_torrent(link, hashid):
    uclient = UtorrentClient()
    uclient.add_url(link)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug("Add hashid %s" % hashid)
    count = 10
    while count:
        torrentlist = uclient.list()
        # noinspection PyUnresolvedReferences
        for torrent in torrentlist[1].get('torrents'):
            if torrent[0].lower() == hashid.lower():
                return hashid, ''
        time.sleep(1)
        count -= 1
    return False, 'uTorrent failed to locate hashid'
