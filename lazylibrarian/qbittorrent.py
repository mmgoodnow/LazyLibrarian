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


import json
import logging
import mimetypes
import os
import random
import string
import time
from http.cookiejar import CookieJar
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, build_opener, Request

from lazylibrarian.common import get_user_agent
from lazylibrarian.config2 import CONFIG
from lazylibrarian.formatter import get_list, make_bytestr, make_unicode


class QbittorrentClient(object):
    # TOKEN_REGEX = "<div id='token' style='display:none;'>([^<>]+)</div>"
    # UTSetting = namedtuple("UTSetting", ["name", "int", "str", "access"])

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.loggerdlcomms = logging.getLogger('special.dlcomms')

        host = CONFIG['QBITTORRENT_HOST']
        port = CONFIG.get_int('QBITTORRENT_PORT')
        if not host or not port:
            self.logger.error('Invalid Qbittorrent host or port, check your config')

        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"http://{host}"

        host = host.rstrip('/')

        if host.endswith('/gui'):
            host = host[:-4]

        if CONFIG['QBITTORRENT_BASE']:
            host = f"{host}:{port}/{CONFIG['QBITTORRENT_BASE'].strip('/')}"
        else:
            host = f"{host}:{port}"
        self.base_url = host
        self.username = CONFIG['QBITTORRENT_USER']
        self.password = CONFIG['QBITTORRENT_PASS']
        self.cookiejar = CookieJar()
        self.opener = self._make_opener()
        self.cmdset = 0
        self._get_sid(self.base_url, self.username, self.password)
        self.api = self._api_version()
        self.hashid = ''

    def _make_opener(self):
        # create opener with cookie handler to carry QBitTorrent SID cookie
        cookie_handler = HTTPCookieProcessor(self.cookiejar)
        handlers = [cookie_handler]
        return build_opener(*handlers)

    def _api_version(self):
        # noinspection PyBroadException
        try:
            if self.cmdset == 2:
                version = self._command('app/webapiVersion')
            else:
                version = int(self._command('version/api'))
        except Exception as err:
            self.logger.warning(f'Error getting api version. qBittorrent {type(err).__name__}: {str(err)}')
            version = 1
        return version

    def _get_sid(self, base_url, username, password):
        # login so we can capture SID cookie
        login_data = make_bytestr(urlencode({'username': username, 'password': password}))
        self.loggerdlcomms.debug(f"Trying {base_url}/login")
        try:
            _ = self.opener.open(f"{base_url}/login", login_data)
            self.cmdset = 1
        except Exception as err:
            self.loggerdlcomms.debug(f'Error getting v1 SID. qBittorrent {type(err).__name__}: {str(err)}')
            self.loggerdlcomms.debug(f"Trying {base_url}/api/v2/auth/login")
            try:
                _ = self.opener.open(f"{base_url}/api/v2/auth/login", login_data)
                self.cmdset = 2
            except Exception as err:
                self.loggerdlcomms.debug(f'Error getting v2 SID. qBittorrent {type(err).__name__}: {str(err)}')

        if not self.cmdset:
            self.logger.warning(f'Unable to log in to {base_url}')
            return

        for cookie in self.cookiejar:
            self.loggerdlcomms.debug(f"login cookie: {cookie.name}, value: {cookie.value}")
        return

    def _command(self, command, args=None, content_type=None, files=None):
        self.loggerdlcomms.debug(f'QBittorrent WebAPI Command: {command}')
        if self.cmdset == 2:
            url = f"{self.base_url}/api/v2/{command}"
        else:
            url = f"{self.base_url}/{command}"
        data = None
        headers = dict()
        self.loggerdlcomms.debug(f'QBittorrent URL: {url}')

        if files or content_type == 'multipart/form-data':
            data, headers = encode_multipart(args, files, '-------------------------acebdf13572468')
        else:
            if args:
                data = make_bytestr(urlencode(args))
            if content_type:
                headers['Content-Type'] = content_type

        request = Request(url, data, headers)

        if CONFIG['PROXY_HOST']:
            for item in get_list(CONFIG['PROXY_TYPE']):
                request.set_proxy(CONFIG['PROXY_HOST'], item)
        request.add_header('User-Agent', get_user_agent())

        try:
            response = self.opener.open(request)
            try:
                content_type = response.headers['content-type']
            except KeyError:
                content_type = ''
            self.loggerdlcomms.debug(f"QBitTorrent content type [{content_type}]")

            resp = response.read()
            # some commands return json
            if content_type == 'application/json':
                if resp:
                    return json.loads(resp)
                return ''
            else:
                # some commands return plain text
                resp = make_unicode(resp)
                self.loggerdlcomms.debug(f"QBitTorrent returned {resp}")
                if command in ['version/api', 'app/webapiVersion']:
                    return resp
                # some just return Ok. or Fails.
                if resp and resp != 'Ok.':
                    self.loggerdlcomms.debug("QBitTorrent returned False")
                    return False
            # some commands return nothing but response code (always 200)
            self.loggerdlcomms.debug("QBitTorrent returned True")
            return True
        except URLError as err:
            self.logger.debug(f'Failed URL: {url}')
            self.logger.debug(f'QBitTorrent webUI raised the following error: {err.reason}')
            return False

    def _get_list(self):
        """
        :rtype: dict
        """
        if self.hashid:
            args = {'hashes': self.hashid.lower()}
        else:
            args = None
        if self.cmdset == 2:
            value = self._command('torrents/info', args)
        else:
            value = self._command('query/torrents', args)
        self.loggerdlcomms.debug(f'get_list() returned {str(value)}')
        return value

    def _get_settings(self):
        if self.cmdset == 2:
            value = self._command('app/preferences')
        else:
            value = self._command('query/preferences')
        self.loggerdlcomms.debug(f'get_settings() returned {len(value)} items')
        return value

    def get_savepath(self, hashid):
        self.loggerdlcomms.debug(f'qb.get_savepath({hashid})')
        hashid = hashid.lower()
        self.hashid = hashid
        torrent_list = self._get_list()
        for torrent in list(torrent_list):
            if torrent['hash'] and torrent['hash'].lower() == hashid:
                return torrent['save_path']
        return None

    def start(self, hashid):
        self.loggerdlcomms.debug(f'qb.start({hashid})')
        args = {'hash': hashid}
        if self.cmdset == 2:
            return self._command('torrents/resume', args, 'application/x-www-form-urlencoded')
        else:
            return self._command('command/resume', args, 'application/x-www-form-urlencoded')

    def pause(self, hashid):
        self.loggerdlcomms.debug(f'qb.pause({hashid})')
        args = {'hash': hashid}
        if self.cmdset == 2:
            return self._command('torrents/pause', args, 'application/x-www-form-urlencoded')
        else:
            return self._command('command/pause', args, 'application/x-www-form-urlencoded')

    def getfiles(self, hashid):
        self.loggerdlcomms.debug(f'qb.getfiles({hashid})')
        if self.cmdset == 2:
            return self._command(f"torrents/files?hash={hashid}")
        else:
            return self._command(f"query/propertiesFiles/{hashid}")

    def getprops(self, hashid):
        self.loggerdlcomms.debug(f'qb.getprops({hashid})')
        if self.cmdset == 2:
            return self._command(f"torrents/properties?hash={hashid}")
        else:
            return self._command(f"query/propertiesGeneral/{hashid}")

    def remove(self, hashid, remove_data=False):
        self.loggerdlcomms.debug(f'qb.remove({hashid},{remove_data})')
        args = {'hashes': hashid}
        if self.cmdset == 2:
            command = 'torrents/delete'
            if remove_data:
                args['deleteFiles'] = 'true'
        else:
            if remove_data:
                command = 'command/deletePerm'
            else:
                command = 'command/delete'
        return self._command(command, args, 'application/x-www-form-urlencoded')


def get_progress(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug(f'get_progress({hashid})')
    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        loggerdlcomms.debug("Failed to login to qBittorrent")
        return -1, '', False
    # noinspection PyProtectedMember
    preferences = qbclient._get_settings()
    loggerdlcomms.debug(str(preferences))
    max_ratio = 0.0
    if 'max_ratio_enabled' in preferences and 'max_ratio' in preferences:
        # noinspection PyTypeChecker
        if preferences['max_ratio_enabled']:
            # noinspection PyTypeChecker
            max_ratio = float(preferences['max_ratio'])
    qbclient.hashid = hashid
    # noinspection PyProtectedMember
    torrent_list = qbclient._get_list()
    if torrent_list:
        for torrent in torrent_list:
            if torrent['hash'].lower() == hashid:
                loggerdlcomms.debug(str(torrent))
                if 'state' in torrent:
                    state = torrent['state']
                else:
                    state = ''
                if 'ratio' in torrent:
                    ratio = float(torrent['ratio'])
                else:
                    ratio = 0.0
                if 'progress' in torrent:
                    try:
                        progress = int(100 * float(torrent['progress']))
                    except ValueError:
                        progress = 0
                else:
                    progress = 0
                finished = False
                if max_ratio and max_ratio <= ratio and state == 'pausedUP':
                    finished = True
                return progress, state, finished
    return -1, '', False


def remove_torrent(hashid, remove_data=False):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug(f'remove_torrent({hashid},{remove_data})')
    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        logger.debug("Failed to login to qBittorrent")
        return False
    qbclient.hashid = hashid
    # noinspection PyProtectedMember
    torrent_list = qbclient._get_list()
    if torrent_list:
        for torrent in torrent_list:
            if torrent['hash'].lower() == hashid:
                remove = True
                if torrent['state'] == 'uploading' or torrent['state'] == 'stalledUP':
                    if not CONFIG.get_bool('SEED_WAIT'):
                        logger.debug(f"{torrent['name']} is seeding, removing torrent and data anyway")
                    else:
                        logger.info(f"{torrent['name']} has not finished seeding yet, torrent will not be removed")
                        remove = False
                if remove:
                    if remove_data:
                        logger.info(f"{torrent['name']} removing torrent and data")
                    else:
                        logger.info(f"{torrent['name']} removing torrent")
                    qbclient.remove(hashid, remove_data)
                    return True
    return False


def check_link():
    """ Check we can talk to qbittorrent"""
    try:
        qbclient = QbittorrentClient()
        if qbclient.cmdset:
            # qbittorrent creates a new label if needed
            # can't see how to get a list of known labels to check against
            return f"qBittorrent login successful, api: {qbclient.api}"
        return "qBittorrent login FAILED\nCheck debug log"
    except Exception as err:
        return f"qBittorrent login FAILED: {type(err).__name__} {str(err)}"


def get_args(qbclient, provider_options):
    """ Get optional arguments based on configuration"""
    args = {'paused': 'true' if CONFIG.get_bool('TORRENT_PAUSED') else 'false'}
    dl_dir = CONFIG['QBITTORRENT_DIR']
    if dl_dir:
        args['savepath'] = dl_dir

    if CONFIG['QBITTORRENT_LABEL']:
        if qbclient.cmdset == 2:
            args['category'] = CONFIG['QBITTORRENT_LABEL']
        else:
            if 6 < qbclient.api < 10:
                args['label'] = CONFIG['QBITTORRENT_LABEL']
            elif qbclient.api >= 10:
                args['category'] = CONFIG['QBITTORRENT_LABEL']

    if "seed_ratio" in provider_options:
        args['ratioLimit'] = provider_options["seed_ratio"]
    if "seed_duration" in provider_options:
        args['seedingTimeLimit'] = provider_options["seed_duration"]

    return args


def add_torrent(link, hashid, provider_options):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'add_torrent({link})')

    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        res = "Failed to login to qBittorrent"
        logger.debug(res)
        return False, res
    args = get_args(qbclient, provider_options)
    loggerdlcomms.debug(f'add_torrent args({args})')
    args['urls'] = link

    if qbclient.cmdset == 2:
        # noinspection PyProtectedMember
        res = qbclient._command('torrents/add', args, 'multipart/form-data')
    else:
        # noinspection PyProtectedMember
        res = qbclient._command('command/download', args, 'multipart/form-data')
    # sometimes returns "Fails." when it hasn't failed
    # sometimes returns "True" when it hasn't added the torrent
    # empty request or unresolved magnet?
    # so look if hashid was added correctly
    if not res:
        logger.debug("add_torrent thinks it failed")

    qbclient.hashid = hashid
    count = 0
    while count < 10:
        count += 1
        time.sleep(1)
        # noinspection PyProtectedMember
        torrents = qbclient._get_list()
        if torrents:
            for item in torrents:
                if item.get('hash') == hashid:
                    if count > 1:
                        loggerdlcomms.debug(f"hashid found in torrent list after {count} seconds")
                    return True, ''
    res = "hashid not found in torrent list, add_torrent failed"
    loggerdlcomms.debug(res)
    return False, res


def add_file(data, hashid, title, provider_options):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug('add_file(data)')
    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        res = "Failed to login to qBittorrent"
        logger.debug(res)
        return False, res
    args = get_args(qbclient, provider_options)
    loggerdlcomms.debug(f'add_torrent args({args})')
    files = {'torrents': {'filename': title, 'content': data}}
    if qbclient.cmdset == 2:
        # noinspection PyProtectedMember
        res = qbclient._command('torrents/add', args, files=files)
    else:
        # noinspection PyProtectedMember
        res = qbclient._command('command/upload', args, files=files)
    if not res:
        # sometimes returns "Fails." when it hasn't failed, so look if hashid was added correctly
        logger.debug("add_file thinks it failed")

    qbclient.hashid = hashid
    count = 0
    while count < 10:
        count += 1
        time.sleep(1)
        # noinspection PyProtectedMember
        torrents = qbclient._get_list()
        if torrents:
            for item in torrents:
                if item.get('hash') == hashid:
                    if count > 1:
                        loggerdlcomms.debug(f"hashid found in torrent list after {count} seconds")
                    if qbclient.cmdset == 2 and CONFIG['QBITTORRENT_LABEL']:
                        args = {'hash': hashid, 'category': CONFIG['QBITTORRENT_LABEL']}
                        # noinspection PyProtectedMember
                        qbclient._command('torrents/setCategory', args, 'application/x-www-form-urlencoded')
                    return True, ''

    res = "hashid not found in torrent list, add_file failed"
    loggerdlcomms.debug(res)
    return False, res


def get_name(hashid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'get_name({hashid})')
    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        logger.debug("Failed to login to qBittorrent")
        return ''
    retries = 5
    torrents = []
    qbclient.hashid = hashid
    while retries:
        # noinspection PyProtectedMember
        torrents = qbclient._get_list()
        if torrents:
            if hashid in str(torrents).lower():
                break
        time.sleep(2)
        retries -= 1

    for tor in torrents:
        if tor['hash'].lower() == hashid:
            return tor['name']
    return ''


def get_files(hashid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'get_files({hashid})')
    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        logger.debug("Failed to login to qBittorrent")
        return ''
    retries = 5

    while retries:
        # noinspection PyProtectedMember
        files = qbclient.getfiles(hashid)
        if files:
            return files
        time.sleep(2)
        retries -= 1
    return ''


def get_folder(hashid):
    logger = logging.getLogger(__name__)
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug(f'get_folder({hashid})')
    hashid = hashid.lower()
    qbclient = QbittorrentClient()
    if not qbclient.cmdset:
        logger.debug("Failed to login to qBittorrent")
        return None

    # Get Active Directory from settings
    # noinspection PyProtectedMember
    settings = qbclient._get_settings()
    # noinspection PyTypeChecker
    active_dir = settings['temp_path']
    # completed_dir = settings['save_path']

    if not active_dir:
        logger.error(
            'Could not get "Keep incomplete torrents in:" directory from QBitTorrent settings, please ensure it is set')
        return None

    # Get Torrent Folder Name
    torrent_folder = qbclient.get_savepath(hashid)

    # If there's no folder yet then it's probably a magnet, try until folder is populated
    if torrent_folder == active_dir or not torrent_folder:
        tries = 1
        while (torrent_folder == active_dir or torrent_folder is None) and tries <= 10:
            tries += 1
            time.sleep(6)
            torrent_folder = qbclient.get_savepath(hashid)

    if torrent_folder == active_dir or not torrent_folder:
        torrent_folder = qbclient.get_savepath(hashid)
        return torrent_folder
    else:
        if os.name != 'nt':
            torrent_folder = torrent_folder.replace('\\', '/')
        return os.path.basename(os.path.normpath(torrent_folder))


_BOUNDARY_CHARS = string.digits + string.ascii_letters


def encode_multipart(fields, files, boundary=None):
    """Encode dict of form fields and dict of files as multipart/form-data.
    Return tuple of (body_string, headers_dict). Each value in files is a dict
    with required keys 'filename' and 'content', and optional 'mimetype' (if
    not specified, tries to guess mime type or uses 'application/octet-stream').
    """

    def escape_quote(s):
        s = make_unicode(s)
        return s.replace('"', '\\"')

    if boundary is None:
        boundary = ''.join(random.choice(_BOUNDARY_CHARS) for _ in range(30))
    lines = []

    if fields:
        fields = dict((make_bytestr(k), make_bytestr(v)) for k, v in fields.items())
        for name, value in list(fields.items()):
            lines.extend((
                f'--{boundary}',
                f'Content-Disposition: form-data; name="{escape_quote(name)}"',
                '',
                make_unicode(value),
            ))

    if files:
        for name, value in list(files.items()):
            filename = value['filename']
            if 'mimetype' in value:
                mimetype = value['mimetype']
            else:
                mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
            lines.extend((
                f'--{boundary}',
                f'Content-Disposition: form-data; name="{escape_quote(name)}"; filename="{escape_quote(filename)}"',
                f'Content-Type: {mimetype}',
                '',
                value['content'],
            ))

    lines.extend((
        f'--{boundary}--',
        '',
    ))

    body = b'\r\n'.join([make_bytestr(ln) for ln in lines])

    headers = {
        'Content-Type': f'multipart/form-data; boundary={boundary}',
        'Content-Length': str(len(body)),
    }

    return body, headers
