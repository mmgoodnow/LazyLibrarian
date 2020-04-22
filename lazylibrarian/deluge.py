#  This file is part of LazyLibrarian.
#  It is just used to talk JSON to the Deluge WebUI
#  A separate library lib.deluge_client is used to talk to the Deluge daemon
#  Lazylibrarian is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with LazyLibrarian.  If not, see <http://www.gnu.org/licenses/>.
# Parts of this file are a part of SickRage.
# Author: Mr_Orange <mr_orange@hotmail.it>
# URL: http://code.google.com/p/sickbeard/
# Adapted for Headphones by <noamgit@gmail.com>
# URL: https://github.com/noam09
# Adapted for LazyLibrarian by Phil Borman
# URL: https://gitlab.com/philborman
#

from __future__ import unicode_literals

import re
import traceback
from base64 import b64encode, b64decode

try:
    import urllib3
    import requests
except ImportError:
    import lib.requests as requests

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.formatter import check_int, makeUnicode
from lazylibrarian.common import make_dirs, path_isdir, syspath
from lib.six import PY2

delugeweb_auth = {}
delugeweb_url = ''
deluge_verify_cert = False
headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}


def addTorrent(link, data=None):
    try:
        retid = False
        result = {}
        if link and link.startswith('magnet:'):
            logger.debug('Deluge: Got a magnet link: %s' % link)
            result = {'type': 'magnet',
                      'url': link}
            retid = _add_torrent_magnet(result)

        elif link and link.startswith('http'):
            logger.debug('Deluge: Got a URL: %s' % link)
            result = {'type': 'url',
                      'url': link}
            retid = _add_torrent_url(result)
        elif link:
            torrentfile = ''
            if data:
                logger.debug('Deluge: Getting .torrent data')
                if b'announce' in data[:40]:
                    torrentfile = data
                else:
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                        logger.debug('Deluge: data doesn\'t look like a torrent, maybe b64encoded')
                    data = b64decode(data)
                    if b'announce' in data[:40]:
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                            logger.debug('Deluge: data looks like a b64encoded torrent')
                        torrentfile = data
                    else:
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                            logger.debug('Deluge: data doesn\'t look like a b64encoded torrent either')

            if not torrentfile:
                logger.debug('Deluge: Getting .torrent from file %s' % link)
                with open(syspath(link), 'rb') as f:
                    torrentfile = f.read()
            # Extract torrent name from .torrent
            try:
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                    logger.debug('Deluge: Getting torrent name length')
                name_length = int(re.findall(b'name([0-9]*):.*?:', torrentfile)[0])
                if name_length and lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                    logger.debug('Deluge: Getting torrent name')
                name = makeUnicode(re.findall(b'name[0-9]*:(.*?):', torrentfile)[0][:name_length])
            except (re.error, IndexError, TypeError):
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                    logger.debug('Deluge: Could not get torrent name, getting file name')
                # get last part of link/path (name only)
                name = link.split('\\')[-1].split('/')[-1]
                # remove '.torrent' suffix
                if name[-len('.torrent'):] == '.torrent':
                    name = name[:-len('.torrent')]
            try:
                logger.debug('Deluge: Sending Deluge torrent with name %s and content [%s...]' %
                             (name, torrentfile[:40]))
            except UnicodeDecodeError:
                logger.debug('Deluge: Sending Deluge torrent with name %s and content [%s...]' %
                             (name.decode('utf-8'), torrentfile[:40].decode('utf-8')))
            result = {'type': 'torrent',
                      'name': name,
                      'content': torrentfile}
            retid = _add_torrent_file(result)

        else:
            logger.error('Deluge: Unknown file type: %s' % link)

        if retid:
            logger.info('Deluge: Torrent sent to Deluge successfully  (%s)' % retid)
            if lazylibrarian.CONFIG['DELUGE_LABEL']:
                labelled = setTorrentLabel(result)
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                    logger.debug('Deluge label returned: %s' % labelled)
            return retid, ''
        else:
            res = 'Deluge returned status %s' % retid
            logger.error(res)
            return False, res

    except Exception as err:
        res = str(err)
        logger.error(res)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            formatted_lines = traceback.format_exc().splitlines()
            logger.debug('; '.join(formatted_lines))
        return False, res


def getTorrentName(torrentid):
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Get torrent name')
    res = getTorrentStatus(torrentid, ["name", "state"])  # type: dict
    if res and res['result']:
        return res['result']['name']
    return ''


def getTorrentFolder(torrentid):
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Get torrent folder')
    res = getTorrentStatus(torrentid, ["save_path", "state"])  # type: dict
    if res and res['result']:
        return res['result']['save_path']
    return ''


def getTorrentFiles(torrentid):
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Get torrent files')
    res = getTorrentStatus(torrentid, ["files", "state"])  # type: dict
    if res and res['result']:
        return res['result']['files']
    return ''


def getTorrentProgress(torrentid):
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Get torrent progress')
    res = getTorrentStatus(torrentid, ["progress", "message", "state", "is_auto_managed",
                                       "stop_at_ratio", "ratio", "stop_ratio"])  # type: dict
    if res and res['result']:
        info = res['result']  # type: dict
        if 'progress' in info:
            finished = info['is_auto_managed'] and info['stop_at_ratio'] and \
                info['state'].lower() == 'paused' and info['ratio'] >= info['stop_ratio']
            return info['progress'], info['message'], finished
        return 0, 'OK', False
    return -1, '', False


def getTorrentStatus(torrentid, data):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    try:
        tries = 2
        while tries:
            if not any(delugeweb_auth):
                _get_auth()

            timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)

            post_json = {"method": "web.get_torrent_status",
                         "params": [torrentid, data],
                         "id": 22}
            response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                     verify=deluge_verify_cert, headers=headers, timeout=timeout)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Status code: %s' % response.status_code)
                logger.debug(str(response.text))

            res = response.json()
            if res and res['error'] and res['error']['code'] == 1:  # not authenticated
                delugeweb_auth = {}  # force retry auth
                tries -= 1
            else:
                return res
    except Exception as err:
        logger.debug('Deluge %s: Could not get torrent info %s: %s' % (str(data), type(err).__name__, str(err)))
        return ''


def removeTorrent(torrentid, remove_data=False):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if not any(delugeweb_auth):
        _get_auth()

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)

    try:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Deluge: Removing torrent %s' % str(torrentid))
        post_json = {"method": "core.remove_torrent", "params": [torrentid, remove_data], "id": 25}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        result = response.json()['result']
        return result
    except Exception as err:
        logger.debug('Deluge: Could not delete torrent %s: %s' % (type(err).__name__, str(err)))
        return False


def _get_auth():
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Authenticating...')
    delugeweb_auth = {}
    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)

    delugeweb_cert = lazylibrarian.CONFIG['DELUGE_CERT']
    delugeweb_host = lazylibrarian.CONFIG['DELUGE_HOST']
    delugeweb_port = check_int(lazylibrarian.CONFIG['DELUGE_PORT'], 0)
    if not delugeweb_host or not delugeweb_port:
        logger.error('Invalid delugeweb host or port, check your config')
        return None

    delugeweb_password = lazylibrarian.CONFIG['DELUGE_PASS']
    if not delugeweb_host.startswith("http"):
        delugeweb_host = 'http://%s' % delugeweb_host

    delugeweb_host = "%s:%s" % (delugeweb_host.rstrip('/'), delugeweb_port)

    if lazylibrarian.CONFIG['DELUGE_BASE']:
        delugeweb_base = lazylibrarian.CONFIG['DELUGE_BASE'].strip('/')
        delugeweb_host = "%s/%s" % (delugeweb_host, delugeweb_base)

    if delugeweb_cert is None or delugeweb_cert.strip() == '':
        deluge_verify_cert = False
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Deluge: FYI no SSL certificate configured, host is %s' % delugeweb_host)
    else:
        deluge_verify_cert = delugeweb_cert
        delugeweb_host = delugeweb_host.replace('http:', 'https:')
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Deluge: Using certificate %s, host is now %s' % (deluge_verify_cert, delugeweb_host))

    delugeweb_url = delugeweb_host + '/json'
    post_json = {"method": "auth.login", "params": [delugeweb_password], "id": 1}

    try:
        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth, timeout=timeout,
                                 verify=deluge_verify_cert, headers=headers)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)
        if response.status_code == 200:
            force_https = False
        else:
            force_https = True
    except Exception as err:
        logger.error('Deluge %s: auth.login returned %s' % (type(err).__name__, str(err)))
        response = None
        force_https = True

    if force_https and not delugeweb_url.startswith('https:'):
        try:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Deluge: Connection failed, let\'s try HTTPS just in case')
            response = requests.post(delugeweb_url.replace('http:', 'https:'), json=post_json, timeout=timeout,
                                     cookies=delugeweb_auth, verify=deluge_verify_cert, headers=headers)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Status code: %s' % response.status_code)
                logger.debug(response.text)
            # If the response didn't fail, change delugeweb_url for the rest of this session
            if response.status_code == 200:
                logger.error('Deluge: Switching to HTTPS, certificate won\'t be verified NO CERTIFICATE WAS CONFIGURED')
                delugeweb_url = delugeweb_url.replace('http:', 'https:')
            else:
                logger.error('Deluge: HTTPS Authentication failed: %s' % response.text)
                return None
        except Exception as e:
            logger.error('Deluge: HTTPS Authentication failed: %s' % str(e))
            return None

    if not response:
        return None

    try:
        auth = response.json()["result"]
        auth_error = response.json()["error"]
    except Exception as err:
        logger.error("JSON error: %s" % str(err))
        logger.error("Response: %s" % response.text)
        return None

    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Authentication result: %s, Error: %s' % (auth, auth_error))
    delugeweb_auth = response.cookies
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Authentication cookies: %s' % str(delugeweb_auth.get_dict()))
    post_json = {"method": "web.connected", "params": [], "id": 10}

    try:
        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

    except Exception as err:
        logger.debug('Deluge %s: web.connected returned %s' % (type(err).__name__, str(err)))
        delugeweb_auth = {}
        return None

    connected = response.json()['result']
    connected_error = response.json()['error']
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Connection result: %s, Error: %s' % (connected, connected_error))

    if not connected:
        post_json = {"method": "web.get_hosts", "params": [], "id": 11}

        try:
            response = requests.post(delugeweb_url, json=post_json, verify=deluge_verify_cert,
                                     cookies=delugeweb_auth, headers=headers)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Status code: %s' % response.status_code)
                logger.debug(response.text)

        except Exception as err:
            logger.debug('Deluge %s: web.get_hosts returned %s' % (type(err).__name__, str(err)))
            delugeweb_auth = {}
            return None

        delugeweb_hosts = response.json()['result']

        # Check if delugeweb_hosts is None before checking its length
        if not delugeweb_hosts or len(delugeweb_hosts) == 0:
            logger.error('Deluge: %s' % response.text)
            delugeweb_auth = {}
            return None

        post_json = {"method": "web.connect", "params": [delugeweb_hosts[0][0]], "id": 11}

        try:
            response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                     verify=deluge_verify_cert, headers=headers)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Status code: %s' % response.status_code)
                logger.debug(response.text)

        except Exception as err:
            logger.debug('Deluge %s: web.connect returned %s' % (type(err).__name__, str(err)))
            delugeweb_auth = {}
            return None

        post_json = {"method": "web.connected", "params": [], "id": 10}

        try:
            response = requests.post(delugeweb_url, json=post_json, verify=deluge_verify_cert,
                                     cookies=delugeweb_auth, headers=headers)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Status code: %s' % response.status_code)
                logger.debug(response.text)

        except Exception as err:
            logger.debug('Deluge %s: web.connected returned %s' % (type(err).__name__, str(err)))
            delugeweb_auth = {}
            return None

        connected = response.json()['result']

        if not connected:
            logger.error('Deluge: WebUI could not connect to daemon')
            delugeweb_auth = {}
            return None

    return auth


def _add_torrent_magnet(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Adding magnet')
    if not any(delugeweb_auth):
        _get_auth()

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        post_json = {"method": "core.add_torrent_magnet", "params": [result['url'], {}], "id": 2}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        result['hash'] = response.json()['result']
        msg = 'Deluge: Response was %s' % result['hash']
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug(msg)
        if 'was None' in msg:
            logger.error('Deluge: Adding magnet failed: Is the WebUI running?')
        return response.json()['result']
    except Exception as err:
        logger.error('Deluge %s: Adding magnet failed: %s' % (type(err).__name__, str(err)))
        return False


def _add_torrent_url(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Adding URL')
    if not any(delugeweb_auth):
        _get_auth()

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        post_json = {"method": "core.add_torrent_url", "params": [result['url'], {}], "id": 32}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)

        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        result['hash'] = response.json()['result']
        msg = 'Deluge: Response was %s' % result['hash']
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug(msg)
        if 'was None' in msg:
            logger.error('Deluge: Adding torrent URL failed: Is the WebUI running?')
        return response.json()['result']
    except Exception as err:
        logger.error('Deluge %s: Adding torrent URL failed: %s' % (type(err).__name__, str(err)))
        return False


def _add_torrent_file(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Adding file')
    if not any(delugeweb_auth):
        _get_auth()

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        # content is torrent file contents (bytes) that needs to be encoded to base64
        # b64encode input/output is bytes, and python3 json serialiser doesnt like bytes
        content = b64encode(result['content'])
        if not PY2:
            content = makeUnicode(content)
        post_json = {"method": "core.add_torrent_file",
                     "params": [result['name'] + '.torrent', content, {}],
                     "id": 2}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)

        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        result['hash'] = response.json()['result']
        msg = 'Deluge: Response was %s' % result['hash']
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug(msg)
        if 'was None' in msg:
            logger.error('Deluge: Adding torrent file failed: Is the WebUI running?')
        return response.json()['result']
    except Exception as err:
        logger.error('Deluge %s: Adding torrent file failed: %s' % (type(err).__name__, str(err)))
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            formatted_lines = traceback.format_exc().splitlines()
            logger.debug('; '.join(formatted_lines))
        return False


def setTorrentLabel(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Setting label')
    label = lazylibrarian.CONFIG['DELUGE_LABEL']
    if not any(delugeweb_auth):
        _get_auth()

    if not label:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Deluge: No Label set')
        return True

    if ' ' in label:
        logger.error('Deluge: Invalid label. Label can\'t contain spaces - replacing with underscores')
        label = label.replace(' ', '_')

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        # check if label already exists and create it if not
        post_json = {"method": 'label.get_labels', "params": [], "id": 3}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        labels = response.json()['result']

        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug("Valid labels: %s" % str(labels))

        if response.json()['error'] is None:
            label = label.lower()  # deluge lowercases labels
            if label not in labels:
                try:
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                        logger.debug('Deluge: %s label doesn\'t exist in Deluge, let\'s add it' % label)
                    post_json = {"method": 'label.add', "params": [label], "id": 4}
                    response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                             verify=deluge_verify_cert, headers=headers, timeout=timeout)
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                        logger.debug('Status code: %s' % response.status_code)
                        logger.debug(response.text)
                    logger.debug('Deluge: %s label added to Deluge' % label)

                except Exception as err:
                    logger.error('Deluge %s: Setting label failed: %s' % (type(err).__name__, str(err)))
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                        formatted_lines = traceback.format_exc().splitlines()
                        logger.debug('; '.join(formatted_lines))
                    if not result:
                        return False
            else:
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                    logger.debug("Label [%s] is valid" % label)

            if not result:
                return True

            # add label to torrent
            post_json = {"method": 'label.set_torrent', "params": [result['hash'], label], "id": 5}

            response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                     verify=deluge_verify_cert, headers=headers, timeout=timeout)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Status code: %s' % response.status_code)
                logger.debug(response.text)
            logger.debug('Deluge: %s label added to torrent' % label)
            return not response.json()['error']
        else:
            logger.debug('Deluge: Label plugin not detected')
            return False
    except Exception as err:
        logger.error('Deluge %s: Adding label failed: %s' % (type(err).__name__, str(err)))
        return False


def setSeedRatio(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Setting seed ratio')
    if not any(delugeweb_auth):
        _get_auth()

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        ratio = None
        if result['ratio']:
            ratio = result['ratio']

        if not ratio:
            return True

        post_json = {"method": "core.set_torrent_stop_at_ratio", "params": [result['hash'], True], "id": 5}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        post_json = {"method": "core.set_torrent_stop_ratio", "params": [result['hash'], float(ratio)], "id": 6}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        return not response.json()['error']
    except Exception as err:
        logger.error('Deluge %s: Setting seedratio failed: %s' % (type(err).__name__, str(err)))
        return False


def setTorrentPath(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Setting download path')
    if not any(delugeweb_auth):
        _get_auth()

    dl_dir = lazylibrarian.CONFIG['DELUGE_DIR']

    if not dl_dir:
        return True

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        post_json = {"method": "core.set_torrent_move_completed", "params": [result['hash'], True], "id": 7}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        if not path_isdir(dl_dir):
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug('Deluge: %s directory doesn\'t exist, let\'s create it' % dl_dir)
            _ = make_dirs(dl_dir)

        post_json = {"method": "core.set_torrent_move_completed_path", "params": [result['hash'], dl_dir], "id": 8}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        return not response.json()['error']
    except Exception as err:
        logger.error('Deluge %s: setTorrentPath failed: %s' % (type(err).__name__, str(err)))
        return False


def setTorrentPause(result):
    global delugeweb_auth, delugeweb_url, headers, deluge_verify_cert
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Pausing torrent')
    if not any(delugeweb_auth):
        _get_auth()

    timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
    try:
        post_json = {"method": "core.pause_torrent", "params": [[result['hash']]], "id": 9}

        response = requests.post(delugeweb_url, json=post_json, cookies=delugeweb_auth,
                                 verify=deluge_verify_cert, headers=headers, timeout=timeout)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug('Status code: %s' % response.status_code)
            logger.debug(response.text)

        return not response.json()['error']
    except Exception as err:
        logger.error('Deluge %s: setTorrentPause failed: %s' % (type(err).__name__, str(err)))
        return False


def checkLink():
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Deluge: Checking connection')
    msg = "Deluge: Connection successful"
    auth = _get_auth()
    if auth:
        res = setTorrentLabel('')
        if res:
            msg += '\nLabel is ok'
        else:
            msg += '\nUnable to set label'
    else:
        msg = "Deluge: Connection FAILED\nCheck debug log"
    logger.debug(msg)
    return msg
