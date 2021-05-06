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


import socket
import ssl
from time import sleep

import lazylibrarian
from lazylibrarian import logger
# noinspection PyUnresolvedReferences
from six.moves import xmlrpc_client


def get_server():
    host = lazylibrarian.CONFIG['RTORRENT_HOST']
    if not host:
        logger.error("rtorrent error: No host found, check your config")
        return False, ''

    host = host.rstrip('/')
    if not host.startswith("http://") and not host.startswith("https://"):
        host = 'http://' + host

    if lazylibrarian.CONFIG['RTORRENT_USER']:
        user = lazylibrarian.CONFIG['RTORRENT_USER']
        password = lazylibrarian.CONFIG['RTORRENT_PASS']
        parts = host.split('://')
        host = parts[0] + '://' + user + ':' + password + '@' + parts[1]

    try:
        socket.setdefaulttimeout(20)  # so we don't freeze if server is not there
        if host.startswith("https://"):
            context = ssl.create_default_context()
            if not lazylibrarian.CONFIG['SSL_VERIFY']:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            server = xmlrpc_client.ServerProxy(host, context=context)
        else:
            server = xmlrpc_client.ServerProxy(host)
        version = server.system.client_version()
        socket.setdefaulttimeout(None)  # reset timeout
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug("rTorrent client version = %s" % version)
    except Exception as e:
        socket.setdefaulttimeout(None)  # reset timeout if failed
        logger.error("xmlrpc_client error: %s" % repr(e))
        return False, ''
    if version:
        return server, version
    else:
        logger.warn('No response from rTorrent server')
        return False, ''


def add_torrent(tor_url, hash_id, data=None):
    server, version = get_server()
    if server is False:
        return False, 'rTorrent unable to connect to server'
    try:
        if data:
            logger.debug('Sending rTorrent content [%s...]' % str(data)[:40])
            if version.startswith('0.9') or version.startswith('1.'):
                _ = server.load.raw('', xmlrpc_client.Binary(data))
            else:
                _ = server.load_raw(xmlrpc_client.Binary(data))
        else:
            logger.debug('Sending rTorrent url [%s...]' % str(tor_url)[:40])
            if version.startswith('0.9') or version.startswith('1.'):
                _ = server.load.normal('', tor_url)  # response isn't anything useful, always 0
            else:
                _ = server.load(tor_url)
        # need a short pause while rtorrent loads it
        retries = 5
        while retries:
            mainview = server.download_list("", "main")
            for tor in mainview:
                if tor.upper() == hash_id.upper():
                    break
            sleep(1)
            retries -= 1

        label = lazylibrarian.CONFIG['RTORRENT_LABEL']
        if label:
            if version.startswith('0.9') or version.startswith('1.'):
                server.d.custom1.set(hash_id, label)
            else:
                server.d.set_custom1(hash_id, label)

        directory = lazylibrarian.CONFIG['RTORRENT_DIR']
        if directory:
            if version.startswith('0.9') or version.startswith('1.'):
                server.d.directory.set(hash_id, directory)
            else:
                server.d.set_directory(hash_id, directory)

        server.d.start(hash_id)

    except Exception as e:
        res = "rTorrent Error: %s: %s" % (type(e).__name__, str(e))
        logger.error(res)
        return False, res

    # wait a while for download to start, that's when rtorrent fills in the name
    name = get_name(hash_id)
    if name:
        if version.startswith('0.9') or version.startswith('1.'):
            directory = server.d.directory(hash_id)
            label = server.d.custom1(hash_id)
        else:
            directory = server.d.get_directory(hash_id)
            label = server.d.get_custom1(hash_id)

        if label:
            logger.debug('rTorrent downloading %s to %s with label %s' % (name, directory, label))
        else:
            logger.debug('rTorrent downloading %s to %s' % (name, directory))
        return hash_id, ''
    return False, 'rTorrent hashid not found'


def get_progress(hash_id):
    server, _ = get_server()
    if server is False:
        return 0, 'error'
    mainview = server.download_list("", "main")
    for tor in mainview:
        if tor.upper() == hash_id.upper():
            if server.d.complete(tor):
                return 100, 'finished'
            return int((server.d.bytes_done(tor) * 100) / server.d.size_bytes(tor)), 'OK'
    return -1, ''


def get_files(hash_id):
    server, _ = get_server()
    if server is False:
        return []

    mainview = server.download_list("", "main")
    for tor in mainview:
        if tor.upper() == hash_id.upper():
            size_files = server.d.size_files(tor)
            cnt = 0
            files = []
            while cnt < size_files:
                target = "%s:f%d" % (tor, cnt)
                path = server.f.path(target)
                size = server.f.size_bytes(target)
                files.append({"path": path, "size": size})
                cnt += 1
            return files
    return []


def get_name(hash_id):
    server, version = get_server()
    if server is False:
        return False

    mainview = server.download_list("", "main")
    for tor in mainview:
        if tor.upper() == hash_id.upper():
            retries = 5
            name = ''
            while retries:
                if version.startswith('0.9') or version.startswith('1.'):
                    name = server.d.name(tor)
                else:
                    name = server.d.get_name(tor)
                if tor.upper() not in name:
                    break
                sleep(5)
                retries -= 1
            return name
    return False  # not found


def get_folder(hash_id):
    server, version = get_server()
    if server is False:
        return False

    mainview = server.download_list("", "main")
    for tor in mainview:
        if tor.upper() == hash_id.upper():
            retries = 5
            name = ''
            while retries:
                if version.startswith('0.9') or version.startswith('1.'):
                    name = server.d.directory(tor)
                else:
                    name = server.d.get_directory(tor)
                if tor.upper() not in name:
                    break
                sleep(5)
                retries -= 1
            return name
    return False  # not found


# noinspection PyUnusedLocal
def remove_torrent(hash_id, remove_data=False):
    server, _ = get_server()
    if server is False:
        return False

    mainview = server.download_list("", "main")
    for tor in mainview:
        if tor.upper() == hash_id.upper():
            return server.d.erase(tor)
    return False  # not found


def check_link():
    server, version = get_server()
    if server is False:
        return "rTorrent login FAILED\nCheck debug log"
    return "rTorrent login successful: rTorrent %s" % version
