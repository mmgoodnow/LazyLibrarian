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
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>

import os
import time
import logging
from lazylibrarian.config2 import CONFIG
from lib.qbittorrent import Client, WrongCredentials


def get_client():
    logger = logging.getLogger(__name__)

    host = CONFIG['QBITTORRENT_HOST']
    port = CONFIG.get_int('QBITTORRENT_PORT')
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"http://{host}"
    host = host.rstrip('/')

    if CONFIG['QBITTORRENT_BASE']:
        url = f"{host}:{port}/{CONFIG['QBITTORRENT_BASE'].strip('/')}"
    else:
        url = f"{host}:{port}"

    try:
        qb = Client(url, CONFIG['QBITTORRENT_USER'], CONFIG['QBITTORRENT_PASS'])
    except WrongCredentials:
        logger.debug("qBittorrent reports Wrong Credentials")
        return None

    if not qb.api_version:
        logger.debug("Failed to login to qBittorrent")
        return None
    return qb


def get_files(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'get_torrent_files({hashid})')
    hashid = hashid.lower()
    qbclient = get_client()
    if not qbclient:
        return ''
    retries = 5

    while retries:
        files = qbclient.get_torrent_files(hashid)
        if files:
            return files
        time.sleep(2)
        retries -= 1
    return ''


def get_name(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'get_name({hashid})')
    hashid = hashid.lower()
    qbclient = get_client()
    if not qbclient:
        return ''

    retries = 5
    cat = CONFIG['QBITTORRENT_LABEL']
    if not cat:
        cat = None
    while retries:
        # get_torrent(hashid) gets info on one torrent but doesn't return all the information
        # eg we are missing name, state, progress
        # so get all of our torrents and then look for the hashid
        torrents = qbclient.torrents(category=cat)
        for torrent in torrents:
            if torrent.get('hash') == hashid:
                if torrent.get('name'):
                    return torrent['name']
        time.sleep(2)
        retries -= 1
    return ''


def get_folder(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'get_folder({hashid})')
    hashid = hashid.lower()
    qbclient = get_client()
    if not qbclient:
        return ''

    retries = 5
    save_path = ''
    cat = CONFIG['QBITTORRENT_LABEL']
    if not cat:
        cat = None
    while retries:
        torrents = qbclient.torrents(category=cat)
        for torrent in torrents:
            if torrent.get('hash') == hashid:
                if torrent.get('save_path'):
                    # If there's no folder yet then it's probably a magnet, try until folder is populated
                    return torrent['save_path']
        time.sleep(6)
        retries -= 1
    if not save_path:
        return ''
    if os.name != 'nt':
        save_path = save_path.replace('\\', '/')
    return os.path.basename(os.path.normpath(save_path))


def get_progress(hashid):
    loggerdlcomms = logging.getLogger('special.dlcomms')
    loggerdlcomms.debug(f'get_progress({hashid})')
    hashid = hashid.lower()
    qbclient = get_client()
    if not qbclient:
        return -1, '', False

    preferences = qbclient.preferences()
    loggerdlcomms.debug(str(preferences))
    max_ratio = 0.0
    if 'max_ratio_enabled' in preferences and 'max_ratio' in preferences:
        if preferences['max_ratio_enabled']:
            max_ratio = float(preferences['max_ratio'])
    cat = CONFIG['QBITTORRENT_LABEL']
    if not cat:
        cat = None
    torrents = qbclient.torrents(category=cat)
    for torrent in torrents:
        if torrent.get('hash') == hashid:
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
    qbclient = get_client()
    if not qbclient:
        return False

    cat = CONFIG['QBITTORRENT_LABEL']
    if not cat:
        cat = None
    torrents = qbclient.torrents(category=cat)
    for torrent in torrents:
        if torrent.get('hash') == hashid:
            remove = True
            if torrent['state'] == 'uploading' or torrent['state'] == 'stalledUP':
                if not CONFIG.get_bool('SEED_WAIT'):
                    logger.debug(f"{torrent['name']} is seeding, removing torrent and data anyway")
                else:
                    logger.info(f"{torrent['name']} has not finished seeding yet, torrent will not be removed")
                    remove = False
            if remove:
                if remove_data:
                    qbclient.delete_permanently(hashid)
                    logger.info(f"{torrent['name']} removing torrent and data")
                else:
                    qbclient.delete(hashid)
                    logger.info(f"{torrent['name']} removing torrent")
                return True
    return False


def check_link():
    """ Check we can talk to qbittorrent"""
    try:
        qbclient = get_client()
        qb_api = qbclient.api_version
        if qb_api:
            qb_version = qbclient.qbittorrent_version
            return f"qBittorrent login successful, api: {qb_api} version: {qb_version}"
        return "qBittorrent login FAILED\nCheck debug log"
    except Exception as err:
        return f"qBittorrent login FAILED: {type(err).__name__} {str(err)}"


def add_file(data, hashid, title, provider_options):
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'add_file(data){title}')
    hashid = hashid.lower()
    qbclient = get_client()
    if not qbclient:
        return False, "Failed to login to qbittorrent"

    kwargs = get_args(provider_options)
    loggerdlcomms.debug(f'{kwargs}')
    qbclient.download_from_file(data, **kwargs)
    count = 0
    while count < 10:
        count += 1
        time.sleep(1)
        # noinspection PyProtectedMember
        torrent = qbclient.get_torrent(hashid)
        if torrent:
            if count > 1:
                loggerdlcomms.debug(f"hashid found in torrent list after {count} seconds")
            return True, ''
    res = "hashid not found in torrent list, add_file failed"
    loggerdlcomms.debug(res)
    return False, res


def add_torrent(link, hashid, provider_options):
    loggerdlcomms = logging.getLogger('special.dlcomms')

    loggerdlcomms.debug(f'add_torrent({link})')

    qbclient = get_client()
    if not qbclient:
        return False, "Failed to login to qbittorrent"

    hashid = hashid.lower()
    kwargs = get_args(provider_options)
    loggerdlcomms.debug(f'{kwargs}')
    qbclient.download_from_link(link, **kwargs)
    count = 0
    while count < 10:
        count += 1
        time.sleep(1)
        # noinspection PyProtectedMember
        torrent = qbclient.get_torrent(hashid)
        if torrent:
            if count > 1:
                loggerdlcomms.debug(f"hashid found in torrent list after {count} seconds")
            return True, ''
    res = "hashid not found in torrent list, add_torrent failed"
    loggerdlcomms.debug(res)
    return False, res


def get_args(provider_options):
    """ Get optional arguments based on configuration"""
    args = {'paused': 'true' if CONFIG.get_bool('TORRENT_PAUSED') else 'false'}
    if CONFIG['QBITTORRENT_DIR']:
        args['savepath'] = CONFIG['QBITTORRENT_DIR']

    if CONFIG['QBITTORRENT_LABEL']:
        args['category'] = CONFIG['QBITTORRENT_LABEL']

    if "seed_ratio" in provider_options:
        args['ratioLimit'] = provider_options["seed_ratio"]
    if "seed_duration" in provider_options:
        args['seedingTimeLimit'] = provider_options["seed_duration"]

    return args
