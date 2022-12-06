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
import re
import time
import unicodedata
from base64 import b16encode, b32decode, b64encode
from hashlib import sha1
import magic

import lazylibrarian
from lazylibrarian import logger, database, nzbget, sabnzbd, classes, utorrent, transmission, qbittorrent, \
    deluge, rtorrent, synology
from lazylibrarian.cache import fetch_url
from lazylibrarian.common import setperm, get_user_agent, proxy_list, make_dirs, \
    path_isdir, syspath, remove
from lazylibrarian.formatter import clean_name, unaccented, get_list, make_unicode, md5_utf8, \
    seconds_to_midnight, check_int, sanitize
from lazylibrarian.postprocess import delete_task, check_contents
from lazylibrarian.providers import block_provider
from lazylibrarian.ircbot import irc_connect, irc_search

from deluge_client import DelugeRPCClient
from .magnet2torrent import magnet2torrent
from lib.bencode import bencode, bdecode

import html5lib
from bs4 import BeautifulSoup

import urllib3
import requests


def use_label(source, library):
    if source in ['DELUGERPC', 'DELUGEWEBUI']:
        labels = lazylibrarian.CONFIG['DELUGE_LABEL']
    elif source in ['TRANSMISSION', 'UTORRENT', 'RTORRENT', 'QBITTORRENT']:
        labels = lazylibrarian.CONFIG['%s_LABEL' % source]
    elif source in ['SABNZBD']:
        labels = lazylibrarian.CONFIG['SAB_CAT']
    elif source in ['NZBGET']:
        labels = lazylibrarian.CONFIG['NZBGET_CATEGORY']
    else:
        labels = ''

    if not library or ',' not in labels:
        return labels

    labels = get_list(labels, ',')
    try:
        if library == 'eBook':
            return labels[0]
        if library == 'AudioBook':
            return labels[1]
        if library == 'magazine':
            return labels[2]
        if library == 'Comic':
            return labels[3]
    except IndexError:
        pass

    return ''


def irc_dl_method(bookid=None, dl_title=None, dl_url=None, library='eBook', provider=None):
    db = database.DBConnection()
    source = provider
    msg = ''
    logger.debug("Starting IRC Download for [%s]" % dl_title)
    fname = ""
    data = None
    myprov = None

    for item in lazylibrarian.IRC_PROV:
        if item['NAME'] == provider or item['DISPNAME'] == provider:
            myprov = item
            break

    if not myprov:
        msg = "%s server not found" % provider
    else:
        myprov['IRC'] = None  # new download, start a new connection
        irc = irc_connect(myprov)
        if not irc:
            msg = "Failed to connect"
            myprov['IRC'] = None
        else:
            fname, data = irc_search(myprov, dl_title, cmd=dl_url, cache=False)
            if not fname:
                myprov['IRC'] = None

    # noinspection PyTypeChecker
    download_id = sha1(bencode(dl_url + ':' + dl_title)).hexdigest()

    if fname and data:
        fname = sanitize(fname)
        destdir = os.path.join(lazylibrarian.directory('Download'), fname)
        if not path_isdir(destdir):
            _ = make_dirs(destdir)

        destfile = os.path.join(destdir, fname)

        try:
            with open(syspath(destfile), 'wb') as bookfile:
                bookfile.write(data)
            setperm(destfile)
        except Exception as e:
            msg = "%s writing book to %s, %s" % (type(e).__name__, destfile, e)
            logger.error(msg)
            return False, msg

        logger.debug('File %s has been downloaded from %s' % (dl_title, dl_url))
        if library == 'eBook':
            db.action('UPDATE books SET status="Snatched" WHERE BookID=?', (bookid,))
        elif library == 'AudioBook':
            db.action('UPDATE books SET audiostatus="Snatched" WHERE BookID=?', (bookid,))
        db.action('UPDATE wanted SET status="Snatched", Source=?, DownloadID=? WHERE NZBurl=? and NZBtitle=?',
                  (source, download_id, dl_url, dl_title))
        return True, ''

    elif not fname:
        msg = 'UPDATE wanted SET status="Failed", Source=?, DownloadID=?, DLResult=? '
        msg += 'WHERE NZBurl=? and NZBtitle=?'
        if not data:
            data = 'Failed'
        db.action(msg, (source, download_id, data, dl_url, dl_title))
        msg = data
        if 'timed out' in data:  # need to reconnect
            provider['IRC'] = None
            logger.error(msg)
    return False, msg


def nzb_dl_method(bookid=None, nzbtitle=None, nzburl=None, library='eBook', label=''):
    db = database.DBConnection()
    source = ''
    download_id = ''

    if lazylibrarian.CONFIG['NZB_DOWNLOADER_SABNZBD'] and lazylibrarian.CONFIG['SAB_HOST']:
        source = "SABNZBD"
        if lazylibrarian.CONFIG['SAB_EXTERNAL_HOST']:
            # new method, download nzb data, write to file, send file to sab, delete file
            data, success = fetch_url(nzburl, raw=True)
            if not success:
                res = 'Failed to read nzb data for sabnzbd: %s' % data
                logger.debug(res)
                download_id = ''
            else:
                logger.debug("Got %s bytes data" % len(data))
                temp_filename = os.path.join(lazylibrarian.CACHEDIR, "nzbfile.nzb")
                with open(syspath(temp_filename), 'wb') as f:
                    f.write(data)
                logger.debug("Data written to file")
                nzb_url = lazylibrarian.CONFIG['SAB_EXTERNAL_HOST']
                if not nzb_url.startswith('http'):
                    if lazylibrarian.CONFIG['HTTPS_ENABLED']:
                        nzb_url = 'https://' + nzb_url
                    else:
                        nzb_url = 'http://' + nzb_url
                if lazylibrarian.CONFIG['HTTP_ROOT']:
                    nzb_url = nzb_url + '/' + lazylibrarian.CONFIG['HTTP_ROOT']
                nzb_url = nzb_url + '/nzbfile.nzb'
                logger.debug("nzb_url [%s]" % nzb_url)
                download_id, res = sabnzbd.sab_nzbd(nzbtitle, nzb_url, remove_data=False, library=library, label=label)
                # returns nzb_ids or False
                logger.debug("Sab returned %s/%s" % (download_id, res))
                # os.unlink(temp_filename)
                # logger.debug("Temp file deleted")
        else:
            download_id, res = sabnzbd.sab_nzbd(nzbtitle, nzburl, remove_data=False, library=library, label=label)
            # returns nzb_ids or False
        if download_id and lazylibrarian.CONFIG['NZB_PAUSED']:
            _ = sabnzbd.sab_nzbd(nzbtitle, 'pause', False, None, download_id, library=library, label=label)

    if lazylibrarian.CONFIG['NZB_DOWNLOADER_NZBGET'] and lazylibrarian.CONFIG['NZBGET_HOST']:
        source = "NZBGET"
        data, success = fetch_url(nzburl, raw=True)
        if not success:
            res = 'Failed to read nzb data for nzbget: %s' % data
            logger.debug(res)
            download_id = ''
        else:
            nzb = classes.NZBDataSearchResult()
            nzb.extraInfo.append(data)
            nzb.name = nzbtitle
            nzb.url = nzburl
            download_id, res = nzbget.send_nzb(nzb, library=library, label=label)
            if download_id and lazylibrarian.CONFIG['NZB_PAUSED']:
                _ = nzbget.send_nzb(nzb, 'GroupPause', download_id)

    if lazylibrarian.CONFIG['NZB_DOWNLOADER_SYNOLOGY'] and lazylibrarian.CONFIG['USE_SYNOLOGY'] and \
            lazylibrarian.CONFIG['SYNOLOGY_HOST']:
        source = "SYNOLOGY_NZB"
        download_id, res = synology.add_torrent(nzburl)  # returns nzb_ids or False

    if lazylibrarian.CONFIG['NZB_DOWNLOADER_BLACKHOLE']:
        source = "BLACKHOLE"
        nzbfile, success = fetch_url(nzburl, raw=True)
        if not success:
            res = 'Error fetching nzb from url [%s]: %s' % (nzburl, nzbfile)
            logger.warn(res)
            return False, res

        if nzbfile:
            nzbname = str(nzbtitle) + '.nzb'
            nzbpath = os.path.join(lazylibrarian.CONFIG['NZB_BLACKHOLEDIR'], nzbname)
            try:
                with open(syspath(nzbpath), 'wb') as f:
                    if isinstance(nzbfile, str):
                        nzbfile = nzbfile.encode('iso-8859-1')
                    f.write(nzbfile)
                logger.debug('NZB file saved to: ' + nzbpath)
                setperm(nzbpath)
                download_id = nzbname

            except Exception as e:
                res = '%s not writable, NZB not saved. %s: %s' % (nzbpath, type(e).__name__, str(e))
                logger.error(res)
                return False, res

    if not source:
        res = 'No NZB download method is enabled, check config.'
        logger.warn(res)
        return False, res

    if download_id:
        logger.debug('Nzbfile has been downloaded from ' + str(nzburl))
        if library == 'eBook':
            db.action('UPDATE books SET status="Snatched" WHERE BookID=?', (bookid,))
        elif library == 'AudioBook':
            db.action('UPDATE books SET audiostatus = "Snatched" WHERE BookID=?', (bookid,))
        db.action('UPDATE wanted SET status="Snatched", Source=?, DownloadID=? WHERE NZBurl=?',
                  (source, download_id, nzburl))
        return True, ''
    else:
        res = 'Failed to send nzb to @ <a href="%s">%s</a>' % (nzburl, source)
        logger.error(res)
        return False, res


def direct_dl_method(bookid=None, dl_title=None, dl_url=None, library='eBook', provider=''):
    db = database.DBConnection()
    source = "DIRECT"
    logger.debug("Starting Direct Download for [%s]" % dl_title)
    proxies = proxy_list()
    headers = {'Accept-encoding': 'gzip', 'User-Agent': get_user_agent()}
    dl_url = make_unicode(dl_url)
    s = requests.Session()
    if provider == 'zlibrary':
        # do we need to login?
        if lazylibrarian.CONFIG.get('BOK_USER') and lazylibrarian.CONFIG.get('BOK_PASS'):
            bok_login_url = lazylibrarian.CONFIG['BOK_LOGIN']
            data = {
                    "isModal": True,
                    "email": lazylibrarian.CONFIG.get('BOK_USER'),
                    "password": lazylibrarian.CONFIG.get('BOK_PASS'),
                    "site_mode": "books",
                    "action": "login",
                    "isSingleLogin": 1,
                    "redirectUrl": "",
                    "gg_json_mode": 1
                }
            headers = {'User-Agent': get_user_agent()}

            if bok_login_url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                response = s.post(bok_login_url, data=data, timeout=90, headers=headers,
                                  verify=lazylibrarian.CONFIG['SSL_CERTS'] if lazylibrarian.CONFIG['SSL_CERTS']
                                  else True)
            else:
                response = s.post(bok_login_url, data=data, timeout=90, headers=headers, verify=False)
            logger.debug("b-ok login response: %s" % response.status_code)
            # use these login cookies for all 1-lib, z-library, b-ok domains
            for c in s.cookies:
                c.domain = ''

        # zlibrary needs a referer header from a zlibrary host
        headers['Referer'] = dl_url
        count, oldest = lazylibrarian.bok_dlcount()
        limit = check_int(lazylibrarian.CONFIG['BOK_DLLIMIT'], 5)
        if limit and count >= limit:
            res = 'Reached Daily download limit (%s)' % limit
            delay = oldest + 24*60*60 - time.time()
            block_provider(provider, res, delay=delay)
            return False, res

    redirects = 0
    while redirects < 5:
        redirects += 1
        try:
            logger.debug("%s: [%s] %s" % (redirects, provider, str(headers)))
            if not dl_url.startswith('http'):
                if headers.get('Referer', '').startswith('https://'):
                    dl_url = 'https://' + dl_url
                else:
                    dl_url = 'http://' + dl_url

            if dl_url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                r = s.get(dl_url, headers=headers, timeout=90, proxies=proxies,
                          verify=lazylibrarian.CONFIG['SSL_CERTS'] if lazylibrarian.CONFIG['SSL_CERTS'] else True)
            else:
                r = s.get(dl_url, headers=headers, timeout=90, proxies=proxies, verify=False)
        except requests.exceptions.Timeout:
            res = 'Timeout fetching file from url: %s' % dl_url
            logger.warn(res)
            return False, res
        except Exception as e:
            res = '%s fetching file from url: %s, %s' % (type(e).__name__, dl_url, str(e))
            logger.warn(res)
            return False, res

        if not str(r.status_code).startswith('2'):
            res = "Got a %s response for %s" % (r.status_code, dl_url)
            logger.debug(res)
            return False, res
        elif len(r.content) < 1000:
            res = "Only got %s bytes for %s" % (len(r.content), dl_title)
            logger.debug(res)
            return False, res
        elif 'application' in r.headers['Content-Type']:
            # application/octet-stream, application/epub+zip, application/x-mobi8-ebook etc.
            extn = ''
            basename = ''
            dl_title = dl_title.strip()
            if ' ' in dl_title:
                basename, extn = dl_title.rsplit(' ', 1)  # last word is often the extension - but not always...
            if extn and extn.lower() not in get_list(lazylibrarian.CONFIG['EBOOK_TYPE']):
                basename = ''
                extn = ''
            if not basename and '.' in dl_title:
                basename, extn = dl_title.rsplit('.', 1)
            if extn and extn.lower() not in get_list(lazylibrarian.CONFIG['EBOOK_TYPE']):
                basename = ''
                extn = ''
            if not basename and magic:
                try:
                    mtype = magic.from_buffer(r.content).upper()
                    logger.debug("magic reports %s" % mtype)
                except Exception as e:
                    logger.debug("%s reading magic from %s, %s" % (type(e).__name__, dl_title, e))
                    mtype = ''
                if 'EPUB' in mtype:
                    extn = 'epub'
                elif 'MOBIPOCKET' in mtype:  # also true for azw and azw3, does it matter?
                    extn = 'mobi'
                elif 'PDF' in mtype:
                    extn = 'pdf'
                elif 'RAR' in mtype:
                    extn = 'cbr'
                elif 'ZIP' in mtype:
                    extn = 'cbz'
                basename = dl_title
            if not extn:
                logger.warn("Don't know the filetype for [%s]" % dl_title)
                basename = dl_title
            else:
                extn = extn.lower()
            if '/' in basename:
                basename = basename.split('/')[0]

            logger.debug("File download got %s bytes for %s" % (len(r.content), basename))

            basename = sanitize(basename)
            destdir = os.path.join(lazylibrarian.directory('Download'), basename)
            if not path_isdir(destdir):
                _ = make_dirs(destdir)

            try:
                hashid = dl_url.split("md5=")[1].split("&")[0]
            except IndexError:
                # noinspection PyTypeChecker
                hashid = sha1(bencode(dl_url)).hexdigest()

            destfile = os.path.join(destdir, basename + '.' + extn)

            if os.name == 'nt':  # Windows has max path length of 256
                destfile = '\\\\?\\' + destfile

            try:
                with open(syspath(destfile), 'wb') as bookfile:
                    bookfile.write(r.content)
                setperm(destfile)
                download_id = hashid
                logger.debug('File %s has been downloaded from %s' % (dl_title, dl_url))
                if library == 'eBook':
                    db.action('UPDATE books SET status="Snatched" WHERE BookID=?', (bookid,))
                elif library == 'AudioBook':
                    db.action('UPDATE books SET audiostatus="Snatched" WHERE BookID=?', (bookid,))
                cmd = 'UPDATE wanted SET status="Snatched", Source=?, DownloadID=?, '
                cmd += 'completed=? WHERE BookID=? and NZBProv=?'
                db.action(cmd, (source, download_id, int(time.time()), bookid, provider))
                return True, ''
            except Exception as e:
                res = "%s writing book to %s, %s" % (type(e).__name__, destfile, e)
                logger.error(res)
                return False, res
        else:
            res = "Got unexpected response type (%s) for %s" % (r.headers['Content-Type'], dl_title)
            if 'text/html' in r.headers['Content-Type'] and provider == 'zlibrary':
                if b'Daily limit reached' in r.content:
                    try:
                        limit = make_unicode(r.content.split(b'more than')[1].split(b'downloads')[0].strip())
                        n = check_int(limit, 0)
                        if n:
                            lazylibrarian.CONFIG['BOK_DLLIMIT'] = n
                    except IndexError:
                        limit = 'unknown'
                    msg = "Daily limit (%s) reached" % limit
                    block_provider(provider, msg, delay=seconds_to_midnight())
                    logger.warn(msg)
                    return False, msg
                elif b'Too many requests' in r.content:
                    msg = "Too many requests"
                    block_provider(provider, msg)
                    logger.warn(msg)
                    return False, msg

            logger.debug(res)
            if redirects and 'text/html' in r.headers['Content-Type'] and provider == 'zlibrary':
                host = lazylibrarian.CONFIG['BOK_HOST']
                headers['Referer'] = dl_url

                if dl_url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                    r = s.get(dl_url, headers=headers, timeout=90, proxies=proxies,
                              verify=lazylibrarian.CONFIG['SSL_CERTS'] if lazylibrarian.CONFIG['SSL_CERTS'] else True)
                else:
                    r = s.get(dl_url, headers=headers, timeout=90, proxies=proxies, verify=False)

                if not str(r.status_code).startswith('2'):
                    return False, "Unable to fetch %s: %s" % (r.url, r.status_code)
                try:
                    res = r.content
                    newsoup = BeautifulSoup(res, "html5lib")
                    a = newsoup.find('a', {"class": "dlButton"})
                    if not a:
                        link = ''
                        if b'Daily limit reached' in res:
                            msg = "Daily limit reached"
                            block_provider(provider, msg, delay=seconds_to_midnight())
                            logger.warn(msg)
                            return False, msg
                        elif b'Too many requests' in res:
                            msg = "Too many requests"
                            block_provider(provider, msg)
                            logger.warn(msg)
                            return False, msg
                    else:
                        link = a.get('href')
                    if link and len(link) > 2:
                        dl_url = host + link
                    else:
                        return False, 'No link available'
                except Exception as e:
                    return False, "An error occurred parsing %s: %s" % (r.url, str(e))
            else:
                cache_location = os.path.join(lazylibrarian.CACHEDIR, "HTMLCache")
                myhash = md5_utf8(dl_url)
                hashfilename = os.path.join(cache_location, myhash[0], myhash[1], myhash + ".html")
                with open(syspath(hashfilename), "wb") as cachefile:
                    cachefile.write(r.content)
                logger.debug("Saved html page: %s" % hashfilename)
                return False, res

    res = 'Failed to download file @ <a href="%s">%s</a>' % (dl_url, dl_url)
    logger.error(res)
    return False, res


def tor_dl_method(bookid=None, tor_title=None, tor_url=None, library='eBook', label=''):
    db = database.DBConnection()
    download_id = False
    source = ''
    torrent = ''

    full_url = tor_url  # keep the url as stored in "wanted" table
    tor_url = make_unicode(tor_url)
    if 'magnet:?' in tor_url:
        # discard any other parameters and just use the magnet link
        tor_url = 'magnet:?' + tor_url.split('magnet:?')[1]
    else:
        # h = HTMLParser()
        # tor_url = h.unescape(tor_url)
        # HTMLParser is probably overkill, we only seem to get &amp;
        #
        tor_url = tor_url.replace('&amp;', '&')

        if '&file=' in tor_url:
            # torznab results need to be re-encoded
            # had a problem with torznab utf-8 encoded strings not matching
            # our utf-8 strings because of long/short form differences
            url, value = tor_url.split('&file=', 1)
            value = unicodedata.normalize('NFC', value)  # normalize to short form
            value = value.encode('unicode-escape')  # then escape the result
            value = make_unicode(value)  # ensure unicode
            value = value.replace(' ', '%20')  # and encode any spaces
            tor_url = url + '&file=' + value

        # strip url back to the .torrent as some sites add extra parameters
        if not tor_url.endswith('.torrent') and '.torrent' in tor_url:
            tor_url = tor_url.split('.torrent')[0] + '.torrent'

        headers = {'Accept-encoding': 'gzip', 'User-Agent': get_user_agent()}
        proxies = proxy_list()

        try:
            logger.debug("Fetching %s" % tor_url)
            if tor_url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                r = requests.get(tor_url, headers=headers, timeout=90, proxies=proxies,
                                 verify=lazylibrarian.CONFIG['SSL_CERTS']
                                 if lazylibrarian.CONFIG['SSL_CERTS'] else True)
            else:
                r = requests.get(tor_url, headers=headers, timeout=90, proxies=proxies, verify=False)
            if str(r.status_code).startswith('2'):
                torrent = r.content
                if not len(torrent):
                    res = "Got empty response for %s" % tor_url
                    logger.warn(res)
                    return False, res
                elif len(torrent) < 100:
                    res = "Only got %s bytes for %s" % (len(torrent), tor_url)
                    logger.warn(res)
                    return False, res
                else:
                    logger.debug("Got %s bytes for %s" % (len(torrent), tor_url))
            else:
                res = "Got a %s response for %s" % (r.status_code, tor_url)
                logger.warn(res)
                return False, res

        except requests.exceptions.Timeout:
            res = 'Timeout fetching file from url: %s' % tor_url
            logger.warn(res)
            return False, res
        except Exception as e:
            # some jackett providers redirect internally using http 301 to a magnet link
            # which requests can't handle, so throws an exception
            logger.debug("Requests exception: %s" % str(e))
            if "magnet:?" in str(e):
                tor_url = 'magnet:?' + str(e).split('magnet:?')[1].strip("'")
                logger.debug("Redirecting to %s" % tor_url)
            else:
                res = '%s fetching file from url: %s, %s' % (type(e).__name__, tor_url, str(e))
                logger.warn(res)
                return False, res

    if not torrent and not tor_url.startswith('magnet:?'):
        res = "No magnet or data, cannot continue"
        logger.warn(res)
        return False, res

    if lazylibrarian.CONFIG['TOR_DOWNLOADER_BLACKHOLE']:
        source = "BLACKHOLE"
        logger.debug("Sending %s to blackhole" % tor_title)
        tor_name = clean_name(tor_title).replace(' ', '_')
        if tor_url and tor_url.startswith('magnet'):
            hashid = calculate_torrent_hash(tor_url)
            if not hashid:
                hashid = tor_name
            if lazylibrarian.CONFIG['TOR_CONVERT_MAGNET']:
                tor_name = 'meta-' + hashid + '.torrent'
                tor_path = os.path.join(lazylibrarian.CONFIG['TORRENT_DIR'], tor_name)
                result = magnet2torrent(tor_url, tor_path)
                if result is not False:
                    logger.debug('Magnet file saved as: %s' % tor_path)
                    download_id = hashid
            else:
                tor_name += '.magnet'
                tor_path = os.path.join(lazylibrarian.CONFIG['TORRENT_DIR'], tor_name)
                msg = ''
                try:
                    msg = 'Opening '
                    with open(syspath(tor_path), 'wb') as torrent_file:
                        msg += 'Writing '
                        if isinstance(torrent, str):
                            torrent = torrent.encode('iso-8859-1')
                        torrent_file.write(torrent)
                    msg += 'SettingPerm '
                    setperm(tor_path)
                    msg += 'Saved '
                    logger.debug('Magnet file saved: %s' % tor_path)
                    download_id = hashid
                except Exception as e:
                    res = "Failed to write magnet to file: %s %s" % (type(e).__name__, str(e))
                    logger.warn(res)
                    logger.debug("Progress: %s Filename [%s]" % (msg, repr(tor_path)))
                    return False, res
        else:
            tor_name += '.torrent'
            tor_path = os.path.join(lazylibrarian.CONFIG['TORRENT_DIR'], tor_name)
            msg = ''
            try:
                msg = 'Opening '
                with open(syspath(tor_path), 'wb') as torrent_file:
                    msg += 'Writing '
                    if isinstance(torrent, str):
                        torrent = torrent.encode('iso-8859-1')
                    torrent_file.write(torrent)
                msg += 'SettingPerm '
                setperm(tor_path)
                msg += 'Saved '
                logger.debug('Torrent file saved: %s' % tor_name)
                download_id = source
            except Exception as e:
                res = "Failed to write torrent to file: %s %s" % (type(e).__name__, str(e))
                logger.warn(res)
                logger.debug("Progress: %s Filename [%s]" % (msg, repr(tor_path)))
                return False, res

    else:
        hashid = calculate_torrent_hash(tor_url, torrent)
        if not hashid:
            res = "Unable to calculate torrent hash from url/data"
            logger.error(res)
            logger.debug("url: %s" % tor_url)
            logger.debug("data: %s" % make_unicode(str(torrent[:50])))
            return False, res

        if lazylibrarian.CONFIG['TOR_DOWNLOADER_UTORRENT'] and lazylibrarian.CONFIG['UTORRENT_HOST']:
            logger.debug("Sending %s to Utorrent" % tor_title)
            source = "UTORRENT"
            download_id, res = utorrent.add_torrent(tor_url, hashid)  # returns hash or False
            if download_id:
                if lazylibrarian.CONFIG['TORRENT_PAUSED']:
                    utorrent.pause_torrent(download_id)
                if not label:
                    label = use_label(source, library)
                if label:
                    utorrent.label_torrent(download_id, label)
                tor_title = utorrent.name_torrent(download_id)

        if lazylibrarian.CONFIG['TOR_DOWNLOADER_RTORRENT'] and lazylibrarian.CONFIG['RTORRENT_HOST']:
            logger.debug("Sending %s to rTorrent" % tor_title)
            source = "RTORRENT"
            if not torrent and tor_url.startswith('magnet:?'):
                logger.debug("Converting magnet to data for rTorrent")
                torrentfile = magnet2torrent(tor_url)
                if torrentfile:
                    with open(syspath(torrentfile), 'rb') as f:
                        torrent = f.read()
                    remove(torrentfile)
                if not torrent:
                    logger.debug("Unable to convert magnet")
            if torrent:
                logger.debug("Sending %s data to rTorrent" % tor_title)
                download_id, res = rtorrent.add_torrent(tor_title, hashid, data=torrent)
            else:
                logger.debug("Sending %s url to rTorrent" % tor_title)
                download_id, res = rtorrent.add_torrent(tor_url, hashid)  # returns hash or False
            if download_id:
                tor_title = rtorrent.get_name(download_id)

        if lazylibrarian.CONFIG['TOR_DOWNLOADER_QBITTORRENT'] and lazylibrarian.CONFIG['QBITTORRENT_HOST']:
            source = "QBITTORRENT"
            logger.debug("Sending %s url to qBittorrent" % tor_title)
            status, res = qbittorrent.add_torrent(tor_url, hashid)  # returns True or False
            if status:
                download_id = hashid
                tor_title = qbittorrent.get_name(hashid)

        if lazylibrarian.CONFIG['TOR_DOWNLOADER_TRANSMISSION'] and lazylibrarian.CONFIG['TRANSMISSION_HOST']:
            source = "TRANSMISSION"
            if not label:
                label = use_label(source, library)
            directory = lazylibrarian.CONFIG['TRANSMISSION_DIR']
            if label and not directory.endswith(label):
                directory = os.path.join(directory, label)
            if torrent:
                logger.debug("Sending %s data to Transmission:%s" % (tor_title, directory))
                # transmission needs b64encoded metainfo to be unicode, not bytes
                download_id, res = transmission.add_torrent(None, directory=directory,
                                                            metainfo=make_unicode(b64encode(torrent)))
            else:
                logger.debug("Sending %s url to Transmission:%s" % (tor_title, directory))
                download_id, res = transmission.add_torrent(tor_url, directory=directory)  # returns id or False
            if download_id:
                # transmission returns it's own int, but we store hashid instead
                download_id = hashid
                if label:
                    transmission.set_label(download_id, label)
                tor_title = transmission.get_torrent_folder(download_id)

        if lazylibrarian.CONFIG['TOR_DOWNLOADER_SYNOLOGY'] and lazylibrarian.CONFIG['USE_SYNOLOGY'] and \
                lazylibrarian.CONFIG['SYNOLOGY_HOST']:
            logger.debug("Sending %s url to Synology" % tor_title)
            source = "SYNOLOGY_TOR"
            download_id, res = synology.add_torrent(tor_url)  # returns id or False
            if download_id:
                tor_title = synology.get_name(download_id)
                if lazylibrarian.CONFIG['TORRENT_PAUSED']:
                    synology.pause_torrent(download_id)

        if lazylibrarian.CONFIG['TOR_DOWNLOADER_DELUGE'] and lazylibrarian.CONFIG['DELUGE_HOST']:
            if not lazylibrarian.CONFIG['DELUGE_USER']:
                # no username, talk to the webui
                source = "DELUGEWEBUI"
                if torrent:
                    logger.debug("Sending %s data to Deluge" % tor_title)
                    download_id, res = deluge.add_torrent(tor_title, data=b64encode(torrent))
                else:
                    logger.debug("Sending %s url to Deluge" % tor_title)
                    download_id, res = deluge.add_torrent(tor_url)  # can be link or magnet, returns hash or False
                if download_id:
                    if not label:
                        label = use_label(source, library)
                    if label:
                        deluge.set_torrent_label(download_id, label)
                    tor_title = deluge.get_torrent_folder(download_id)
                else:
                    return False, res
            else:
                # have username, talk to the daemon
                source = "DELUGERPC"
                client = DelugeRPCClient(lazylibrarian.CONFIG['DELUGE_HOST'],
                                         int(lazylibrarian.CONFIG['DELUGE_PORT']),
                                         lazylibrarian.CONFIG['DELUGE_USER'],
                                         lazylibrarian.CONFIG['DELUGE_PASS'],
                                         decode_utf8=True)
                try:
                    client.connect()
                    args = {"name": tor_title}
                    if tor_url.startswith('magnet'):
                        res = "Sending %s magnet to DelugeRPC" % tor_title
                        logger.debug(res)
                        download_id = client.call('core.add_torrent_magnet', tor_url, args)
                    elif torrent:
                        res = "Sending %s data to DelugeRPC" % tor_title
                        logger.debug(res)
                        download_id = client.call('core.add_torrent_file', tor_title,
                                                  b64encode(torrent), args)
                    else:
                        res = "Sending %s url to DelugeRPC" % tor_title
                        logger.debug(res)
                        download_id = client.call('core.add_torrent_url', tor_url, args)
                    if download_id:
                        if lazylibrarian.CONFIG['TORRENT_PAUSED']:
                            _ = client.call('core.pause_torrent', download_id)
                        if not label:
                            label = use_label(source, library)
                        if label:
                            _ = client.call('label.set_torrent', download_id, label.lower())
                        result = client.call('core.get_torrent_status', download_id, {})
                        if 'name' in result:
                            tor_title = result['name']
                    else:
                        res += ' failed'
                        logger.error(res)
                        return False, res

                except Exception as e:
                    res = 'DelugeRPC failed %s %s' % (type(e).__name__, str(e))
                    logger.error(res)
                    return False, res

    if not source:
        res = 'No torrent download method is enabled, check config.'
        logger.warn(res)
        return False, res

    if download_id:
        if tor_title:
            if make_unicode(download_id).upper() in make_unicode(tor_title).upper():
                logger.warn('%s: name contains hash, probably unresolved magnet' % source)
            else:
                tor_title = unaccented(tor_title, only_ascii=False)
                # need to check against reject words list again as the name may have changed
                # library = magazine eBook AudioBook to determine which reject list
                # but we can't easily do the per-magazine rejects
                if library == 'magazine':
                    reject_list = get_list(lazylibrarian.CONFIG['REJECT_MAGS'], ',')
                elif library == 'eBook':
                    reject_list = get_list(lazylibrarian.CONFIG['REJECT_WORDS'], ',')
                elif library == 'AudioBook':
                    reject_list = get_list(lazylibrarian.CONFIG['REJECT_AUDIO'], ',')
                elif library == 'Comic':
                    reject_list = get_list(lazylibrarian.CONFIG['REJECT_COMIC'], ',')
                else:
                    logger.debug("Invalid library [%s] in tor_dl_method" % library)
                    reject_list = []

                rejected = False
                lower_title = tor_title.lower()
                for word in reject_list:
                    if word in lower_title:
                        rejected = "Rejecting torrent name %s, contains %s" % (tor_title, word)
                        logger.debug(rejected)
                        break
                if not rejected:
                    rejected = check_contents(source, download_id, library, tor_title)
                if rejected:
                    db.action('UPDATE wanted SET status="Failed",DLResult=? WHERE NZBurl=?',
                              (rejected, full_url))
                    if lazylibrarian.CONFIG['DEL_FAILED']:
                        delete_task(source, download_id, True)
                    return False, rejected
                else:
                    logger.debug('%s setting torrent name to [%s]' % (source, tor_title))
                    db.action('UPDATE wanted SET NZBtitle=? WHERE NZBurl=?', (tor_title, full_url))

        if library == 'eBook':
            db.action('UPDATE books SET status="Snatched" WHERE BookID=?', (bookid,))
        elif library == 'AudioBook':
            db.action('UPDATE books SET audiostatus="Snatched" WHERE BookID=?', (bookid,))
        db.action('UPDATE wanted SET status="Snatched", Source=?, DownloadID=? WHERE NZBurl=?',
                  (source, download_id, full_url))
        return True, ''

    res = 'Failed to send torrent to %s' % source
    logger.error(res)
    return False, res


def calculate_torrent_hash(link, data=None):
    """
    Calculate the torrent hash from a magnet link or data. Returns empty string
    when it cannot create a torrent hash given the input data.
    """
    try:
        torrent_hash = re.findall(r"urn:btih:(\w{32,40})", link)[0]
        if len(torrent_hash) == 32:
            torrent_hash = b16encode(b32decode(torrent_hash)).lower()
    except (re.error, IndexError, TypeError):
        if data:
            try:
                # noinspection PyUnresolvedReferences
                info = bdecode(data)["info"]
                # noinspection PyTypeChecker
                torrent_hash = sha1(bencode(info)).hexdigest()
            except Exception as e:
                logger.error("Error calculating hash: %s" % e)
                return ''
        else:
            logger.error("Cannot calculate torrent hash without magnet link or data")
            return ''
    logger.debug('Torrent Hash: %s' % str(torrent_hash))
    return torrent_hash
