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
import os
import shutil
import time
from xml.etree import ElementTree
try:
    import urllib3
    import requests
except ImportError:
    import lib.requests as requests
from six import PY2

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.common import getUserAgent, proxyList, listdir, path_isfile, path_isdir, syspath, remove
from lazylibrarian.formatter import check_int, md5_utf8, makeBytestr, seconds_to_midnight, plural, makeUnicode


def gr_api_sleep():
    time_now = time.time()
    delay = time_now - lazylibrarian.LAST_GOODREADS
    if delay < 1.0:
        sleep_time = 1.0 - delay
        lazylibrarian.GR_SLEEP += sleep_time
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
            logger.debug("GoodReads sleep %.3f, total %.3f" % (sleep_time, lazylibrarian.GR_SLEEP))
        time.sleep(sleep_time)
    lazylibrarian.LAST_GOODREADS = time_now


def cv_api_sleep():
    time_now = time.time()
    delay = time_now - lazylibrarian.LAST_COMICVINE
    if delay < 1.0:
        sleep_time = 1.0 - delay
        lazylibrarian.CV_SLEEP += sleep_time
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
            logger.debug("ComicVine sleep %.3f, total %.3f" % (sleep_time, lazylibrarian.CV_SLEEP))
        time.sleep(sleep_time)
    lazylibrarian.LAST_COMICVINE = time_now


def fetchURL(URL, headers=None, retry=True, raw=None):
    """ Return the result of fetching a URL and True if success
        Otherwise return error message and False
        Return data as raw/bytes in python2 or if raw == True
        On python3 default to unicode, need to set raw=True for images/data
        Allow one retry on timeout by default"""
    URL = makeUnicode(URL)
    if 'googleapis' in URL:
        lazylibrarian.GB_CALLS += 1
        for entry in lazylibrarian.PROVIDER_BLOCKLIST:
            if entry["name"] == 'googleapis':
                if int(time.time()) < int(entry['resume']):
                    return "Blocked", False
                else:
                    lazylibrarian.PROVIDER_BLOCKLIST.remove(entry)
                    lazylibrarian.GB_CALLS = 0

    if raw is None:
        if PY2:
            raw = True
        else:
            raw = False

    if headers is None:
        # some sites insist on having a user-agent, default is to add one
        # if you don't want any headers, send headers=[]
        headers = {'User-Agent': getUserAgent()}
    proxies = proxyList()
    try:
        # jackett query all indexers needs a longer timeout
        # /torznab/all/api?q=  or v2.0/indexers/all/results/torznab/api?q=
        if '/torznab/' in URL and ('/all/' in URL or '/aggregate/' in URL):
            timeout = check_int(lazylibrarian.CONFIG['HTTP_EXT_TIMEOUT'], 90)
        else:
            timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
        if URL.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
            r = requests.get(URL, headers=headers, timeout=timeout, proxies=proxies,
                             verify=lazylibrarian.CONFIG['SSL_CERTS'] if lazylibrarian.CONFIG['SSL_CERTS'] else True)
        else:
            r = requests.get(URL, headers=headers, timeout=timeout, proxies=proxies, verify=False)

        if str(r.status_code).startswith('2'):  # (200 OK etc)
            if raw:
                return r.content, True
            return r.text, True
        elif r.status_code == 403 and 'googleapis' in URL:
            logger.debug(r.text)
            # noinspection PyBroadException
            try:
                source = r.json()
                msg = source['error']['message']
            except Exception:
                msg = "Error 403: see debug log"

            if 'Limit Exceeded' in msg:
                # how long until midnight Pacific Time when google reset the quotas
                delay = seconds_to_midnight() + 28800  # PT is 8hrs behind UTC
                if delay > 86400:
                    delay -= 86400  # no roll-over to next day
            else:
                # might be forbidden for a different reason where midnight might not matter
                # eg "Cannot determine user location for geographically restricted operation"
                delay = 3600

            for entry in lazylibrarian.PROVIDER_BLOCKLIST:
                if entry["name"] == 'googleapis':
                    lazylibrarian.PROVIDER_BLOCKLIST.remove(entry)
            newentry = {"name": 'googleapis', "resume": int(time.time()) + delay, "reason": msg}
            lazylibrarian.PROVIDER_BLOCKLIST.append(newentry)

        # noinspection PyBroadException
        try:
            # noinspection PyProtectedMember
            msg = requests.status_codes._codes[r.status_code][0]
        except Exception:
            msg = r.text
        return "Response status %s: %s" % (r.status_code, msg), False
    except requests.exceptions.Timeout as e:
        if not retry:
            logger.error("fetchURL: Timeout getting response from %s" % URL)
            return "Timeout %s" % str(e), False
        logger.debug("fetchURL: retrying - got timeout on %s" % URL)
        result, success = fetchURL(URL, headers=headers, retry=False, raw=raw)
        return result, success
    except Exception as e:
        return "Exception %s: %s" % (type(e).__name__, str(e)), False


def cache_img(img_type, img_ID, img_url, refresh=False):
    """ Cache the image from the given filename or URL in the local images cache
        linked to the id, return the link to the cached file, success, was_in_cache
        or error message, False, False if failed to cache """

    if img_type not in ['book', 'author', 'magazine', 'comic']:
        logger.error('Internal error in cache_img, img_type = [%s]' % img_type)
        img_type = 'book'

    cachefile = os.path.join(lazylibrarian.CACHEDIR, img_type, img_ID + '.jpg')
    link = 'cache/%s/%s.jpg' % (img_type, img_ID)
    if path_isfile(cachefile) and not refresh:  # overwrite any cached image
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
            logger.debug("Cached %s image exists %s" % (img_type, cachefile))
        return link, True, True

    if img_url.startswith('http'):
        result, success = fetchURL(img_url, raw=True)
        if success:
            try:
                with open(syspath(cachefile), 'wb') as img:
                    img.write(result)
                return link, True, False
            except Exception as e:
                logger.error("%s writing image to %s, %s" % (type(e).__name__, cachefile, str(e)))
                logger.error("Image url: %s" % img_url)
                return str(e), False, False
        return result, False, False
    else:
        try:
            shutil.copyfile(img_url, cachefile)
            return link, True, True
        except Exception as e:
            logger.error("%s copying image to %s, %s" % (type(e).__name__, cachefile, str(e)))
            return str(e), False, False


def gr_xml_request(my_url, useCache=True, expire=True):
    # respect goodreads api limit
    result, in_cache = get_cached_request(url=my_url, useCache=useCache, cache="XML", expire=expire)
    return result, in_cache


def gb_json_request(my_url, useCache=True, expire=True):
    result, in_cache = get_cached_request(url=my_url, useCache=useCache, cache="JSON", expire=expire)
    return result, in_cache


def html_request(my_url, useCache=True, expire=True):
    result, in_cache = get_cached_request(url=my_url, useCache=useCache, cache="HTML", expire=expire)
    return result, in_cache


def get_cached_request(url, useCache=True, cache="XML", expire=True):
    # hashfilename = hash of url
    # if hashfilename exists in cache and isn't too old, return its contents
    # if not, read url and store the result in the cache
    # return the result, and boolean True if source was cache
    #
    cacheLocation = cache + "Cache"
    cacheLocation = os.path.join(lazylibrarian.CACHEDIR, cacheLocation)
    myhash = md5_utf8(url)
    valid_cache = False
    source = None
    hashfilename = os.path.join(cacheLocation, myhash[0], myhash[1], myhash + "." + cache.lower())
    expiry = lazylibrarian.CONFIG['CACHE_AGE'] * 24 * 60 * 60  # expire cache after this many seconds

    if useCache and path_isfile(hashfilename):
        cache_modified_time = os.stat(hashfilename).st_mtime
        time_now = time.time()
        if expire and cache_modified_time < time_now - expiry:
            # Cache entry is too old, delete it
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
                logger.debug("Expiring %s" % myhash)
            os.remove(syspath(hashfilename))
        else:
            valid_cache = True

    if valid_cache:
        lazylibrarian.CACHE_HIT = int(lazylibrarian.CACHE_HIT) + 1
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
            logger.debug("CacheHandler: Returning CACHED response %s for %s" % (hashfilename, url))
        if cache == "JSON":
            try:
                source = json.load(open(hashfilename))
            except ValueError:
                logger.error("Error decoding json from %s" % hashfilename)
                # normally delete bad data, but keep for inspection if debug logging cache
                if not (lazylibrarian.LOGLEVEL & lazylibrarian.log_cache):
                    remove(hashfilename)
                return None, False
        elif cache == "HTML":
            with open(syspath(hashfilename), "rb") as cachefile:
                source = cachefile.read()
        elif cache == "XML":
            with open(syspath(hashfilename), "rb") as cachefile:
                result = cachefile.read()
            if result and result.startswith(b'<?xml'):
                try:
                    source = ElementTree.fromstring(result)
                except UnicodeEncodeError:
                    # seems sometimes the page contains utf-16 but the header says it's utf-8
                    try:
                        result = result.decode('utf-16').encode('utf-8')
                        source = ElementTree.fromstring(result)
                    except (ElementTree.ParseError, UnicodeEncodeError, UnicodeDecodeError):
                        logger.error("Error parsing xml from %s" % hashfilename)
                        source = None
                except ElementTree.ParseError:
                    logger.error("Error parsing xml from %s" % hashfilename)
                    source = None
            if source is None:
                logger.error("Error reading xml from %s" % hashfilename)
                # normally delete bad data, but keep for inspection if debug logging cache
                if not (lazylibrarian.LOGLEVEL & lazylibrarian.log_cache):
                    remove(hashfilename)
                return None, False
    else:
        lazylibrarian.CACHE_MISS = int(lazylibrarian.CACHE_MISS) + 1
        if cache == 'XML':
            gr_api_sleep()
            result, success = fetchURL(url, raw=True)
        else:
            result, success = fetchURL(url)

        if success:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
                logger.debug("CacheHandler: Storing %s %s for %s" % (cache, myhash, url))
            if cache == "JSON":
                try:
                    source = json.loads(result)
                    if not expiry:
                        return source, False
                except Exception as e:
                    logger.error("%s decoding json from %s" % (type(e).__name__, url))
                    logger.debug("%s : %s" % (e, result))
                    return None, False
                json.dump(source, open(hashfilename, "w"))
            elif cache == "HTML":
                source = makeBytestr(result)
                with open(syspath(hashfilename), "wb") as cachefile:
                    cachefile.write(source)
            elif cache == "XML":
                result = makeBytestr(result)
                if result and result.startswith(b'<?xml'):
                    try:
                        source = ElementTree.fromstring(result)
                        if not expiry:
                            return source, False
                    except UnicodeEncodeError:
                        # sometimes we get utf-16 data labelled as utf-8
                        try:
                            result = result.decode('utf-16').encode('utf-8')
                            source = ElementTree.fromstring(result)
                            if not expiry:
                                return source, False
                        except (ElementTree.ParseError, UnicodeEncodeError, UnicodeDecodeError):
                            logger.error("Error parsing xml from %s" % url)
                            source = None
                    except ElementTree.ParseError:
                        logger.error("Error parsing xml from %s" % url)
                        source = None

                if source is not None:
                    with open(syspath(hashfilename), "wb") as cachefile:
                        cachefile.write(result)
                else:
                    logger.error("Error getting xml data from %s" % url)
                    return None, False
        else:
            logger.debug("Got error response for %s: %s" % (url, result.split('<')[0]))
            if 'goodreads' in url and '503' in result:
                time.sleep(1)
            return None, False
    return source, valid_cache


def cleanCache():
    """ Remove unused files from the cache - delete if expired or unused.
        Check JSONCache  WorkCache  XMLCache  SeriesCache Author  Book  Magazine  Comic  IRC
        Check covers and authorimages etc referenced in the database exist
        and change database entry if missing, expire old pastissues table entries """

    myDB = database.DBConnection()
    myDB.upsert("jobs", {"Start": time.time()}, {"Name": "CLEANCACHE"})
    result = []
    expiry = check_int(lazylibrarian.IRC_CACHE_EXPIRY, 0)
    cache = os.path.join(lazylibrarian.CACHEDIR, "IRCCache")
    cleaned = 0
    kept = 0
    if expiry and path_isdir(cache):
        time_now = time.time()
        for cached_file in listdir(cache):
            target = os.path.join(cache, cached_file)
            cache_modified_time = os.stat(target).st_mtime
            if cache_modified_time < time_now - expiry:
                # Cache is old, delete entry
                remove(target)
                cleaned += 1
            else:
                kept += 1
    msg = "Cleaned %i expired %s from IRCCache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
    result.append(msg)
    logger.debug(msg)

    expiry = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
    expire_caches = ["JSONCache", "XMLCache"]
    for cache in expire_caches:
        cache = os.path.join(lazylibrarian.CACHEDIR, cache)
        cleaned = 0
        kept = 0
        time_now = time.time()
        if expiry and path_isdir(cache):
            for i in '0123456789abcdef':
                for j in '0123456789abcdef':
                    for cached_file in listdir(os.path.join(cache, i, j)):
                        target = os.path.join(cache, i, j, cached_file)
                        cache_modified_time = os.stat(target).st_mtime
                        if cache_modified_time < time_now - (expiry * 24 * 60 * 60):  # expire after this many seconds
                            # Cache is old, delete entry
                            remove(target)
                            cleaned += 1
                        else:
                            kept += 1
        msg = "Cleaned %i expired %s from %s, kept %i" % (cleaned, plural(cleaned, "file"), cache, kept)
        result.append(msg)
        logger.debug(msg)

    cache = os.path.join(lazylibrarian.CACHEDIR, "WorkCache")
    cleaned = 0
    kept = 0
    if path_isdir(cache):
        for i in '0123456789abcdef':
            for j in '0123456789abcdef':
                for cached_file in listdir(os.path.join(cache, i, j)):
                    try:
                        bookid = cached_file.split('.')[0]
                    except IndexError:
                        logger.error('Clean Cache: Error splitting %s' % cached_file)
                        continue
                    item = myDB.match('select BookID from books where BookID=?', (bookid,))
                    if not item:
                        # WorkPage no longer referenced in database, delete cached_file
                        remove(os.path.join(cache, i, j, cached_file))
                        cleaned += 1
                    else:
                        kept += 1
    msg = "Cleaned %i orphan %s from WorkCache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
    result.append(msg)
    logger.debug(msg)

    cache = os.path.join(lazylibrarian.CACHEDIR, "SeriesCache")
    cleaned = 0
    kept = 0
    if path_isdir(cache):
        for cached_file in listdir(cache):
            try:
                seriesid = cached_file.split('.')[0]
            except IndexError:
                logger.error('Clean Cache: Error splitting %s' % cached_file)
                continue
            item = myDB.match('select SeriesID from series where SeriesID=?', (seriesid,))
            if not item:
                # SeriesPage no longer referenced in database, delete cached_file
                remove(os.path.join(cache, cached_file))
                cleaned += 1
            else:
                kept += 1
    msg = "Cleaned %i orphan %s from SeriesCache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
    result.append(msg)
    logger.debug(msg)

    cache = os.path.join(lazylibrarian.CACHEDIR, "magazine")
    cleaned = 0
    kept = 0
    if path_isdir(cache):
        for cached_file in listdir(cache):
            item = myDB.match('select * from issues where cover=?', ('cache/magazine/%s' % cached_file,))
            if not item:
                remove(os.path.join(cache, cached_file))
                cleaned += 1
            else:
                kept += 1
    msg = "Cleaned %i orphan %s from magazine cache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
    result.append(msg)
    logger.debug(msg)

    cache = lazylibrarian.CACHEDIR
    cleaned = 0
    kept = 0
    cachedir = os.path.join(cache, 'author')
    try:
        if path_isdir(cachedir):
            res = myDB.select('SELECT AuthorImg from authors where AuthorImg like "cache/author/%"')
            images = []
            for item in res:
                images.append(item['AuthorImg'][13:])
            logger.debug("Checking %s author images" % len(images))
            for cached_file in listdir(cachedir):
                if cached_file not in images:
                    # Author Image no longer referenced in database, delete cached_file
                    remove(os.path.join(cachedir, cached_file))
                    cleaned += 1
                else:
                    kept += 1
        msg = "Cleaned %i orphan %s from AuthorCache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
        result.append(msg)
        logger.debug(msg)
    except Exception as e:
        logger.debug(str(e))

    cachedir = os.path.join(cache, 'book')
    cleaned = 0
    kept = 0
    try:
        if path_isdir(cachedir):
            res = myDB.select('SELECT BookImg from books where BookImg like "cache/book/%"')
            images = []
            for item in res:
                images.append(item['BookImg'][11:])
            logger.debug("Checking %s book images" % len(images))
            for cached_file in listdir(cachedir):
                if cached_file not in images:
                    remove(os.path.join(cachedir, cached_file))
                    cleaned += 1
                else:
                    kept += 1
        msg = "Cleaned %i orphan %s from BookCache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
        result.append(msg)
        logger.debug(msg)
    except Exception as e:
        logger.debug(str(e))

    # at this point there should be no more .jpg files in the root of the cachedir
    # any that are still there are for books/authors deleted from database
    cleaned = 0
    kept = 0
    for cached_file in listdir(cache):
        if cached_file.endswith('.jpg'):
            remove(os.path.join(cache, cached_file))
            cleaned += 1
        else:
            kept += 1
    msg = "Cleaned %i orphan %s from ImageCache, kept %i" % (cleaned, plural(cleaned, "file"), kept)
    result.append(msg)
    logger.debug(msg)

    # verify the cover images referenced in the database are present
    images = myDB.action('select BookImg,BookName,BookID from books')
    cachedir = os.path.join(lazylibrarian.CACHEDIR, 'book')
    cleaned = 0
    kept = 0
    for item in images:
        keep = True
        imgfile = ''
        if item['BookImg'] is None or item['BookImg'] == '':
            keep = False
        if keep and not item['BookImg'].startswith('http') and not item['BookImg'] == "images/nocover.png":
            # html uses '/' as separator, but os might not
            imgname = item['BookImg'].rsplit('/')[-1]
            imgfile = os.path.join(cachedir, imgname)
            if not path_isfile(imgfile):
                keep = False
        if keep:
            kept += 1
        else:
            cleaned += 1
            logger.debug('Cover missing for %s %s' % (item['BookName'], imgfile))
            myDB.action('update books set BookImg="images/nocover.png" where Bookid=?', (item['BookID'],))

    msg = "Cleaned %i missing %s, kept %i" % (cleaned, plural(cleaned, "cover"), kept)
    result.append(msg)
    logger.debug(msg)

    # verify the author images referenced in the database are present
    images = myDB.action('select AuthorImg,AuthorName,AuthorID from authors')
    cachedir = os.path.join(lazylibrarian.CACHEDIR, 'author')
    cleaned = 0
    kept = 0
    for item in images:
        keep = True
        imgfile = ''
        if item['AuthorImg'] is None or item['AuthorImg'] == '':
            keep = False
        if keep and not item['AuthorImg'].startswith('http') and not item['AuthorImg'] == "images/nophoto.png":
            # html uses '/' as separator, but os might not
            imgname = item['AuthorImg'].rsplit('/')[-1]
            imgfile = os.path.join(cachedir, imgname)
            if not path_isfile(imgfile):
                keep = False
        if keep:
            kept += 1
        else:
            cleaned += 1
            logger.debug('Image missing for %s %s' % (item['AuthorName'], imgfile))
            myDB.action('update authors set AuthorImg="images/nophoto.png" where AuthorID=?', (item['AuthorID'],))

    msg = "Cleaned %i missing author %s, kept %i" % (cleaned, plural(cleaned, "image"), kept)
    result.append(msg)

    expiry = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
    if expiry:
        time_now = time.time()
        too_old = time_now - (expiry * 24 * 60 * 60)
        # delete any pastissues table entries that are too old
        count = myDB.match('SELECT COUNT(*) as counter from pastissues')
        if count:
            total = count['counter']
        else:
            total = 0

        count = myDB.match("SELECT COUNT(*) as counter from pastissues WHERE Added>0 and Added<?", (too_old,))
        if count:
            old = count['counter']
        else:
            old = 0
        myDB.action("DELETE from pastissues WHERE Added>0 and Added<?", (too_old,))
        msg = "Cleaned %i old pastissues, kept %i" % (old, total - old)
        result.append(msg)

    myDB.upsert("jobs", {"Finish": time.time()}, {"Name": "CLEANCACHE"})
    logger.debug(msg)
    return result
