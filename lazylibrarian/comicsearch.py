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


import threading
import traceback
import time
import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.formatter import getList, plural, dateFormat, unaccented, replace_all, check_int, \
    now, dispName
from lazylibrarian.providers import IterateOverRSSSites, IterateOverTorrentSites, IterateOverNewzNabSites, \
    IterateOverDirectSites, IterateOverIRCSites
from lazylibrarian.common import scheduleJob
from lazylibrarian.comicid import cv_identify, cx_identify
from lazylibrarian.notifiers import notify_snatch, custom_notify_snatch
from lazylibrarian.downloadmethods import NZBDownloadMethod, TORDownloadMethod, DirectDownloadMethod

try:
    from fuzzywuzzy import fuzz
except ImportError:
    from lib.fuzzywuzzy import fuzz


# '0': '', '1': '', '2': '', '3': '', '4': '', '5': '', '6': '', '7': '', '8': '', '9': '',
dictrepl = {'...': '', '.': ' ', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', ' + ': ' ', '"': '',
            ',': ' ', '*': '', '(': '', ')': '', '[': '', ']': '', '#': '', '\'': '',
            ':': '', '!': '', '-': ' ', r'\s\s': ' '}


def searchItem(comicid=None):
    """
    Call all active search providers to search for comic by id
    return a list of results, each entry in list containing percentage_match, title, provider, size, url
    """
    results = []

    if not comicid:
        return results

    myDB = database.DBConnection()
    cmd = 'SELECT Title,SearchTerm from comics WHERE Status="Active" and ComicID=?'
    match = myDB.match(cmd, (comicid,))
    if not match:
        logger.debug("No comic match for %s" % comicid)
        return results

    cat = 'comic'
    book = {'library': cat, 'bookid': comicid, 'bookName': match['Title']}
    searchterm = match['SearchTerm']
    if not searchterm:
        searchterm = match['Title']
    book['searchterm'] = searchterm.replace('+', ' ')

    nprov = lazylibrarian.USE_NZB() + lazylibrarian.USE_TOR() + lazylibrarian.USE_RSS()
    nprov += lazylibrarian.USE_DIRECT() + lazylibrarian.USE_IRC()
    logger.debug('Searching %s provider%s (%s) for %s' % (nprov, plural(nprov), cat, searchterm))

    if lazylibrarian.USE_NZB():
        resultlist, nprov = IterateOverNewzNabSites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.USE_TOR():
        resultlist, nprov = IterateOverTorrentSites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.USE_DIRECT():
        resultlist, nprov = IterateOverDirectSites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.USE_IRC():
        resultlist, nprov = IterateOverIRCSites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.USE_RSS():
        resultlist, nprov, dltypes = IterateOverRSSSites()
        if nprov and dltypes != 'C':
            results += resultlist

    # reprocess to get consistent results
    searchresults = []
    for item in results:
        provider = ''
        title = ''
        url = ''
        size = ''
        date = ''
        mode = ''
        if 'dispname' in item:
            provider = item['dispname']
        elif 'nzbprov' in item:
            provider = item['nzbprov']
        elif 'tor_prov' in item:
            provider = item['tor_prov']
        elif 'rss_prov' in item:
            provider = item['rss_prov']
        if 'nzbtitle' in item:
            title = item['nzbtitle']
        if 'nzburl' in item:
            url = item['nzburl']
        if 'nzbsize' in item:
            size = item['nzbsize']
        if 'nzbdate' in item:
            date = item['nzbdate']
        if 'nzbmode' in item:
            mode = item['nzbmode']
        if 'tor_title' in item:
            title = item['tor_title']
        if 'tor_url' in item:
            url = item['tor_url']
        if 'tor_size' in item:
            size = item['tor_size']
        if 'tor_date' in item:
            date = item['tor_date']
        if 'tor_type' in item:
            mode = item['tor_type']

        if title and provider and mode and url:
            # Not all results have a date or a size
            if not size:
                size = '1000'
            if date:
                date = dateFormat(date)
            url = url.encode('utf-8')
            if mode == 'torznab':
                # noinspection PyTypeChecker
                if url.startswith(b'magnet'):
                    mode = 'magnet'

            # calculate match percentage - torrents might have words_with_underscore_separator
            part_title = title.replace('_', ' ').split('(')[0]
            # strip any trailing words that are just digits
            while part_title and part_title[-1].isdigit():
                part_title = part_title[:-1].strip()
            searchmatch = searchterm.replace('+', ' ')
            score = fuzz.token_set_ratio(searchmatch, part_title)

            # lose a point for each extra word in the title so we get the closest match
            words = len(getList(searchterm))
            words -= len(getList(title))
            score -= abs(words)
            rejected = False
            if score >= 40:  # ignore wildly wrong results?

                maxsize = check_int(lazylibrarian.CONFIG['REJECT_MAXCOMIC'], 0)
                minsize = check_int(lazylibrarian.CONFIG['REJECT_MINCOMIC'], 0)
                filetypes = getList(lazylibrarian.CONFIG['COMIC_TYPE'])
                banwords = getList(lazylibrarian.CONFIG['REJECT_COMIC'], ',')
                size_mb = check_int(size, 1000)
                size_mb = round(float(size_mb) / 1048576, 2)

                if not rejected and maxsize and size_mb > maxsize:
                    rejected = True
                    logger.debug("Rejecting %s, too large (%sMb)" % (title, size_mb))

                if not rejected and minsize and size_mb < minsize:
                    rejected = True
                    logger.debug("Rejecting %s, too small (%sMb)" % (title, size_mb))

                if not rejected:
                    resultTitle = unaccented(replace_all(title, dictrepl), only_ascii=False).strip()
                    words = getList(resultTitle.lower())
                    for word in words:
                        if word in banwords:
                            logger.debug("Rejecting %s, contains %s" % (title, word))
                            rejected = True
                            break
                    if not rejected and filetypes:
                        for word in filetypes:
                            if word in words:
                                score += 1
                if not rejected:
                    result = {'score': score, 'title': title, 'provider': provider, 'size': size_mb, 'date': date,
                              'url': url, 'mode': mode, 'bookid': comicid}
                    searchresults.append(result)

    logger.debug('Found %s %s results for %s' % (len(searchresults), cat, searchterm))
    return searchresults


def cron_search_comics():
    if 'SEARCHALLCOMICS' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_comics()
    else:
        logger.debug("SEARCHALLCOMICS is already running")


def search_comics(comicid=None):
    # noinspection PyBroadException
    try:
        threadname = threading.currentThread().name
        if "Thread-" in threadname:
            if not comicid:
                threading.currentThread().name = "SEARCHALLCOMICS"
            else:
                threading.currentThread().name = "SEARCHCOMIC"

        myDB = database.DBConnection()
        cmd = "SELECT ComicID,Title, aka from comics WHERE Status='Active'"
        count = 0
        if comicid:
            # single comic search
            cmd += ' AND ComicID=?'
            comics = myDB.select(cmd, (comicid,))
        else:
            # search for all active comics
            comics = myDB.select(cmd)
            logger.debug("Found %s active comics" % len(comics))

        for comic in comics:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLCOMICS":
                logger.debug("Aborting %s" % threadname)
                break
            comicid = comic['ComicID']
            aka = getList(comic['aka'])
            id_list = comicid
            if len(aka):
                id_list = id_list + ', ' + ', '.join(aka)
            found = 0
            notfound = 0
            foundissues = {}
            res = searchItem(comicid)
            for item in res:
                match = None
                if item['score'] >= 85:
                    if comic['ComicID'].startswith('CV'):
                        match = cv_identify(item['title'])
                    elif comic['ComicID'].startswith('CX'):
                        match = cx_identify(item['title'])
                if match:
                    if match[3]['seriesid'] == comicid or match[3]['seriesid'] in aka:
                        found += 1
                        if match[4]:
                            if match[4] not in foundissues:
                                foundissues[match[4]] = item
                    else:
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                            logger.debug("No match (%s) want %s: %s" %
                                         (match[3]['seriesid'], id_list, item['title']))
                        notfound += 1
                else:
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                        logger.debug("No match [%s%%] %s" % (item['score'], item['title']))
                    notfound += 1

            total = len(foundissues)
            haveissues = myDB.select("SELECT IssueID from comicissues WHERE ComicID=?", (comicid,))
            have = []
            located = []
            for item in haveissues:
                have.append(int(item['IssueID']))
            for item in foundissues.keys():
                located.append(item)
            for item in located:
                if item in have:
                    foundissues.pop(item)

            logger.debug("Found %s results, %s match, %s fail, %s distinct, Have %s, Missing %s" %
                         (len(res), found, notfound, total, sorted(have),
                          sorted(foundissues.keys())))

            for issue in foundissues:
                item = foundissues[issue]
                match = myDB.match('SELECT Status from wanted WHERE NZBtitle=? and NZBprov=?',
                                   (item['title'], item['provider']))
                if match:
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_searching:
                        logger.debug('%s is already marked %s' % (item['title'], match['Status']))
                else:
                    bookid = "%s_%s" % (item['bookid'], issue)
                    controlValueDict = {
                        "NZBtitle": item['title'],
                        "NZBprov": item['provider']
                    }
                    newValueDict = {
                        "NZBurl": item['url'],
                        "BookID": bookid,
                        "NZBdate": item['date'],
                        "AuxInfo": "comic",
                        "Status": "Matched",
                        "NZBsize": item['size'],
                        "NZBmode": item['mode']
                    }
                    myDB.upsert("wanted", newValueDict, controlValueDict)

                    if item['mode'] in ["torznab", "torrent", "magnet"]:
                        snatch, res = TORDownloadMethod(
                            bookid,
                            item['title'],
                            item['url'],
                            'comic')
                    elif item['mode'] == 'direct':
                        snatch, res = DirectDownloadMethod(
                            bookid,
                            item['title'],
                            item['url'],
                            'comic',
                            item['provider'])
                    elif item['mode'] == 'nzb':
                        snatch, res = NZBDownloadMethod(
                            bookid,
                            item['title'],
                            item['url'],
                            'comic')
                    else:
                        res = 'Unhandled mode [%s] for %s' % (item['mode'], item["url"])
                        logger.error(res)
                        snatch = 0

                    if snatch:
                        count += 1
                        logger.info('Downloading %s from %s' % (item['title'], item["provider"]))
                        myDB.action('UPDATE wanted SET nzbdate=? WHERE NZBurl=?', (now(), item["url"]))
                        custom_notify_snatch("%s %s" % (bookid, item['url']))
                        notify_snatch("Comic %s from %s at %s" %
                                      (unaccented(item['title'], only_ascii=False),
                                       dispName(item["provider"]), now()))
                        scheduleJob(action='Start', target='PostProcessor')
                    else:
                        myDB.action('UPDATE wanted SET status="Failed",DLResult=? WHERE NZBurl=?',
                                    (res, item["url"]))

            time.sleep(check_int(lazylibrarian.CONFIG['SEARCH_RATELIMIT'], 0))
        logger.info("ComicSearch for Wanted items complete, found %s comic%s" % (count, plural(count)))
        myDB.upsert("jobs", {"LastRun": time.time()}, {"Name": threading.currentThread().name})
    except Exception:
        logger.error('Unhandled exception in search_comics: %s' % traceback.format_exc())
    finally:
        threading.currentThread().name = "WEBSERVER"
