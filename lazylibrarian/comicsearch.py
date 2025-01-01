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
import logging
import lazylibrarian

from lazylibrarian.config2 import CONFIG
from lazylibrarian import database
from lazylibrarian.formatter import get_list, plural, date_format, unaccented, replace_all, check_int, \
    now, thread_name
from lazylibrarian.providers import iterate_over_rss_sites, iterate_over_torrent_sites, iterate_over_znab_sites, \
    iterate_over_direct_sites, iterate_over_irc_sites
from lazylibrarian.scheduling import schedule_job, SchedulerCommand
from lazylibrarian.comicid import cv_identify, cx_identify
from lazylibrarian.notifiers import notify_snatch, custom_notify_snatch
from lazylibrarian.downloadmethods import nzb_dl_method, tor_dl_method, direct_dl_method
from rapidfuzz import fuzz


# '0': '', '1': '', '2': '', '3': '', '4': '', '5': '', '6': '', '7': '', '8': '', '9': '',
dictrepl = {'...': '', '.': ' ', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', ' + ': ' ', '"': '',
            ',': ' ', '*': '', '(': '', ')': '', '[': '', ']': '', '#': '', '\'': '',
            ':': '', '!': '', '-': ' ', r'\s\s': ' '}


def search_item(comicid=None):
    """
    Call all active search providers to search for comic by id
    return a list of results, each entry in list containing percentage_match, title, provider, size, url
    """
    logger = logging.getLogger(__name__)
    results = []

    if not comicid:
        return results

    db = database.DBConnection()
    try:
        cmd = "SELECT Title,SearchTerm from comics WHERE Status='Active' and ComicID=?"
        match = db.match(cmd, (comicid,))
    finally:
        db.close()
    if not match:
        logger.debug("No comic match for %s" % comicid)
        return results

    cat = 'comic'
    book = {'library': cat, 'bookid': comicid, 'bookName': match['Title']}
    searchterm = match['SearchTerm']
    if not searchterm:
        searchterm = match['Title']
    book['searchterm'] = searchterm.replace('+', ' ')

    nprov = CONFIG.total_active_providers()
    logger.debug('Searching %s %s (%s) for %s' % (nprov, plural(nprov, "provider"), cat, searchterm))

    if CONFIG.use_nzb():
        resultlist, nprov = iterate_over_znab_sites(book, cat)
        if nprov:
            results += resultlist
    if CONFIG.use_tor():
        resultlist, nprov = iterate_over_torrent_sites(book, cat)
        if nprov:
            results += resultlist
    if CONFIG.use_direct():
        resultlist, nprov = iterate_over_direct_sites(book, cat)
        if nprov:
            results += resultlist
    if CONFIG.use_irc():
        resultlist, nprov = iterate_over_irc_sites(book, cat)
        if nprov:
            results += resultlist
    if CONFIG.use_rss():
        resultlist, nprov, dltypes = iterate_over_rss_sites()
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
                date = date_format(date, context=title)
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

            # lose a point for each extra word in the title, so we get the closest match
            words = len(get_list(searchterm))
            words -= len(get_list(title))
            score -= abs(words)
            rejected = False
            if score >= 40:  # ignore wildly wrong results?

                maxsize = CONFIG.get_int('REJECT_MAXCOMIC')
                minsize = CONFIG.get_int('REJECT_MINCOMIC')
                filetypes = get_list(CONFIG['COMIC_TYPE'])
                banwords = CONFIG.get_csv('REJECT_COMIC')
                size_mb = check_int(size, 1000)
                size_mb = round(float(size_mb) / 1048576, 2)

                if not rejected and maxsize and size_mb > maxsize:
                    rejected = True
                    logger.debug("Rejecting %s, too large (%sMb)" % (title, size_mb))

                if not rejected and minsize and size_mb < minsize:
                    rejected = True
                    logger.debug("Rejecting %s, too small (%sMb)" % (title, size_mb))

                if not rejected:
                    result_title = unaccented(replace_all(title, dictrepl), only_ascii=False).strip()
                    words = get_list(result_title.lower())
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
    logger = logging.getLogger(__name__)
    if 'SEARCHALLCOMICS' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_comics()
    else:
        logger.debug("SEARCHALLCOMICS is already running")


def search_comics(comicid=None):
    logger = logging.getLogger(__name__)
    loggersearching = logging.getLogger('special.searching')
    threadname = thread_name()
    if "Thread" in threadname:
        if not comicid:
            thread_name("SEARCHALLCOMICS")
        else:
            thread_name("SEARCHCOMIC")

    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        cmd = "SELECT ComicID,Title, aka from comics WHERE Status='Active'"
        if comicid:
            # single comic search
            cmd += " AND ComicID=?"
            comics = db.select(cmd, (comicid,))
        else:
            # search for all active comics
            comics = db.select(cmd)
            logger.debug("Found %s active comics" % len(comics))

        for comic in comics:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLCOMICS":
                logger.debug("Aborting %s" % threadname)
                break
            comicid = comic['ComicID']
            aka = get_list(comic['aka'])
            id_list = comicid
            if len(aka):
                id_list = id_list + ', ' + ', '.join(aka)
            found = 0
            notfound = 0
            foundissues = {}
            res = search_item(comicid)
            for item in res:
                match = None
                if item['score'] >= 85:
                    loggersearching.debug("Trying to match %s" % item['title'])
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
                        loggersearching.debug("No match (%s) want %s: %s" % (match[3]['seriesid'],
                                                                             id_list, item['title']))
                        notfound += 1
                else:
                    loggersearching.debug("No match [%s%%] %s" % (item['score'], item['title']))
                    notfound += 1

            total = len(foundissues)
            haveissues = db.select("SELECT IssueID from comicissues WHERE ComicID=?", (comicid,))
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
            threading.Thread(target=download_comiclist, name='DL-COMICLIST', args=[foundissues]).start()

            time.sleep(CONFIG.get_int('SEARCH_RATELIMIT'))
        logger.info("ComicSearch for Wanted items complete")
        db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
    except Exception:
        logger.error('Unhandled exception in search_comics: %s' % traceback.format_exc())
    finally:
        db.close()
        thread_name("WEBSERVER")


def download_comiclist(foundissues):
    logger = logging.getLogger(__name__)
    loggesearching = logging.getLogger('special.searching')
    db = database.DBConnection()
    try:
        snatched = 0
        for issue in foundissues:
            item = foundissues[issue]
            match = db.match('SELECT Status from wanted WHERE NZBtitle=? and NZBprov=?',
                             (item['title'], item['provider']))
            if match:
                loggesearching.debug('%s is already marked %s' % (item['title'], match['Status']))
            else:
                bookid = "%s_%s" % (item['bookid'], issue)
                control_value_dict = {
                    "NZBtitle": item['title'],
                    "NZBprov": item['provider']
                }
                new_value_dict = {
                    "NZBurl": item['url'],
                    "BookID": bookid,
                    "NZBdate": item['date'],
                    "AuxInfo": "comic",
                    "Status": "Matched",
                    "NZBsize": item['size'],
                    "NZBmode": item['mode']
                }
                db.upsert("wanted", new_value_dict, control_value_dict)

                if item['mode'] in ["torznab", "torrent", "magnet"]:
                    snatch, res = tor_dl_method(
                        bookid,
                        item['title'],
                        item['url'],
                        'comic',
                        provider=item['provider'])
                elif item['mode'] == 'direct':
                    snatch, res = direct_dl_method(
                        bookid,
                        item['title'],
                        item['url'],
                        'comic',
                        item['provider'])
                elif item['mode'] == 'nzb':
                    snatch, res = nzb_dl_method(
                        bookid,
                        item['title'],
                        item['url'],
                        'comic')
                else:
                    res = 'Unhandled mode [%s] for %s' % (item['mode'], item["url"])
                    logger.error(res)
                    snatch = 0

                if snatch:
                    snatched += 1
                    logger.info('Downloading %s from %s' % (item['title'], item["provider"]))
                    db.action("UPDATE wanted SET nzbdate=? WHERE NZBurl=?", (now(), item['url']))
                    custom_notify_snatch("%s %s" % (bookid, item['url']))
                    notify_snatch("Comic %s from %s at %s" %
                                  (unaccented(item['title'], only_ascii=False),
                                   CONFIG.disp_name(item["provider"]), now()))
                else:
                    db.action("UPDATE wanted SET status='Failed',DLResult=? WHERE NZBurl=?",
                              (res, item["url"]))
    finally:
        db.close()
    if snatched:
        schedule_job(SchedulerCommand.START, target='PostProcessor')
