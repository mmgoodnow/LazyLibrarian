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


import datetime
import logging
import re
import threading
import time
import traceback

import lazylibrarian
from lazylibrarian import database
from lazylibrarian.config2 import CONFIG
from lazylibrarian.downloadmethods import nzb_dl_method, tor_dl_method, direct_dl_method
from lazylibrarian.formatter import plural, now, replace_all, unaccented, \
    nzbdate2format, get_list, month2num, datecompare, check_int, check_year, age, thread_name
from lazylibrarian.notifiers import notify_snatch, custom_notify_snatch
from lazylibrarian.providers import iterate_over_znab_sites, iterate_over_torrent_sites, iterate_over_rss_sites, \
    iterate_over_direct_sites, iterate_over_irc_sites
from lazylibrarian.scheduling import schedule_job, SchedulerCommand
from lazylibrarian.telemetry import TELEMETRY


def cron_search_magazines():
    if 'SEARCHALLMAG' not in [n.name for n in [t for t in threading.enumerate()]]:
        search_magazines()


def search_magazines(mags=None, reset=False, backissues=False):
    # produce a list of magazines to search for, then search all enabled providers
    TELEMETRY.record_usage_data('Search/Magazine')
    logger = logging.getLogger(__name__)
    loggersearching = logging.getLogger('special.searching')
    threadname = thread_name()
    if "Thread" in threadname:
        if not mags:
            thread_name("SEARCHALLMAG")
            threadname = "SEARCHALLMAG"
        else:
            thread_name("SEARCHMAG")
    db = database.DBConnection()
    # noinspection PyBroadException
    try:

        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        searchlist = []

        if not mags:  # backlog search
            searchmags = db.select("SELECT Title,Regex,DateType,LastAcquired,IssueDate from magazines "
                                   "WHERE Status='Active'")
        else:
            searchmags = []
            for magazine in mags:
                searchmags_temp = db.select("SELECT Title,Regex,DateType,LastAcquired,IssueDate from magazines "
                                            "WHERE Title=? AND Status='Active'", (magazine['bookid'],))
                for terms in searchmags_temp:
                    searchmags.append(terms)

        if len(searchmags) == 0:
            logger.debug("No magazines to search for")
            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            thread_name("WEBSERVER")
            return

        logger.info(f"Searching for {len(searchmags)} {plural(len(searchmags), 'magazine')}")

        for searchmag in searchmags:
            bookid = searchmag['Title']
            searchterms = get_list(searchmag['Regex'], ',')
            datetype = searchmag['DateType']
            if not datetype:
                datetype = ''

            if not searchterms:
                dic = {'...': '', ' & ': ' and ', ' + ': ' plus ', ' = ': ' ', '?': '', '$': 's',
                       '"': '', ',': '', '*': ''}
                searchterm = replace_all(searchmag['Title'], dic)
                searchterms = [re.sub(r'[.\-/]', ' ', searchterm)]  # single item in a list

            for searchterm in searchterms:
                searchlist.append({"bookid": bookid, "searchterm": searchterm, "datetype": datetype,
                                   "library": 'magazine'})

        if not searchlist:
            logger.warning('There is nothing to search for.  Mark some magazines as active.')

        for book in searchlist:
            if lazylibrarian.STOPTHREADS and threadname == "SEARCHALLMAG":
                logger.debug(f"Aborting {threadname}")
                break

            resultlist = []

            if CONFIG.use_nzb():
                resultlist, nproviders = iterate_over_znab_sites(book, 'mag')
                if not nproviders:
                    # don't nag. Show warning message no more than every 20 mins
                    timenow = int(time.time())
                    if check_int(lazylibrarian.TIMERS['NO_NZB_MSG'], 0) + 1200 < timenow:
                        logger.warning('No nzb providers are available. Check config and blocklist')
                        lazylibrarian.TIMERS['NO_NZB_MSG'] = timenow
                else:
                    # prefer larger nzb over smaller ones which may be par2 repair files?
                    resultlist = sorted(resultlist, key=lambda d: check_int(d['nzbsize'], 0), reverse=True)

            if CONFIG.use_direct():
                dir_resultlist, nproviders = iterate_over_direct_sites(book, 'mag')
                if not nproviders:
                    # don't nag. Show warning message no more than every 20 mins
                    timenow = int(time.time())
                    if check_int(lazylibrarian.TIMERS['NO_DIRECT_MSG'], 0) + 1200 < timenow:
                        logger.warning('No direct providers are available. Check config and blocklist')
                        lazylibrarian.TIMERS['NO_DIRECT_MSG'] = timenow

                if dir_resultlist:
                    for item in dir_resultlist:  # reformat the results so they look like nzbs
                        resultlist.append({
                            'bookid': item['bookid'],
                            'nzbprov': item['tor_prov'],
                            'nzbtitle': item['tor_title'],
                            'nzburl': item['tor_url'],
                            'nzbdate': 'Fri, 01 Jan 1970 00:00:00 +0100',  # fake date as none returned
                            'nzbsize': item['tor_size'],
                            'nzbmode': 'direct'
                        })

            if CONFIG.use_irc():
                irc_resultlist, nproviders = iterate_over_irc_sites(book, 'mag')
                if not nproviders:
                    # don't nag. Show warning message no more than every 20 mins
                    timenow = int(time.time())
                    if check_int(lazylibrarian.TIMERS['NO_IRC_MSG'], 0) + 1200 < timenow:
                        logger.warning('No irc providers are available. Check config and blocklist')
                        lazylibrarian.TIMERS['NO_IRC_MSG'] = timenow

                if irc_resultlist:
                    for item in irc_resultlist:  # reformat the results so they look like nzbs
                        resultlist.append({
                            'bookid': item['bookid'],
                            'nzbprov': item['tor_prov'],
                            'nzbtitle': item['tor_title'],
                            'nzburl': item['tor_url'],
                            'nzbdate': 'Fri, 01 Jan 1970 00:00:00 +0100',  # fake date as none returned
                            'nzbsize': item['tor_size'],
                            'nzbmode': 'irc'
                        })

            if CONFIG.use_tor():
                tor_resultlist, nproviders = iterate_over_torrent_sites(book, 'mag')
                if not nproviders:
                    # don't nag. Show warning message no more than every 20 mins
                    timenow = int(time.time())
                    if check_int(lazylibrarian.TIMERS['NO_TOR_MSG'], 0) + 1200 < timenow:
                        logger.warning('No tor providers are available. Check config and blocklist')
                        lazylibrarian.TIMERS['NO_TOR_MSG'] = timenow

                if tor_resultlist:
                    for item in tor_resultlist:  # reformat the torrent results so they look like nzbs
                        resultlist.append({
                            'bookid': item['bookid'],
                            'nzbprov': item['tor_prov'],
                            'nzbtitle': item['tor_title'],
                            'nzburl': item['tor_url'],
                            'nzbdate': 'Fri, 01 Jan 1970 00:00:00 +0100',  # fake date as none returned from torrents
                            'nzbsize': item['tor_size'],
                            'nzbmode': 'torrent'
                        })

            if CONFIG.use_rss():
                rss_resultlist, nproviders, dltypes = iterate_over_rss_sites()
                if not nproviders or 'M' not in dltypes:
                    # don't nag. Show warning message no more than every 20 mins
                    timenow = int(time.time())
                    if check_int(lazylibrarian.TIMERS['NO_RSS_MSG'], 0) + 1200 < timenow:
                        logger.warning('No rss providers are available. Check config and blocklist')
                        lazylibrarian.TIMERS['NO_RSS_MSG'] = timenow

                if rss_resultlist:
                    for item in rss_resultlist:  # reformat the rss results so they look like nzbs
                        if 'M' in item['types']:
                            resultlist.append({
                                'bookid': book['bookid'],
                                'nzbprov': item['tor_prov'],
                                'nzbtitle': item['tor_title'],
                                'nzburl': item['tor_url'],
                                'nzbdate': item['tor_date'],  # may be fake date as none returned from rss torrents
                                'nzbsize': item['tor_size'],
                                'nzbmode': item['tor_type']
                            })

            if not resultlist:
                logger.debug(f"No results for magazine {book['searchterm']}")
            else:
                bad_name = 0
                bad_date = 0
                old_date = 0
                rejects = 0
                total_nzbs = 0
                new_date = 0
                maglist = []
                issues = []
                bookid = ''
                for nzb in resultlist:
                    total_nzbs += 1
                    bookid = nzb['bookid']
                    nzbtitle = nzb['nzbtitle']
                    nzbtitle = nzbtitle.replace('"', '').replace("'", "")  # suppress " in titles
                    nzburl = nzb['nzburl']
                    nzbprov = nzb['nzbprov']
                    nzbdate_temp = nzb['nzbdate']
                    nzbsize_temp = nzb['nzbsize']
                    nzbsize_temp = check_int(nzbsize_temp, 1000)  # not all torrents returned by torznab have a size
                    nzbsize = round(float(nzbsize_temp) / 1048576, 2)
                    nzbdate = nzbdate2format(nzbdate_temp)
                    nzbmode = nzb['nzbmode']

                    # Need to make sure that substrings of magazine titles don't get found
                    # (e.g. Maxim USA will find Maximum PC USA) so split into "words"
                    dic = {'.': ' ', '-': ' ', '/': ' ', '_': ' ', '(': '', ')': '', '[': ' ', ']': ' ', '#': '# '}
                    nzbtitle_formatted = replace_all(nzbtitle, dic)

                    # remove extra spaces if they're in a row
                    nzbtitle_formatted = " ".join(nzbtitle_formatted.split())
                    nzbtitle_exploded = nzbtitle_formatted.split()

                    results = db.match('SELECT * from magazines WHERE Title=? COLLATE NOCASE', (bookid,))
                    if not results:
                        logger.debug(f'Magazine [{nzbtitle}] does not match search term [{bookid}].')
                        bad_name += 1
                    else:
                        rejected = False
                        maxsize = CONFIG.get_int('REJECT_MAGSIZE')
                        if maxsize and nzbsize > maxsize:
                            logger.debug(f"Rejecting {nzbtitle}, too large ({nzbsize}Mb)")
                            rejected = True

                        if not rejected:
                            minsize = CONFIG.get_int('REJECT_MAGMIN')
                            if minsize and nzbsize < minsize:
                                logger.debug(f"Rejecting {nzbtitle}, too small ({nzbsize}Mb)")
                                rejected = True

                        if not rejected:
                            bookid_exploded = replace_all(bookid, dic).split()

                            # Check nzb has magazine title and a date/issue nr
                            # eg The MagPI July 2015
                            if len(nzbtitle_exploded) > len(bookid_exploded):
                                # needs to be longer as it has to include a date
                                # check all the words in the mag title are in the nzbtitle
                                rejected = False
                                wlist = []
                                for word in nzbtitle_exploded:
                                    if word == '&':
                                        word = 'and'
                                    elif word == '+':
                                        word = 'and'
                                    wlist.append(word.lower())
                                for word in bookid_exploded:
                                    if word == '&':
                                        word = 'and'
                                    elif word == '+':
                                        word = 'and'
                                    if word.lower() not in wlist:
                                        logger.debug(f"Rejecting {nzbtitle}, missing [{word}]")
                                        rejected = True
                                        break

                                if rejected:
                                    logger.debug(
                                        f"Magazine title match failed {bookid} for {nzbtitle_formatted}")
                                else:
                                    logger.debug(
                                        f"Magazine title matched {bookid} for {nzbtitle_formatted}")
                            else:
                                logger.debug(f"Magazine name too short ({len(nzbtitle_exploded)})")
                                rejected = True

                        if not rejected and CONFIG.get_bool('BLACKLIST_FAILED'):
                            blocked = db.match("SELECT * from wanted WHERE NZBurl=? and Status='Failed'", (nzburl,))
                            if blocked:
                                logger.debug(f"Rejecting {nzbtitle_formatted}, blacklisted at {blocked['NZBprov']}")
                                rejected = True

                        if not rejected and CONFIG.get_bool('BLACKLIST_PROCESSED'):
                            blocked = db.match('SELECT * from wanted WHERE NZBurl=?', (nzburl,))
                            if blocked:
                                logger.debug(f"Rejecting {nzbtitle_formatted}, blacklisted at {blocked['NZBprov']}")
                                rejected = True

                        if not rejected:
                            reject_list = get_list(results['Reject'])
                            reject_list += get_list(CONFIG['REJECT_MAGS'], ',')
                            lower_title = unaccented(nzbtitle_formatted, only_ascii=False).lower().split()
                            lower_bookid = unaccented(bookid, only_ascii=False).lower().split()
                            if reject_list:
                                loggersearching.debug(f'Reject: {reject_list}')
                                loggersearching.debug(f'Title: {lower_title}')
                                loggersearching.debug(f'Bookid: {lower_bookid}')
                            for word in reject_list:
                                word = unaccented(word).lower()
                                if word in lower_title and word not in lower_bookid:
                                    rejected = True
                                    logger.debug(f"Rejecting {nzbtitle_formatted}, contains {word}")
                                    break
                            if not rejected:
                                reject_list = get_list(results['Reject'])
                                if '*' in reject_list:  # strict rejection mode, no extraneous words
                                    nouns = get_list(CONFIG['ISSUE_NOUNS'])
                                    nouns.extend(get_list(CONFIG['VOLUME_NOUNS']))
                                    nouns.extend(get_list(CONFIG['MAG_NOUNS']))
                                    nouns.extend(get_list(CONFIG['MAG_TYPE']))
                                    for word in lower_title:
                                        if word not in lower_bookid and word not in nouns:
                                            valid = False
                                            for f in range(1, 13):
                                                if (word in lazylibrarian.MONTHNAMES[0][f] or
                                                        unaccented(word).lower() in lazylibrarian.MONTHNAMES[1][f]):
                                                    valid = True
                                                    break
                                            if not valid:
                                                rejected = True
                                                logger.debug(
                                                    f"Rejecting {nzbtitle_formatted}, strict, contains {word}")
                                                break
                        if rejected:
                            rejects += 1
                        else:
                            datetype = book['datetype']
                            issuenum_type, issuedate, year = get_issue_date(nzbtitle_exploded, datetype=datetype)
                            if issuenum_type:
                                logger.debug(
                                    f'Issue {issuedate} (datestyle {issuenum_type}) for {nzbtitle_formatted},'
                                    f' {datetype}')
                                datetype_ok = True

                                if datetype:
                                    # check all wanted parts are in the result
                                    # Day Month Year Vol Iss (MM needs two months)

                                    if 'M' in datetype and issuenum_type not in [1, 2, 3, 4, 5, 6, 7, 12]:
                                        datetype_ok = False
                                    if 'D' in datetype and issuenum_type not in [3, 5, 6]:
                                        datetype_ok = False
                                    if 'MM' in datetype and issuenum_type not in [1]:  # bi monthly
                                        datetype_ok = False
                                    if 'V' in datetype and issuenum_type not in [2, 8, 9, 10, 11, 12, 13, 14, 17, 18]:
                                        datetype_ok = False
                                    if 'I' in datetype and issuenum_type not in [2, 10, 11, 12, 13, 14, 16, 17, 18]:
                                        datetype_ok = False
                                    if 'Y' in datetype and issuenum_type not in [1, 2, 3, 4, 5, 6, 7, 8, 10,
                                                                                 12, 13, 15, 16, 18]:
                                        datetype_ok = False
                            else:
                                datetype_ok = False
                                logger.debug(
                                    f'Magazine {nzbtitle_formatted} not in a recognised date format [{datetype}]')
                                bad_date += 1
                                # allow issues with good name but bad date to be included
                                # so user can manually select them, incl those with issue numbers
                                issuedate = "1970-01-01"  # provide a fake date for bad-date issues

                            # wanted issues go into wanted table marked "Wanted"
                            #  the rest into pastissues table marked "Skipped" or "Have"
                            insert_table = "pastissues"
                            comp_date = 0
                            if datetype_ok:
                                if issuedate.isdigit() and 'I' in datetype:
                                    issuedate = issuedate.zfill(4)
                                    if 'Y' in datetype:
                                        issuedate = year + issuedate

                                control_date = results['IssueDate']
                                logger.debug(f"Control date: [{control_date}]")
                                if not control_date:  # we haven't got any copies of this magazine yet
                                    # get a rough time just over MAX_AGE days ago to compare to, in format yyyy-mm-dd
                                    # could perhaps calc differently for weekly, biweekly etc.
                                    # For magazines with only an issue number use zero as we can't tell age

                                    if issuedate.isdigit():
                                        logger.debug(f'Magazine comparing issue numbers ({issuedate})')
                                        control_date = 0
                                    elif re.match(r'\d+-\d\d-\d\d', str(issuedate)):
                                        start_time = time.time()
                                        start_time -= CONFIG.get_int('MAG_AGE') * 24 * 60 * 60
                                        if start_time < 0:  # limit of unixtime (1st Jan 1970)
                                            start_time = 0
                                        control_date = time.strftime("%Y-%m-%d", time.localtime(start_time))
                                        logger.debug(f'Magazine date comparing to {control_date}')
                                    else:
                                        logger.debug(f'Magazine unable to find comparison type [{issuedate}]')
                                        control_date = 0

                                if str(control_date).isdigit() and str(issuedate).isdigit():
                                    if not control_date:
                                        comp_date = CONFIG.get_int('MAG_AGE') - age(nzbdate)
                                    else:
                                        comp_date = int(issuedate) - int(control_date)
                                elif re.match(r'\d+-\d\d-\d\d', str(control_date)) and \
                                        re.match(r'\d+-\d\d-\d\d', str(issuedate)):
                                    # only grab a copy if it's newer than the most recent we have,
                                    # or newer than a month ago if we have none
                                    # unless backissues is True
                                    comp_date = datecompare(issuedate, control_date)
                                elif backissues:
                                    comp_date = 1
                                else:
                                    # invalid comparison of date and issue number
                                    comp_date = 0
                                    if re.match(r'\d+-\d\d-\d\d', str(control_date)):
                                        if issuenum_type > 9 and year:
                                            # we assumed it was an issue number, but it could be a date
                                            year = check_int(year, 0)
                                            if issuenum_type in [10, 12, 13]:
                                                issuedate = int(issuedate[:4])
                                            issuenum = check_int(issuedate, 0)
                                            if year and 1 <= issuenum <= 12:
                                                issuedate = "%04d-%02d-01" % (year, issuenum)
                                                comp_date = datecompare(issuedate, control_date)
                                        if not comp_date:
                                            logger.debug(f'Magazine {nzbtitle_formatted} failed: Expecting a date')
                                    else:
                                        logger.debug(f'Magazine {nzbtitle_formatted} failed: Expecting issue number')
                                    if not comp_date:
                                        bad_date += 1
                                        issuedate = "1970-01-01"

                            if issuedate == "1970-01-01":
                                logger.debug(f'This issue of {nzbtitle_formatted} is unknown age; skipping.')
                            elif not datetype_ok:
                                logger.debug(f'This issue of {nzbtitle_formatted} not in a wanted date format.')
                            elif comp_date > 0:
                                # keep track of what we're going to download, so we don't download dupes
                                new_date += 1
                                issue = f"{bookid},{issuedate}"
                                if issue not in issues:
                                    maglist.append({
                                        'bookid': bookid,
                                        'nzbprov': nzbprov,
                                        'nzbtitle': nzbtitle,
                                        'nzburl': nzburl,
                                        'nzbmode': nzbmode
                                    })
                                    logger.debug(f'This issue of {nzbtitle_formatted} is new, downloading')
                                    issues.append(issue)
                                    logger.debug(f'Magazine request number {len(issues)}')
                                    loggersearching.debug(str(issues))
                                    insert_table = "wanted"
                                    nzbdate = now()  # when we asked for it
                                else:
                                    logger.debug(f'This issue of {issue} is already flagged for download; skipping')
                                    continue
                            else:
                                loggersearching.debug(f'This issue of {nzbtitle_formatted} is old; skipping.')
                                old_date += 1

                            mag_entry = db.match(f'SELECT * from issues WHERE title=? and issuedate=?',
                                                 (bookid, issuedate))
                            if mag_entry:
                                logger.info(f'This issue of {nzbtitle_formatted} is already downloaded; skipping')
                                continue
                            # store only the _new_ matching results
                            #  Don't add a new entry if this issue has been found on an earlier search
                            #  and status has been user-set ( we only delete the "Skipped" ones )
                            #  In "wanted" table it might be already snatched/downloading/processing
                            mag_entry = db.match(f'SELECT Status from {insert_table} WHERE NZBtitle=? and NZBprov=?',
                                                 (nzbtitle, nzbprov))
                            if mag_entry and insert_table != 'wanted':
                                logger.info(
                                    f"{nzbtitle} is already in {insert_table} marked {mag_entry['Status']}; skipping")
                                continue
                            control_value_dict = {
                                "NZBtitle": nzbtitle,
                                "NZBprov": nzbprov
                            }
                            new_value_dict = {
                                "NZBurl": nzburl,
                                "BookID": bookid,
                                "NZBdate": nzbdate,
                                "AuxInfo": issuedate,
                                "Status": "Wanted",
                                "NZBsize": nzbsize,
                                "NZBmode": nzbmode
                            }
                            if insert_table == 'pastissues':
                                # try to mark ones we've already got
                                match = db.match("SELECT * from issues WHERE Title=? AND IssueDate=?",
                                                 (bookid, issuedate))
                                if match:
                                    new_value_dict["Status"] = "Have"
                                else:
                                    new_value_dict["Status"] = "Skipped"
                                new_value_dict["Added"] = int(time.time())
                            db.upsert(insert_table, new_value_dict, control_value_dict)
                            logger.info(f"Added {nzbtitle} to {insert_table} marked {new_value_dict['Status']}")

                msg = f"Found {total_nzbs} {plural(total_nzbs, 'result')} for {bookid}. {new_date} new,"
                msg += f' {old_date} old, {bad_date} fail date, {bad_name} fail name,'
                msg += f' {rejects} rejected: {len(maglist)} to download'
                logger.info(msg)

                threading.Thread(target=download_maglist, name='DL-MAGLIST', args=[maglist, 'pastissues']).start()

            time.sleep(CONFIG.get_int('SEARCH_RATELIMIT'))

        logger.info("Search for magazines complete")
        if reset:
            schedule_job(action=SchedulerCommand.RESTART, target='search_magazines')

    except Exception:
        logger.error(f'Unhandled exception in search_magazines: {traceback.format_exc()}')
    finally:
        db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
        db.close()
        thread_name("WEBSERVER")


def download_maglist(maglist, table='wanted'):
    logger = logging.getLogger(__name__)
    snatched = 0
    db = database.DBConnection()
    try:
        for magazine in maglist:
            if magazine['nzbmode'] in ["torznab", "torrent", "magnet"]:
                snatch, res = tor_dl_method(
                    magazine['bookid'],
                    magazine['nzbtitle'],
                    magazine['nzburl'],
                    'magazine',
                    provider=magazine['nzbprov'])
            elif magazine['nzbmode'] == 'direct':
                snatch, res = direct_dl_method(
                    magazine['bookid'],
                    magazine['nzbtitle'],
                    magazine['nzburl'],
                    'magazine',
                    magazine['nzbprov'])
            elif magazine['nzbmode'] == 'nzb':
                snatch, res = nzb_dl_method(
                    magazine['bookid'],
                    magazine['nzbtitle'],
                    magazine['nzburl'],
                    'magazine')
            else:
                res = f"Unhandled NZBmode [{magazine['nzbmode']}] for {magazine['nzburl']}"
                logger.error(res)
                snatch = 0
            if snatch:
                snatched += 1
                if table == 'pastissues':
                    db.action("UPDATE pastissues set status=? WHERE NZBurl=?", ('Snatched', magazine["nzburl"]))
                logger.info(f"Downloading {magazine['nzbtitle']} from {magazine['nzbprov']}")
                custom_notify_snatch(f"{magazine['bookid']} {magazine['nzburl']}")
                notify_snatch(
                    f"Magazine {unaccented(magazine['nzbtitle'], only_ascii=False)} from "
                    f"{CONFIG.disp_name(magazine['nzbprov'])} at {now()}")
            else:
                db.action(f"UPDATE {table} SET status='Failed',DLResult=? WHERE NZBurl=?",
                          (res, magazine["nzburl"]))
    except Exception as e:
        logger.error(str(e))
    finally:
        db.close()
        if snatched:
            schedule_job(action=SchedulerCommand.START, target='PostProcessor')


def get_issue_date(nzbtitle_exploded, datetype=''):
    logger = logging.getLogger(__name__)
    issuenum_type = 0
    issuedate = ''
    year = 0
    if not datetype:
        datetype = ''
    # Magazine names have many different styles of date
    # These are the ones we can currently match...
    # 1 MonthName MonthName YYYY (bi-monthly just use first month as date)
    # 2 nn, MonthName YYYY  where nn is an assumed issue number (use issue OR month with/without year)
    # 3 DD MonthName YYYY (daily, weekly, bi-weekly, monthly)
    # 4 MonthName YYYY (monthly)
    # 5 MonthName DD YYYY or MonthName DD, YYYY (daily, weekly, bi-weekly, monthly)
    # 6 YYYY MM DD or YYYY MonthName DD (daily, weekly, bi-weekly, monthly)
    # 7 YYYY MM or YYYY MonthName (monthly)
    # 8 Volume x Issue y in either order, with year
    # 9 Volume x Issue y in either order, without year
    # 10 Issue/No/Nr/Vol/# nn, YYYY (prepend year to zero filled issue number)
    # 11 Issue/No/Nr/Vol/# nn (no year found, hopefully rolls on year on year)
    # 12 nn YYYY issue number without Issue/No/Nr/Vol/# in front (unsure, nn could be issue or month number)
    # 13 issue and year as a single 6 digit string eg 222015 (some uploaders use this, reverse it to YYYYIIII)
    # 14 3 or more digit zero padded issue number eg 0063 (issue with no year)
    # 15 just a year (annual)
    # 16 to 18 internal issuedates used for filenames, YYYYIIII, VVVVIIII, YYYYVVVVIIII
    #
    issuenouns = get_list(CONFIG['ISSUE_NOUNS'])
    volumenouns = get_list(CONFIG['VOLUME_NOUNS'])
    nouns = issuenouns
    nouns.extend(volumenouns)

    pos = 0
    while pos < len(nzbtitle_exploded):
        year = check_year(nzbtitle_exploded[pos])
        if year and pos:
            month = month2num(nzbtitle_exploded[pos - 1])
            if month:
                if pos > 1:
                    month2 = month2num(nzbtitle_exploded[pos - 2])
                    if month2:
                        # bimonthly, for now just use first month
                        month = min(month, month2)
                        day = 1
                        issuenum_type = 1
                    else:
                        day = check_int(re.sub(r"\D", "", nzbtitle_exploded[pos - 2]), 0)
                        if pos > 2 and nzbtitle_exploded[pos - 3].lower().strip('.') in nouns:
                            # definitely an issue or volume number
                            issuedate = str(day)
                            issuenum_type = 10
                            break
                        elif day > 31:  # probably issue/volume number nn
                            if 'I' in datetype or 'V' in datetype:
                                issuedate = str(day)
                                issuenum_type = 10
                                break
                            else:
                                issuenum_type = 4
                                day = 1
                        elif day:
                            issuenum_type = 3
                        else:
                            issuenum_type = 4
                            day = 1
                else:
                    issuenum_type = 4
                    day = 1

                if not issuedate:
                    issuedate = "%04d-%02d-%02d" % (year, month, day)
                try:
                    _ = datetime.date(year, month, day)
                    break
                except ValueError:
                    issuenum_type = 0
                except OverflowError:
                    logger.debug(f"Overflow [{str(nzbtitle_exploded)}]")
                    issuenum_type = 0
        pos += 1

    # MonthName DD YYYY or MonthName DD, YYYY
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            year = check_year(nzbtitle_exploded[pos])
            if year and (pos > 1):
                month = month2num(nzbtitle_exploded[pos - 2])
                if month:
                    day = check_int(re.sub(r"\D", "", nzbtitle_exploded[pos - 1]), 0)
                    try:
                        _ = datetime.date(year, month, day)
                        issuedate = "%04d-%02d-%02d" % (year, month, day)
                        issuenum_type = 5
                        break
                    except ValueError:
                        issuenum_type = 0
                    except OverflowError:
                        logger.debug(f"Overflow [{str(nzbtitle_exploded)}]")
                        issuenum_type = 0

            pos += 1

    # YYYY MM_or_MonthName or YYYY MM_or_MonthName DD
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            year = check_year(nzbtitle_exploded[pos])
            if year and pos + 1 < len(nzbtitle_exploded):
                month = month2num(nzbtitle_exploded[pos + 1])
                if not month:
                    month = check_int(nzbtitle_exploded[pos + 1], 0)
                if month:
                    if pos + 2 < len(nzbtitle_exploded):
                        day = check_int(re.sub(r"\D", "", nzbtitle_exploded[pos + 2]), 0)
                        if day:
                            issuenum_type = 6
                        else:
                            issuenum_type = 7
                            day = 1
                    else:
                        issuenum_type = 7
                        day = 1
                    try:
                        _ = datetime.date(year, month, day)
                        issuedate = "%04d-%02d-%02d" % (year, month, day)
                        break
                    except ValueError:
                        issuenum_type = 0
                    except OverflowError:
                        logger.debug(f"Overflow [{str(nzbtitle_exploded)}]")
                        issuenum_type = 0
            pos += 1

    # scan for a year in the name
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            year = check_year(nzbtitle_exploded[pos])
            if year:
                break
            pos += 1

        # Volume x Issue y in either order, with/without year in any position
        vol = 0
        iss = 0
        pos = 0
        while pos + 1 < len(nzbtitle_exploded):
            res = check_int(nzbtitle_exploded[pos + 1], 0)
            if res:
                if nzbtitle_exploded[pos] in issuenouns:
                    iss = res
                if nzbtitle_exploded[pos] in volumenouns:
                    vol = res
            if vol and iss:
                if year:
                    issuedate = "%s%04d%04d" % (year, vol, iss)
                    issuenum_type = 8
                else:
                    issuedate = "%04d%04d" % (vol, iss)
                    issuenum_type = 9
                break
            pos += 1

    # Issue/No/Nr/Vol/# nn with/without year in any position
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            # might be "Vol.3" or "#12" with no space between noun and number
            splitted = re.split(r'(\d+)', nzbtitle_exploded[pos].lower())
            if splitted[0].strip('.') in nouns:
                if len(splitted) > 1:
                    issue = check_int(splitted[1], 0)
                    if issue:
                        issuedate = str(issue)
                        # we searched for year prior to datestyle 8/9
                        if year:
                            issuenum_type = 10  # Issue/No/Nr/Vol nn, YYYY
                        else:
                            issuenum_type = 11  # Issue/No/Nr/Vol nn
                        break
                if pos + 1 < len(nzbtitle_exploded):
                    issue = check_int(nzbtitle_exploded[pos + 1], 0)
                    if issue:
                        issuedate = str(issue)
                        # we searched for year prior to datestyle 8/9
                        if year:
                            issuenum_type = 10  # Issue/No/Nr/Vol nn, YYYY
                        else:
                            issuenum_type = 11  # Issue/No/Nr/Vol nn
                        break
                    # No. 19.2 -> 2019 02 but 02 might be a number, not a month
                    issue = nzbtitle_exploded[pos + 1]
                    if issue.count('.') == 1 and issue.replace('.', '').isdigit():
                        year, issuedate = issue.split('.')
                        if len(year) == 2:
                            year = f'20{year}'
                        if len(issuedate) == 1:
                            issuedate = f'0{issuedate}'
                        if len(year) == 4 and len(issuedate) == 2:
                            issuenum_type = 10
                            break
            pos += 1

    # nn YYYY issue number without "Nr" before it
    if not issuenum_type and year:
        pos = 1
        while pos < len(nzbtitle_exploded):
            year = check_year(nzbtitle_exploded[pos])
            if year:
                issue = check_int(nzbtitle_exploded[pos - 1], 0)
                if issue:
                    issuedate = str(issue)
                    issuenum_type = 12
                    break
            pos += 1

    # issue and year as a single 6 digit string e.g. 222015
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            issue = nzbtitle_exploded[pos]
            if issue.isdigit() and len(issue) == 6:
                year = check_year(int(issue[2:]))
                if year:
                    issue = int(issue[:2])
                    issuedate = str(issue).zfill(4)
                    issuenum_type = 13
                    break
            pos += 1

    # issue as a 3 or more digit string with leading zero e.g. 0063
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            issue = nzbtitle_exploded[pos]
            if issue.isdigit():
                if (len(issue) > 2 and issue[0] == '0') or (datetype and 'I' in datetype):
                    issuedate = issue
                    year = 0
                    issuenum_type = 14
                    break
            pos += 1

    # Annual - only a year found, year was found prior to datestyle 8/9
    if not issuenum_type and year:
        issuedate = f"{year}-01-01"
        issuenum_type = 15

    # YYYYIIII internal issuedates for filenames
    if not issuenum_type:
        pos = 0
        while pos < len(nzbtitle_exploded):
            issue = nzbtitle_exploded[pos]
            if issue.isdigit():
                if len(issue) == 8:
                    if check_year(issue[:4]):  # YYYYIIII
                        year = issue[:4]
                        issuedate = issue
                        issuenum_type = 16
                        break
                    else:
                        issuedate = issue  # VVVVIIII
                        issuenum_type = 17
                        break
                elif len(issuedate) == 12:  # YYYYVVVVIIII
                    year = issue[:4]
                    issuedate = issue
                    issuenum_type = 18
                    break
            pos += 1
    return issuenum_type, issuedate, year
