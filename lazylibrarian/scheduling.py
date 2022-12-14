#  This file is part of Lazylibrarian.
#
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

# Purpose:
#   Scheduling functionality

from typing import Optional
import time
import datetime
import traceback
from lib.apscheduler.scheduler import Scheduler

import lazylibrarian
from lazylibrarian import database, logger, importer, bookwork
from lazylibrarian.formatter import thread_name, plural, check_int

# Notification Types
NOTIFY_SNATCH = 1
NOTIFY_DOWNLOAD = 2
NOTIFY_FAIL = 3

notifyStrings = {NOTIFY_SNATCH: "Started Download", NOTIFY_DOWNLOAD: "Added to Library", NOTIFY_FAIL: "Download failed"}

# Scheduler
SCHED: Scheduler

def initscheduler():
    global SCHED
    SCHED = Scheduler(misfire_grace_time=30)


def startscheduler():
    SCHED.start()


def shutdownscheduler():
    try:
        if SCHED:
            # noinspection PyUnresolvedReferences
            SCHED.shutdown(wait=False)
    except NameError:
        pass

def next_run_time(when_run, test_now: Optional[datetime.datetime] = None):
    """
    Returns a readable approximation of how long until a job will be run,
    given a string representing the last time it was run
    """
    try:
        when_run = datetime.datetime.strptime(when_run, '%Y-%m-%d %H:%M:%S')
        timenow = datetime.datetime.now() if not test_now else test_now
        td = when_run - timenow
        diff = td.total_seconds()  # time difference in seconds
    except ValueError as e:
        lazylibrarian.logger.error("Error getting next run for [%s] %s" % (when_run, str(e)))
        diff = 0
        td = ''

    td = str(td)
    if 'days,' in td: # > 1 day, just return days
        return td.split('s,')[0] + 's'
    elif 'day,' in td and not "0:00:00" in td: # 1 day and change, or 1 day?
        diff += 86400

    # calculate whole units, plus round up by adding 1(true) if remainder >= half
    days = int(diff / 86400) + (diff % 86400 >= 43200)
    hours = int(diff / 3600) + (diff % 3600 >= 1800)
    minutes = int(diff / 60) + (diff % 60 >= 30)
    seconds = int(diff)

    if days > 1:
        return "%i days" % days
    elif hours > 1:
        return "%i hours" % hours
    elif minutes > 1:
        return "%i minutes" % minutes
    elif seconds == 1:
        return "1 second"
    else:
        return "%i seconds" % seconds

def nextrun(target=None, interval=0, action='', hours=False):
    """ Check when a job is next due to run and log it
        Return startdate for the job """
    if target is None:
        return ''

    if action == 'StartNow':
        lazylibrarian.STOPTHREADS = False
        hours = False
        interval = 0

    db = database.DBConnection()
    columns = db.select('PRAGMA table_info(jobs)')
    if not columns:  # no such table
        lastrun = 0
    else:
        res = db.match('SELECT Finish from jobs WHERE Name=?', (target,))
        if res and res['Finish']:
            lastrun = res['Finish']
        else:
            lastrun = 0

    if target == 'sync_to_goodreads':
        newtarget = 'sync_to_gr'
    else:
        newtarget = target

    nextruntime = ''
    for job in SCHED.get_jobs():
        if newtarget in str(job):
            nextruntime = job.split('at: ')[1].split('.')[0].strip(')')
            break

    if nextruntime:
        startdate = datetime.datetime.strptime(nextruntime, '%Y-%m-%d %H:%M:%S')
        msg = "%s %s job in %s" % (action, target, next_run_time(startdate))
    else:
        if hours:
            interval *= 60

        next_run_in = lastrun + (interval * 60) - time.time()
        if next_run_in < 60:
            next_run_in = 60  # overdue, start in 1 minute

        startdate = datetime.datetime.fromtimestamp(time.time() + next_run_in)

        next_run_in = int(next_run_in / 60)
        if next_run_in < 1:
            next_run_in = 1

        if next_run_in <= 120:
            msg = "%s %s job in %s %s" % (action, target, next_run_in, plural(next_run_in, "minute"))
        else:
            hours = int(next_run_in / 60)
            if hours <= 48:
                msg = "%s %s job in %s %s" % (action, target, hours, plural(hours, "hour"))
            else:
                days = int(hours / 24)
                msg = "%s %s job in %s %s" % (action, target, days, plural(days, "day"))
    if lastrun:
        msg += " (Last run %s)" % ago(lastrun)
    logger.debug(msg)

    return startdate



def schedule_job(action='Start', target=None):
    """ Start or stop or restart a cron job by name e.g.
        target=search_magazines, target=process_dir, target=search_book """
    if target is None:
        return

    # Import all of the functions we may schedule
    # Late import to avoid circular references
    from lazylibrarian import postprocess, searchmag, searchbook, searchrss, \
        comicsearch, versioncheck, grsync, cache

    if target == 'PostProcessor':  # more readable
        newtarget = 'process_dir'
    elif target == 'sync_to_goodreads':
        newtarget = 'sync_to_gr'
    else:
        newtarget = target

    if action in ['Stop', 'Restart']:
        for job in SCHED.get_jobs():
            if newtarget in str(job):
                SCHED.unschedule_job(job)
                logger.debug("Stop %s job" % target)
                break

    if action in ['Start', 'Restart', 'StartNow']:
        for job in SCHED.get_jobs():
            if newtarget in str(job):
                logger.debug("%s %s job, already scheduled" % (action, target))
                return  # return if already running, if not, start a new one

        if 'process_dir' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SCAN_INTERVAL'], 0)
            if interval:
                startdate = nextrun("POSTPROCESS", interval, action)
                SCHED.add_interval_job(postprocess.cron_process_dir,
                                       minutes=interval, start_date=startdate)

        elif 'search_magazines' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_MAGINTERVAL'], 0)
            if interval and (lazylibrarian.use_tor() or lazylibrarian.use_nzb()
                             or lazylibrarian.use_rss() or lazylibrarian.use_direct()
                             or lazylibrarian.use_irc()):
                startdate = nextrun("SEARCHALLMAG", interval, action)
                if interval <= 600:  # for bigger intervals switch to hours
                    SCHED.add_interval_job(searchmag.cron_search_magazines,
                                           minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    SCHED.add_interval_job(searchmag.cron_search_magazines,
                                           hours=hours, start_date=startdate)
        elif 'search_book' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_BOOKINTERVAL'], 0)
            if interval and (lazylibrarian.use_nzb() or lazylibrarian.use_tor()
                             or lazylibrarian.use_direct() or lazylibrarian.use_irc()):
                startdate = nextrun("SEARCHALLBOOKS", interval, action)
                if interval <= 600:
                    SCHED.add_interval_job(searchbook.cron_search_book,
                                           minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    SCHED.add_interval_job(searchbook.cron_search_book,
                                           hours=hours, start_date=startdate)
        elif 'search_rss_book' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCHRSS_INTERVAL'], 0)
            if interval and lazylibrarian.use_rss():
                startdate = nextrun("SEARCHALLRSS", interval, action)
                if interval <= 600:
                    SCHED.add_interval_job(searchrss.cron_search_rss_book,
                                           minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    SCHED.add_interval_job(searchrss.cron_search_rss_book,
                                           hours=hours, start_date=startdate)
        elif 'search_wishlist' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['WISHLIST_INTERVAL'], 0)
            if interval and lazylibrarian.use_wishlist():
                startdate = nextrun("SEARCHWISHLIST", interval, action, True)
                SCHED.add_interval_job(searchrss.cron_search_wishlist,
                                       hours=interval, start_date=startdate)

        elif 'search_comics' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['SEARCH_COMICINTERVAL'], 0)
            if interval and (lazylibrarian.use_nzb() or lazylibrarian.use_tor()
                             or lazylibrarian.use_direct() or lazylibrarian.use_irc()):
                startdate = nextrun("SEARCHALLCOMICS", interval, action, True)
                SCHED.add_interval_job(comicsearch.cron_search_comics,
                                       hours=interval, start_date=startdate)

        elif 'check_for_updates' in newtarget:
            interval = check_int(lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'], 0)
            if interval:
                startdate = nextrun("VERSIONCHECK", interval, action, True)
                SCHED.add_interval_job(versioncheck.check_for_updates,
                                       hours=interval, start_date=startdate)

        elif 'sync_to_gr' in newtarget and lazylibrarian.CONFIG['GR_SYNC']:
            interval = check_int(lazylibrarian.CONFIG['GOODREADS_INTERVAL'], 0)
            if interval:
                startdate = nextrun("GRSYNC", interval, action, True)
                SCHED.add_interval_job(grsync.cron_sync_to_gr,
                                       hours=interval, start_date=startdate)

        elif 'clean_cache' in newtarget:
            days = lazylibrarian.CONFIG['CACHE_AGE']
            if days:
                interval = 8
                startdate = nextrun("CLEANCACHE", interval, action, True)
                SCHED.add_interval_job(cache.clean_cache,
                                       hours=interval, start_date=startdate)

        elif 'author_update' in newtarget or 'series_update' in newtarget:
            # Try to get all authors/series scanned evenly inside the cache age
            maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
            if maxage:
                typ = newtarget.replace('_update', '')
                if typ == 'author':
                    task = 'AUTHORUPDATE'
                else:
                    task = 'SERIESUPDATE'

                overdue, total, _, _, days = is_overdue(typ)

                if days == maxage:
                    due = "due"
                else:
                    due = "overdue"
                logger.debug("Found %s %s from %s %s update" % (
                             overdue, plural(overdue, typ), total, due))

                interval = maxage * 60 * 24
                interval = interval / max(total, 1)
                interval = int(interval * 0.80)  # allow some update time

                if interval < 5:  # set a minimum interval of 5 minutes, so we don't upset goodreads/librarything api
                    interval = 5

                startdate = nextrun(task, interval, action)
                if interval <= 600:  # for bigger intervals switch to hours
                    if typ == 'author':
                        SCHED.add_interval_job(author_update, minutes=interval, start_date=startdate)
                    else:
                        SCHED.add_interval_job(series_update, minutes=interval, start_date=startdate)
                else:
                    hours = int(interval / 60)
                    if typ == 'author':
                        SCHED.add_interval_job(author_update, hours=hours, start_date=startdate)
                    else:
                        SCHED.add_interval_job(series_update, hours=hours, start_date=startdate)
        else:
            logger.debug("No %s scheduled" % target)


def author_update(restart=True, only_overdue=True):
    threadname = thread_name()
    if threadname and "Thread-" in threadname:
        thread_name("AUTHORUPDATE")

    db = database.DBConnection()
    msg = ''

    # noinspection PyBroadException
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        if check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            overdue, total, name, ident, days = is_overdue('author')
            if not total:
                msg = "There are no monitored authors"
            elif not overdue and only_overdue:
                msg = 'Oldest author info (%s) is %s %s old, no update due' % (name,
                                                                               days, plural(days, "day"))
            else:
                logger.info('Starting update for %s' % name)
                importer.add_author_to_db(refresh=True, authorid=ident, reason="author_update %s" % name)
                if lazylibrarian.STOPTHREADS:
                    return ''
                msg = 'Updated author %s' % name
            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            if total and restart and not lazylibrarian.STOPTHREADS:
                schedule_job("Restart", "author_update")
    except Exception:
        logger.error('Unhandled exception in AuthorUpdate: %s' % traceback.format_exc())
        msg = "Unhandled exception in AuthorUpdate"
    finally:
        return msg


def series_update(restart=True, only_overdue=True):
    threadname = thread_name()
    if threadname and "Thread-" in threadname:
        thread_name("SERIESUPDATE")

    db = database.DBConnection()
    msg = ''

    # noinspection PyBroadException
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": thread_name()})
        if check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
            overdue, total, name, ident, days = is_overdue('series')
            if not total:
                msg = "There are no monitored series"
            elif not overdue and only_overdue:
                msg = 'Oldest series info (%s) is %s %s old, no update due' % (name,
                                                                               days, plural(days, "day"))
            else:
                logger.info('Starting series update for %s' % name)
                bookwork.add_series_members(ident)
                msg = 'Updated series %s' % name
            logger.debug(msg)

            db.upsert("jobs", {"Finish": time.time()}, {"Name": thread_name()})
            if total and restart and not lazylibrarian.STOPTHREADS:
                schedule_job("Restart", "series_update")
    except Exception:
        logger.error('Unhandled exception in series_update: %s' % traceback.format_exc())
        msg = "Unhandled exception in series_update"
    finally:
        return msg


def all_author_update(refresh=False):
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        cmd = 'SELECT AuthorID from authors WHERE Status="Active" or Status="Loading" or Status="Wanted"'
        cmd += ' order by Updated ASC'
        activeauthors = db.select(cmd)
        lazylibrarian.AUTHORS_UPDATE = 1
        logger.info('Starting update for %i active %s' % (len(activeauthors), plural(len(activeauthors), "author")))
        for author in activeauthors:
            if lazylibrarian.STOPTHREADS:
                logger.debug("Aborting ActiveAuthorUpdate")
                break
            importer.add_author_to_db(refresh=refresh, authorid=author['AuthorID'],
                                                    reason="all_author_update")
        logger.info('Active author update complete')
        msg = 'Updated %i active %s' % (len(activeauthors), plural(len(activeauthors), "author"))
        logger.debug(msg)
    except Exception:
        msg = 'Unhandled exception in all_author_update: %s' % traceback.format_exc()
        logger.error(msg)
    finally:
        lazylibrarian.AUTHORS_UPDATE = 0
    return msg


def restart_jobs(start='Restart'):
    lazylibrarian.STOPTHREADS = start == 'Stop'
    for item in ['PostProcessor', 'search_book', 'search_rss_book', 'search_wishlist', 'series_update',
                 'search_magazines', 'search_comics', 'check_for_updates', 'author_update', 'sync_to_goodreads',
                 'clean_cache']:
        schedule_job(start, item)


def ensure_running(jobname):
    lazylibrarian.STOPTHREADS = False
    found = False
    for job in SCHED.get_jobs():
        if jobname in str(job):
            found = True
            break
    if not found:
        schedule_job('Start', jobname)


def check_running_jobs():
    # make sure the relevant jobs are running
    # search jobs start when something gets marked "wanted" but are
    # not aware of any config changes that happen later, ie enable or disable providers,
    # so we check whenever config is saved
    # postprocessor is started when something gets marked "snatched"
    # and cancels itself once everything is processed so should be ok
    # but check anyway for completeness...

    lazylibrarian.STOPTHREADS = False
    db = database.DBConnection()
    snatched = db.match("SELECT count(*) as counter from wanted WHERE Status = 'Snatched'")
    seeding = db.match("SELECT count(*) as counter from wanted WHERE Status = 'Seeding'")
    wanted = db.match("SELECT count(*) as counter FROM books WHERE Status = 'Wanted'")
    if snatched or seeding:
        ensure_running('PostProcessor')
    if wanted:
        if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_direct() or \
                lazylibrarian.use_irc():
            ensure_running('search_book')
        if lazylibrarian.use_rss():
            ensure_running('search_rss_book')
    else:
        schedule_job('Stop', 'search_book')
        schedule_job('Stop', 'search_rss_book')
    if lazylibrarian.use_wishlist():
        ensure_running('search_wishlist')
    else:
        schedule_job('Stop', 'search_wishlist')

    if lazylibrarian.use_nzb() or lazylibrarian.use_tor() or lazylibrarian.use_rss() or \
            lazylibrarian.use_direct() or lazylibrarian.use_irc():
        ensure_running('search_magazines')
        ensure_running('search_comics')
    else:
        schedule_job('Stop', 'search_magazines')
        schedule_job('Stop', 'search_comics')

    ensure_running('author_update')
    ensure_running('series_update')

def is_overdue(which="author"):
    overdue = 0
    total = 0
    name = ''
    ident = ''
    days = 0
    maxage = check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0)
    if maxage:
        db = database.DBConnection()
        if which == 'author':
            cmd = 'SELECT AuthorName,AuthorID,Updated from authors WHERE Status="Active" or Status="Loading"'
            cmd += ' or Status="Wanted" '
            if lazylibrarian.CONFIG['BOOK_API'] == 'OpenLibrary':
                cmd += 'and AuthorID LIKE "OL%A" '
            else:
                cmd += 'and AuthorID NOT LIKE "OL%A" '
            cmd += 'order by Updated ASC'
            res = db.select(cmd)
            total = len(res)
            if total:
                name = res[0]['AuthorName']
                ident = res[0]['AuthorID']
                dtnow = time.time()
                days = int((dtnow - res[0]['Updated']) / (24 * 60 * 60))
                for item in res:
                    diff = (dtnow - item['Updated']) / (24 * 60 * 60)
                    if diff > maxage:
                        overdue += 1
                    else:
                        break
        if which == 'series':
            cmd = 'SELECT SeriesName,SeriesID,Updated from Series where Status="Active" or Status="Wanted"'
            cmd += ' order by Updated ASC'
            res = db.select(cmd)
            total = len(res)
            if total:
                name = res[0]['SeriesName']
                ident = res[0]['SeriesID']
                dtnow = time.time()
                days = int((dtnow - res[0]['Updated']) / (24 * 60 * 60))
                for item in res:
                    diff = (dtnow - item['Updated']) / (24 * 60 * 60)
                    if diff > maxage:
                        overdue += 1
                    else:
                        break
    return overdue, total, name, ident, days


def ago(when):
    """ Return human-readable string of how long ago something happened
        when = seconds count """

    diff = time.time() - when
    # calculate whole units, plus round up by adding 1(true) if remainder >= half
    days = int(diff / 86400) + (diff % 86400 >= 43200)
    hours = int(diff / 3600) + (diff % 3600 >= 1800)
    minutes = int(diff / 60) + (diff % 60 >= 30)
    seconds = int(diff)

    if days > 1:
        return "%i days ago" % days
    elif hours > 1:
        return "%i hours ago" % hours
    elif minutes > 1:
        return "%i minutes ago" % minutes
    elif seconds > 1:
        return "%i seconds ago" % seconds
    else:
        return "just now"

def show_jobs():
    result = []
    db = database.DBConnection()
    for job in SCHED.get_jobs():
        job = str(job)
        if "search_magazines" in job:
            jobname = "Magazine search"
            threadname = "SEARCHALLMAG"
        elif "search_comics" in job:
            jobname = "Comic search"
            threadname = "SEARCHALLCOMICS"
        elif "check_for_updates" in job:
            jobname = "Check for Update"
            threadname = "VERSIONCHECK"
        elif "search_book" in job:
            jobname = "Book search"
            threadname = "SEARCHALLBOOKS"
        elif "search_rss_book" in job:
            jobname = "rss book search"
            threadname = "SEARCHALLRSS"
        elif "search_wishlist" in job:
            jobname = "Wishlist search"
            threadname = "SEARCHWISHLIST"
        elif "PostProcessor" in job:
            jobname = "PostProcessor"
            threadname = "POSTPROCESS"
        elif "cron_process_dir" in job:
            jobname = "PostProcessor"
            threadname = "POSTPROCESS"
        elif "author_update" in job:
            jobname = "Update authors"
            threadname = "AUTHORUPDATE"
        elif "series_update" in job:
            jobname = "Update series"
            threadname = "SERIESUPDATE"
        elif "sync_to_gr" in job:
            jobname = "Goodreads Sync"
            threadname = "GRSYNC"
        elif "clean_cache" in job:
            jobname = "Clean cache"
            threadname = "CLEANCACHE"
        else:
            jobname = job.split(' ')[0].split('.')[2]
            threadname = jobname.upper()

        # jobinterval = job.split('[')[1].split(']')[0]
        jobtime = job.split('at: ')[1].split('.')[0].strip(')')
        jobtime = next_run_time(jobtime)
        timeparts = jobtime.split(' ')
        if timeparts[0] == '1' and timeparts[1].endswith('s'):
            timeparts[1] = timeparts[1][:-1]
        jobinfo = "%s: Next run in %s %s" % (jobname, timeparts[0], timeparts[1])
        res = db.match('SELECT Start,Finish from jobs WHERE Name="%s"' % threadname)

        if res:
            if res['Start'] > res['Finish']:
                jobinfo += " (Running since %s)" % ago(res['Start'])
            elif res['Finish']:
                jobinfo += " (Last run %s)" % ago(res['Finish'])
        result.append(jobinfo)

    result.append(' ')
    overdue, total, name, _, days = is_overdue('author')
    if name:
        result.append('Oldest author info (%s) is %s %s old' % (name, days, plural(days, "day")))
    if not overdue:
        result.append("There are no authors needing update")
    elif days == check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
        result.append("Found %s %s from %s due update" % (overdue, plural(overdue, "author"), total))
    else:
        result.append("Found %s %s from %s overdue update" % (overdue, plural(overdue, "author"), total))

    overdue, total, name, _, days = is_overdue('series')
    if name:
        result.append('Oldest series info (%s) is %s %s old' % (name, days, plural(days, "day")))
    if not overdue:
        result.append("There are no series needing update")
    elif days == check_int(lazylibrarian.CONFIG['CACHE_AGE'], 0):
        result.append("Found %s series from %s due update" % (overdue, total))
    else:
        result.append("Found %s series from %s overdue update" % (overdue, total))
    return result
