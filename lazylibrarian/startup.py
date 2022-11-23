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

# Purpose:
#   Contains global startup and initialization code for LL

import lazylibrarian

import locale
import os
import sys
import time
import sqlite3
import calendar
import json
import subprocess
import signal
import traceback
import tarfile
import cherrypy
import requests

from shutil import rmtree

from lazylibrarian.common import path_isfile, path_isdir, remove, listdir, log_header, syspath, restart_jobs, \
    initscheduler, startscheduler, shutdownscheduler
from lazylibrarian import config, database, versioncheck
from lazylibrarian import CONFIG
from lazylibrarian.formatter import check_int, get_list, unaccented, make_unicode
from lazylibrarian.dbupgrade import check_db, db_current_version
from lazylibrarian.cache import fetch_url
from lazylibrarian.logger import RotatingLogger, lazylibrarian_log, error, debug, warn, info


def startup_parsecommandline(mainfile, args, seconds_to_sleep=4):
    # All initializartion that needs to happen before logging starts
    if hasattr(sys, 'frozen'):
        lazylibrarian.FULL_PATH = os.path.abspath(sys.executable)
    else:
        lazylibrarian.FULL_PATH = os.path.abspath(mainfile)

    lazylibrarian.PROG_DIR = os.path.dirname(lazylibrarian.FULL_PATH)
    lazylibrarian.ARGS = sys.argv[1:]
    lazylibrarian.DOCKER = '/config' in lazylibrarian.ARGS and lazylibrarian.FULL_PATH.startswith('/app/')

    lazylibrarian.SYS_ENCODING = None

    try:
        locale.setlocale(locale.LC_ALL, "")
        lazylibrarian.SYS_ENCODING = locale.getpreferredencoding()
    except (locale.Error, IOError):
        pass

    # for OSes that are poorly configured I'll just force UTF-8
    # windows cp1252 can't handle some accented author names,
    # eg "Marie Kondō" U+014D: LATIN SMALL LETTER O WITH MACRON, but utf-8 does
    if not lazylibrarian.SYS_ENCODING or lazylibrarian.SYS_ENCODING in (
            'ANSI_X3.4-1968', 'US-ASCII', 'ASCII') or '1252' in lazylibrarian.SYS_ENCODING:
        lazylibrarian.SYS_ENCODING = 'UTF-8'

    # Set arguments
    from optparse import OptionParser

    p = OptionParser()
    p.add_option('-d', '--daemon', action="store_true",
                 dest='daemon', help="Run the server as a daemon")
    p.add_option('-q', '--quiet', action="store_true",
                 dest='quiet', help="Don't log to console")
    p.add_option('-j', '--nojobs', action="store_true",
                 dest='nojobs', help="Don't start background tasks")
    p.add_option('--debug', action="store_true",
                 dest='debug', help="Show debuglog messages")
    p.add_option('--nolaunch', action="store_true",
                 dest='nolaunch', help="Don't start browser")
    p.add_option('--update', action="store_true",
                 dest='update', help="Update to latest version (only git or source installs)")
    p.add_option('--upgrade', action="store_true",
                 dest='update', help="Update to latest version (only git or source installs)")
    p.add_option('--port',
                 dest='port', default=None,
                 help="Force webinterface to listen on this port")
    p.add_option('--datadir',
                 dest='datadir', default=None,
                 help="Path to the data directory")
    p.add_option('--config',
                 dest='config', default=None,
                 help="Path to config.ini file")
    p.add_option('-p', '--pidfile',
                 dest='pidfile', default=None,
                 help="Store the process id in the given file")
    p.add_option('-u', '--userid',
                 dest='userid', default=None,
                 help="Login as this userid")
    p.add_option('--loglevel',
                 dest='loglevel', default=None,
                 help="Debug loglevel")

    options, _ = p.parse_args(args)

    lazylibrarian.LOGLEVEL = 1
    if options.debug:
        lazylibrarian.LOGLEVEL = 2

    if options.quiet:
        lazylibrarian.LOGLEVEL = 0

    if options.daemon:
        if os.name != 'nt':
            lazylibrarian.DAEMON = True
            # lazylibrarian.daemonize()
        else:
            print("Daemonize not supported under Windows, starting normally")

    if options.nolaunch:
        lazylibrarian.CONFIG['LAUNCH_BROWSER'] = False

    if options.nojobs:
        lazylibrarian.STOPTHREADS = True
    else:
        lazylibrarian.STOPTHREADS = False

    if options.datadir:
        lazylibrarian.DATADIR = str(options.datadir)
    else:
        lazylibrarian.DATADIR = lazylibrarian.PROG_DIR

    if not path_isdir(lazylibrarian.DATADIR):
        try:
            os.makedirs(lazylibrarian.DATADIR)
        except OSError:
            raise SystemExit('Could not create data directory: ' + lazylibrarian.DATADIR + '. Exit ...')

    if not os.access(lazylibrarian.DATADIR, os.W_OK):
        raise SystemExit('Cannot write to the data directory: ' + lazylibrarian.DATADIR + '. Exit ...')

    if options.update:
        lazylibrarian.SIGNAL = 'update'
        # This is the "emergency recovery" update in case lazylibrarian won't start.
        # Set up some dummy values for the update as we have not read the config file yet
        lazylibrarian.CONFIG['GIT_PROGRAM'] = ''
        lazylibrarian.CONFIG['GIT_USER'] = 'lazylibrarian'
        lazylibrarian.CONFIG['GIT_REPO'] = 'lazylibrarian'
        lazylibrarian.CONFIG['GIT_HOST'] = 'gitlab'
        lazylibrarian.CONFIG['USER_AGENT'] = 'lazylibrarian'
        lazylibrarian.CONFIG['HTTP_TIMEOUT'] = 30
        lazylibrarian.CONFIG['PROXY_HOST'] = ''
        lazylibrarian.CONFIG['SSL_CERTS'] = ''
        lazylibrarian.CONFIG['SSL_VERIFY'] = False
        if lazylibrarian.CACHEDIR == '':
            lazylibrarian.CACHEDIR = os.path.join(lazylibrarian.PROG_DIR, 'cache')
        lazylibrarian.CONFIG['LOGLIMIT'] = 2000
        lazylibrarian.CONFIG['LOGDIR'] = os.path.join(lazylibrarian.DATADIR, 'Logs')
        if not path_isdir(lazylibrarian.CONFIG['LOGDIR']):
            try:
                os.makedirs(lazylibrarian.CONFIG['LOGDIR'])
            except OSError:
                raise SystemExit('Could not create log directory: ' + lazylibrarian.CONFIG['LOGDIR'] + '. Exit ...')

        versioncheck.get_install_type()
        if lazylibrarian.CONFIG['INSTALL_TYPE'] not in ['git', 'source']:
            lazylibrarian.SIGNAL = None
            print('Cannot update, not a git or source installation')
        else:
            lazylibrarian.startup.shutdown(restart=True, update=True)

    if options.loglevel:
        try:
            lazylibrarian.LOGLEVEL = int(options.loglevel)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_cherrypy:
                lazylibrarian.CHERRYPYLOG = 1
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_requests:
                lazylibrarian.REQUESTSLOG = 1
        except ValueError:
            lazylibrarian.LOGLEVEL = 2

    if options.config:
        lazylibrarian.CONFIGFILE = str(options.config)
    else:
        lazylibrarian.CONFIGFILE = os.path.join(lazylibrarian.DATADIR, "config.ini")

    if options.pidfile:
        if lazylibrarian.DAEMON:
            lazylibrarian.PIDFILE = str(options.pidfile)

    print("Lazylibrarian (pid %s) is starting up..." % os.getpid())
    time.sleep(
        seconds_to_sleep)  # allow time for old task to exit if restarting. Needs to free logfile and server port.

    icon = os.path.join(lazylibrarian.CACHEDIR, 'alive.png')
    if path_isfile(icon):
        remove(icon)

    # create database and config
    lazylibrarian.DBFILE = os.path.join(lazylibrarian.DATADIR, 'lazylibrarian.db')

    config.readConfigFile()

    return options


def init_logs():
    # Initialized log files. Until this is done, do not use the 
    config.check_ini_section('General')
    # False to silence logging until logger initialised
    for key in ['LOGLIMIT', 'LOGFILES', 'LOGSIZE', 'LOGDIR']:
        item_type, section, default = config.CONFIG_DEFINITIONS[key]
        lazylibrarian.CONFIG[key.upper()] = config.check_setting(item_type, section, key.lower(), default, log=False)

    if not lazylibrarian.CONFIG['LOGDIR']:
        lazylibrarian.CONFIG['LOGDIR'] = os.path.join(lazylibrarian.DATADIR, 'Logs')

    # Create logdir
    if not path_isdir(lazylibrarian.CONFIG['LOGDIR']):
        try:
            os.makedirs(lazylibrarian.CONFIG['LOGDIR'])
        except OSError as e:
            print('%s : Unable to create folder for logs: %s' % (lazylibrarian.CONFIG['LOGDIR'], str(e)))

    # Start the logger, silence console logging if we need to
    cfgloglevel = check_int(config.check_setting('int', 'General', 'loglevel', 1, log=False), 9)
    if lazylibrarian.LOGLEVEL == 1:  # default if no debug or quiet on cmdline
        if cfgloglevel == 9:  # default value if none in config
            lazylibrarian.LOGLEVEL = 1  # If not set in Config or cmdline, then lets set to NORMAL
        else:
            lazylibrarian.LOGLEVEL = cfgloglevel  # Config setting picked up

    lazylibrarian.CONFIG['LOGLEVEL'] = lazylibrarian.LOGLEVEL
    lazylibrarian_log.init_logger(loglevel=lazylibrarian.CONFIG['LOGLEVEL'])
    info("Log (%s) Level set to [%s]- Log Directory is [%s] - Config level is [%s]" % (
        lazylibrarian.LOGTYPE, lazylibrarian.CONFIG['LOGLEVEL'],
        lazylibrarian.CONFIG['LOGDIR'], cfgloglevel))
    if lazylibrarian.CONFIG['LOGLEVEL'] > 2:
        info("Screen Log set to EXTENDED DEBUG")
    elif lazylibrarian.CONFIG['LOGLEVEL'] == 2:
        info("Screen Log set to DEBUG")
    elif lazylibrarian.CONFIG['LOGLEVEL'] == 1:
        info("Screen Log set to INFO")
    else:
        info("Screen Log set to WARN/ERROR")


def init_config():
    initscheduler()
    config.config_read()
    lazylibrarian.UNRARLIB, lazylibrarian.RARFILE = get_unrarlib()


def init_caches():
    # override detected encoding if required
    if lazylibrarian.CONFIG['SYS_ENCODING']:
        lazylibrarian.SYS_ENCODING = lazylibrarian.CONFIG['SYS_ENCODING']

    # Put the cache dir in the data dir for now
    lazylibrarian.CACHEDIR = os.path.join(lazylibrarian.DATADIR, 'cache')
    if not path_isdir(lazylibrarian.CACHEDIR):
        try:
            os.makedirs(lazylibrarian.CACHEDIR)
        except OSError as e:
            error('Could not create cachedir; %s' % e)

    for item in ['book', 'author', 'SeriesCache', 'JSONCache', 'XMLCache', 'WorkCache', 'HTMLCache',
                 'magazine', 'comic', 'IRCCache', 'icrawler', 'mako']:
        cachelocation = os.path.join(lazylibrarian.CACHEDIR, item)
        try:
            os.makedirs(cachelocation)
        except OSError as e:
            if not path_isdir(cachelocation):
                error('Could not create %s: %s' % (cachelocation, e))

    # nest these caches 2 levels to make smaller/faster directory lists
    caches = ["XMLCache", "JSONCache", "WorkCache", "HTMLCache"]
    for item in caches:
        pth = os.path.join(lazylibrarian.CACHEDIR, item)
        for i in '0123456789abcdef':
            for j in '0123456789abcdef':
                cachelocation = os.path.join(pth, i, j)
                try:
                    os.makedirs(cachelocation)
                except OSError as e:
                    if not path_isdir(cachelocation):
                        error('Could not create %s: %s' % (cachelocation, e))
        for itm in listdir(pth):
            if len(itm) > 2:
                os.rename(syspath(os.path.join(pth, itm)),
                          syspath(os.path.join(pth, itm[0], itm[1], itm)))
    last_run_version = None
    last_run_interface = None
    makocache = os.path.join(lazylibrarian.CACHEDIR, 'mako')
    version_file = os.path.join(makocache, 'python_version.txt')

    if os.path.isfile(version_file):
        with open(version_file, 'r') as fp:
            last_time = fp.read().strip(' \n\r')
        if ':' in last_time:
            last_run_version, last_run_interface = last_time.split(':', 1)
        else:
            last_run_version = last_time

    clean_cache = False
    if last_run_version != sys.version.split()[0]:
        if last_run_version:
            debug("Python version change (%s to %s)" % (last_run_version, sys.version.split()[0]))
        else:
            debug("Previous python version unknown, now %s" % sys.version.split()[0])
        clean_cache = True
    if last_run_interface != lazylibrarian.CONFIG['HTTP_LOOK']:
        if last_run_interface:
            debug("Interface change (%s to %s)" % (last_run_interface, lazylibrarian.CONFIG['HTTP_LOOK']))
        else:
            debug("Previous interface unknown, now %s" % lazylibrarian.CONFIG['HTTP_LOOK'])
        clean_cache = True
    if clean_cache:
        debug("Clearing mako cache")
        rmtree(makocache)
        os.makedirs(makocache)
        with open(version_file, 'w') as fp:
            fp.write(sys.version.split()[0] + ':' + lazylibrarian.CONFIG['HTTP_LOOK'])

    # keep track of last api calls so we don't call more than once per second
    # to respect api terms, but don't wait un-necessarily either
    # keep track of how long we slept
    time_now = int(time.time())
    lazylibrarian.TIMERS['LAST_LT'] = time_now
    lazylibrarian.TIMERS['LAST_GR'] = time_now
    lazylibrarian.TIMERS['LAST_CV'] = time_now
    lazylibrarian.TIMERS['LAST_BOK'] = time_now
    lazylibrarian.TIMERS['SLEEP_GR'] = 0.0
    lazylibrarian.TIMERS['SLEEP_LT'] = 0.0
    lazylibrarian.TIMERS['SLEEP_CV'] = 0.0
    lazylibrarian.TIMERS['SLEEP_BOK'] = 0.0
    lazylibrarian.GB_CALLS = 0

    if lazylibrarian.CONFIG['BOOK_API'] != 'GoodReads':
        lazylibrarian.CONFIG['GR_SYNC'] = 0
        lazylibrarian.CONFIG['GR_FOLLOW'] = 0
        lazylibrarian.CONFIG['GR_FOLLOWNEW'] = 0


def init_database():
    # Initialize the database
    try:
        db = database.DBConnection()
        result = db.match('PRAGMA user_version')
        check = db.match('PRAGMA integrity_check')
        if result:
            version = result[0]
        else:
            version = 0
        info("Database is v%s, integrity check: %s" % (version, check[0]))
    except Exception as e:
        error("Can't connect to the database: %s %s" % (type(e).__name__, str(e)))
        sys.exit(0)

    if version:
        db_changes = check_db()
        if db_changes:
            db.action('PRAGMA user_version=%s' % db_current_version)
            db.action('vacuum')
            debug("Upgraded database schema to v%s with %s changes" % (db_current_version, db_changes))

    db.close()
    # group_concat needs sqlite3 >= 3.5.4
    # foreign_key needs sqlite3 >= 3.6.19 (Oct 2009)
    try:
        sqlv = getattr(sqlite3, 'sqlite_version', None)
        parts = sqlv.split('.')
        if int(parts[0]) == 3:
            if int(parts[1]) < 6 or int(parts[1]) == 6 and int(parts[2]) < 19:
                error("Your version of sqlite3 is too old, please upgrade to at least v3.6.19")
                sys.exit(0)
    except Exception as e:
        warn("Unable to parse sqlite3 version: %s %s" % (type(e).__name__, str(e)))


def init_build_debug_header(online):
    debuginfo = log_header(online)
    for item in debuginfo.splitlines():
        if 'missing' in item:
            warn(item)


def init_build_lists():
    lazylibrarian.GRGENRES = build_genres()
    lazylibrarian.MONTHNAMES = build_monthtable()
    lazylibrarian.NEWUSER_MSG = build_logintemplate()
    lazylibrarian.NEWFILE_MSG = build_filetemplate()
    lazylibrarian.BOOKSTRAP_THEMELIST = build_bookstrap_themes(lazylibrarian.PROG_DIR)


def get_unrarlib():
    """ Detect presence of unrar library
        Return type of library and rarfile()
    """
    # noinspection PyBroadException
    from unrar import rarfile
    if lazylibrarian.CONFIG['PREF_UNRARLIB'] == 1:
        return 1, rarfile

    if not rarfile or lazylibrarian.CONFIG['PREF_UNRARLIB'] == 2:
        # noinspection PyBroadException
        try:
            from lib.UnRAR2 import RarFile
            return 2, RarFile
        except Exception:
            if rarfile:
                return 1, rarfile
    return 0, None


def build_bookstrap_themes(prog_dir):
    themelist = []
    if not path_isdir(os.path.join(prog_dir, 'data', 'interfaces', 'bookstrap')):
        return themelist  # return empty if bookstrap interface not installed

    url = 'http://bootswatch.com/api/3.json'
    result, success = fetch_url(url, headers=None, retry=False)
    if not success:
        debug("Error getting bookstrap themes : %s" % result)
        return themelist

    try:
        results = json.loads(result)
        for theme in results['themes']:
            themelist.append(theme['name'].lower())
    except Exception as e:
        # error reading results
        warn('JSON Error reading bookstrap themes, %s %s' % (type(e).__name__, str(e)))

    info("Bookstrap found %i themes" % len(themelist))
    return themelist


def build_logintemplate():
    default_msg = "Your lazylibrarian username is {username}\nYour password is {password}\n"
    default_msg += "You can log in to lazylibrarian and change these to something more memorable\n"
    default_msg += "You have been given {permission} access\n"
    msg_file = os.path.join(lazylibrarian.DATADIR, 'logintemplate.text')
    if path_isfile(msg_file):
        try:
            # noinspection PyArgumentList
            with open(syspath(msg_file), 'r', encoding='utf-8') as msg_data:
                res = msg_data.read()
            for item in ["{username}", "{password}", "{permission}"]:
                if item not in res:
                    warn("Invalid login template in %s, no %s" % (msg_file, item))
                    return default_msg
            info("Loaded login template from %s" % msg_file)
            return res
        except Exception as e:
            error('Failed to load %s, %s %s' % (msg_file, type(e).__name__, str(e)))
    debug("Using default login template")
    return default_msg


def build_filetemplate():
    default_msg = "{name}{method}{link}"
    msg_file = os.path.join(lazylibrarian.DATADIR, 'filetemplate.text')
    if path_isfile(msg_file):
        try:
            with open(syspath(msg_file), 'r', encoding='utf-8') as msg_data:
                res = msg_data.read()
            for item in ["{name}", "{method}", "{link}"]:
                if item not in res:
                    warn("Invalid attachment template in %s, no %s" % (msg_file, item))
                    return default_msg
            info("Loaded attachment template from %s" % msg_file)
            return res
        except Exception as e:
            error('Failed to load %s, %s %s' % (msg_file, type(e).__name__, str(e)))
    debug("Using default attachment template")
    return default_msg


def build_genres():
    for json_file in [os.path.join(lazylibrarian.DATADIR, 'genres.json'),
                      os.path.join(lazylibrarian.PROG_DIR, 'example.genres.json')]:
        if path_isfile(json_file):
            try:
                with open(syspath(json_file), 'r', encoding='utf-8') as json_data:
                    res = json.load(json_data)
                info("Loaded genres from %s" % json_file)
                return res
            except Exception as e:
                error('Failed to load %s, %s %s' % (json_file, type(e).__name__, str(e)))
    error('No valid genres.json file found')
    return {"genreLimit": 4, "genreUsers": 10, "genreExclude": [], "genreExcludeParts": [], "genreReplace": {}}


def build_monthtable():
    table = []
    json_file = os.path.join(lazylibrarian.DATADIR, 'monthnames.json')
    if path_isfile(json_file):
        try:
            with open(syspath(json_file)) as json_data:
                table = json.load(json_data)
            mlist = ''
            # list alternate entries as each language is in twice (long and short month names)
            for item in table[0][::2]:
                mlist += item + ' '
            debug('Loaded monthnames.json : %s' % mlist)
        except Exception as e:
            error('Failed to load monthnames.json, %s %s' % (type(e).__name__, str(e)))

    if not table:
        # Default Month names table to hold long/short month names for multiple languages
        # which we can match against magazine issues
        table = [
            ['en_GB.UTF-8', 'en_GB.UTF-8'],
            ['january', 'jan'],
            ['february', 'feb'],
            ['march', 'mar'],
            ['april', 'apr'],
            ['may', 'may'],
            ['june', 'jun'],
            ['july', 'jul'],
            ['august', 'aug'],
            ['september', 'sep'],
            ['october', 'oct'],
            ['november', 'nov'],
            ['december', 'dec']
        ]

    if len(get_list(CONFIG['IMP_MONTHLANG'])) == 0:  # any extra languages wanted?
        return table
    try:
        current_locale = locale.setlocale(locale.LC_ALL, '')  # read current state.
        if 'LC_CTYPE' in current_locale:
            current_locale = locale.setlocale(locale.LC_CTYPE, '')
        # getdefaultlocale() doesnt seem to work as expected on windows, returns 'None'
        debug('Current locale is %s' % current_locale)
    except locale.Error as e:
        debug("Error getting current locale : %s" % str(e))
        return table

    lang = str(current_locale)
    # check not already loaded, also all english variants and 'C' use the same month names
    if lang in table[0] or ((lang.startswith('en_') or lang == 'C') and 'en_' in str(table[0])):
        debug('Month names for %s already loaded' % lang)
    else:
        debug('Loading month names for %s' % lang)
        table[0].append(lang)
        for f in range(1, 13):
            table[f].append(unaccented(calendar.month_name[f]).lower())
        table[0].append(lang)
        for f in range(1, 13):
            table[f].append(unaccented(calendar.month_abbr[f]).lower().strip('.'))
        info("Added month names for locale [%s], %s, %s ..." % (
            lang, table[1][len(table[1]) - 2], table[1][len(table[1]) - 1]))

    for lang in get_list(CONFIG['IMP_MONTHLANG']):
        try:
            if lang in table[0] or ((lang.startswith('en_') or lang == 'C') and 'en_' in str(table[0])):
                debug('Month names for %s already loaded' % lang)
            else:
                locale.setlocale(locale.LC_ALL, lang)
                debug('Loading month names for %s' % lang)
                table[0].append(lang)
                for f in range(1, 13):
                    table[f].append(unaccented(calendar.month_name[f]).lower())
                table[0].append(lang)
                for f in range(1, 13):
                    table[f].append(unaccented(calendar.month_abbr[f]).lower().strip('.'))
                locale.setlocale(locale.LC_ALL, current_locale)  # restore entry state
                info("Added month names for locale [%s], %s, %s ..." % (
                    lang, table[1][len(table[1]) - 2], table[1][len(table[1]) - 1]))
        except Exception as e:
            locale.setlocale(locale.LC_ALL, current_locale)  # restore entry state
            warn("Unable to load requested locale [%s] %s %s" % (lang, type(e).__name__, str(e)))
            try:
                wanted_lang = lang.split('_')[0]
                params = ['locale', '-a']
                res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                all_locales = make_unicode(res).split()
                locale_list = []
                for a_locale in all_locales:
                    if a_locale.startswith(wanted_lang):
                        locale_list.append(a_locale)
                if locale_list:
                    warn("Found these alternatives: " + str(locale_list))
                else:
                    warn("Unable to find an alternative")
            except Exception as e:
                warn("Unable to get a list of alternatives, %s %s" % (type(e).__name__, str(e)))
            debug("Set locale back to entry state %s" % current_locale)

    # with open(json_file, 'w') as f:
    #    json.dump(table, f)
    return table


def create_version_file(filename):
    # flatpak insists on PROG_DIR being read-only so we have to move version.txt into CACHEDIR
    old_file = os.path.join(lazylibrarian.PROG_DIR, filename)
    version_file = os.path.join(lazylibrarian.CACHEDIR, filename)
    if path_isfile(old_file):
        if not path_isfile(version_file):
            try:
                with open(syspath(old_file), 'r') as s:
                    with open(syspath(version_file), 'w') as d:
                        d.write(s.read())
            except OSError:
                warn("Unable to copy %s" % filename)
        try:
            os.remove(old_file)
        except OSError:
            pass

    return version_file


def init_version_checks(version_file):
    if lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'] == 0:
        debug('Automatic update checks are disabled')
        # pretend we're up to date so we don't keep warning the user
        # version check button will still override this if you want to
        lazylibrarian.CONFIG['LATEST_VERSION'] = lazylibrarian.CONFIG['CURRENT_VERSION']
        lazylibrarian.CONFIG['COMMITS_BEHIND'] = 0
    else:
        # Set the install type (win,git,source) &
        # check the version when the application starts
        versioncheck.check_for_updates()

        debug('Current Version [%s] - Latest remote version [%s] - Install type [%s]' % (
            lazylibrarian.CONFIG['CURRENT_VERSION'], lazylibrarian.CONFIG['LATEST_VERSION'],
            lazylibrarian.CONFIG['INSTALL_TYPE']))

        if check_int(lazylibrarian.CONFIG['GIT_UPDATED'], 0) == 0:
            if lazylibrarian.CONFIG['CURRENT_VERSION'] == lazylibrarian.CONFIG['LATEST_VERSION']:
                if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git' and lazylibrarian.CONFIG['COMMITS_BEHIND'] == 0:
                    lazylibrarian.CONFIG['GIT_UPDATED'] = str(int(time.time()))
                    debug('Setting update timestamp to now')

    # if gitlab doesn't recognise a hash it returns 0 commits
    if lazylibrarian.CONFIG['CURRENT_VERSION'] != lazylibrarian.CONFIG['LATEST_VERSION'] \
            and lazylibrarian.CONFIG['COMMITS_BEHIND'] == 0:
        if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git':
            res, _ = versioncheck.run_git('remote -v')
            if 'gitlab.com' in res:
                warn('Unrecognised version, LazyLibrarian may have local changes')
            else:  # upgrading from github
                warn("Upgrading git origin")
                versioncheck.run_git('remote rm origin')
                versioncheck.run_git('remote add origin https://gitlab.com/LazyLibrarian/LazyLibrarian.git')
                versioncheck.run_git('config master.remote origin')
                versioncheck.run_git('config master.merge refs/heads/master')
                res, _ = versioncheck.run_git('pull origin master')
                if 'CONFLICT' in res:
                    warn("Forcing reset to fix merge conflicts")
                    versioncheck.run_git('reset --hard origin/master')
                versioncheck.run_git('branch --set-upstream-to=origin/master master')
                lazylibrarian.SIGNAL = 'restart'
        elif lazylibrarian.CONFIG['INSTALL_TYPE'] == 'source':
            warn('Unrecognised version [%s] to force upgrade delete %s' % (
                lazylibrarian.CONFIG['CURRENT_VERSION'], version_file))

    if not path_isfile(version_file) and lazylibrarian.CONFIG['INSTALL_TYPE'] == 'source':
        # User may be running an old source zip, so try to force update
        lazylibrarian.CONFIG['COMMITS_BEHIND'] = 1
        lazylibrarian.SIGNAL = 'update'
        # but only once in case the update fails, don't loop
        with open(syspath(version_file), 'w') as f:
            f.write("UNKNOWN SOURCE")

    if lazylibrarian.CONFIG['COMMITS_BEHIND'] <= 0 and lazylibrarian.SIGNAL == 'update':
        lazylibrarian.SIGNAL = None
        if lazylibrarian.CONFIG['COMMITS_BEHIND'] == 0:
            debug('Not updating, LazyLibrarian is already up to date')
        else:
            debug('Not updating, LazyLibrarian has local changes')


def launch_browser(host, port, root):
    import webbrowser
    if host == '0.0.0.0':
        host = 'localhost'

    if lazylibrarian.CONFIG['HTTPS_ENABLED']:
        protocol = 'https'
    else:
        protocol = 'http'
    if root and not root.startswith('/'):
        root = '/' + root
    try:
        webbrowser.open('%s://%s:%i%s/home' % (protocol, host, port, root))
    except Exception as e:
        error('Could not launch browser:%s  %s' % (type(e).__name__, str(e)))


def start_schedulers():
    if not lazylibrarian.UPDATE_MSG:
        lazylibrarian.SHOW_EBOOK = 1 if lazylibrarian.CONFIG['EBOOK_TAB'] else 0
        lazylibrarian.SHOW_AUDIO = 1 if lazylibrarian.CONFIG['AUDIO_TAB'] else 0
        lazylibrarian.SHOW_MAGS = 1 if lazylibrarian.CONFIG['MAG_TAB'] else 0
        lazylibrarian.SHOW_COMICS = 1 if lazylibrarian.CONFIG['COMIC_TAB'] else 0

        if lazylibrarian.CONFIG['ADD_SERIES']:
            lazylibrarian.SHOW_SERIES = 1
        if not lazylibrarian.CONFIG['SERIES_TAB']:
            lazylibrarian.SHOW_SERIES = 0

    if lazylibrarian.CONFIG['GR_URL'] == 'https://goodreads.org':
        lazylibrarian.CONFIG['GR_URL'] = 'https://www.goodreads.com'
    # Crons and scheduled jobs started here
    # noinspection PyUnresolvedReferences
    startscheduler()
    if not lazylibrarian.STOPTHREADS:
        restart_jobs(start='Start')


def logmsg(level, msg):
    # log messages to logger if initialised, or print if not.
    if RotatingLogger.is_initialized():
        if level == 'error':
            error(msg)
        elif level == 'debug':
            debug(msg)
        elif level == 'warn':
            warn(msg)
        else:
            info(msg)
    else:
        print(level.upper(), msg)


def shutdown(restart=False, update=False, quit=True, testing=False):
    if not testing:
        cherrypy.engine.exit()
        logmsg('info', 'cherrypy server exit')
    shutdownscheduler()
    # config_write() don't automatically rewrite config on exit

    if not restart and not update:
        logmsg('info', 'LazyLibrarian (pid %s) is shutting down...' % os.getpid())
        if lazylibrarian.DOCKER:
            # force container to shutdown
            # NOTE we don't seem to have sufficient permission to so this, so disabled the shutdown button
            os.kill(1, signal.SIGKILL)

    updated = False
    if update:
        logmsg('info', 'LazyLibrarian is updating...')
        try:
            updated = versioncheck.update()
            if updated:
                logmsg('info', 'Lazylibrarian version updated')
                makocache = os.path.join(lazylibrarian.CACHEDIR, 'mako')
                rmtree(makocache)
                os.makedirs(makocache)
                lazylibrarian.CONFIG['GIT_UPDATED'] = str(int(time.time()))
                config.config_write('Git')
        except Exception as e:
            logmsg('warn', 'LazyLibrarian failed to update: %s %s. Restarting.' % (type(e).__name__, str(e)))
            logmsg('error', str(traceback.format_exc()))
    if lazylibrarian.PIDFILE:
        logmsg('info', 'Removing pidfile %s' % lazylibrarian.PIDFILE)
        os.remove(syspath(lazylibrarian.PIDFILE))

    if restart:
        logmsg('info', 'LazyLibrarian is restarting ...')
        if not lazylibrarian.DOCKER:
            # Try to use the currently running python executable, as it is known to work
            # if not able to determine, sys.executable returns empty string or None
            # and we have to go looking for it...
            executable = sys.executable

            if not executable:
                prg = "python3"
                if os.name == 'nt':
                    params = ["where", prg]
                    try:
                        executable = subprocess.check_output(params, stderr=subprocess.STDOUT)
                        executable = make_unicode(executable).strip()
                    except Exception as e:
                        debug("where %s failed: %s %s" % (prg, type(e).__name__, str(e)))
                else:
                    params = ["which", prg]
                    try:
                        executable = subprocess.check_output(params, stderr=subprocess.STDOUT)
                        executable = make_unicode(executable).strip()
                    except Exception as e:
                        debug("which %s failed: %s %s" % (prg, type(e).__name__, str(e)))

            if not executable:
                executable = 'python'  # default if not found

            popen_list = [executable, lazylibrarian.FULL_PATH]
            popen_list += lazylibrarian.ARGS
            while '--update' in popen_list:
                popen_list.remove('--update')
            while '--upgrade' in popen_list:
                popen_list.remove('--upgrade')
            if lazylibrarian.LOGLEVEL:
                for item in ['--quiet', '-q', '--debug']:
                    if item in popen_list:
                        popen_list.remove(item)
            if '--nolaunch' not in popen_list:
                popen_list += ['--nolaunch']

            with open(syspath(os.path.join(CONFIG['LOGDIR'], 'upgrade.log')), 'a') as upgradelog:
                if updated:
                    upgradelog.write("%s %s\n" % (time.ctime(),
                                                  'Restarting LazyLibrarian with ' + str(popen_list)))
                subprocess.Popen(popen_list, cwd=os.getcwd())

                if 'HTTP_HOST' in CONFIG:
                    # updating a running instance, not an --update
                    # wait for it to open the httpserver
                    host = lazylibrarian.CONFIG['HTTP_HOST']
                    if '0.0.0.0' in host:
                        host = 'localhost'  # windows doesn't like 0.0.0.0

                    if not host.startswith('http'):
                        host = 'http://' + host

                    # depending on proxy might need host:port/root or just host/root
                    if lazylibrarian.CONFIG['HTTP_ROOT']:
                        server1 = "%s:%s/%s" % (host, lazylibrarian.CONFIG['HTTP_PORT'],
                                                lazylibrarian.CONFIG['HTTP_ROOT'].lstrip('/'))
                        server2 = "%s/%s" % (host, lazylibrarian.CONFIG['HTTP_ROOT'].lstrip('/'))
                    else:
                        server1 = "%s:%s" % (host, lazylibrarian.CONFIG['HTTP_PORT'])
                        server2 = ''

                    msg = "Waiting for %s to start" % server1
                    if updated:
                        upgradelog.write("%s %s\n" % (time.ctime(), msg))
                    logmsg("info", msg)
                    pawse = 18
                    success = False
                    res = ''
                    while pawse:
                        # noinspection PyBroadException
                        try:
                            r = requests.get(server1)
                            res = r.status_code
                            if res == 200 or res == 401:
                                break
                        except Exception:
                            r = None

                        if not r and server2:
                            # noinspection PyBroadException
                            try:
                                r = requests.get(server2)
                                res = r.status_code
                                if res == 200 or res == 401:
                                    break
                            except Exception:
                                pass

                        print("Waiting... %s %s" % (pawse, res))
                        time.sleep(5)
                        pawse -= 1

                    if update:
                        archivename = 'backup.tgz'
                        if success:
                            msg = 'Reached webserver page %s, deleting backup' % res
                            if updated:
                                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                            logmsg("info", msg)
                            try:
                                os.remove(syspath(archivename))
                            except OSError as e:
                                if e.errno != 2:  # doesn't exist is ok
                                    msg = '{} {} {} {}'.format(type(e).__name__, 'deleting backup file:',
                                                               archivename, e.strerror)
                                    logmsg("warn", msg)
                        else:
                            msg = 'Webserver failed to start, reverting update'
                            upgradelog.write("%s %s\n" % (time.ctime(), msg))
                            logmsg("info", msg)
                            if tarfile.is_tarfile(archivename):
                                try:
                                    with tarfile.open(archivename) as tar:
                                        tar.extractall()
                                    success = True
                                except Exception as e:
                                    msg = 'Failed to unpack tarfile %s (%s): %s' % \
                                          (type(e).__name__, archivename, str(e))
                                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                    logmsg("warn", msg)
                            else:
                                msg = "Invalid archive"
                                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                logmsg("warn", msg)
                            if success:
                                msg = "Restarting from backup"
                                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                logmsg("info", msg)
                                subprocess.Popen(popen_list, cwd=os.getcwd())

    if quit:
        logmsg('info', 'Lazylibrarian (pid %s) is exiting now' % os.getpid())
        sys.exit(0)
