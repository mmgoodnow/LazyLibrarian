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

import calendar
import json
import locale
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import tarfile
import time
import traceback
from shutil import rmtree
from typing import Any

import cherrypy
import requests
import urllib3

import lazylibrarian
from lazylibrarian import database, versioncheck
from lazylibrarian.blockhandler import BLOCKHANDLER
from lazylibrarian.cache import fetch_url
from lazylibrarian.common import log_header
from lazylibrarian.config2 import CONFIG, LLConfigHandler
from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.dbupgrade import check_db, db_current_version, upgrade_needed, db_upgrade
from lazylibrarian.filesystem import DIRS, path_isfile, path_isdir, syspath, remove_file, listdir
from lazylibrarian.formatter import check_int, get_list, unaccented, make_unicode
from lazylibrarian.logconfig import LOGCONFIG
from lazylibrarian.notifiers import APPRISE_VER
from lazylibrarian.scheduling import restart_jobs, initscheduler, startscheduler, shutdownscheduler, SchedulerCommand


class StartupLazyLibrarian:
    logger: logging.Logger

    def startup_parsecommandline(self, mainfile, args, testing=False) -> (Any, str):
        """ Parse command line, return options and configfile to use """
        # All initializartion that needs to happen before logging starts
        self.logger.debug('Parsing command line')
        if hasattr(sys, 'frozen'):
            DIRS.set_fullpath_args(os.path.abspath(sys.executable), sys.argv[1:])
        else:
            DIRS.set_fullpath_args(os.path.abspath(mainfile), sys.argv[1:])

        lazylibrarian.DOCKER = '/config' in DIRS.ARGS and DIRS.FULL_PATH.startswith('/app/')

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
        p.add_option('--nolaunch', action="store_true",
                     dest='nolaunch', help="Don't start browser")
        p.add_option('--update', action="store_true",
                     dest='update', help="Update to latest version (only git or source installs)")
        p.add_option('--upgrade', action="store_true",
                     dest='update', help="Update to latest version (only git or source installs)")
        p.add_option('--port',
                     dest='port', default=None,
                     help="Force webinterface to listen on this port")
        p.add_option('--noipv6',
                     dest='noipv6', default=None,
                     help="Do not attempt to use IPv6")
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

        if options.quiet:
            # Don't output anything at all to the console
            LOGCONFIG.remove_console_handlers()

        if options.loglevel:
            try:
                LOGCONFIG.change_root_loglevel(options.loglevel)
            except ValueError as e:
                self.logger.warning(f'loglevel parameter must be a valid log level, error {str(e)}')

        if options.noipv6:
            # A hack, found here: https://stackoverflow.com/questions/33046733/force-requests-to-use-ipv4-ipv6
            urllib3.util.connection.HAS_IPV6 = False  # type: ignore

        if options.daemon:
            if os.name != 'nt':
                lazylibrarian.DAEMON = True
                # lazylibrarian.daemonize()
            else:
                print("Daemonize not supported under Windows, starting normally")

        if options.port:
            options.port = check_int(options.port, 0)

        if options.nojobs:
            lazylibrarian.STOPTHREADS = True
        else:
            lazylibrarian.STOPTHREADS = False

        if options.datadir:
            DIRS.set_datadir(str(options.datadir))
        else:
            DIRS.set_datadir(DIRS.PROG_DIR)

        if options.update:
            lazylibrarian.SIGNAL = 'update'
            # This is the "emergency recovery" update in case lazylibrarian won't start.
            # Set up some dummy values for the update as we have not read the config file yet
            CONFIG.reset_to_default([
                'GIT_PROGRAM', 'GIT_USER', 'GIT_REPO', 'GIT_REPO', 'USER_AGENT', 'HTTP_TIMEOUT', 'PROXY_HOST',
                'SSL_CERTS', 'SSL_VERIFY', 'LOGLIMIT',
            ])
            DIRS.ensure_cache_dir()
            CONFIG['LOGDIR'] = DIRS.ensure_data_subdir('Logs')

            versioncheck.get_install_type()
            if CONFIG['INSTALL_TYPE'] not in ['git', 'source']:
                lazylibrarian.SIGNAL = None
                print('Cannot update, not a git or source installation')
            else:
                self.shutdown(update=True, exit=True, testing=False)

        if options.config:
            configfile = str(options.config)
        else:
            configfile = os.path.join(DIRS.DATADIR, "config.ini")

        if options.pidfile:
            if lazylibrarian.DAEMON:
                lazylibrarian.PIDFILE = str(options.pidfile)

        if not testing:
            self.logger.info("Lazylibrarian (pid %s) is starting up..." % os.getpid())
            # allow a bit of time for old task to exit if restarting. Needs to free logfile and server port.
            time.sleep(2)

        icon = os.path.join(DIRS.CACHEDIR, 'alive.png')
        if path_isfile(icon):
            remove_file(icon)

        return options, configfile

    @staticmethod
    def load_config(configfile: str, options: Any):
        """ Load the config file, perform post-load fixups to ensure consistent state, and
        apply any command line options that override loaded settings """
        config = lazylibrarian.config2.CONFIG  # Don't create a new instance
        config.load_configfile(configfile=configfile)
        config.post_load_fixup()
        DIRS.ensure_log_dir()

        if options.nolaunch:
            config.set_bool('LAUNCH_BROWSER', False)

    def init_loggers(self, console_only: bool):
        """ Initialize log files. Until this is done, do not use the logger """
        if console_only:
            LOGCONFIG.initialize_console_only_log(redact=False)
        else:
            LOGCONFIG.initialize_log_config(
                max_size=CONFIG.get_int('LOGSIZE'),
                max_number=CONFIG.get_int('LOGFILES'),
                redactui=CONFIG.get_bool('LOGREDACT'),
                redactfiles=CONFIG.get_bool('LOGFILEREDACT'))
        self.logger = logging.getLogger(__name__)

    def init_misc(self, config: ConfigDict):
        """ Other initialization."""
        BLOCKHANDLER.set_config(CONFIG, CONFIG.providers("NEWZNAB"), CONFIG.providers("TORZNAB"))
        initscheduler()
        lazylibrarian.UNRARLIB, lazylibrarian.RARFILE = self.get_unrarlib(config)

        if config.get_bool('NO_IPV6'):
            # A hack, found here: https://stackoverflow.com/questions/33046733/force-requests-to-use-ipv4-ipv6
            urllib3.util.connection.HAS_IPV6 = False  # type: ignore

        logger = logging.getLogger(__name__)
        if APPRISE_VER:  # If APPRISE can't be found, show old notifiers
            logger.info("Apprise library (%s) installed" % APPRISE_VER)
        else:
            logger.warning("Did not find Apprise notifications library")
            CONFIG.set_bool('HIDE_OLD_NOTIFIERS', False)

    def init_caches(self, config: LLConfigHandler):
        # override detected encoding if required
        if config['SYS_ENCODING']:
            lazylibrarian.SYS_ENCODING = config['SYS_ENCODING']

        for item in ['book', 'author', 'SeriesCache', 'JSONCache', 'XMLCache', 'WorkCache', 'HTMLCache',
                     'magazine', 'comic', 'IRCCache', 'icrawler', 'mako']:
            cachelocation = os.path.join(DIRS.CACHEDIR, item)
            try:
                os.makedirs(cachelocation)
            except OSError as e:
                if not path_isdir(cachelocation):
                    self.logger.error('Could not create %s: %s' % (cachelocation, e))

        # nest these caches 2 levels to make smaller/faster directory lists
        caches = ["XMLCache", "JSONCache", "WorkCache", "HTMLCache"]
        for item in caches:
            pth = os.path.join(DIRS.CACHEDIR, item)
            for i in '0123456789abcdef':
                for j in '0123456789abcdef':
                    cachelocation = os.path.join(pth, i, j)
                    try:
                        os.makedirs(cachelocation)
                    except OSError as e:
                        if not path_isdir(cachelocation):
                            self.logger.error('Could not create %s: %s' % (cachelocation, e))
            for itm in listdir(pth):
                if len(itm) > 2:
                    os.rename(syspath(os.path.join(pth, itm)),
                              syspath(os.path.join(pth, itm[0], itm[1], itm)))
        last_run_version = None
        last_run_interface = None
        makocache = DIRS.get_mako_cachedir()
        version_file = config.get_mako_versionfile()

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
                self.logger.debug("Python version change (%s to %s)" % (last_run_version, sys.version.split()[0]))
            else:
                self.logger.debug("Previous python version unknown, now %s" % sys.version.split()[0])
            clean_cache = True
        if last_run_interface != config['HTTP_LOOK']:
            if last_run_interface:
                self.logger.debug("Interface change (%s to %s)" % (last_run_interface, config['HTTP_LOOK']))
            else:
                self.logger.debug("Previous interface unknown, now %s" % config['HTTP_LOOK'])
            clean_cache = True
        if clean_cache:
            self.logger.debug("Clearing mako cache")
            rmtree(makocache)
            os.makedirs(makocache)
            with open(version_file, 'w') as fp:
                fp.write(sys.version.split()[0] + ':' + config['HTTP_LOOK'])

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

        if config['BOOK_API'] != 'GoodReads':
            config.set_bool('GR_SYNC', False)
            config.set_bool('GR_FOLLOW', False)
            config.set_bool('GR_FOLLOWNEW', False)

    def init_database(self, config: LLConfigHandler):
        # Initialize the database
        try:
            db = database.DBConnection()
            result = db.match('PRAGMA user_version')
            check = db.match('PRAGMA integrity_check')
            if result:
                version = result[0]
            else:
                version = 0
            self.logger.info("Database is v%s, integrity check: %s" % (version, check[0]))
        except Exception as e:
            self.logger.error("Can't connect to the database: %s %s" % (type(e).__name__, str(e)))
            sys.exit(0)

        curr_ver = upgrade_needed()
        if curr_ver:
            lazylibrarian.UPDATE_MSG = 'Updating database to version %s' % curr_ver
            db_upgrade(curr_ver)

        if version:
            db_changes = check_db()
            if db_changes:
                db.action('PRAGMA user_version=%s' % db_current_version)
                db.action('vacuum')
                self.logger.debug("Upgraded database schema to v%s with %s changes" % (db_current_version, db_changes))

        db.close()
        # group_concat needs sqlite3 >= 3.5.4
        # foreign_key needs sqlite3 >= 3.6.19 (Oct 2009)
        try:
            sqlv = getattr(sqlite3, 'sqlite_version', None)
            parts = sqlv.split('.')
            if int(parts[0]) == 3:
                if int(parts[1]) < 6 or int(parts[1]) == 6 and int(parts[2]) < 19:
                    self.logger.error("Your version of sqlite3 is too old, please upgrade to at least v3.6.19")
                    sys.exit(0)
        except Exception as e:
            self.logger.warning("Unable to parse sqlite3 version: %s %s" % (type(e).__name__, str(e)))

    def init_build_debug_header(self, online):
        debuginfo = log_header(online)
        for item in debuginfo.splitlines():
            if 'missing' in item:
                self.logger.warning(item)

    def init_build_lists(self, config: ConfigDict):
        lazylibrarian.GRGENRES = self.build_genres()
        lazylibrarian.MONTHNAMES = self.build_monthtable(config)
        lazylibrarian.NEWUSER_MSG = self.build_logintemplate()
        lazylibrarian.NEWFILE_MSG = self.build_filetemplate()
        lazylibrarian.BOOKSTRAP_THEMELIST = self.build_bookstrap_themes(DIRS.PROG_DIR)

    @staticmethod
    def get_unrarlib(config: ConfigDict):
        """ Detect presence of unrar library
            Return type of library and rarfile()
        """
        rarfile = None
        # noinspection PyBroadException
        try:
            # noinspection PyUnresolvedReferences
            from unrar import rarfile
            if config.get_int('PREF_UNRARLIB') == 1:
                return 1, rarfile
        except Exception:
            # noinspection PyBroadException
            try:
                from lib.unrar import rarfile
                if config.get_int('PREF_UNRARLIB') == 1:
                    return 1, rarfile
            except Exception:
                pass

        if not rarfile or config.get_int('PREF_UNRARLIB') == 2:
            # noinspection PyBroadException
            try:
                from lib.UnRAR2 import RarFile
                return 2, RarFile
            except Exception:
                if rarfile:
                    return 1, rarfile
        return 0, None

    def build_bookstrap_themes(self, prog_dir):
        themelist = []
        if not path_isdir(os.path.join(prog_dir, 'data', 'interfaces', 'bookstrap')):
            return themelist  # return empty if bookstrap interface not installed

        url = 'http://bootswatch.com/api/3.json'
        result, success = fetch_url(url, headers=None, retry=False)
        if not success:
            self.logger.debug("Error getting bookstrap themes : %s" % result)
            return themelist

        try:
            results = json.loads(result)
            for theme in results['themes']:
                themelist.append(theme['name'].lower())
        except Exception as e:
            # error reading results
            self.logger.warning('JSON Error reading bookstrap themes, %s %s' % (type(e).__name__, str(e)))

        self.logger.info("Bookstrap found %i themes" % len(themelist))
        return themelist

    def build_logintemplate(self):
        default_msg = "Your lazylibrarian username is {username}\nYour password is {password}\n"
        default_msg += "You can log in to lazylibrarian and change these to something more memorable\n"
        default_msg += "You have been given {permission} access\n"
        msg_file = os.path.join(DIRS.DATADIR, 'logintemplate.text')
        if path_isfile(msg_file):
            try:
                # noinspection PyArgumentList
                with open(syspath(msg_file), 'r', encoding='utf-8') as msg_data:
                    res = msg_data.read()
                for item in ["{username}", "{password}", "{permission}"]:
                    if item not in res:
                        self.logger.warning("Invalid login template in %s, no %s" % (msg_file, item))
                        return default_msg
                self.logger.info("Loaded login template from %s" % msg_file)
                return res
            except Exception as e:
                self.logger.error('Failed to load %s, %s %s' % (msg_file, type(e).__name__, str(e)))
        self.logger.debug("Using default login template")
        return default_msg

    def build_filetemplate(self):
        default_msg = "{name}{method}{link}"
        msg_file = os.path.join(DIRS.DATADIR, 'filetemplate.text')
        if path_isfile(msg_file):
            try:
                with open(syspath(msg_file), 'r', encoding='utf-8') as msg_data:
                    res = msg_data.read()
                for item in ["{name}", "{method}", "{link}"]:
                    if item not in res:
                        self.logger.warning("Invalid attachment template in %s, no %s" % (msg_file, item))
                        return default_msg
                self.logger.info("Loaded attachment template from %s" % msg_file)
                return res
            except Exception as e:
                self.logger.error('Failed to load %s, %s %s' % (msg_file, type(e).__name__, str(e)))
        self.logger.debug("Using default attachment template")
        return default_msg

    def build_genres(self):
        for json_file in [os.path.join(DIRS.DATADIR, 'genres.json'),
                          os.path.join(DIRS.PROG_DIR, 'example.genres.json')]:
            if path_isfile(json_file):
                try:
                    with open(syspath(json_file), 'r', encoding='utf-8') as json_data:
                        res = json.load(json_data)
                    self.logger.info("Loaded genres from %s" % json_file)
                    return res
                except Exception as e:
                    self.logger.error('Failed to load %s, %s %s' % (json_file, type(e).__name__, str(e)))
        self.logger.error('No valid genres.json file found')
        return {"genreLimit": 4, "genreUsers": 10, "genreExclude": [], "genreExcludeParts": [], "genreReplace": {}}

    def build_monthtable(self, config: ConfigDict):
        table = []
        json_file = os.path.join(DIRS.DATADIR, 'monthnames.json')
        if path_isfile(json_file):
            try:
                with open(syspath(json_file)) as json_data:
                    table = json.load(json_data)
                mlist = ''
                # list alternate entries as each language is in twice (long and short month names)
                for item in table[0][::2]:
                    mlist += item + ' '
                self.logger.debug('Loaded monthnames.json : %s' % mlist)
            except Exception as e:
                self.logger.error('Failed to load monthnames.json, %s %s' % (type(e).__name__, str(e)))

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

        if len(get_list(config['IMP_MONTHLANG'])) == 0:  # any extra languages wanted?
            return table
        try:
            current_locale = locale.setlocale(locale.LC_ALL, '')  # read current state.
            if 'LC_CTYPE' in current_locale:
                current_locale = locale.setlocale(locale.LC_CTYPE, '')
            # getdefaultlocale() doesnt seem to work as expected on windows, returns 'None'
            self.logger.debug('Current locale is %s' % current_locale)
        except locale.Error as e:
            self.logger.debug("Error getting current locale : %s" % str(e))
            return table

        lang = str(current_locale)
        # check not already loaded, also all english variants and 'C' use the same month names
        if lang in table[0] or ((lang.startswith('en_') or lang == 'C') and 'en_' in str(table[0])):
            self.logger.debug('Month names for %s already loaded' % lang)
        else:
            self.logger.debug('Loading month names for %s' % lang)
            table[0].append(lang)
            for f in range(1, 13):
                table[f].append(unaccented(calendar.month_name[f]).lower())
            table[0].append(lang)
            for f in range(1, 13):
                table[f].append(unaccented(calendar.month_abbr[f]).lower().strip('.'))
            self.logger.info("Added month names for locale [%s], %s, %s ..." % (
                lang, table[1][len(table[1]) - 2], table[1][len(table[1]) - 1]))

        for lang in get_list(config['IMP_MONTHLANG']):
            try:
                if lang in table[0] or ((lang.startswith('en_') or lang == 'C') and 'en_' in str(table[0])):
                    self.logger.debug('Month names for %s already loaded' % lang)
                else:
                    locale.setlocale(locale.LC_ALL, lang)
                    self.logger.debug('Loading month names for %s' % lang)
                    table[0].append(lang)
                    for f in range(1, 13):
                        table[f].append(unaccented(calendar.month_name[f]).lower())
                    table[0].append(lang)
                    for f in range(1, 13):
                        table[f].append(unaccented(calendar.month_abbr[f]).lower().strip('.'))
                    locale.setlocale(locale.LC_ALL, current_locale)  # restore entry state
                    self.logger.info("Added month names for locale [%s], %s, %s ..." % (
                        lang, table[1][len(table[1]) - 2], table[1][len(table[1]) - 1]))
            except Exception as e:
                locale.setlocale(locale.LC_ALL, current_locale)  # restore entry state
                self.logger.warning("Unable to load requested locale [%s] %s %s" % (lang, type(e).__name__, str(e)))
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
                        self.logger.warning("Found these alternatives: " + str(locale_list))
                    else:
                        self.logger.warning("Unable to find an alternative")
                except Exception as e:
                    self.logger.warning("Unable to get a list of alternatives, %s %s" % (type(e).__name__, str(e)))
                self.logger.debug("Set locale back to entry state %s" % current_locale)

        # with open(json_file, 'w') as f:
        #    json.dump(table, f)
        return table

    def create_version_file(self, filename):
        # flatpak insists on PROG_DIR being read-only so we have to move version.txt into CACHEDIR
        old_file = os.path.join(DIRS.PROG_DIR, filename)
        version_file = os.path.join(DIRS.CACHEDIR, filename)
        if path_isfile(old_file):
            if not path_isfile(version_file):
                try:
                    with open(syspath(old_file), 'r') as s:
                        with open(syspath(version_file), 'w') as d:
                            d.write(s.read())
                except OSError:
                    self.logger.warning(f"Unable to copy {filename}")
            try:
                os.remove(old_file)
            except OSError:
                pass

        return version_file

    def init_version_checks(self, version_file):
        if CONFIG.get_int('VERSIONCHECK_INTERVAL') == 0:
            self.logger.debug('Automatic update checks are disabled')
            # pretend we're up to date so we don't keep warning the user
            # version check button will still override this if you want to
            CONFIG.set_str('LATEST_VERSION', CONFIG['CURRENT_VERSION'])
            CONFIG.set_int('COMMITS_BEHIND', 0)
        else:
            # Set the install type (win,git,source) &
            # check the version when the application starts
            versioncheck.check_for_updates()

            self.logger.debug('Current Version [%s] - Latest remote version [%s] - Install type [%s]' % (
                CONFIG['CURRENT_VERSION'], CONFIG['LATEST_VERSION'],
                CONFIG['INSTALL_TYPE']))

            if CONFIG.get_int('GIT_UPDATED') == 0:
                if CONFIG['CURRENT_VERSION'] == CONFIG['LATEST_VERSION']:
                    if CONFIG['INSTALL_TYPE'] == 'git' and CONFIG.get_int('COMMITS_BEHIND') == 0:
                        CONFIG.set_int('GIT_UPDATED', int(time.time()))
                        self.logger.debug('Setting update timestamp to now')

        # if gitlab doesn't recognise a hash it returns 0 commits
        if CONFIG['CURRENT_VERSION'] != CONFIG['LATEST_VERSION'] \
                and CONFIG.get_int('COMMITS_BEHIND') == 0:
            if CONFIG['INSTALL_TYPE'] == 'git':
                res, _ = versioncheck.run_git('remote -v')
                if 'gitlab.com' in str(res):
                    self.logger.warning('Unrecognised version, LazyLibrarian may have local changes')
            elif CONFIG['INSTALL_TYPE'] == 'source':
                self.logger.warning('Unrecognised version [%s] to force upgrade delete %s' % (
                    CONFIG['CURRENT_VERSION'], version_file))

        if not path_isfile(version_file) and CONFIG['INSTALL_TYPE'] == 'source':
            # User may be running an old source zip, so try to force update
            CONFIG.set_int('COMMITS_BEHIND', 1)
            lazylibrarian.SIGNAL = 'update'
            # but only once in case the update fails, don't loop
            with open(syspath(version_file), 'w') as f:
                f.write("UNKNOWN SOURCE")

        if CONFIG.get_int('COMMITS_BEHIND') <= 0:
            lazylibrarian.SIGNAL = None
            if CONFIG.get_int('COMMITS_BEHIND') == 0:
                self.logger.debug('Not updating, LazyLibrarian is already up to date')
            else:
                self.logger.debug('Not updating, LazyLibrarian has local changes')

        if '**MANUAL**' in lazylibrarian.COMMIT_LIST:
            lazylibrarian.SIGNAL = None
            self.logger.info("Update available, but needs manual installation")

    def launch_browser(self, host, port, root):
        import webbrowser
        if host == '0.0.0.0':
            host = 'localhost'

        if CONFIG.get_bool('HTTPS_ENABLED'):
            protocol = 'https'
        else:
            protocol = 'http'
        if root and not root.startswith('/'):
            root = '/' + root
        try:
            webbrowser.open(f'{protocol}://{host}:{port}{root}/home')
        except Exception as e:
            self.logger.error('Could not launch browser:%s  %s' % (type(e).__name__, str(e)))

    def start_schedulers(self):
        if CONFIG['GR_URL'] == 'https://goodreads.org':
            CONFIG.set_url('GR_URL', 'https://www.goodreads.com')
        # Crons and scheduled jobs started here
        # noinspection PyUnresolvedReferences
        startscheduler()
        if not lazylibrarian.STOPTHREADS:
            restart_jobs(command=SchedulerCommand.START)

    def shutdown(self, restart=False, update=False, exit=False, testing=False):
        if not testing:
            cherrypy.engine.exit()
            time.sleep(2)
            state = str(cherrypy.engine.state)
            self.logger.info("Cherrypy state %s" % state)
        shutdownscheduler()
        if not testing:
            if self.logger.isEnabledFor(logging.DEBUG):  # TODO add a separate setting
                CONFIG.create_access_summary(syspath(DIRS.get_logfile('configaccess.log')))
            CONFIG.add_access_errors_to_log()
            CONFIG.save_config_and_backup_old(restart_jobs=False)

        if not restart and not update:
            self.logger.info('LazyLibrarian (pid %s) is shutting down...' % os.getpid())
            if lazylibrarian.DOCKER:
                # force container to shutdown
                # NOTE we don't seem to have sufficient permission to so this, so disabled the shutdown button
                os.kill(1, signal.SIGKILL)

        # We are now shutting down. Remove all file handlers from the logger, keeping only console handlers
        rootlogger = logging.getLogger('root')
        for handler in rootlogger.handlers:
            if handler.name != 'console':
                rootlogger.removeHandler(handler)

        updated = False
        if update:
            self.logger.info('LazyLibrarian is updating...')
            try:
                updated = versioncheck.update()
                if updated:
                    self.logger.info('Lazylibrarian version updated')
                    makocache = os.path.join(DIRS.CACHEDIR, 'mako')
                    rmtree(makocache)
                    os.makedirs(makocache)
                    CONFIG.set_int('GIT_UPDATED', int(time.time()))
                    CONFIG.save_config_and_backup_old(section='Git')
            except Exception as e:
                self.logger.warning('LazyLibrarian failed to update: %s %s. Restarting.' % (type(e).__name__, str(e)))
                self.logger.error(str(traceback.format_exc()))

        if lazylibrarian.PIDFILE:
            self.logger.info('Removing pidfile %s' % lazylibrarian.PIDFILE)
            os.remove(syspath(lazylibrarian.PIDFILE))

        if restart and not exit:
            self.logger.info('LazyLibrarian is restarting ...')
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
                            self.logger.debug("where %s failed: %s %s" % (prg, type(e).__name__, str(e)))
                    else:
                        params = ["which", prg]
                        try:
                            executable = subprocess.check_output(params, stderr=subprocess.STDOUT)
                            executable = make_unicode(executable).strip()
                        except Exception as e:
                            self.logger.debug("which %s failed: %s %s" % (prg, type(e).__name__, str(e)))

                if not executable:
                    executable = 'python'  # default if not found

                popen_list = [executable, DIRS.FULL_PATH]
                popen_list += DIRS.ARGS
                while '--update' in popen_list:
                    popen_list.remove('--update')
                while '--upgrade' in popen_list:
                    popen_list.remove('--upgrade')
                if '--nolaunch' not in popen_list:
                    popen_list += ['--nolaunch']

                with open(syspath(DIRS.get_logfile('upgrade.log')), 'a') as upgradelog:
                    if updated:
                        upgradelog.write("%s %s\n" % (time.ctime(),
                                                      'Restarting LazyLibrarian with ' + str(popen_list)))
                    subprocess.Popen(popen_list, cwd=os.getcwd())

                    if 'HTTP_HOST' in CONFIG:
                        # updating a running instance, not an --update
                        # wait for it to open the httpserver
                        host = CONFIG['HTTP_HOST']
                        if '0.0.0.0' in host:
                            host = 'localhost'  # windows doesn't like 0.0.0.0

                        if not host.startswith('http'):
                            host = 'http://' + host

                        # depending on proxy might need host:port/root or just host/root
                        if CONFIG['HTTP_ROOT']:
                            server1 = "%s:%s/%s" % (host, CONFIG['HTTP_PORT'],
                                                    CONFIG['HTTP_ROOT'].lstrip('/'))
                            server2 = "%s/%s" % (host, CONFIG['HTTP_ROOT'].lstrip('/'))
                        else:
                            server1 = "%s:%s" % (host, CONFIG['HTTP_PORT'])
                            server2 = ''

                        msg = "Waiting for %s to start" % server1
                        if updated:
                            upgradelog.write("%s %s\n" % (time.ctime(), msg))
                        self.logger.info(msg)
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
                                self.logger.info(msg)
                                try:
                                    os.remove(syspath(archivename))
                                except OSError as e:
                                    if e.errno != 2:  # doesn't exist is ok
                                        msg = '{} {} {} {}'.format(type(e).__name__, 'deleting backup file:',
                                                                   archivename, e.strerror)
                                        self.logger.warning(msg)
                            else:
                                msg = 'Webserver failed to start, reverting update'
                                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                self.logger.info(msg)
                                if tarfile.is_tarfile(archivename):
                                    try:
                                        with tarfile.open(archivename) as tar:
                                            tar.extractall()
                                        success = True
                                    except Exception as e:
                                        msg = 'Failed to unpack tarfile %s (%s): %s' % \
                                              (type(e).__name__, archivename, str(e))
                                        upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                        self.logger.warning(msg)
                                else:
                                    msg = "Invalid archive"
                                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                    self.logger.warning(msg)
                                if success:
                                    msg = "Restarting from backup"
                                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                                    self.logger.info(msg)
                                    subprocess.Popen(popen_list, cwd=os.getcwd())

        if exit:
            self.logger.info('Lazylibrarian (pid %s) is exiting now' % os.getpid())
            sys.exit(0)
