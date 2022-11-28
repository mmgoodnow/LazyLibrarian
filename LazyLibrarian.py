#!/usr/bin/env python
# -*- coding: UTF-8 -*-

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
#   Main file for starting LazyLibrarian

from __future__ import print_function

import sys
import time

import lazylibrarian
from lazylibrarian import startup, webStart, logger, notifiers
from lazylibrarian.formatter import thread_name
import configparser


# The following should probably be made configurable at the settings level
# This fix is put in place for systems with broken SSL (like QNAP)
opt_out_of_certificate_verification = True
if opt_out_of_certificate_verification:
    # noinspection PyBroadException
    try:
        import ssl
        # noinspection PyProtectedMember
        ssl._create_default_https_context = ssl._create_unverified_context
    except Exception:
        pass

# ==== end block (should be configurable at settings level)

if sys.version[0] != '3':
    sys.stderr.write("This version of lazylibrarian requires python 3\n")
    exit(0)
    
def main():
   # rename this thread
    thread_name("MAIN")

    options = startup.startup_parsecommandline(__file__, args = sys.argv[1:], seconds_to_sleep = 2)

    startup.init_logs()
    startup.init_config()
    startup.init_caches()
    startup.init_database()
    startup.init_build_debug_header(online = True)
    startup.init_build_lists()

    version_file = startup.create_version_file('version.txt')
    startup.init_version_checks(version_file)

    if lazylibrarian.APPRISE and lazylibrarian.APPRISE[0].isdigit():
        logger.info("Apprise library (%s) installed" % lazylibrarian.APPRISE)
    else:
        logger.warn("Looking for Apprise library: %s" % lazylibrarian.APPRISE)
        lazylibrarian.APPRISE = ''
        lazylibrarian.CONFIG['HIDE_OLD_NOTIFIERS'] = False

    if lazylibrarian.DAEMON:
        lazylibrarian.daemonize()

    # Try to start the server.
    if options.port:
        lazylibrarian.CONFIG['HTTP_PORT'] = options.port
        logger.info('Starting LazyLibrarian on forced port: %s, webroot "%s"' %
                    (lazylibrarian.CONFIG['HTTP_PORT'], lazylibrarian.CONFIG['HTTP_ROOT']))
    else:
        lazylibrarian.CONFIG['HTTP_PORT'] = int(lazylibrarian.CONFIG['HTTP_PORT'])
        logger.info('Starting LazyLibrarian on port: %s, webroot "%s"' %
                    (lazylibrarian.CONFIG['HTTP_PORT'], lazylibrarian.CONFIG['HTTP_ROOT']))

    webStart.initialize({
        'http_port': lazylibrarian.CONFIG['HTTP_PORT'],
        'http_host': lazylibrarian.CONFIG['HTTP_HOST'],
        'http_root': lazylibrarian.CONFIG['HTTP_ROOT'],
        'http_user': lazylibrarian.CONFIG['HTTP_USER'],
        'http_pass': lazylibrarian.CONFIG['HTTP_PASS'],
        'http_proxy': lazylibrarian.CONFIG['HTTP_PROXY'],
        'https_enabled': lazylibrarian.CONFIG['HTTPS_ENABLED'],
        'https_cert': lazylibrarian.CONFIG['HTTPS_CERT'],
        'https_key': lazylibrarian.CONFIG['HTTPS_KEY'],
        'opds_enabled': lazylibrarian.CONFIG['OPDS_ENABLED'],
        'opds_authentication': lazylibrarian.CONFIG['OPDS_AUTHENTICATION'],
        'opds_username': lazylibrarian.CONFIG['OPDS_USERNAME'],
        'opds_password': lazylibrarian.CONFIG['OPDS_PASSWORD'],
        'authentication': lazylibrarian.CONFIG['AUTH_TYPE'],
        'login_timeout': 43800,
    })

    if options.userid:
        lazylibrarian.LOGINUSER = options.userid
    else:
        lazylibrarian.LOGINUSER = None

    if lazylibrarian.CONFIG['LAUNCH_BROWSER'] and not options.nolaunch:
        startup.launch_browser(lazylibrarian.CONFIG['HTTP_HOST'],
                                     lazylibrarian.CONFIG['HTTP_PORT'],
                                     lazylibrarian.CONFIG['HTTP_ROOT'])

    startup.start_schedulers()

    while True:
        if not lazylibrarian.SIGNAL:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                startup.shutdown()
        else:
            if lazylibrarian.SIGNAL == 'shutdown':
                startup.shutdown()
            elif lazylibrarian.SIGNAL == 'restart':
                startup.shutdown(restart=True)
            elif lazylibrarian.SIGNAL == 'update':
                startup.shutdown(restart=True, update=True)
            lazylibrarian.SIGNAL = None


if __name__ == "__main__":
    main()
