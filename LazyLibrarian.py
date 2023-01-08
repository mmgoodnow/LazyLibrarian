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

import logging
import sys
import time

import lazylibrarian
from lazylibrarian import startup, webStart
from lazylibrarian.formatter import thread_name

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
    starter = startup.StartupLazyLibrarian()
    # Set up a console-only logger until config is read
    starter.init_loggers(console_only=True)
    # Read command line and return options
    options, configfile = starter.startup_parsecommandline(__file__, args=sys.argv[1:])
    # Load config.ini and initialize CONFIG and DIRS
    starter.load_config(configfile, options)
    # Read logging config and initialize loggers
    starter.init_loggers(console_only=False)
    # Run initialization that needs CONFIG to be loaded
    starter.init_misc(lazylibrarian.config2.CONFIG)
    starter.init_caches(lazylibrarian.config2.CONFIG)
    starter.init_database(lazylibrarian.config2.CONFIG)
    starter.init_build_debug_header(online=True)
    starter.init_build_lists(lazylibrarian.config2.CONFIG)
    logger = logging.getLogger(__name__)

    version_file = starter.create_version_file('version.txt')
    starter.init_version_checks(version_file)

    if lazylibrarian.DAEMON:
        lazylibrarian.daemonize()

    # Try to start the server.
    if options.port:
        lazylibrarian.config2.CONFIG.set_int('HTTP_PORT', options.port)
        logger.info('Starting LazyLibrarian on forced port: %s, webroot "%s"' %
                    (lazylibrarian.config2.CONFIG['HTTP_PORT'], lazylibrarian.config2.CONFIG['HTTP_ROOT']))
    else:
        logger.info('Starting LazyLibrarian on port: %s, webroot "%s"' %
                    (lazylibrarian.config2.CONFIG['HTTP_PORT'], lazylibrarian.config2.CONFIG['HTTP_ROOT']))

    webStart.initialize({
        'http_port': lazylibrarian.config2.CONFIG.get_int('HTTP_PORT'),
        'http_host': lazylibrarian.config2.CONFIG['HTTP_HOST'],
        'http_root': lazylibrarian.config2.CONFIG['HTTP_ROOT'],
        'http_user': lazylibrarian.config2.CONFIG['HTTP_USER'],
        'http_pass': lazylibrarian.config2.CONFIG['HTTP_PASS'],
        'http_proxy': lazylibrarian.config2.CONFIG.get_bool('HTTP_PROXY'),
        'https_enabled': lazylibrarian.config2.CONFIG.get_bool('HTTPS_ENABLED'),
        'https_cert': lazylibrarian.config2.CONFIG['HTTPS_CERT'],
        'https_key': lazylibrarian.config2.CONFIG['HTTPS_KEY'],
        'opds_enabled': lazylibrarian.config2.CONFIG['OPDS_ENABLED'],
        'opds_authentication': lazylibrarian.config2.CONFIG.get_bool('OPDS_AUTHENTICATION'),
        'opds_username': lazylibrarian.config2.CONFIG['OPDS_USERNAME'],
        'opds_password': lazylibrarian.config2.CONFIG['OPDS_PASSWORD'],
        'authentication': lazylibrarian.config2.CONFIG['AUTH_TYPE'],
        'login_timeout': 43800,
    })

    if options.userid:
        lazylibrarian.LOGINUSER = options.userid
    else:
        lazylibrarian.LOGINUSER = None

    if lazylibrarian.config2.CONFIG.get_bool('LAUNCH_BROWSER') and not options.nolaunch:
        starter.launch_browser(lazylibrarian.config2.CONFIG['HTTP_HOST'],
                               lazylibrarian.config2.CONFIG['HTTP_PORT'],
                               lazylibrarian.config2.CONFIG['HTTP_ROOT'])

    starter.start_schedulers()

    while True:
        if not lazylibrarian.SIGNAL:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                starter.shutdown(quit=True)
        else:
            if lazylibrarian.SIGNAL == 'shutdown':
                starter.shutdown(quit=True)
            elif lazylibrarian.SIGNAL == 'restart':
                starter.shutdown(restart=True)
            elif lazylibrarian.SIGNAL == 'update':
                starter.shutdown(update=True)
            lazylibrarian.SIGNAL = None


if __name__ == "__main__":
    main()
