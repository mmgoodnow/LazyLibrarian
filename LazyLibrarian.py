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
import signal
import sys
import time

import subprocess
import os
import shutil
import importlib


# Remove bundled libraries and replace with a system one if available
# Only check if not on docker as we can't modify the contents of a docker container
# Record the results in a file so we don't check at every startup
docker = '/config' in sys.argv and sys.argv[0].startswith('/app/')
bypass_file = os.path.join(os.getcwd(), 'unbundled.libs')
if not docker and not os.path.isfile(bypass_file):
    dependencies = [
        # pip name, bundled name, aka
        ('bs4', '', ''),
        ('html5lib', '', ''),
        ('webencodings', '', ''),
        ('requests', '', ''), 
        ('urllib3', '', ''),
        ('pyOpenSSL', None, 'OpenSSL'),
        ('cherrypy', '', ''),
        ('cherrypy_cors', 'cherrypy_cors.py', ''),
        ('httpagentparser', '', ''),
        ('mako', '', ''),
        ('httplib2', '', ''),
        ('Pillow', None, 'PIL'),
        ('apprise', None, ''),
        ('PyPDF3', '', ''),
        ('python_magic', 'magic', 'magic'),
        ('thefuzz', '', ''),
        ('Levenshtein', None, ''),
        ('deluge_client', '', ''),
    ]

    bundled = {}
    distro = {}
    missing = []
    for item in dependencies:
        if item[1] is not None:  # there may be a bundled version
            name = item[0]
            for finder in sys.meta_path:
                spec = finder.find_spec(importlib.util.resolve_name(name, None), None)
                if spec is not None:
                    if 'LazyLibrarian' in spec.origin:
                        bundled[name] = spec.origin
                    else:
                        distro[name] = spec.origin

    current_dir = sys.path.pop(0)  # don't look in current working directory
    for item in dependencies:
        name = item[2] if item[2] else item[0]
        if name not in distro:
            spec = None
            for finder in sys.meta_path:
                spec = finder.find_spec(importlib.util.resolve_name(name, None), None)
                if spec is not None:
                    distro[name] = spec.origin
                    break
            if not spec:
                missing.append(name)

    for item in missing:
        try:
            reply = subprocess.run([sys.executable, '-m', 'pip', 'install', item], check=True, capture_output=True, text=True).stdout
            distro[item] = 'new install'
            missing.remove(item)
        except subprocess.CalledProcessError as e:
            print(str(e))

    if missing:
        print("Failed to install %s" % str(missing))

    deletable = []
    for item in dependencies:
        if item[1] is not None:
            if item[2] and item[2] in distro or item[0] in distro:
                deletable.append(item[1] if item[1] else item[0])

    cwd = os.getcwd()
    removed = []
    for item in deletable:
        f = os.path.join(cwd, item)
        # might have already been deleted
        if os.path.isdir(f):
            shutil.rmtree(f)
            print("Removed bundled ", item)
            removed.append(item)
        if os.path.isfile(f):
            os.remove(f)
            print("Removed bundled ", item)
            removed.append(item)
    with open(bypass_file, 'w') as f:
        f.write(str(removed))
    sys.path.insert(0, current_dir)

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

MIN_PYTHON_VERSION = (3, 7)

if sys.version_info < MIN_PYTHON_VERSION:
    sys.stderr.write("This version of Lazylibrarian requires Python %d.%d or later.\n" % MIN_PYTHON_VERSION)
    exit(0)


def sig_shutdown():
    lazylibrarian.SIGNAL = 'shutdown'


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

    signal.signal(signal.SIGTERM, sig_shutdown)

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
