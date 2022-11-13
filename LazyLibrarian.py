#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
from __future__ import print_function

import locale
import os
import sys
import time

import lazylibrarian
from lazylibrarian import startup, webStart, logger, versioncheck, dbupgrade, notifiers
from lazylibrarian.formatter import check_int, thread_name
from lazylibrarian.versioncheck import run_git
from lazylibrarian.common import path_isfile, syspath
# noinspection PyUnresolvedReferences
from six.moves import configparser


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


def main():
   # rename this thread
    thread_name("MAIN")

    options = startup.startup_parsecommandline(__file__, args = sys.argv[1:], seconds_to_sleep = 2)

    # REMINDER ############ NO LOGGING BEFORE HERE ###############
    # There is no point putting in any logging above this line, as its not set till after initialize.
    startup.initialize(options)

    # flatpak insists on PROG_DIR being read-only so we have to move version.txt into CACHEDIR
    old_file = os.path.join(lazylibrarian.PROG_DIR, 'version.txt')
    version_file = os.path.join(lazylibrarian.CACHEDIR, 'version.txt')
    if path_isfile(old_file):
        if not path_isfile(version_file):
            try:
                with open(syspath(old_file), 'r') as s:
                    with open(syspath(version_file), 'w') as d:
                        d.write(s.read())
            except OSError:
                logger.warn("Unable to copy version.txt")
        try:
            os.remove(old_file)
        except OSError:
            pass

    if lazylibrarian.CONFIG['VERSIONCHECK_INTERVAL'] == 0:
        logger.debug('Automatic update checks are disabled')
        # pretend we're up to date so we don't keep warning the user
        # version check button will still override this if you want to
        lazylibrarian.CONFIG['LATEST_VERSION'] = lazylibrarian.CONFIG['CURRENT_VERSION']
        lazylibrarian.CONFIG['COMMITS_BEHIND'] = 0
    else:
        # Set the install type (win,git,source) &
        # check the version when the application starts
        versioncheck.check_for_updates()

        logger.debug('Current Version [%s] - Latest remote version [%s] - Install type [%s]' % (
            lazylibrarian.CONFIG['CURRENT_VERSION'], lazylibrarian.CONFIG['LATEST_VERSION'],
            lazylibrarian.CONFIG['INSTALL_TYPE']))

        if check_int(lazylibrarian.CONFIG['GIT_UPDATED'], 0) == 0:
            if lazylibrarian.CONFIG['CURRENT_VERSION'] == lazylibrarian.CONFIG['LATEST_VERSION']:
                if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git' and lazylibrarian.CONFIG['COMMITS_BEHIND'] == 0:
                    lazylibrarian.CONFIG['GIT_UPDATED'] = str(int(time.time()))
                    logger.debug('Setting update timestamp to now')

    # if gitlab doesn't recognise a hash it returns 0 commits
    if lazylibrarian.CONFIG['CURRENT_VERSION'] != lazylibrarian.CONFIG['LATEST_VERSION'] \
            and lazylibrarian.CONFIG['COMMITS_BEHIND'] == 0:
        if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git':
            res, _ = run_git('remote -v')
            if 'gitlab.com' in res:
                logger.warn('Unrecognised version, LazyLibrarian may have local changes')
            else:  # upgrading from github
                logger.warn("Upgrading git origin")
                run_git('remote rm origin')
                run_git('remote add origin https://gitlab.com/LazyLibrarian/LazyLibrarian.git')
                run_git('config master.remote origin')
                run_git('config master.merge refs/heads/master')
                res, _ = run_git('pull origin master')
                if 'CONFLICT' in res:
                    logger.warn("Forcing reset to fix merge conflicts")
                    run_git('reset --hard origin/master')
                run_git('branch --set-upstream-to=origin/master master')
                lazylibrarian.SIGNAL = 'restart'
        elif lazylibrarian.CONFIG['INSTALL_TYPE'] == 'source':
            logger.warn('Unrecognised version [%s] to force upgrade delete %s' % (
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
            logger.debug('Not updating, LazyLibrarian is already up to date')
        else:
            logger.debug('Not updating, LazyLibrarian has local changes')

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

    curr_ver = dbupgrade.upgrade_needed()
    if curr_ver:
        lazylibrarian.UPDATE_MSG = 'Updating database to version %s' % curr_ver
        dbupgrade.dbupgrade(curr_ver)

    startup.start()

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
