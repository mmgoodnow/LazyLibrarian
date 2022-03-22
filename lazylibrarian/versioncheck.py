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

from __future__ import print_function

import os
import platform
import re
import subprocess
import tarfile
import threading
import time
from shutil import rmtree

import lazylibrarian
try:
    import urllib3
    import requests
except (ImportError, ModuleNotFoundError):
    try:
        import lib.requests as requests
    except (ImportError, ModuleNotFoundError) as e:
        print(str(e))
        print('Unable to load requests library')
        exit(1)

from lazylibrarian import logger, version, database
from lazylibrarian.common import get_user_agent, proxy_list, walk, listdir, path_isdir, syspath
from lazylibrarian.formatter import check_int, make_unicode, thread_name


def logmsg(level, msg):
    # log messages to logger if initialised, or print if not.
    if lazylibrarian.__INITIALIZED__:
        if level == 'error':
            logger.error(msg)
        elif level == 'info':
            logger.info(msg)
        elif level == 'debug':
            logger.debug(msg)
        elif level == 'warn':
            logger.warn(msg)
        else:
            logger.info(msg)
    else:
        print(level.upper(), msg)


def run_git(args):
    # Function to execute GIT commands taking care of error logging etc
    if lazylibrarian.CONFIG['GIT_PROGRAM']:
        git_locations = ['"' + lazylibrarian.CONFIG['GIT_PROGRAM'] + '"']
    else:
        git_locations = ['git']

    if platform.system().lower() == 'darwin':
        git_locations.append('/usr/local/git/bin/git')

    output = None
    err = None

    for cur_git in git_locations:

        cmd = cur_git + ' ' + args

        cmd = make_unicode(cmd)
        try:
            logmsg('debug', 'Execute: "%s" with shell in %s' % (cmd, lazylibrarian.PROG_DIR))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 shell=True, cwd=lazylibrarian.PROG_DIR)
            output, err = p.communicate()
            output = make_unicode(output).strip('\n')

            logmsg('debug', 'Git output: [%s]' % output)
            if err:
                err = make_unicode(err)
                logmsg('debug', 'Git err: [%s]' % err)
            elif not output:
                output = ' '

        except OSError:
            logmsg('debug', 'Command ' + cmd + ' didn\'t work, couldn\'t find git')
            continue

        if 'not found' in output or "not recognized as an internal or external command" in output:
            logmsg('debug', 'Unable to find git with command ' + cmd)
            logmsg('error', 'git not found - please ensure git executable is in your PATH')
            output = None
        elif 'fatal:' in output or err:
            logmsg('error', 'Git returned bad info. Are you sure this is a git installation?')
            output = None
        elif output:
            break

    return output, err


#
# function to determine what type of install we are on & sets current Branch value
# - windows
# - git based
# - deployed source code


def get_install_type():
    # need a way of detecting if we are running a windows .exe file
    # (which we can't upgrade)  rather than just running git or source on windows
    # We use a string in the version.py file for this
    # FUTURE:   Add a version number string in this file too?
    try:
        install = version.LAZYLIBRARIAN_VERSION.lower()
    except AttributeError:
        install = 'unknown'

    if install in ['windows', 'win32build']:
        lazylibrarian.CONFIG['INSTALL_TYPE'] = 'win'
        lazylibrarian.CURRENT_BRANCH = 'Windows'

    elif install == 'package':  # deb, rpm, other non-upgradeable
        lazylibrarian.CONFIG['INSTALL_TYPE'] = 'package'
        lazylibrarian.CONFIG['GIT_BRANCH'] = 'master'

    elif path_isdir(os.path.join(lazylibrarian.PROG_DIR, '.git')):
        lazylibrarian.CONFIG['INSTALL_TYPE'] = 'git'
        lazylibrarian.CONFIG['GIT_BRANCH'] = get_current_git_branch()
    else:
        lazylibrarian.CONFIG['INSTALL_TYPE'] = 'source'
        lazylibrarian.CONFIG['GIT_BRANCH'] = 'master'

    logmsg('debug', '%s install detected. Setting Branch to [%s]' %
           (lazylibrarian.CONFIG['INSTALL_TYPE'], lazylibrarian.CONFIG['GIT_BRANCH']))


def get_current_version():
    # Establish the version of the installed app for Source or GIT only
    # Global variable set in LazyLibrarian.py on startup as it should be
    if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'win':
        logmsg('debug', 'Windows install - no update available')

        # Don't have a way to update exe yet, but don't want to set VERSION to None
        version_string = 'Windows Install'

    elif lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git':
        output, _ = run_git('rev-parse HEAD')

        if not output:
            logmsg('error', 'Couldn\'t find latest git installed version.')
            cur_commit_hash = 'GIT Cannot establish version'
        else:
            cur_commit_hash = output.strip()

            # noinspection PyTypeChecker
            if not re.match('^[a-z0-9]+$', cur_commit_hash):
                logmsg('error', 'Output doesn\'t look like a hash, not using it')
                cur_commit_hash = 'GIT invalid hash return'

        version_string = cur_commit_hash

    elif lazylibrarian.CONFIG['INSTALL_TYPE'] in ['source']:

        version_file = os.path.join(lazylibrarian.CACHEDIR, 'version.txt')

        if not os.path.isfile(version_file):
            version_string = 'No Version File'
            logmsg('debug', 'Version file [%s] missing.' % version_file)
            return version_string
        else:
            fp = open(version_file, 'r')
            current_version = fp.read().strip(' \n\r')
            fp.close()

            if current_version:
                version_string = current_version
            else:
                version_string = 'No Version set in file'
                return version_string
    elif lazylibrarian.CONFIG['INSTALL_TYPE'] in ['package']:
        try:
            v = version.LAZYLIBRARIAN_HASH
        except AttributeError:
            v = "Unknown Version"
        version_string = v
    else:
        logmsg('error', 'Install Type not set - cannot get version value')
        version_string = 'Install type not set'
        return version_string

    updated = update_version_file(version_string)
    if updated:
        logmsg('debug', 'Install type [%s] Local Version is set to [%s] ' % (
            lazylibrarian.CONFIG['INSTALL_TYPE'], version_string))
    else:
        logmsg('debug', 'Install type [%s] Local Version is unchanged [%s] ' % (
            lazylibrarian.CONFIG['INSTALL_TYPE'], version_string))

    return version_string


def get_current_git_branch():
    # Returns current branch name of installed version from GIT
    # return "NON GIT INSTALL" if INSTALL TYPE is not GIT
    # Can only work for GIT driven installs, so check install type
    if lazylibrarian.CONFIG['INSTALL_TYPE'] != 'git':
        logmsg('debug', 'Non GIT Install doing get_current_git_branch')
        return 'NON GIT INSTALL'

    # use git rev-parse --abbrev-ref HEAD which returns the name of the current branch
    current_branch, _ = run_git('rev-parse --abbrev-ref HEAD')
    current_branch = str(current_branch)
    current_branch = current_branch.strip(' \n\r')

    if not current_branch:
        logmsg('error', 'Failed to return current branch value')
        return 'InvalidBranch'

    logmsg('debug', 'Current local branch of repo is [%s] ' % current_branch)

    return current_branch


def check_for_updates():
    """ Called at startup, from webserver with thread name WEBSERVER, or as a cron job """
    auto_update = False
    suppress = False
    if 'Thread-' in thread_name():
        thread_name("CRON-VERSIONCHECK")
        auto_update = lazylibrarian.CONFIG['AUTO_UPDATE']
    # noinspection PyBroadException
    try:
        db = database.DBConnection()
        columns = db.match('PRAGMA table_info(jobs)')
        if columns:
            db.upsert("jobs", {"Start": time.time()}, {"Name": "VERSIONCHECK"})
    except Exception:
        # jobs table might not exist yet
        pass

    logmsg('debug', 'Setting Install Type, Current & Latest Version and Commit status')
    get_install_type()
    lazylibrarian.CONFIG['CURRENT_VERSION'] = get_current_version()
    # if last dobytang version, force first gitlab version hash
    if lazylibrarian.CONFIG['CURRENT_VERSION'].startswith('45d4f24'):
        lazylibrarian.CONFIG['CURRENT_VERSION'] = 'd9002e449db276e0416a8d19423143cc677b2e84'
        lazylibrarian.CONFIG['GIT_UPDATED'] = 0  # and ignore timestamp to force upgrade
    lazylibrarian.CONFIG['LATEST_VERSION'] = get_latest_version()
    if lazylibrarian.CONFIG['CURRENT_VERSION'] == lazylibrarian.CONFIG['LATEST_VERSION']:
        lazylibrarian.CONFIG['COMMITS_BEHIND'] = 0
        lazylibrarian.COMMIT_LIST = ""
    else:
        commits, lazylibrarian.COMMIT_LIST = get_commit_difference_from_git()
        lazylibrarian.CONFIG['COMMITS_BEHIND'] = commits

        if auto_update and commits > 0:
            for name in [n.name.lower() for n in [t for t in threading.enumerate()]]:
                for word in ['update', 'scan', 'import', 'sync', 'process']:
                    if word in name:
                        suppress = True
                        logmsg('warn', 'Suppressed auto-update as %s running' % name)
                        break
            if not suppress:
                plural = ''
                if commits > 1:
                    plural = 's'
                logmsg('info', 'Auto updating %s commit%s' % (commits, plural))
                lazylibrarian.SIGNAL = 'update'
    logmsg('debug', 'Update check complete')
    # noinspection PyBroadException
    try:
        db = database.DBConnection()
        columns = db.match('PRAGMA table_info(jobs)')
        if columns:
            db.upsert("jobs", {"Finish": time.time()}, {"Name": "VERSIONCHECK"})
    except Exception:
        # jobs table might not exist yet
        pass


def get_latest_version():
    # Return latest version from git
    # if GIT install return latest on current branch
    # if nonGIT install return latest from master

    if lazylibrarian.CONFIG['INSTALL_TYPE'] in ['git', 'source', 'package']:
        latest_version = get_latest_version_from_git()
    elif lazylibrarian.CONFIG['INSTALL_TYPE'] in ['win']:
        latest_version = 'WINDOWS INSTALL'
    else:
        latest_version = 'UNKNOWN INSTALL'

    lazylibrarian.CONFIG['LATEST_VERSION'] = latest_version
    return latest_version


def get_latest_version_from_git():
    # Don't call directly, use get_latest_version as wrapper.
    latest_version = 'Unknown'

    # Can only work for non Windows driven installs, so check install type
    if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'win':
        logmsg('debug', 'Error - should not be called under a windows install')
        latest_version = 'WINDOWS INSTALL'
    else:
        # check current branch value of the local git repo as folks may pull from a branch not master
        branch = lazylibrarian.CONFIG['GIT_BRANCH']

        if branch == 'InvalidBranch':
            logmsg('debug', 'Failed to get a valid branch name from local repo')
        else:
            if branch.lower() == 'package':  # check packages against master
                branch = 'master'
            # Get the latest commit available from git
            if 'gitlab' in lazylibrarian.CONFIG['GIT_HOST']:
                url = 'https://lazylibrarian.gitlab.io/version.json'
            else:
                url = 'https://api.%s/repos/%s/%s/commits/%s' % (
                    lazylibrarian.CONFIG['GIT_HOST'], lazylibrarian.CONFIG['GIT_USER'],
                    lazylibrarian.CONFIG['GIT_REPO'], branch)

            logmsg('debug',
                   'Retrieving latest version information from git command=[%s]' % url)

            timestamp = check_int(lazylibrarian.CONFIG['GIT_UPDATED'], 0)
            age = ''
            if timestamp:
                # timestring for 'If-Modified-Since' needs to be english short day/month names and in gmt
                # we already have english month names stored in MONTHNAMES[] but need capitalising
                # so use hard coded versions here instead
                daynames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                monnames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                tm = time.gmtime(timestamp)
                age = "%s, %02d %s %04d %02d:%02d:%02d GMT" % (daynames[tm.tm_wday], tm.tm_mday,
                                                               monnames[tm.tm_mon], tm.tm_year, tm.tm_hour,
                                                               tm.tm_min, tm.tm_sec)
            try:
                headers = {'User-Agent': get_user_agent()}
                if age:
                    logmsg('debug', 'Checking if modified since %s' % age)
                    headers.update({'If-Modified-Since': age})
                proxies = proxy_list()
                timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
                if url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                    r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies,
                                     verify=lazylibrarian.CONFIG['SSL_CERTS']
                                     if lazylibrarian.CONFIG['SSL_CERTS'] else True)
                else:
                    r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies, verify=False)

                if str(r.status_code).startswith('2'):
                    if 'gitlab' in lazylibrarian.CONFIG['GIT_HOST']:
                        latest_version = r.json()
                    else:
                        git = r.json()
                        latest_version = git['sha']
                    logmsg('debug', 'Branch [%s] Latest Version has been set to [%s]' % (
                        branch, latest_version))
                elif str(r.status_code) == '304':
                    latest_version = lazylibrarian.CONFIG['CURRENT_VERSION']
                    logmsg('debug', 'Not modified, currently on Latest Version')
                else:
                    logmsg('warn', 'Could not get the latest commit from git')
                    logmsg('debug', 'git latest version returned %s' % r.status_code)
                    latest_version = 'Not_Available_From_Git'
            except Exception as e:
                logmsg('warn', 'Could not get the latest commit from git')
                logmsg('debug', 'git %s for %s: %s' % (type(e).__name__, url, str(e)))
                latest_version = 'Not_Available_From_Git'

    return latest_version


def get_commit_difference_from_git():
    # See how many commits behind we are
    # Takes current latest version value and tries to diff it with the latest version in the current branch.
    commit_list = ''
    commits = -1
    if lazylibrarian.CONFIG['LATEST_VERSION'] == 'Not_Available_From_Git':
        commits = 0  # don't report a commit diff as we don't know anything
        commit_list = 'Unable to get latest version from %s' % lazylibrarian.CONFIG['GIT_HOST']
        logmsg('info', commit_list)
    elif lazylibrarian.CONFIG['CURRENT_VERSION'] and commits != 0:
        if 'gitlab' in lazylibrarian.CONFIG['GIT_HOST']:
            url = 'https://%s/api/v4/projects/%s%%2F%s/repository/compare?from=%s&to=%s' % (
                lazylibrarian.GITLAB_TOKEN, lazylibrarian.CONFIG['GIT_USER'],
                lazylibrarian.CONFIG['GIT_REPO'], lazylibrarian.CONFIG['CURRENT_VERSION'],
                lazylibrarian.CONFIG['LATEST_VERSION'])
        else:
            url = 'https://api.%s/repos/%s/%s/compare/%s...%s' % (
                lazylibrarian.CONFIG['GIT_HOST'], lazylibrarian.CONFIG['GIT_USER'],
                lazylibrarian.CONFIG['GIT_REPO'], lazylibrarian.CONFIG['CURRENT_VERSION'],
                lazylibrarian.CONFIG['LATEST_VERSION'])
        logmsg('debug', 'Check for differences between local & repo by [%s]' % url)

        try:
            headers = {'User-Agent': get_user_agent()}
            proxies = proxy_list()
            timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
            if url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies,
                                 verify=lazylibrarian.CONFIG['SSL_CERTS']
                                 if lazylibrarian.CONFIG['SSL_CERTS'] else True)
            else:
                r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies, verify=False)
            git = r.json()
            # for gitlab, commits = len(git['commits'])  no status/ahead/behind
            if 'gitlab' in lazylibrarian.CONFIG['GIT_HOST']:
                if 'commits' in git:
                    commits = len(git['commits'])
                    st = ''
                    ahead = 0
                    behind = 0
                    if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git':
                        output, _ = run_git('rev-list --left-right --count master...origin/master')
                        if output:
                            a, b = output.split(None, 1)
                            ahead = check_int(a, 0)
                            behind = check_int(b, 0)
                            if ahead + behind:
                                st = "Differ"
                            else:
                                st = "Even"
                    if st:
                        msg = 'Git: Status [%s] - Ahead [%s] - Behind [%s] - Total Commits [%s]' % (
                               st, ahead, behind, commits)
                    else:
                        msg = 'Git: Total Commits [%s]' % commits
                    logmsg('debug', msg)
                else:
                    logmsg('warn', 'Could not get difference status from git: %s' % str(git))

                if commits > 0:
                    for item in git['commits']:
                        commit_list = "%s\n%s" % (item['title'], commit_list)
            else:
                if 'total_commits' in git:
                    commits = int(git['total_commits'])
                    msg = 'Git: Status [%s] - Ahead [%s] - Behind [%s] - Total Commits [%s]' % (
                        git['status'], git['ahead_by'], git['behind_by'], git['total_commits'])
                    logmsg('debug', msg)
                else:
                    logmsg('warn', 'Could not get difference status from git: %s' % str(git))

                if commits > 0:
                    for item in git['commits']:
                        commit_list = "%s\n%s" % (item['commit']['message'], commit_list)
        except Exception as e:
            logmsg('warn', 'Could not get difference status from git: %s' % type(e).__name__)

    if commits > 1:
        logmsg('info', 'New version is available. You are %s commits behind' % commits)
    elif commits == 1:
        logmsg('info', 'New version is available. You are one commit behind')
    elif commits == 0:
        logmsg('info', 'Lazylibrarian is up to date')
    else:
        logmsg('info', 'Unknown version of lazylibrarian. Run the updater to identify your version')

    return commits, commit_list


def update_version_file(new_version_id):
    # Update version.txt located in LL cache dir.
    version_path = os.path.join(lazylibrarian.CACHEDIR, 'version.txt')

    try:
        # noinspection PyBroadException
        try:
            with open(syspath(version_path), 'r') as ver_file:
                current_version = ver_file.read().strip(' \n\r')
            if current_version == new_version_id:
                return False
        except Exception:
            pass

        logmsg('debug', "Updating [%s] with value [%s]" % (version_path, new_version_id))
        with open(syspath(version_path), 'w') as ver_file:
            ver_file.write(new_version_id)
        lazylibrarian.CONFIG['CURRENT_VERSION'] = new_version_id
        return True

    except Exception as e:
        logmsg('error',
               "Unable to write current version to version.txt: %s" % str(e))
        return False


def update():
    with open(syspath(os.path.join(lazylibrarian.CONFIG['LOGDIR'], 'upgrade.log')), 'a') as upgradelog:
        if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'win':
            msg = 'Windows .exe updating not supported yet.'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)
            return False
        if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'package':
            msg = 'Please use your package manager to update'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)
            return False
        if lazylibrarian.DOCKER:
            msg = 'Docker does not officially allow upgrading the program inside the container,'
            msg += ' but we\'ll try anyway...'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)

        try:
            # try to create a backup in case the upgrade is faulty...
            backup_file = os.path.join(lazylibrarian.PROG_DIR, "backup.tgz")
            msg = 'Backing up prior to upgrade'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)
            zf = tarfile.open(backup_file, mode='w:gz')
            for folder in ['cherrypy', 'data', 'init', 'lazylibrarian', 'LazyLibrarian.app',
                           'lib', 'lib3', 'mako']:
                path = os.path.join(lazylibrarian.PROG_DIR, folder)
                for root, _, files in walk(path):
                    for item in files:
                        if not item.endswith('.pyc'):
                            base = root[len(lazylibrarian.PROG_DIR) + 1:]
                            zf.add(os.path.join(root, item), arcname=os.path.join(base, item))
            for item in ['LazyLibrarian.py', 'epubandmobi.py', 'example_custom_notification.py',
                         'example_custom_notification.sh', 'example_ebook_convert.py',
                         'example.genres.json', 'example.monthnames.json']:
                zf.add(os.path.join(lazylibrarian.PROG_DIR, item), arcname=item)
            zf.close()
            msg = 'Saved current version to %s' % backup_file
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)
        except Exception as e:
            msg = "Failed to create backup: %s" % str(e)
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg("error", msg)

        if lazylibrarian.CONFIG['INSTALL_TYPE'] == 'git':
            branch = get_current_git_branch()

            _, _ = run_git('stash clear')
            output, _ = run_git('pull --no-rebase origin ' + branch)  # type: str

            if not output:
                msg = 'Couldn\'t download latest version'
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logmsg('error', msg)
                return False

            for line in output.split('\n'):
                if 'Already up to date' in line:
                    msg = 'No update available: ' + str(output)
                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                    logmsg('info', msg)
                elif 'Aborting' in line or 'local changes' in line:
                    msg = 'Unable to update: ' + str(output)
                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                    logmsg('error', msg)
                    return False

            # Update version.txt and timestamp
            if 'LATEST_VERSION' not in lazylibrarian.CONFIG:
                lazylibrarian.CONFIG['LATEST_VERSION'] = 'Unknown'
                url = 'https://lazylibrarian.gitlab.io/version.json'
                if lazylibrarian.CONFIG['SSL_VERIFY']:
                    r = requests.get(url, timeout=30, verify=lazylibrarian.CONFIG['SSL_CERTS']
                                     if lazylibrarian.CONFIG['SSL_CERTS'] else True)
                else:
                    r = requests.get(url, timeout=30, verify=False)
                if str(r.status_code).startswith('2'):
                    lazylibrarian.CONFIG['LATEST_VERSION'] = r.json()

            update_version_file(lazylibrarian.CONFIG['LATEST_VERSION'])
            upgradelog.write("%s %s\n" % (time.ctime(), "Updated version file to %s" %
                             lazylibrarian.CONFIG['LATEST_VERSION']))
            lazylibrarian.CONFIG['GIT_UPDATED'] = str(int(time.time()))
            lazylibrarian.CONFIG['CURRENT_VERSION'] = lazylibrarian.CONFIG['LATEST_VERSION']
            return True

        elif lazylibrarian.CONFIG['INSTALL_TYPE'] == 'source':
            if 'gitlab' in lazylibrarian.CONFIG['GIT_HOST']:
                tar_download_url = 'https://%s/%s/%s/-/archive/%s/%s-%s.tar.gz' % (
                    lazylibrarian.GITLAB_TOKEN, lazylibrarian.CONFIG['GIT_USER'],
                    lazylibrarian.CONFIG['GIT_REPO'], lazylibrarian.CONFIG['GIT_BRANCH'],
                    lazylibrarian.CONFIG['GIT_REPO'], lazylibrarian.CONFIG['GIT_BRANCH'])
            else:
                tar_download_url = 'https://%s/%s/%s/tarball/%s' % (
                    lazylibrarian.CONFIG['GIT_HOST'], lazylibrarian.CONFIG['GIT_USER'],
                    lazylibrarian.CONFIG['GIT_REPO'], lazylibrarian.CONFIG['GIT_BRANCH'])
            update_dir = os.path.join(lazylibrarian.PROG_DIR, 'update')

            rmtree(update_dir, ignore_errors=True)
            os.mkdir(update_dir)

            try:
                msg = 'Downloading update from: ' + tar_download_url
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logmsg('info', msg)
                headers = {'User-Agent': get_user_agent()}
                proxies = proxy_list()
                timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
                if tar_download_url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
                    r = requests.get(tar_download_url, timeout=timeout, headers=headers, proxies=proxies,
                                     verify=lazylibrarian.CONFIG['SSL_CERTS']
                                     if lazylibrarian.CONFIG['SSL_CERTS'] else True)
                else:
                    r = requests.get(tar_download_url, timeout=timeout, headers=headers, proxies=proxies, verify=False)
            except requests.exceptions.Timeout:
                msg = "Timeout retrieving new version from " + tar_download_url
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logmsg('error', msg)
                return False
            except Exception as e:
                errmsg = str(e)
                msg = "Unable to retrieve new version from " + tar_download_url
                msg += ", can't update: %s" % errmsg
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logmsg('error', msg)
                return False

            download_name = r.url.split('/')[-1]

            tar_download_path = os.path.join(lazylibrarian.PROG_DIR, download_name)

            # Save tar to disk
            with open(syspath(tar_download_path), 'wb') as f:
                f.write(r.content)

            msg = 'Extracting file ' + tar_download_path
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)
            try:
                with tarfile.open(tar_download_path) as tar:
                    tar.extractall(update_dir)
            except Exception as e:
                msg = 'Failed to unpack tarfile %s (%s): %s' % (type(e).__name__,
                                                                tar_download_path, str(e))
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logmsg('error', msg)
                return False

            msg = 'Deleting file ' + tar_download_path
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('info', msg)
            os.remove(syspath(tar_download_path))

            # Find update dir name
            update_dir = make_unicode(update_dir)
            logmsg('debug', "update_dir [%s]" % update_dir)
            update_dir_contents = [x for x in listdir(update_dir) if path_isdir(os.path.join(update_dir, x))]
            if len(update_dir_contents) != 1:
                msg = "Invalid update data, update failed: " + str(update_dir_contents)
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logmsg('error', msg)
                return False
            content_dir = os.path.join(update_dir, update_dir_contents[0])
            logmsg('debug', "update_dir_contents [%s]" % str(update_dir_contents))
            logmsg('debug', "Walking %s" % content_dir)
            # walk temp folder and move files to main folder
            for rootdir, _, filenames in walk(content_dir):
                rootdir = rootdir[len(content_dir) + 1:]
                for curfile in filenames:
                    old_path = os.path.join(content_dir, rootdir, curfile)
                    new_path = os.path.join(lazylibrarian.PROG_DIR, rootdir, curfile)
                    if old_path == new_path:
                        msg = "PROG_DIR [%s] content_dir [%s] rootdir [%s] curfile [%s]" % (
                               lazylibrarian.PROG_DIR, content_dir, rootdir, curfile)
                        upgradelog.write("%s %s\n" % (time.ctime(), msg))
                        logmsg('error', msg)
                    if curfile.endswith('.dll'):
                        # can't update a dll on windows if it's mapped into the system
                        # but as the dll doesn't change just skip past it.
                        # If we need to update it in the future we will need to rename it
                        # or use a different upgrade mechanism
                        logmsg('debug', "Skipping %s" % curfile)
                    else:
                        if os.path.isfile(syspath(new_path)):
                            os.remove(syspath(new_path))
                        os.renames(syspath(old_path), syspath(new_path))

            # Update version.txt and timestamp
            update_version_file(lazylibrarian.CONFIG['LATEST_VERSION'])
            upgradelog.write("%s %s\n" % (time.ctime(), "Updated version file to %s" %
                             lazylibrarian.CONFIG['LATEST_VERSION']))
            lazylibrarian.CONFIG['GIT_UPDATED'] = str(int(time.time()))
            lazylibrarian.CONFIG['CURRENT_VERSION'] = lazylibrarian.CONFIG['LATEST_VERSION']
            return True

        else:
            msg = "Cannot perform update - Install Type not set"
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logmsg('error', msg)
            return False
