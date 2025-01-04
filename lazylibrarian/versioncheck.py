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

import logging
import os
import platform
import re
import subprocess
import tarfile
import threading
import time
import datetime
from shutil import rmtree, move

import lazylibrarian
from lazylibrarian.config2 import CONFIG
import requests
from lazylibrarian import version, database
from lazylibrarian.common import get_user_agent, proxy_list, docker, dbbackup
from lazylibrarian.filesystem import DIRS, path_isdir, syspath, listdir, walk
from lazylibrarian.formatter import check_int, make_unicode, thread_name
from lazylibrarian.telemetry import TELEMETRY


def run_git(args):
    logger = logging.getLogger(__name__)
    # Function to execute GIT commands taking care of error logging etc
    if CONFIG['GIT_PROGRAM']:
        git_locations = ['"' + CONFIG['GIT_PROGRAM'] + '"']
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
            logger.debug('Execute: "%s" with shell in %s' % (cmd, DIRS.PROG_DIR))
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 shell=True, cwd=DIRS.PROG_DIR)
            output, err = p.communicate()
            output = make_unicode(output).strip('\n')

            logger.debug('Git output: [%s]' % output)
            if err:
                err = make_unicode(err)
                logger.debug('Git err: [%s]' % err)
            elif not output:
                output = ' '

        except OSError:
            logger.debug('Command ' + cmd + ' didn\'t work, couldn\'t find git')
            continue

        if 'not found' in output or "not recognized as an internal or external command" in output:
            logger.debug('Unable to find git with command ' + cmd)
            logger.error('git not found - please ensure git executable is in your PATH')
            output = None
        elif 'fatal:' in output or err:
            logger.error('Git returned bad info. Are you sure this is a git installation?')
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
    logger = logging.getLogger(__name__)
    try:
        install = version.LAZYLIBRARIAN_VERSION.lower()
    except AttributeError:
        install = 'unknown'

    if install in ['windows', 'win32build']:
        CONFIG.set_str('INSTALL_TYPE', 'win')
        lazylibrarian.CURRENT_BRANCH = 'Windows'

    elif install == 'package':  # deb, rpm, other non-upgradeable
        CONFIG.set_str('INSTALL_TYPE', 'package')
        CONFIG.set_str('GIT_BRANCH', 'master')

    elif path_isdir(os.path.join(DIRS.PROG_DIR, '.git')):
        CONFIG.set_str('INSTALL_TYPE', 'git')
        CONFIG.set_str('GIT_BRANCH', get_current_git_branch())
    else:
        CONFIG.set_str('INSTALL_TYPE', 'source')
        CONFIG.set_str('GIT_BRANCH', 'master')

    logger.debug('%s install detected. Setting Branch to [%s]' %
                 (CONFIG['INSTALL_TYPE'], CONFIG['GIT_BRANCH']))


def get_current_version() -> str:
    # Establish the version of the installed app for Source or GIT only
    # Global variable set in LazyLibrarian.py on startup as it should be
    logger = logging.getLogger(__name__)
    if CONFIG['INSTALL_TYPE'] == 'win':
        logger.debug('Windows install - no update available')

        # Don't have a way to update exe yet, but don't want to set VERSION to None
        version_string = 'Windows Install'

    elif CONFIG['INSTALL_TYPE'] == 'git':
        output, _ = run_git('rev-parse HEAD')

        if not output:
            logger.error('Couldn\'t find latest git installed version.')
            cur_commit_hash = 'GIT HEAD cannot establish version'
        else:
            cur_commit_hash = output.strip()

            # noinspection PyTypeChecker
            if not re.match('^[a-z0-9]+$', cur_commit_hash):
                logger.error('Output doesn\'t look like a hash, not using it')
                cur_commit_hash = 'No Hash found'

        version_string = cur_commit_hash

    elif CONFIG['INSTALL_TYPE'] in ['source']:
        version_file = os.path.join(DIRS.CACHEDIR, 'version.txt')
        if not version_file.startswith(DIRS.PROG_DIR):
            old_location = os.path.join(DIRS.PROG_DIR, 'version.txt')
            if os.path.isfile(old_location):
                # we did an --update with no --datadir,
                # so move version.txt into the right cache folder
                logger.debug(f'Moving {old_location} to {version_file}')
                move(old_location, version_file)

        if not os.path.isfile(version_file):
            version_string = 'Missing Version file'
            logger.debug('Version file [%s] missing.' % version_file)
            return version_string
        else:
            with open(version_file, 'r') as fp:
                current_version = fp.read().strip(' \n\r')

            if current_version:
                version_string = current_version
            else:
                version_string = 'Invalid Version file'
                return version_string
    elif CONFIG['INSTALL_TYPE'] in ['package']:
        try:
            v = version.LAZYLIBRARIAN_HASH
        except AttributeError:
            v = "Unknown Version"
        version_string = v
    else:
        logger.error('Install Type not set - cannot get version value')
        version_string = 'No Type set'
        return version_string

    updated = update_version_file(version_string)
    if updated:
        logger.debug('Install type [%s] Local Version is set to [%s] ' % (
            CONFIG['INSTALL_TYPE'], version_string))
    else:
        logger.debug('Install type [%s] Local Version is unchanged [%s] ' % (
            CONFIG['INSTALL_TYPE'], version_string))

    return version_string


def get_current_git_branch():
    # Returns current branch name of installed version from GIT
    # return "NON GIT INSTALL" if INSTALL TYPE is not GIT
    # Can only work for GIT driven installs, so check install type
    logger = logging.getLogger(__name__)
    if CONFIG['INSTALL_TYPE'] != 'git':
        logger.debug('Non GIT Install doing get_current_git_branch')
        return 'NON GIT INSTALL'

    # use git rev-parse --abbrev-ref HEAD which returns the name of the current branch
    current_branch, _ = run_git('rev-parse --abbrev-ref HEAD')
    current_branch = str(current_branch)
    current_branch = current_branch.strip(' \n\r')

    if not current_branch:
        logger.error('Failed to return current branch value')
        return 'InvalidBranch'

    logger.debug('Current local branch of repo is [%s] ' % current_branch)

    return current_branch


def check_for_updates():
    """ Called at startup, from webserver with thread name WEBSERVER, or as a cron job """
    logger = logging.getLogger(__name__)
    auto_update = False
    suppress = False
    if 'Thread' in thread_name():
        thread_name("CRON-VERSIONCHECK")
        auto_update = CONFIG.get_bool('AUTO_UPDATE')

    db = database.DBConnection()
    columns = db.match('PRAGMA table_info(jobs)')
    if columns:
        db.upsert("jobs", {"Start": time.time()}, {"Name": "VERSIONCHECK"})
    db.close()

    logger.debug('Setting Install Type, Current & Latest Version and Commit status')
    get_install_type()
    CONFIG.set_str('CURRENT_VERSION', get_current_version())
    # if last dobytang version, force first gitlab version hash
    if CONFIG['CURRENT_VERSION'].startswith('45d4f24'):
        CONFIG.set_str('CURRENT_VERSION', 'd9002e449db276e0416a8d19423143cc677b2e84')
        CONFIG.set_int('GIT_UPDATED', 0)  # and ignore timestamp to force upgrade
    get_latest_version()
    # allow comparison of long and short hashes
    if CONFIG['LATEST_VERSION'].startswith(CONFIG['CURRENT_VERSION']):
        CONFIG.set_int('COMMITS_BEHIND', 0)
        lazylibrarian.COMMIT_LIST = ""
    else:
        commits, lazylibrarian.COMMIT_LIST = get_commit_difference_from_git()
        CONFIG.set_int('COMMITS_BEHIND', commits)
        if auto_update and commits > 0:
            for name in [n.name.lower() for n in [t for t in threading.enumerate()]]:
                for word in ['update', 'scan', 'import', 'sync', 'process']:
                    if word in name:
                        suppress = True
                        logger.warning('Suppressed auto-update as %s running' % name)
                        break

            if not suppress and '** MANUAL **' in lazylibrarian.COMMIT_LIST:
                suppress = True
                logger.warning('Suppressed auto-update as manual install needed')

            if not suppress:
                plural = ''
                if commits > 1:
                    plural = 's'
                logger.info('Auto updating %s commit%s' % (commits, plural))
                lazylibrarian.SIGNAL = 'update'

    # testing - force a fake update
    # lazylibrarian.COMMIT_LIST = 'test update system'
    # lazylibrarian.SIGNAL = 'update'
    # lazylibrarian.CONFIG['COMMITS_BEHIND'] = 1
    # lazylibrarian.CONFIG['CURRENT_VERSION'] = 'testing'

    logger.debug(f"Update check complete. Behind {CONFIG['COMMITS_BEHIND']}")
    db = database.DBConnection()
    columns = db.match('PRAGMA table_info(jobs)')
    if columns:
        db.upsert("jobs", {"Finish": time.time()}, {"Name": "VERSIONCHECK"})
    db.close()


def get_latest_version():
    # Return latest version from git
    # if GIT install return latest on current branch
    # if nonGIT install return latest from master
    created_at = ''
    if CONFIG['INSTALL_TYPE'] in ['git', 'source', 'package']:
        latest_version, created_at = get_latest_version_from_git()
    elif CONFIG['INSTALL_TYPE'] in ['win']:
        latest_version = 'WINDOWS INSTALL'
    else:
        latest_version = 'UNKNOWN INSTALL'

    CONFIG.set_str('LATEST_VERSION', latest_version)
    if created_at:
        try:
            updated = int(time.mktime(datetime.datetime.fromisoformat(created_at).timetuple()))
            CONFIG.set_int('GIT_UPDATED', updated)
        except ValueError:
            pass
    return latest_version


def get_latest_version_from_git():
    # Don't call directly, use get_latest_version as wrapper.
    logger = logging.getLogger(__name__)
    latest_version = 'Unknown'
    created_at = ''

    # Can only work for non Windows driven installs, so check install type
    if CONFIG['INSTALL_TYPE'] == 'win':
        logger.debug('Error - should not be called under a windows install')
        latest_version = 'WINDOWS INSTALL'
    else:
        # check current branch value of the local git repo as folks may pull from a branch not master
        branch = CONFIG['GIT_BRANCH']

        if branch == 'InvalidBranch':
            logger.debug('Failed to get a valid branch name from local repo')
        else:
            if branch.lower() == 'package':  # check packages against master
                branch = 'master'

            project = CONFIG['GIT_PROJECT']
            if not project:
                project = '9317860'  # default lazylibrarian

            url = f"https://gitlab.com/api/v4/projects/{project}/repository/commits"
            # Get the latest commit available from git
            logger.debug('Retrieving latest version information from git command=[%s]' % url)

            timestamp = CONFIG.get_int('GIT_UPDATED')
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
                    logger.debug('Checking if modified since %s' % age)
                    headers.update({'If-Modified-Since': age})
                proxies = proxy_list()
                timeout = CONFIG.get_int('HTTP_TIMEOUT')
                if url.startswith('https') and CONFIG['SSL_VERIFY']:
                    r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies,
                                     verify=CONFIG['SSL_CERTS']
                                     if CONFIG['SSL_CERTS'] else True)
                else:
                    r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies, verify=False)

                if str(r.status_code).startswith('2'):
                    try:
                        latest_version = r.json()[0]['id']
                        created_at = r.json()[0]['created_at']
                        logger.debug(f"Branch {branch} Latest Version [{latest_version}] {created_at}")
                    except Exception as err:
                        logger.warning(f'Error {type(err).__name__} reading json response')
                        logger.error(f'{r.json()}')
                elif str(r.status_code) == '304':
                    latest_version = CONFIG['CURRENT_VERSION']
                    logger.debug('Not modified, currently on Latest Version')
                else:
                    logger.warning(f'Could not get the latest commit from git ({r.status_code})')
                    logger.error(f'{url}: ({headers})')
                    latest_version = f'Not_Available_From_Git : {r.status_code} : {url}'
            except Exception as err:
                logger.warning(f'Could not get the latest commit from git: {type(err).__name__}')
                logger.error(f'for {url}: {str(err)}')
                latest_version = f'Not_Available_From_Git : {type(err).__name__} : {url}'

    return latest_version, created_at


def get_commit_difference_from_git() -> (int, str):
    """ See how many commits behind we are.
    Takes current latest version value and tries to diff it with the latest version in the current branch.
    Returns # of commits behind, and the list of commits as a string """
    logger = logging.getLogger(__name__)
    commit_list = ''
    commits = -1
    if CONFIG['LATEST_VERSION'] and CONFIG['LATEST_VERSION'].startswith('Not_Available_From_Git'):
        CONFIG.set_str('LATEST_VERSION', 'HEAD')
        commit_list = 'Unable to get latest version from %s' % CONFIG['GIT_HOST']
        logger.info(commit_list)
    if re.match('^[a-z0-9]+$', CONFIG['CURRENT_VERSION']):  # does it look like a hash, not an error message
        url = 'https://%s/api/v4/projects/%s%%2F%s/repository/compare?from=%s&to=%s' % (
            lazylibrarian.GITLAB_TOKEN, CONFIG['GIT_USER'],
            CONFIG['GIT_REPO'], CONFIG['CURRENT_VERSION'],
            CONFIG['LATEST_VERSION'])
        logger.debug('Check for differences between local & repo by [%s]' % url)

        try:
            headers = {'User-Agent': get_user_agent()}
            proxies = proxy_list()
            timeout = CONFIG.get_int('HTTP_TIMEOUT')
            if url.startswith('https') and CONFIG.get_bool('SSL_VERIFY'):
                r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies,
                                 verify=CONFIG['SSL_CERTS']
                                 if CONFIG['SSL_CERTS'] else True)
            else:
                r = requests.get(url, timeout=timeout, headers=headers, proxies=proxies, verify=False)
            git = r.json()
            # for gitlab, commits = len(git['commits'])  no status/ahead/behind
            if 'commits' in git:
                commits = len(git['commits'])
                st = ''
                ahead = 0
                behind = 0
                if CONFIG['INSTALL_TYPE'] == 'git':
                    branch = CONFIG['GIT_BRANCH']
                    output, _ = run_git('rev-list --left-right --count %s...origin/%s' % (branch, branch))
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
                logger.debug(msg)
            else:
                logger.warning('Could not get difference status from git: %s' % str(git))
            if commits > 0:
                if CONFIG['LATEST_VERSION'] == 'HEAD':
                    CONFIG.set_str('LATEST_VERSION', git['commit']['id'])
                for item in git['commits']:
                    commit_list = "%s\n%s" % (item['title'], commit_list)
        except Exception as err:
            logger.warning('Could not get difference status from git: %s' % type(err).__name__)

    if commits > 1:
        logger.info('New version is available. You are %s commits behind' % commits)
    elif commits == 1:
        logger.info('New version is available. You are one commit behind')
    elif commits == 0:
        logger.info('Lazylibrarian is up to date')
        if CONFIG['LATEST_VERSION'] == 'HEAD':
            commit_list = ''
            CONFIG.set_str('LATEST_VERSION', CONFIG['CURRENT_VERSION'])
    else:
        commit_list = (f"Unknown version of lazylibrarian ({CONFIG['CURRENT_VERSION']})\n"
                       f"Run lazylibrarian with --update to identify your version")
        logger.info(commit_list)
    return commits, commit_list


def update_version_file(new_version_id):
    # Update version.txt located in LL cache dir.
    logger = logging.getLogger(__name__)
    version_path = os.path.join(DIRS.CACHEDIR, 'version.txt')

    try:
        with open(syspath(version_path), 'r') as ver_file:
            current_version = ver_file.read().strip(' \n\r')
        if current_version == new_version_id:
            return False
    except Exception as err:
        logger.error("Unable to read current version from version.txt: %s" % str(err))
        pass

    logger.debug("Updating [%s] with value [%s]" % (version_path, new_version_id))
    try:
        with open(syspath(version_path), 'w') as ver_file:
            ver_file.write(new_version_id)
        CONFIG.set_str('CURRENT_VERSION', new_version_id)
        return True

    except Exception as err:
        logger.error("Unable to write current version to version.txt: %s" % str(err))
        return False


def update():
    TELEMETRY.record_usage_data('Version/Update')
    logger = logging.getLogger(__name__)
    logdir = DIRS.ensure_data_subdir('Logs')
    with open(os.path.join(logdir, 'upgrade.log'), 'a') as upgradelog:
        if CONFIG['INSTALL_TYPE'] == 'win':
            msg = 'Windows .exe updating not supported yet.'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
            return False
        if CONFIG['INSTALL_TYPE'] == 'package':
            msg = 'Please use your package manager to update'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
            return False
        if docker():
            msg = 'Docker does not allow upgrading the program inside the container,'
            msg += ' please rebuild your docker container instead'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
            return False
        try:
            # try to create a backup in case the upgrade is faulty...
            backup_file = os.path.join(DIRS.PROG_DIR, "backup.tgz")
            msg = 'Backing up prior to upgrade'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
            dbbackup_file = ''
            if CONFIG['BACKUP_DB']:
                dbbackup_file, _ = dbbackup('upgrade')
            zf = tarfile.open(backup_file, mode='w:gz')
            prog_folders = ['data', 'init', 'lazylibrarian', 'LazyLibrarian.app', 'lib', 'unittests']
            for folder in prog_folders:
                path = os.path.join(DIRS.PROG_DIR, folder)
                for root, _, files in walk(path):
                    for item in files:
                        if not item.endswith('.pyc'):
                            base = root[len(DIRS.PROG_DIR) + 1:]
                            zf.add(os.path.join(root, item), arcname=os.path.join(base, item))
            for item in ['LazyLibrarian.py', 'epubandmobi.py', 'example_custom_notification.py',
                         'example_custom_notification.sh', 'example_ebook_convert.py', 'example_filetemplate.txt',
                         'example.genres.json', 'example_html_filetemplate.txt', 'example_logintemplate.txt',
                         'example.monthnames.json', 'updater.py', 'pyproject.toml']:
                path = os.path.join(DIRS.PROG_DIR, item)
                if os.path.exists(path):
                    zf.add(path, arcname=item)

            current_version = ''
            version_file = os.path.join(DIRS.CACHEDIR, 'version.txt')
            if os.path.isfile(version_file):
                with open(version_file, 'r') as fp:
                    current_version = fp.read().strip(' \n\r')
                zf.add(version_file, arcname='version.txt')
            zf.close()
            msg = f'Saved current version {current_version} to {backup_file}'
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
        except Exception as err:
            msg = "Failed to create backup: %s" % str(err)
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.error(msg)

        if CONFIG['INSTALL_TYPE'] == 'git':
            branch = get_current_git_branch()

            _, _ = run_git('stash clear')
            output, _ = run_git('pull --no-rebase origin ' + branch)  # type: str

            if not output:
                msg = 'Couldn\'t download latest version'
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logger.error(msg)
                return False

            for line in output.split('\n'):
                if 'Already up to date' in line:
                    msg = 'No update available: ' + str(output)
                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                    logger.info(msg)
                    if os.path.exists(backup_file):
                        os.remove(backup_file)
                    if os.path.exists(dbbackup_file):
                        os.remove(dbbackup_file)
                    break
                elif 'Aborting' in line or 'local changes' in line:
                    msg = 'Unable to update: ' + str(output)
                    upgradelog.write("%s %s\n" % (time.ctime(), msg))
                    logger.error(msg)
                    return False

            get_latest_version()
            update_version_file(CONFIG['LATEST_VERSION'])
            upgradelog.write("%s %s\n" % (time.ctime(), "Updated version file to %s" %
                                          CONFIG['LATEST_VERSION']))
            CONFIG.set_str('CURRENT_VERSION', CONFIG['LATEST_VERSION'])
            return True

        elif CONFIG['INSTALL_TYPE'] == 'source':
            tar_download_url = 'https://%s/%s/%s/-/archive/%s/%s-%s.tar.gz' % (
                lazylibrarian.GITLAB_TOKEN, CONFIG['GIT_USER'],
                CONFIG['GIT_REPO'], CONFIG['GIT_BRANCH'],
                CONFIG['GIT_REPO'], CONFIG['GIT_BRANCH'])
            update_dir = os.path.join(DIRS.PROG_DIR, 'update')

            rmtree(update_dir, ignore_errors=True)
            os.mkdir(update_dir)

            try:
                msg = 'Downloading update from: ' + tar_download_url
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logger.info(msg)
                headers = {'User-Agent': get_user_agent()}
                proxies = proxy_list()
                timeout = CONFIG.get_int('HTTP_TIMEOUT')
                if tar_download_url.startswith('https') and CONFIG.get_bool('SSL_VERIFY'):
                    r = requests.get(tar_download_url, timeout=timeout, headers=headers, proxies=proxies,
                                     verify=CONFIG['SSL_CERTS']
                                     if CONFIG['SSL_CERTS'] else True)
                else:
                    r = requests.get(tar_download_url, timeout=timeout, headers=headers, proxies=proxies, verify=False)
            except requests.exceptions.Timeout:
                msg = "Timeout retrieving new version from " + tar_download_url
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logger.error(msg)
                return False
            except Exception as err:
                errmsg = str(err)
                msg = "Unable to retrieve new version from " + tar_download_url
                msg += ", can't update: %s" % errmsg
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logger.error(msg)
                return False

            download_name = r.url.split('/')[-1]

            tar_download_path = os.path.join(DIRS.PROG_DIR, download_name)

            # Save tar to disk
            with open(syspath(tar_download_path), 'wb') as f:
                f.write(r.content)

            msg = 'Extracting file ' + tar_download_path
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
            try:
                with tarfile.open(tar_download_path) as tar:
                    tar.extractall(update_dir)
            except Exception as err:
                msg = 'Failed to unpack tarfile %s (%s): %s' % (type(err).__name__,
                                                                tar_download_path, str(err))
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logger.error(msg)
                return False

            msg = 'Deleting file ' + tar_download_path
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.info(msg)
            os.remove(syspath(tar_download_path))

            # Find update dir name
            update_dir = make_unicode(update_dir)
            logger.debug("update_dir [%s]" % update_dir)
            update_dir_contents = [x for x in listdir(update_dir) if path_isdir(os.path.join(update_dir, x))]
            if len(update_dir_contents) != 1:
                msg = "Invalid update data, update failed: " + str(update_dir_contents)
                upgradelog.write("%s %s\n" % (time.ctime(), msg))
                logger.error(msg)
                return False
            content_dir = os.path.join(update_dir, update_dir_contents[0])
            logger.debug("update_dir_contents [%s]" % str(update_dir_contents))
            logger.debug("Walking %s" % content_dir)
            # walk temp folder and move files to main folder
            for rootdir, _, filenames in walk(content_dir):
                rootdir = rootdir[len(content_dir) + 1:]
                for curfile in filenames:
                    old_path = os.path.join(content_dir, rootdir, curfile)
                    new_path = os.path.join(DIRS.PROG_DIR, rootdir, curfile)
                    if old_path == new_path:
                        msg = "PROG_DIR [%s] content_dir [%s] rootdir [%s] curfile [%s]" % (
                               DIRS.PROG_DIR, content_dir, rootdir, curfile)
                        upgradelog.write("%s %s\n" % (time.ctime(), msg))
                        logger.error(msg)
                    if curfile.endswith('.dll'):
                        # can't update a dll on windows if it's mapped into the system
                        # but as the dll doesn't change just skip past it.
                        # If we need to update it in the future we will need to rename it
                        # or use a different upgrade mechanism
                        logger.debug("Skipping %s" % curfile)
                    else:
                        if os.path.isfile(syspath(new_path)):
                            os.remove(syspath(new_path))
                        os.renames(syspath(old_path), syspath(new_path))

            # Update version.txt and timestamp
            get_latest_version()
            update_version_file(CONFIG['LATEST_VERSION'])
            upgradelog.write("%s %s\n" % (time.ctime(), "Updated version file to %s" %
                                          CONFIG['LATEST_VERSION']))
            CONFIG.set_str('CURRENT_VERSION', CONFIG['LATEST_VERSION'])
            return True

        else:
            msg = "Cannot perform update - Install Type not set"
            upgradelog.write("%s %s\n" % (time.ctime(), msg))
            logger.error(msg)
            return False
