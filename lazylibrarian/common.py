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
#   Common, basic functions for LazyLibrary

import glob

import mako
import os
import platform
import random
import string
import sys
import time
import subprocess

import zipfile
import re
import ssl
import sqlite3
import cherrypy
import urllib3
import requests
import webencodings
import bs4
import html5lib

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.config2 import CONFIG
from lazylibrarian.configdefs import CONFIG_GIT
from lazylibrarian.formatter import get_list, make_unicode
from lazylibrarian.filesystem import DIRS, syspath, path_exists, remove_file, \
    listdir, walk, setperm


def get_user_agent() -> str:
    # Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36
    if CONFIG['USER_AGENT']:
        return CONFIG['USER_AGENT']
    else:
        return 'LazyLibrarian' + ' (' + platform.system() + ' ' + platform.release() + ')'


def multibook(foldername, recurse=False):
    # Check for more than one book in the folder(tree). Note we can't rely on basename
    # being the same, so just check for more than one bookfile of the same type
    # Return which type we found multiples of, or empty string if no multiples
    filetypes = CONFIG['EBOOK_TYPE']

    if recurse:
        for _, _, f in walk(foldername):
            flist = [item for item in f]
            for item in filetypes:
                counter = 0
                for fname in flist:
                    if fname.endswith(item):
                        counter += 1
                        if counter > 1:
                            return item
    else:
        flist = listdir(foldername)
        for item in filetypes:
            counter = 0
            for fname in flist:
                if fname.endswith(item):
                    counter += 1
                    if counter > 1:
                        return item
    return ''


def proxy_list():
    proxies = None
    if CONFIG['PROXY_HOST']:
        proxies = {}
        for item in get_list(CONFIG['PROXY_TYPE']):
            if item in ['http', 'https']:
                proxies.update({item: CONFIG['PROXY_HOST']})
    return proxies


def is_valid_email(emails):
    if not emails:
        return False
    elif ',' in emails:
        emails = get_list(emails)
    else:
        emails = [emails]

    for email in emails:
        if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", email) is None:
            return False
    return True


def pwd_generator(size=10, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def pwd_check(password):
    # password rules:
    # At least 8 digits long
    # with no spaces
    # we don't enforce mix of alnum as longer passwords
    # made of random words are more secure
    if len(password) < 8:
        return False
    # if not any(char.isdigit() for char in password):
    #    return False
    # if not any(char.isalpha() for char in password):
    #    return False
    if any(char.isspace() for char in password):
        return False
    return True


def mime_type(filename):
    name = make_unicode(filename).lower()
    if name.endswith('.epub'):
        return 'application/epub+zip'
    elif name.endswith('.mobi') or name.endswith('.azw'):
        return 'application/x-mobipocket-ebook'
    elif name.endswith('.azw3'):
        return 'application/x-mobi8-ebook'
    elif name.endswith('.pdf'):
        return 'application/pdf'
    elif name.endswith('.mp3'):
        return 'audio/mpeg3'
    elif name.endswith('.m4a'):
        return 'audio/mp4'
    elif name.endswith('.m4b'):
        return 'audio/mp4'
    elif name.endswith('.flac'):
        return 'audio/flac'
    elif name.endswith('.ogg'):
        return 'audio/ogg'
    elif name.endswith('.zip'):
        return 'application/x-zip-compressed'
    elif name.endswith('.xml'):
        return 'application/rss+xml'
    elif name.endswith('.cbz'):
        return 'application/x-cbz'
    elif name.endswith('.cbr'):
        return 'application/x-cbr'
    return "application/x-download"


def module_available(module_name):
    if sys.version_info < (3, 0):
        import importlib
        # noinspection PyDeprecation
        loader = importlib.find_loader(module_name)
    elif sys.version_info <= (3, 3):
        import pkgutil
        loader = pkgutil.find_loader(module_name)
    elif sys.version_info >= (3, 4):
        import importlib
        loader = importlib.util.find_spec(module_name)
    else:
        loader = None
    return loader is not None


def get_calibre_id(data):
    logger.debug(str(data))
    fname = data.get('BookFile', '')
    if fname:  # it's a book
        author = data.get('AuthorName', '')
        title = data.get('BookName', '')
    else:
        title = data.get('IssueDate', '')
        if title:  # it's a magazine issue
            author = data.get('Title', '')
            fname = data.get('IssueFile', '')
        else:  # assume it's a comic issue
            title = data.get('IssueID', '')
            author = data.get('ComicID', '')
            fname = data.get('IssueFile', '')
    try:
        fname = os.path.dirname(fname)
        calibre_id = fname.rsplit('(', 1)[1].split(')')[0]
        if not calibre_id.isdigit():
            calibre_id = ''
    except IndexError:
        calibre_id = ''
    if not calibre_id:
        # ask calibre for id of this issue
        res, err, rc = lazylibrarian.calibre.calibredb('search', ['author:"%s" title:"%s"' % (author, title)])
        if not rc:
            try:
                calibre_id = res.split(',')[0].strip()
            except IndexError:
                calibre_id = ''
    logger.debug('Calibre ID [%s]' % calibre_id)
    return calibre_id


def clear_log():
    error = False
    if os.name == 'nt':
        return "Screen log cleared"

    logger.lazylibrarian_log.stop_logger()
    for f in glob.glob(CONFIG['LOGDIR'] + "/*.log*"):
        try:
            os.remove(syspath(f))
        except OSError as err:
            error = err.strerror
            logger.debug("Failed to remove %s : %s" % (f, error))

    logger.lazylibrarian_log.init_logger(config=CONFIG)

    if error:
        return 'Failed to clear logfiles: %s' % error
    else:
        return "Log cleared, level set to [%s]- Log Directory is [%s]" % (
            lazylibrarian_log.LOGLEVEL, CONFIG['LOGDIR'])


# noinspection PyUnresolvedReferences,PyPep8Naming
def log_header(online=True):
    popen_list = [sys.executable, DIRS.FULL_PATH]
    popen_list += DIRS.ARGS
    header = "Startup cmd: %s\n" % str(popen_list)
    header += "config file: %s\n" % CONFIG.configfilename
    header += 'Interface: %s\n' % CONFIG['HTTP_LOOK']
    header += 'Loglevel: %s\n' % lazylibrarian_log.LOGLEVEL
    header += 'Sys_Encoding: %s\n' % lazylibrarian.SYS_ENCODING
    for item in CONFIG_GIT:
        if item == 'GIT_UPDATED':
            timestamp = CONFIG.get_int(item)
            header += '%s: %s\n' % (item.lower(), time.ctime(timestamp))
        else:
            header += '%s: %s\n' % (item.lower(), CONFIG[item])
    try:
        header += 'package version: %s\n' % lazylibrarian.version.PACKAGE_VERSION
    except AttributeError:
        pass
    try:
        header += 'packaged by: %s\n' % lazylibrarian.version.PACKAGED_BY
    except AttributeError:
        pass

    db_version = 0
    db = database.DBConnection()
    result = db.match('PRAGMA user_version')
    if result and result[0]:
        value = str(result[0])
        if value.isdigit():
            db_version = int(value)
    uname = platform.uname()
    header += "db version: %s\n" % db_version
    header += "Python version: %s\n" % sys.version.split('\n')
    header += "uname: %s\n" % str(uname)
    header += "Platform: %s\n" % platform.platform(aliased=True)
    if uname[0] == 'Darwin':
        header += "mac_ver: %s\n" % str(platform.mac_ver())
    elif uname[0] == 'Windows':
        header += "win_ver: %s\n" % str(platform.win32_ver())
    if 'urllib3' in globals():
        header += "urllib3: %s\n" % getattr(urllib3, '__version__', None)
    else:
        header += "urllib3: not found\n"
    header += "requests: %s\n" % getattr(requests, '__version__', None)
    if online:
        try:
            if CONFIG.get_bool('SSL_VERIFY'):
                tls_version = requests.get('https://www.howsmyssl.com/a/check', timeout=30,
                                           verify=CONFIG['SSL_CERTS']
                                           if CONFIG['SSL_CERTS'] else True).json()['tls_version']
            else:
                logger.info('Checking TLS version, you can ignore any "InsecureRequestWarning" message')
                tls_version = requests.get('https://www.howsmyssl.com/a/check', timeout=30,
                                           verify=False).json()['tls_version']
            if '1.2' not in tls_version and '1.3' not in tls_version:
                header += 'tls: missing required functionality. Try upgrading to v1.2 or newer. You have '
        except Exception as err:
            tls_version = str(err)
        header += "tls: %s\n" % tls_version

    header += "cherrypy: %s\n" % getattr(cherrypy, '__version__', None)
    header += "sqlite3: %s\n" % getattr(sqlite3, 'sqlite_version', None)
    header += "mako: %s\n" % getattr(mako, '__version__', None)
    header += "webencodings: %s\n" % getattr(webencodings, 'VERSION', None)

    from lazylibrarian.notifiers import APPRISE_VER
    if APPRISE_VER and APPRISE_VER[0].isdigit():
        header += "apprise: %s\n" % APPRISE_VER
    else:
        header += "apprise: not found\n"
    if lazylibrarian.UNRARLIB == 1:
        vers = lazylibrarian.RARFILE.unrarlib.RARGetDllVersion()
        header += "unrar: %s\n" % vers
    elif lazylibrarian.UNRARLIB == 2:
        import lib.UnRAR2 as UnRAR2
        vers = getattr(UnRAR2, '__version__', None)
        header += "unrar2: %s\n" % vers
        if os.name == 'nt':
            vers = UnRAR2.windows.RARGetDllVersion()
            header += "unrar dll: %s\n" % vers
    else:
        header += "unrar: not found\n"

    header += "bs4: %s\n" % getattr(bs4, '__version__', None)
    header += "html5lib: %s\n" % getattr(html5lib, '__version__', None)

    try:
        import PIL
        vers = getattr(PIL, '__version__', None)
        header += "python imaging: %s\n" % vers
        import icrawler
        header += "icrawler: %s\n" % getattr(icrawler, '__version__', None)
    except ImportError:
        header += "python imaging: not found, unable to use icrawler\n"

    header += "openssl: %s\n" % getattr(ssl, 'OPENSSL_VERSION', None)
    X509 = None
    cryptography = None
    try:
        # pyOpenSSL 0.14 and above use cryptography for OpenSSL bindings. The _x509
        # attribute is only present on those versions.
        # noinspection PyUnresolvedReferences
        import OpenSSL
    except ImportError:
        header += "pyOpenSSL: not found\n"
        OpenSSL = None

    if OpenSSL:
        try:
            # noinspection PyUnresolvedReferences
            from OpenSSL.crypto import X509
        except ImportError:
            header += "pyOpenSSL.crypto X509: not found\n"

    if X509:
        # noinspection PyCallingNonCallable
        x509 = X509()
        if getattr(x509, "_x509", None) is None:
            header += "pyOpenSSL: module missing required functionality. Try upgrading to v0.14 or newer. You have "
        header += "pyOpenSSL: %s\n" % getattr(OpenSSL, '__version__', None)

    if OpenSSL:
        try:
            import OpenSSL.SSL
        except (ImportError, AttributeError) as err:
            header += 'pyOpenSSL missing SSL module/attribute: %s\n' % err

    if OpenSSL:
        try:
            # get_extension_for_class method added in `cryptography==1.1`; not available in older versions
            # but need cryptography >= 1.3.4 for access from pyopenssl >= 0.14
            # noinspection PyUnresolvedReferences
            import cryptography
        except ImportError:
            header += "cryptography: not found\n"

    if cryptography:
        try:
            # noinspection PyUnresolvedReferences
            from cryptography.x509.extensions import Extensions
            if getattr(Extensions, "get_extension_for_class", None) is None:
                header += "cryptography: module missing required functionality."
                header += " Try upgrading to v1.3.4 or newer. You have "
            header += "cryptography: %s\n" % getattr(cryptography, '__version__', None)
        except ImportError:
            header += "cryptography Extensions: not found\n"

    import thefuzz as fuzz
    vers = getattr(fuzz, '__version__', None)
    header += "fuzz: %s\n" % vers if vers else 'not found'
    if vers:
        # noinspection PyBroadException
        try:
            import Levenshtein
            vers = getattr(Levenshtein, "__version__", None)
            if not vers:
                vers = "installed"
        except Exception:
            vers = "not found"
        header += "Levenshtein: %s\n" % vers

    try:
        import magic
        try:
            if hasattr(magic, "magic_version"):
                vers = magic.magic_version()
            else:
                # noinspection PyProtectedMember
                vers = magic.libmagic._name
        except AttributeError:
            vers = 'not found'
    except Exception:  # magic might fail for multiple reasons
        vers = 'not found'
    header += "magic: %s\n" % vers

    return header


def save_log():
    if not path_exists(CONFIG['LOGDIR']):
        return 'LOGDIR does not exist'

    basename = DIRS.get_logfile('lazylibrarian.log')
    outfile = DIRS.get_logfile('debug')

    out = open(syspath(outfile + '.tmp'), 'w', encoding='utf-8')

    nextfile = True
    extn = 0
    redacts = 0
    while nextfile:
        fname = basename
        if extn > 0:
            fname = fname + '.' + str(extn)
        if not path_exists(fname):
            logger.debug("logfile [%s] does not exist" % fname)
            nextfile = False
        else:
            logger.debug('Processing logfile [%s]' % fname)
            linecount = 0
            lines = reversed(list(open(syspath(fname), 'r', encoding="utf-8")))
            for line in lines:
                for item in CONFIG.REDACTLIST:
                    if item in line:
                        line = line.replace(item, '<redacted>')
                        redacts += 1

                item = "Apprise: url:"
                startpos = line.find(item)
                if startpos >= 0:
                    startpos += len(item)
                    endpos = line.find('//', startpos)
                    line = line[:endpos] + '<redacted>'
                    redacts += 1

                out.write(line)
                if "Debug log ON" in line:
                    logger.debug('Found "Debug log ON" line %s in %s' % (linecount, fname))
                    nextfile = False
                    break
                linecount += 1
            extn += 1

    if path_exists(CONFIG.configfilename):
        out.write(u'---END-CONFIG---------------------------------\n')
        lines = reversed(list(open(syspath(CONFIG.configfilename), 'r', encoding="utf-8")))
        for line in lines:
            for item in CONFIG.REDACTLIST:
                if item in line:
                    line = line.replace(item, '<redacted>')
                    redacts += 1
            out.write(line)
        out.write(u'---CONFIG-------------------------------------\n')
    out.close()
    logfile = open(syspath(outfile + '.log'), 'w', encoding='utf-8')
    logfile.write(log_header())
    linecount = 0
    lines = reversed(list(open(syspath(outfile + '.tmp'), 'r', encoding="utf-8")))
    for line in lines:
        logfile.write(line)
        linecount += 1
    logfile.close()
    remove_file(outfile + '.tmp')
    logger.debug("Redacted %s passwords/apikeys" % redacts)
    logger.debug("%s log lines written to %s" % (linecount, outfile + '.log'))
    with zipfile.ZipFile(outfile + '.zip', 'w') as myzip:
        myzip.write(outfile + '.log', 'debug.log')
    remove_file(outfile + '.log')
    return "Debug log saved as %s" % (outfile + '.zip')


def zip_audio(source, zipname, bookid):
    """ Zip up all the audiobook parts in source folder to zipname
        Check if zipfile already exists, if not create a new one
        Doesn't actually check for audiobook parts, just zips everything
        including any .jpg etc.
        Return full path to zipfile
    """
    zip_file = os.path.join(source, zipname + '.zip')
    if not path_exists(zip_file):
        logger.debug('Zipping up %s' % zipname)
        namevars = lazylibrarian.bookrename.name_vars(bookid)
        singlefile = namevars['AudioSingleFile']

        cnt = 0
        with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as myzip:
            for rootdir, _, filenames in walk(source):
                for filename in filenames:
                    # don't include self or our special index file
                    if not filename.endswith('.zip') and not filename.endswith('.ll'):
                        bname, extn = os.path.splitext(filename)
                        # don't include singlefile
                        if bname != singlefile:
                            cnt += 1
                            myzip.write(os.path.join(rootdir, filename), filename)
        logger.debug('Zipped up %s files' % cnt)
        _ = setperm(zip_file)
    return zip_file


def run_script(params):
    if os.name == 'nt' and params[0].endswith('.py'):
        params.insert(0, sys.executable)
    logger.debug(str(params))
    try:
        if os.name != 'nt':
            p = subprocess.Popen(params, preexec_fn=lambda: os.nice(10),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res, err = p.communicate()
        if lazylibrarian_log.LOGLEVEL & logger.log_dlcomms:
            logger.debug(make_unicode(res))
            logger.debug(make_unicode(err))
        return p.returncode, make_unicode(res), make_unicode(err)
    except Exception as er:
        err = "run_script exception: %s %s" % (type(er).__name__, str(er))
        logger.error(err)
        return 1, '', err


def calibre_prg(prgname):
    # Try to locate a calibre ancilliary program
    # Try explicit path or in the calibredb location
    # or in current path or system path
    target = ''
    if prgname == 'ebook-convert':
        target = CONFIG['EBOOK_CONVERT']
    if not target:
        calibre = CONFIG['IMP_CALIBREDB']
        if calibre:
            target = os.path.join(os.path.dirname(calibre), prgname)
        else:
            logger.debug("No calibredb configured")

    if not target or not os.path.exists(target):
        target = os.path.join(os.getcwd(), prgname)
        if not os.path.exists(target):
            logger.debug("%s not found" % target)
            if os.name == 'nt':
                try:
                    params = ["where", prgname]
                    res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                    target = make_unicode(res).strip()
                except Exception as err:
                    logger.debug("where %s failed: %s %s" % (prgname, type(err).__name__, str(err)))
                    target = ''
            else:
                try:
                    params = ["which", prgname]
                    res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                    target = make_unicode(res).strip()
                except Exception as err:
                    logger.debug("which %s failed: %s %s" % (prgname, type(err).__name__, str(err)))
                    target = ''
    if target:
        logger.debug("Using %s" % target)
        try:
            params = [target, "--version"]
            res = subprocess.check_output(params, stderr=subprocess.STDOUT)
            res = make_unicode(res).strip().split("(")[1].split(")")[0]
            logger.debug("Found %s version %s" % (prgname, res))
        except Exception as err:
            logger.debug("%s --version failed: %s %s" % (prgname, type(err).__name__, str(err)))
            target = ''
    return target


def only_punctuation(value):
    for c in value:
        if c not in string.punctuation and c not in string.whitespace:
            return False
    return True
