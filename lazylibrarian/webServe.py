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


import datetime
import hashlib
import json
import logging
import os
import random
import re
import subprocess
import threading
import time
import traceback
import uuid
from shutil import copyfile, rmtree

import cherrypy
from cherrypy.lib.static import serve_file
from urllib.parse import quote_plus, unquote_plus, urlsplit, urlunsplit

import lazylibrarian
from lazylibrarian.config2 import CONFIG, wishlist_type
from lazylibrarian import database, notifiers, versioncheck, magazinescan, comicscan, \
    qbittorrent, utorrent, rtorrent, transmission, sabnzbd, nzbget, deluge, synology, grsync
from lazylibrarian.configtypes import ConfigBool
from lazylibrarian.auth import AuthController
from lazylibrarian.bookrename import name_vars
from lazylibrarian.bookwork import set_series, delete_empty_series, add_series_members, NEW_WHATWORK
from lazylibrarian.cache import cache_img, ImageType
from lazylibrarian.calibre import calibre_test, sync_calibre_list, calibredb, get_calibre_id
from lazylibrarian.comicid import cv_identify, cx_identify, name_words, title_words
from lazylibrarian.comicsearch import search_comics
from lazylibrarian.common import create_support_zip, log_header, pwd_generator, pwd_check, \
    is_valid_email, mime_type, zip_audio, run_script
from lazylibrarian.filesystem import DIRS, path_isfile, path_isdir, syspath, path_exists, remove_file, listdir, walk, \
    setperm, safe_move, safe_copy, opf_file, csv_file, book_file, get_directory
from lazylibrarian.scheduling import schedule_job, show_jobs, restart_jobs, check_running_jobs, \
    ensure_running, all_author_update, show_stats, SchedulerCommand
from lazylibrarian.csvfile import import_csv, export_csv, dump_table, restore_table
from lazylibrarian.dbupgrade import check_db
from lazylibrarian.downloadmethods import nzb_dl_method, tor_dl_method, direct_dl_method, \
    irc_dl_method
from lazylibrarian.formatter import unaccented, plural, now, today, check_int, \
    safe_unicode, clean_name, surname_first, sort_definite, get_list, make_unicode, make_utf8bytes, \
    md5_utf8, date_format, check_year, replace_quotes_with, format_author_name, check_float, thread_name
from lazylibrarian.gb import GoogleBooks
from lazylibrarian.gr import GoodReads
from lazylibrarian.images import get_book_cover, create_mag_cover, coverswap, get_author_image, createthumb
from lazylibrarian.importer import add_author_to_db, add_author_name_to_db, update_totals, search_for, \
    get_preferred_author_name
from lazylibrarian.librarysync import library_scan
from lazylibrarian.logconfig import LOGCONFIG
from lazylibrarian.manualbook import search_item
from lazylibrarian.notifiers import notify_snatch, custom_notify_snatch
from lazylibrarian.ol import OpenLibrary
from lazylibrarian.opds import OPDS
from lazylibrarian.opfedit import opf_read, opf_write
from lazylibrarian.postprocess import process_alternate, process_dir, delete_task, get_download_progress, \
    create_opf, process_book_from_dir, process_issues
from lazylibrarian.providers import test_provider
from lazylibrarian.rssfeed import gen_feed
from lazylibrarian.searchbook import search_book
from lazylibrarian.searchmag import search_magazines, download_maglist
from lazylibrarian.searchrss import search_wishlist
from lazylibrarian.telemetry import TELEMETRY
from lazylibrarian.blockhandler import BLOCKHANDLER
from deluge_client import DelugeRPCClient
from mako import exceptions
from mako.lookup import TemplateLookup

from thefuzz import fuzz

lastauthor = ''
lastmagazine = ''
lastcomic = ''


def clear_mako_cache(userid=0):
    logger = logging.getLogger(__name__)
    if userid:
        logger.warning("Clearing mako cache %s" % userid)
        makocache = os.path.join(DIRS.CACHEDIR, 'mako', str(userid))
    else:
        logger.warning("Clearing mako cache")
        makocache = os.path.join(DIRS.CACHEDIR, 'mako')
    try:
        rmtree(makocache, ignore_errors=True)
        # noinspection PyArgumentList
        os.makedirs(makocache, exist_ok=True)
    except Exception as e:
        logger.error("Error clearing mako cache: %s" % str(e))


def serve_template(templatename, **kwargs):
    thread_name("WEBSERVER")
    logger = logging.getLogger(__name__)
    loggeradmin = logging.getLogger('special.admin')

    interface_dir = os.path.join(str(DIRS.PROG_DIR), 'data', 'interfaces')
    template_dir = os.path.join(str(interface_dir), CONFIG['HTTP_LOOK'])
    if not path_isdir(template_dir):
        logger.error("Unable to locate template [%s], reverting to bookstrap" % template_dir)
        CONFIG.set_str('HTTP_LOOK', 'bookstrap')
        template_dir = os.path.join(str(interface_dir), CONFIG['HTTP_LOOK'])

    if templatename in ['logs.html', 'history.html']:
        # don't cache these so we can change refresh rate
        module_directory = None
    else:
        module_directory = os.path.join(DIRS.CACHEDIR, 'mako')
    _hplookup = TemplateLookup(directories=[template_dir], input_encoding='utf-8',
                               module_directory=module_directory)
    # noinspection PyBroadException
    try:
        style = CONFIG['BOOKSTRAP_THEME']
        userprefs = 0
        usertheme = ''
        if lazylibrarian.UPDATE_MSG:
            template = _hplookup.get_template("dbupdate.html")
            return template.render(perm=0, message="Database upgrade in progress, please wait...",
                                   title="Database Upgrade", timer=5, style=style)

        if not CONFIG.get_bool('USER_ACCOUNTS'):
            perm = lazylibrarian.perm_admin
            try:
                template = _hplookup.get_template(templatename)
            except (AttributeError, KeyError):
                clear_mako_cache()
                template = _hplookup.get_template(templatename)
        else:
            username = ''  # anyone logged in yet?
            userid = 0
            perm = 0
            res = None
            cookie = None
            db = database.DBConnection()
            try:
                if lazylibrarian.LOGINUSER:
                    res = db.match('SELECT UserName,Perms from users where UserID=?', (lazylibrarian.LOGINUSER,))
                    if res:
                        cherrypy.response.cookie['ll_uid'] = lazylibrarian.LOGINUSER
                        userid = lazylibrarian.LOGINUSER
                        logger.debug("Auto-login for %s" % res['UserName'])
                        lazylibrarian.SHOWLOGOUT = 0
                    else:
                        logger.debug("Auto-login failed for userid %s" % lazylibrarian.LOGINUSER)
                        cherrypy.response.cookie['ll_uid'] = ''
                        cherrypy.response.cookie['ll_uid']['expires'] = 0
                        cherrypy.response.cookie['ll_prefs'] = '0'
                        cherrypy.response.cookie['ll_prefs']['expires'] = 0
                    lazylibrarian.LOGINUSER = None

                else:
                    cookie = cherrypy.request.cookie
                    if cookie and 'll_uid' in list(cookie.keys()):
                        res = db.match('SELECT UserName,Perms,UserID from users where UserID=?', (cookie['ll_uid'].value,))
                    if not res:
                        columns = db.select('PRAGMA table_info(users)')
                        if not columns:  # no such table
                            cnt = 0
                        else:
                            cnt = db.match("select count(*) as counter from users")
                        if cnt and cnt['counter'] == 1 and CONFIG.get_bool('SINGLE_USER') and \
                                templatename not in ["register.html", "response.html", "opds.html"]:
                            res = db.match('SELECT UserName,Perms,Prefs,UserID from users')
                            cherrypy.response.cookie['ll_uid'] = res['UserID']
                            cherrypy.response.cookie['ll_prefs'] = res['Prefs']
                            logger.debug("Auto-login for %s" % res['UserName'])
                            lazylibrarian.SHOWLOGOUT = 0
                        else:
                            lazylibrarian.SHOWLOGOUT = 1
                if res:
                    perm = check_int(res['Perms'], 0)
                    username = res['UserName']
                    userid = res['UserID']
                    try:
                        res2 = db.match('SELECT Theme from users where UserID=?', (userid,))
                        if res2:
                            usertheme = res2['Theme']
                            if not usertheme:
                                usertheme = ''
                    except Exception as e:
                        logger.debug("Unable to get user theme for %s: %s" % (userid, str(e)))
            finally:
                db.close()
            if cookie and 'll_prefs' in list(cookie.keys()):
                userprefs = check_int(cookie['ll_prefs'].value, 0)

            if perm == 0 and templatename not in ["register.html", "response.html", "opds.html"]:
                if CONFIG.get_str('auth_type') == 'FORM':
                    templatename = "formlogin.html"
                else:
                    templatename = "login.html"
            elif (templatename == 'config.html' and not perm & lazylibrarian.perm_config) or \
                    (templatename == 'logs.html' and not perm & lazylibrarian.perm_logs) or \
                    (templatename == 'history.html' and not perm & lazylibrarian.perm_history) or \
                    (templatename == 'managebooks.html' and not perm & lazylibrarian.perm_managebooks) or \
                    (templatename == 'books.html' and not perm & lazylibrarian.perm_ebook) or \
                    (templatename == 'author.html' and not perm & lazylibrarian.perm_ebook
                     and not perm & lazylibrarian.perm_audio) or \
                    (templatename in ['magazines.html', 'issues.html', 'manageissues.html']
                     and not perm & lazylibrarian.perm_magazines) or \
                    (templatename in ['comics.html', 'comicissues.html', 'comicresults.html']
                     and not perm & lazylibrarian.perm_comics) or \
                    (templatename == 'audio.html' and not perm & lazylibrarian.perm_audio) or \
                    (templatename == 'choosetype.html' and not perm & lazylibrarian.perm_download) or \
                    (templatename in ['series.html', 'members.html'] and not perm & lazylibrarian.perm_series) or \
                    (templatename in ['editauthor.html', 'editbook.html'] and not perm & lazylibrarian.perm_edit) or \
                    (templatename in ['manualsearch.html', 'searchresults.html']
                     and not perm & lazylibrarian.perm_search):
                logger.warning('User %s attempted to access %s' % (username, templatename))
                if CONFIG.get_str('auth_type') == 'FORM':
                    templatename = "formlogin.html"
                else:
                    templatename = "login.html"

            loggeradmin.debug("User %s: %s %s %s %s" % (username, perm, userprefs, usertheme, templatename))

            theme = usertheme.split('_', 1)[0]
            if theme and theme != CONFIG['HTTP_LOOK']:
                template_dir = os.path.join(str(interface_dir), theme)
                if not path_isdir(template_dir):
                    logger.error("Unable to locate template [%s], reverting to bookstrap" % template_dir)
                    CONFIG.set_str('HTTP_LOOK', 'bookstrap')
                    template_dir = os.path.join(str(interface_dir), CONFIG['HTTP_LOOK'])

                module_directory = os.path.join(DIRS.CACHEDIR, 'mako', str(userid))
                _hplookup = TemplateLookup(directories=[template_dir], input_encoding='utf-8',
                                           module_directory=module_directory)
            try:
                template = _hplookup.get_template(templatename)
            except (AttributeError, KeyError):
                clear_mako_cache(userid)
                template = _hplookup.get_template(templatename)
                
        theme = usertheme.split('_', 1)
        if len(theme) > 1:
            style = theme[1]

        if templatename in ["login.html", "formlogin.html"]:
            cherrypy.response.cookie['ll_template'] = ''
            return template.render(perm=0, title="Redirected", style=style)

        # keep template name for help context
        cherrypy.response.cookie['ll_template'] = templatename
        return template.render(perm=perm, pref=userprefs, style=style, **kwargs)

    except Exception:
        return exceptions.html_error_template().render()


# noinspection PyProtectedMember,PyGlobalUndefined,PyGlobalUndefined
class WebInterface(object):

    auth = AuthController()

    @cherrypy.expose
    def index(self):
        raise cherrypy.HTTPRedirect("home")

    @cherrypy.expose
    def authors(self):
        title = 'Authors'
        if lazylibrarian.IGNORED_AUTHORS:
            if CONFIG.get_bool('IGNORE_PAUSED'):
                title = 'Inactive Authors'
            else:
                title = 'Ignored Authors'
        return serve_template(templatename="index.html", title=title)

    @cherrypy.expose
    def home(self):
        logger = logging.getLogger(__name__)
        home = CONFIG.get_str('HOMEPAGE')
        logger.debug("Homepage [%s]" % home)
        if home == 'eBooks':
            raise cherrypy.HTTPRedirect("books")
        elif home == 'Series':
            raise cherrypy.HTTPRedirect("series")
        elif home == 'AudioBooks':
            raise cherrypy.HTTPRedirect("audio")
        elif home == 'Magazines':
            raise cherrypy.HTTPRedirect("magazines")
        elif home == 'Comics':
            raise cherrypy.HTTPRedirect("comics")
        else:
            raise cherrypy.HTTPRedirect("authors")

    @cherrypy.expose
    def profile(self):
        title = 'User Profile'
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            db = database.DBConnection()
            try:
                user = db.match('SELECT UserName,UserID,Name,Email,SendTo,BookType,Theme from users where UserID=?',
                                (cookie['ll_uid'].value,))
                if user:
                    subs = db.select('SELECT Type,WantID from subscribers WHERE UserID=?', (cookie['ll_uid'].value,))
                    subscriptions = ''
                    for item in subs:
                        if subscriptions:
                            subscriptions += '\n'
                        item_name = ''
                        if item['Type'] == 'author':
                            res = db.match('SELECT AuthorName from authors WHERE authorid=?', (item['WantID'],))
                            if res:
                                item_name = "(%s)" % res['AuthorName']
                        elif item['Type'] == 'series':
                            res = db.match('SELECT SeriesName from series WHERE seriesid=?', (item['WantID'],))
                            if res:
                                item_name = "(%s)" % res['SeriesName']
                        elif item['Type'] == 'comic':
                            try:
                                comicid, issueid = item['WantID'].split('_')
                            except ValueError:
                                comicid = ''
                            if comicid:
                                res = db.match('SELECT Title from comics WHERE comicid=?', (comicid,))
                                if res:
                                    item_name = "(%s)" % res['Title']
                        subscriptions += '%s %s %s' % (item['Type'], item['WantID'], item_name)
                    user = dict(user)
                    if not user['Theme']:
                        user['Theme'] = ''
                    themelist = ['Default']
                    for item in lazylibrarian.BOOKSTRAP_THEMELIST:
                        themelist.append('bookstrap_' + item)
                    return serve_template(templatename="profile.html", title=title, user=user, subs=subscriptions,
                                          typelist=get_list(CONFIG['EBOOK_TYPE']), themelist=themelist)
            finally:
                db.close()
        return serve_template(templatename="index.html", title=title)

    # noinspection PyUnusedLocal
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_index(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        rows = []
        filtered = []
        rowlist = []
        userid = None
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)

        db = database.DBConnection()
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)

            cmd = 'SELECT AuthorImg,AuthorName,LastBook,LastDate,Status,AuthorLink,LastLink,'
            cmd += 'HaveBooks,UnignoredBooks,AuthorID,LastBookID,DateAdded,Reason from authors '
            if lazylibrarian.IGNORED_AUTHORS:
                cmd += 'where Status == "Ignored" '
                if CONFIG.get_bool('IGNORE_PAUSED'):
                    cmd += 'or Status == "Paused" '
            else:
                cmd += 'where Status != "Ignored" '
                if CONFIG.get_bool('IGNORE_PAUSED'):
                    cmd += 'and  Status != "Paused" '

            myauthors = []
            if userid and userprefs & lazylibrarian.pref_myauthors:
                res = db.select('SELECT WantID from subscribers WHERE Type="author" and UserID=?', (userid,))
                loggerserverside.debug("User subscribes to %s authors" % len(res))
                for author in res:
                    myauthors.append(author['WantID'])
                cmd += ' and AuthorID in (' + ', '.join(('"{0}"'.format(w) for w in myauthors)) + ')'

            cmd += ' order by AuthorName COLLATE NOCASE'

            loggerserverside.debug("get_index %s" % cmd)

            rowlist = db.select(cmd)
            # At his point we want to sort and filter _before_ adding the html as it's much quicker
            # turn the sqlite rowlist into a list of lists
            if len(rowlist):
                for row in rowlist:  # iterate through the sqlite3.Row objects
                    arow = list(row)
                    if CONFIG.get_bool('SORT_SURNAME'):
                        arow[1] = surname_first(arow[1], postfixes=CONFIG.get_list('NAME_POSTFIX'))
                    if CONFIG.get_bool('SORT_DEFINITE'):
                        arow[2] = sort_definite(arow[2], articles=CONFIG.get_list('NAME_DEFINITE'))
                    arow[3] = date_format(arow[3], '', f'{arow[1]}/{arow[2]}')
                    nrow = arow[:4]
                    havebooks = check_int(arow[7], 0)
                    totalbooks = check_int(arow[8], 0)
                    if totalbooks:
                        percent = int((havebooks * 100.0) / totalbooks)
                    else:
                        percent = 0
                    if percent > 100:
                        percent = 100

                    if percent <= 25:
                        css = 'danger'
                    elif percent <= 50:
                        css = 'warning'
                    elif percent <= 75:
                        css = 'info'
                    else:
                        css = 'success'

                    arow[12] = replace_quotes_with(arow[12], '')
                    nrow.append(percent)
                    nrow.extend(arow[4:-2])
                    bar = ''
                    nrow.append(bar)
                    nrow.extend(arow[11:])
                    rows.append(nrow)  # add each rowlist to the masterlist
                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in rows if ssearch.lower() in str(x).lower()]
                else:
                    filtered = rows

                sortcolumn = int(isort_col) - 1
                if sortcolumn == 2:
                    sortcolumn = 13
                elif sortcolumn > 2:
                    sortcolumn -= - 1

                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                              reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

            loggerserverside.debug("get_index returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_index filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_index: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            db.close()
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      'loading': lazylibrarian.AUTHORS_UPDATE,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @staticmethod
    def label_thread(name=None):
        if name:
            thread_name(name)
        else:
            threadname = thread_name()
            if "Thread-" in threadname:
                thread_name("WEBSERVER")

    # USERS ############################################################

    @cherrypy.expose
    def help(self):
        cookie = cherrypy.request.cookie
        if cookie and 'll_template' in list(cookie.keys()):
            template = cookie['ll_template'].value
            for item in [['index.html', 'overview'],
                         ['books.html', 'ebooks'],
                         ['series.html', 'series'],
                         ['audio.html', 'audiobooks'],
                         ['magazines.html', 'magazines'],
                         ['managebooks.html', 'manage'],
                         ['history.html', 'history'],
                         ['logs.html', 'logs'],
                         ['config.html', 'config_menus'],
                         ['author.html', 'authors'],
                         ['issues.html', 'magazine_detail'],
                         ['users.html', 'config_users'],
                         ]:
                if template == item[0]:
                    page = item[1]
                    if template == 'config.html':
                        if 'configTab' in list(cookie.keys()):
                            tab = check_int(cookie['configTab'].value, 1)
                            tabs = ['interface', 'importing', 'downloaders', 'providers', 'processing',
                                    'notifications', 'categories', 'filters', 'genres']
                            try:
                                page = 'config_' + tabs[tab - 1]
                            except IndexError:
                                pass
                    raise cherrypy.HTTPRedirect("https://lazylibrarian.gitlab.io/" + page)
        raise cherrypy.HTTPRedirect("https://lazylibrarian.gitlab.io/")

    @cherrypy.expose
    def logout(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        if cookie and 'll_uid' in list(cookie.keys()):
            db = database.DBConnection()
            try:
                db.action('UPDATE users SET prefs=? where UserID=?', (userprefs, cookie['ll_uid'].value))
            finally:
                db.close()
        cherrypy.response.cookie['ll_uid'] = ''
        cherrypy.response.cookie['ll_uid']['expires'] = 0
        cherrypy.response.cookie['ll_prefs'] = '0'
        cherrypy.response.cookie['ll_prefs']['expires'] = 0
        # cherrypy.lib.sessions.expire()
        raise cherrypy.HTTPRedirect("home")

    @cherrypy.expose
    def user_register(self):
        self.label_thread("REGISTER")
        return serve_template(templatename="register.html", title="User Registration / Contact form")

    @cherrypy.expose
    def user_update(self, **kwargs):
        if 'password' in kwargs and 'password2' in kwargs and kwargs['password']:
            if kwargs['password'] != kwargs['password2']:
                return "Passwords do not match"
        if kwargs['password']:
            if not pwd_check(kwargs['password']):
                return "Password must be at least 8 digits long\nand not contain spaces"
        logger = logging.getLogger(__name__)
        changes = ''
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
            db = database.DBConnection()
            try:
                user = db.match('SELECT UserName,Name,Email,Password,BookType,SendTo,Theme from users where UserID=?',
                                (userid,))
                if user:
                    if kwargs['username'] and user['UserName'] != kwargs['username']:
                        # if username changed, must not have same username as another user
                        match = db.match('SELECT UserName from users where UserName=?', (kwargs['username'],))
                        if match:
                            return "Unable to change username: already exists"
                        else:
                            changes += ' username'
                            db.action('UPDATE users SET UserName=? WHERE UserID=?', (kwargs['username'], userid))

                    if kwargs['fullname'] and user['Name'] != kwargs['fullname']:
                        changes += ' name'
                        db.action('UPDATE users SET Name=? WHERE UserID=?', (kwargs['fullname'], userid))

                    if user['Email'] != kwargs['email']:
                        changes += ' email'
                        db.action('UPDATE users SET email=? WHERE UserID=?', (kwargs['email'], userid))

                    if user['Theme'] != kwargs['theme']:
                        valid = False
                        theme = kwargs['theme']
                        if theme in ['None', 'Default', '']:
                            theme = ''
                        if not theme:
                            valid = True
                        if theme == 'legacy':
                            valid = True
                        else:
                            parts = theme.split('_', 1)
                            if parts[0] == 'bookstrap':
                                if len(parts) == 2 and parts[1] in lazylibrarian.BOOKSTRAP_THEMELIST:
                                    valid = True
                        if valid:
                            changes += ' Theme'
                            db.action('UPDATE users SET Theme=? WHERE UserID=?', (theme, userid))
                        else:
                            logger.warning("Invalid user theme [%s]" % theme)

                    if user['SendTo'] != kwargs['sendto']:
                        changes += ' sendto'
                        db.action('UPDATE users SET sendto=? WHERE UserID=?', (kwargs['sendto'], userid))

                    if user['BookType'] != kwargs['booktype']:
                        changes += ' BookType'
                        db.action('UPDATE users SET BookType=? WHERE UserID=?', (kwargs['booktype'], userid))

                    if kwargs['password']:
                        pwd = md5_utf8(kwargs['password'])
                        if pwd != user['password']:
                            changes += ' password'
                            db.action('UPDATE users SET password=? WHERE UserID=?', (pwd, userid))

                    # only allow admin to change these
                    # if kwargs['calread'] and user['CalibreRead'] != kwargs['calread']:
                    #     changes += ' CalibreRead'
                    #     db.action('UPDATE users SET CalibreRead=? WHERE UserID=?', (kwargs['calread'], userid))

                    # if kwargs['caltoread'] and user['CalibreToRead'] != kwargs['caltoread']:
                    #     changes += ' CalibreToRead'
                    #     db.action('UPDATE users SET CalibreToRead=? WHERE UserID=?', (kwargs['caltoread'], userid))
            finally:
                db.close()

            if changes:
                return 'Updated user details:%s' % changes
        return "No changes made"

    @cherrypy.expose
    def user_login(self, **kwargs):
        # anti-phishing
        # block ip address if over 3 failed usernames in a row.
        # don't count attempts older than 24 hrs
        logger = logging.getLogger(__name__)
        self.label_thread("LOGIN")
        limit = int(time.time()) - 1 * 60 * 60
        lazylibrarian.USER_BLOCKLIST[:] = [x for x in lazylibrarian.USER_BLOCKLIST if x[1] > limit]
        remote_ip = cherrypy.request.remote.ip
        cnt = 0
        for item in lazylibrarian.USER_BLOCKLIST:
            if item[0] == remote_ip:
                cnt += 1
        if cnt >= 3:
            msg = "IP address [%s] is blocked" % remote_ip
            logger.warning(msg)
            return msg

        # is it a retry login (failed user/pass)
        cookie = cherrypy.request.cookie
        if not cookie or 'll_uid' not in list(cookie.keys()):
            cherrypy.response.cookie['ll_uid'] = ''
            cherrypy.response.cookie['ll_prefs'] = ''
        username = ''
        password = ''
        res = {}
        pwd = ''
        if 'username' in kwargs:
            username = kwargs['username']
        if 'password' in kwargs:
            password = kwargs['password']
        if username and password:
            pwd = md5_utf8(password)
            db = database.DBConnection()
            try:
                res = db.match('SELECT UserID,Prefs,Password from users where username=?', (username,))  # type: dict
            finally:
                db.close()
        if res and pwd == res['Password']:
            cherrypy.response.cookie['ll_uid'] = res['UserID']
            cherrypy.response.cookie['ll_prefs'] = res['Prefs']
            if 'remember' in kwargs:
                cherrypy.response.cookie['ll_uid']['Max-Age'] = '86400'

            # successfully logged in, clear any failed attempts
            lazylibrarian.USER_BLOCKLIST[:] = [x for x in lazylibrarian.USER_BLOCKLIST if not x[0] == username]
            logger.debug("User %s logged in" % username)
            lazylibrarian.SHOWLOGOUT = 1
            return ''
        elif res:
            # anti-phishing. Block user if 3 failed passwords in a row.
            cnt = 0
            for item in lazylibrarian.USER_BLOCKLIST:
                if item[0] == username:
                    cnt += 1
            if cnt >= 2:
                msg = "Too many failed attempts. Reset password or retry after 1 hour"
                logger.warning("Blocked user: %s: [%s] %s" % (username, remote_ip, msg))
            else:
                lazylibrarian.USER_BLOCKLIST.append((username, int(time.time())))
                msg = "Wrong password entered. You have %s %s left" % (2 - cnt, plural(2 - cnt, "attempt"))
            logger.warning("Failed login attempt: %s: [%s] %s" % (username, remote_ip, lazylibrarian.LOGIN_MSG))
        else:
            # invalid or missing username, or valid user but missing password
            msg = "Invalid user or password."
            logger.warning("Blocked IP: %s: [%s] %s" % (username, remote_ip, msg))
            lazylibrarian.USER_BLOCKLIST.append((remote_ip, int(time.time())))
        return msg

    @cherrypy.expose
    def user_contact(self, **kwargs):
        self.label_thread('USERCONTACT')
        logger = logging.getLogger(__name__)
        remote_ip = cherrypy.request.remote.ip
        msg = 'IP: %s\n' % remote_ip
        for item in kwargs:
            if kwargs[item]:
                line = "%s: %s\n" % (item, unaccented(kwargs[item], only_ascii=False))
            else:
                line = "%s: \n" % item
            msg += line
        if 'email' in kwargs and kwargs['email']:
            result = notifiers.email_notifier.notify_message('Message from LazyLibrarian User',
                                                             msg, CONFIG['ADMIN_EMAIL'])
            if result:
                return "Message sent to admin, you will receive a reply by email"
            else:
                logger.error("Unable to send message to admin: %s" % msg)
                return "Message not sent, please try again later"
        else:
            return "No message sent, no return email address"

    @cherrypy.expose
    def user_admin(self):
        self.label_thread('USERADMIN')
        db = database.DBConnection()
        try:
            users = db.select('SELECT UserName from users')
        finally:
            db.close()
        return serve_template(templatename="users.html", title="Manage User Accounts", users=users)

    @cherrypy.expose
    def update_feeds(self, **kwargs):
        logger = logging.getLogger(__name__)
        if 'value' in kwargs and kwargs['value'] == '':
            # cancel or [x] pressed
            return 'No changes made'
        user = kwargs.pop('user', '')
        value = get_list(kwargs.pop('value[]', ''))
        cnt = 0
        db = database.DBConnection()
        try:
            for item in kwargs:
                if '[text]' in item:
                    feedname = kwargs[item]
                    feednum = kwargs.get(item.replace('[text]', '[value]'), '')
                    if feedname and feednum:
                        res = db.match('SELECT * from subscribers WHERE Type=? and UserID=? and WantID=?',
                                       ("feed", user, feedname))
                        if feednum in value:
                            if res:
                                logger.debug("%s %s was already subscribed" % (feedname, user))
                            else:
                                cnt += 1
                                db.action('INSERT INTO subscribers (Type, UserID, WantID) VALUES (?, ?, ?)',
                                          ("feed", user, feedname))
                                logger.debug("Subscribed %s to %s" % (user, feedname))
                        else:
                            if res:
                                cnt += 1
                                db.action('DELETE from subscribers WHERE Type=? and UserID=? and WantID=?',
                                          ("feed", user, feedname))
                                logger.debug("Unsubscribed %s to %s" % (user, feedname))
                            else:
                                logger.debug("%s %s was already unsubscribed" % (feedname, user))
        finally:
            db.close()

        return "Changed %s %s" % (cnt, plural(cnt, 'feed'))

    @cherrypy.expose
    def user_feeds(self, **kwargs):
        logger = logging.getLogger(__name__)
        user = kwargs['user']
        if user:
            feedlist = []
            value = []
            cnt = 0
            db = database.DBConnection()
            try:
                feeds = db.select('SELECT * from subscribers where Type="feed" and UserID=?', (user,))
            finally:
                db.close()
            for provider in CONFIG.providers('RSS'):
                wishtype = wishlist_type(provider['HOST'])
                if wishtype:
                    cnt += 1
                    subscribed = False
                    for item in feeds:
                        if item['WantID'] == provider['DISPNAME']:
                            subscribed = True
                            break
                    feedlist.append({'text': provider['DISPNAME'], 'value': str(cnt)})
                    if subscribed:
                        value.append(str(cnt))
            res = json.dumps({'feeds': feedlist, 'value': value})
            logger.debug(res)
            return res
        return json.dumps({'feeds': '', 'value': ''})

    @cherrypy.expose
    def admin_delete(self, **kwargs):
        db = database.DBConnection()
        try:
            user = kwargs['user']
            if user:
                match = db.match('SELECT Perms from users where UserName=?', (user,))
                if match:
                    perm = check_int(match['Perms'], 0)
                    if perm & 1:
                        count = 0
                        perms = db.select('SELECT Perms from users')
                        for item in perms:
                            val = check_int(item['Perms'], 0)
                            if val & lazylibrarian.perm_config:
                                count += 1
                        if count < 2:
                            return "Unable to delete last administrator"
                    db.action('DELETE from users WHERE UserName=?', (user,))
                    return "User %s deleted" % user
                return "User not found"
            return "No user!"
        finally:
            db.close()

    @cherrypy.expose
    def get_user_profile(self, **kwargs):
        db = database.DBConnection()
        try:
            match = db.match('SELECT * from users where UserName=?', (kwargs['user'],))
            if match:
                subs = db.select('SELECT Type,WantID from subscribers WHERE UserID=?', (match['userid'],))
                subscriptions = ''
                for item in subs:
                    if subscriptions:
                        subscriptions += '\n'
                    subscriptions += '%s %s' % (item['Type'], item['WantID'])
                res = json.dumps({'email': match['Email'], 'name': match['Name'], 'perms': match['Perms'],
                                  'calread': match['CalibreRead'], 'caltoread': match['CalibreToRead'],
                                  'sendto': match['SendTo'], 'booktype': match['BookType'],
                                  'userid': match['UserID'], 'subs': subscriptions, 'theme': match['Theme']})
            else:
                res = json.dumps({'email': '', 'name': '', 'perms': '0', 'calread': '', 'caltoread': '',
                                  'sendto': '', 'booktype': '', 'userid': '', 'subs': '', 'theme': ''})
        finally:
            db.close()
        return res

    @cherrypy.expose
    def admin_users(self, **kwargs):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            user = kwargs['user']
            new_user = not user

            if new_user:
                msg = "New user NOT added: "
                if not kwargs['username']:
                    return msg + "No username given"
                else:
                    # new user must not have same username as an existing one
                    match = db.match('SELECT UserName from users where UserName=?', (kwargs['username'],))
                    if match:
                        return msg + "Username already exists"

                if not kwargs['fullname']:
                    return msg + "No fullname given"

                if not kwargs['email']:
                    return msg + "No email given"

                if not is_valid_email(kwargs['email']):
                    return msg + "Invalid email given"

                perms = check_int(kwargs['perms'], 0)
                if not perms:
                    return msg + "No permissions or invalid permissions given"
                if not kwargs['password']:
                    return msg + "No password given"

                if perms == lazylibrarian.perm_admin:
                    perm_msg = 'ADMIN'
                elif perms == lazylibrarian.perm_friend:
                    perm_msg = 'Friend'
                elif perms == lazylibrarian.perm_guest:
                    perm_msg = 'Guest'
                else:
                    perm_msg = 'Custom %s' % perms

                msg = lazylibrarian.NEWUSER_MSG.replace('{username}', kwargs['username']).replace(
                    '{password}', kwargs['password']).replace(
                    '{permission}', perm_msg)

                result = notifiers.email_notifier.notify_message('LazyLibrarian New Account', msg, kwargs['email'])

                if result:
                    cmd = 'INSERT into users (UserID, UserName, Name, Password, Email, SendTo, Perms)'
                    cmd += ' VALUES (?, ?, ?, ?, ?, ?, ?)'
                    db.action(cmd, (pwd_generator(), kwargs['username'], kwargs['fullname'],
                                    md5_utf8(kwargs['password']), kwargs['email'], kwargs['sendto'], perms))
                    msg = "New user added: %s: %s" % (kwargs['username'], perm_msg)
                    msg += "<br>Email sent to %s" % kwargs['email']
                    cnt = db.match("select count(*) as counter from users")
                    if cnt['counter'] > 1:
                        lazylibrarian.SHOWLOGOUT = 1
                else:
                    msg = "New user NOT added"
                    msg += "<br>Failed to send email to %s" % kwargs['email']
                return msg

            else:
                if user != kwargs['username']:
                    # if username changed, must not have same username as another user
                    match = db.match('SELECT UserName from users where UserName=?', (kwargs['username'],))
                    if match:
                        return "Username already exists"

                changes = ''
                cmd = 'SELECT UserID,Name,Email,SendTo,Password,Perms,CalibreRead,CalibreToRead,BookType,Theme'
                cmd += ' from users where UserName=?'
                details = db.match(cmd, (user,))

                if details:
                    userid = details['UserID']
                    if kwargs['username'] and kwargs['username'] != user:
                        changes += ' username'
                        db.action('UPDATE users SET UserName=? WHERE UserID=?', (kwargs['username'], userid))

                    if kwargs['fullname'] and details['Name'] != kwargs['fullname']:
                        changes += ' name'
                        db.action('UPDATE users SET Name=? WHERE UserID=?', (kwargs['fullname'], userid))

                    if details['Email'] != kwargs['email']:
                        if kwargs['email']:
                            if not is_valid_email(kwargs['email']):
                                return "Invalid email given"
                        changes += ' email'
                        db.action('UPDATE users SET email=? WHERE UserID=?', (kwargs['email'], userid))

                    if details['SendTo'] != kwargs['sendto']:
                        if kwargs['sendto']:
                            if not is_valid_email(kwargs['sendto']):
                                return "Invalid sendto email given"
                        changes += ' sendto'
                        db.action('UPDATE users SET sendto=? WHERE UserID=?', (kwargs['sendto'], userid))

                    if kwargs['password']:
                        pwd = md5_utf8(kwargs['password'])
                        if pwd != details['Password']:
                            changes += ' password'
                            db.action('UPDATE users SET password=? WHERE UserID=?', (pwd, userid))

                    if details['Theme'] != kwargs['theme']:
                        valid = False
                        if kwargs['theme'] == 'legacy':
                            valid = True
                        elif kwargs['theme']:
                            parts = kwargs['theme'].split('_', 1)
                            if parts[0] == 'bookstrap':
                                if len(parts) == 2 and parts[1] in lazylibrarian.BOOKSTRAP_THEMELIST:
                                    valid = True
                        if valid:
                            changes += ' Theme'
                            db.action('UPDATE users SET Theme=? WHERE UserID=?', (kwargs['theme'], userid))
                        else:
                            logger.warning("Invalid user theme [%s]" % kwargs['theme'])

                    if details['CalibreRead'] != kwargs['calread']:
                        changes += ' CalibreRead'
                        db.action('UPDATE users SET CalibreRead=? WHERE UserID=?', (kwargs['calread'], userid))

                    if details['CalibreToRead'] != kwargs['caltoread']:
                        changes += ' CalibreToRead'
                        db.action('UPDATE users SET CalibreToRead=? WHERE UserID=?', (kwargs['caltoread'], userid))

                    if details['BookType'] != kwargs['booktype']:
                        changes += ' BookType'
                        db.action('UPDATE users SET BookType=? WHERE UserID=?', (kwargs['booktype'], userid))

                    if details['Perms'] != kwargs['perms']:
                        oldperm = check_int(details['Perms'], 0)
                        newperm = check_int(kwargs['perms'], 0)
                        if oldperm & 1 and not newperm & 1:
                            count = 0
                            perms = db.select('SELECT Perms from users')
                            for item in perms:
                                val = check_int(item['Perms'], 0)
                                if val & 1:
                                    count += 1
                            if count < 2:
                                return "Unable to remove last administrator"
                        if oldperm != newperm:
                            changes += ' Perms'
                            db.action('UPDATE users SET Perms=? WHERE UserID=?', (kwargs['perms'], userid))

                    if changes:
                        return 'Updated user details:%s' % changes
                return "No changes made"
        finally:
            db.close()

    @cherrypy.expose
    def password_reset(self, **kwargs):
        self.label_thread('PASSWORD_RESET')
        logger = logging.getLogger(__name__)
        res = {}
        remote_ip = cherrypy.request.remote.ip
        db = database.DBConnection()
        try:
            if 'username' in kwargs and kwargs['username']:
                logger.debug("Reset password request from %s, IP:%s" % (kwargs['username'], remote_ip))
                res = db.match('SELECT UserID,Email from users where username=?', (kwargs['username'],))  # type: dict
                if res:
                    if 'email' in kwargs and kwargs['email']:
                        if res['Email']:
                            if kwargs['email'] == res['Email']:
                                msg = ''
                            else:
                                msg = 'Email does not match our records'
                        else:
                            msg = 'No email address registered'
                    else:
                        msg = 'No email address supplied'
                else:
                    msg = "Unknown username"
            else:
                msg = "Who are you?"

            if res and not msg:
                new_pwd = pwd_generator()
                msg = "Your new password is %s" % new_pwd
                result = notifiers.email_notifier.notify_message('LazyLibrarian New Password', msg, res['Email'])
                if result:
                    pwd = md5_utf8(new_pwd)
                    db.action("UPDATE users SET Password=? WHERE UserID=?", (pwd, res['UserID']))
                    return "Password reset, check your email"
                else:
                    msg = "Failed to send email to [%s]" % res['Email']
        finally:
            db.close()
        msg = "Password not reset: %s" % msg
        logger.error("%s IP:%s" % (msg, remote_ip))
        return msg

    @cherrypy.expose
    def generatepwd(self):
        return pwd_generator()

    # SERIES ############################################################
    @cherrypy.expose
    def remove_series(self, seriesid):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            seriesdata = db.match("SELECT * from series WHERE seriesid=?", (seriesid,))
            if seriesdata:
                db.action("DELETE from series WHERE SeriesID=?", (seriesid,))
            else:
                logger.info('Missing series %s' % seriesid)
        finally:
            db.close()
        raise cherrypy.HTTPRedirect("series")

    @cherrypy.expose
    def edit_series(self, seriesid):
        logger = logging.getLogger(__name__)
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        self.label_thread('EDIT_SERIES')
        db = database.DBConnection()
        try:
            seriesdata = db.match("SELECT * from series WHERE seriesid=?", (seriesid,))
            if seriesdata:
                # use bookid to update seriesdata['Reason']
                seriesdata = dict(seriesdata)
                cmd = "select BookName,BookID from books where (bookid=?) or (gr_id=?) or (lt_workid=?)"
                reason = seriesdata['Reason']
                bookinfo = db.match(cmd, (reason, reason, reason))
                if bookinfo:
                    seriesdata['Reason'] = "%s: %s" % (bookinfo['BookID'], bookinfo['BookName'])

                cmd = "SELECT SeriesNum,BookName from member,books WHERE books.BookID=member.BookID and seriesid=?"
                memberdata = db.select(cmd, (seriesid,))
                # sort properly by seriesnum as sqlite doesn't (yet) have natural sort
                members = []
                for item in memberdata:
                    members.append(dict(item))
                self.natural_sort(members, key=lambda y: y['SeriesNum'] if y['SeriesNum'] is not None else '')
                return serve_template(templatename="editseries.html", title="Edit Series", config=seriesdata,
                                      members=members)
            else:
                logger.info('Missing series %s' % seriesid)
                raise cherrypy.HTTPError(404, "Series %s not found" % seriesid)
        finally:
            db.close()

    @cherrypy.expose
    def series_update(self, seriesid='', **kwargs):
        logger = logging.getLogger(__name__)
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        db = database.DBConnection()
        try:
            if seriesid:
                old_data = db.match("SELECT * from series where seriesid=?", (seriesid,))
                if old_data:
                    updated = False
                    seriesname = old_data['SeriesName']
                    seriesid = old_data['SeriesID']
                    new_id = kwargs.get('new_id')
                    if new_id and new_id != seriesid:
                        seriesid = new_id
                        updated = True
                    new_name = kwargs.get('new_name')
                    if new_name and new_name != seriesname:
                        seriesname = new_name
                        updated = True
                    if updated:
                        db.action('PRAGMA foreign_keys = OFF')
                        db.action("UPDATE series SET SeriesID=?, SeriesName=? WHERE SeriesID=?",
                                  (seriesid, seriesname, old_data['SeriesID']))
                        if seriesid != old_data['SeriesID']:
                            for table in ['member', 'seriesauthors']:
                                cmd = "UPDATE " + table + " SET SeriesID=? WHERE SeriesID=?"
                                db.action(cmd, (seriesid, old_data['SeriesID']))
                        db.action('PRAGMA foreign_keys = ON')
                        logger.debug("Updated series info for %s:%s" % (seriesid, seriesname))
                else:
                    logger.debug("No match updating series %s" % seriesid)
                    raise cherrypy.HTTPError(404, "Series %s not found" % seriesid)
        finally:
            db.close()
        raise cherrypy.HTTPRedirect("series")

    @cherrypy.expose
    def refresh_series(self, seriesid):
        threadname = 'SERIESMEMBERS_%s' % seriesid
        if threadname not in [n.name for n in [t for t in threading.enumerate()]]:
            threading.Thread(target=add_series_members, name=threadname, args=[seriesid, True]).start()
        raise cherrypy.HTTPRedirect("series_members?seriesid=%s&ignored=False" % seriesid)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_series(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        rows = []
        filtered = []
        rowlist = []
        userid = None
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)

        # noinspection PyBroadException
        try:
            # kwargs is used by datatables to pass params
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)

            which_status = 'All'
            if kwargs['whichStatus']:
                which_status = kwargs['whichStatus']

            author_id = ''
            if kwargs['AuthorID']:
                author_id = kwargs['AuthorID']

            if not author_id or author_id == 'None':
                author_id = ''

            # We pass series.SeriesID twice for datatables as the render function modifies it,
            # and we need it in two columns. There is probably a better way...
            cmd = 'SELECT series.SeriesID,AuthorName,SeriesName,series.Status,seriesauthors.AuthorID,series.SeriesID,'
            cmd += 'Have,Total,series.Reason from series,authors,seriesauthors,member'
            cmd += ' where authors.AuthorID=seriesauthors.AuthorID and series.SeriesID=seriesauthors.SeriesID'
            cmd += ' and member.seriesid=series.seriesid'  # and seriesnum=1'
            args = []
            if which_status == 'Empty':
                cmd += ' and Have = 0'
            elif which_status == 'Partial':
                cmd += ' and Have > 0'
            elif which_status == 'Complete':
                cmd += ' and Have > 0 and Have = Total'
            elif which_status not in ['All', 'None']:
                cmd += ' and series.Status=?'
                args.append(which_status)
            if author_id:
                cmd += ' and seriesauthors.AuthorID=?'
                args.append(author_id)

            myseries = []
            db = database.DBConnection()
            try:
                if userid and userprefs & lazylibrarian.pref_myseries:
                    res = db.select('SELECT WantID from subscribers WHERE Type="series" and UserID=?', (userid,))
                    loggerserverside.debug("User subscribes to %s series" % len(res))
                    for series in res:
                        myseries.append(series['WantID'])
                    cmd += ' and series.seriesID in (' + ', '.join('"{0}"'.format(w) for w in myseries) + ')'

                cmd += ' GROUP BY series.seriesID'
                cmd += ' order by AuthorName,SeriesName'

                loggerserverside.debug("get_series %s: %s" % (cmd, str(args)))

                if args:
                    rowlist = db.select(cmd, tuple(args))
                else:
                    rowlist = db.select(cmd)
            finally:
                db.close()

            # turn the sqlite rowlist into a list of lists
            if len(rowlist):
                for row in rowlist:  # iterate through the sqlite3.Row objects
                    entry = list(row)  # turn sqlite objects into lists
                    rows.append(entry)  # add the rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in rows if ssearch.lower() in str(x).lower()]
                else:
                    filtered = rows

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                for row in filtered:
                    if CONFIG.get_bool('SORT_SURNAME'):
                        row[1] = surname_first(row[1], postfixes=CONFIG.get_list('NAME_POSTFIX'))
                    have = check_int(row[6], 0)
                    total = check_int(row[7], 0)
                    if total:
                        percent = int((have * 100.0) / total)
                    else:
                        percent = 0

                    if percent > 100:
                        percent = 100

                    row.append(percent)

                if sortcolumn == 3:  # percent
                    sortcolumn = 9
                if sortcolumn == 4:  # status
                    sortcolumn = 3

                if sortcolumn == 9:  # sort on percent,-total
                    if ssort_dir_0 == "desc":
                        filtered.sort(key=lambda y: (-int(y[9]), int(y[7])))
                    else:
                        filtered.sort(key=lambda y: (int(y[9]), -int(y[7])))
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

            loggerserverside.debug("get_series returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_series filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_series: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def series(self, authorid=None, which_status=None):
        title = "Series"
        if authorid:
            db = database.DBConnection()
            try:
                match = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            finally:
                db.close()
            if match:
                title = "%s Series" % match['AuthorName']
            if '&' in title and '&amp;' not in title:
                title = title.replace('&', '&amp;')

        return serve_template(templatename="series.html", title=title, authorid=authorid, series=[],
                              whichStatus=which_status)

    @cherrypy.expose
    def series_members(self, seriesid, ignored=False):
        db = database.DBConnection()
        try:
            cmd = 'SELECT SeriesName,series.SeriesID,AuthorName,seriesauthors.AuthorID'
            cmd += ' from series,authors,seriesauthors'
            cmd += ' where authors.AuthorID=seriesauthors.AuthorID and series.SeriesID=seriesauthors.SeriesID'
            cmd += ' and series.SeriesID=?'
            series = db.match(cmd, (seriesid,))
            cmd = 'SELECT member.BookID,BookName,SeriesNum,BookImg,books.Status,AuthorName,authors.AuthorID,'
            cmd += 'BookLink,WorkPage,AudioStatus,BookSub'
            cmd += ' from member,series,books,authors'
            cmd += ' where series.SeriesID=member.SeriesID and books.BookID=member.BookID'
            cmd += ' and books.AuthorID=authors.AuthorID and '
            if not ignored or ignored == 'False':
                cmd += '(books.Status != "Ignored" or AudioStatus != "Ignored")'
            else:
                cmd += '(books.Status == "Ignored" and AudioStatus == "Ignored")'
            cmd += ' and series.SeriesID=? order by SeriesName'
            members = db.select(cmd, (seriesid,))
            # is it a multi-author series?
            multi = "False"
            authorid = ''
            for item in members:
                if not authorid:
                    authorid = item['AuthorID']
                else:
                    if not authorid == item['AuthorID']:
                        multi = "True"
                        break

            email = ''
            to_read = set()
            have_read = set()
            reading = set()
            abandoned = set()
            cookie = cherrypy.request.cookie
            if cookie and 'll_uid' in list(cookie.keys()):
                cmd = 'SELECT UserName,ToRead,HaveRead,Reading,Abandoned,Perms,SendTo from users where UserID=?'
                res = db.match(cmd, (cookie['ll_uid'].value,))
                if res:
                    to_read = set(get_list(res['ToRead']))
                    have_read = set(get_list(res['HaveRead']))
                    reading = set(get_list(res['Reading']))
                    abandoned = set(get_list(res['Abandoned']))
                    email = res['SendTo']
        finally:
            db.close()
        # turn the sqlite rowlist into a list of lists
        rows = []

        if len(members):
            # the masterlist to be filled with the row data
            for row in members:  # iterate through the sqlite3.Row objects
                entry = list(row)
                if entry[0] in to_read:
                    flag = '&nbsp;<i class="far fa-bookmark"></i>'
                elif entry[0] in have_read:
                    flag = '&nbsp;<i class="fas fa-bookmark"></i>'
                elif entry[0] in reading:
                    flag = '&nbsp;<i class="fas fa-play-circle"></i>'
                elif entry[0] in abandoned:
                    flag = '&nbsp;<i class="fas fa-ban"></i>'
                else:
                    flag = ''
                if entry[10]:  # is there a subtitle
                    bk_name = '%s<br><small><i>%s</i></small>' % (entry[1], entry[10])
                else:
                    bk_name = entry[1]
                newrow = {'BookID': entry[0], 'BookName': bk_name, 'SeriesNum': entry[2], 'BookImg': entry[3],
                          'Status': entry[4], 'AuthorName': entry[5], 'AuthorID': entry[6],
                          'BookLink': entry[7] if entry[7] else '', 'WorkPage': entry[8] if entry[8] else '',
                          'AudioStatus': entry[9], 'Flag': flag}

                rows.append(newrow)  # add the new dict to the masterlist

        return serve_template(templatename="members.html", title=series['SeriesName'],
                              members=rows, series=series, multi=multi, ignored=ignored, email=email)

    @cherrypy.expose
    def mark_series(self, action=None, **args):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        args.pop('book_table_length', None)
        try:
            if action:
                for seriesid in args:
                    if action in ["Wanted", "Active", "Skipped", "Ignored", "Paused"]:
                        match = db.match('SELECT SeriesName from series WHERE SeriesID=?', (seriesid,))
                        if match:
                            db.upsert("series", {'Status': action}, {'SeriesID': seriesid})
                            logger.debug('Status set to "%s" for "%s"' % (action, match['SeriesName']))
                            if action in ['Wanted', 'Active']:
                                threadname = 'SERIESMEMBERS_%s' % seriesid
                                if threadname not in [n.name for n in [t for t in threading.enumerate()]]:
                                    threading.Thread(target=add_series_members, name=threadname,
                                                     args=[seriesid]).start()
                                ensure_running('series_update')
                            else:
                                # stop monitoring
                                db.action("UPDATE series SET Updated=0 WHERE SeriesID=?", (seriesid,))
                    elif action in ["Unread", "Read", "ToRead", "Reading", "Abandoned"]:
                        cookie = cherrypy.request.cookie
                        if cookie and 'll_uid' in list(cookie.keys()):
                            res = db.match('SELECT ToRead,HaveRead,Reading,Abandoned from users where UserID=?',
                                           (cookie['ll_uid'].value,))
                            if res:
                                to_read = set(get_list(res['ToRead']))
                                have_read = set(get_list(res['HaveRead']))
                                reading = set(get_list(res['Reading']))
                                abandoned = set(get_list(res['Abandoned']))
                                members = db.select('SELECT bookid from member where seriesid=?', (seriesid,))
                                if members:
                                    for item in members:
                                        bookid = item['bookid']
                                        if action == "Unread":
                                            to_read.discard(bookid)
                                            have_read.discard(bookid)
                                            reading.discard(bookid)
                                            abandoned.discard(bookid)
                                            logger.debug('Status set to "unread" for "%s"' % bookid)
                                        elif action == "Read":
                                            to_read.discard(bookid)
                                            reading.discard(bookid)
                                            abandoned.discard(bookid)
                                            have_read.add(bookid)
                                            logger.debug('Status set to "read" for "%s"' % bookid)
                                        elif action == "ToRead":
                                            reading.discard(bookid)
                                            abandoned.discard(bookid)
                                            have_read.discard(bookid)
                                            to_read.add(bookid)
                                            logger.debug('Status set to "to read" for "%s"' % bookid)
                                        elif action == "Reading":
                                            reading.add(bookid)
                                            abandoned.discard(bookid)
                                            have_read.discard(bookid)
                                            to_read.discard(bookid)
                                            logger.debug('Status set to "reading" for "%s"' % bookid)
                                        elif action == "Abandoned":
                                            reading.discard(bookid)
                                            abandoned.add(bookid)
                                            have_read.discard(bookid)
                                            to_read.discard(bookid)
                                            logger.debug('Status set to "abandoned" for "%s"' % bookid)
                                    cmd = 'UPDATE users SET ToRead=?,HaveRead=?,Reading=?,Abandoned=? WHERE UserID=?'
                                    db.action(cmd, (', '.join('"{0}"'.format(w) for w in to_read),
                                                    ', '.join('"{0}"'.format(w) for w in have_read),
                                                    ', '.join('"{0}"'.format(w) for w in reading),
                                                    ', '.join('"{0}"'.format(w) for w in abandoned),
                                                    cookie['ll_uid'].value))
                    elif action == 'Subscribe':
                        cookie = cherrypy.request.cookie
                        if cookie and 'll_uid' in list(cookie.keys()):
                            userid = cookie['ll_uid'].value
                            res = db.match("SELECT * from subscribers WHERE UserID=? and Type=? and WantID=?",
                                           (userid, 'series', seriesid))
                            if res:
                                logger.debug("User %s is already subscribed to %s" % (userid, seriesid))
                            else:
                                db.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                                          (userid, 'series', seriesid))
                                logger.debug("Subscribe %s to series %s" % (userid, seriesid))
                    elif action == 'Unsubscribe':
                        cookie = cherrypy.request.cookie
                        if cookie and 'll_uid' in list(cookie.keys()):
                            userid = cookie['ll_uid'].value
                            db.action('DELETE from subscribers WHERE UserID=? and Type=? and WantID=?',
                                      (userid, 'series', seriesid))
                            logger.debug("Unsubscribe %s to series %s" % (userid, seriesid))

                if "redirect" in args:
                    if not args['redirect'] == 'None':
                        raise cherrypy.HTTPRedirect("series?authorid=%s" % args['redirect'])
                raise cherrypy.HTTPRedirect("series")
        finally:
            db.close()

    # CONFIG ############################################################

    @cherrypy.expose
    def save_filters(self):
        self.label_thread('WEBSERVER')
        savedir = DIRS.DATADIR
        mags = dump_table('magazines', savedir)
        msg = "%d %s exported" % (mags, plural(mags, "magazine"))
        return msg

    @cherrypy.expose
    def save_users(self):
        self.label_thread('WEBSERVER')
        savedir = DIRS.DATADIR
        users = dump_table('users', savedir)
        msg = "%d %s exported" % (users, plural(users, "user"))
        return msg

    @cherrypy.expose
    def load_filters(self):
        self.label_thread('WEBSERVER')
        savedir = DIRS.DATADIR
        mags = restore_table('magazines', savedir)
        msg = "%d %s imported" % (mags, plural(mags, "magazine"))
        return msg

    @cherrypy.expose
    def load_users(self):
        self.label_thread('WEBSERVER')
        savedir = DIRS.DATADIR
        users = restore_table('users', savedir)
        msg = "%d %s imported" % (users, plural(users, "user"))
        return msg

    @cherrypy.expose
    def config(self):
        self.label_thread('CONFIG')
        http_look_dir = os.path.join(DIRS.PROG_DIR, 'data' + os.path.sep + 'interfaces')
        http_look_list = [name for name in listdir(http_look_dir)
                          if path_isdir(os.path.join(http_look_dir, name))]
        status_list = ['Skipped', 'Wanted', 'Have', 'Ignored']
        apprise_list = lazylibrarian.notifiers.apprise_notify.AppriseNotifier.notify_types()

        mags_list = []
        comics_list = []
        db = database.DBConnection()
        try:
            comics = db.select(
                'SELECT * from comics ORDER by Title COLLATE NOCASE')
            magazines = db.select(
                'SELECT * from magazines ORDER by Title COLLATE NOCASE')
        finally:
            db.close()
        if comics:
            for mag in comics:
                title = mag['Title']
                genre = mag['Genre']
                if not genre:
                    genre = ""
                comics_list.append({
                    'Title': title,
                    'Genre': genre
                })
        if magazines:
            for mag in magazines:
                title = mag['Title']
                regex = mag['Regex']
                genre = mag['Genre']
                if not genre:
                    genre = ""
                if not regex:
                    regex = ""
                reject = mag['Reject']
                if not reject:
                    reject = ""
                datetype = mag['DateType']
                if not datetype:
                    datetype = ""
                coverpage = check_int(mag['CoverPage'], 1)
                mags_list.append({
                    'Title': title,
                    'Reject': reject,
                    'Regex': regex,
                    'Genre': genre,
                    'DateType': datetype,
                    'CoverPage': coverpage
                })

        BLOCKHANDLER.check_day()

        # Don't pass the whole config, no need to pass the
        # lazylibrarian.globals
        namevars = name_vars('test')
        testvars = {}
        for item in namevars:
            testvars[item] = namevars[item].replace(' ', '&nbsp;')
        config = {
            "http_look_list": http_look_list,
            "apprise_list": apprise_list,
            "status_list": status_list,
            "magazines_list": mags_list,
            "comics_list": comics_list,
            "namevars": testvars,
            "updated": time.ctime(CONFIG.get_int('GIT_UPDATED'))
        }
        for item in CONFIG.config.values():
            if isinstance(item, ConfigBool):
                item.reset_read_count()  # Reset read counts as we use this to determine which settings have changed
        return serve_template(templatename="config.html", title="Settings", config=config)

    @cherrypy.expose
    def config_update(self, **kwargs):
        """ Update config based on settings in the UI """
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            adminmsg = ''
            if 'user_accounts' in kwargs:
                logger.error('CFG2: Need to handle user account changes')
                if kwargs['user_accounts']:
                    email = ''
                    if 'admin_email' in kwargs and kwargs['admin_email']:
                        email = kwargs['admin_email']
                    else:
                        adminmsg += 'Please set a contact email so users can make requests<br>'

                    if email and not is_valid_email(email):
                        adminmsg += 'Contact email looks invalid, please check<br>'

                    if CONFIG['HTTP_USER'] != '':
                        adminmsg += 'Please remove WEBSERVER USER as user accounts are active<br>'

                    admin = db.match('SELECT password from users where name="admin"')
                    if admin:
                        if admin['password'] == md5_utf8('admin'):
                            adminmsg += "The default admin user is \"admin\" and password is \"admin\"<br>"
                            adminmsg += "This is insecure, please change it on Config -> User Admin<br>"

            # store any genre changes
            genre_changes = ''
            genrelimit = check_int(kwargs.get('genrelimit', 0), 0)
            if lazylibrarian.GRGENRES.get('genreLimit', 10) != genrelimit:
                lazylibrarian.GRGENRES['genreLimit'] = genrelimit
                genre_changes += 'limit '
            genreusers = check_int(kwargs.get('genreusers', 0), 0)
            if lazylibrarian.GRGENRES.get('genreUsers', 10) != genreusers:
                lazylibrarian.GRGENRES['genreUsers'] = genreusers
                genre_changes += 'users '
            newexcludes = sorted(get_list(kwargs.get('genreexclude', ''), ','))
            if sorted(lazylibrarian.GRGENRES.get('genreExclude', [])) != newexcludes:
                lazylibrarian.GRGENRES['genreExclude'] = newexcludes
                genre_changes += 'excludes '
            newexcludes = sorted(get_list(kwargs.get('genreexcludeparts', ''), ','))
            if sorted(lazylibrarian.GRGENRES.get('genreExcludeParts', [])) != newexcludes:
                lazylibrarian.GRGENRES['genreExcludeParts'] = newexcludes
                genre_changes += 'parts '
            # now the replacements
            genredict = {}
            for item in kwargs:
                if item.startswith('genrereplace['):
                    mykey = make_unicode(item.split('[')[1].split(']')[0])
                    myval = make_unicode(kwargs.get(item, ''))
                    if myval:
                        genredict[mykey] = myval

            # new genre to add
            if 'genrenew' in kwargs and 'genreold' in kwargs:
                if kwargs['genrenew'] and kwargs['genreold']:
                    genredict[make_unicode(kwargs['genreold'])] = make_unicode(kwargs['genrenew'])
                    genre_changes += 'new-entry '

            dicts_same = False
            if len(lazylibrarian.GRGENRES.get('genreReplace', {})) != len(genredict):
                genre_changes += 'dict-length '
            else:
                shared_items = {k: lazylibrarian.GRGENRES['genreReplace'][k] for k in lazylibrarian.GRGENRES['genreReplace']
                                if k in genredict and lazylibrarian.GRGENRES['genreReplace'][k] == genredict[k]}
                if len(shared_items) != len(genredict):
                    genre_changes += 'shared-values '
                else:
                    dicts_same = True

            if not dicts_same:
                lazylibrarian.GRGENRES['genreReplace'] = genredict

            if genre_changes:
                logger.debug("Genre changes: %s" % genre_changes)
                logger.debug("Writing out new genres.json")
                newdict = {
                    'genreLimit': lazylibrarian.GRGENRES['genreLimit'],
                    'genreUsers': lazylibrarian.GRGENRES['genreUsers'],
                    'genreExclude': lazylibrarian.GRGENRES['genreExclude'],
                    'genreExcludeParts': lazylibrarian.GRGENRES['genreExcludeParts'],
                    'genreReplace': lazylibrarian.GRGENRES['genreReplace'],
                }
                with open(syspath(os.path.join(DIRS.DATADIR, 'genres.json')), 'w') as f:
                    json.dump(newdict, f, indent=4)
                logger.debug("Applying genre changes")
                check_db()

            # now the config file entries
            for key, item in CONFIG.config.items():
                if key.lower() in kwargs:
                    value = kwargs[key.lower()]
                    CONFIG.set_from_ui(key, value)
                else:
                    if isinstance(item, ConfigBool) and item.get_read_count() > 0:
                        item.set_from_ui(False)  # Set other items to False that we've seen (i.e. are shown)
            CONFIG.ensure_valid_homepage()

            magazines = db.select('SELECT * from magazines')
            if magazines:
                count = 0
                for mag in magazines:
                    title = mag['Title']
                    reject = mag['Reject']
                    regex = mag['Regex']
                    genres = mag['Genre']
                    datetype = mag['DateType']
                    coverpage = check_int(mag['CoverPage'], 1)
                    # seems kwargs parameters from cherrypy are sometimes passed as latin-1,
                    # can't see how to configure it, so we need to correct it on accented magazine names
                    # eg "Elle Quebec" where we might have e-acute stored as unicode
                    # e-acute is \xe9 in latin-1  but  \xc3\xa9 in utf-8
                    # otherwise the comparison fails, but sometimes accented characters won't
                    # fit latin-1 but fit utf-8 how can we tell ???
                    if not isinstance(title, str):
                        try:
                            title = title.encode('latin-1')
                        except UnicodeEncodeError:
                            try:
                                title = title.encode('utf-8')
                            except UnicodeEncodeError:
                                logger.warning('Unable to convert title [%s]' % repr(title))
                                title = unaccented(title, only_ascii=False)

                    new_value_dict = {}
                    new_reject = kwargs.get('reject_list[%s]' % title, None)
                    if not new_reject == reject:
                        new_value_dict['Reject'] = new_reject
                    new_regex = kwargs.get('regex[%s]' % title, None)
                    if not new_regex == regex:
                        new_value_dict['Regex'] = new_regex
                    new_genres = kwargs.get('genre_list[%s]' % title, None)
                    if not new_genres == genres:
                        new_value_dict['Genre'] = new_genres
                    new_datetype = kwargs.get('datetype[%s]' % title, None)
                    if not new_datetype == datetype:
                        new_value_dict['DateType'] = new_datetype
                    new_coverpage = check_int(kwargs.get('coverpage[%s]' % title, None), 1)
                    if not new_coverpage == coverpage:
                        new_value_dict['CoverPage'] = new_coverpage
                    if new_value_dict:
                        count += 1
                        db.upsert("magazines", new_value_dict, {'Title': title})
                if count:
                    logger.info("Magazine %s filters updated" % count)
        finally:
            db.close()

        CONFIG.update_providers_from_ui(kwargs)

        # Convert legacy log settings
        logtype = kwargs.get('log_type', '')
        if logtype == 'Quiet':
            newloglevel = logging.CRITICAL
        elif logtype == 'Normal':
            newloglevel = logging.INFO
        elif logtype == 'Debug':
            newloglevel = logging.DEBUG
        else:  # legacy interface, no log_type
            newloglevel = int(kwargs.get('loglevel', logging.INFO))

        # Enable/disable special loggers based on UI
        specials = []
        for logger in LOGCONFIG.get_special_logger_list():
            shortname = LOGCONFIG.get_short_special_logger_name(logger.name)
            uiname = f'log_{shortname}'
            if uiname in kwargs:
                specials.append(shortname)
        specialcsv = ','.join(specials)
        LOGCONFIG.enable_only_these_special_debuglogs(specialcsv)
        CONFIG.set_csv('LOGSPECIALDEBUG', specialcsv)

        # Store this in CONFIG so it's persisted. OnChange triggers event to activate.
        CONFIG.set_int('LOGLEVEL', newloglevel)
        CONFIG.save_config_and_backup_old(restart_jobs=True)
        if not lazylibrarian.STOPTHREADS:
            check_running_jobs()

        if adminmsg:
            return serve_template(templatename="response.html", prefix="",
                                  title="User Accounts", message=adminmsg, timer=0)

        raise cherrypy.HTTPRedirect("config")

    # SEARCH ############################################################

    @cherrypy.expose
    def search(self, name):
        self.label_thread('SEARCH')
        if not name:
            raise cherrypy.HTTPRedirect("home")

        if name.lower().startswith('authorid:'):
            self.add_author_id(name[9:])
        elif name.lower().startswith('bookid:'):
            self.add_book(name[7:])
        else:
            db = database.DBConnection()
            try:
                loadingauthorids = db.select("SELECT AuthorID from authors where status != 'Loading'")
                authorids = db.select("SELECT AuthorID from authors where status = 'Loading'")
                booksearch = db.select("SELECT Status,AudioStatus,BookID from books")
            finally:
                db.close()
            authorlist = []
            for item in loadingauthorids:
                authorlist.append(item['AuthorID'])
            loadlist = []
            for item in authorids:
                loadlist.append(item['AuthorID'])
            booklist = []
            for item in booksearch:
                booklist.append(item['BookID'])

            searchresults = search_for(name)
            return serve_template(templatename="searchresults.html", title='Search Results: "' + name + '"',
                                  searchresults=searchresults, authorlist=authorlist, loadlist=loadlist,
                                  booklist=booklist, booksearch=booksearch)

    # AUTHOR ############################################################

    @cherrypy.expose
    def mark_authors(self, action=None, redirect=None, **args):
        logger = logging.getLogger(__name__)
        for arg in ['author_table_length', 'ignored']:
            args.pop(arg, None)
        if not redirect:
            redirect = "authors"
        if action:
            db = database.DBConnection()
            try:
                for authorid in args:
                    check = db.match("SELECT AuthorName from authors WHERE AuthorID=?", (authorid,))
                    if not check:
                        logger.warning('Unable to set Status to "%s" for "%s"' % (action, authorid))
                    elif action in ["Active", "Wanted", "Paused", "Ignored"]:
                        db.upsert("authors", {'Status': action}, {'AuthorID': authorid})
                        logger.info('Status set to "%s" for "%s"' % (action, check['AuthorName']))
                    elif action == "Delete":
                        logger.info("Removing author and books: %s" % check['AuthorName'])
                        books = db.select("SELECT BookFile from books WHERE AuthorID=? AND BookFile is not null",
                                          (authorid,))
                        for book in books:
                            if path_exists(book['BookFile']):
                                try:
                                    rmtree(os.path.dirname(book['BookFile']), ignore_errors=True)
                                except Exception as e:
                                    logger.warning('rmtree failed on %s, %s %s' %
                                                   (book['BookFile'], type(e).__name__, str(e)))

                        db.action('DELETE from authors WHERE AuthorID=?', (authorid,))
                    elif action == "Remove":
                        logger.info("Removing author: %s" % check['AuthorName'])
                        db.action('DELETE from authors WHERE AuthorID=?', (authorid,))
                    elif action == 'Subscribe':
                        cookie = cherrypy.request.cookie
                        if cookie and 'll_uid' in list(cookie.keys()):
                            userid = cookie['ll_uid'].value
                            res = db.match("SELECT * from subscribers WHERE UserID=? and Type=? and WantID=?",
                                           (userid, 'author', authorid))
                            if res:
                                logger.debug("User %s is already subscribed to %s" % (userid, authorid))
                            else:
                                db.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                                          (userid, 'author', authorid))
                                logger.debug("Subscribe %s to author %s" % (userid, authorid))
                    elif action == 'Unsubscribe':
                        cookie = cherrypy.request.cookie
                        if cookie and 'll_uid' in list(cookie.keys()):
                            userid = cookie['ll_uid'].value
                            db.action('DELETE from subscribers WHERE UserID=? and Type=? and WantID=?',
                                      (userid, 'author', authorid))
                            logger.debug("Unsubscribe %s author %s" % (userid, authorid))
            finally:
                db.close()

        raise cherrypy.HTTPRedirect(redirect)

    # noinspection PyGlobalUndefined
    @cherrypy.expose
    def author_page(self, authorid, book_lang=None, library='eBook', ignored=False, book_filter=''):
        global lastauthor
        db = database.DBConnection()
        try:
            user = 0
            email = ''
            cookie = cherrypy.request.cookie
            if cookie and 'll_uid' in list(cookie.keys()):
                user = cookie['ll_uid'].value
                res = db.match('SELECT SendTo from users where UserID=?', (user,))
                if res and res['SendTo']:
                    email = res['SendTo']

            if ignored:
                languages = db.select(
                    "SELECT DISTINCT BookLang from books WHERE AuthorID=? AND Status ='Ignored'", (authorid,))
            else:
                languages = db.select(
                    "SELECT DISTINCT BookLang from books WHERE AuthorID=? AND Status !='Ignored'", (authorid,))

            author = dict(db.match("SELECT * from authors WHERE AuthorID=?", (authorid,)))
        finally:
            db.close()

        types = []
        if CONFIG.get_bool('EBOOK_TAB'):
            types.append('eBook')
        if CONFIG.get_bool('AUDIO_TAB'):
            types.append('AudioBook')
        if types and library not in types:
            library = types[0]
        if not types:
            library = None
        if not author:
            raise cherrypy.HTTPRedirect("authors")

        # if we've changed author, reset to first page of new authors books
        if authorid == lastauthor:
            firstpage = 'false'
        else:
            lastauthor = authorid
            firstpage = 'true'

        authorname = author['AuthorName']
        if not authorname:  # still loading?
            raise cherrypy.HTTPRedirect("authors")

        author['AuthorBorn'] = date_format(author['AuthorBorn'], CONFIG['AUTHOR_DATE_FORMAT'])
        author['AuthorDeath'] = date_format(author['AuthorDeath'], CONFIG['AUTHOR_DATE_FORMAT'])

        return serve_template(
            templatename="author.html", title=quote_plus(make_utf8bytes(authorname)[0]), author=author,
            languages=languages, booklang=book_lang, types=types, library=library, ignored=ignored,
            showseries=CONFIG.get_int('SERIES_TAB'), firstpage=firstpage, user=user, email=email,
            book_filter=book_filter)

    @cherrypy.expose
    def set_author(self, authorid, status):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authorsearch = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            if authorsearch:
                author_name = authorsearch['AuthorName']
                logger.info("%s author: %s" % (status, author_name))

                control_value_dict = {'AuthorID': authorid}
                new_value_dict = {'Status': status}
                db.upsert("authors", new_value_dict, control_value_dict)
                logger.debug(
                    'AuthorID [%s]-[%s] %s - redirecting to Author home page' % (authorid, author_name, status))
                raise cherrypy.HTTPRedirect("author_page?authorid=%s" % authorid)
            else:
                logger.debug('pause_author Invalid authorid [%s]' % authorid)
                raise cherrypy.HTTPError(404, "AuthorID %s not found" % authorid)
        finally:
            db.close()

    @cherrypy.expose
    def pause_author(self, authorid):
        self.set_author(authorid, 'Paused')

    @cherrypy.expose
    def want_author(self, authorid):
        self.set_author(authorid, 'Wanted')

    @cherrypy.expose
    def resume_author(self, authorid):
        self.set_author(authorid, 'Active')

    @cherrypy.expose
    def ignore_author(self, authorid):
        self.set_author(authorid, 'Ignored')

    @cherrypy.expose
    def remove_author(self, authorid):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authorsearch = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            if authorsearch:  # to stop error if try to remove an author while they are still loading
                author_name = authorsearch['AuthorName']
                logger.info("Removing all references to author: %s" % author_name)
                db.action('DELETE from authors WHERE AuthorID=?', (authorid,))
                # if the author was the only remaining contributor to a series, remove the series
                orphans = db.select('select seriesid from series except select seriesid from seriesauthors')
                for orphan in orphans:
                    db.action('DELETE from series where seriesid=?', (orphan[0],))
        finally:
            db.close()
        raise cherrypy.HTTPRedirect("authors")

    @cherrypy.expose
    def refresh_author(self, authorid):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authorsearch = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            if authorsearch:  # to stop error if try to refresh an author while they are still loading
                if authorid.startswith('OL'):
                    ol = OpenLibrary(authorid)
                    author = ol.get_author_info(authorid=authorid, refresh=True)
                else:
                    gr = GoodReads(authorid)
                    author = gr.get_author_info(authorid=authorid)
                if author and authorid != author['authorid']:
                    logger.debug("Authorid changed from %s to %s" % (authorid, author['authorid']))
                    db.action("PRAGMA foreign_keys = OFF")
                    db.action('UPDATE books SET AuthorID=? WHERE AuthorID=?',
                              (author['authorid'], authorid))
                    db.action('UPDATE seriesauthors SET AuthorID=? WHERE AuthorID=?',
                              (author['authorid'], authorid), suppress='UNIQUE')
                    if author['authorid'].startswith('OL'):
                        db.action('UPDATE authors SET AuthorID=?,ol_id=? WHERE AuthorID=?',
                                  (author['authorid'], author['authorid'], authorid), suppress='UNIQUE')
                    else:
                        db.action('UPDATE authors SET AuthorID=?,gr_id=? WHERE AuthorID=?',
                                  (author['authorid'], author['authorid'], authorid), suppress='UNIQUE')
                    db.action("PRAGMA foreign_keys = ON")
                    authorid = author['authorid']
                threading.Thread(target=add_author_to_db, name='REFRESHAUTHOR_%s' % authorid,
                                 args=[None, True, authorid, True, "WebServer refresh_author %s" % authorid]).start()
                raise cherrypy.HTTPRedirect("author_page?authorid=%s" % authorid)
            else:
                logger.debug('refresh_author Invalid authorid [%s]' % authorid)
                raise cherrypy.HTTPError(404, "AuthorID %s not found" % authorid)
        finally:
            db.close()

    @cherrypy.expose
    def follow_author(self, authorid):
        # empty GRfollow is not-yet-used, zero means manually unfollowed so sync leaves it alone
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authorsearch = db.match('SELECT AuthorName, GRfollow from authors WHERE AuthorID=?', (authorid,))
            if authorsearch:
                if authorsearch['GRfollow'] and authorsearch['GRfollow'] != '0':
                    logger.warning("Already Following %s" % authorsearch['AuthorName'])
                else:
                    msg = grsync.grfollow(authorid, True)
                    if msg.startswith('Unable'):
                        logger.warning(msg)
                    else:
                        logger.info(msg)
                        followid = msg.split("followid=")[1]
                        db.action("UPDATE authors SET GRfollow=? WHERE AuthorID=?", (followid, authorid))
            else:
                msg = "Invalid authorid to follow (%s)" % authorid
                logger.error(msg)
                raise cherrypy.HTTPError(404, msg)
        finally:
            db.close()

        raise cherrypy.HTTPRedirect("author_page?authorid=%s" % authorid)

    @cherrypy.expose
    def unfollow_author(self, authorid):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authorsearch = db.match('SELECT AuthorName, GRfollow from authors WHERE AuthorID=?', (authorid,))
            if authorsearch:
                if not authorsearch['GRfollow'] or authorsearch['GRfollow'] == '0':
                    logger.warning("Not Following %s" % authorsearch['AuthorName'])
                else:
                    msg = grsync.grfollow(authorid, False)
                    if msg.startswith('Unable'):
                        logger.warning(msg)
                    else:
                        db.action("UPDATE authors SET GRfollow='0' WHERE AuthorID=?", (authorid,))
                        logger.info(msg)
            else:
                msg = "Invalid authorid to unfollow (%s)" % authorid
                logger.error(msg)
                raise cherrypy.HTTPError(404, msg)
        finally:
            db.close()
        raise cherrypy.HTTPRedirect("author_page?authorid=%s" % authorid)

    @cherrypy.expose
    def library_scan_author(self, authorid, **kwargs):
        logger = logging.getLogger(__name__)
        loggerfuzz = logging.getLogger('special.fuzz')
        db = database.DBConnection()
        try:
            authorsearch = db.match('SELECT AuthorName from authors WHERE AuthorID=?', (authorid,))
            if authorsearch:  # to stop error if try to refresh an author while they are still loading
                author_name = authorsearch['AuthorName']
                types = []
                if CONFIG.get_bool('EBOOK_TAB'):
                    types.append('eBook')
                if CONFIG.get_bool('AUDIO_TAB'):
                    types.append('AudioBook')
                if not types:
                    raise cherrypy.HTTPRedirect('authors')
                library = types[0]
                if 'library' in kwargs and kwargs['library'] in types:
                    library = kwargs['library']

                if library == 'AudioBook':
                    authordir = safe_unicode(os.path.join(get_directory('AudioBook'), author_name))
                else:  # if library == 'eBook':
                    authordir = safe_unicode(os.path.join(get_directory('eBook'), author_name))
                if not path_isdir(authordir):
                    # books might not be in exact same authorname folder due to capitalisation
                    # or accent stripping etc.
                    # eg Calibre puts books into folder "Eric Van Lustbader", but
                    # goodreads told lazylibrarian he's "Eric van Lustbader", note the lowercase 'v'
                    # or calibre calls "Neil deGrasse Tyson" "Neil DeGrasse Tyson" with a capital 'D'
                    # so try a fuzzy match...
                    libdir = os.path.dirname(authordir)
                    matchname, exists = get_preferred_author_name(author_name)
                    if exists:
                        author_name = matchname
                    matchname = unaccented(matchname).lower()
                    bestmatch = [0, '']
                    for item in listdir(libdir):
                        match = fuzz.ratio(format_author_name(unaccented(item), CONFIG.get_list('NAME_POSTFIX')).lower(),
                                           matchname)
                        if match >= CONFIG.get_int('NAME_RATIO'):
                            authordir = os.path.join(libdir, item)
                            loggerfuzz.debug("Fuzzy match folder %s%% %s for %s" % (match, item, author_name))
                            # Add this name variant as an aka if not already there?
                            break
                        elif match > bestmatch[0]:
                            bestmatch = [match, item]

                if not path_isdir(authordir):
                    # if still not found, see if we have a book by them, and what directory it's in
                    if library == 'AudioBook':
                        sourcefile = 'AudioFile'
                    else:
                        sourcefile = 'BookFile'
                    cmd = 'SELECT %s from books,authors where books.AuthorID = authors.AuthorID' % sourcefile
                    cmd += '  and AuthorName=? and %s <> ""' % sourcefile
                    anybook = db.match(cmd, (author_name,))
                    if anybook:
                        authordir = safe_unicode(os.path.dirname(os.path.dirname(anybook[sourcefile])))
                if path_isdir(authordir):
                    remv = CONFIG.get_bool('FULL_SCAN')
                    try:
                        threading.Thread(target=library_scan, name='AUTHOR_SCAN_%s' % authorid,
                                         args=[authordir, library, authorid, remv]).start()
                    except Exception as e:
                        logger.error('Unable to complete the scan: %s %s' % (type(e).__name__, str(e)))
                else:
                    # maybe we don't have any of their books
                    logger.warning('Unable to find author directory: %s' % authordir)

                raise cherrypy.HTTPRedirect("author_page?authorid=%s&library=%s" % (authorid, library))
            else:
                logger.debug('ScanAuthor Invalid authorid [%s]' % authorid)
                raise cherrypy.HTTPError(404, "AuthorID %s not found" % authorid)
        finally:
            db.close()

    @cherrypy.expose
    def add_author(self, authorname):
        threading.Thread(target=add_author_name_to_db, name='ADDAUTHOR',
                         args=[authorname, False, True, 'WebServer add_author %s' % authorname]).start()
        time.sleep(2)  # so we get some data before going to authorpage
        raise cherrypy.HTTPRedirect("authors")

    @cherrypy.expose
    def add_author_id(self, authorid):
        threading.Thread(target=add_author_to_db, name='ADDAUTHORID',
                         args=['', False, authorid, True, 'WebServer add_author_id %s' % authorid]).start()
        time.sleep(2)  # so we get some data before going to authorpage
        raise cherrypy.HTTPRedirect("author_page?authorid=%s" % authorid)

    @cherrypy.expose
    def toggle_auth(self):
        if lazylibrarian.IGNORED_AUTHORS:  # show ignored/paused ones, or active/wanted ones
            lazylibrarian.IGNORED_AUTHORS = False
        else:
            lazylibrarian.IGNORED_AUTHORS = True
        raise cherrypy.HTTPRedirect("authors")

    @cherrypy.expose
    def toggle_my_auth(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        userprefs = userprefs ^ lazylibrarian.pref_myauthors
        cherrypy.response.cookie['ll_prefs'] = userprefs
        raise cherrypy.HTTPRedirect("authors")

    @cherrypy.expose
    def toggle_my_series(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        userprefs = userprefs ^ lazylibrarian.pref_myseries
        cherrypy.response.cookie['ll_prefs'] = userprefs
        raise cherrypy.HTTPRedirect("series")

    @cherrypy.expose
    def toggle_my_feeds(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        userprefs = userprefs ^ lazylibrarian.pref_myfeeds
        cherrypy.response.cookie['ll_prefs'] = userprefs
        raise cherrypy.HTTPRedirect("books")

    @cherrypy.expose
    def toggle_my_a_feeds(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        userprefs = userprefs ^ lazylibrarian.pref_myafeeds
        cherrypy.response.cookie['ll_prefs'] = userprefs
        raise cherrypy.HTTPRedirect("audio")

    @cherrypy.expose
    def toggle_my_mags(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        userprefs = userprefs ^ lazylibrarian.pref_mymags
        cherrypy.response.cookie['ll_prefs'] = userprefs
        raise cherrypy.HTTPRedirect("magazines")

    @cherrypy.expose
    def toggle_my_comics(self):
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        userprefs = userprefs ^ lazylibrarian.pref_mycomics
        cherrypy.response.cookie['ll_prefs'] = userprefs
        raise cherrypy.HTTPRedirect("comics")

    # BOOKS #############################################################

    @cherrypy.expose
    def booksearch(self, author=None, title=None, bookid=None, action=''):
        self.label_thread('BOOKSEARCH')
        if '_title' in action:
            searchterm = title
        elif '_author' in action:
            searchterm = author
        else:  # if '_full' in action: or legacy interface
            searchterm = '%s %s' % (author, title)
            searchterm = searchterm.strip()

        if action == 'e_full':
            cat = 'book'
        elif action == 'a_full':
            cat = 'audio'
        elif action:
            cat = 'general'
        else:  # legacy interface
            cat = 'book'

        results = search_item(searchterm, bookid, cat)
        library = 'eBook'
        if action.startswith('a_'):
            library = 'AudioBook'
        return serve_template(templatename="manualsearch.html", title=library + ' Search Results: "' +
                              searchterm + '"', bookid=bookid, results=results, library=library)

    @cherrypy.expose
    def count_providers(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        count = CONFIG.total_active_providers()
        return "Searching %s providers, please wait..." % count

    @cherrypy.expose
    def snatch_book(self, bookid=None, mode=None, provider=None, url=None, size=None, library=None, title=''):
        logger = logging.getLogger(__name__)
        logger.debug("snatch %s bookid %s mode=%s from %s url=[%s] %s" %
                     (library, bookid, mode, provider, url, title))
        db = database.DBConnection()
        try:
            bookdata = db.match('SELECT AuthorID, BookName from books WHERE BookID=?', (bookid,))
            if bookdata:
                size_temp = check_int(size, 1000)  # Need to cater for when this is NONE (Issue 35)
                size = round(float(size_temp) / 1048576, 2)
                control_value_dict = {"NZBurl": url}
                new_value_dict = {
                    "NZBprov": provider,
                    "BookID": bookid,
                    "NZBdate": now(),  # when we asked for it
                    "NZBsize": size,
                    "NZBtitle": bookdata["BookName"],
                    "NZBmode": mode,
                    "AuxInfo": library,
                    "Status": "Snatched"
                }
                db.upsert("wanted", new_value_dict, control_value_dict)
                author_id = bookdata["AuthorID"]
                if mode == 'direct':
                    snatch, res = direct_dl_method(bookid, bookdata["BookName"], url, library, provider)
                elif mode in ["torznab", "torrent", "magnet"]:
                    snatch, res = tor_dl_method(bookid, bookdata["BookName"], url, library)
                elif mode == 'nzb':
                    snatch, res = nzb_dl_method(bookid, bookdata["BookName"], url, library)
                elif mode == 'irc':
                    if title:
                        snatch, res = irc_dl_method(bookid, title, url, library, provider)
                    else:
                        snatch, res = irc_dl_method(bookid, bookdata["BookName"], url, library, provider)
                else:
                    res = 'Unhandled NZBmode [%s] for %s' % (mode, url)
                    logger.error(res)
                    snatch = False
                if snatch:
                    logger.info('Downloading %s %s from %s' % (library, bookdata["BookName"], provider))
                    custom_notify_snatch("%s %s" % (bookid, library))
                    notify_snatch("%s from %s at %s" % (unaccented(bookdata["BookName"], only_ascii=False),
                                                        CONFIG.disp_name(provider), now()))
                    schedule_job(action=SchedulerCommand.START, target='PostProcessor')
                else:
                    db.action('UPDATE wanted SET status="Failed",DLResult=? WHERE NZBurl=?', (res, url))
                raise cherrypy.HTTPRedirect("author_page?authorid=%s&library=%s" % (author_id, library))
            else:
                logger.debug('snatch_book Invalid bookid [%s]' % bookid)
                raise cherrypy.HTTPError(404, "BookID %s not found" % bookid)
        finally:
            db.close()

    @cherrypy.expose
    def audio(self, booklang=None, book_filter=''):
        user = 0
        email = ''
        db = database.DBConnection()
        try:
            cookie = cherrypy.request.cookie
            if cookie and 'll_uid' in list(cookie.keys()):
                user = cookie['ll_uid'].value
                res = db.match('SELECT SendTo from users where UserID=?', (user,))
                if res and res['SendTo']:
                    email = res['SendTo']
            if not booklang or booklang == 'None':
                booklang = None
            languages = db.select(
                'SELECT DISTINCT BookLang from books WHERE AUDIOSTATUS !="Skipped" AND AUDIOSTATUS !="Ignored"')
        finally:
            db.close()
        return serve_template(templatename="audio.html", title='AudioBooks', books=[],
                              languages=languages, booklang=booklang, user=user, email=email, book_filter=book_filter)

    @cherrypy.expose
    def books(self, booklang=None, book_filter=''):
        user = 0
        email = ''
        db = database.DBConnection()
        cookie = cherrypy.request.cookie
        try:
            if cookie and 'll_uid' in list(cookie.keys()):
                user = cookie['ll_uid'].value
                res = db.match('SELECT SendTo from users where UserID=?', (user,))
                if res and res['SendTo']:
                    email = res['SendTo']
            if not booklang or booklang == 'None':
                booklang = None
            languages = db.select('SELECT DISTINCT BookLang from books WHERE STATUS !="Skipped" AND STATUS !="Ignored"')
        finally:
            db.close()
        return serve_template(templatename="books.html", title='eBooks', books=[],
                              languages=languages, booklang=booklang, user=user, email=email, book_filter=book_filter)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_books(self, idisplay_start=0, idisplay_length=100, i_sort_col_0=0, ssort_dir_0="desc", ssearch="", **kwargs):
        rows = []
        filtered = []
        rowlist = []
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        db = database.DBConnection()
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)

            to_read = set()
            have_read = set()
            reading = set()
            abandoned = set()
            flag_to = 0
            flag_have = 0
            userid = None
            userprefs = 0
            if not CONFIG.get_bool('USER_ACCOUNTS'):
                perm = lazylibrarian.perm_admin
            else:
                perm = 0
                cookie = cherrypy.request.cookie
                if cookie and 'll_prefs' in list(cookie.keys()):
                    userprefs = check_int(cookie['ll_prefs'].value, 0)
                if cookie and 'll_uid' in list(cookie.keys()):
                    userid = cookie['ll_uid'].value
                    cmd = 'SELECT UserName,ToRead,HaveRead,Reading,Abandoned,Perms from users where UserID=?'
                    res = db.match(cmd, (userid,))
                    if res:
                        perm = check_int(res['Perms'], 0)
                        to_read = set(get_list(res['ToRead']))
                        have_read = set(get_list(res['HaveRead']))
                        reading = set(get_list(res['Reading']))
                        abandoned = set(get_list(res['Abandoned']))
                        loggerserverside.debug("get_books userid %s read %s,%s,%s,%s" % (
                            cookie['ll_uid'].value, len(to_read), len(have_read), len(reading), len(abandoned)))

            cmd = 'SELECT bookimg,authorname,bookname,bookrate,bookdate,books.status,books.bookid,booklang,'
            cmd += ' booksub,booklink,workpage,books.authorid,seriesdisplay,booklibrary,audiostatus,audiolibrary,'
            cmd += ' group_concat(series.seriesid || "~" || series.seriesname || " #" || member.seriesnum, "^")'
            cmd += ' as series, bookgenre,bookadded,scanresult,lt_workid,'
            cmd += ' group_concat(series.seriesname || " #" || member.seriesnum, "; ") as altsub'
            cmd += ' FROM books, authors'
            cmd += ' LEFT OUTER JOIN member ON (books.BookID = member.BookID)'
            cmd += ' LEFT OUTER JOIN series ON (member.SeriesID = series.SeriesID)'
            cmd += ' WHERE books.AuthorID = authors.AuthorID'

            types = []
            if CONFIG.get_bool('EBOOK_TAB'):
                types.append('eBook')
            if CONFIG.get_bool('AUDIO_TAB'):
                types.append('AudioBook')
            if types:
                library = types[0]
                if 'library' in kwargs and kwargs['library'] in types:
                    library = kwargs['library']
            else:
                library = None

            status_type = 'books.status'
            if library == 'AudioBook':
                status_type = 'audiostatus'
            args = []

            if kwargs['source'] == "Manage":
                if kwargs['whichStatus'] == 'ToRead':
                    cmd += ' and books.bookID in (' + ', '.join('"{0}"'.format(w) for w in to_read) + ')'
                elif kwargs['whichStatus'] == 'Read':
                    cmd += ' and books.bookID in (' + ', '.join('"{0}"'.format(w) for w in have_read) + ')'
                elif kwargs['whichStatus'] == 'Reading':
                    cmd += ' and books.bookID in (' + ', '.join('"{0}"'.format(w) for w in reading) + ')'
                elif kwargs['whichStatus'] == 'Abandoned':
                    cmd += ' and books.bookID in (' + ', '.join('"{0}"'.format(w) for w in abandoned) + ')'
                elif kwargs['whichStatus'] != 'All':
                    cmd += " and " + status_type + "='" + kwargs['whichStatus'] + "'"

            elif kwargs['source'] == "Books":
                cmd += " and books.STATUS !='Skipped' AND books.STATUS !='Ignored'"
            elif kwargs['source'] == "Audio":
                cmd += " and AUDIOSTATUS !='Skipped' AND AUDIOSTATUS !='Ignored'"
            elif kwargs['source'] == "Author":
                cmd += ' and books.AuthorID=?'
                args.append(kwargs['AuthorID'])
                if 'ignored' in kwargs and kwargs['ignored'] == "True":
                    cmd += ' and %s="Ignored"' % status_type
                else:
                    cmd += ' and %s != "Ignored"' % status_type

            if kwargs['source'] in ["Books", "Author", "Audio"]:
                # for these we need to check and filter on BookLang if set
                if 'booklang' in kwargs and kwargs['booklang'] != '' and kwargs['booklang'] != 'None':
                    cmd += ' and BOOKLANG=?'
                    args.append(kwargs['booklang'])

            if kwargs['source'] in ["Books", "Audio"]:
                if userid and userprefs & lazylibrarian.pref_myfeeds or \
                        userprefs & lazylibrarian.pref_myafeeds:
                    loggerserverside.debug("Getting user booklist")
                    mybooks = []
                    res = db.select('SELECT WantID from subscribers WHERE Type="author" and UserID=?', (userid,))
                    loggerserverside.debug("User subscribes to %s authors" % len(res))
                    for authorid in res:
                        bookids = db.select('SELECT BookID from books WHERE AuthorID=?', (authorid['WantID'],))
                        for bookid in bookids:
                            mybooks.append(bookid['BookID'])

                    res = db.select('SELECT WantID from subscribers WHERE Type="series" and UserID=?', (userid,))
                    loggerserverside.debug("User subscribes to %s series" % len(res))
                    for series in res:
                        sel = 'SELECT BookID from member,series WHERE series.seriesid=?'
                        sel += ' and member.seriesid=series.seriesid'
                        bookids = db.select(sel, (series['WantID'],))
                        for bookid in bookids:
                            mybooks.append(bookid['BookID'])

                    res = db.select('SELECT WantID from subscribers WHERE Type="feed" and UserID=?', (userid,))
                    loggerserverside.debug("User subscribes to %s feeds" % len(res))
                    for feed in res:
                        sel = 'SELECT BookID from books WHERE Requester like "%?%"'
                        sel += '  or AudioRequester like "%?%"'
                        bookids = db.select(sel, (feed['WantID'], feed['WantID']))
                        for bookid in bookids:
                            mybooks.append(bookid['BookID'])

                    mybooks = set(mybooks)
                    loggerserverside.debug("User booklist length %s" % len(mybooks))
                    cmd += ' and books.bookID in (' + ', '.join('"{0}"'.format(w) for w in mybooks) + ')'

            cmd += ' GROUP BY bookimg, authorname, bookname, bookrate, bookdate, books.status, books.bookid,'
            cmd += ' booklang, booksub, booklink, workpage, books.authorid, booklibrary, '
            cmd += ' audiostatus, audiolibrary, bookgenre, bookadded, scanresult, lt_workid'

            loggerserverside.debug("get_books %s: %s" % (cmd, str(args)))
            rowlist = db.select(cmd, tuple(args))
            loggerserverside.debug("get_books selected %s" % len(rowlist))

            if library is None:
                rowlist = []
            # At his point we want to sort and filter _before_ adding the html as it's much quicker
            # turn the sqlite rowlist into a list of lists
            if len(rowlist):
                for row in rowlist:  # iterate through the sqlite3.Row objects
                    entry = list(row)
                    if entry[16] is None:
                        entry[16] = ""
                    if CONFIG.get_bool('SORT_SURNAME'):
                        entry[1] = surname_first(entry[1], postfixes=CONFIG.get_list('NAME_POSTFIX'))
                    if CONFIG.get_bool('SORT_DEFINITE'):
                        entry[2] = sort_definite(entry[2], articles=CONFIG.get_list('NAME_DEFINITE'))
                    rows.append(entry)  # add each rowlist to the masterlist
                loggerserverside.debug("get_books surname/definite completed")

                if ssearch:
                    loggerserverside.debug("filter [%s]" % ssearch)
                    if library is not None:
                        search_fields = ['AuthorName', 'BookName', 'BookDate', 'Status', 'BookID',
                                         'BookLang', 'BookSub', 'AuthorID', 'BookGenre',
                                         'ScanResult']
                        if library == 'AudioBook':
                            search_fields[3] = 'AudioStatus'

                        filtered = list()
                        for row in rowlist:
                            _dict = dict(row)
                            for key in search_fields:
                                if _dict[key] and ssearch.lower() in _dict[key].lower():
                                    filtered.append(list(row))
                                    break
                    else:
                        filtered = [x for x in rows if ssearch.lower() in str(x).lower()]
                else:
                    filtered = rows

                # table headers and column headers do not match at this point
                sortcolumn = int(i_sort_col_0)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                if sortcolumn < 4:  # author, title
                    sortcolumn -= 1
                elif sortcolumn == 4:  # series
                    sortcolumn = 12
                elif sortcolumn == 8:  # status
                    if status_type == 'audiostatus':
                        sortcolumn = 14
                    else:
                        sortcolumn = 5
                elif sortcolumn == 7:  # added or listed
                    if kwargs['source'] == "Manage":
                        sortcolumn = 18
                    else:
                        if status_type == 'audiostatus':
                            sortcolumn = 15
                        else:
                            sortcolumn = 13
                else:  # rating, date
                    sortcolumn -= 2

                loggerserverside.debug("final sortcolumn %d" % sortcolumn)

                if sortcolumn in [12, 13, 15, 18]:  # series, dates
                    self.natural_sort(filtered, key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                      reverse=ssort_dir_0 == "desc")
                elif sortcolumn in [2]:  # title
                    filtered.sort(key=lambda y: y[sortcolumn].lower() if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

                # now add html to the ones we want to display
                data = []  # the masterlist to be filled with the html data
                for row in rows:
                    worklink = ''
                    sitelink = ''
                    if CONFIG.get_bool('RATESTARS'):
                        bookrate = int(round(check_float(row[3], 0.0)))
                        if bookrate > 5:
                            bookrate = 5
                    else:
                        bookrate = row[3]

                    if row[20]:  # is there a librarything workid
                        worklink = '<a href="' + CONFIG['LT_URL'] + '/' + 'work/' + \
                                   row[20] + '"><small><i>LibraryThing</i></small></a>'
                    elif row[10]:  # is there a workpage link
                        worklink = '<a href="' + row[10] + '" ><small><i>LibraryThing</i></small></a>'
                    else:
                        row[10] = ''
                        row[20] = ''

                    editpage = '<a href="edit_book?bookid=' + row[6] + '&library=' + library + \
                        '"><small><i>Manual</i></a>'

                    if not row[9]:
                        row[9] = ''
                    elif row[9].startswith('/works/OL'):
                        ref = CONFIG['OL_URL'] + row[9]
                        sitelink = '<a href="%s"><small><i>OpenLibrary</i></small></a>' % ref

                    elif 'goodreads' in row[9]:
                        sitelink = '<a href="%s"><small><i>GoodReads</i></small></a>' % row[9]
                    elif 'books.google.com' in row[9] or 'market.android.com' in row[9]:
                        sitelink = '<a href="%s"><small><i>GoogleBooks</i></small></a>' % row[9]
                    title = row[2]
                    if row[8] and ' #' not in row[8]:  # is there a subtitle that's not series info
                        title = '%s<br><small><i>%s</i></small>' % (title, row[8])
                    # elif row[20]:  # series info
                    #     title = '%s<br><small><i>(%s)</i></small>' % (title, row[20])
                    title = title + '<br>' + sitelink + ' ' + worklink
                    bookgenre = row[17]

                    if perm & lazylibrarian.perm_edit:
                        title = title + ' ' + editpage

                    if CONFIG.get_bool('SHOW_GENRES') and bookgenre and bookgenre != 'Unknown':
                        arr = bookgenre.split(',')
                        genres = ''
                        for a in arr:
                            if kwargs['source'] == "Audio":
                                genres = genres + ' <a href=\'audio?book_filter=' + a.strip() + '\'">' + \
                                         a.strip() + '</a>'
                            elif kwargs['source'] == "Books":
                                genres = genres + ' <a href=\'books?book_filter=' + a.strip() + '\'">' + \
                                         a.strip() + '</a>'
                            elif kwargs['source'] == "Author":
                                genres = genres + ' <a href=\'author_page?authorid=' + row[11] + '&book_filter=' + \
                                         a.strip() + '\'">' + a.strip() + '</a>'
                            else:
                                genres + genres + ' ' + a.strip()
                        genres = genres.strip()
                        title += ' [' + genres + ']'

                    if row[6] in to_read:
                        flag = '&nbsp;<i class="far fa-bookmark"></i>'
                        flag_to += 1
                    elif row[6] in have_read:
                        flag = '&nbsp;<i class="fas fa-bookmark"></i>'
                        flag_have += 1
                    elif row[6] in reading:
                        flag = '&nbsp;<i class="fas fa-play-circle"></i>'
                    elif row[6] in abandoned:
                        flag = '&nbsp;<i class="fas fa-ban"></i>'
                    else:
                        flag = ''

                    if status_type == 'audiostatus' and kwargs['source'] == 'Audio':
                        row[5] = row[14]
                        row[13] = row[15]

                    # Need to pass bookid and status twice for legacy as datatables modifies first one
                    thisrow = [row[6], row[0], row[1], title, row[12], bookrate, date_format(row[4], ''),
                               row[5], row[11], row[6],
                               date_format(row[13], CONFIG['DATE_FORMAT']),
                               row[5], row[16], flag]

                    if kwargs['source'] == "Manage":
                        cmd = "SELECT Time,Interval,Count from failedsearch WHERE Bookid=? AND Library='eBook'"
                        searches = db.match(cmd, (row[6],))
                        if searches:
                            thisrow.append("%s/%s" % (searches['Count'], searches['Interval']))
                            try:
                                thisrow.append(time.strftime("%d %b %Y", time.localtime(float(searches['Time']))))
                            except (ValueError, TypeError):
                                thisrow.append('')
                        else:
                            thisrow.append('0')
                            thisrow.append('')
                    elif kwargs['source'] == 'Author':
                        thisrow.append(row[14])
                        thisrow.append(date_format(row[15], CONFIG['DATE_FORMAT']))

                    thisrow.append(row[18])
                    thisrow.append(row[19])
                    data.append(thisrow)

                rows = data

            loggerserverside.debug("get_books %s returning %s to %s, flagged %s,%s" % (
                    kwargs['source'], displaystart, displaystart + displaylength, flag_to, flag_have))
            loggerserverside.debug("get_books filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_books: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      }
            if kwargs['source'] == 'Author':
                status = db.match("SELECT Status from authors WHERE authorid=?", (kwargs['AuthorID'],))
                mydict['loading'] = status['Status'] == 'Loading'
            elif kwargs['source'] == 'Books':
                mydict['loading'] = lazylibrarian.EBOOK_UPDATE
            elif kwargs['source'] == 'Audio':
                mydict['loading'] = lazylibrarian.AUDIO_UPDATE
            db.close()
            loggerserverside.debug(str(mydict))
            return mydict

    @staticmethod
    def natural_sort(lst, key=lambda s: s, reverse=False):
        """
        Sort the list into natural alphanumeric order.
        """

        def convert(text):
            return int(text) if text and text.isdigit() else text

        def get_alphanum_key_func(skey):
            return lambda s: [convert(c) for c in re.split('([0-9]+)', skey(s))]

        sort_key = get_alphanum_key_func(key)
        lst.sort(key=sort_key, reverse=reverse)

    @cherrypy.expose
    def add_book(self, bookid=None, authorid=None, library=None):
        if library == 'eBook':
            ebook_status = "Wanted"
            audio_status = "Skipped"
        elif library == 'AudioBook':
            audio_status = "Wanted"
            ebook_status = "Skipped"
        elif library == 'Both':
            audio_status = "Wanted"
            ebook_status = "Wanted"
        else:
            if CONFIG.get_bool('AUDIO_TAB'):
                audio_status = "Wanted"
            else:
                audio_status = "Skipped"
            if CONFIG.get_bool('EBOOK_TAB'):
                ebook_status = "Wanted"
            else:
                ebook_status = "Skipped"

        author_id = ''
        db = database.DBConnection()
        try:
            match = db.match('SELECT AuthorID from books WHERE BookID=?', (bookid,))
            if not match and authorid:
                _ = add_author_to_db(None, False, authorid, False, 'WebServer add_book %s' % bookid)
                match = db.match('SELECT AuthorID from books WHERE BookID=?', (bookid,))
            if match:
                db.upsert("books", {'Status': ebook_status, 'AudioStatus': audio_status},
                          {'BookID': bookid})
                author_id = match['AuthorID']
                update_totals(author_id)
            else:
                if CONFIG['BOOK_API'] == "GoogleBooks":
                    gb = GoogleBooks(bookid)
                    t = threading.Thread(target=gb.find_book, name='GB-BOOK',
                                         args=[bookid, ebook_status, audio_status, "Added by user"])
                    t.start()
                elif CONFIG['BOOK_API'] == "GoodReads":
                    gr = GoodReads(bookid)
                    t = threading.Thread(target=gr.find_book, name='GR-BOOK',
                                         args=[bookid, ebook_status, audio_status, "Added by user"])
                    t.start()
                else:  # if lazylibrarian.CONFIG['BOOK_API'] == "OpenLibrary":
                    ol = OpenLibrary(bookid)
                    t = threading.Thread(target=ol.find_book, name='OL-BOOK',
                                         args=[bookid, ebook_status, audio_status, "Added by user"])
                    t.start()
                t.join(timeout=10)  # 10 s to add book before redirect
        finally:
            db.close()

        if CONFIG.get_bool('IMP_AUTOSEARCH'):
            books = [{"bookid": bookid}]
            self.start_book_search(books)

        if author_id:
            raise cherrypy.HTTPRedirect("author_page?authorid=%s" % author_id)
        else:
            if CONFIG.get_bool('EBOOK_TAB'):
                raise cherrypy.HTTPRedirect("books")
            elif CONFIG.get_bool('AUDIO_TAB'):
                raise cherrypy.HTTPRedirect("audio")
        raise cherrypy.HTTPRedirect("authors")

    @cherrypy.expose
    def start_book_search(self, books=None, library=None, force=False):
        logger = logging.getLogger(__name__)
        if books:
            if CONFIG.use_any():
                if force:
                    name = 'FORCE-SEARCHBOOK'
                else:
                    name = 'SEARCHBOOK'
                logger.debug("Starting search_book thread")
                threading.Thread(target=search_book, name=name, args=[books, library]).start()
                booktype = library
                if not booktype:
                    booktype = 'book'  # all types
                logger.debug("Searching for %s with id: %s" % (booktype, books[0]["bookid"]))
            else:
                logger.warning("Not searching for book, no search methods set, check config.")
        else:
            logger.debug("BookSearch called with no books")

    @cherrypy.expose
    def search_for_book(self, bookid=None, library=None):
        author_id = ''
        db = database.DBConnection()
        try:
            bookdata = db.match('SELECT AuthorID from books WHERE BookID=?', (bookid,))
        finally:
            db.close()
        if bookdata:
            author_id = bookdata["AuthorID"]

            # start searchthreads
            books = [{"bookid": bookid}]
            self.start_book_search(books, library=library, force=True)

        if author_id:
            raise cherrypy.HTTPRedirect("author_page?authorid=%s" % author_id)
        else:
            raise cherrypy.HTTPRedirect("books")

    @cherrypy.expose
    def request_book(self, **kwargs):
        self.label_thread('REQUEST_BOOK')
        logger = logging.getLogger(__name__)
        prefix = ''
        title = 'Request Error'
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            db = database.DBConnection()
            try:
                res = db.match('SELECT Name,UserName,UserID,Email from users where UserID=?', (cookie['ll_uid'].value,))
                if res:
                    cmd = 'SELECT BookFile,AudioFile,AuthorName,BookName from books,authors WHERE BookID=?'
                    cmd += ' and books.AuthorID = authors.AuthorID'
                    bookdata = db.match(cmd, (kwargs['bookid'],))
                    kwargs.update(bookdata)
                    kwargs.update(res)
                    kwargs.update({'message': 'Request to Download'})

                    remote_ip = cherrypy.request.remote.ip
                    msg = 'IP: %s\n' % remote_ip
                    for item in kwargs:
                        if kwargs[item]:
                            line = "%s: %s\n" % (item, unaccented(kwargs[item], only_ascii=False))
                        else:
                            line = "%s: \n" % item
                        msg += line

                    types = []
                    if CONFIG.get_bool('EBOOK_TAB'):
                        types.append('eBook')
                    if CONFIG.get_bool('AUDIO_TAB'):
                        types.append('AudioBook')

                    booktype = 'book'
                    if types:
                        if 'library' in kwargs and kwargs['library'] in types:
                            booktype = kwargs['library']

                    title = "%s: %s" % (booktype, bookdata['BookName'])

                    if 'email' in kwargs and kwargs['email']:
                        result = notifiers.email_notifier.notify_message('Request from LazyLibrarian User',
                                                                         msg, CONFIG['ADMIN_EMAIL'])
                        if result:
                            prefix = "Message sent"
                            msg = "You will receive a reply by email"
                        else:
                            logger.error("Unable to send message to: %s" % msg)
                            prefix = "Message not sent"
                            msg = "Please try again later"
                    else:
                        prefix = "Unable to send message"
                        msg = "No email address supplied"
                else:
                    msg = "Unknown user"
            finally:
                db.close()
        else:
            msg = "Nobody logged in?"

        if prefix == "Message sent":
            timer = 5
        else:
            timer = 0
        return serve_template(templatename="response.html", prefix=prefix,
                              title=title, message=msg, timer=timer)

    @cherrypy.expose
    def serve_comic(self, feedid=None):
        logger = logging.getLogger(__name__)
        logger.debug("Serve Comic [%s]" % feedid)
        return self.serve_item(feedid, "comic")

    @cherrypy.expose
    def serve_img(self, feedid=None):
        logger = logging.getLogger(__name__)
        logger.debug("Serve Image [%s]" % feedid)
        return self.serve_item(feedid, "img")

    @cherrypy.expose
    def serve_book(self, feedid=None):
        logger = logging.getLogger(__name__)
        logger.debug("Serve Book [%s]" % feedid)
        return self.serve_item(feedid, "book")

    @cherrypy.expose
    def serve_audio(self, feedid=None):
        logger = logging.getLogger(__name__)
        logger.debug("Serve Audio [%s]" % feedid)
        return self.serve_item(feedid, "audio")

    @cherrypy.expose
    def serve_issue(self, feedid=None):
        logger = logging.getLogger(__name__)
        logger.debug("Serve Issue [%s]" % feedid)
        return self.serve_item(feedid, "issue")

    @cherrypy.expose
    def serve_item(self, feedid, ftype):
        logger = logging.getLogger(__name__)
        userid = feedid[:10]
        itemid = feedid[10:]
        itemid = itemid.split('.')[0]  # discard any extension
        if len(userid) != 10:
            logger.debug("Invalid userID [%s]" % userid)
            return

        target = ''

        size = re.search(r'_w\d+$', itemid)
        if size:
            size = check_int(size.group()[2:], 0)
            itemid = itemid[:-5]
        else:
            size = 0

        db = database.DBConnection()
        try:
            res = db.match('SELECT UserName,Perms,BookType from users where UserID=?', (userid,))
            if res:
                perm = check_int(res['Perms'], 0)
                preftype = res['BookType']
            else:
                logger.debug("Invalid userID [%s]" % userid)
                return

            if not perm & lazylibrarian.perm_download:
                logger.debug("Insufficient permissions for userID [%s]" % userid)
                return

            if ftype == 'img':
                if itemid:
                    res = db.match('SELECT BookName,BookImg from books WHERE BookID=?', (itemid,))
                    if res:
                        logger.debug("Itemid %s matches ebook" % itemid)
                        if size:
                            target = createthumb(os.path.join(DIRS.DATADIR, res['BookImg']), size, False)
                        if not target:
                            target = os.path.join(DIRS.DATADIR, res['BookImg'])
                        if path_isfile(target):
                            return self.send_file(target, name=res['BookName'] + os.path.splitext(res['BookImg'])[1])
                    else:
                        res = db.match('SELECT Title,Cover from issues WHERE IssueID=?', (itemid,))
                        if res:
                            logger.debug("Itemid %s matches issue" % itemid)
                            if size:
                                target = createthumb(os.path.join(DIRS.DATADIR, res['Cover']), size, False)
                            if not target:
                                target = os.path.join(DIRS.DATADIR, res['Cover'])
                            if path_isfile(target):
                                return self.send_file(target, name=res['Title'] + os.path.splitext(res['Cover'])[1])
                        else:
                            try:
                                comicid, issueid = itemid.split('_')
                                cmd = 'SELECT Title,Cover from comics,comicissues WHERE '
                                cmd += 'comics.ComicID=comicissues.ComicID and comics.ComicID=? and IssueID=?'
                                res = db.match(cmd, (comicid, issueid))
                            except (IndexError, ValueError):
                                res = None
                            if res:
                                logger.debug("Itemid %s matches comicid" % itemid)
                                if size:
                                    target = createthumb(os.path.join(DIRS.DATADIR, res['Cover']), size, False)
                                if not target:
                                    target = os.path.join(DIRS.DATADIR, res['Cover'])
                                if path_isfile(target):
                                    return self.send_file(target, name=res['Title'] + os.path.splitext(res['Cover'])[1])

                logger.debug("Itemid %s no match" % itemid)
                target = os.path.join(DIRS.PROG_DIR, 'data', 'images', 'll192.png')
                if path_isfile(target):
                    return self.send_file(target, name='lazylibrarian.png')

            elif ftype == 'comic':
                try:
                    comicid, issueid = itemid.split('_')
                    cmd = 'SELECT Title,IssueFile from comics,comicissues WHERE comics.ComicID=comicissues.ComicID'
                    cmd += ' and comics.ComicID=? and IssueID=?'
                    res = db.match(cmd, (comicid, issueid))
                except (IndexError, ValueError):
                    res = None
                    issueid = 0

                if res:
                    target = res['IssueFile']
                    if target and path_isfile(target):
                        logger.debug('Opening %s %s' % (ftype, target))
                        return self.send_file(target, name="%s %s%s" % (res['Title'], issueid,
                                              os.path.splitext(target)[1]))

            elif ftype == 'audio':
                res = db.match('SELECT AudioFile,BookName from books WHERE BookID=?', (itemid,))
                if res:
                    cnt = 0
                    myfile = res['AudioFile']
                    # count the audiobook parts
                    if myfile and path_isfile(myfile):
                        parentdir = os.path.dirname(myfile)
                        for _, _, filenames in walk(parentdir):
                            for filename in filenames:
                                if CONFIG.is_valid_booktype(filename, 'audiobook'):
                                    cnt += 1

                    if cnt > 1 and not CONFIG.get_bool('RSS_PODCAST'):
                        target = zip_audio(os.path.dirname(myfile), res['BookName'], itemid)
                        if target and path_isfile(target):
                            logger.debug('Opening %s %s' % (ftype, target))
                            return self.send_file(target, name=res['BookName'] + '.zip')

                    if myfile and path_isfile(myfile):
                        logger.debug('Opening %s %s' % (ftype, myfile))
                        return self.send_file(myfile)

            elif ftype == 'book':
                res = db.match('SELECT BookFile,BookName from books WHERE BookID=?', (itemid,))
                if res:
                    myfile = res['BookFile']
                    fname, extn = os.path.splitext(myfile)
                    types = []
                    for item in get_list(CONFIG['EBOOK_TYPE']):
                        target = fname + '.' + item
                        if path_isfile(target):
                            types.append(item)

                    # serve user preferred type if available, or system preferred type
                    if preftype and preftype in types:
                        extn = preftype
                    else:
                        extn = types[0]
                    myfile = fname + '.' + extn
                    if path_isfile(myfile):
                        logger.debug('Opening %s %s' % (ftype, myfile))
                        return self.send_file(myfile)

            elif ftype == 'issue':
                res = db.match('SELECT Title,IssueFile from issues WHERE IssueID=?', (itemid,))
                if res:
                    myfile = res['IssueFile']
                    if myfile and path_isfile(myfile):
                        logger.debug('Opening %s %s' % (ftype, myfile))
                        return self.send_file(myfile, name="%s %s%s" % (res['Title'], itemid,
                                              os.path.splitext(myfile)[1]))
        finally:
            db.close()
        logger.warning("No file found for %s %s" % (ftype, itemid))

    @cherrypy.expose
    def send_book(self, bookid=None, library=None, redirect=None, booktype=None):
        return self.open_book(bookid=bookid, library=library, redirect=redirect, booktype=booktype, email=True)

    @cherrypy.expose
    def open_book(self, bookid=None, library=None, redirect=None, booktype=None, email=False):
        logger = logging.getLogger(__name__)
        loggeradmin = logging.getLogger('special.admin')
        loggeradmin.debug("%s %s %s %s %s" % (bookid, library, redirect, booktype, email))
        self.label_thread('OPEN_BOOK')
        # we need to check the user priveleges and see if they can download the book
        db = database.DBConnection()
        try:
            if not CONFIG.get_bool('USER_ACCOUNTS'):
                perm = lazylibrarian.perm_admin
                preftype = ''
            else:
                perm = 0
                preftype = ''
                cookie = cherrypy.request.cookie
                if cookie and 'll_uid' in list(cookie.keys()):
                    res = db.match('SELECT UserName,Perms,BookType from users where UserID=?',
                                   (cookie['ll_uid'].value,))
                    if res:
                        perm = check_int(res['Perms'], 0)
                        preftype = res['BookType']

            if booktype is not None:
                preftype = booktype

            cmd = 'SELECT BookFile,AudioFile,AuthorName,BookName from books,authors WHERE BookID=?'
            cmd += ' and books.AuthorID = authors.AuthorID'
            bookdata = db.match(cmd, (bookid,))
        finally:
            db.close()
        if not bookdata:
            logger.warning('Missing bookid: %s' % bookid)
        else:
            if perm & lazylibrarian.perm_download:
                author_name = bookdata["AuthorName"]
                book_name = bookdata["BookName"]
                if library == 'AudioBook':
                    bookfile = bookdata["AudioFile"]
                    if bookfile and path_isfile(bookfile):
                        parentdir = os.path.dirname(bookfile)
                        namevars = name_vars(bookid)
                        singlename = namevars['AudioSingleFile']
                        singlefile = ''
                        # noinspection PyBroadException
                        try:
                            for fname in listdir(parentdir):
                                if CONFIG.is_valid_booktype(fname, booktype='audio'):
                                    bname, extn = os.path.splitext(fname)
                                    if bname == singlename:
                                        # found name matching the AudioSingleFile
                                        singlefile = os.path.join(parentdir, fname)
                                        break
                        except Exception:
                            pass

                        if booktype == 'whole' and singlefile and path_isfile(singlefile):
                            if email:
                                logger.debug('Emailing %s %s' % (library, singlefile))
                            else:
                                logger.debug('Opening %s %s' % (library, singlefile))
                            return self.send_file(singlefile, name=os.path.basename(singlefile), email=email)

                        index = os.path.join(parentdir, 'playlist.ll')
                        if path_isfile(index):
                            if booktype == 'zip':
                                zipfile = zip_audio(parentdir, book_name, bookid)
                                if zipfile and path_isfile(zipfile):
                                    if email:
                                        logger.debug('Emailing %s %s' % (library, zipfile))
                                    else:
                                        logger.debug('Opening %s %s' % (library, zipfile))
                                    return self.send_file(zipfile, name="%s.zip" % book_name, email=email)
                            idx = check_int(booktype, 0)
                            if idx:
                                with open(syspath(index), 'r') as f:
                                    part = f.read().splitlines()[idx - 1]
                                bookfile = os.path.join(parentdir, part)
                                if bookfile and path_isfile(bookfile):
                                    if email:
                                        logger.debug('Emailing %s %s' % (library, bookfile))
                                    else:
                                        logger.debug('Opening %s %s' % (library, bookfile))
                                    return self.send_file(bookfile, name="%s part%s%s" %
                                                          (book_name, idx, os.path.splitext(bookfile)[1]),
                                                          email=email)
                            # noinspection PyUnusedLocal
                            cnt = sum(1 for line in open(index))
                            if cnt <= 1:
                                if email:
                                    logger.debug('Emailing %s %s' % (library, bookfile))
                                else:
                                    logger.debug('Opening %s %s' % (library, bookfile))
                                return self.send_file(bookfile, email=email)
                            else:
                                msg = "Please select which part to "
                                if email:
                                    msg += "email"
                                else:
                                    msg += "download"
                                item = 1
                                partlist = ''
                                while item <= cnt:
                                    if partlist:
                                        partlist += ' '
                                    partlist += str(item)
                                    item += 1
                                    partlist += ' zip'
                                    if singlefile and path_isfile(singlefile):
                                        partlist += ' whole'
                                safetitle = book_name.replace('&', '&amp;').replace("'", "")

                                return serve_template(templatename="choosetype.html",
                                                      title=safetitle, pop_message=msg,
                                                      pop_types=partlist, bookid=bookid,
                                                      valid=get_list(partlist.replace(' ', ',')),
                                                      email=email)
                        if email:
                            logger.debug('Emailing %s %s' % (library, bookfile))
                        else:
                            logger.debug('Opening %s %s' % (library, bookfile))
                        return self.send_file(bookfile, email=email)
                else:
                    library = 'eBook'
                    bookfile = bookdata["BookFile"]
                    if bookfile and path_isfile(bookfile):
                        fname, _ = os.path.splitext(bookfile)
                        types = []
                        for item in get_list(CONFIG['EBOOK_TYPE']):
                            target = fname + '.' + item
                            if path_isfile(target):
                                types.append(item)
                        logger.debug('Preftype:%s Types:%s' % (preftype, str(types)))
                        if preftype and len(types):
                            if preftype in types:
                                bookfile = fname + '.' + preftype
                            else:
                                msg = "%s<br> Not available as %s, only " % (book_name, preftype)
                                typestr = ''
                                for item in types:
                                    if typestr:
                                        typestr += ' '
                                    typestr += item
                                msg += typestr
                                return serve_template(templatename="choosetype.html",
                                                      title="Not Available", pop_message=msg,
                                                      pop_types=typestr, bookid=bookid,
                                                      valid=get_list(CONFIG['EBOOK_TYPE']),
                                                      email=email)
                        elif len(types) > 1:
                            msg = "Please select format to "
                            if email:
                                msg += "email"
                            else:
                                msg += "download"
                            typestr = ''
                            for item in types:
                                if typestr:
                                    typestr += ' '
                                typestr += item
                            return serve_template(templatename="choosetype.html",
                                                  title="Choose Type", pop_message=msg,
                                                  pop_types=typestr, bookid=bookid,
                                                  valid=get_list(CONFIG['EBOOK_TYPE']),
                                                  email=email)
                        if len(types) and bookfile and path_isfile(bookfile):
                            if email:
                                logger.debug('Emailing %s %s' % (library, bookfile))
                                return self.send_file(bookfile, name=book_name, email=email)
                            else:
                                logger.debug('Opening %s %s' % (library, bookfile))
                                return self.send_file(bookfile, email=email)
                        else:
                            logger.debug('Unable to send %s %s, no valid types?' % (library, book_name))

                logger.info('Missing %s %s, %s [%s]' % (library, author_name, book_name, bookfile))
                if library == 'AudioBook':
                    raise cherrypy.HTTPRedirect("audio")
                else:
                    raise cherrypy.HTTPRedirect("books")
            else:
                return self.request_book(library=library, bookid=bookid, redirect=redirect)

    @cherrypy.expose
    def edit_author(self, authorid=None, images=False):
        self.label_thread('EDIT_AUTHOR')
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            data = db.match('SELECT * from authors WHERE AuthorID=?', (authorid,))
        finally:
            db.close()
        if data:
            photos = []
            if images:
                res = get_author_image(authorid=authorid, refresh=False, max_num=5)
                if res and path_isdir(res):
                    basedir = res.replace(DIRS.DATADIR, '').lstrip('/')
                    for item in listdir(res):
                        photos.append([item, os.path.join(basedir, item)])
            return serve_template(templatename="editauthor.html", title="Edit Author", config=data,
                                  images=photos)
        else:
            logger.info('Missing author %s:' % authorid)

    # noinspection PyUnusedLocal
    # kwargs needed for passing utf8 hidden input
    @cherrypy.expose
    def author_update(self, authorid='', authorname='', authorborn='', authordeath='', authorimg='',
                      editordata='', manual='0', **kwargs):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authdata = db.match('SELECT * from authors WHERE AuthorID=?', (authorid,))
            if authdata:
                edited = ""
                if not authorborn or authorborn == 'None':
                    authorborn = None
                if not authordeath or authordeath == 'None':
                    authordeath = None
                if authorimg == 'None':
                    authorimg = ''
                manual = bool(check_int(manual, 0))

                if authdata["AuthorBorn"] != authorborn:
                    edited += "Born "
                if authdata["AuthorDeath"] != authordeath:
                    edited += "Died "
                if 'cover' in kwargs:
                    if kwargs['cover'] == "manual":
                        if authorimg and (authdata["AuthorImg"] != authorimg):
                            edited += "Image manual"
                    elif kwargs['cover'] != "current":
                        authorimg = os.path.join(DIRS.DATADIR, kwargs['cover'])
                        edited += "Image %s " % kwargs['cover']

                if authdata["About"] != editordata:
                    edited += "Description "
                if not (bool(check_int(authdata["Manual"], 0)) == manual):
                    edited += "Manual "

                if authdata["AuthorName"] != authorname:
                    match = db.match('SELECT AuthorName from authors where AuthorName=?', (authorname,))
                    if match:
                        logger.debug("Unable to rename, new author name %s already exists" % authorname)
                        authorname = authdata["AuthorName"]
                    else:
                        edited += "Name "

                if edited:
                    # Check dates, format to yyyy/mm/dd
                    # use None to clear date
                    # Leave unchanged if fails datecheck
                    if authorborn is not None:
                        ab = date_format(authorborn)
                        if len(ab) == 10:
                            authorborn = ab
                        else:
                            logger.warning("Author Born date [%s] rejected" % authorborn)
                            authorborn = authdata["AuthorBorn"]  # leave unchanged
                            edited = edited.replace('Born ', '')

                    if authordeath is not None:
                        ab = date_format(authordeath)
                        if len(ab) == 10:
                            authordeath = ab
                        else:
                            logger.warning("Author Died date [%s] rejected" % authordeath)
                            authordeath = authdata["AuthorDeath"]  # leave unchanged
                            edited = edited.replace('Died ', '')

                    if not authorimg:
                        authorimg = authdata["AuthorImg"]
                    else:
                        if authorimg == 'none':
                            authorimg = os.path.join(DIRS.PROG_DIR, 'data', 'images', 'nophoto.png')

                        rejected = True

                        # Cache file image
                        if not path_isfile(authorimg):
                            logger.warning("Failed to find file %s" % authorimg)
                        else:
                            extn = os.path.splitext(authorimg)[1].lower()
                            if extn and extn in ['.jpg', '.jpeg', '.png', '.webp']:
                                destfile = os.path.join(DIRS.CACHEDIR, 'author', authorid + '.jpg')
                                try:
                                    copyfile(authorimg, destfile)
                                    logger.debug("%s->%s" % (authorimg, destfile))
                                    setperm(destfile)
                                    authorimg = 'cache/author/' + authorid + '.jpg'
                                    rejected = False
                                except Exception as why:
                                    logger.warning("Failed to copy file %s, %s %s" %
                                                   (authorimg, type(why).__name__, str(why)))
                            else:
                                logger.warning("Invalid extension on [%s]" % authorimg)

                        if authorimg.startswith('http'):
                            # cache image from url
                            # extn = os.path.splitext(authorimg)[1].lower()
                            # if extn and extn in ['.jpg', '.jpeg', '.png', '.webp']:
                            authorimg, success, _ = cache_img(ImageType.AUTHOR, authorid, authorimg, refresh=True)
                            if success:
                                rejected = False

                        if rejected:
                            logger.warning("Author Image [%s] rejected" % authorimg)
                            authorimg = authdata["AuthorImg"]
                            edited = edited.replace('Image ', '')

                    control_value_dict = {'AuthorID': authorid}
                    new_value_dict = {
                        'AuthorName': authorname,
                        'AuthorBorn': authorborn,
                        'AuthorDeath': authordeath,
                        'AuthorImg': authorimg,
                        'About': editordata,
                        'Manual': bool(manual)
                    }
                    db.upsert("authors", new_value_dict, control_value_dict)
                    logger.info('Updated [ %s] for %s' % (edited, authorname))

                else:
                    logger.debug('Author [%s] has not been changed' % authorname)
        finally:
            db.close()

        icrawlerdir = os.path.join(DIRS.CACHEDIR, 'icrawler', authorid)
        rmtree(icrawlerdir, ignore_errors=True)
        raise cherrypy.HTTPRedirect("author_page?authorid=%s" % authorid)

    @cherrypy.expose
    def edit_book(self, bookid=None, library='eBook', images=False):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"        
        self.label_thread('EDIT_BOOK')
        TELEMETRY.record_usage_data()
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            authors = db.select(
                "SELECT AuthorName from authors WHERE Status !='Ignored' ORDER by AuthorName COLLATE NOCASE")
            cmd = 'SELECT BookName,BookID,BookSub,BookGenre,BookLang,BookDesc,books.Manual,AuthorName,'
            cmd += 'books.AuthorID,BookDate,ScanResult,BookAdded,BookIsbn,WorkID,LT_WorkID,Narrator,BookFile '
            cmd += 'from books,authors WHERE books.AuthorID = authors.AuthorID and BookID=?'
            bookdata = db.match(cmd, (bookid,))
            cmd = 'SELECT SeriesName, SeriesNum from member,series '
            cmd += 'where series.SeriesID=member.SeriesID and BookID=?'
            seriesdict = db.select(cmd, (bookid,))
        finally:
            db.close()
        if bookdata:
            bookdata = dict(bookdata)
            bookdata['library'] = library
            if library != "AudioBook":
                bookdata.pop('Narrator', None)
            covers = []
            if images:
                # flickr needs an apikey and doesn't seem to have authors or book covers
                # baidu doesn't like bots, message: "Forbid spider access"
                sources = ['current', 'cover', 'goodreads', 'librarything', 'openlibrary',
                           'googleisbn', 'bing', 'googleimage']
                if NEW_WHATWORK:
                    sources.append('whatwork')
                for source in sources:
                    cover, _ = get_book_cover(bookid, source)
                    if cover:
                        covers.append([source, cover])

            bookfile = bookdata['BookFile']
            if bookfile and path_isfile(bookfile):
                opffile = opf_file(os.path.dirname(bookfile))
            else:
                opffile = ''
            if opffile and path_isfile(opffile):
                opf_template, replaces = opf_read(opffile)
                remove_file(opf_template)  # we don't need the template file yet
            else:
                replaces = []
            subs = []

            for item in replaces:
                # remove ones that are duplicated in bookdata, don't want two fields editing the same item
                # can't modify replaces list while iterating so make a new list
                if item[0] not in ['title', 'creator', 'ISBN', 'date', 'description']:
                    subs.append(item)
            return serve_template(templatename="editbook.html", title="Edit Book", config=bookdata,
                                  seriesdict=seriesdict, authors=authors, covers=covers, replaces=subs)
        else:
            logger.info('Missing book %s' % bookid)

    @cherrypy.expose
    def book_update(self, bookname='', bookid='', booksub='', bookgenre='', booklang='', bookdate='',
                    manual='0', authorname='', cover='', newid='', editordata='', bookisbn='', workid='',
                    **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        logger = logging.getLogger(__name__)
        if bookid:
            db = database.DBConnection()
            try:
                scanresult = ''
                if kwargs.get('importfrom'):
                    source = kwargs['importfrom']
                    folder = ''
                    library = ''
                    if path_isfile(source):
                        folder = os.path.dirname(source)
                    elif path_isdir(source):
                        folder = source
                    if folder:
                        if book_file(folder, booktype='audiobook', config=CONFIG):
                            library = 'Audio'
                        elif book_file(folder, booktype='ebook', config=CONFIG):
                            library = 'eBook'
                    if library:
                        res = process_book_from_dir(folder, library, bookid)
                        if res:
                            scanresult = 'Imported manually from %s' % folder
                        else:
                            logger.debug("Failed to import %s from %s" % (bookid, source))
                            raise cherrypy.HTTPRedirect("edit_book?bookid=%s" % bookid)
                    else:
                        logger.debug("No %s found in %s" % (library, source))

                cmd = 'SELECT BookName,BookSub,BookGenre,BookLang,BookImg,BookDate,BookDesc,books.Manual,AuthorName,'
                cmd += 'books.AuthorID, BookIsbn, WorkID, ScanResult, BookFile'
                cmd += ' from books,authors WHERE books.AuthorID = authors.AuthorID and BookID=?'
                bookdata = db.match(cmd, (bookid,))
                if bookdata:
                    edited = ''
                    moved = False
                    if bookgenre == 'None':
                        bookgenre = ''
                    manual = bool(check_int(manual, 0))

                    if newid and not (bookid == newid):
                        cmd = "SELECT BookName,Authorname from books,authors "
                        cmd += "WHERE books.AuthorID = authors.AuthorID and BookID=?"
                        match = db.match(cmd, (newid,))
                        if match:
                            logger.warning("Cannot change bookid to %s, in use by %s/%s" %
                                           (newid, match['BookName'], match['AuthorName']))
                        else:
                            logger.warning("Updating bookid is not supported yet")
                            # edited += "BookID "
                    if scanresult and not (bookdata["ScanResult"] == scanresult):
                        edited += "ScanResult "
                    if not (bookdata["BookName"] == bookname):
                        edited += "Title "
                    if not (bookdata["BookSub"] == booksub):
                        edited += "Subtitle "
                    if not (bookdata["BookDesc"] == editordata):
                        edited += "Description "
                    if not (bookdata["BookGenre"] == bookgenre):
                        edited += "Genre "
                    if not (bookdata["BookLang"] == booklang):
                        edited += "Language "
                    if not (bookdata["BookIsbn"] == bookisbn):
                        edited += "ISBN "
                    if not (bookdata["WorkID"] == workid):
                        edited += "WorkID "
                    if not (bookdata["BookDate"] == bookdate):
                        if bookdate == '0000':
                            edited += "Date "
                        else:
                            # googlebooks sometimes gives yyyy, sometimes yyyy-mm, sometimes yyyy-mm-dd
                            if len(bookdate) == 4:
                                y = check_year(bookdate)
                            elif len(bookdate) in [7, 10]:
                                y = check_year(bookdate[:4])
                                if y and len(bookdate) == 7:
                                    try:
                                        _ = datetime.date(int(bookdate[:4]), int(bookdate[5:7]), 1)
                                    except ValueError:
                                        y = 0
                                elif y and len(bookdate) == 10:
                                    try:
                                        _ = datetime.date(int(bookdate[:4]), int(bookdate[5:7]), int(bookdate[8:]))
                                    except ValueError:
                                        y = 0
                            else:
                                y = 0
                            if y:
                                edited += "Date "
                            else:
                                bookdate = bookdata["BookDate"]
                    if not (bool(check_int(bookdata["Manual"], 0)) == manual):
                        edited += "Manual "
                    if not (bookdata["AuthorName"] == authorname):
                        moved = True

                    covertype = ''
                    if cover == 'librarything':
                        covertype = '_lt'
                    elif cover == 'whatwork':
                        covertype = '_ww'
                    elif cover == 'goodreads':
                        covertype = '_gr'
                    elif cover == 'openlibrary':
                        covertype = '_ol'
                    elif cover == 'googleisbn':
                        covertype = '_gi'
                    elif cover == 'googleimage':
                        covertype = '_gb'
                    elif cover == 'bing':
                        covertype = '_bi'
                    elif cover == 'cover':
                        covertype = '_cover'

                    if covertype:
                        cachedir = DIRS.CACHEDIR
                        coverid = uuid.uuid4().hex
                        coverlink = 'cache/book/' + coverid + '.jpg'
                        coverfile = os.path.join(cachedir, "book", coverid + '.jpg')
                        newcoverfile = os.path.join(cachedir, "book", bookid + covertype + '.jpg')
                        if path_exists(newcoverfile):
                            copyfile(newcoverfile, coverfile)
                        edited += 'Cover (%s)' % cover
                    else:
                        coverlink = bookdata['BookImg']

                    if edited:
                        control_value_dict = {'BookID': bookid}
                        new_value_dict = {
                            'BookName': bookname,
                            'BookSub': booksub,
                            'BookGenre': bookgenre,
                            'BookLang': booklang,
                            'BookDate': bookdate,
                            'BookDesc': editordata,
                            'BookIsbn': bookisbn,
                            'WorkID': workid,
                            'BookImg': coverlink,
                            'ScanResult': scanresult,
                            'Manual': bool(manual)
                        }
                        db.upsert("books", new_value_dict, control_value_dict)

                    cmd = 'SELECT SeriesName, SeriesNum, series.SeriesID from member,series '
                    cmd += 'where series.SeriesID=member.SeriesID and BookID=?'
                    old_series = db.select(cmd, (bookid,))
                    old_list = []
                    new_list = []
                    dict_counter = 0
                    while "series[%s][name]" % dict_counter in kwargs:
                        s_name = kwargs["series[%s][name]" % dict_counter]
                        s_name = clean_name(s_name, '&/')
                        s_num = kwargs["series[%s][number]" % dict_counter]
                        match = db.match('SELECT SeriesID from series WHERE SeriesName=?', (s_name,))
                        if match:
                            new_list.append([match['SeriesID'], s_num, s_name])
                        else:
                            new_list.append(['', s_num, s_name])
                        dict_counter += 1
                    if 'series[new][name]' in kwargs and 'series[new][number]' in kwargs:
                        if kwargs['series[new][name]']:
                            s_name = kwargs["series[new][name]"]
                            s_name = clean_name(s_name, '&/')
                            s_num = kwargs['series[new][number]']
                            new_list.append(['', s_num, s_name])
                    for item in old_series:
                        old_list.append([item['SeriesID'], item['SeriesNum'], item['SeriesName']])

                    series_changed = False
                    for item in old_list:
                        if item[1:] not in [i[1:] for i in new_list]:
                            series_changed = True
                    for item in new_list:
                        if item[1:] not in [i[1:] for i in old_list]:
                            series_changed = True
                    if series_changed:
                        set_series(new_list, bookid, reason=scanresult)
                        delete_empty_series()
                        edited += "Series "

                    bookfile = bookdata['BookFile']
                    if bookfile and path_isfile(bookfile):
                        opffile = opf_file(os.path.dirname(bookfile))
                    else:
                        opffile = ''
                    if opffile and path_isfile(opffile):
                        opf_template, replaces = opf_read(opffile)
                    else:
                        opf_template = ''
                        replaces = []

                    if opf_template:
                        subs = []
                        for item in replaces:
                            if item[0] == 'title':
                                subs.append((item[0], bookname))
                            elif item[0] == 'creator':
                                subs.append((item[0], authorname))
                            elif item[0] == 'ISBN':
                                subs.append((item[0], bookisbn))
                            elif item[0] == 'date':
                                subs.append((item[0], bookdate))
                            elif item[0] == 'description':
                                subs.append((item[0], editordata))
                            elif item[0] in kwargs:
                                if item[1] != kwargs[item[0]]:
                                    edited += item[0] + ' '
                                    subs.append((item[0], kwargs[item[0]]))
                                else:
                                    subs.append((item[0], item[1]))
                            else:
                                subs.append((item[0], item[1]))

                        if edited:
                            new_opf = opf_write(opf_template, subs)
                            remove_file(opf_template)
                            remove_file(opffile)
                            safe_move(new_opf, opffile)
                    if edited:
                        logger.info('Updated [ %s] for %s' % (edited, bookname))
                    else:
                        logger.debug('Book [%s] has not been changed' % bookname)

                    if moved:
                        authordata = db.match('SELECT AuthorID from authors WHERE AuthorName=?', (authorname,))
                        if authordata:
                            control_value_dict = {'BookID': bookid}
                            new_value_dict = {'AuthorID': authordata['AuthorID']}
                            db.upsert("books", new_value_dict, control_value_dict)
                            update_totals(bookdata["AuthorID"])  # we moved from here
                            update_totals(authordata['AuthorID'])  # to here

                        logger.info('Book [%s] has been moved' % bookname)
                    else:
                        logger.debug('Book [%s] has not been moved' % bookname)
                    if edited or moved:
                        data = db.match("SELECT * from books,authors WHERE books.authorid=authors.authorid and BookID=?",
                                        (bookid,))
                        if data['BookFile'] and path_isfile(data['BookFile']):
                            dest_path = os.path.dirname(data['BookFile'])
                            global_name = os.path.splitext(os.path.basename(data['BookFile']))[0]
                            if opf_template:  # we already have a valid (new) opffile
                                dest_opf = os.path.join(dest_path, global_name + '.opf')
                                if opffile != dest_opf:
                                    safe_copy(opffile, dest_opf)
                            else:
                                create_opf(dest_path, data, global_name, overwrite=True)

                    raise cherrypy.HTTPRedirect("edit_book?bookid=%s" % bookid)
            finally:
                db.close()
        raise cherrypy.HTTPRedirect("books")

    @cherrypy.expose
    def mark_books(self, authorid=None, seriesid=None, action=None, redirect=None, **args):
        logger = logging.getLogger(__name__)
        if 'library' in args:
            library = args['library']
        else:
            library = 'eBook'
            if redirect == 'audio':
                library = 'AudioBook'

        if 'marktype' in args:
            library = args['marktype']

        for arg in ['book_table_length', 'ignored', 'library', 'booklang', 'marktype', 'AuthorID']:
            args.pop(arg, None)

        cookie = None
        to_read = []
        have_read = []
        reading = []
        abandoned = []

        db = database.DBConnection()
        try:
            if not redirect:
                redirect = "books"
            check_totals = []
            if redirect == 'author':
                check_totals = [authorid]
            reading_lists = ["Unread", "Read", "ToRead", "Reading", "Abandoned"]
            if action:
                if action in reading_lists:
                    cookie = cherrypy.request.cookie
                    if cookie and 'll_uid' in list(cookie.keys()):
                        userdata = db.match('SELECT ToRead,HaveRead,Reading,Abandoned from users where UserID=?',
                                            (cookie['ll_uid'].value,))
                        if userdata:
                            to_read = set(get_list(userdata['ToRead']))
                            have_read = set(get_list(userdata['HaveRead']))
                            reading = set(get_list(userdata['Reading']))
                            abandoned = set(get_list(userdata['Abandoned']))

                for bookid in args:
                    if action in reading_lists:
                        to_read.discard(bookid)
                        have_read.discard(bookid)
                        reading.discard(bookid)
                        abandoned.discard(bookid)

                        if action == "Read":
                            have_read.add(bookid)
                        elif action == "ToRead":
                            to_read.add(bookid)
                        elif action == "Reading":
                            reading.add(bookid)
                        elif action == "Abandoned":
                            abandoned.add(bookid)
                        logger.debug('Status set to %s for %s' % (action, bookid))

                    elif action in ["Skipped", "Have", "Ignored", "IgnoreBoth", "Wanted", "WantBoth"]:
                        bookdata = db.match('SELECT AuthorID,BookName,Status,AudioStatus from books WHERE BookID=?',
                                            (bookid,))
                        if bookdata:
                            authorid = bookdata['AuthorID']
                            bookname = bookdata['BookName']
                            if authorid not in check_totals:
                                check_totals.append(authorid)
                            if (action == "Wanted" and library == 'eBook') or action == "WantBoth":
                                if bookdata['Status'] == "Open":
                                    logger.debug('eBook "%s" is already marked Open' % bookname)
                                else:
                                    db.upsert("books", {'Status': 'Wanted'}, {'BookID': bookid})
                                    logger.debug('Status set to "Wanted" for "%s"' % bookname)
                            if (action == "Wanted" and library == 'AudioBook') or action == "WantBoth":
                                if bookdata['AudioStatus'] == "Open":
                                    logger.debug('AudioBook "%s" is already marked Open' % bookname)
                                else:
                                    db.upsert("books", {'AudioStatus': 'Wanted'}, {'BookID': bookid})
                                    logger.debug('AudioStatus set to "Wanted" for "%s"' % bookname)
                            if (action == "Ignored" and library == 'eBook') or action == "IgnoreBoth":
                                db.upsert("books", {'Status': "Ignored", 'ScanResult': 'User %s' % action},
                                          {'BookID': bookid})
                                logger.debug('Status set to "Ignored" for "%s"' % bookname)
                            if (action == "Ignored" and library == 'AudioBook') or action == "IgnoreBoth":
                                db.upsert("books", {'AudioStatus': "Ignored", 'ScanResult': 'User %s' % action},
                                          {'BookID': bookid})
                                logger.debug('AudioStatus set to "Ignored" for "%s"' % bookname)
                            if action in ["Skipped", "Have"]:
                                if library == 'eBook':
                                    db.upsert("books", {'Status': action, 'ScanResult': 'User %s' % action},
                                              {'BookID': bookid})
                                    logger.debug('Status set to "%s" for "%s"' % (action, bookname))
                                if library == 'AudioBook':
                                    db.upsert("books", {'AudioStatus': action, 'ScanResult': 'User %s' % action},
                                              {'BookID': bookid})
                                    logger.debug('AudioStatus set to "%s" for "%s"' % (action, bookname))
                        else:
                            logger.warning("Unable to set status %s for %s" % (action, bookid))
                    elif action == "NoDelay":
                        db.action("delete from failedsearch WHERE BookID=? AND Library=?", (bookid, library))
                        logger.debug('%s delay set to zero for %s' % (library, bookid))
                    elif action in ["Remove", "Delete"]:
                        cmd = 'SELECT AuthorName,Bookname,BookFile,AudioFile,books.AuthorID from books,authors '
                        cmd += 'WHERE BookID=? and books.AuthorID = authors.AuthorID'
                        bookdata = db.match(cmd, (bookid,))
                        if bookdata:
                            authorid = bookdata['AuthorID']
                            bookname = bookdata['BookName']
                            if authorid not in check_totals:
                                check_totals.append(authorid)
                            if action == "Delete":
                                if 'Audio' in library:
                                    bookfile = bookdata['AudioFile']
                                    if bookfile and path_isfile(bookfile):
                                        try:
                                            rmtree(os.path.dirname(bookfile), ignore_errors=True)
                                            logger.info('AudioBook %s deleted from disc' % bookname)
                                        except Exception as e:
                                            logger.warning('rmtree failed on %s, %s %s' %
                                                           (bookfile, type(e).__name__, str(e)))

                                if 'eBook' in library:
                                    bookfile = bookdata['BookFile']
                                    if bookfile and path_isfile(bookfile):
                                        try:
                                            rmtree(os.path.dirname(bookfile), ignore_errors=True)
                                            deleted = True
                                        except Exception as e:
                                            logger.warning('rmtree failed on %s, %s %s' %
                                                           (bookfile, type(e).__name__, str(e)))
                                            deleted = False

                                        if deleted:
                                            logger.info('eBook %s deleted from disc' % bookname)
                                            if CONFIG['IMP_CALIBREDB'] and \
                                                    CONFIG.get_bool('IMP_CALIBRE_EBOOK'):
                                                self.delete_from_calibre(bookdata)

                            authorcheck = db.match('SELECT Status from authors WHERE AuthorID=?', (authorid,))
                            if authorcheck:
                                if authorcheck['Status'] not in ['Active', 'Wanted']:
                                    db.action('delete from books where bookid=?', (bookid,))
                                    db.action('delete from wanted where bookid=?', (bookid,))
                                    logger.info('Removed "%s" from database' % bookname)
                                elif 'eBook' in library:
                                    db.upsert("books", {"Status": "Ignored", "ScanResult": "User deleted"},
                                              {"BookID": bookid})
                                    logger.debug('Status set to Ignored for "%s"' % bookname)
                                elif 'Audio' in library:
                                    db.upsert("books", {"AudioStatus": "Ignored", "ScanResult": "User deleted"},
                                              {"BookID": bookid})
                                    logger.debug('AudioStatus set to Ignored for "%s"' % bookname)
                            else:
                                db.action('delete from books where bookid=?', (bookid,))
                                db.action('delete from wanted where bookid=?', (bookid,))
                                logger.info('Removed "%s" from database' % bookname)

                if action in reading_lists and cookie:
                    db.action('UPDATE users SET ToRead=?,HaveRead=?,Reading=?,Abandoned=? WHERE UserID=?',
                              (', '.join('"{0}"'.format(w) for w in to_read),
                               ', '.join('"{0}"'.format(w) for w in have_read),
                               ', '.join('"{0}"'.format(w) for w in reading),
                               ', '.join('"{0}"'.format(w) for w in abandoned),
                               cookie['ll_uid'].value))
        finally:
            db.close()

        if check_totals:
            for author in check_totals:
                update_totals(author)

        # start searchthreads
        if action in ['Wanted', 'WantBoth']:
            books = []
            for arg in args:
                books.append({"bookid": arg})

            if CONFIG.use_any():
                if check_int(CONFIG['SEARCH_BOOKINTERVAL'], 0):
                    logger.debug("Starting search threads, library=%s, action=%s" %
                                 (library, action))
                    if action == 'WantBoth' or (action == 'Wanted' and 'eBook' in library):
                        threading.Thread(target=search_book, name='SEARCHBOOK',
                                         args=[books, 'eBook']).start()
                    if action == 'WantBoth' or (action == 'Wanted' and 'Audio' in library):
                        threading.Thread(target=search_book, name='SEARCHBOOK',
                                         args=[books, 'AudioBook']).start()

        if redirect == "author":
            if 'eBook' in library:
                raise cherrypy.HTTPRedirect("author_page?authorid=%s&library=%s" % (authorid, 'eBook'))
            if 'Audio' in library:
                raise cherrypy.HTTPRedirect("author_page?authorid=%s&library=%s" % (authorid, 'AudioBook'))
        elif redirect in ["books", "audio"]:
            raise cherrypy.HTTPRedirect(redirect)
        elif redirect == "members":
            raise cherrypy.HTTPRedirect("series_members?seriesid=%s&ignored=False" % seriesid)
        elif 'Audio' in library:
            raise cherrypy.HTTPRedirect("manage?library=%s" % 'AudioBook')
        raise cherrypy.HTTPRedirect("manage?library=%s" % 'eBook')

    # WALL #########################################################

    @cherrypy.expose
    def mag_wall(self, title=''):
        self.label_thread('MAGWALL')
        cmd = 'SELECT IssueFile,IssueID,IssueDate,Title,Cover from issues'
        args = None
        if title:
            title = title.replace('&amp;', '&')
            cmd += ' WHERE Title=?'
            args = (title,)
        cmd += ' order by IssueAcquired DESC'
        db = database.DBConnection()
        try:
            issues = db.select(cmd, args)
        finally:
            db.close()
        if not len(issues):
            raise cherrypy.HTTPRedirect("magazines")
        else:
            mod_issues = []
            count = 0
            maxcount = CONFIG.get_int('MAX_WALL')
            for issue in issues:
                this_issue = dict(issue)
                if not this_issue.get('Cover') or not this_issue['Cover'].startswith('cache/'):
                    this_issue['Cover'] = 'images/nocover.jpg'
                else:
                    fname, extn = os.path.splitext(this_issue['Cover'])
                    imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                    if path_isfile(imgfile):
                        this_issue['Cover'] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                    else:
                        imgfile = os.path.join(DIRS.CACHEDIR, this_issue['Cover'][6:])
                        imgthumb = createthumb(imgfile, 200, False)
                        if imgthumb:
                            this_issue['Cover'] = "%s%s" % ('cache/',
                                                            imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
                this_issue['Title'] = issue['Title'].replace('&amp;', '&')
                mod_issues.append(this_issue)
                count += 1
                if maxcount and count >= maxcount:
                    title = "%s (Top %i)" % (title, count)
                    break

        return serve_template(
            templatename="coverwall.html", title=title, results=mod_issues, redirect="magazines",
            columns=CONFIG['WALL_COLUMNS'])

    @cherrypy.expose
    def comic_wall(self, comicid=None):
        self.label_thread('COMICWALL')
        cmd = 'SELECT IssueFile,IssueID,comics.ComicID,Title,Cover from comicissues,comics WHERE '
        cmd += 'comics.ComicID = comicissues.ComicID'
        args = None
        if comicid:
            cmd += ' AND comics.ComicID=?'
            args = (comicid,)
        cmd += ' order by IssueAcquired DESC'
        db = database.DBConnection()
        try:
            issues = db.select(cmd, args)
        finally:
            db.close()
        title = ''
        if not len(issues):
            raise cherrypy.HTTPRedirect("comics")
        else:
            mod_issues = []
            count = 0
            maxcount = CONFIG.get_int('MAX_WALL')
            for issue in issues:
                this_issue = dict(issue)
                if not this_issue.get('Cover') or not this_issue['Cover'].startswith('cache/'):
                    this_issue['Cover'] = 'images/nocover.jpg'
                else:
                    fname, extn = os.path.splitext(this_issue['Cover'])
                    imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                    if path_isfile(imgfile):
                        this_issue['Cover'] = "%s%s" % ('cache/',
                                                        imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                    else:
                        imgfile = os.path.join(DIRS.CACHEDIR, this_issue['Cover'][6:])
                        imgthumb = createthumb(imgfile, 200, False)
                        if imgthumb:
                            this_issue['Cover'] = "%s%s" % ('cache/',
                                                            imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
                mod_issues.append(this_issue)
                count += 1
                if maxcount and count >= maxcount:
                    title = "%s (Top %i)" % (title, count)
                    break

        return serve_template(
            templatename="coverwall.html", title=title, results=mod_issues, redirect="comic",
            columns=CONFIG['WALL_COLUMNS'])

    @cherrypy.expose
    def book_wall(self, have='0'):
        self.label_thread('BOOKWALL')
        if have == '1':
            cmd = 'SELECT BookLink,BookImg,BookID,BookName from books where Status="Open" order by BookLibrary DESC'
            title = 'Recently Downloaded Books'
        else:
            cmd = 'SELECT BookLink,BookImg,BookID,BookName from books where Status != "Ignored" order by BookAdded DESC'
            title = 'Recently Added Books'
        db = database.DBConnection()
        try:
            results = db.select(cmd)
        finally:
            db.close()
        if not len(results):
            raise cherrypy.HTTPRedirect("books")
        maxcount = CONFIG.get_int('MAX_WALL')
        if maxcount and len(results) > maxcount:
            results = results[:maxcount]
            title = "%s (Top %i)" % (title, len(results))
        ret = []
        for result in results:
            item = dict(result)
            if not item['BookLink']:
                item['BookLink'] = ''
            elif item['BookLink'].startswith('/works/OL'):
                item['BookLink'] = CONFIG['OL_URL'] + item['BookLink']

            if not item.get('BookImg') or not item['BookImg'].startswith('cache/'):
                item['BookImg'] = 'images/nocover.jpg'
            else:
                fname, extn = os.path.splitext(item['BookImg'])
                imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                if path_isfile(imgfile):
                    item['BookImg'] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                else:
                    imgfile = os.path.join(DIRS.CACHEDIR, item['BookImg'][6:])
                    imgthumb = createthumb(imgfile, 200, False)
                    if imgthumb:
                        item['BookImg'] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
            ret.append(item)
        return serve_template(
            templatename="coverwall.html", title=title, results=ret, redirect="books", have=have,
            columns=CONFIG['WALL_COLUMNS'])

    @cherrypy.expose
    def author_wall(self, have='1'):
        self.label_thread('AUTHORWALL')
        cmd = 'SELECT Status,AuthorImg,AuthorID,AuthorName,HaveBooks,TotalBooks from authors '
        if have == '1':
            cmd += 'where Status="Active" or Status="Wanted" order by AuthorName ASC'
            title = 'Active/Wanted Authors'
        else:
            cmd += 'where Status !="Active" and Status != "Wanted" order by AuthorName ASC'
            title = 'Inactive Authors'
        db = database.DBConnection()
        try:
            results = db.select(cmd)
        finally:
            db.close()
        if not len(results):
            raise cherrypy.HTTPRedirect("authors")

        ret = []
        for result in results:
            item = dict(result)
            if not item.get('AuthorImg') or not item['AuthorImg'].startswith('cache/'):
                item['AuthorImg'] = 'images/nocover.jpg'
            else:
                fname, extn = os.path.splitext(item['AuthorImg'])
                imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                if path_isfile(imgfile):
                    item['AuthorImg'] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                else:
                    imgfile = os.path.join(DIRS.CACHEDIR, item['AuthorImg'][6:])
                    imgthumb = createthumb(imgfile, 200, False)
                    if imgthumb:
                        item['AuthorImg'] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
            ret.append(item)
        return serve_template(
            templatename="coverwall.html", title=title, results=ret, redirect="authors", have=have,
            columns=CONFIG['WALL_COLUMNS'])

    @cherrypy.expose
    def audio_wall(self):
        self.label_thread('AUDIOWALL')
        db = database.DBConnection()
        try:
            results = db.select(
                'SELECT AudioFile,BookImg,BookID,BookName from books where AudioStatus="Open" order by AudioLibrary DESC')
        finally:
            db.close()
        if not len(results):
            raise cherrypy.HTTPRedirect("audio")
        title = "Recent AudioBooks"
        maxcount = CONFIG.get_int('MAX_WALL')
        if maxcount and len(results) > maxcount:
            results = results[:maxcount]
            title = "%s (Top %i)" % (title, len(results))
        ret = []
        for result in results:
            item = dict(result)
            if not item.get('BookImg') or not item['BookImg'].startswith('cache/'):
                item['BookImg'] = 'images/nocover.jpg'
            else:
                fname, extn = os.path.splitext(item['BookImg'])
                imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                if path_isfile(imgfile):
                    item['BookImg'] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                else:
                    imgfile = os.path.join(DIRS.CACHEDIR, item['BookImg'][6:])
                    imgthumb = createthumb(imgfile, 200, False)
                    if imgthumb:
                        item['BookImg'] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
            ret.append(item)
        return serve_template(
            templatename="coverwall.html", title=title, results=ret, redirect="audio",
            columns=CONFIG['WALL_COLUMNS'])

    @cherrypy.expose
    def wall_columns(self, redirect=None, count=None, have=0, title=''):
        title = title.split(' (')[0].replace(' ', '+')
        columns = check_int(CONFIG['WALL_COLUMNS'], 6)
        if count == 'up' and columns <= 12:
            columns += 1
        elif count == 'down' and columns > 1:
            columns -= 1
        CONFIG.set_int('WALL_COLUMNS', columns)
        if redirect == 'audio':
            raise cherrypy.HTTPRedirect('audio_wall')
        elif redirect == 'books':
            raise cherrypy.HTTPRedirect('book_wall?have=%s' % have)
        elif redirect == 'magazines':
            if title:
                raise cherrypy.HTTPRedirect('mag_wall?title=%s' % title)
            else:
                raise cherrypy.HTTPRedirect('mag_wall')
        elif redirect == 'comic':
            if title:
                raise cherrypy.HTTPRedirect('comic_wall?comicid=%s' % title)
            else:
                raise cherrypy.HTTPRedirect('comic_wall')
        elif redirect == 'authors':
            raise cherrypy.HTTPRedirect('author_wall?have=%s' % have)
        else:
            raise cherrypy.HTTPRedirect('home')

    # COMICS #########################################################

    @cherrypy.expose
    def edit_comic(self, comicid=None):
        self.label_thread('EDIT_COMIC')
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            data = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
        finally:
            db.close()
        if data:
            return serve_template(templatename="editcomic.html", title="Edit Comic", config=data)
        else:
            logger.info('Missing comic %s:' % comicid)
            raise cherrypy.HTTPError(404, "Comic ID %s not found" % comicid)

    # noinspection PyUnusedLocal
    @cherrypy.expose
    def comic_update(self, comicid='', new_name='', new_id='', aka='', editordata='', **kwargs):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            comicdata = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
            if comicdata:
                edited = ""
                if comicdata["Title"] != new_name:
                    edited += "Title "
                if comicdata["aka"] != aka:
                    edited += "aka "
                if comicdata["Description"] != editordata:
                    edited += "Description "

                if comicid != new_id:
                    match = db.match('SELECT ComicID from comics where ComicID=?', (new_id,))
                    if match:
                        logger.debug("Unable to use new ID, %s already exists" % new_id)
                    else:
                        db.action('PRAGMA foreign_keys = OFF')
                        db.action("UPDATE comics SET comicid=? WHERE comicid=?", (new_id, comicid))
                        db.action("UPDATE comicissues SET comicid=? WHERE comicid=?", (new_id, comicid))
                        db.action('PRAGMA foreign_keys = ON')
                        logger.debug("Updated comicid %s to %s" % (comicid, new_id))
                        comicid = new_id

                if edited:
                    control_value_dict = {'ComicID': comicid}
                    new_value_dict = {
                        'Title': new_name,
                        'aka': aka,
                        'Description': editordata
                    }
                    db.upsert("comics", new_value_dict, control_value_dict)
                    logger.info('Updated [ %s] for %s' % (edited, comicdata["Title"]))
                else:
                    logger.debug('Comic [%s] has not been changed' % comicdata["Title"])
                raise cherrypy.HTTPRedirect("comicissue_page?comicid=%s" % comicid)
            else:
                logger.warning("Invalid comicid [%s]" % comicid)
                raise cherrypy.HTTPError(404, "Comic ID %s not found" % comicid)
        finally:
            db.close()

    @cherrypy.expose
    def search_for_comic(self, comicid=None):
        db = database.DBConnection()
        try:
            bookdata = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
        finally:
            db.close()
        if bookdata:
            # start searchthreads
            self.start_comic_search(comicid)
            raise cherrypy.HTTPRedirect("comicissue_page?comicid=%s" % comicid)
        raise cherrypy.HTTPRedirect("comics")

    @cherrypy.expose
    def start_comic_search(self, comicid=None):
        logger = logging.getLogger(__name__)
        if comicid:
            if CONFIG.use_any():
                threading.Thread(target=search_comics, name='SEARCHCOMIC', args=[comicid]).start()
                logger.debug("Searching for comic ID %s" % comicid)
            else:
                logger.warning("Not searching for comic, no download methods set, check config")
        else:
            logger.debug("ComicSearch called with no comic ID")

    @cherrypy.expose
    def comics(self, comic_filter=''):
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            user = cookie['ll_uid'].value
        else:
            user = 0
        # use server-side processing
        covers = 1
        if not CONFIG['TOGGLES'] and not CONFIG.get_bool('COMIC_IMG'):
            covers = 0
        return serve_template(templatename="comics.html", title="Comics", comics=[],
                              covercount=covers, user=user, comic_filter=comic_filter)

    # noinspection PyUnusedLocal
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_comics(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        rows = []
        filtered = []
        rowlist = []
        userid = None
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)
            db = database.DBConnection()
            try:
                cmd = 'select comics.*,(select count(*) as counter from comicissues '
                cmd += 'where comics.comicid = comicissues.comicid) as Iss_Cnt from comics'

                mycomics = []
                if userid and userprefs & lazylibrarian.pref_mycomics:
                    res = db.select('SELECT WantID from subscribers WHERE Type="comic" and UserID=?', (userid,))
                    loggerserverside.debug("User subscribes to %s comics" % len(res))
                    for mag in res:
                        mycomics.append(mag['WantID'])
                    cmd += ' WHERE comics.comicid in (' + ', '.join('"{0}"'.format(w) for w in mycomics) + ')'
                cmd += ' order by Title'
                rowlist = db.select(cmd)
            finally:
                db.close()

            if len(rowlist):
                newrowlist = []
                for mag in rowlist:
                    mag = dict(mag)  # turn sqlite objects into dicts
                    entry = [mag['ComicID'], mag['LatestCover'], mag['Title'], mag['Iss_Cnt'], mag['LastAcquired'],
                             mag['LatestIssue'], mag['Status'], mag['IssueStatus'], mag['Start'],
                             mag['Publisher'], mag['Link'], mag['Genre']]
                    newrowlist.append(entry)  # add each rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in newrowlist if ssearch.lower() in str(x).lower()]
                else:
                    filtered = newrowlist

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                if sortcolumn in [4, 5]:  # dates
                    self.natural_sort(filtered, key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                      reverse=ssort_dir_0 == "desc")
                elif sortcolumn == 2:  # title
                    filtered.sort(key=lambda y: y[sortcolumn].lower() if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

                for row in rows:
                    if not row[1] or not row[1].startswith('cache/'):
                        row[1] = 'images/nocover.jpg'
                    else:
                        fname, extn = os.path.splitext(row[1])
                        imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                        if path_isfile(imgfile):
                            row[1] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                        else:
                            imgfile = os.path.join(DIRS.CACHEDIR, row[1][6:])
                            imgthumb = createthumb(imgfile, 200, False)
                            if imgthumb:
                                row[1] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
                    row[4] = date_format(row[4], CONFIG['DATE_FORMAT'])
                    if row[5] and row[5].isdigit():
                        if len(row[5]) == 8:
                            if check_year(row[5][:4]):
                                row[5] = 'Issue %d %s' % (int(row[5][4:]), row[5][:4])
                            else:
                                row[5] = 'Vol %d #%d' % (int(row[5][:4]), int(row[5][4:]))
                        elif len(row[5]) == 12:
                            row[5] = 'Vol %d #%d %s' % (int(row[5][4:8]), int(row[5][8:]), row[5][:4])
                    else:
                        row[5] = date_format(row[5], CONFIG['ISS_FORMAT'])

            loggerserverside.debug("get_comics returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_comics filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_comics: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      'loading': lazylibrarian.COMIC_UPDATE,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def comic_scan(self, **kwargs):
        logger = logging.getLogger(__name__)
        if 'comicid' in kwargs:
            comicid = kwargs['comicid']
        else:
            comicid = None

        if 'COMIC_SCAN' not in [n.name for n in [t for t in threading.enumerate()]]:
            try:
                if comicid:
                    threading.Thread(target=comicscan.comic_scan, name='COMIC_SCAN', args=[comicid]).start()
                else:
                    threading.Thread(target=comicscan.comic_scan, name='COMIC_SCAN', args=[]).start()
            except Exception as e:
                logger.error('Unable to complete the scan: %s %s' % (type(e).__name__, str(e)))
        else:
            logger.debug('COMIC_SCAN already running')
        if comicid:
            raise cherrypy.HTTPRedirect("comicissue_page?comicid=%s" % comicid)
        else:
            raise cherrypy.HTTPRedirect("comics")

    @cherrypy.expose
    def comicissue_page(self, comicid):
        global lastcomic
        db = database.DBConnection()
        try:
            mag_data = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
        finally:
            db.close()
        if not mag_data:
            raise cherrypy.HTTPError(404, "Comic ID %s not found" % comicid)
        title = mag_data['Title']
        if title and '&' in title and '&amp;' not in title:
            safetitle = title.replace('&', '&amp;')
        else:
            safetitle = title

        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            user = cookie['ll_uid'].value
        else:
            user = 0
        # use server-side processing
        if not CONFIG['TOGGLES'] and not CONFIG.get_bool('COMIC_IMG'):
            covercount = 0
        else:
            covercount = 1

        # if we've changed comic, reset to first page of new comics issues
        if comicid == lastcomic:
            firstpage = 'false'
        else:
            lastcomic = comicid
            firstpage = 'true'

        return serve_template(templatename="comicissues.html", comicid=comicid,
                              title=safetitle, issues=[], covercount=covercount, user=user,
                              firstpage=firstpage)

    @cherrypy.expose
    def open_comic(self, comicid=None, issueid=None):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            if comicid and '_' in comicid:
                comicid = comicid.split('_')[0]

            # we may want to open an issue with an issueid
            if comicid and issueid:
                cmd = 'SELECT Title,IssueFile from comics,comicissues WHERE comics.ComicID=comicissues.ComicID'
                cmd += ' and comics.ComicID=? and IssueID=?'
                iss_data = db.match(cmd, (comicid, issueid))
                if iss_data:
                    issue_file = iss_data["IssueFile"]
                    if issue_file and path_isfile(issue_file):
                        logger.debug('Opening file %s' % issue_file)
                        return self.send_file(issue_file, name="%s %s%s" %
                                              (iss_data["Title"], issueid, os.path.splitext(issue_file)[1]))

            # or we may just have a comicid to find comic in comicissues table
            cmd = 'SELECT Title,IssueFile,IssueID from comics,comicissues WHERE comics.ComicID=comicissues.ComicID'
            cmd += ' and comics.ComicID=?'
            iss_data = db.select(cmd, (comicid,))
        finally:
            db.close()
        if len(iss_data) == 0:
            logger.warning("No issues for comic %s" % comicid)
            raise cherrypy.HTTPRedirect("comics")

        if len(iss_data) == 1 and CONFIG.get_bool('COMIC_SINGLE'):  # we only have one issue, get it
            title = iss_data[0]["Title"]
            issue_id = iss_data[0]["IssueID"]
            issue_file = iss_data[0]["IssueFile"]
            if issue_file and path_isfile(issue_file):
                logger.debug('Opening %s - %s' % (comicid, issue_id))
                return self.send_file(issue_file, name="%s %s%s" % (title, issue_id, os.path.splitext(issue_file)[1]))
            else:
                logger.warning("No issue %s for comic %s" % (issue_id, title))
                raise cherrypy.HTTPError(404, "Comic Issue %s not found for %s" % (issue_id, title))

        else:  # multiple issues, show a list
            logger.debug("%s has %s %s" % (comicid, len(iss_data), plural(len(iss_data), "issue")))
            raise cherrypy.HTTPRedirect("comicissue_page?comicid=%s" % comicid)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_comic_issues(self, idisplay_start=0, idisplay_length=100, isort_col=0,
                         ssort_dir_0="desc", ssearch="", **kwargs):
        rows = []
        filtered = []
        rowlist = []
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')

        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)

            comicid = kwargs['comicid']
            db = database.DBConnection()
            try:
                mag_data = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
                title = mag_data['Title']
                rowlist = db.select('SELECT * from comicissues WHERE ComicID=? order by IssueID DESC', (comicid,))
            finally:
                db.close()
            if len(rowlist):
                newrowlist = []
                for mag in rowlist:
                    mag = dict(mag)  # turn sqlite objects into dicts
                    entry = [title, mag['Cover'], mag['IssueID'], mag['IssueAcquired'], "%s_%s" % (
                             comicid, mag['IssueID'])]
                    newrowlist.append(entry)  # add each rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in newrowlist if ssearch.lower() in str(x).lower()]
                else:
                    filtered = newrowlist

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                if sortcolumn in [2, 3]:  # dates
                    self.natural_sort(filtered, key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                      reverse=ssort_dir_0 == "desc")
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

            for row in rows:
                if not row[1] or not row[1].startswith('cache/'):
                    row[1] = 'images/nocover.jpg'
                else:
                    fname, extn = os.path.splitext(row[1])
                    imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                    if path_isfile(imgfile):
                        row[1] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                    else:
                        imgfile = os.path.join(DIRS.CACHEDIR, row[1][6:])
                        imgthumb = createthumb(imgfile, 200, False)
                        if imgthumb:
                            row[1] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
                row[3] = date_format(row[3], CONFIG['DATE_FORMAT'])
                row[2] = date_format(row[2], CONFIG['ISS_FORMAT'])

            loggerserverside.debug("get_comic_issues returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_comic_issues filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_comic_issues: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      'loading': lazylibrarian.COMIC_UPDATE,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def find_comic(self, title=None, **kwargs):
        # search for a comic title and produce a list of likely matches
        # noinspection PyGlobalUndefined
        global comicresults
        logger = logging.getLogger(__name__)
        comicresults = []
        if not title or title == 'None':
            raise cherrypy.HTTPRedirect("comics")
        else:
            title = replace_quotes_with(title, '')
            db = database.DBConnection()
            try:
                exists = db.match('SELECT Title from comics WHERE Title=?', (title,))
                if exists:
                    logger.debug("Comic %s already exists (%s)" % (title, exists['Title']))
                else:
                    cvres = cv_identify(title, best=False)
                    if title.startswith('CV'):
                        for item in cvres:
                            item['fuzz'] = fuzz.token_sort_ratio(title, item['seriesid'])
                            comicresults.append(item)
                    else:
                        cxres = cx_identify(title, best=False)
                        words = name_words(title)
                        titlewords = ' '.join(title_words(words))
                        for item in cvres:
                            item['fuzz'] = fuzz.token_sort_ratio(titlewords, item['title'])
                            comicresults.append(item)
                        for item in cxres:
                            item['fuzz'] = fuzz.token_sort_ratio(titlewords, item['title'])
                            comicresults.append(item)
                        comicresults = sorted(comicresults, key=lambda x: -(check_int(x["fuzz"], 0)))
                    comicids = db.select("SELECT ComicID from comics")
                    comiclist = []
                    for item in comicids:
                        comiclist.append(item['ComicID'])
                    return serve_template(templatename="comicresults.html", title="Comics",
                                          results=comicresults, comicids=comiclist)
            finally:
                db.close()

            if kwargs.get('comicfilter'):
                raise cherrypy.HTTPRedirect("comics?comic_filter=" + kwargs.get('comicfilter'))
            else:
                raise cherrypy.HTTPRedirect("comics")

    @cherrypy.expose
    def add_comic(self, comicid=None, **kwargs):
        # add a comic from a list in comicresults.html
        global comicresults
        logger = logging.getLogger(__name__)
        apikey = CONFIG['CV_APIKEY']
        if not comicid or comicid == 'None':
            raise cherrypy.HTTPRedirect("comics")
        elif comicid.startswith('CV') and not apikey:
            msg = "Please obtain an apikey from https://comicvine.gamespot.com/api/"
            logger.warning(msg)
            raise cherrypy.HTTPError(404, msg)

        else:
            db = database.DBConnection()
            try:
                exists = db.match('SELECT Title from comics WHERE ComicID=?', (comicid,))
                if exists:
                    logger.debug("Comic %s already exists (%s)" % (exists['Title'], exists['comicid']))
                else:
                    match = False
                    try:
                        for item in comicresults:
                            if item['seriesid'] == comicid:
                                aka = ''
                                akares = cv_identify(item['title'])
                                if not akares:
                                    akares = cx_identify(item['title'])
                                if akares and akares[3]['seriesid'] != comicid:
                                    aka = akares[3]['seriesid']
                                db.action('INSERT INTO comics (ComicID, Title, Status, Added, LastAcquired, ' +
                                          'Updated, LatestIssue, IssueStatus, LatestCover, SearchTerm, Start, ' +
                                          'First, Last, Publisher, Link, aka) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                          (comicid, item['title'], 'Active', now(), None,
                                           now(), None, 'Wanted', None, item['searchterm'], item['start'],
                                           item['first'], item['last'], item['publisher'], item['link'], aka))
                                match = True
                                break
                    except NameError:
                        match = False
                    if not match:
                        msg = "Failed to get data for %s" % comicid
                        logger.warning(msg)
                        raise cherrypy.HTTPError(404, msg)
            finally:
                db.close()
        if kwargs.get('comicfilter'):
            raise cherrypy.HTTPRedirect("comics?comic_filter=" + kwargs.get('comicfilter'))
        else:
            raise cherrypy.HTTPRedirect("comics")

    @cherrypy.expose
    def mark_comics(self, action=None, **args):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            args.pop('book_table_length', None)
            for item in args:
                if action == "Paused" or action == "Active":
                    control_value_dict = {"ComicID": item}
                    new_value_dict = {"Status": action}
                    db.upsert("comics", new_value_dict, control_value_dict)
                    logger.info('Status of comic %s changed to %s' % (item, action))
                if action == "Delete":
                    issues = db.select('SELECT IssueFile from comicissues WHERE ComicID=?', (item,))
                    logger.debug('Deleting comic %s from disc' % item)
                    issuedir = ''
                    for issue in issues:  # delete all issues of this comic
                        result = self.delete_issue(issue['IssueFile'])
                        if result:
                            logger.debug('Issue %s deleted from disc' % issue['IssueFile'])
                            issuedir = os.path.dirname(issue['IssueFile'])
                        else:
                            logger.debug('Failed to delete %s' % (issue['IssueFile']))

                    # if the directory is now empty, delete that too
                    if issuedir and CONFIG.get_bool('COMIC_DELFOLDER'):
                        magdir = os.path.dirname(issuedir)
                        try:
                            os.rmdir(syspath(magdir))
                            logger.debug('Comic directory %s deleted from disc' % magdir)
                        except OSError:
                            logger.debug('Comic directory %s is not empty' % magdir)
                        logger.info('Comic %s deleted from disc' % item)

                if action == "Remove" or action == "Delete":
                    db.action('DELETE from comics WHERE ComicID=?', (item,))
                    db.action('DELETE from wanted where BookID=?', (item,))
                    logger.info('Comic %s removed from database' % item)
                if action == "Reset":
                    control_value_dict = {"ComicID": item}
                    new_value_dict = {
                        "LastAcquired": '',
                        "LatestIssue": '',
                        "LatestCover": '',
                        "IssueStatus": "Wanted"
                    }
                    db.upsert("comics", new_value_dict, control_value_dict)
                    logger.info('Comic %s details reset' % item)

                if action == 'Subscribe':
                    cookie = cherrypy.request.cookie
                    if cookie and 'll_uid' in list(cookie.keys()):
                        userid = cookie['ll_uid'].value
                        res = db.match("SELECT * from subscribers WHERE UserID=? and Type=? and WantID=?",
                                       (userid, 'comic', item))
                        if res:
                            logger.debug("User %s is already subscribed to %s" % (userid, item))
                        else:
                            db.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                                      (userid, 'comic', item))
                            logger.debug("Subscribe %s to comic %s" % (userid, item))
                if action == 'Unsubscribe':
                    cookie = cherrypy.request.cookie
                    if cookie and 'll_uid' in list(cookie.keys()):
                        userid = cookie['ll_uid'].value
                        db.action('DELETE from subscribers WHERE UserID=? and Type=? and WantID=?',
                                  (userid, 'comic', item))
                        logger.debug("Unsubscribe %s to comic %s" % (userid, item))
        finally:
            db.close()

        raise cherrypy.HTTPRedirect("comics")

    @cherrypy.expose
    def mark_comic_issues(self, action=None, **args):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            args.pop('book_table_length', None)
            comicid = None
            for item in args:
                comicid, issueid = item.split('_')
                cmd = 'SELECT IssueFile,Title,comics.ComicID from comics,comicissues WHERE '
                cmd += 'comics.ComicID = comicissues.ComicID and comics.ComicID=? and IssueID=?'
                issue = db.match(cmd, (comicid, issueid))
                if issue:
                    if action == "Delete":
                        result = self.delete_issue(issue['IssueFile'])
                        if result:
                            logger.info('Issue %s of %s deleted from disc' % (issueid, issue['Title']))
                    if action == "Remove" or action == "Delete":
                        db.action('DELETE from comicissues WHERE ComicID=? and IssueID=?', (comicid, issueid))
                        logger.info('Issue %s of %s removed from database' % (issueid, issue['Title']))
                        # Set issuedate to issuedate of most recent issue we have
                        # Set latestcover to most recent issue cover
                        # Set lastacquired to acquired date of most recent issue we have
                        # Set added to acquired date of the earliest issue we have
                        cmd = 'select IssueID,IssueAcquired,IssueFile from comicissues where ComicID=?'
                        cmd += ' order by IssueID '
                        newest = db.match(cmd + 'DESC', (comicid,))
                        oldest = db.match(cmd + 'ASC', (comicid,))
                        control_value_dict = {'ComicID': comicid}
                        if newest and oldest:
                            old_acquired = ''
                            new_acquired = ''
                            cover = ''
                            issuefile = newest['IssueFile']
                            if path_exists(issuefile):
                                cover = os.path.splitext(issuefile)[0] + '.jpg'
                                mtime = os.path.getmtime(syspath(issuefile))
                                new_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))
                            issuefile = oldest['IssueFile']
                            if path_exists(issuefile):
                                mtime = os.path.getmtime(syspath(issuefile))
                                old_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))

                            new_value_dict = {
                                'LatestIssue': newest['IssueID'],
                                'LatestCover': cover,
                                'LastAcquired': new_acquired,
                                'Added': old_acquired
                            }
                        else:
                            new_value_dict = {
                                'LatestIssue': '',
                                'LastAcquired': '',
                                'LatestCover': '',
                                'Added': ''
                            }
                        db.upsert("comics", new_value_dict, control_value_dict)
        finally:
            db.close()
        if comicid:
            raise cherrypy.HTTPRedirect("comicissue_page?comicid=%s" % comicid)

        raise cherrypy.HTTPRedirect("comics")

    # MAGAZINES #########################################################

    # noinspection PyUnusedLocal
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_mags(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        rows = []
        filtered = []
        rowlist = []
        userid = None
        userprefs = 0
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            userid = cookie['ll_uid'].value
        if cookie and 'll_prefs' in list(cookie.keys()):
            userprefs = check_int(cookie['ll_prefs'].value, 0)
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)
            db = database.DBConnection()
            try:
                cmd = 'select magazines.*,(select count(*) as counter from issues where magazines.title = issues.title)'
                cmd += ' as Iss_Cnt from magazines'

                mymags = []
                if userid and userprefs & lazylibrarian.pref_mymags:
                    res = db.select('SELECT WantID from subscribers WHERE Type="magazine" and UserID=?', (userid,))
                    loggerserverside.debug("User subscribes to %s magazines" % len(res))
                    maglist = ''
                    for mag in res:
                        if maglist:
                            maglist += ', '
                        maglist += '"%s"' % mag['WantID']
                    cmd += ' WHERE Title in (' + maglist + ')'
                cmd += ' order by Title'

                loggerserverside.debug(cmd)
                rowlist = db.select(cmd)
            finally:
                db.close()

            if len(rowlist):
                newrowlist = []
                for mag in rowlist:
                    mag = dict(mag)  # turn sqlite objects into dicts
                    entry = [mag['Title'], mag['LatestCover'], mag['Title'], mag['Iss_Cnt'], mag['LastAcquired'],
                             mag['IssueDate'], mag['Status'], mag['IssueStatus'], mag['Genre']]

                    newrowlist.append(entry)  # add each rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in newrowlist if ssearch.lower() in str(x).lower()]
                else:
                    filtered = newrowlist

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                if sortcolumn in [4, 5]:  # dates
                    self.natural_sort(filtered, key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                      reverse=ssort_dir_0 == "desc")
                elif sortcolumn == 2:  # title
                    filtered.sort(key=lambda y: y[sortcolumn].lower() if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

                for row in rows:
                    row[4] = date_format(row[4], CONFIG['DATE_FORMAT'])
                    if row[5] and row[5].isdigit():
                        if len(row[5]) == 8:
                            if check_year(row[5][:4]):
                                row[5] = 'Issue %d %s' % (int(row[5][4:]), row[5][:4])
                            else:
                                row[5] = 'Vol %d #%d' % (int(row[5][:4]), int(row[5][4:]))
                        elif len(row[5]) == 12:
                            row[5] = 'Vol %d #%d %s' % (int(row[5][4:8]), int(row[5][8:]), row[5][:4])
                    else:
                        row[5] = date_format(row[5], CONFIG['ISS_FORMAT'])

                    if not row[1] or not row[1].startswith('cache/'):
                        row[1] = 'images/nocover.jpg'
                    else:
                        fname, extn = os.path.splitext(row[1])
                        imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                        if path_isfile(imgfile):
                            row[1] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                        else:
                            imgfile = os.path.join(DIRS.CACHEDIR, row[1][6:])
                            imgthumb = createthumb(imgfile, 200, False)
                            if imgthumb:
                                row[1] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
                    row[0] = quote_plus(make_utf8bytes(row[0])[0])

            loggerserverside.debug("get_mags returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_mags filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_mags: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      'loading': lazylibrarian.MAG_UPDATE,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def magazines(self, mag_filter=''):
        db = database.DBConnection()
        try:
            cookie = cherrypy.request.cookie
            if cookie and 'll_uid' in list(cookie.keys()):
                user = cookie['ll_uid'].value
                res = db.match('SELECT SendTo from users where UserID=?', (user,))
                if res and res['SendTo']:
                    email = res['SendTo']
        finally:
            db.close()
        # use server-side processing
        covers = 1
        if not CONFIG['TOGGLES'] and not CONFIG.get_bool('MAG_IMG'):
            covers = 0
        return serve_template(templatename="magazines.html", title="Magazines", magazines=[],
                              covercount=covers, user=user, email=email, mag_filter=mag_filter)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_issues(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        rows = []
        filtered = []
        rowlist = []
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)

            title = kwargs['title'].replace('&amp;', '&')
            db = database.DBConnection()
            try:
                rowlist = db.select('SELECT * from issues WHERE Title=? order by IssueDate DESC', (title,))
            finally:
                db.close()
            if len(rowlist):
                newrowlist = []
                for mag in rowlist:
                    mag = dict(mag)  # turn sqlite objects into dicts
                    entry = [mag['Title'], mag['Cover'], mag['IssueDate'], mag['IssueAcquired'],
                             mag['IssueID']]
                    newrowlist.append(entry)  # add each rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in newrowlist if ssearch.lower() in str(x).lower()]
                else:
                    filtered = newrowlist

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                if sortcolumn in [2, 3]:  # dates
                    self.natural_sort(filtered, key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                      reverse=ssort_dir_0 == "desc")
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

            for row in rows:
                if not row[1] or not row[1].startswith('cache/'):
                    row[1] = 'images/nocover.jpg'
                else:
                    fname, extn = os.path.splitext(row[1])
                    imgfile = os.path.join(DIRS.CACHEDIR, '%s_w200%s' % (fname[6:], extn))
                    if path_isfile(imgfile):
                        row[1] = "%s%s" % ('cache/', imgfile[len(DIRS.CACHEDIR):].lstrip(os.sep))
                    else:
                        imgfile = os.path.join(DIRS.CACHEDIR, row[1][6:])
                        imgthumb = createthumb(imgfile, 200, False)
                        if imgthumb:
                            row[1] = "%s%s" % ('cache/', imgthumb[len(DIRS.CACHEDIR):].lstrip(os.sep))
                row[3] = date_format(row[3], CONFIG['DATE_FORMAT'])
                if row[2] and row[2].isdigit():
                    if len(row[2]) == 8:
                        # Year/Issue or Volume/Issue with no year
                        if check_year(row[2][:4]):
                            row[2] = 'Issue %d %s' % (int(row[2][4:]), row[2][:4])
                        else:
                            row[2] = 'Vol %d #%d' % (int(row[2][:4]), int(row[2][4:]))
                    elif len(row[2]) == 12:
                        row[2] = 'Vol %d #%d %s' % (int(row[2][4:8]), int(row[2][8:]), row[2][:4])
                else:
                    row[2] = date_format(row[2], CONFIG['ISS_FORMAT'])

            loggerserverside.debug("get_issues returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_issues filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_issues: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      'loading': lazylibrarian.MAG_UPDATE,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def issue_page(self, title):
        global lastmagazine
        if title and '&' in title and '&amp;' not in title:  # could use htmlparser but seems overkill for just '&'
            safetitle = title.replace('&', '&amp;')
        else:
            safetitle = title

        # if we've changed magazine, reset to first page of new magazines issues
        if title == lastmagazine:
            firstpage = 'false'
        else:
            lastmagazine = title
            firstpage = 'true'

        # use server-side processing
        if not CONFIG['TOGGLES'] and not CONFIG.get_bool('MAG_IMG'):
            covercount = 0
        else:
            covercount = 1

        user = 0
        email = ''
        cookie = cherrypy.request.cookie
        if cookie and 'll_uid' in list(cookie.keys()):
            user = cookie['ll_uid'].value
            db = database.DBConnection()
            try:
                res = db.match('SELECT SendTo from users where UserID=?', (user,))
            finally:
                db.close()
            if res and res['SendTo']:
                email = res['SendTo']
        return serve_template(templatename="issues.html", title=safetitle, issues=[], covercount=covercount,
                              user=user, email=email, firstpage=firstpage)

    @cherrypy.expose
    def past_issues(self, mag=None, **kwargs):
        if not mag or mag == 'None':
            title = "Past Issues"
        else:
            title = mag
        which_status = kwargs.get('whichStatus', '')
        if not which_status or which_status == 'None':
            which_status = "Skipped"
        return serve_template(
            templatename="manageissues.html", title=title, issues=[], whichStatus=which_status, mag=mag)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_past_issues(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc",
                        ssearch="", **kwargs):
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        # kwargs is used by datatables to pass params
        rows = []
        filtered = []
        rowlist = []
        db = database.DBConnection()
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)
            # need to filter on whichStatus and optional mag title
            cmd = 'SELECT NZBurl, NZBtitle, NZBdate, Auxinfo, NZBprov from pastissues WHERE Status=?'
            args = [kwargs['whichStatus']]
            if 'mag' in kwargs and kwargs['mag'] != 'None':
                cmd += ' AND BookID=?'
                args.append(kwargs['mag'].replace('&amp;', '&'))

            loggerserverside.debug("get_past_issues %s: %s" % (cmd, str(args)))
            rowlist = db.select(cmd, tuple(args))
            if len(rowlist):
                for row in rowlist:  # iterate through the sqlite3.Row objects
                    entry = list(row)  # turn sqlite objects into lists
                    rows.append(entry)  # add the rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in rows if ssearch.lower() in str(x).lower()]
                else:
                    filtered = rows

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                              reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    rows = filtered
                else:
                    rows = filtered[displaystart:(displaystart + displaylength)]

                for row in rows:  # iterate through the sqlite3.Row objects
                    # title needs spaces for column resizing
                    title = row[1]
                    title = title.replace('.', ' ')
                    row[1] = title
                    # make this shorter and with spaces for column resizing
                    provider = row[4]
                    if len(provider) > 20:
                        while len(provider) > 20 and '/' in provider:
                            provider = provider.split('/', 1)[1]
                        provider = provider.replace('/', ' ')
                        row[4] = provider

            loggerserverside.debug("get_past_issues returning %s to %s" % (displaystart, displaystart + displaylength))
            loggerserverside.debug("get_past_issues filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_past_issues: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            db.close()
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def send_mag(self, bookid=None):
        return self.open_mag(bookid=bookid, email=True)

    @cherrypy.expose
    def open_mag(self, bookid=None, email=False):
        logger = logging.getLogger(__name__)
        bookid = unquote_plus(bookid)
        db = database.DBConnection()
        try:
            # we may want to open an issue with a hashed bookid
            mag_data = db.match('SELECT * from issues WHERE IssueID=?', (bookid,))
            if mag_data:
                issue_file = mag_data["IssueFile"]
                if issue_file and path_isfile(issue_file):
                    if email:
                        logger.debug('Emailing file %s' % issue_file)
                    else:
                        logger.debug('Opening file %s' % issue_file)
                    return self.send_file(issue_file, name="%s %s%s" %
                                          (mag_data["Title"], mag_data["IssueDate"],
                                           os.path.splitext(issue_file)[1]), email=email)

            # or we may just have a title to find magazine in issues table
            mag_data = db.select('SELECT * from issues WHERE Title=?', (bookid,))
        finally:
            db.close()
        # if len(mag_data) == 0:
        #    logger.warning("No issues for magazine %s" % bookid)
        #    raise cherrypy.HTTPRedirect("magazines")

        if len(mag_data) == 1 and CONFIG.get_bool('MAG_SINGLE'):  # we only have one issue, get it
            issue_date = mag_data[0]["IssueDate"]
            issue_file = mag_data[0]["IssueFile"]
            if issue_file and path_isfile(issue_file):
                if email:
                    logger.debug('Emailing %s - %s' % (bookid, issue_date))
                else:
                    logger.debug('Opening %s - %s' % (bookid, issue_date))
                return self.send_file(issue_file, name="%s %s%s" % (bookid, issue_date,
                                      os.path.splitext(issue_file)[1]), email=email)
            else:
                logger.warning("No issue %s for magazine %s" % (issue_date, bookid))
                raise cherrypy.HTTPRedirect("magazines")
        else:  # multiple issues, show a list
            logger.debug("%s has %s %s" % (bookid, len(mag_data), plural(len(mag_data), "issue")))
            raise cherrypy.HTTPRedirect("issue_page?title=%s" % quote_plus(make_utf8bytes(bookid)[0]))

    @cherrypy.expose
    def mark_past_issues(self, action=None, **args):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            maglist = []
            args.pop('book_table_length', None)

            for nzburl in args:
                nzburl = make_unicode(nzburl)
                # some NZBurl have &amp;  some have just & so need to try both forms
                if '&' in nzburl and '&amp;' not in nzburl:
                    nzburl2 = nzburl.replace('&', '&amp;')
                elif '&amp;' in nzburl:
                    nzburl2 = nzburl.replace('&amp;', '&')
                else:
                    nzburl2 = ''

                if not nzburl2:
                    title = db.select('SELECT * from pastissues WHERE NZBurl=?', (nzburl,))
                else:
                    title = db.select('SELECT * from pastissues WHERE NZBurl=? OR NZBurl=?', (nzburl, nzburl2))

                for item in title:
                    nzburl = item['NZBurl']
                    if action == 'Remove':
                        db.action('DELETE from pastissues WHERE NZBurl=?', (nzburl,))
                        logger.debug('Item %s removed from past issues' % item['NZBtitle'])
                        maglist.append({'nzburl': nzburl})
                    elif action == 'Wanted':
                        bookid = item['BookID']
                        nzbprov = item['NZBprov']
                        nzbtitle = item['NZBtitle']
                        nzbmode = item['NZBmode']
                        nzbsize = item['NZBsize']
                        auxinfo = item['AuxInfo']
                        maglist.append({
                            'bookid': bookid,
                            'nzbprov': nzbprov,
                            'nzbtitle': nzbtitle,
                            'nzburl': nzburl,
                            'nzbmode': nzbmode
                        })
                        # copy into wanted table
                        control_value_dict = {'NZBurl': nzburl}
                        new_value_dict = {
                            'BookID': bookid,
                            'NZBtitle': nzbtitle,
                            'NZBdate': now(),
                            'NZBprov': nzbprov,
                            'Status': action,
                            'NZBsize': nzbsize,
                            'AuxInfo': auxinfo,
                            'NZBmode': nzbmode
                        }
                        db.upsert("wanted", new_value_dict, control_value_dict)

                    elif action in ['Ignored', 'Skipped']:
                        db.action('UPDATE pastissues set status=? WHERE NZBurl=?', (action, nzburl))
                        logger.debug('Item %s marked %s in past issues' % (item['NZBtitle'], action))
                        maglist.append({'nzburl': nzburl})
        finally:
            db.close()

        if action == 'Remove':
            logger.info('Removed %s %s from past issues' % (len(maglist), plural(len(maglist), "item")))
        else:
            logger.info('Status set to %s for %s past %s' % (action, len(maglist), plural(len(maglist), "issue")))
        # start searchthreads
        if action == 'Wanted':
            threading.Thread(target=download_maglist, name='DL-MAGLIST', args=[maglist, 'wanted']).start()
        raise cherrypy.HTTPRedirect("past_issues")

    @cherrypy.expose
    def mark_issues(self, action=None, **args):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            title = ''
            args.pop('book_table_length', None)

            if action:
                for item in args:
                    issue = db.match('SELECT IssueFile,Title,IssueDate,Cover from issues WHERE IssueID=?', (item,))
                    if issue:
                        issue = dict(issue)
                        title = issue['Title']
                        if 'reCover' in action:
                            coverfile = create_mag_cover(issue['IssueFile'], refresh=True,
                                                         pagenum=check_int(action[-1], 1))
                            myhash = uuid.uuid4().hex
                            hashname = os.path.join(DIRS.CACHEDIR, 'magazine', '%s.jpg' % myhash)
                            copyfile(coverfile, hashname)
                            setperm(hashname)
                            control_value_dict = {"IssueFile": issue['IssueFile']}
                            newcover = 'cache/magazine/%s.jpg' % myhash
                            new_value_dict = {"Cover": newcover}
                            db.upsert("Issues", new_value_dict, control_value_dict)
                            latest = db.match("select LatestCover,IssueDate from magazines where title=?", (title,))
                            if latest:
                                if latest['IssueDate'] == issue['IssueDate'] and latest['LatestCover'] != newcover:
                                    db.action("UPDATE magazines SET LatestCover=? WHERE Title=?", (newcover, title))
                            issue['Cover'] = newcover
                            issue['CoverFile'] = coverfile  # for updating calibre cover
                            if CONFIG['IMP_CALIBREDB'] and CONFIG.get_bool('IMP_CALIBRE_MAGAZINE'):
                                self.update_calibre_issue_cover(issue)

                        if action == 'coverswap':
                            coverfile = None
                            if CONFIG['MAG_COVERSWAP']:
                                params = [CONFIG['MAG_COVERSWAP'], issue['IssueFile']]
                                logger.debug("Coverswap %s" % params)
                                try:
                                    res = subprocess.check_output(params, stderr=subprocess.STDOUT)
                                    logger.info(res)
                                    coverfile = create_mag_cover(issue['IssueFile'], refresh=True, pagenum=1)
                                except subprocess.CalledProcessError as e:
                                    logger.warning(e.output)
                            else:
                                res = coverswap(issue['IssueFile'])
                                if res:
                                    coverfile = create_mag_cover(issue['IssueFile'], refresh=True, pagenum=1)
                            if coverfile:
                                myhash = uuid.uuid4().hex
                                hashname = os.path.join(DIRS.CACHEDIR, 'magazine', '%s.jpg' % myhash)
                                copyfile(coverfile, hashname)
                                setperm(hashname)
                                control_value_dict = {"IssueFile": issue['IssueFile']}
                                newcover = 'cache/magazine/%s.jpg' % myhash
                                new_value_dict = {"Cover": newcover}
                                db.upsert("Issues", new_value_dict, control_value_dict)
                                latest = db.match("select LatestCover,IssueDate from magazines where title=?", (title,))
                                if latest:
                                    if latest['IssueDate'] == issue['IssueDate'] and latest['LatestCover'] != newcover:
                                        db.action("UPDATE magazines SET LatestCover=? WHERE Title=?", (newcover, title))
                                issue['Cover'] = newcover
                                issue['CoverFile'] = coverfile  # for updating calibre cover
                                if CONFIG['IMP_CALIBREDB'] and CONFIG.get_bool('IMP_CALIBRE_MAGAZINE'):
                                    self.update_calibre_issue_cover(issue)

                        if action == "Delete":
                            result = self.delete_issue(issue['IssueFile'])
                            if result:
                                logger.info('Issue %s of %s deleted from disc' % (issue['IssueDate'], issue['Title']))
                                if CONFIG['IMP_CALIBREDB'] and CONFIG.get_bool('IMP_CALIBRE_MAGAZINE'):
                                    self.delete_from_calibre(issue)
                        if action == "Remove" or action == "Delete":
                            db.action('DELETE from issues WHERE IssueID=?', (item,))
                            logger.info('Issue %s of %s removed from database' % (issue['IssueDate'], issue['Title']))
                            # Set magazine_issuedate to issuedate of most recent issue we have
                            # Set latestcover to most recent issue cover
                            # Set magazine_lastacquired to acquired date of most recent issue we have
                            # Set magazine_added to acquired date of the earliest issue we have
                            cmd = 'select IssueDate,IssueAcquired,IssueFile,Cover from issues where title=?'
                            cmd += ' order by IssueDate '
                            newest = db.match(cmd + 'DESC', (title,))
                            oldest = db.match(cmd + 'ASC', (title,))
                            control_value_dict = {'Title': title}
                            if newest and oldest:
                                old_acquired = ''
                                new_acquired = ''
                                cover = newest['Cover']
                                issuefile = newest['IssueFile']
                                if path_exists(issuefile):
                                    mtime = os.path.getmtime(syspath(issuefile))
                                    new_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))
                                issuefile = oldest['IssueFile']
                                if path_exists(issuefile):
                                    mtime = os.path.getmtime(syspath(issuefile))
                                    old_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))

                                new_value_dict = {
                                    'IssueDate': newest['IssueDate'],
                                    'LatestCover': cover,
                                    'LastAcquired': new_acquired,
                                    'MagazineAdded': old_acquired
                                }
                            else:
                                new_value_dict = {
                                    'IssueDate': '',
                                    'LastAcquired': '',
                                    'LatestCover': '',
                                    'MagazineAdded': ''
                                }
                            db.upsert("magazines", new_value_dict, control_value_dict)
        finally:
            db.close()
        if title:
            raise cherrypy.HTTPRedirect("issue_page?title=%s" % quote_plus(make_utf8bytes(title)[0]))
        else:
            raise cherrypy.HTTPRedirect("magazines")

    @staticmethod
    def delete_from_calibre(data):
        logger = logging.getLogger(__name__)
        calibre_id = get_calibre_id(data)
        if calibre_id:
            res, err, rc = calibredb('remove', [calibre_id])
            logger.debug("Delete result: %s [%s] %s" % (res, err, rc))

    @staticmethod
    def update_calibre_issue_cover(issue):
        logger = logging.getLogger(__name__)
        calibre_id = get_calibre_id(issue)
        if calibre_id:
            res, err, rc = calibredb('set_metadata', ['--field', 'cover:%s' % issue['CoverFile']], [calibre_id])
            logger.debug("Update result: %s [%s] %s" % (res, err, rc))

    @staticmethod
    def delete_issue(issuefile):
        logger = logging.getLogger(__name__)
        try:
            # delete the magazine file and any cover image / opf
            remove_file(issuefile)
            fname, extn = os.path.splitext(issuefile)
            for extn in ['.opf', '.jpg']:
                remove_file(fname + extn)
            # if the directory is now empty, delete that too
            if CONFIG.get_bool('MAG_DELFOLDER'):
                try:
                    os.rmdir(syspath(os.path.dirname(issuefile)))
                except OSError as e:
                    logger.debug('Directory %s not deleted: %s' % (os.path.dirname(issuefile), str(e)))
            return True
        except Exception as e:
            logger.warning('delete issue failed on %s, %s %s' % (issuefile, type(e).__name__, str(e)))
            return False

    @cherrypy.expose
    def mark_magazines(self, action=None, **args):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            args.pop('book_table_length', None)

            for item in args:
                title = make_unicode(unquote_plus(item))
                if action == "Paused" or action == "Active":
                    control_value_dict = {"Title": title}
                    new_value_dict = {"Status": action}
                    db.upsert("magazines", new_value_dict, control_value_dict)
                    logger.info('Status of magazine %s changed to %s' % (title, action))
                if action == "Delete":
                    issues = db.select('SELECT * from issues WHERE Title=?', (title,))
                    logger.debug('Deleting magazine %s from disc' % title)
                    issuedir = ''
                    for issue in issues:  # delete all issues of this magazine
                        result = self.delete_issue(issue['IssueFile'])
                        if result:
                            logger.debug('Issue %s deleted from disc' % issue['IssueFile'])
                            if CONFIG['IMP_CALIBREDB'] and CONFIG.get_bool('IMP_CALIBRE_MAGAZINE'):
                                self.delete_from_calibre(issue)
                            issuedir = os.path.dirname(issue['IssueFile'])
                        else:
                            logger.debug('Failed to delete %s' % (issue['IssueFile']))

                    # if the directory is now empty, delete that too
                    if issuedir and CONFIG.get_bool('MAG_DELFOLDER'):
                        magdir = os.path.dirname(issuedir)
                        try:
                            os.rmdir(syspath(magdir))
                            logger.debug('Magazine directory %s deleted from disc' % magdir)
                        except OSError:
                            logger.debug('Magazine directory %s is not empty' % magdir)
                        logger.info('Magazine %s deleted from disc' % title)

                if action == "Remove" or action == "Delete":
                    db.action('DELETE from magazines WHERE Title=?', (title,))
                    db.action('DELETE from pastissues WHERE BookID=?', (title,))
                    db.action('DELETE from wanted where BookID=?', (title,))
                    logger.info('Magazine %s removed from database' % title)
                elif action == "Reset":
                    control_value_dict = {"Title": title}
                    new_value_dict = {
                        "LastAcquired": '',
                        "IssueDate": '',
                        "LatestCover": '',
                        "IssueStatus": "Wanted"
                    }
                    db.upsert("magazines", new_value_dict, control_value_dict)
                    logger.info('Magazine %s details reset' % title)
                elif action == 'Subscribe':
                    cookie = cherrypy.request.cookie
                    if cookie and 'll_uid' in list(cookie.keys()):
                        userid = cookie['ll_uid'].value
                        res = db.match("SELECT * from subscribers WHERE UserID=? and Type=? and WantID=?",
                                       (userid, 'magazine', title))
                        if res:
                            logger.debug("User %s is already subscribed to %s" % (userid, title))
                        else:
                            db.action('INSERT into subscribers (UserID, Type, WantID) VALUES (?, ?, ?)',
                                      (userid, 'magazine', title))
                            logger.debug("Subscribe %s to magazine %s" % (userid, title))
                elif action == 'Unsubscribe':
                    cookie = cherrypy.request.cookie
                    if cookie and 'll_uid' in list(cookie.keys()):
                        userid = cookie['ll_uid'].value
                        db.action('DELETE from subscribers WHERE UserID=? and Type=? and WantID=?',
                                  (userid, 'magazine', title))
                        logger.debug("Unsubscribe %s to magazine %s" % (userid, title))
        finally:
            db.close()

        raise cherrypy.HTTPRedirect("magazines")

    @cherrypy.expose
    def search_for_mag(self, bookid=None):
        logger = logging.getLogger(__name__)
        bookid = unquote_plus(bookid)
        db = database.DBConnection()
        try:
            bookdata = db.match('SELECT * from magazines WHERE Title=? COLLATE NOCASE', (bookid,))
        finally:
            db.close()
        if bookdata:
            # start searchthreads
            mags = [{"bookid": bookdata['Title']}]
            self.start_magazine_search(mags)
            raise cherrypy.HTTPRedirect("magazines")
        else:
            logger.warning("Magazine %s was not found in the library" % bookid)
            raise cherrypy.HTTPError(404, "Magazine %s not found" % bookid)

    @cherrypy.expose
    def start_magazine_search(self, mags=None):
        logger = logging.getLogger(__name__)
        if mags:
            if CONFIG.use_any():
                threading.Thread(target=search_magazines, name='SEARCHMAG', args=[mags, False]).start()
                logger.debug("Searching for magazine with title: %s" % mags[0]["bookid"])
            else:
                logger.warning("Not searching for magazine, no download methods set, check config")
        else:
            logger.debug("MagazineSearch called with no magazines")

    @cherrypy.expose
    def add_magazine(self, title=None, **kwargs):
        logger = logging.getLogger(__name__)
        if not title or title == 'None':
            raise cherrypy.HTTPRedirect("magazines")
        else:
            db = database.DBConnection()
            try:
                reject = None
                if '~' in title:  # separate out the "reject words" list
                    reject = title.split('~', 1)[1].strip()
                    title = title.split('~', 1)[0].strip()

                # replace any non-ascii quotes/apostrophes with ascii ones eg "Collector's"
                title = replace_quotes_with(title, "'")
                title_exploded = title.split()
                # replace symbols by words
                new_title = []
                for word in title_exploded:
                    if word == '&':
                        word = 'and'
                    elif word == '+':
                        word = 'and'
                    new_title.append(word)
                title = ' '.join(new_title)
                exists = db.match('SELECT * from magazines WHERE Title=? COLLATE NOCASE', (title,))
                if exists:
                    logger.debug("Magazine %s already exists (%s)" % (title, exists['Title']))
                else:
                    # title = title.title()
                    control_value_dict = {"Title": title}
                    new_value_dict = {
                        "Regex": None,
                        "Reject": reject,
                        "Genre": "",
                        "DateType": "",
                        "Status": "Active",
                        "MagazineAdded": today(),
                        "IssueStatus": "Wanted"
                    }
                    db.upsert("magazines", new_value_dict, control_value_dict)
                    mags = [{"bookid": title}]
                    if CONFIG.get_bool('IMP_AUTOSEARCH'):
                        self.start_magazine_search(mags)
            finally:
                db.close()
            if kwargs.get('magfilter'):
                raise cherrypy.HTTPRedirect("magazines?mag_filter=" + kwargs.get('magfilter'))
            else:
                raise cherrypy.HTTPRedirect("magazines")

    # UPDATES ###########################################################

    @cherrypy.expose
    def check_for_updates(self):
        self.label_thread('UPDATES')
        versioncheck.check_for_updates()
        if CONFIG.get_int('COMMITS_BEHIND') == 0:
            if lazylibrarian.COMMIT_LIST:
                message = "unknown status"
                messages = lazylibrarian.COMMIT_LIST.replace('\n', '<br>')
                message = message + '<br><small>' + messages
            else:
                message = "up to date"
        elif CONFIG.get_int('COMMITS_BEHIND') > 0:
            message = "behind by %s %s" % (CONFIG.get_int('COMMITS_BEHIND'),
                                           plural(CONFIG.get_int('COMMITS_BEHIND'), "commit"))
            messages = lazylibrarian.COMMIT_LIST.replace('\n', '<br>')
            message = message + '<br><small>' + messages
            if '**MANUAL**' in lazylibrarian.COMMIT_LIST:
                message += "Update needs manual installation"
        else:
            message = "unknown version"
            messages = "Your version is not recognized at<br>https://%s/%s/%s  Branch: %s" % (
                CONFIG['GIT_HOST'], CONFIG['GIT_USER'],
                CONFIG['GIT_REPO'], CONFIG['GIT_BRANCH'])
            message = message + '<br><small>' + messages

        return "LazyLibrarian is %s" % message

    @cherrypy.expose
    def force_update(self):
        logger = logging.getLogger(__name__)
        if 'AAUPDATE' not in [n.name for n in [t for t in threading.enumerate()]]:
            threading.Thread(target=all_author_update, name='AAUPDATE', args=[False]).start()
        else:
            logger.debug('AAUPDATE already running')
        raise cherrypy.HTTPRedirect("home")

    @cherrypy.expose
    def update(self):
        logger = logging.getLogger(__name__)
        self.label_thread('UPDATING')
        logger.debug('(webServe-Update) - Performing update')
        lazylibrarian.SIGNAL = 'update'
        message = 'Updating...'
        icon = os.path.join(DIRS.CACHEDIR, 'alive.png')
        if path_isfile(icon):
            logger.debug("remove %s" % icon)
            remove_file(icon)
        return serve_template(templatename="shutdown.html", prefix='LazyLibrarian is ', title="Updating",
                              message=message, timer=90)

    # IMPORT/EXPORT #####################################################

    @cherrypy.expose
    def library_scan(self, **kwargs):
        logger = logging.getLogger(__name__)
        types = []
        if CONFIG.get_bool('EBOOK_TAB'):
            types.append('eBook')
        if CONFIG.get_bool('AUDIO_TAB'):
            types.append('AudioBook')
        if not types:
            raise cherrypy.HTTPRedirect('authors')
        library = types[0]
        if 'library' in kwargs and kwargs['library'] in types:
            library = kwargs['library']

        removed = CONFIG.get_bool('FULL_SCAN')
        threadname = "%s_SCAN" % library.upper()
        if threadname not in [n.name for n in [t for t in threading.enumerate()]]:
            try:
                threading.Thread(target=library_scan, name=threadname, args=[None, library, None, removed]).start()
            except Exception as e:
                logger.error('Unable to complete the scan: %s %s' % (type(e).__name__, str(e)))
        else:
            logger.debug('%s already running' % threadname)
        if library == 'AudioBook':
            raise cherrypy.HTTPRedirect("audio")
        raise cherrypy.HTTPRedirect("books")

    @cherrypy.expose
    def magazine_scan(self, **kwargs):
        logger = logging.getLogger(__name__)
        if 'title' in kwargs:
            title = kwargs['title']
            title = title.replace('&amp;', '&')
        else:
            title = ''

        if 'MAGAZINE_SCAN' not in [n.name for n in [t for t in threading.enumerate()]]:
            try:
                if title:
                    threading.Thread(target=magazinescan.magazine_scan, name='MAGAZINE_SCAN', args=[title]).start()
                else:
                    threading.Thread(target=magazinescan.magazine_scan, name='MAGAZINE_SCAN', args=[]).start()
            except Exception as e:
                logger.error('Unable to complete the scan: %s %s' % (type(e).__name__, str(e)))
        else:
            logger.debug('MAGAZINE_SCAN already running')
        if title:
            raise cherrypy.HTTPRedirect("issue_page?title=%s" % quote_plus(make_utf8bytes(title)[0]))
        else:
            raise cherrypy.HTTPRedirect("magazines")

    @cherrypy.expose
    def include_alternate(self, library='eBook'):
        logger = logging.getLogger(__name__)
        if 'ALT-LIBRARYSCAN' not in [n.name for n in [t for t in threading.enumerate()]]:
            try:
                threading.Thread(target=library_scan, name='ALT-LIBRARYSCAN',
                                 args=[CONFIG['ALTERNATE_DIR'], library, None, False]).start()
            except Exception as e:
                logger.error('Unable to complete the libraryscan: %s %s' % (type(e).__name__, str(e)))
        else:
            logger.debug('ALT-LIBRARYSCAN already running')
        raise cherrypy.HTTPRedirect("manage?library=%s" % library)

    @cherrypy.expose
    def import_issues(self, title=None):
        logger = logging.getLogger(__name__)
        if not title:
            logger.error("No title to import")
            raise cherrypy.HTTPRedirect("magazines")
        if 'IMPORTISSUES' not in [n.name for n in [t for t in threading.enumerate()]]:
            try:
                threading.Thread(target=process_issues, name='IMPORTISSUES',
                                 args=[CONFIG['ALTERNATE_DIR'], title]).start()
            except Exception as e:
                logger.error('Unable to complete the import: %s %s' % (type(e).__name__, str(e)))
        else:
            logger.debug('IMPORTISSUES already running')
        raise cherrypy.HTTPRedirect("issue_page?title=%s" % title)

    @cherrypy.expose
    def import_alternate(self, library='eBook'):
        logger = logging.getLogger(__name__)
        if 'IMPORTALT' not in [n.name for n in [t for t in threading.enumerate()]]:
            try:
                threading.Thread(target=process_alternate, name='IMPORTALT',
                                 args=[CONFIG['ALTERNATE_DIR'], library, True]).start()
            except Exception as e:
                logger.error('Unable to complete the import: %s %s' % (type(e).__name__, str(e)))
        else:
            logger.debug('IMPORTALT already running')
        raise cherrypy.HTTPRedirect("manage?library=%s" % library)

    @cherrypy.expose
    def rss_feed(self, **kwargs):
        logger = logging.getLogger(__name__)
        self.label_thread('RSSFEED')
        if 'type' in kwargs:
            ftype = kwargs['type']
        else:
            return

        if 'limit' in kwargs:
            limit = kwargs['limit']
        else:
            limit = '10'

        if 'authorid' in kwargs:
            authorid = kwargs['authorid']
        else:
            authorid = None

        if 'onetitle' in kwargs:
            onetitle = kwargs['onetitle']
        else:
            onetitle = None

        # url might end in .xml
        if not limit.isdigit():
            try:
                limit = int(limit.split('.')[0])
            except (IndexError, ValueError):
                limit = 10

        userid = 0
        if 'user' in kwargs:
            userid = kwargs['user']
        else:
            cookie = cherrypy.request.cookie
            if cookie and 'll_uid' in list(cookie.keys()):
                userid = cookie['ll_uid'].value

        scheme, netloc, path, qs, anchor = urlsplit(cherrypy.url())

        my_ip = CONFIG['RSS_HOST']
        if not my_ip:
            my_ip = cherrypy.request.headers.get('X-Forwarded-Host')
        if not my_ip:
            my_ip = cherrypy.request.headers.get('Host')
        if not my_ip:
            my_ip = netloc
        path = path.replace('rss_feed', '').rstrip('/')

        baseurl = urlunsplit((scheme, my_ip, path, qs, anchor))

        remote_ip = cherrypy.request.headers.get('X-Forwarded-For')  # apache2
        if not remote_ip:
            remote_ip = cherrypy.request.headers.get('X-Host')  # lighthttpd
        if not remote_ip:
            remote_ip = cherrypy.request.headers.get('Remote-Addr')
        if not remote_ip:
            remote_ip = cherrypy.request.remote.ip
        remote_ip = remote_ip.split(',')[0]

        if onetitle:
            filename = 'LazyLibrarian_RSS_%s.xml' % unquote_plus(onetitle).replace('&amp;', '&')
        else:
            filename = 'LazyLibrarian_RSS_%s.xml' % ftype
        logger.debug("rss Feed request %s %s%s: %s %s" % (limit, ftype, plural(limit), remote_ip, userid))
        cherrypy.response.headers["Content-Type"] = 'application/rss+xml'
        cherrypy.response.headers["Content-Disposition"] = 'attachment; filename="%s"' % filename
        res = gen_feed(ftype, limit=limit, user=userid, baseurl=baseurl, authorid=authorid, onetitle=onetitle)
        return res.encode('UTF-8')

    @cherrypy.expose
    def import_csv(self, library=''):
        logger = logging.getLogger(__name__)
        if 'IMPORTCSV' not in [n.name for n in [t for t in threading.enumerate()]]:
            self.label_thread('IMPORTCSV')
            try:
                csvfile = csv_file(CONFIG['ALTERNATE_DIR'], library=library)
                if path_exists(csvfile):
                    message = "Importing books (background task) from %s" % csvfile
                    threading.Thread(target=import_csv, name='IMPORTCSV',
                                     args=[CONFIG['ALTERNATE_DIR'], library]).start()
                else:
                    message = "No %s CSV file in [%s]" % (library, CONFIG['ALTERNATE_DIR'])
            except Exception as e:
                message = 'Unable to complete the import: %s %s' % (type(e).__name__, str(e))
                logger.error(message)
        else:
            message = 'IMPORTCSV already running'
            logger.debug(message)
        return message

    @cherrypy.expose
    def export_csv(self, library=''):
        self.label_thread('EXPORTCSV')
        message = export_csv(CONFIG['ALTERNATE_DIR'], library=library)
        message = message.replace('\n', '<br>')
        return message

    # JOB CONTROL #######################################################

    @cherrypy.expose
    def shutdown(self):
        self.label_thread('SHUTDOWN')
        logger = logging.getLogger(__name__)
        # lazylibrarian.config_write()
        lazylibrarian.SIGNAL = 'shutdown'
        message = 'closing ...'
        icon = os.path.join(DIRS.CACHEDIR, 'alive.png')
        if path_isfile(icon):
            logger.debug("remove %s" % icon)
            remove_file(icon)
        return serve_template(templatename="shutdown.html", prefix='LazyLibrarian is ', title="Close library",
                              message=message, timer=30)

    @cherrypy.expose
    def restart(self):
        self.label_thread('RESTART')
        logger = logging.getLogger(__name__)
        lazylibrarian.SIGNAL = 'restart'
        message = 'reopening ...'
        icon = os.path.join(DIRS.CACHEDIR, 'alive.png')
        if path_isfile(icon):
            logger.debug("remove %s" % icon)
            remove_file(icon)
        return serve_template(templatename="shutdown.html", prefix='LazyLibrarian is ', title="Reopen library",
                              message=message, timer=50)

    @cherrypy.expose
    def show_jobs(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        # show the current status of LL cron jobs
        resultlist = show_jobs()
        result = ''
        for line in resultlist:
            result = result + line + '\n'
        return result

    @cherrypy.expose
    def show_apprise(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        logger = logging.getLogger(__name__)
        # show the available notifiers
        apprise_list = lazylibrarian.notifiers.apprise_notify.AppriseNotifier.notify_types()
        result = ''
        results = []
        try:
            for entry in apprise_list:
                if isinstance(entry, str):
                    results.append(entry)
            results.sort(key=str.casefold)
            result = "\n".join(results)
        except Exception as e:
            logger.debug(str(e))
        return result

    @cherrypy.expose
    def show_stats(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        # show some database status info
        resultlist = show_stats()
        result = ''
        for line in resultlist:
            result = result + line + '\n'
        return result

    @cherrypy.expose
    def restart_jobs(self):
        restart_jobs(command=SchedulerCommand.RESTART)
        # return self.show_jobs()

    @cherrypy.expose
    def stop_jobs(self):
        restart_jobs(command=SchedulerCommand.STOP)
        # return self.show_jobs()

    # LOGGING ###########################################################

    @cherrypy.expose
    def clear_log(self):
        logger = logging.getLogger(__name__)
        LOGCONFIG.clear_ui_log()
        logger.info('Screen log cleared')
        raise cherrypy.HTTPRedirect("logs")

    @cherrypy.expose
    def toggle_detailed_logs(self):
        detail = CONFIG.get_bool('DETAILEDUILOG')
        CONFIG.set_bool('DETAILEDUILOG', not detail)
        raise cherrypy.HTTPRedirect("logs")

    @cherrypy.expose
    def delete_logs(self):
        logger = logging.getLogger(__name__)
        result = LOGCONFIG.delete_log_files((CONFIG['LOGDIR']))
        logger.info(result)
        raise cherrypy.HTTPRedirect("logs")

    @cherrypy.expose
    def get_support_zip(self):
        # Save the redacted log and config to a zipfile
        self.label_thread('SAVELOG')
        logger = logging.getLogger(__name__)
        msg, zipfile = create_support_zip()
        logger.info(msg)
        return cherrypy.lib.static.serve_file(zipfile, 'application/x-download', 'attachment', os.path.basename(zipfile))
        # raise cherrypy.HTTPRedirect("logs")

    @cherrypy.expose
    def log_header(self):
        # Return the log header info
        result = log_header()
        return result

    @cherrypy.expose
    def logs(self):
        return serve_template(templatename="logs.html", title="Log", lineList=[])  # lazylibrarian.LOGLIST)

    # noinspection PyUnusedLocal
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_log(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        # kwargs is used by datatables to pass params
        logger = logging.getLogger(__name__)
        rows = filtered = []
        total = 0

        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)

            filtered, total = LOGCONFIG.get_ui_logrows(ssearch)

            sortcolumn = int(isort_col)
            filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                          reverse=ssort_dir_0 == "desc")
            rows = filtered if displaylength < 0 else filtered[displaystart:(displaystart + displaylength)]
        except Exception:
            logger.error('Unhandled exception in get_log: %s' % traceback.format_exc())
            rows = filtered = []
        finally:
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': total,
                      'aaData': rows,
                      }
            return mydict

    # HISTORY ###########################################################

    @cherrypy.expose
    def history(self):
        return serve_template(templatename="history.html", title="History", history=[])

    # noinspection PyUnusedLocal
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_history(self, idisplay_start=0, idisplay_length=100, isort_col=0, ssort_dir_0="desc", ssearch="", **kwargs):
        rows = []
        filtered = []
        rowlist = []
        self.label_thread('WEBSERVER')
        logger = logging.getLogger(__name__)
        loggerserverside = logging.getLogger('special.serverside')
        db = database.DBConnection()
        # noinspection PyBroadException
        try:
            displaystart = int(idisplay_start)
            displaylength = int(idisplay_length)
            CONFIG.set_int('DISPLAYLENGTH', displaylength)
            snatching = 0
            cmd = "SELECT NZBTitle,AuxInfo,BookID,NZBProv,NZBDate,NZBSize,Status,Source,DownloadID,rowid from wanted"
            rowlist = db.select(cmd)
            # turn the sqlite rowlist into a list of dicts
            if len(rowlist):
                # the masterlist to be filled with the row data
                for row in rowlist:  # iterate through the sqlite3.Row objects
                    entry = list(row)  # turn sqlite objects into lists
                    rows.append(entry)  # add the rowlist to the masterlist

                if ssearch:
                    loggerserverside.debug("filter %s" % ssearch)
                    filtered = [x for x in rows if ssearch.lower() in str(x).lower()]
                else:
                    filtered = rows

                sortcolumn = int(isort_col)
                loggerserverside.debug("sortcolumn %d" % sortcolumn)

                # use rowid to get most recently added first (monitoring progress)
                if sortcolumn == 6:
                    sortcolumn = 9

                if sortcolumn == 5:
                    self.natural_sort(filtered, key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                      reverse=ssort_dir_0 == "desc")
                else:
                    filtered.sort(key=lambda y: y[sortcolumn] if y[sortcolumn] is not None else '',
                                  reverse=ssort_dir_0 == "desc")

                if displaylength < 0:  # display = all
                    nrows = filtered
                else:
                    nrows = filtered[displaystart:(displaystart + displaylength)]

                lazylibrarian.HIST_REFRESH = 0
                rows = []
                for row in nrows:
                    row = list(row)
                    row[8] = make_unicode(row[8])  # delugerpc returns bytes
                    # title needs spaces, not dots, for column resizing
                    title = row[0]  # type: str
                    if title:
                        title = title.replace('.', ' ')
                        row[0] = title
                    # provider name needs to be shorter and with spaces for column resizing
                    if row[3]:
                        row[3] = CONFIG.disp_name(row[3].strip('/'))
                    # separate out rowid and other additions, so we don't break legacy interface
                    rowid = row[9]
                    row = row[:9]
                    if row[6] == 'Snatched':
                        snatching += 1
                        if snatching <= 5:
                            progress, _ = get_download_progress(row[7], row[8])
                            row.append(progress)
                            if progress < 100:
                                lazylibrarian.HIST_REFRESH = CONFIG.get_int('HIST_REFRESH')
                        else:
                            row.append(-1)
                    else:
                        row.append(-1)
                    row.append(rowid)
                    row.append(row[4])  # keep full datetime for tooltip
                    row[4] = date_format(row[4], CONFIG['DATE_FORMAT'])

                    if row[1] in ['eBook', 'AudioBook']:
                        btn = '<button onclick="bookinfo(\'' + row[2]
                        btn += '\')" class="button btn-link text-left" type="button" '
                        btn += '>' + row[1] + '</button>'
                        row[1] = btn
                        auth = db.match('SELECT authorid from books where bookid=?', (row[2],))
                        if auth:
                            # noinspection PyBroadException
                            try:
                                btn = '<a href=\'author_page?authorid='
                                btn += auth['authorid']
                                btn += '\'">' + row[2] + '</a>'
                                row[2] = btn
                            except Exception:
                                logger.debug("Unexpected authorid [%s]" % repr(auth))
                    elif row[1] == 'comic':
                        btn = '<a href=\'open_comic?comicid=' + row[2].split('_')[0] + '\'">' + row[2] + '</a>'
                        row[2] = btn
                    else:
                        # noinspection PyBroadException
                        try:
                            if row[1] and re.match(r"^[0-9.-]+$", row[1]) is not None:  # Magazine
                                safetitle = quote_plus(make_utf8bytes(row[2])[0])
                                btn = '<a href=\'open_mag?bookid=' + safetitle + '\'">' + row[2] + '</a>'
                                row[2] = btn
                        except Exception:
                            logger.debug("Unexpected auxinfo [%s] %s" % (row[1], row[2]))
                            continue
                    rows.append(row)

            loggerserverside.debug("get_history returning %s to %s, snatching %s" %
                                   (displaystart, displaystart + displaylength, snatching))
            loggerserverside.debug("get_history filtered %s from %s:%s" % (len(filtered), len(rowlist), len(rows)))
        except Exception:
            logger.error('Unhandled exception in get_history: %s' % traceback.format_exc())
            rows = []
            rowlist = []
            filtered = []
        finally:
            db.close()
            mydict = {'iTotalDisplayRecords': len(filtered),
                      'iTotalRecords': len(rowlist),
                      'aaData': rows,
                      }
            loggerserverside.debug(str(mydict))
            return mydict

    @cherrypy.expose
    def bookdesc(self, bookid=None):
        # noinspection PyGlobalUndefined
        global lastauthor
        img = None
        title = None
        text = None
        if bookid:
            db = database.DBConnection()
            try:
                if bookid.startswith('A_'):
                    cmd = "SELECT AuthorName,About,AuthorImg from authors WHERE authorid=?"
                    res = db.match(cmd, (bookid[2:],))
                elif bookid.startswith('CV') or bookid.startswith('CX'):
                    try:
                        comicid, issueid = bookid.split('_')
                        cmd = "SELECT Title as BookName,comicissues.Description as BookDesc,Cover as BookImg,"
                        cmd += "Contributors from comics,comicissues where "
                        cmd += "comics.comicid = comicissues.comicid and comics.comicid=? and issueid=?"
                        res = db.match(cmd, (comicid, issueid))
                    except ValueError:
                        cmd = "SELECT Title as BookName,Description as BookDesc,LatestCover as BookImg"
                        cmd += " from comics where comicid=?"
                        res = db.match(cmd, (bookid,))
                else:
                    cmd = "SELECT BookName,BookDesc,BookImg,AuthorID from books WHERE bookid=?"
                    res = db.match(cmd, (bookid,))
                if res:
                    res = dict(res)
                    text = res.get('BookDesc')
                    if not text:
                        text = res.get('About')
                    contributors = res.get('Contributors')
                    if contributors:
                        text += '<br><br>' + contributors
                    img = res.get('BookImg')
                    if not img:
                        img = res.get('AuthorImg')
                    title = res.get('BookName')
                    if not title:
                        title = res.get('AuthorName')
                    if 'AuthorID' in res:
                        lastauthor = res['AuthorID']
            finally:
                db.close()
        if not img:
            img = 'images/nocover.jpg'
        if not title:
            title = 'BookID not found'
        if not text:
            text = 'No Description'
        return img + '^' + title + '^' + text

    @cherrypy.expose
    def dlinfo(self, target=None):
        if '^' not in target:
            return ''
        db = database.DBConnection()
        try:
            status, rowid = target.split('^')
            if status == 'Ignored':
                match = db.match('select ScanResult from books WHERE bookid=?', (rowid,))
                message = 'Reason: %s<br>' % match['ScanResult']
            else:
                cmd = 'select NZBurl,NZBtitle,NZBdate,NZBprov,Status,NZBsize,AuxInfo,NZBmode,DLResult,Source,DownloadID '
                cmd += 'from wanted where rowid=?'
                match = db.match(cmd, (rowid,))
                dltype = match['AuxInfo']
                if dltype not in ['eBook', 'AudioBook']:
                    if not dltype:
                        dltype = 'eBook'
                    else:
                        dltype = 'Magazine'
                message = "Title: %s<br>" % match['NZBtitle']
                message += "Type: %s %s<br>" % (match['NZBmode'], dltype)
                message += "Date: %s<br>" % match['NZBdate']
                message += "Size: %s Mb<br>" % match['NZBsize']
                message += "Provider: %s<br>" % CONFIG.disp_name(match['NZBprov'])
                message += "Downloader: %s<br>" % match['Source']
                message += "DownloadID: %s<br>" % match['DownloadID']
                message += "URL: %s<br>" % match['NZBurl']
                if status == 'Processed':
                    message += "File: %s<br>" % match['DLResult']
                elif status == 'Seeding':
                    message += status
                else:
                    message += "Error: %s<br>" % match['DLResult']
        finally:
            db.close()
        return message

    @cherrypy.expose
    def deletehistory(self, rowid=None):
        logger = logging.getLogger(__name__)
        if not rowid:
            logger.warning("No rowid in deletehistory")
        else:
            db = database.DBConnection()
            try:
                match = db.match('SELECT NZBtitle,Status from wanted WHERE rowid=?', (rowid,))
                if match:
                    logger.debug('Deleting %s history item %s' % (match['Status'], match['NZBtitle']))
                    db.action('DELETE from wanted WHERE rowid=?', (rowid,))
                else:
                    logger.warning("No rowid %s in history" % rowid)
            finally:
                db.close()

    @cherrypy.expose
    def markhistory(self, rowid=None):
        logger = logging.getLogger(__name__)
        if not rowid:
            return
        db = database.DBConnection()
        try:
            match = db.match('SELECT NZBtitle,Status,BookID,AuxInfo from wanted WHERE rowid=?', (rowid,))
            logger.debug('Marking %s history item %s as Failed' % (match['Status'], match['NZBtitle']))
            db.action('UPDATE wanted SET Status="Failed" WHERE rowid=?', (rowid,))
            book_type = match['AuxInfo']
            if book_type not in ['AudioBook', 'eBook']:
                if not book_type:
                    book_type = 'eBook'
                else:
                    book_type = 'Magazine'
            if book_type == 'AudioBook':
                db.action('UPDATE books SET audiostatus="Wanted" WHERE BookID=?', (match['BookID'],))
            else:
                db.action('UPDATE books SET status="Wanted" WHERE BookID=?', (match['BookID'],))
        finally:
            db.close()

    @cherrypy.expose
    def clearhistory(self, status=None):
        logger = logging.getLogger(__name__)
        db = database.DBConnection()
        try:
            if not status or status == 'all':
                logger.info("Clearing all history")
                # also reset the Snatched status in book table to Wanted and cancel any failed download task
                # ONLY reset if status is still Snatched, as maybe a later task succeeded
                status = "Snatched"
                cmd = 'SELECT BookID,AuxInfo,Source,DownloadID from wanted WHERE Status=?'
                rowlist = db.select(cmd, (status,))
                for book in rowlist:
                    if book['BookID'] != 'unknown':
                        if book['AuxInfo'] == 'eBook':
                            db.action('UPDATE books SET Status="Wanted" WHERE Bookid=? AND Status=?',
                                      (book['BookID'], status))
                        elif book['AuxInfo'] == 'AudioBook':
                            db.action('UPDATE books SET AudioStatus="Wanted" WHERE Bookid=? AND AudioStatus=?',
                                      (book['BookID'], status))
                        if CONFIG.get_bool('DEL_FAILED'):
                            delete_task(book['Source'], book['DownloadID'], True)
                db.action("DELETE from wanted")
            else:
                logger.info("Clearing history where status is %s" % status)
                if status == 'Snatched':
                    # also reset the Snatched status in book table to Wanted and cancel any failed download task
                    # ONLY reset if status is still Snatched, as maybe a later task succeeded
                    cmd = 'SELECT BookID,AuxInfo,Source,DownloadID from wanted WHERE Status=?'
                    rowlist = db.select(cmd, (status,))
                    for book in rowlist:
                        if book['BookID'] != 'unknown':
                            if book['AuxInfo'] == 'eBook':
                                db.action('UPDATE books SET Status="Wanted" WHERE Bookid=? AND Status=?',
                                          (book['BookID'], status))
                            elif book['AuxInfo'] == 'AudioBook':
                                db.action('UPDATE books SET AudioStatus="Wanted" WHERE Bookid=? AND AudioStatus=?',
                                          (book['BookID'], status))
                        if CONFIG.get_bool('DEL_FAILED'):
                            delete_task(book['Source'], book['DownloadID'], True)
                db.action('DELETE from wanted WHERE Status=?', (status,))
        finally:
            db.close()
        raise cherrypy.HTTPRedirect("history")

    @cherrypy.expose
    def testprovider(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("TESTPROVIDER")
        if 'name' in kwargs and kwargs['name']:
            host = ''
            api = ''
            if 'host' in kwargs and kwargs['host']:
                host = kwargs['host']
            if 'api' in kwargs and kwargs['api']:
                api = kwargs['api']
            result, name = test_provider(kwargs['name'], host=host, api=api)
            if result is False:
                msg = "%s test FAILED, check debug log" % name
            elif result is True:
                msg = "%s test PASSED" % name
                CONFIG.save_config_and_backup_old(section=kwargs['name'])
            else:
                msg = "%s test PASSED, found %s" % (name, result)
                CONFIG.save_config_and_backup_old(section=kwargs['name'])
        else:
            msg = "Invalid or missing name in testprovider"
        return msg

    @cherrypy.expose
    def clearblocked(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        logger = logging.getLogger(__name__)
        # clear any currently blocked providers
        num = BLOCKHANDLER.clear_all()
        result = 'Cleared %s blocked %s' % (num, plural(num, "provider"))
        logger.debug(result)
        return result

    @cherrypy.expose
    def showblocked(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        logger = logging.getLogger(__name__)
        # show any currently blocked providers
        result = BLOCKHANDLER.get_text_list_of_blocks()
        logger.debug(result)
        return result

    @cherrypy.expose
    def cleardownloads(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        logger = logging.getLogger(__name__)
        # clear download counters
        db = database.DBConnection()
        try:
            count = db.match('SELECT COUNT(*) as counter FROM downloads')
            if count:
                num = count['counter']
            else:
                num = 0
            result = 'Deleted download counter for %s %s' % (num, plural(num, "provider"))
            db.action('DELETE from downloads')
        finally:
            db.close()
        logger.debug(result)
        return result

    @cherrypy.expose
    def showdownloads(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        # show provider download totals
        result = ''
        db = database.DBConnection()
        try:
            downloads = db.select('SELECT Count,Provider FROM downloads ORDER BY Count DESC')
        finally:
            db.close()
        for line in downloads:
            provname = CONFIG.disp_name(line['Provider'].strip('/'))
            new_entry = "%4d - %s\n" % (line['Count'], provname)
            result += new_entry

        if result == '':
            result = 'No downloads'
        return result

    @cherrypy.expose
    def sync_to_calibre(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        if 'CalSync' in [n.name for n in [t for t in threading.enumerate()]]:
            msg = 'Calibre Sync is already running'
        else:
            self.label_thread('CalSync')
            cookie = cherrypy.request.cookie
            if cookie and 'll_uid' in list(cookie.keys()):
                userid = cookie['ll_uid'].value
                msg = sync_calibre_list(userid=userid)
                self.label_thread('WEBSERVER')
            else:
                msg = "No userid found"
        return msg

    @cherrypy.expose
    def sync_to_goodreads(self):
        if 'GRSync' not in [n.name for n in [t for t in threading.enumerate()]]:
            cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
            self.label_thread('GRSync')
            msg = grsync.sync_to_gr()
            self.label_thread('WEBSERVER')
        else:
            msg = 'Goodreads Sync is already running'
        return msg

    @cherrypy.expose
    def grauth_step1(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        if 'gr_api' in kwargs:
            CONFIG.set_str('GR_API', kwargs['gr_api'])
        if 'gr_secret' in kwargs:
            CONFIG.set_str('GR_SECRET', kwargs['gr_secret'])
        ga = grsync.GrAuth()
        res = ga.goodreads_oauth1()
        return res

    @cherrypy.expose
    def grauth_step2(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        ga = grsync.GrAuth()
        return ga.goodreads_oauth2()

    @cherrypy.expose
    def test_gr_auth(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'gr_api' in kwargs:
            CONFIG.set_str('GR_API', kwargs['gr_api'])
        if 'gr_secret' in kwargs:
            CONFIG.set_str('GR_SECRET', kwargs['gr_secret'])
        if 'gr_oauth_token' in kwargs:
            CONFIG.set_str('GR_OAUTH_TOKEN', kwargs['gr_oauth_token'])
        if 'gr_oauth_secret' in kwargs:
            CONFIG.set_str('GR_OAUTH_SECRET', kwargs['gr_oauth_secret'])
        res = grsync.test_auth()
        if res.startswith('Pass:'):
            CONFIG.save_config_and_backup_old(section='API')
        return res

    # NOTIFIERS #########################################################

    @cherrypy.expose
    def twitter_step1(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        return notifiers.twitter_notifier._get_authorization()

    @cherrypy.expose
    def twitter_step2(self, key):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        if key:
            result = notifiers.twitter_notifier._get_credentials(key)
            if result:
                CONFIG.save_config_and_backup_old(section='Twitter')
                return "Key verification successful"
            else:
                return "Unable to verify key"
        else:
            return "No Key provided"

    @cherrypy.expose
    def test_twitter(self):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        result = notifiers.twitter_notifier.test_notify()
        if result:
            return "Tweet successful, check your twitter to make sure it worked"
        else:
            return "Error sending tweet"

    @cherrypy.expose
    def test_android_pn(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'url' in kwargs:
            CONFIG.set_str('ANDROIDPN_URL', kwargs['url'])
        if 'username' in kwargs:
            CONFIG.set_str('ANDROIDPN_USERNAME', kwargs['username'])
        if 'broadcast' in kwargs:
            if kwargs['broadcast'] == 'True':
                CONFIG.set_bool('ANDROIDPN_BROADCAST', True)
            else:
                CONFIG.set_bool('ANDROIDPN_BROADCAST', False)
        result = notifiers.androidpn_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='AndroidPN')
            return "Test AndroidPN notice sent successfully"
        else:
            return "Test AndroidPN notice failed"

    @cherrypy.expose
    def test_boxcar(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'token' in kwargs:
            CONFIG.set_str('BOXCAR_TOKEN', kwargs['token'])
        result = notifiers.boxcar_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='Boxcar')
            return "Boxcar notification successful,\n%s" % result
        else:
            return "Boxcar notification failed"

    @cherrypy.expose
    def test_pushbullet(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'token' in kwargs:
            CONFIG.set_str('PUSHBULLET_TOKEN', kwargs['token'])
        if 'device' in kwargs:
            CONFIG.set_str('PUSHBULLET_DEVICEID', kwargs['device'])
        result = notifiers.pushbullet_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='PushBullet')
            return "Pushbullet notification successful,\n%s" % result
        else:
            return "Pushbullet notification failed"

    @cherrypy.expose
    def test_pushover(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'apitoken' in kwargs:
            CONFIG.set_str('PUSHOVER_APITOKEN', kwargs['apitoken'])
        if 'keys' in kwargs:
            CONFIG.set_str('PUSHOVER_KEYS', kwargs['keys'])
        if 'priority' in kwargs:
            res = check_int(kwargs['priority'], 0, positive=False)
            if res < -2 or res > 1:
                res = 0
            CONFIG.set_int('PUSHOVER_PRIORITY', res)
        if 'device' in kwargs:
            CONFIG.set_str('PUSHOVER_DEVICE', kwargs['device'])

        result = notifiers.pushover_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='Pushover')
            return "Pushover notification successful,\n%s" % result
        else:
            return "Pushover notification failed"

    @cherrypy.expose
    def test_telegram(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'token' in kwargs:
            CONFIG.set_str('TELEGRAM_TOKEN', kwargs['token'])
        if 'userid' in kwargs:
            CONFIG.set_str('TELEGRAM_USERID', kwargs['userid'])

        result = notifiers.telegram_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='Telegram')
            return "Test Telegram notice sent successfully"
        else:
            return "Test Telegram notice failed"

    @cherrypy.expose
    def test_prowl(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'apikey' in kwargs:
            CONFIG.set_str('PROWL_APIKEY', kwargs['apikey'])
        if 'priority' in kwargs:
            CONFIG.set_int('PROWL_PRIORITY', check_int(kwargs['priority'], 0))

        result = notifiers.prowl_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='Prowl')
            return "Test Prowl notice sent successfully"
        else:
            return "Test Prowl notice failed"

    @cherrypy.expose
    def test_growl(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'apikey' in kwargs:
            CONFIG.set_str('GROWL_HOST', kwargs['host'])
        if 'priority' in kwargs:
            CONFIG.set_str('GROWL_PASSWORD', kwargs['password'])

        result = notifiers.growl_notifier.test_notify()
        if result:
            CONFIG.save_config_and_backup_old(section='Growl')
            return "Test Growl notice sent successfully"
        else:
            return "Test Growl notice failed"

    @cherrypy.expose
    def test_slack(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'token' in kwargs:
            CONFIG.set_str('SLACK_TOKEN', kwargs['token'])
        if 'url' in kwargs:
            CONFIG.set_str('SLACK_URL', kwargs['url'])

        result = notifiers.slack_notifier.test_notify()
        if result != "ok":
            return "Slack notification failed,\n%s" % result
        else:
            CONFIG.save_config_and_backup_old(section='Slack')
            return "Slack notification successful"

    @cherrypy.expose
    def test_custom(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'script' in kwargs:
            CONFIG.set_str('CUSTOM_SCRIPT', kwargs['script'])
        result = notifiers.custom_notifier.test_notify()
        if result is False:
            return "Custom notification failed"
        else:
            CONFIG.save_config_and_backup_old(section='Custom')
            return "Custom notification successful"

    @cherrypy.expose
    def test_email(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'tls' in kwargs:
            if kwargs['tls'] == 'True':
                CONFIG.set_bool('EMAIL_TLS', True)
            else:
                CONFIG.set_bool('EMAIL_TLS', False)
        if 'ssl' in kwargs:
            if kwargs['ssl'] == 'True':
                CONFIG.set_bool('EMAIL_SSL', True)
            else:
                CONFIG.set_bool('EMAIL_SSL', False)
        if 'sendfile' in kwargs:
            if kwargs['sendfile'] == 'True':
                CONFIG.set_bool('EMAIL_SENDFILE_ONDOWNLOAD', True)
            else:
                CONFIG.set_bool('EMAIL_SENDFILE_ONDOWNLOAD', False)
        if 'emailfrom' in kwargs:
            CONFIG.set_str('EMAIL_FROM', kwargs['emailfrom'])
        if 'emailto' in kwargs:
            CONFIG.set_str('EMAIL_TO', kwargs['emailto'])
        if 'server' in kwargs:
            CONFIG.set_str('EMAIL_SMTP_SERVER', kwargs['server'])
        if 'user' in kwargs:
            CONFIG.set_str('EMAIL_SMTP_USER', kwargs['user'])
        if 'password' in kwargs:
            CONFIG.set_str('EMAIL_SMTP_PASSWORD', kwargs['password'])
        if 'port' in kwargs:
            CONFIG.set_int('EMAIL_SMTP_PORT', check_int(kwargs['port'], 0))

        result = notifiers.email_notifier.test_notify()
        if not result:
            return "Email notification failed"
        else:
            CONFIG.save_config_and_backup_old(section='Email')
            return "Email notification successful, check your email"

    # API ###############################################################

    @cherrypy.expose
    def api(self, **kwargs):
        from lazylibrarian.api import Api
        a = Api()
        # noinspection PyArgumentList
        a.check_params(**kwargs)
        return a.fetch_data

    @cherrypy.expose
    def generate_api(self):
        logger = logging.getLogger(__name__)
        api_key = hashlib.sha224(str(random.getrandbits(256)).encode('utf-8')).hexdigest()[0:32]
        CONFIG.set_str('API_KEY', api_key)
        logger.info("New API generated")
        return api_key

    # ALL ELSE ##########################################################

    @cherrypy.expose
    def force_process(self, source=None):
        logger = logging.getLogger(__name__)
        if 'POSTPROCESSOR' not in [n.name for n in [t for t in threading.enumerate()]]:
            threading.Thread(target=process_dir, name='POSTPROCESSOR', args=[True]).start()
            schedule_job(action=SchedulerCommand.RESTART, target='PostProcessor')
        else:
            logger.debug('POSTPROCESSOR already running')
        raise cherrypy.HTTPRedirect(source)

    @cherrypy.expose
    def force_wish(self, source=None):
        logger = logging.getLogger(__name__)
        if CONFIG.use_wishlist():
            search_wishlist()
        else:
            logger.warning('WishList search called but no wishlist providers set')
        if source:
            raise cherrypy.HTTPRedirect(source)
        raise cherrypy.HTTPRedirect('books')

    @cherrypy.expose
    def force_search(self, source=None, title=None):
        logger = logging.getLogger(__name__)
        if source in ["magazines", 'comics']:
            if CONFIG.use_any():
                if title:
                    title = title.replace('&amp;', '&')
                    if source == 'magazines':
                        self.search_for_mag(bookid=title)
                    elif source == 'comics':
                        self.search_for_comic(comicid=title)
                elif source == 'magazines' and 'SEARCHALLMAG' not in [
                        n.name for n in [t for t in threading.enumerate()]]:
                    threading.Thread(target=search_magazines, name='SEARCHALLMAG', args=[]).start()
                    schedule_job(action=SchedulerCommand.RESTART, target='search_magazines')
                elif source == 'comics' and 'SEARCHALLCOMICS' not in [
                        n.name for n in [t for t in threading.enumerate()]]:
                    threading.Thread(target=search_comics, name='SEARCHALLCOMICS', args=[]).start()
                    schedule_job(action=SchedulerCommand.RESTART, target='search_comics')
            else:
                logger.warning('Search called but no download providers set')
        elif source in ["books", "audio"]:
            if CONFIG.use_any():
                if 'SEARCHALLBOOKS' not in [n.name for n in [t for t in threading.enumerate()]]:
                    schedule_job(SchedulerCommand.STOP, "search_book")
                    schedule_job(SchedulerCommand.STARTNOW, "search_book")
                if CONFIG.use_rss():
                    schedule_job(SchedulerCommand.STOP, "search_rss_book")
                    schedule_job(SchedulerCommand.STARTNOW, "search_rss_book")
            else:
                logger.warning('Search called but no download providers set')
        else:
            logger.debug("force_search called with bad source")
            raise cherrypy.HTTPRedirect('books')
        raise cherrypy.HTTPRedirect(source)

    @cherrypy.expose
    def manage(self, **kwargs):
        types = []
        if CONFIG.get_bool('EBOOK_TAB'):
            types.append('eBook')
        if CONFIG.get_bool('AUDIO_TAB'):
            types.append('AudioBook')
        if not types:
            raise cherrypy.HTTPRedirect('authors')
        library = types[0]
        which_status = 'Wanted'
        if 'library' in kwargs and kwargs['library'] in types:
            library = kwargs['library']
        if 'whichStatus' in kwargs and kwargs['whichStatus']:
            which_status = kwargs['whichStatus']
        if which_status == 'None':
            which_status = "Wanted"
        return serve_template(templatename="managebooks.html", title="Manage %ss" % library,
                              books=[], types=types, library=library, whichStatus=which_status)

    @cherrypy.expose
    def test_deluge(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        loggerdlcomms = logging.getLogger('special.dlcomms')
        if 'host' in kwargs:
            CONFIG.set_str('DELUGE_HOST', kwargs['host'])
        if 'base' in kwargs:
            CONFIG.set_str('DELUGE_BASE', kwargs['base'])
        if 'cert' in kwargs:
            CONFIG.set_str('DELUGE_CERT', kwargs['cert'])
        if 'port' in kwargs:
            CONFIG.set_int('DELUGE_PORT', check_int(kwargs['port'], 0))
        if 'pwd' in kwargs:
            CONFIG.set_str('DELUGE_PASS', kwargs['pwd'])
        if 'label' in kwargs:
            CONFIG.set_str('DELUGE_LABEL', kwargs['label'])
        if 'user' in kwargs:
            CONFIG.set_str('DELUGE_USER', kwargs['user'])

        try:
            if not CONFIG['DELUGE_USER']:
                # no username, talk to the webui
                msg = deluge.check_link()
                if 'FAILED' in msg:
                    return msg
            else:
                # if there's a username, talk to the daemon directly
                # if daemon, no cert used
                CONFIG.set_str('DELUGE_CERT', '')
                # and host must not contain http:// or https://
                host = CONFIG['DELUGE_HOST']
                host = host.replace('https://', '').replace('http://', '')
                CONFIG.set_str('DELUGE_HOST', host)
                client = DelugeRPCClient(CONFIG['DELUGE_HOST'],
                                         check_int(CONFIG['DELUGE_PORT'], 0),
                                         CONFIG['DELUGE_USER'],
                                         CONFIG['DELUGE_PASS'])
                client.connect()
                msg = "Deluge: Daemon connection Successful\n"
                if CONFIG['DELUGE_LABEL']:
                    labels = client.call('label.get_labels')
                    if labels:
                        loggerdlcomms.debug("Valid labels: %s" % str(labels))
                    else:
                        msg += "Deluge daemon seems to have no labels set\n"

                    mylabel = CONFIG['DELUGE_LABEL'].lower()
                    if mylabel != CONFIG['DELUGE_LABEL']:
                        CONFIG.set_str('DELUGE_LABEL', mylabel)

                    labels = [make_unicode(s) for s in labels]
                    if mylabel not in labels:
                        res = client.call('label.add', mylabel)
                        if not res:
                            msg += "Label [%s] was added" % CONFIG['DELUGE_LABEL']
                        else:
                            msg = str(res)
                    else:
                        msg += 'Label [%s] is valid' % CONFIG['DELUGE_LABEL']
            # success, save settings
            CONFIG.save_config_and_backup_old(section='DELUGE')
            return msg

        except Exception as e:
            msg = "Deluge: Daemon connection FAILED\n"
            if 'Connection refused' in str(e):
                msg += str(e)
                msg += "Check Deluge daemon HOST and PORT settings"
            elif 'need more than 1 value' in str(e):
                msg += "Invalid USERNAME or PASSWORD"
            else:
                msg += type(e).__name__ + ' ' + str(e)
            return msg

    @cherrypy.expose
    def test_sabnzbd(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('SAB_HOST', kwargs['host'])
        if 'port' in kwargs:
            CONFIG.set_int('SAB_PORT', check_int(kwargs['port'], 0))
        if 'user' in kwargs:
            CONFIG.set_str('SAB_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('SAB_PASS', kwargs['pwd'])
        if 'api' in kwargs:
            CONFIG.set_str('SAB_API', kwargs['api'])
        if 'cat' in kwargs:
            CONFIG.set_str('SAB_CAT', kwargs['cat'])
        if 'subdir' in kwargs:
            CONFIG.set_str('SAB_SUBDIR', kwargs['subdir'])
        msg = sabnzbd.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='sab_nzbd')
        return msg

    @cherrypy.expose
    def test_nzbget(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('NZBGET_HOST', kwargs['host'])
        if 'port' in kwargs:
            CONFIG.set_int('NZBGET_PORT', check_int(kwargs['port'], 0))
        if 'user' in kwargs:
            CONFIG.set_str('NZBGET_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('NZBGET_PASS', kwargs['pwd'])
        if 'cat' in kwargs:
            CONFIG.set_str('NZBGET_CATEGORY', kwargs['cat'])
        if 'pri' in kwargs:
            CONFIG.set_int('NZBGET_PRIORITY', check_int(kwargs['pri'], 0))
        msg = nzbget.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='NZBGet')
        return msg

    @cherrypy.expose
    def test_transmission(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('TRANSMISSION_HOST', kwargs['host'])
        if 'base' in kwargs:
            CONFIG.set_str('TRANSMISSION_BASE', kwargs['base'])
        if 'port' in kwargs:
            CONFIG.set_int('TRANSMISSION_PORT', check_int(kwargs['port'], 0))
        if 'user' in kwargs:
            CONFIG.set_str('TRANSMISSION_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('TRANSMISSION_PASS', kwargs['pwd'])
        msg = transmission.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='TRANSMISSION')
        return msg

    @cherrypy.expose
    def test_qbittorrent(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('QBITTORRENT_HOST', kwargs['host'])
        if 'port' in kwargs:
            CONFIG.set_int('QBITTORRENT_PORT', check_int(kwargs['port'], 0))
        if 'base' in kwargs:
            CONFIG.set_str('QBITTORRENT_BASE', kwargs['base'])
        if 'user' in kwargs:
            CONFIG.set_str('QBITTORRENT_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('QBITTORRENT_PASS', kwargs['pwd'])
        if 'label' in kwargs:
            CONFIG.set_str('QBITTORRENT_LABEL', kwargs['label'])
        msg = qbittorrent.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='QBITTORRENT')
        return msg

    @cherrypy.expose
    def test_utorrent(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('UTORRENT_HOST', kwargs['host'])
        if 'port' in kwargs:
            CONFIG.set_int('UTORRENT_PORT', check_int(kwargs['port'], 0))
        if 'base' in kwargs:
            CONFIG.set_str('UTORRENT_BASE', kwargs['base'])
        if 'user' in kwargs:
            CONFIG.set_str('UTORRENT_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('UTORRENT_PASS', kwargs['pwd'])
        if 'label' in kwargs:
            CONFIG.set_str('UTORRENT_LABEL', kwargs['label'])
        msg = utorrent.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='UTORRENT')
        return msg

    @cherrypy.expose
    def test_rtorrent(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('RTORRENT_HOST', kwargs['host'])
        if 'dir' in kwargs:
            CONFIG.set_str('RTORRENT_DIR', kwargs['dir'])
        if 'user' in kwargs:
            CONFIG.set_str('RTORRENT_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('RTORRENT_PASS', kwargs['pwd'])
        if 'label' in kwargs:
            CONFIG.set_str('RTORRENT_LABEL', kwargs['label'])
        msg = rtorrent.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='RTORRENT')
        return msg

    @cherrypy.expose
    def test_synology(self, **kwargs):
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        thread_name("WEBSERVER")
        if 'host' in kwargs:
            CONFIG.set_str('SYNOLOGY_HOST', kwargs['host'])
        if 'port' in kwargs:
            CONFIG.set_int('SYNOLOGY_PORT', check_int(kwargs['port'], 0))
        if 'user' in kwargs:
            CONFIG.set_str('SYNOLOGY_USER', kwargs['user'])
        if 'pwd' in kwargs:
            CONFIG.set_str('SYNOLOGY_PASS', kwargs['pwd'])
        if 'dir' in kwargs:
            CONFIG.set_str('SYNOLOGY_DIR', kwargs['dir'])
        msg = synology.check_link()
        if 'success' in msg:
            CONFIG.save_config_and_backup_old(section='SYNOLOGY')
        return msg

    @cherrypy.expose
    def test_ffmpeg(self, **kwargs):
        thread_name("WEBSERVER")
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        loggerpostprocess = logging.getLogger('special.postprocess')
        if 'prg' in kwargs and kwargs['prg']:
            CONFIG.set_str('FFMPEG', kwargs['prg'])
        ffmpeg = CONFIG['FFMPEG']
        try:
            if loggerpostprocess.isEnabledFor(logging.DEBUG):
                ffmpeg_env = os.environ.copy()
                ffmpeg_env["FFREPORT"] = DIRS.get_logfile("ffmpeg-test-%s.log" %
                                                          now().replace(':', '-').replace(' ', '-'))
                params = [ffmpeg, "-version", "-report"]
            else:
                params = [ffmpeg, "-version"]
                ffmpeg_env = None

            if os.name != 'nt':
                res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                              stderr=subprocess.STDOUT, env=ffmpeg_env)
            else:
                res = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

            ff_ver = make_unicode(res).strip().split("Copyright")[0].split()[-1]
            lazylibrarian.FFMPEGVER = ff_ver
            return "Found ffmpeg version %s" % ff_ver
        except Exception as e:
            lazylibrarian.FFMPEGVER = None
            return "ffmpeg -version failed: %s %s" % (type(e).__name__, str(e))

    @cherrypy.expose
    def test_ebook_convert(self, **kwargs):
        thread_name("WEBSERVER")
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        if 'prg' in kwargs and kwargs['prg']:
            CONFIG.set_str('EBOOK_CONVERT', kwargs['prg'])
        prg = CONFIG['EBOOK_CONVERT']
        try:
            params = [prg, "--version"]
            res = subprocess.check_output(params, stderr=subprocess.STDOUT)
            res = make_unicode(res).strip().split("(")[1].split(")")[0]
            return "Found ebook-convert version %s" % res
        except Exception as e:
            return "ebook-convert --version failed: %s %s" % (type(e).__name__, str(e))

    @cherrypy.expose
    def test_calibredb(self, **kwargs):
        thread_name("WEBSERVER")
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        if 'prg' in kwargs and kwargs['prg']:
            CONFIG.set_str('IMP_CALIBREDB', kwargs['prg'])
        return calibre_test()

    @cherrypy.expose
    def test_preprocessor(self, **kwargs):
        thread_name("WEBSERVER")
        cherrypy.response.headers['Cache-Control'] = "max-age=0,no-cache,no-store"
        if 'prg' in kwargs and kwargs['prg']:
            CONFIG.set_str('EXT_PREPROCESS', kwargs['prg'])
        if len(CONFIG['EXT_PREPROCESS']):
            params = [CONFIG['EXT_PREPROCESS'], 'test', '']
            rc, res, err = run_script(params)
            if rc:
                return "Preprocessor returned %s: res[%s] err[%s]" % (rc, res, err)
        else:
            return "No preprocessor set in config"
        return res

    @cherrypy.expose
    def opds(self, **kwargs):
        self.label_thread('OPDS Server')
        op = OPDS()
        op.check_params(**kwargs)
        data = op.fetch_data()
        return data

    @staticmethod
    def send_file(myfile, name=None, email=False):
        logger = logging.getLogger(__name__)
        if CONFIG.get_bool('USER_ACCOUNTS'):
            cookie = cherrypy.request.cookie
            msg = ''
            if email and cookie and 'll_uid' in list(cookie.keys()):
                db = database.DBConnection()
                try:
                    res = db.match('SELECT SendTo from users where UserID=?', (cookie['ll_uid'].value,))
                finally:
                    db.close()
                if res and res['SendTo']:
                    logger.debug("Emailing %s to %s" % (myfile, res['SendTo']))
                    if name:
                        msg = lazylibrarian.NEWFILE_MSG.replace('{name}', name).replace(
                            '{method}', ' is attached').replace('{link}', '')
                    result = notifiers.email_notifier.email_file(subject="Message from LazyLibrarian",
                                                                 message=msg, to_addr=res['SendTo'],
                                                                 files=[myfile])
                    if result:
                        msg = "Emailed file %s to %s" % (os.path.basename(myfile), res['SendTo'])
                        logger.debug(msg)
                    else:
                        msg = "Failed to email file %s to %s" % (os.path.basename(myfile), res['SendTo'])
                        logger.error(msg)
                return serve_template(templatename="choosetype.html", title='Send file',
                                      pop_message=msg, pop_types='', bookid='', valid='', email=email)
        if not name:
            name = os.path.basename(myfile)
        if path_isfile(myfile):
            return serve_file(myfile, mime_type(myfile), "attachment", name=name)
        else:
            logger.error("No file [%s]" % myfile)

    # TELEMETRY ##########################################################

    @cherrypy.expose
    def get_telemetry_data(self, **kwargs):
        send_config = kwargs['send_config']
        send_usage = kwargs['send_usage']
        return TELEMETRY.get_data_for_ui_preview(send_config, send_usage)

    @cherrypy.expose
    def reset_telemetry_usage_data(self):
        return TELEMETRY.clear_usage_data()

    @cherrypy.expose
    def submit_telemetry_data(self, **kwargs):
        server = kwargs['server']
        send_config = kwargs['send_config']
        send_usage = kwargs['send_usage']
        result, _ = TELEMETRY.submit_data(server, send_config, send_usage)
        return result

    @cherrypy.expose
    def test_telemetry_server(self, **kwargs):
        return TELEMETRY.test_server(kwargs['server'])

    @cherrypy.expose
    def set_current_tabs(self, **kwargs):
        if 'config_tab' in kwargs:
            CONFIG.set_from_ui('CONFIG_TAB_NUM', kwargs['config_tab'])

    @cherrypy.expose
    def enable_telemetry(self, **kwargs):
        CONFIG.set_bool('TELEMETRY_ENABLE', True)
        CONFIG.set_int('TELEMETRY_INTERVAL', 6)
        CONFIG.set_url('TELEMETRY_SERVER', 'https://conceded-moose-5564.dataplicity.io/telemetry')
        CONFIG.save_config_and_backup_old(section='Telemetry')
        return "Thank you for enabling Telemetry"
