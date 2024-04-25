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

# based on code found in https://gist.github.com/gpiancastelli/537923 by Giulio Piancastelli

import threading
import time
import logging
import traceback
# noinspection PyUnresolvedReferences
import xml.dom.minidom
from string import Template
from urllib.parse import urlencode, parse_qsl

import lazylibrarian
from lazylibrarian.config2 import CONFIG
from lazylibrarian import database
from lazylibrarian.cache import gr_api_sleep
from lazylibrarian.formatter import plural, get_list, check_int, thread_name
from lazylibrarian.gr import GoodReads

import lib.oauth2 as oauth

client = ''
request_token = ''
consumer = ''
token = ''
user_id = ''


class GrAuth:
    def __init__(self):
        return

    @staticmethod
    def goodreads_oauth1():
        global client, request_token, consumer
        logger = logging.getLogger(__name__)
        loggergrsync = logging.getLogger('special.grsync')
        if CONFIG['GR_API'] == 'ckvsiSDsuqh7omh74ZZ6Q':
            msg = "Please get your own personal GoodReads api key from https://www.goodreads.com/api/keys and try again"
            return msg
        if not CONFIG['GR_SECRET']:
            return "Invalid or missing GR_SECRET"

        if CONFIG['GR_OAUTH_TOKEN'] and CONFIG['GR_OAUTH_SECRET']:
            return "Already authorised"

        request_token_url = '%s/oauth/request_token' % CONFIG['GR_URL']
        authorize_url = '%s/oauth/authorize' % CONFIG['GR_URL']
        # access_token_url = '%s/oauth/access_token' % 'https://www.goodreads.com'

        consumer = oauth.Consumer(key=str(CONFIG['GR_API']),
                                  secret=str(CONFIG['GR_SECRET']))

        client = oauth.Client(consumer)

        try:
            response, content = client.request(request_token_url, 'GET')

        except Exception as e:
            logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
            if type(e).__name__ == 'SSLError':
                logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
            return "Exception in client.request: see error log"

        if not response['status'].startswith('2'):
            if content:
                logger.debug(str(content))
            return 'Invalid response [%s] from: %s' % (response['status'], request_token_url)

        request_token = dict(parse_qsl(content))
        request_token = {key.decode("utf-8"): request_token[key].decode("utf-8") for key in request_token}
        loggergrsync.debug("oauth1: %s" % str(request_token))
        if 'oauth_token' in request_token:
            authorize_link = '%s?oauth_token=%s' % (authorize_url, request_token['oauth_token'])
            return authorize_link
        else:
            return "No oauth_token, got %s" % content

    # noinspection PyTypeChecker
    @staticmethod
    def goodreads_oauth2():
        global request_token, consumer, token, client
        logger = logging.getLogger(__name__)
        loggergrsync = logging.getLogger('special.grsync')
        try:
            if request_token and 'oauth_token' in request_token and 'oauth_token_secret' in request_token:
                # noinspection PyTypeChecker
                token = oauth.Token(request_token['oauth_token'], request_token['oauth_token_secret'])
            else:
                return "Unable to run oAuth2 - Have you run oAuth1?"
        except Exception as e:
            logger.error("Exception in oAuth2: %s %s" % (type(e).__name__, traceback.format_exc()))
            return "Unable to run oAuth2 - Have you run oAuth1?"

        access_token_url = '%s/oauth/access_token' % CONFIG['GR_URL']

        client = oauth.Client(consumer, token)

        try:
            response, content = client.request(access_token_url, 'POST')
        except Exception as e:
            logger.error("Exception in oauth2 client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
            if type(e).__name__ == 'SSLError':
                logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
            return "Error in oauth2 client request: see error log"

        if not response['status'].startswith('2'):
            return 'Invalid response [%s] from %s' % (response['status'], access_token_url)

        access_token = dict(parse_qsl(content))
        access_token = {key.decode("utf-8"): access_token[key].decode("utf-8") for key in access_token}
        loggergrsync.debug("oauth2: %s" % str(access_token))
        CONFIG.set_str('GR_OAUTH_TOKEN', access_token['oauth_token'])
        CONFIG.set_str('GR_OAUTH_SECRET', access_token['oauth_token_secret'])
        CONFIG.save_config_and_backup_old(section='API')
        return "Authorisation complete"

    def get_user_id(self):
        global consumer, client, token, user_id
        logger = logging.getLogger(__name__)
        if not CONFIG['GR_API'] or not CONFIG['GR_SECRET'] or not \
                CONFIG['GR_OAUTH_TOKEN'] or not CONFIG['GR_OAUTH_SECRET']:
            logger.warning("Goodreads user id error: Please authorise first")
            return ""
        else:
            try:
                consumer = oauth.Consumer(key=str(CONFIG['GR_API']),
                                          secret=str(CONFIG['GR_SECRET']))
                token = oauth.Token(CONFIG['GR_OAUTH_TOKEN'], CONFIG['GR_OAUTH_SECRET'])
                client = oauth.Client(consumer, token)
                user_id = self.get_userid()
                if not user_id:
                    logger.warning("Goodreads userid error")
                    return ""
                return user_id
            except Exception as e:
                logger.error("Unable to get UserID: %s %s" % (type(e).__name__, str(e)))
                return ""

    def get_shelf_list(self):
        global consumer, client, token, user_id
        logger = logging.getLogger(__name__)
        loggergrsync = logging.getLogger('special.grsync')
        if not CONFIG['GR_API'] or not CONFIG['GR_SECRET'] or not \
                CONFIG['GR_OAUTH_TOKEN'] or not CONFIG['GR_OAUTH_SECRET']:
            logger.warning("Goodreads get shelf error: Please authorise first")
            return []
        else:
            #
            # loop over each page of shelves
            #     loop over each shelf
            #         add shelf to list
            #
            consumer = oauth.Consumer(key=str(CONFIG['GR_API']),
                                      secret=str(CONFIG['GR_SECRET']))
            token = oauth.Token(CONFIG['GR_OAUTH_TOKEN'], CONFIG['GR_OAUTH_SECRET'])
            client = oauth.Client(consumer, token)
            user_id = self.get_userid()
            if not user_id:
                logger.warning("Goodreads userid error")
                return []
            current_page = 0
            shelves = []
            page_shelves = 1
            while page_shelves:
                current_page += 1
                page_shelves = 0
                shelf_template = Template('${base}/shelf/list.xml?user_id=${user_id}&key=${key}&page=${page}')
                body = urlencode({})
                headers = {'Content-Type': 'application/x-www-form-urlencoded'}
                request_url = shelf_template.substitute(base=CONFIG['GR_URL'], user_id=user_id,
                                                        page=current_page, key=CONFIG['GR_API'])
                gr_api_sleep()
                try:
                    response, content = client.request(request_url, 'GET', body, headers)
                except Exception as e:
                    logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
                    if type(e).__name__ == 'SSLError':
                        logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
                    return shelves

                if not response['status'].startswith('2'):
                    logger.error('Failure status: %s for page %s' % (response['status'], current_page))
                    loggergrsync.debug(request_url)
                else:
                    # noinspection PyUnresolvedReferences
                    xmldoc = xml.dom.minidom.parseString(content)

                    shelf_list = xmldoc.getElementsByTagName('shelves')[0]
                    for item in shelf_list.getElementsByTagName('user_shelf'):
                        shelf_name = item.getElementsByTagName('name')[0].firstChild.nodeValue
                        shelf_count = item.getElementsByTagName('book_count')[0].firstChild.nodeValue
                        shelf_exclusive = item.getElementsByTagName('exclusive_flag')[0].firstChild.nodeValue
                        shelves.append({'name': shelf_name, 'books': shelf_count, 'exclusive': shelf_exclusive})
                        page_shelves += 1

                        loggergrsync.debug('Shelf %s : %s: Exclusive %s' % (shelf_name, shelf_count, shelf_exclusive))

                    loggergrsync.debug('Found %s shelves on page %s' % (page_shelves, current_page))

            logger.debug('Found %s %s on %s %s' % (len(shelves), plural(len(shelves), "shelf"),
                                                   current_page - 1, plural(current_page - 1, "page")))
            # print shelves
            return shelves

    def follow_author(self, authorid=None, follow=True):
        global consumer, client, token, user_id
        logger = logging.getLogger(__name__)
        if not CONFIG['GR_API'] or not CONFIG['GR_SECRET'] or not \
                CONFIG['GR_OAUTH_TOKEN'] or not CONFIG['GR_OAUTH_SECRET']:
            logger.warning("Goodreads follow author error: Please authorise first")
            return False, 'Unauthorised'

        consumer = oauth.Consumer(key=str(CONFIG['GR_API']),
                                  secret=str(CONFIG['GR_SECRET']))
        token = oauth.Token(CONFIG['GR_OAUTH_TOKEN'], CONFIG['GR_OAUTH_SECRET'])
        client = oauth.Client(consumer, token)
        user_id = self.get_userid()
        if not user_id:
            return False, "Goodreads userid error"

        # follow https://www.goodreads.com/author_followings?id=AUTHOR_ID&format=xml
        # unfollow https://www.goodreads.com/author_followings/AUTHOR_FOLLOWING_ID?format=xml
        gr_api_sleep()

        if follow:
            body = urlencode({'id': authorid, 'format': 'xml'})
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            try:
                response, content = client.request('%s/author_followings' % CONFIG['GR_URL'],
                                                   'POST', body, headers)
            except Exception as e:
                logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
                if type(e).__name__ == 'SSLError':
                    logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
                return False, "Error in client.request: see error log"
        else:
            body = urlencode({'format': 'xml'})
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            try:
                response, content = client.request('%s/author_followings/%s' % (CONFIG['GR_URL'],
                                                                                authorid), 'DELETE', body, headers)
            except Exception as e:
                logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
                if type(e).__name__ == 'SSLError':
                    logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
                return False, "Error in client.request: see error log"

        if follow and response['status'] == '422':
            return True, 'Already following'

        if response['status'].startswith('2'):
            if follow:
                return True, content.split('<id>')[1].split('</id>')[0]
            return True, ''
        return False, 'Failure status: %s' % response['status']

    def create_shelf(self, shelf='lazylibrarian', exclusive=False, sortable=False):
        global consumer, client, token, user_id
        logger = logging.getLogger(__name__)
        if not CONFIG['GR_API'] or not CONFIG['GR_SECRET'] or not \
                CONFIG['GR_OAUTH_TOKEN'] or not CONFIG['GR_OAUTH_SECRET']:
            logger.warning("Goodreads create shelf error: Please authorise first")
            return False, 'Unauthorised'

        consumer = oauth.Consumer(key=str(CONFIG['GR_API']),
                                  secret=str(CONFIG['GR_SECRET']))
        token = oauth.Token(CONFIG['GR_OAUTH_TOKEN'], CONFIG['GR_OAUTH_SECRET'])
        client = oauth.Client(consumer, token)
        user_id = self.get_userid()
        if not user_id:
            return False, "Goodreads userid error"

        # could also pass [featured] [exclusive_flag] [sortable_flag] all default to False
        shelf_info = {'user_shelf[name]': shelf}
        if exclusive:
            shelf_info['user_shelf[exclusive_flag]'] = 'true'
        if sortable:
            shelf_info['user_shelf[sortable_flag]'] = 'true'
        body = urlencode(shelf_info)
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        gr_api_sleep()

        try:
            response, _ = client.request('%s/user_shelves.xml' % CONFIG['GR_URL'], 'POST',
                                         body, headers)
        except Exception as e:
            logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
            if type(e).__name__ == 'SSLError':
                logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
            return False, "Error in client.request: see error log"

        if not response['status'].startswith('2'):
            msg = 'Failure status: %s' % response['status']
            return False, msg
        return True, ''

    def get_gr_shelf_contents(self, shelf='to-read'):
        global consumer, client, token, user_id
        logger = logging.getLogger(__name__)
        loggergrsync = logging.getLogger('special.grsync')
        if not CONFIG['GR_API'] or not CONFIG['GR_SECRET'] or not \
                CONFIG['GR_OAUTH_TOKEN'] or not CONFIG['GR_OAUTH_SECRET']:
            logger.warning("Goodreads shelf contents error: Please authorise first")
            return []
        else:
            #
            # loop over each page of owned books
            #     loop over each book
            #         add book to list
            #
            consumer = oauth.Consumer(key=str(CONFIG['GR_API']),
                                      secret=str(CONFIG['GR_SECRET']))
            token = oauth.Token(CONFIG['GR_OAUTH_TOKEN'], CONFIG['GR_OAUTH_SECRET'])
            client = oauth.Client(consumer, token)
            user_id = self.get_userid()
            if not user_id:
                logger.warning("Goodreads userid error")
                return []

            logger.debug('User id is: ' + user_id)

            current_page = 0
            total_books = 0
            gr_list = []

            while True:
                current_page += 1
                content = self.get_shelf_books(current_page, shelf)
                # noinspection PyUnresolvedReferences
                xmldoc = xml.dom.minidom.parseString(content)

                page_books = 0
                for book in xmldoc.getElementsByTagName('book'):
                    book_id, book_title = self.get_book_info(book)

                    if loggergrsync.isEnabledFor(logging.DEBUG):
                        try:
                            loggergrsync.debug('Book %10s : %s' % (str(book_id), book_title))
                        except UnicodeEncodeError:
                            loggergrsync.debug('Book %10s : %s' % (str(book_id), 'Title Messed Up By Unicode Error'))

                    gr_list.append(book_id)

                    page_books += 1
                    total_books += 1

                loggergrsync.debug('Found %s books on page %s (total = %s)' % (page_books, current_page, total_books))
                if page_books == 0:
                    break

            logger.debug('Found %s books on shelf' % total_books)
            return gr_list

    #############################
    #
    # who are we?
    #
    @staticmethod
    def get_userid():
        global client, user_id
        logger = logging.getLogger(__name__)
        gr_api_sleep()

        try:
            # noinspection PyUnresolvedReferences
            response, content = client.request('%s/api/auth_user' % CONFIG['GR_URL'], 'GET')
        except Exception as e:
            logger.error("Error in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
            if type(e).__name__ == 'SSLError':
                logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
            return ''
        if not response['status'].startswith('2'):
            logger.error('Cannot fetch userid: %s' % response['status'])
            return ''

        # noinspection PyUnresolvedReferences
        userxml = xml.dom.minidom.parseString(content)
        user_id = userxml.getElementsByTagName('user')[0].attributes['id'].value
        return str(user_id)

    #############################
    #
    # fetch xml for a page of books on a shelf
    #
    # noinspection PyUnresolvedReferences
    @staticmethod
    def get_shelf_books(page, shelf_name):
        global client, user_id
        logger = logging.getLogger(__name__)
        loggergrsync = logging.getLogger('special.grsync')
        data = '${base}/review/list?format=xml&v=2&id=${user_id}&sort=author&order=a'
        data += '&key=${key}&page=${page}&per_page=100&shelf=${shelf_name}'
        owned_template = Template(data)
        body = urlencode({})
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        request_url = owned_template.substitute(base=CONFIG['GR_URL'], user_id=user_id, page=page,
                                                key=CONFIG['GR_API'], shelf_name=shelf_name)
        gr_api_sleep()
        try:
            response, content = client.request(request_url, 'GET', body, headers)
        except Exception as e:
            logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
            if type(e).__name__ == 'SSLError':
                logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
            return "Error in client.request: see error log"
        if not response['status'].startswith('2'):
            logger.error('Failure status: %s for %s page %s' % (response['status'], shelf_name, page))
            loggergrsync.debug(request_url)
        return content

    #############################
    #
    # grab id and title from a <book> node
    #
    @staticmethod
    def get_book_info(book):
        book_id = book.getElementsByTagName('id')[0].firstChild.nodeValue
        book_title = book.getElementsByTagName('title')[0].firstChild.nodeValue
        return book_id, book_title

    # noinspection PyUnresolvedReferences
    @staticmethod
    def book_to_list(book_id, shelf_name, action='add'):
        global client
        logger = logging.getLogger(__name__)
        if action == 'remove':
            body = urlencode({'name': shelf_name, 'book_id': book_id, 'a': 'remove'})
        else:
            body = urlencode({'name': shelf_name, 'book_id': book_id})
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        gr_api_sleep()
        try:
            response, content = client.request('%s/shelf/add_to_shelf.xml' % CONFIG['GR_URL'],
                                               'POST', body, headers)
        except Exception as e:
            logger.error("Exception in client.request: %s %s" % (type(e).__name__, traceback.format_exc()))
            if type(e).__name__ == 'SSLError':
                logger.warning("SSLError: if running Ubuntu 20.04/20.10 see lazylibrarian FAQ")
            return False, "Error in client.request: see error log"

        if not response['status'].startswith('2'):
            msg = 'Failure status: %s' % response['status']
            return False, msg
        return True, content

        #############################


def test_auth():
    global user_id
    ga = GrAuth()
    try:
        user_id = ga.get_user_id()
    except Exception as e:
        return "GR Auth %s: %s" % (type(e).__name__, str(e))
    if user_id:
        return "Pass: UserID is %s" % user_id
    else:
        return "Failed, check the debug log"


def cron_sync_to_gr():
    if 'GRSync' not in [n.name for n in [t for t in threading.enumerate()]]:
        _ = sync_to_gr()
    else:
        logger = logging.getLogger(__name__)
        logger.debug("GRSync is already running")


def sync_to_gr():
    logger = logging.getLogger(__name__)
    msg = ''
    new_books = []
    new_audio = []

    thread_name('GRSync')
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        db.upsert("jobs", {"Start": time.time()}, {"Name": "GRSYNC"})
        if CONFIG.get_bool('GR_SYNCUSER'):
            user = db.match("SELECT * from users WHERE UserID=?", (CONFIG['GR_USER'],))

            if not user:
                msg = 'Unable to sync user to goodreads, invalid userid'
            else:
                to_shelf, to_ll = grsync('Read', 'read', user=user)
                msg += "%s %s to Read shelf\n" % (to_shelf, plural(to_shelf, "change"))
                msg += "%s %s to Read from GoodReads\n" % (len(to_ll), plural(len(to_ll), "change"))
                to_shelf, to_ll = grsync('Reading', 'currently-reading', user=user)
                msg += "%s %s to Reading shelf\n" % (to_shelf, plural(to_shelf, "change"))
                msg += "%s %s to Reading from GoodReads\n" % (len(to_ll), plural(len(to_ll), "change"))
                to_shelf, to_ll = grsync('Unread', 'unread', user=user)
                msg += "%s %s to Unread shelf\n" % (to_shelf, plural(to_shelf, "change"))
                msg += "%s %s to Unread from GoodReads\n" % (len(to_ll), plural(len(to_ll), "change"))
                to_shelf, to_ll = grsync('Abandoned', 'abandoned', user=user)
                msg += "%s %s to Abandoned shelf\n" % (to_shelf, plural(to_shelf, "change"))
                msg += "%s %s to Abandoned from GoodReads\n" % (len(to_ll), plural(len(to_ll), "change"))
                to_shelf, to_ll = grsync('To-Read', 'to-read', user=user)
                msg += "%s %s to To-Read shelf\n" % (to_shelf, plural(to_shelf, "change"))
                msg += "%s %s to To-Read from GoodReads\n" % (len(to_ll), plural(len(to_ll), "change"))
                perm = check_int(user['Perms'], 0)
                if to_ll and perm & lazylibrarian.perm_search:
                    if CONFIG.get_bool('EBOOK_TAB'):
                        for item in to_ll:
                            new_books.append({"bookid": item})
                    if CONFIG.get_bool('AUDIO_TAB'):
                        for item in to_ll:
                            new_audio.append({"bookid": item})

        else:  # library sync
            if CONFIG['GR_OWNED'] and \
                    CONFIG['GR_WANTED'] == CONFIG['GR_OWNED']:
                msg += "Unable to sync ebooks, WANTED and OWNED must be different shelves\n"
            elif CONFIG.get_bool('AUDIO_TAB') and CONFIG['GR_AOWNED'] and \
                    CONFIG['GR_AOWNED'] == CONFIG['GR_AWANTED']:
                msg += "Unable to sync audiobooks, WANTED and OWNED must be different shelves\n"
            else:
                if CONFIG['GR_WANTED'] and \
                        CONFIG['GR_AWANTED'] == CONFIG['GR_WANTED']:
                    # wanted audio and ebook on same shelf
                    to_read_shelf, ll_wanted = grsync('Wanted', CONFIG['GR_WANTED'], 'Audio/eBook')
                    msg += "%s %s to %s shelf\n" % (to_read_shelf, plural(to_read_shelf, "change"),
                                                    CONFIG['GR_WANTED'])
                    msg += "%s %s to Wanted from GoodReads\n" % (len(ll_wanted), plural(len(ll_wanted), "change"))
                    if ll_wanted:
                        for item in ll_wanted:
                            new_books.append({"bookid": item})
                            new_audio.append({"bookid": item})

                else:  # see if wanted on separate shelves
                    if CONFIG['GR_WANTED']:
                        to_read_shelf, ll_wanted = grsync('Wanted', CONFIG['GR_WANTED'], 'eBook')
                        msg += "%s %s to %s shelf\n" % (to_read_shelf, plural(to_read_shelf, "change"),
                                                        CONFIG['GR_WANTED'])
                        msg += "%s %s to eBook Wanted from GoodReads\n" % (len(ll_wanted),
                                                                           plural(len(ll_wanted), "change"))
                        if ll_wanted:
                            for item in ll_wanted:
                                new_books.append({"bookid": item})

                    if CONFIG['GR_AWANTED']:
                        to_read_shelf, ll_wanted = grsync('Wanted', CONFIG['GR_AWANTED'], 'AudioBook')
                        msg += "%s %s to %s shelf\n" % (to_read_shelf, plural(to_read_shelf, "change"),
                                                        CONFIG['GR_AWANTED'])
                        msg += "%s %s to Audio Wanted from GoodReads\n" % (len(ll_wanted),
                                                                           plural(len(ll_wanted), "change"))
                        if ll_wanted:
                            for item in ll_wanted:
                                new_audio.append({"bookid": item})

                if CONFIG['GR_OWNED'] and \
                        CONFIG['GR_AOWNED'] == CONFIG['GR_OWNED']:
                    # owned audio and ebook on same shelf
                    to_owned_shelf, ll_have = grsync('Open', CONFIG['GR_OWNED'], 'Audio/eBook')
                    msg += "%s %s to %s shelf\n" % (to_owned_shelf, plural(to_owned_shelf, "change"),
                                                    CONFIG['GR_OWNED'])
                    msg += "%s %s to Owned from GoodReads\n" % (len(ll_have), plural(len(ll_have), "change"))
                else:
                    if CONFIG['GR_OWNED']:
                        to_owned_shelf, ll_have = grsync('Open', CONFIG['GR_OWNED'], 'eBook')
                        msg += "%s %s to %s shelf\n" % (to_owned_shelf, plural(to_owned_shelf, "change"),
                                                        CONFIG['GR_OWNED'])
                        msg += "%s %s to eBook Owned from GoodReads\n" % (len(ll_have), plural(len(ll_have), "change"))
                    if CONFIG['GR_AOWNED']:
                        to_owned_shelf, ll_have = grsync('Open', CONFIG['GR_AOWNED'], 'AudioBook')
                        msg += "%s %s to %s shelf\n" % (to_owned_shelf, plural(to_owned_shelf, "change"),
                                                        CONFIG['GR_AOWNED'])
                        msg += "%s %s to Audio Owned from GoodReads\n" % (len(ll_have), plural(len(ll_have), "change"))

        logger.info(msg.strip('\n').replace('\n', ', '))
        db.upsert("jobs", {"Finish": time.time()}, {"Name": "GRSYNC"})
    except Exception:
        logger.error("Exception in sync_to_gr: %s" % traceback.format_exc())
    finally:
        db.close()
        if new_books:
            threading.Thread(target=lazylibrarian.searchrss.search_rss_book, name='GRSYNCRSSBOOKS',
                             args=[new_books, 'eBook']).start()
            threading.Thread(target=lazylibrarian.searchbook.search_book, name='GRSYNCBOOKS',
                             args=[new_books, 'eBook']).start()
        if new_audio:
            threading.Thread(target=lazylibrarian.searchrss.search_rss_book, name='GRSYNCRSSAUDIO',
                             args=[new_audio, 'AudioBook']).start()
            threading.Thread(target=lazylibrarian.searchbook.search_book, name='GRSYNCAUDIO',
                             args=[new_audio, 'AudioBook']).start()

        thread_name('WEBSERVER')
        return msg


def grfollow(authorid, follow=True):
    db = database.DBConnection()
    try:
        match = db.match('SELECT AuthorName,GRfollow from authors WHERE authorid=?', (authorid,))
    finally:
        db.close()
    if match:
        if follow:
            action = 'Follow'
            aname = match['AuthorName']
            actionid = authorid
        else:
            action = 'Unfollow'
            aname = authorid
            actionid = match['GRfollow']

        ga = GrAuth()
        res, msg = ga.follow_author(actionid, follow)
        if res:
            if follow:
                return "%s author %s, followid=%s" % (action, aname, msg)
            else:
                return "%s author %s" % (action, aname)
        else:
            return "Unable to %s %s: %s" % (action, authorid, msg)
    else:
        return "Unable to (un)follow %s, invalid authorid" % authorid


def grsync(status, shelf, library='eBook', reset=False, user=None):
    # noinspection PyBroadException
    logger = logging.getLogger(__name__)
    loggergrsync = logging.getLogger('special.grsync')
    dstatus = status
    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        usershelf = None
        if user:
            usershelf = shelf + '_' + user['UserID']
            logger.info('Syncing %s %ss to %s shelf' % (status, library, shelf))
            if shelf == 'read':
                ll_list = get_list(user['HaveRead'])
            elif shelf == 'currently-reading':
                ll_list = get_list(user['Reading'])
            elif shelf == 'abandoned':
                ll_list = get_list(user['Abandoned'])
            elif shelf == 'to-read':
                ll_list = get_list(user['ToRead'])
            else:  # if shelf == 'unread':
                unread = set()
                res = db.select("SELECT gr_id from books where status in ('Open', 'Have')")
                for item in res:
                    unread.add(item['gr_id'])
                read = set()
                for item in ['HaveRead', 'To-Read', 'Reading', 'Abandoned']:
                    try:
                        contents = set(get_list(user[item]))
                    except IndexError:
                        contents = set()
                    read = read.union(contents)
                unread.difference_update(read)
                ll_list = list(unread)

        else:
            if dstatus == "Open":
                dstatus += "/Have"
            logger.info('Syncing %s %ss to %s shelf' % (dstatus, library, shelf))

            if library == 'eBook':
                if status == 'Open':
                    cmd = "select gr_id from books where status in ('Open', 'Have')"
                elif status == 'Wanted':
                    cmd = "select gr_id from books where status in ('Wanted', 'Snatched', 'Matched')"
                else:
                    cmd = "select gr_id from books where status=?", (status,)
                results = db.select(cmd)
            elif library == 'AudioBook':
                if status == 'Open':
                    cmd = "select gr_id from books where audiostatus in ('Open', 'Have')"
                elif status == 'Wanted':
                    cmd = "select gr_id from books where audiostatus in ('Wanted', 'Snatched', 'Matched')"
                else:
                    cmd = "select gr_id from books where audiostatus=%s" % status
                results = db.select(cmd)
            else:  # 'Audio/eBook'
                if status == 'Open':
                    cmd = "select gr_id from books where status in ('Open', 'Have') or audiostatus in ('Open', 'Have')"
                elif status == 'Wanted':
                    cmd = ("select gr_id from books where status in ('Wanted', 'Snatched', 'Matched') or "
                           "audiostatus in ('Wanted', 'Snatched', 'Matched')")
                else:
                    cmd = "select gr_id from books where status=%s or audiostatus=%s" % (status, status)
                results = db.select(cmd)

            ll_list = []
            for terms in results:
                ll_list.append(terms['gr_id'])

        ga = GrAuth()
        gr = None
        shelves = ga.get_shelf_list()
        found = False
        for item in shelves:  # type: dict
            if item['name'].lower() == shelf.lower():
                found = True
                break
        if not found:
            if user:
                res, msg = ga.create_shelf(shelf=shelf, exclusive=True)
            else:
                res, msg = ga.create_shelf(shelf=shelf)
            if not res:
                logger.debug("Unable to create shelf %s: %s" % (shelf, msg))
                return 0, []
            else:
                # make sure no old info lying around
                if user:
                    db.match('DELETE from sync where UserID="goodreads" and Label=?', (usershelf,))
                else:
                    db.match('DELETE from sync where UserID="goodreads" and Label=?', (shelf,))
                logger.debug("Created new goodreads shelf: %s" % shelf)

        gr_shelf = ga.get_gr_shelf_contents(shelf=shelf)

        logger.info("There are %s %s %ss, %s on goodreads %s shelf" %
                    (len(ll_list), dstatus, library, len(gr_shelf), shelf))

        if reset and not CONFIG.get_bool('GR_SYNCREADONLY'):
            logger.info("Removing old goodreads shelf contents")
            for book in gr_shelf:
                try:
                    r, content = ga.book_to_list(book, shelf, action='remove')
                except Exception as e:
                    logger.error("Error removing %s from %s: %s %s" % (
                                    book, shelf, type(e).__name__, str(e)))
                    r = None
                    content = ''
                if r:
                    gr_shelf.remove(book)
                    loggergrsync.debug("%10s removed from %s shelf" % (book, shelf))
                else:
                    logger.warning("Failed to remove %s from %s shelf: %s" % (book, shelf, content))

        # Sync method for WANTED:
        # Get results of last_sync (if any)
        # For each book in last_sync
        #    if not in ll_list, new deletion, remove from gr_shelf
        #    if not in gr_shelf, new deletion, remove from ll_list, mark Skipped
        # For each book in ll_list
        #    if not in last_sync, new addition, add to gr_shelf
        # For each book in gr_shelf
        #    if not in last sync, new addition, add to ll_list, mark Wanted
        #
        # save ll WANTED as last_sync

        # For HAVE/OPEN method is the same, but only change status if HAVE, not OPEN
        if user:
            res = db.match('select SyncList from sync where UserID="goodreads" and Label=?', (usershelf,))
        else:
            res = db.match('select SyncList from sync where UserID="goodreads" and Label=?', (shelf,))
        last_sync = []
        shelf_changed = 0
        ll_changed = []
        if res and not reset:
            last_sync = get_list(res['SyncList'])

        added_to_shelf = list(set(gr_shelf) - set(last_sync) - set(ll_list))
        removed_from_shelf = list(set(last_sync) - set(gr_shelf))
        added_to_ll = list(set(ll_list) - set(last_sync) - set(gr_shelf))
        removed_from_ll = list(set(last_sync) - set(ll_list))
        # remove any that have no gr_id
        added_to_shelf = [i for i in added_to_shelf if i]
        removed_from_shelf = [i for i in removed_from_shelf if i]
        added_to_ll = [i for i in added_to_ll if i]
        removed_from_ll = [i for i in removed_from_ll if i]

        logger.info("%s missing from lazylibrarian %s" % (len(removed_from_ll), shelf))
        if removed_from_ll:
            logger.debug(', '.join(removed_from_ll))
        if not CONFIG.get_bool('GR_SYNCREADONLY'):
            for book in removed_from_ll:
                # first the deletions since last sync...
                try:
                    res, content = ga.book_to_list(book, shelf, action='remove')
                except Exception as e:
                    logger.error("Error removing %s from %s: %s %s" % (book, shelf, type(e).__name__, str(e)))
                    res = None
                    content = ''
                if res:
                    logger.debug("%10s removed from %s shelf" % (book, shelf))
                    shelf_changed += 1
                else:
                    if '404' not in content:  # already removed is ok
                        loggergrsync.warning("Failed to remove %s from %s shelf: %s" % (book, shelf, content))

        logger.info("%s missing from goodreads %s" % (len(removed_from_shelf), shelf))
        if removed_from_shelf:
            logger.debug(', '.join(removed_from_shelf))
        for book in removed_from_shelf:
            # deleted from goodreads
            cmd = "select Status,AudioStatus,BookName from books where gr_id=?"
            res = db.match(cmd, (book,))
            if not res:
                logger.debug('Adding new %s %s to database' % (library, book))
                if not gr:
                    gr = GoodReads(book)
                gr.find_book(book, None, None, "Added by grsync")
                res = db.match(cmd, (book,))
            if not res:
                logger.warning('%s %s not found in database' % (library, book))
            elif user:
                try:
                    ll_list.remove(book)
                    logger.debug("%10s removed from user %s" % (book, shelf))
                except ValueError:
                    pass
            else:
                if 'eBook' in library:
                    if res['Status'] in ['Have', 'Wanted']:
                        db.action("UPDATE books SET Status='Skipped' WHERE gr_id=?", (book,))
                        ll_changed.append(book)
                        logger.debug("%10s set to Skipped" % book)
                    else:
                        if res['Status'] == 'Open' and shelf == CONFIG['GR_OWNED']:
                            logger.warning("Adding book %s [%s] back to %s shelf" % (
                                res['BookName'], book, CONFIG['GR_OWNED']))
                            try:
                                _, _ = ga.book_to_list(book, shelf, action='add')
                            except Exception as e:
                                logger.error("Error adding %s back to %s: %s %s" % (
                                    book, CONFIG['GR_OWNED'], type(e).__name__, str(e)))
                        else:
                            logger.warning("Not marking %s [%s] as Skipped, book is marked %s" % (
                                        res['BookName'], book, res['Status']))

                if 'Audio' in library:
                    if res['AudioStatus'] in ['Have', 'Wanted']:
                        db.action("UPDATE books SET AudioStatus='Skipped' WHERE gr_id=?", (book,))
                        ll_changed.append(book)
                        logger.debug("%10s set to Skipped" % book)
                    else:
                        if res['AudioStatus'] == 'Open' and shelf == CONFIG['GR_AOWNED']:
                            logger.warning("Adding audiobook %s [%s] back to %s shelf" % (
                                res['BookName'], book, CONFIG['GR_AOWNED']))
                            try:
                                _, _ = ga.book_to_list(book, shelf, action='add')
                            except Exception as e:
                                logger.error("Error adding %s back to %s: %s %s" % (
                                    book, CONFIG['GR_AOWNED'], type(e).__name__, str(e)))
                        else:
                            logger.warning("Not marking %s [%s] as Skipped, audiobook is marked %s" % (
                                        res['BookName'], book, res['AudioStatus']))

        # new additions to lazylibrarian
        logger.info("%s new in lazylibrarian %s" % (len(added_to_ll), shelf))
        if added_to_ll:
            logger.debug(', '.join(added_to_ll))
        if not CONFIG.get_bool('GR_SYNCREADONLY'):
            for book in added_to_ll:
                try:
                    res, content = ga.book_to_list(book, shelf, action='add')
                except Exception as e:
                    logger.error("Error adding %s to %s: %s %s" % (book, shelf, type(e).__name__, str(e)))
                    res = None
                    content = ''
                if res:
                    logger.debug("%10s added to %s shelf" % (book, shelf))
                    shelf_changed += 1
                else:
                    if '404' in content:
                        bookinfo = db.match("SELECT BookName from books where gr_id=?", (book,))
                        if bookinfo:
                            content = "%s: %s" % (content, bookinfo['BookName'])
                    logger.warning("Failed to add %s to %s shelf: %s" % (book, shelf, content))

        # new additions to goodreads shelf
        logger.info("%s new in goodreads %s" % (len(added_to_shelf), shelf))
        if added_to_shelf:
            logger.debug(', '.join(added_to_shelf))
        for book in added_to_shelf:
            cmd = "select Status,AudioStatus,BookName from books where gr_id=?"
            res = db.match(cmd, (book,))
            if not res:
                logger.debug('Adding new book %s to database' % book)
                if not gr:
                    gr = GoodReads(book)
                gr.find_book(book, None, None, "Added by grsync")
                res = db.match(cmd, (book,))
            if not res:
                logger.warning('Book %s not found in database' % book)
            elif user:
                ll_list.append(book)
                logger.debug("%10s added to user %s" % (book, shelf))
                shelf_changed += 1
                perm = check_int(user['Perms'], 0)
                if status == 'Wanted' and perm & lazylibrarian.perm_status:
                    if CONFIG.get_bool('EBOOK_TAB') and res['Status'] not in ['Open', 'Have']:
                        db.action("UPDATE books SET Status='Wanted' WHERE gr_id=?", (book,))
                        ll_changed.append(book)
                        logger.debug("%10s set to Wanted" % book)
                    if CONFIG.get_bool('AUDIO_TAB') and res['AudioStatus'] not in ['Open', 'Have']:
                        db.action("UPDATE books SET AudioStatus='Wanted' WHERE gr_id=?", (book,))
                        ll_changed.append(book)
                        logger.debug("%10s set to Wanted" % book)
            else:
                if 'eBook' in library:
                    if status == 'Open':
                        if res['Status'] == 'Open':
                            loggergrsync.warning("%s [%s] is already marked Open" % (res['BookName'], book))
                        else:
                            db.action("UPDATE books SET Status='Have' WHERE gr_id=?", (book,))
                            ll_changed.append(book)
                            logger.debug("%10s set to Have" % book)
                    elif status == 'Wanted':
                        # if in "wanted" and already marked "Open/Have", optionally delete from "wanted"
                        # (depending on user prefs, to-read and wanted might not be the same thing)
                        if CONFIG.get_bool('GR_UNIQUE') and res['Status'] in ['Open', 'Have'] \
                                and not CONFIG.get_bool('GR_SYNCREADONLY'):
                            try:
                                r, content = ga.book_to_list(book, shelf, action='remove')
                            except Exception as e:
                                logger.error("Error removing %s [%s] from %s: %s %s" % (
                                             res['BookName'], book, shelf, type(e).__name__, str(e)))
                                r = None
                                content = ''
                            if r:
                                logger.debug("%10s removed from %s shelf" % (book, shelf))
                                shelf_changed += 1
                            else:
                                logger.warning("Failed to remove %s from %s shelf: %s" % (book, shelf, content))
                        elif res['Status'] not in ['Open', 'Have']:
                            db.action("UPDATE books SET Status='Wanted' WHERE gr_id=?", (book,))
                            ll_changed.append(book)
                            logger.debug("%10s set to Wanted" % book)
                        else:
                            logger.warning("Not setting %s [%s] as Wanted, already marked %s" %
                                           (res['BookName'], book, res['Status']))
                if 'Audio' in library:
                    if status == 'Open':
                        if res['AudioStatus'] == 'Open':
                            loggergrsync.warning("%s [%s] is already marked Open" % (res['BookName'], book))
                        else:
                            db.action("UPDATE books SET AudioStatus='Have' WHERE gr_id=?", (book,))
                            ll_changed.append(book)
                            logger.debug("%10s set to Have" % book)
                    elif status == 'Wanted':
                        # if in "wanted" and already marked "Open/Have", optionally delete from "wanted"
                        # (depending on user prefs, to-read and wanted might not be the same thing)
                        if CONFIG.get_bool('GR_UNIQUE') and res['AudioStatus'] in ['Open', 'Have'] \
                                and not CONFIG.get_bool('GR_SYNCREADONLY'):
                            try:
                                r, content = ga.book_to_list(book, shelf, action='remove')
                            except Exception as e:
                                logger.error("Error removing %s [%s] from %s: %s %s" % (
                                             res['BookName'], book, shelf, type(e).__name__, str(e)))
                                r = None
                                content = ''
                            if r:
                                logger.debug("%10s removed from %s shelf" % (book, shelf))
                                shelf_changed += 1
                            else:
                                logger.warning("Failed to remove %s from %s shelf: %s" % (book, shelf, content))
                        elif res['AudioStatus'] not in ['Open', 'Have']:
                            db.action("UPDATE books SET AudioStatus='Wanted' WHERE gr_id=?", (book,))
                            ll_changed.append(book)
                            logger.debug("%10s set to Wanted" % book)
                        else:
                            logger.warning("Not setting %s [%s] as Wanted, already marked %s" %
                                           (res['BookName'], book, res['Status']))

        # set new definitive list for ll
        if user:
            new_value_dict = {}
            control_value_dict = {"UserID": user['UserID']}
            ll_set = set(ll_list)
            count = len(ll_set)
            books = ', '.join(str(x) for x in ll_set)

            if shelf == 'read':
                new_value_dict = {'HaveRead': books}
            elif shelf == 'currently-reading':
                new_value_dict = {'Reading': books}
            elif shelf == 'to-read':
                new_value_dict = {'ToRead': books}
            elif shelf == 'abandoned':
                new_value_dict = {'Abandoned': books}
            if new_value_dict:
                db.upsert("users", new_value_dict, control_value_dict)

                if shelf_changed:
                    for exclusive_shelf in ['HaveRead', 'ToRead', 'Reading' 'Abandoned']:
                        if exclusive_shelf not in new_value_dict:
                            old_set = set(get_list(user[exclusive_shelf]))
                            new_set = old_set - ll_set
                            if len(old_set) != len(new_set):
                                new_list = ', '.join(str(x) for x in new_set)
                                db.upsert("users", {exclusive_shelf: new_list}, control_value_dict)
                                logger.debug("Removed duplicates from %s shelf" % exclusive_shelf)
        else:
            # get new definitive list from ll
            if 'eBook' in library:
                cmd = "select gr_id from books where status=?"
                if status == 'Open':
                    cmd += " or status='Have'"
                if 'Audio' in library:
                    cmd += " or audiostatus=?"
                    if status == 'Open':
                        cmd += " or audiostatus='Have'"
                    results = db.select(cmd, (status, status))
                else:
                    results = db.select(cmd, (status,))
            else:
                cmd = "select gr_id from books where audiostatus=?"
                if status == 'Open':
                    cmd += " or audiostatus='Have'"
                results = db.select(cmd, (status,))

            ll_list = []
            for terms in results:
                ll_list.append(terms['gr_id'])
            ll_set = set(ll_list)
            count = len(ll_set)
            books = ', '.join(str(x) for x in ll_set)

        # store as comparison for next sync
        if shelf != 'unread':
            if user:
                control_value_dict = {"UserID": "goodreads", "Label": usershelf}
            else:
                control_value_dict = {"UserID": "goodreads", "Label": shelf}
            new_value_dict = {"Date": str(time.time()), "Synclist": books}
            # goodreads user does not exist in user table
            db.action('PRAGMA foreign_keys = OFF')
            db.upsert("sync", new_value_dict, control_value_dict)
            db.action('PRAGMA foreign_keys = ON')
        logger.debug('Sync %s to %s shelf complete, contains %s' % (status, shelf, count))
        return shelf_changed, ll_changed

    except Exception:
        logger.error('Unhandled exception in grsync: %s' % traceback.format_exc())
        return 0, []
    finally:
        db.close()
