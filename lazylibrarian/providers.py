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

import json
import re
import time
from xml.etree import ElementTree
from typing import Dict

from urllib.parse import urlencode, urlparse

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.cache import fetch_url
from lazylibrarian.filesystem import syspath
from lazylibrarian.directparser import direct_gen, direct_bok, direct_bfi
from lazylibrarian.formatter import age, today, plural, clean_name, unaccented, get_list, check_int, \
    make_unicode, seconds_to_midnight, make_utf8bytes, no_umlauts, month2num
from lazylibrarian.ircbot import irc_connect, irc_search, irc_results, irc_leave
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.torrentparser import torrent_kat, torrent_tpb, torrent_wwt, torrent_zoo, torrent_tdl, \
    torrent_trf, torrent_lime

import lib.feedparser as feedparser

import html5lib
from bs4 import BeautifulSoup


def test_provider(name: str, host=None, api=None):
    print(1,host, name, api)
    db = database.DBConnection()
    res = db.match("SELECT authorname,authorid from authors order by totalbooks desc")
    if res:
        testname = res['authorname']
        testid = res['authorid']
        res = db.match("SELECT bookname from books where authorid=? order by bookrate desc", (testid,))
        if res:
            testbook = res['bookname']
        else:
            testbook = ''
    else:
        testname = "Agatha Christie"
        testbook = "Poirot"

    book = {'searchterm': testname, 'authorName': testname, 'library': 'eBook', 'bookid': '1',
            'bookName': testbook, 'bookSub': ''}

    if name == 'TPB':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('TPB_HOST', host)
        return torrent_tpb(book, test=True), "Pirate Bay"
    if name == 'WWT':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('WWT_HOST', host)
        return torrent_wwt(book, test=True), "WorldWideTorrents"
    if name == 'KAT':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('KAT_HOST', host)
        return torrent_kat(book, test=True), "KickAss Torrents"
    if name == 'ZOO':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('ZOO_HOST', host)
        return torrent_zoo(book, test=True), "Zooqle"
    if name == 'LIME':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('LIME_HOST', host)
        return torrent_lime(book, test=True), "LimeTorrents"
    if name == 'TDL':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('TDL_HOST', host)
        return torrent_tdl(book, test=True), "TorrentDownloads"
    if name == 'TRF':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('TRF_HOST', host)
        return torrent_trf(book, test=True), "Torrof"

    if name.startswith('gen_'):
        for provider in lazylibrarian.CONFIG.providers('GEN'):
            if provider['NAME'].lower() == name:
                if provider['DISPNAME']:
                    name = provider['DISPNAME']
                logger.debug("Testing %s" % name)
                if host:
                    provider['HOST'] = host
                if api:
                    provider['SEARCH'] = api
                return direct_gen(book, prov=provider['NAME'].lower(), test=True), name

    if name == 'BOK':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('BOK_HOST', host)
        return direct_bok(book, prov=name, test=True), "ZLibrary"
    if name == 'BFI':
        logger.debug("Testing provider %s" % name)
        if host:
            lazylibrarian.CONFIG.set_str('BFI_HOST', host)
        return direct_bfi(book, prov=name, test=True), "BookFi"

    if name.startswith('rss_'):
        try:
            for provider in lazylibrarian.CONFIG.providers('RSS'):
                if provider['NAME'].lower() == name:
                    if provider['DISPNAME']:
                        name = provider['DISPNAME']
                    logger.debug("Testing provider %s" % name)

                    if not host:
                        host = provider['HOST']
                    if 'goodreads' in host:
                        if 'list_rss' in host:
                            return goodreads(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                             provider['DISPNAME'], test=True), provider['DISPNAME']
                        if '/show/' in host or '/book/' in host:
                            # goodreads listopia html page
                            return listopia(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                            provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'amazon' in host and '/charts' in host:
                        return amazon(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                      provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'nytimes' in host:
                        return ny_times(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                        provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'publishersweekly' in host:
                        return publishersweekly(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                                provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'apps.npr.org' in host:
                        return appsnprorg(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                          provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'penguinrandomhouse' in host:
                        return penguinrandomhouse(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                                  provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'barnesandnoble' in host:
                        return barnesandnoble(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                              provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'bookdepository' in host:
                        return bookdepository(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                              provider['DISPNAME'], test=True), provider['DISPNAME']
                    elif 'indigo' in host:
                        return indigo(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                      provider['DISPNAME'], test=True), provider['DISPNAME']
                    else:
                        return rss(host, provider['NAME'], provider.get_int('DLPRIORITY'),
                                   provider['DISPNAME'], test=True), provider['DISPNAME']
        except IndexError:
            pass
        except Exception as e:
            logger.debug("Exception: %s" % str(e))

    if name.startswith('apprise_'):
        for provider in lazylibrarian.CONFIG.providers('APPRISE'):
            if provider['NAME'].lower() == name:
                if provider['DISPNAME']:
                    name = provider['DISPNAME']
                logger.debug("Testing notifier %s" % name)
                # noinspection PyUnresolvedReferences
                noti = lazylibrarian.notifiers.apprise_notify.AppriseNotifier()
                return noti.test_notify(host), name
        return False, name

    # for torznab/newznab get capabilities first, unless locked,
    # then try book search if enabled, fall back to general search
    if name.startswith('torznab_'):
        try:
            for provider in lazylibrarian.CONFIG.providers('TORZNAB'):
                if provider['NAME'].lower() == name:
                    if provider['DISPNAME']:
                        name = provider['DISPNAME']
                    logger.debug("Testing provider %s" % name)
                    if provider.get_bool('MANUAL'):
                        logger.debug("Capabilities are set to manual for %s" % provider['NAME'])
                    else:
                        if host:
                            if host[-1:] == '/':
                                host = host[:-1]
                            provider['HOST'] = host
                        if api:
                            ap, seed = api.split(' : ', 1)
                            provider['API'] = ap
                            provider.set_int('SEEDERS', seed)
                        provider = get_capabilities(provider, force=True)

                    if provider['BOOKSEARCH']:
                        success, error_msg = newznab_plus(book, provider, 'book', 'torznab', True)
                        if not success:
                            if cancel_search_type('book', error_msg, provider):
                                success, _ = newznab_plus(book, provider, 'generalbook', 'torznab', True)
                    else:
                        success, _ = newznab_plus(book, provider, 'generalbook', 'torznab', True)

                    return success, provider['DISPNAME']
        except IndexError:
            pass
        except Exception as e:
            logger.debug("Exception: %s" % str(e))

    if name.startswith('newznab_'):
        try:
            for provider in lazylibrarian.CONFIG.providers('NEWZNAB'):
                if provider['NAME'].lower() == name:
                    if provider['DISPNAME']:
                        name = provider['DISPNAME']
                    logger.debug("Testing provider %s" % name)
                    if provider.get_bool('MANUAL'):
                        logger.debug("Capabilities are set to manual for %s" % provider['NAME'])
                    else:
                        if host:
                            provider['HOST'] = host
                        if api:
                            provider['API'] = api

                        provider = get_capabilities(provider, force=True)
                    if provider['BOOKSEARCH']:
                        success, error_msg = newznab_plus(book, provider, 'book', 'newznab', True)
                        if not success:
                            if cancel_search_type('book', error_msg, provider):
                                success, _ = newznab_plus(book, provider, 'generalbook', 'newznab', True)
                    else:
                        success, _ = newznab_plus(book, provider, 'generalbook', 'newznab', True)
                    return success, provider['DISPNAME']
        except IndexError:
            pass
        except Exception as e:
            logger.debug("Exception: %s" % str(e))

    if name.startswith('irc_'):
        try:
            for provider in lazylibrarian.CONFIG.providers('IRC'):
                if provider['NAME'].lower() == name:
                    if provider['DISPNAME']:
                        name = provider['DISPNAME']
                        if host:
                            server, channel = host.split(' : ', 1)
                            provider['SERVER'] = server
                            provider['CHANNEL'] = channel
                        if api:
                            snick, spass, ssearch = api.split(' : ', 2)
                            provider['BOTNICK'] = snick
                            provider['BOTPASS'] = spass
                            provider['SEARCH'] = ssearch
                    logger.debug("Testing provider %s" % name)
                    provider.set_connection(None)  # start a new connection
                    success, _ = ircsearch(book, provider, "book", True)
                    return success, name
        except IndexError:
            pass
        except Exception as e:
            logger.debug("Exception: %s" % str(e))

    msg = "Unknown provider [%s]" % name
    logger.error(msg)
    return False, msg


def get_searchterm(book, search_type):
    authorname = clean_name(book['authorName'], "'")
    bookname = clean_name(book['bookName'], "'")
    if search_type in ['book', 'audio'] or 'short' in search_type:
        if bookname == authorname and book['bookSub']:
            # books like "Spike Milligan: Man of Letters"
            # where we split the title/subtitle on ':'
            bookname = clean_name(book['bookSub'], "'")
        if bookname.startswith(authorname) and len(bookname) > len(authorname):
            # books like "Spike Milligan In his own words"
            # where we don't want to look for "Spike Milligan Spike Milligan In his own words"
            bookname = bookname[len(authorname) + 1:]
        bookname = bookname.strip()

        # no initials or extensions after surname eg L. E. Modesitt Jr. -> Modesitt
        # and Charles H. Elliott, Phd -> Charles Elliott
        # but Tom Holt -> Tom Holt
        # Calibre directories may have trailing '.' replaced by '_'  eg Jr_
        authorname = authorname.replace('.', ' ').replace('_', ' ')
        if ' ' in authorname:
            authorname_exploded = authorname.split()
            authorname = ''
            postfix = get_list(lazylibrarian.CONFIG['NAME_POSTFIX'])
            for word in authorname_exploded:
                # word = word.rstrip('.').rstrip('_')
                if len(word) > 1 and word.lower() not in postfix:
                    if authorname:
                        authorname += ' '
                    authorname += word

        if 'short' in search_type and '(' in bookname:
            bookname = bookname.split('(')[0].strip()

    bookname = bookname.replace('#', '_').replace('/', '_')
    return authorname, bookname


def get_capabilities(provider: ConfigDict, force=False):
    """
    query provider for caps if none loaded yet, or if config entry is too old and not set manually.
    """
    if not force and len(provider['UPDATED']) == 10:  # any stored values?
        match = True
        if (age(provider['UPDATED']) > lazylibrarian.CONFIG.get_int('CACHE_AGE')) and not provider.get_bool('MANUAL'):
            logger.debug('Stored capabilities for %s are too old' % provider['HOST'])
            match = False
    else:
        match = False

    if match:
        logger.debug('Using stored capabilities for %s' % provider['HOST'])
    else:
        host = provider['HOST']
        if not str(host[:4]) == "http":
            host = 'http://' + host
        if host[-1:] == '/':
            host = host[:-1]
        if host[-4:] == '/api':
            url = host + '?t=caps'
        else:
            url = host + '/api?t=caps'

        # most providers will give you caps without an api key
        logger.debug('Requesting capabilities for %s' % url)
        source_xml, success = fetch_url(url, retry=False, raw=True)
        data = None
        if not success:
            logger.debug("Error getting xml from %s, %s" % (url, source_xml))
        else:
            try:
                data = ElementTree.fromstring(source_xml)
                if data.tag == 'error':
                    logger.debug("Unable to get capabilities: %s" % data.attrib)
                    success = False
            except (ElementTree.ParseError, UnicodeEncodeError):
                logger.debug("Error parsing xml from %s, %s" % (url, repr(source_xml)))
                success = False
        if not success:
            # If it failed, retry with api key
            if provider['API']:
                url = url + '&apikey=' + provider['API']
                logger.debug('Retrying capabilities with apikey for %s' % url)
                source_xml, success = fetch_url(url, raw=True)
                if not success:
                    logger.debug("Error getting xml from %s, %s" % (url, source_xml))
                else:
                    try:
                        data = ElementTree.fromstring(source_xml)
                        if data.tag == 'error':
                            logger.debug("Unable to get capabilities: %s" % data.attrib)
                            success = False
                    except (ElementTree.ParseError, UnicodeEncodeError):
                        logger.debug("Error parsing xml from %s, %s" % (url, repr(source_xml)))
                        success = False
            else:
                logger.debug('Unable to retry capabilities, no apikey for %s' % url)

        if not success:
            logger.warn("Unable to get capabilities for %s: No data returned" % url)
            # might be a temporary error
            if provider['BOOKCAT'] or provider['MAGCAT'] or provider['AUDIOCAT']:
                logger.debug('Using old stored capabilities for %s' % provider['HOST'])
            else:
                # or might be provider doesn't do caps
                logger.debug('Using default capabilities for %s' % provider['HOST'])
                for key in ['GENERALSEARCH', 'EXTENDED', 'BOOKCAT', 'AUDIOCAT', 'COMICCAT', 'MAGCAT',
                    'BOOKSEARCH', 'MAGSEARCH', 'AUDIOSEARCH', 'COMICSEARCH']:
                    item = provider.get_item(key)
                    if item:
                        item.reset_to_default()
                provider['UPDATED'] = str(today)
                provider.set_int('APILIMIT', 0)
                provider.set_int('RATELIMIT', 0)
                lazylibrarian.CONFIG.save_config_and_backup_old(section=provider['NAME'])
        elif data is not None:
            logger.debug("Parsing xml for capabilities of %s" % url)
            #
            # book search isn't mentioned in the caps xml returned by
            # nzbplanet,jackett,oznzb,usenet-crawler, so we can't use it as a test
            # but the newznab+ ones usually support t=book and categories in 7000 range
            # whereas nZEDb ones don't support t=book and use categories in 8000 range
            # also some providers give searchtype but no supportedparams, so we still
            # can't tell what queries will be accepted
            # also category names can be lowercase or Mixed, magazine subcat name isn't
            # consistent, and subcat can be just subcat or category/subcat subcat > lang
            # eg "Magazines" "Mags" or "Books/Magazines" "Mags > French"
            # Load all languages for now as we don't know which the user might want
            #
            #  set some defaults
            #
            for key in ['GENERALSEARCH', 'EXTENDED', 'BOOKCAT', 'AUDIOCAT', 'COMICCAT', 'MAGCAT',
                'BOOKSEARCH', 'MAGSEARCH', 'AUDIOSEARCH', 'COMICSEARCH']:
                item = provider.get_item(key)
                if item:
                    item.reset_to_default()
            search = data.find('searching/search')
            if search is not None:
                # noinspection PyUnresolvedReferences
                if 'available' in search.attrib:
                    # noinspection PyUnresolvedReferences
                    if search.attrib['available'] == 'yes':
                        provider['GENERALSEARCH'] = 'search'
            limits = data.find('limits')
            if limits is not None:
                try:
                    limit = limits.attrib.get('apimax')
                    if limit:
                        provider.set_int('APILIMIT', check_int(limit, 0))
                        logger.debug(f"{provider['HOST']} apilimit {limit}")
                except Exception as e:
                    logger.debug('Error getting apilimit from %s: %s %s' % (provider['HOST'],
                                                                            type(e).__name__, str(e)))

            categories = data.iter('category')
            for cat in categories:
                if 'name' in cat.attrib:
                    if cat.attrib['name'].lower() == 'audio':
                        provider['AUDIOCAT'] = cat.attrib['id']
                        subcats = cat.iter('subcat')
                        if not subcats:
                            subcats = cat.iter('subCategories')
                        for subcat in subcats:
                            if 'audiobook' in subcat.attrib['name'].lower():
                                provider['AUDIOCAT'] = subcat.attrib['id']

                    elif cat.attrib['name'].lower() == 'books':
                        provider['BOOKCAT'] = cat.attrib['id']
                        # if no specific magazine/comic subcategory, use books
                        provider['MAGCAT'] = cat.attrib['id']
                        provider['COMICCAT'] = cat.attrib['id']
                        # set default booksearch
                        if provider['BOOKCAT'] == '7000':
                            # looks like newznab+, should support book-search
                            provider['BOOKSEARCH'] = 'book'
                        else:
                            # looks like nZEDb, probably no book-search
                            provider['BOOKSEARCH'] = ''                        # but check in case we got some settings back
                        search = data.find('searching/book-search')
                        if search:
                            # noinspection PyUnresolvedReferences
                            if 'available' in search.attrib:
                                # noinspection PyUnresolvedReferences
                                if search.attrib['available'] == 'yes':
                                    provider['BOOKSEARCH'] = 'book'
                                else:
                                    provider['BOOKSEARCH'] = ''
                        # subcategories override main category (not in addition to)
                        # but allow multile subcategories (mags->english, mags->french)
                        subcats = cat.iter('subcat')
                        if not subcats:
                            subcats = cat.iter('subCategories')
                        ebooksubs = ''
                        magsubs = ''
                        comicsubs = ''
                        for subcat in subcats:
                            if 'ebook' in subcat.attrib['name'].lower():
                                if ebooksubs:
                                    ebooksubs = ebooksubs + ','
                                ebooksubs = ebooksubs + subcat.attrib['id']
                            if 'magazines' in subcat.attrib['name'].lower() or 'mags' in subcat.attrib['name'].lower():
                                if magsubs:
                                    magsubs = magsubs + ','
                                magsubs = magsubs + subcat.attrib['id']
                            if 'comic' in subcat.attrib['name'].lower():
                                if comicsubs:
                                    comicsubs = comicsubs + ','
                                comicsubs = comicsubs + subcat.attrib['id']
                        if ebooksubs:
                            provider['BOOKCAT'] = ebooksubs
                        if magsubs:
                            provider['MAGCAT'] = magsubs
                        if comicsubs:
                            provider['COMICCAT'] = comicsubs
            logger.info("Categories: Books %s : Mags %s : Audio %s : Comic %s : BookSearch '%s'" %
                        (provider['BOOKCAT'], provider['MAGCAT'], provider['AUDIOCAT'], provider['COMICCAT'],
                         provider['BOOKSEARCH']))
            provider['UPDATED'] = today()
            lazylibrarian.CONFIG.save_config_and_backup_old(section=provider['NAME'])
    return provider


def provider_is_blocked(name: str):
    """ Check if provider is blocked because of previous errors """
    # Reset api counters if it's a new day
    if lazylibrarian.NABAPICOUNT != today():
        lazylibrarian.NABAPICOUNT = today()
        for provider in lazylibrarian.CONFIG.providers('NEWZNAB'):
            provider.set_int('APICOUNT', 0)
        for provider in lazylibrarian.CONFIG.providers('TORZNAB'):
            provider.set_int('APICOUNT', 0)

    timenow = int(time.time())
    for entry in lazylibrarian.PROVIDER_BLOCKLIST:
        if entry["name"] == name:
            if timenow < int(entry['resume']):
                return True
            else:
                lazylibrarian.PROVIDER_BLOCKLIST.remove(entry)
    return False


def block_provider(who, why, delay=None):
    if delay is None:
        delay = lazylibrarian.CONFIG.get_int('BLOCKLIST_TIMER')
    if len(why) > 80:
        why = why[:80]
    if not delay:
        logger.debug('Not blocking %s,%s as timer is zero' % (who, why))
    else:
        mins = int(delay / 60) + (delay % 60 > 0)
        logger.info("Blocking provider %s for %s minutes because %s" % (who, mins, why))
        timenow = int(time.time())
        for entry in lazylibrarian.PROVIDER_BLOCKLIST:
            if entry["name"] == who:
                lazylibrarian.PROVIDER_BLOCKLIST.remove(entry)
        newentry = {"name": who, "resume": timenow + delay, "reason": why}
        lazylibrarian.PROVIDER_BLOCKLIST.append(newentry)
    logger.debug("Provider Blocklist contains %s %s" % (len(lazylibrarian.PROVIDER_BLOCKLIST),
                                                        plural(len(lazylibrarian.PROVIDER_BLOCKLIST), 'entry')))


def iterate_over_newznab_sites(book=None, search_type=None):
    """
    Purpose of this function is to read the config file, and loop through all active NewsNab+
    sites and return the compiled results list from all sites back to the caller
    We get called with book[] and searchType of "book", "mag", "general" etc
    """

    resultslist = []
    providers = 0

    for provider in lazylibrarian.CONFIG.providers('NEWZNAB'):
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s" % (provider['HOST'], provider['ENABLED'], provider['DLTYPES']))
        if provider['ENABLED'] and search_type:
            ignored = False
            if provider_is_blocked(provider['HOST']):
                logger.debug('%s is BLOCKED' % provider['HOST'])
                ignored = True
            elif "book" in search_type and 'E' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for eBook" % provider['HOST'])
                ignored = True
            elif "audio" in search_type and 'A' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for AudioBook" % provider['HOST'])
                ignored = True
            elif "mag" in search_type and 'M' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for Magazine" % provider['HOST'])
                ignored = True
            elif "comic" in search_type and 'C' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for Comic" % provider['HOST'])
                ignored = True
            if not ignored:
                if provider.get_int('APILIMIT'):
                    if 'APICOUNT' in provider:
                        res = provider.get_int('APICOUNT')
                    else:
                        res = 0
                    if res >= provider.get_int('APILIMIT'):
                        block_provider(provider['HOST'], 'Reached Daily API limit (%s)' %
                                       provider['APILIMIT'], delay=seconds_to_midnight())
                    else:
                        provider.set_int('APICOUNT', res + 1)

                if not provider_is_blocked(provider['HOST']):
                    ratelimit = provider.get_int('RATELIMIT')
                    if ratelimit:
                        if provider.get_int('LASTUSED') > 0:
                            delay = provider.get_int('LASTUSED') + ratelimit - time.time()
                            if delay > 0:
                                time.sleep(delay)
                        provider.set_int('LASTUSED', int(time.time()))

                    provider = get_capabilities(provider)
                    providers += 1
                    logger.debug('Querying provider %s' % provider['HOST'])
                    resultslist += newznab_plus(book, provider, search_type, "nzb")[1]

    for provider in lazylibrarian.CONFIG.providers('TORZNAB'):
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s" % (provider['HOST'], provider['ENABLED'], provider['DLTYPES']))
        if provider['ENABLED'] and search_type:
            ignored = False
            if provider_is_blocked(provider['HOST']):
                logger.debug('%s is BLOCKED' % provider['HOST'])
                ignored = True
            elif search_type in ['book', 'shortbook', 'titlebook'] and 'E' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for eBook" % provider['HOST'])
                ignored = True
            elif "audio" in search_type and 'A' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for AudioBook" % provider['HOST'])
                ignored = True
            elif "mag" in search_type and 'M' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for Magazine" % provider['HOST'])
                ignored = True
            elif "comic" in search_type and 'C' not in provider['DLTYPES']:
                logger.debug("Ignoring %s for Comic" % provider['HOST'])
                ignored = True
            if not ignored:
                if provider.get_int('APILIMIT'):
                    if 'APICOUNT' in provider:
                        res = check_int(provider['APICOUNT'], 0)
                    else:
                        res = 0
                    if res >= provider.get_int('APILIMIT'):
                        block_provider(provider['HOST'], 'Reached Daily API limit (%s)' %
                                       provider['APILIMIT'], delay=seconds_to_midnight())
                    else:
                        provider.set_int('APICOUNT', res + 1)

                if not provider_is_blocked(provider['HOST']):
                    ratelimit = provider.get_int('RATELIMIT')
                    if ratelimit:
                        if provider.get_int('LASTUSED') > 0:
                            delay = provider.get_int('LASTUSED') + ratelimit - time.time()
                            if delay > 0:
                                time.sleep(delay)
                        provider.set_int('LASTUSED', int(time.time()))

                    provider = get_capabilities(provider)
                    providers += 1
                    logger.debug('[IterateOverTorzNabSites] - %s' % provider['HOST'])
                    resultslist += newznab_plus(book, provider, search_type, "torznab")[1]

    return resultslist, providers


def iterate_over_torrent_sites(book=None, search_type=None):
    resultslist = []
    providers = 0

    if search_type and search_type not in ['mag', 'comic'] and not search_type.startswith('general'):
        authorname, bookname = get_searchterm(book, search_type)
        if 'title' in search_type:
            book['searchterm'] = bookname
        else:
            book['searchterm'] = authorname + ' ' + bookname
        book['searchterm'] = no_umlauts(book['searchterm'])

    for prov in ['KAT', 'TPB', 'WWT', 'ZOO', 'TDL', 'TRF', 'LIME']:
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s" % (prov, lazylibrarian.CONFIG[prov],
                                             lazylibrarian.CONFIG[prov + '_DLTYPES']))
        if lazylibrarian.CONFIG[prov]:
            ignored = False
            if provider_is_blocked(prov):
                logger.debug('%s is BLOCKED' % lazylibrarian.CONFIG[prov + '_HOST'])
                ignored = True
            elif search_type in ['book', 'shortbook', 'titlebook'] and \
                    'E' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for eBook" % prov)
                ignored = True
            elif "audio" in search_type and 'A' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for AudioBook" % prov)
                ignored = True
            elif "mag" in search_type and 'M' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for Magazine" % prov)
                ignored = True
            elif "comic" in search_type and 'C' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for Comic" % prov)
                ignored = True
            if not ignored:
                logger.debug('[iterate_over_torrent_sites] - %s' % lazylibrarian.CONFIG[prov + '_HOST'])
                if prov == 'KAT':
                    results, error = torrent_kat(book)
                elif prov == 'TPB':
                    results, error = torrent_tpb(book)
                elif prov == 'WWT':
                    results, error = torrent_wwt(book)
                elif prov == 'ZOO':
                    results, error = torrent_zoo(book)
                elif prov == 'TRF':
                    results, error = torrent_trf(book)
                elif prov == 'TDL':
                    results, error = torrent_tdl(book)
                elif prov == 'LIME':
                    results, error = torrent_lime(book)
                else:
                    results = ''
                    error = ''
                    logger.error('iterate_over_torrent_sites called with unknown provider [%s]' % prov)

                if error:
                    block_provider(prov, error)
                else:
                    resultslist += results
                    providers += 1

    return resultslist, providers


def iterate_over_direct_sites(book=None, search_type=None):
    resultslist = []
    providers = 0
    if search_type not in ['mag', 'comic'] and not search_type.startswith('general'):
        authorname, bookname = get_searchterm(book, search_type)
        if 'title' in search_type:
            book['searchterm'] = bookname
        else:
            book['searchterm'] = authorname + ' ' + bookname

    for prov in lazylibrarian.CONFIG.providers('GEN'):
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s" % (prov['NAME'], prov['ENABLED'], prov['DLTYPES']))
        if prov.get_bool('ENABLED'):
            ignored = False
            if provider_is_blocked(prov['NAME']):
                logger.debug('%s is BLOCKED' % prov['NAME'])
                ignored = True
            elif search_type in ['book', 'shortbook', 'titlebook'] and 'E' not in prov['DLTYPES']:
                logger.debug("Ignoring %s for eBook" % prov['NAME'])
                ignored = True
            elif "audio" in search_type and 'A' not in prov['DLTYPES']:
                logger.debug("Ignoring %s for AudioBook" % prov['NAME'])
                ignored = True
            elif "mag" in search_type and 'M' not in prov['DLTYPES']:
                logger.debug("Ignoring %s for Magazine" % prov['NAME'])
                ignored = True
            elif "comic" in search_type and 'C' not in prov['DLTYPES']:
                logger.debug("Ignoring %s for Comic" % prov['NAME'])
                ignored = True
            if not ignored:
                logger.debug('Querying %s' % prov['NAME'])
                results, error = direct_gen(book, prov['NAME'])
                if error:
                    block_provider(prov['NAME'], error)
                else:
                    resultslist += results
                    providers += 1

    for prov in ['BOK']:
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s" % (prov, lazylibrarian.CONFIG[prov],
                                             lazylibrarian.CONFIG[prov + '_DLTYPES']))
        if lazylibrarian.CONFIG[prov]:
            ignored = False
            if provider_is_blocked('zlibrary'):
                logger.debug('zlibrary is BLOCKED')
                ignored = True
            elif search_type in ['book', 'shortbook', 'titlebook'] and \
                    'E' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for eBook" % prov)
                ignored = True
            elif "audio" in search_type and 'A' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for AudioBook" % prov)
                ignored = True
            elif "mag" in search_type and 'M' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for Magazine" % prov)
                ignored = True
            elif "comic" in search_type and 'C' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for Comic" % prov)
                ignored = True
            if not ignored:
                logger.debug('Querying %s' % prov)
                results, error = direct_bok(book, prov)
                if error:
                    # use a short delay for site unavailable etc
                    delay = lazylibrarian.CONFIG.get_int('BLOCKLIST_TIMER')
                    count, oldest = lazylibrarian.bok_dlcount()
                    if count and count >= lazylibrarian.CONFIG.get_int('BOK_DLLIMIT'):
                        # rolling 24hr delay if limit reached
                        delay = oldest + 24*60*60 - time.time()
                    block_provider('zlibrary', error, delay=delay)
                else:
                    resultslist += results
                    providers += 1

    for prov in ['BFI']:
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s" % (prov, lazylibrarian.CONFIG[prov],
                                             lazylibrarian.CONFIG[prov + '_DLTYPES']))
        if lazylibrarian.CONFIG[prov]:
            ignored = False
            if provider_is_blocked(prov):
                logger.debug('%s is BLOCKED' % prov)
                ignored = True
            elif search_type in ['book', 'shortbook', 'titlebook'] and \
                    'E' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for eBook" % prov)
                ignored = True
            elif "audio" in search_type and 'A' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for AudioBook" % prov)
                ignored = True
            elif "mag" in search_type and 'M' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for Magazine" % prov)
                ignored = True
            elif "comic" in search_type and 'C' not in lazylibrarian.CONFIG[prov + '_DLTYPES']:
                logger.debug("Ignoring %s for Comic" % prov)
                ignored = True
            if not ignored:
                logger.debug('Querying %s' % prov)
                results, error = direct_bfi(book, prov)
                if error:
                    block_provider(prov, error)
                else:
                    resultslist += results
                    providers += 1

    return resultslist, providers


def iterate_over_rss_sites():
    resultslist = []
    providers = 0
    dltypes = ''
    for provider in lazylibrarian.CONFIG.providers('RSS'):
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s %s" % (provider['DISPNAME'], provider['ENABLED'],
                                                provider['DLTYPES'], provider['LABEL']))
        if provider['ENABLED'] and not lazylibrarian.wishlist_type(provider['HOST']):
            if provider_is_blocked(provider['HOST']):
                logger.debug('%s is BLOCKED' % provider['HOST'])
            else:
                providers += 1
                logger.debug('[iterate_over_rss_sites] - %s' % provider['HOST'])
                resultslist += rss(provider['HOST'], provider['NAME'], provider.get_int('DLPRIORITY'),
                                   provider['DISPNAME'], provider['DLTYPES'], False, provider['LABEL'])
                dltypes += provider['DLTYPES']

    return resultslist, providers, ''.join(set(dltypes))


def iterate_over_wishlists():
    resultslist = []
    providers = 0
    for provider in lazylibrarian.CONFIG.providers('RSS'):
        if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
            logger.debug("DLTYPES: %s: %s %s %s" % (provider['DISPNAME'], provider['ENABLED'],
                                                provider['DLTYPES'], provider['LABEL']))
        if provider['ENABLED']:
            wishtype = lazylibrarian.wishlist_type(provider['HOST'])
            if wishtype == 'goodreads':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += goodreads(provider['HOST'], provider['NAME'],
                                             provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                             provider['DLTYPES'], False, provider['LABEL'])
            elif wishtype == 'listopia':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += listopia(provider['HOST'], provider['NAME'],
                                            provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                            provider['DLTYPES'], False, provider['LABEL'])
            elif wishtype == 'amazon':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += amazon(provider['HOST'], provider['NAME'],
                                          provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                          provider['DLTYPES'], False, provider['LABEL'])
            elif wishtype == 'ny_times':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += ny_times(provider['HOST'], provider['NAME'],
                                            provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                            provider['DLTYPES'], False, provider['LABEL'])
            elif wishtype == 'publishersweekly':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += publishersweekly(provider['HOST'], provider['NAME'],
                                                    provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                                    provider['DLTYPES'], False, provider['LABEL'])

            elif wishtype == 'apps.npr.org':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += appsnprorg(provider['HOST'], provider['NAME'],
                                              provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                              provider['DLTYPES'], False, provider['LABEL'])

            elif wishtype == 'penguinrandomhouse':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += penguinrandomhouse(provider['HOST'], provider['NAME'],
                                                      provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                                      provider['DLTYPES'], False, provider['LABEL'])
            elif wishtype == 'barnesandnoble':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += barnesandnoble(provider['HOST'], provider['NAME'],
                                                  provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                                  provider['DLTYPES'], False, provider['LABEL'])

            elif wishtype == 'bookdepository':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += bookdepository(provider['HOST'], provider['NAME'],
                                                  provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                                  provider['DLTYPES'], False, provider['LABEL'])
            elif wishtype == 'indigo':
                if provider_is_blocked(provider['HOST']):
                    logger.debug('%s is BLOCKED' % provider['HOST'])
                else:
                    providers += 1
                    logger.debug('[iterate_over_wishlists] - %s' % provider['HOST'])
                    resultslist += indigo(provider['HOST'], provider['NAME'],
                                          provider.get_int('DLPRIORITY'), provider['DISPNAME'],
                                          provider['DLTYPES'], False, provider['LABEL'])
    return resultslist, providers


def iterate_over_irc_sites(book=None, search_type=None):
    resultslist = []
    providers = 0
    try:
        for provider in lazylibrarian.CONFIG.providers('IRC'):
            if lazylibrarian_log.LOGLEVEL & logger.log_iterateproviders:
                logger.debug("DLTYPES: %s: %s %s" % (provider['DISPNAME'], provider['ENABLED'], provider['DLTYPES']))
            if provider['ENABLED']:
                ignored = False
                if provider_is_blocked(provider['SERVER']):
                    logger.debug('%s is BLOCKED' % provider['SERVER'])
                    ignored = True
                elif search_type in ['book', 'shortbook', 'titlebook'] and 'E' not in provider['DLTYPES']:
                    logger.debug("Ignoring %s for eBook" % provider['DISPNAME'])
                    ignored = True
                elif "audio" in search_type and 'A' not in provider['DLTYPES']:
                    logger.debug("Ignoring %s for AudioBook" % provider['DISPNAME'])
                    ignored = True
                elif "mag" in search_type and 'M' not in provider['DLTYPES']:
                    logger.debug("Ignoring %s for Magazine" % provider['DISPNAME'])
                    ignored = True
                elif "comic" in search_type and 'M' not in provider['DLTYPES']:
                    logger.debug("Ignoring %s for Comic" % provider['DISPNAME'])
                    ignored = True
                elif not search_type or 'general' in search_type:
                    logger.debug("Ignoring %s for General search" % provider['DISPNAME'])
                    ignored = True
                if not ignored:
                    providers += 1
                    logger.debug('[iterate_over_irc_sites] - %s' % provider['SERVER'])
                    success, results = ircsearch(book, provider, search_type)
                    if success:
                        resultslist += results
    except Exception as e:
        logger.error(str(e))
    finally:
        return resultslist, providers


def ircsearch(book, provider: ConfigDict, search_type, test=False):
    results = []
    if not provider['SERVER']:
        logger.error("No server for %s" % provider['NAME'])
        return False, results
    if not provider['CHANNEL']:
        logger.error("No channel for %s" % provider['NAME'])
        return False, results

    irc = irc_connect(provider)
    if not irc:
        logger.error("Failed to connect to %s" % provider['SERVER'])
        provider.set_connection(None)
        return False, results

    # if test:
    #     return True, results

    if search_type not in ['mag', 'comic']:
        # For irc search we use just the author name and cache the results
        # so we can search long and short from the same resultset
        # but allow a separate "title only" search
        authorname, bookname = get_searchterm(book, search_type)
        if 'title' in search_type:
            book['searchterm'] = bookname
        else:
            book['searchterm'] = authorname
        logger.debug("Searching %s:%s for %s" % (provider['DISPNAME'],
                                                 provider['CHANNEL'], book['searchterm']))
        fname, data = irc_search(provider, book['searchterm'], cache=True)
        if not fname and 'timed out' in make_unicode(data):  # need to reconnect
            provider.set_connection(None)
            logger.error(data)
            return False, results
        if fname:
            results = irc_results(provider, fname)
        elif test and 'No results' in make_unicode(data):
            return 0, provider['SERVER']

    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), provider['SERVER']))
    try:
        irc_leave(provider)
    except Exception as e:
        logger.error(str(e))
    if test:
        return len(results), provider['SERVER']
    return True, results


def ny_times(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    ny_times best-sellers query function, return all the results in a list
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host

    url = host
    provider = host.split('best-sellers')[1].strip('/')
    if provider:
        provider = provider.split('/')[0]
    else:
        provider = 'best-sellers'
    if not dispname:
        dispname = provider

    result, success = fetch_url(url)

    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)

    elif result:
        logger.debug('Parsing results from %s' % url)
        data = result.split('itemProp="itemListElement"')
        for entry in data[1:]:
            try:
                title = make_unicode(entry.split('itemProp="name">')[1].split('<')[0])
                author_name = make_unicode(entry.split('itemProp="author">by ')[1].split('<')[0])
                author_name = author_name.split(' and ')[0].strip()  # multi-author, use first one
                results.append({
                    'rss_prov': provider,
                    'rss_feed': feednr,
                    'rss_title': title,
                    'rss_author': author_name,
                    'rss_bookid': '',
                    'rss_isbn': '',
                    'priority': priority,
                    'dispname': dispname,
                    'types': types,
                    'label': label,
                })
            except IndexError:
                pass
    else:
        logger.debug('No data returned from %s' % url)

    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    if test:
        return len(results)
    return results


def amazon(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    Amazon charts html page
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host

    if '/charts/' in host:
        provider = host.split('/charts')[1]
    else:
        provider = host

    url = host
    result, success = fetch_url(url)
    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)
    elif result:
        logger.debug('Parsing results from %s' % url)
        soup = BeautifulSoup(result, 'html5lib')
        authors = soup.find_all("div", {"class": "kc-rank-card-author"})
        titles = soup.find_all("div", {"class": "kc-rank-card-title"})

        if len(authors) == len(titles):
            res = []
            authnames = []
            for item in authors:
                authnames.append(item.get('title'))
            booknames = []
            for item in titles:
                booknames.append(item.text.replace('\n', '').strip())
            temp_res = list(zip(authnames, booknames))
            # suppress blanks and duplicates
            for item in temp_res:
                if item[0] and item[1] and item not in res:
                    res.append(item)

            for item in res:
                results.append({
                    'rss_prov': provider,
                    'rss_feed': feednr,
                    'rss_title': item[1],
                    'rss_author': item[0],
                    'rss_bookid': '',
                    'rss_isbn': '',
                    'priority': priority,
                    'dispname': dispname,
                    'types': types,
                    'label': label,
                })

    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    if test:
        return len(results)
    return results


def publishersweekly(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    publishersweekly best-sellers voir dans configLazy folder pour les commentaires
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host

    url = host
    provider = host.split('/pw/nielsen/')[1].strip('.html')
    if provider:
        provider = provider.split('/')[0]
    else:
        provider = 'best-sellers'
    if not dispname:
        dispname = provider

    result, success = fetch_url(url)
    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)

    elif result:
        logger.debug('Parsing results from %s' % url)
        data = result.split('class="nielsen-bookinfo"')
        for entry in data[1:]:
            try:
                title = make_unicode(entry.split('<div')[1])
                title = re.sub('<.*?>', '', title)
                title = title.split('">')[1].strip()
                author_name = make_unicode(entry.split('<div>')[1].split(', Author')[0])
                rss_isbn = make_unicode(entry.split('<div')[3].split('<br>')[1])
                author_name = author_name.split(' and ')[0].strip()  # multi-author, use first one
                results.append({
                    'rss_prov': provider,
                    'rss_feed': feednr,
                    'rss_title': title,
                    'rss_author': author_name,
                    'rss_bookid': '',
                    'rss_isbn': rss_isbn,
                    'priority': priority,
                    'dispname': dispname,
                    'types': types,
                    'label': label,
                })
            except IndexError:
                pass
    else:
        logger.debug('No data returned from %s' % url)
    if test:
        return len(results)
    return results


def appsnprorg(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    best-book aoos,npr.org
    """
    results = []
    basehost = host
    booknames = []
    authnames = []
    if not str(host)[:4] == "http":
        host = 'http://' + host

    url = host
    provider = host.split('/best-books/')[0]
    if provider:
        provider = provider.split('/')[0]
    else:
        provider = 'apps.nprorg'
    if not dispname:
        dispname = provider

    try:
        year_url = url.split('year=')[1]
        url_to_json = (url.split('#')[0]) + year_url + "-detail.json"
        result, success = fetch_url(url_to_json)
    except IndexError as e:
        success = False
        result = str(e)

    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)
    #
    elif result:

        data = json.loads(result)
        res = []
        isbn = []
        for books in data:
            temp_dic = data[books]
            booknames.append(temp_dic["title"])
            authnames.append(temp_dic["author"])
            isbn.append(temp_dic["isbn"])
        temp_res = list(zip(authnames, booknames, isbn))
        for item in temp_res:
            if item[0] and item[1] and item not in res:
                res.append(item)
        for item in res:
            results.append({
                'rss_prov': provider,
                'rss_feed': feednr,
                'rss_title': item[1],
                'rss_author': item[0],
                'rss_bookid': '',
                'rss_isbn': item[2],
                'priority': priority,
                'dispname': dispname,
                'types': types,
                'label': label,
            })

    else:
        logger.debug('No data returned from %s' % url)

    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    if test:
        return len(results)
    return results


def penguinrandomhouse(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    penguinrandomhouse html page
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host

    if '/books/' in host:
        provider = host.split('/books')[0]
    else:
        provider = "penguinrandomhouse"

    url = host
    result, success = fetch_url(url)
    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)
    elif result:
        logger.debug('Parsing results from %s' % url)

        soup = BeautifulSoup(result, 'html5lib')
        data = soup.find_all(id="tmpl-indc")
        resultnumber = data[0].get('totalresults')
        if resultnumber:
            authnames = []
            booknames = []
            res = []
            book_cat = data[0].get('cat')
            # requesting ajax page
            url = "https://www.penguinrandomhouse.com/ajaxc/categories/books/?from=0&to=" + resultnumber + \
                  "&contentId=" + book_cat.lower() + \
                  "&elClass=book&dataType=html&catFilter=best-sellers&sortType=frontlistiest_onsale"
            # page of all the book
            result, success = fetch_url(url)
            soup = BeautifulSoup(result, 'html5lib')
            titles = soup.find_all('div', {'class': 'title'})
            authors = soup.find_all('div', {'class': 'contributor'})

            if len(titles) == len(authors):
                for item in authors:
                    tmp = item.text.strip()
                    if ' and ' in tmp:
                        if ',' in tmp:
                            authnames.append(tmp.split(',')[0].strip())
                        else:
                            authnames.append(tmp.split(' and ')[0].strip())
                    else:
                        authnames.append(tmp)
                for item in titles:
                    booknames.append(item.text.replace('\n', '').strip())
                temp_res = list(zip(authnames, booknames))
                for item in temp_res:
                    if item[0] and item[1] and item not in res:
                        res.append(item)

                for item in res:
                    results.append({
                        'rss_prov': provider,
                        'rss_feed': feednr,
                        'rss_title': item[1],
                        'rss_author': item[0],
                        'rss_bookid': '',
                        'rss_isbn': '',
                        'priority': priority,
                        'dispname': dispname,
                        'types': types,
                        'label': label,
                    })
    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    if test:
        return len(results)
    return results


def barnesandnoble(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    Barneandnoble charts html page
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host
    provider = "barnesandnoble"

    url = host
    result, success = fetch_url(url)
    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)
    elif result:
        logger.debug('Parsing results from %s' % url)
        soup = BeautifulSoup(result, 'html5lib')
        titles = soup.find_all("div", {"class": "product-shelf-title product-info-title pt-xs"})
        authors = soup.find_all("div", {"class": "product-shelf-author pt-0 mt-1"})

        if len(authors) == len(titles):
            res = []
            authnames = []
            for item in authors:
                tmp = item.text.strip()
                if "," in tmp:
                    authnames.append(tmp.split(',')[0].split('by')[1].strip())
                else:
                    authnames.append(tmp.split('by')[1].strip())
            booknames = []
            for item in titles:
                booknames.append(str(item.contents[1]).split('title="')[1].split('"')[0].strip())
            temp_res = list(zip(authnames, booknames))
            # suppress blanks and duplicates
            for item in temp_res:
                if item[0] and item[1] and item not in res:
                    res.append(item)

            for item in res:
                results.append({
                    'rss_prov': provider,
                    'rss_feed': feednr,
                    'rss_title': item[1],
                    'rss_author': item[0],
                    'rss_bookid': '',
                    'rss_isbn': '',
                    'priority': priority,
                    'dispname': dispname,
                    'types': types,
                    'label': label,
                })
    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    if test:
        return len(results)
    return results


def bookdepository(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    bookdepository
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host
    provider = "bookdepository"
    page = 1
    next_page = True
    url = host

    while next_page:
        time.sleep(1)
        if 'page=' in host:
            host = str(url.split('page=')[0])
        if '?' in host:
            url = "%s&page=%i" % (host, page)
        else:
            url = "%s?page=%i" % (host, page)

        result, success = fetch_url(url)

        next_page = False
        if not success:
            logger.error('Error fetching data from %s: %s' % (url, result))
            if not test:
                block_provider(basehost, result)
        elif result:
            logger.debug('Parsing results from %s' % url)
            soup = BeautifulSoup(result, 'html5lib')
            titles = soup.find_all("h3", {"class": "title"})

            authors = soup.find_all("p", {"class": "author"})
            # take the number of result to see when max page
            resultnumber = soup.find_all("div", {"class": "search-info"})
            actual_result = str(resultnumber[0].text).split()[3].strip()
            max_result = str(resultnumber[0].text).split()[5].strip()
            next_page = actual_result != max_result

            if len(authors) == len(titles):
                res = []
                authnames = []
                for item in authors:
                    authnames.append(item.text.strip())
                booknames = []
                for item in titles:
                    booknames.append(item.text.strip())
                temp_res = list(zip(authnames, booknames))
                for item in temp_res:
                    if item[0] and item[1] and item not in res:
                        res.append(item)

                for item in res:
                    results.append({
                        'rss_prov': provider,
                        'rss_feed': feednr,
                        'rss_title': item[1],
                        'rss_author': item[0],
                        'rss_bookid': '',
                        'rss_isbn': '',
                        'priority': priority,
                        'dispname': dispname,
                        'types': types,
                        'label': label,
                    })

        if test:
            logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
            return len(results)

        page += 1

    return results


def indigo(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    indigo book list
    """
    # May have to check again, the api seems to return XML in browser from time to time,
    # may have to do a checkup to see if the result is xml
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host
    provider = "indigo"
    # The first page is 0
    page = 0
    next_page = True
    url = host

    result, success = fetch_url(url)
    if not success:
        logger.error('Error fetching data from %s: %s' % (url, result))
        if not test:
            block_provider(basehost, result)

    elif result:
        logger.debug('Parsing results from %s' % url)
        api = 'https://www.chapters.indigo.ca/en-ca/api/v1/merchandising/GetCmsProductList/?sortDirection=0'
        api += '&sortKey=Default&rangeLength=0&rangeStart=0&pageSize=12'
        list_id = re.findall(r'(?<="productLists":\[{"ContentID":).*?,', result)[0].split(',')[0]
        list_id = "&id=" + str(list_id)
        while next_page:
            time.sleep(1)
            if '?' in api:
                urlapi = "%s&page=%i%s" % (api, page, list_id)
            else:
                urlapi = "%s?page=%i%s" % (api, page, list_id)

            # Response in List format
            apiresult, success = fetch_url(urlapi)
            soup = BeautifulSoup(apiresult, 'html5lib')
            # Test if page result is empty
            if soup.text.strip() == "[]" or soup.text.strip() == "<feff>":
                return len(results)
            else:
                next_page = True

            # replace weird character , delete new line
            apiresult = apiresult.replace('\n', '').strip().replace(':', ': ').replace("“", "'").replace("”", "'")

            # conver to list of dict
            apilist = list(eval(apiresult))

            # List containt word without quote
            titles = []
            authors = []
            isbn = []
            for item in apilist:
                tmp_dic = item
                titles.append(tmp_dic['FullTitle'])
                authors.append(tmp_dic['MajorContributorName'])
                isbn.append(tmp_dic['ExternalProductId'])

            if len(authors) == len(titles):
                res = []
                temp_res = list(zip(authors, titles, isbn))
                # suppress blanks and duplicates
                for item in temp_res:
                    if item[0] and item[1] and item not in res:
                        res.append(item)

                for item in res:
                    results.append({
                        'rss_prov': provider,
                        'rss_feed': feednr,
                        'rss_title': item[1],
                        'rss_author': item[0],
                        'rss_bookid': '',
                        'rss_isbn': item[2],
                        'priority': priority,
                        'dispname': dispname,
                        'types': types,
                        'label': label,
                    })
            page += 1
    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    return results


def listopia(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    Goodreads Listopia query function, return all the results in a list
    """
    results = []
    maxpage = priority
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host

    page = 1
    next_page = True
    if '/show/' in host:
        provider = host.split('/show/')[1]
    elif '/book/' in host:
        provider = host.split('/book/')[1]
    else:
        provider = host

    if not dispname:
        dispname = provider

    while next_page:
        if '?' in host:
            url = "%s&page=%i" % (host, page)
        else:
            url = "%s?page=%i" % (host, page)

        result, success = fetch_url(url)

        next_page = False

        if not success:
            logger.error('Error fetching data from %s: %s' % (url, result))
            if not test:
                block_provider(basehost, result)

        elif result:
            logger.debug('Parsing results from %s' % url)
            data = result.split('<td valign="top" class="number">')
            for entry in data[1:]:
                try:
                    # index = entry.split('<')[0]
                    title = make_unicode(entry.split('<a title="')[1].split('"')[0])
                    book_id = entry.split('data-resource-id="')[1].split('"')[0]
                    author_name = make_unicode(entry.split('<a class="authorName"')[1].split(
                        '"name">')[1].split('<')[0])
                    results.append({
                        'rss_prov': provider,
                        'rss_feed': feednr,
                        'rss_title': title,
                        'rss_author': author_name,
                        'rss_bookid': book_id,
                        'rss_isbn': '',
                        'priority': priority,
                        'dispname': dispname,
                        'types': types,
                        'label': label,
                    })
                    if '/show/' in host:  # listopia can be multiple pages
                        next_page = True
                except IndexError:
                    pass
        else:
            logger.debug('No data returned from %s' % url)

        if test:
            logger.debug("Test found %i %s from %s" % (len(results), plural(len(results), "result"), host))
            return len(results)

        page += 1
        if maxpage:
            if page > maxpage:
                logger.warn('Maximum results page reached, still more results available')
                next_page = False

    logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    return results


def goodreads(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    Goodreads rss query function, return all the results in a list, can handle multiple wishlists
    but expects goodreads format (looks for goodreads category names)
    """
    results = []
    basehost = host
    if not str(host)[:4] == "http":
        host = 'http://' + host

    url = host

    result, success = fetch_url(url)

    if success:
        data = feedparser.parse(result)
    else:
        logger.error('Error fetching data from %s: %s' % (host, result))
        if not test:
            block_provider(basehost, result)
        return []

    if data:
        logger.debug('Parsing results from %s' % url)
        provider = data['feed']['link']
        if not dispname:
            dispname = provider
        logger.debug("rss %s returned %i %s" % (provider, len(data.entries), plural(len(data.entries), "result")))
        for post in data.entries:
            title = ''
            book_id = ''
            author_name = ''
            isbn = ''
            if 'title' in post:
                title = post.title
            if 'book_id' in post:
                book_id = post.book_id
            if 'author_name' in post:
                author_name = post.author_name
            if 'isbn' in post:
                isbn = post.isbn
            if title and author_name:
                results.append({
                    'rss_prov': provider,
                    'rss_feed': feednr,
                    'rss_title': title,
                    'rss_author': author_name,
                    'rss_bookid': book_id,
                    'rss_isbn': isbn,
                    'priority': priority,
                    'dispname': dispname,
                    'types': types,
                    'label': label,
                })
        logger.debug("Found %i %s from %s" % (len(results), plural(len(results), "result"), host))
    else:
        logger.debug('No data returned from %s' % host)
    if test:
        return len(results)
    return results


def rss(host=None, feednr=None, priority=0, dispname=None, types='E', test=False, label=''):
    """
    Generic rss query function, just return all the results from the rss feed in a list
    """
    results = []
    success = False
    result = ''

    url = str(host)
    if not str(url)[:4] == "http" and not str(url)[:4] == "file":
        url = 'http://' + url

    if str(url)[:4] == "http":
        result, success = fetch_url(url)
    elif str(url)[:4] == "file":
        success = False
        file_path = urlparse(url).path
        # noinspection PyBroadException
        try:
            with open(syspath(file_path), "r") as rss_provider:
                success = True
                result = rss_provider.read()
        except Exception:
            logger.error("%s rss file provider doesn't exist" % url)

    if success:
        data = feedparser.parse(result)
    else:
        logger.error('Error fetching data from %s: %s' % (host, result))
        block_provider(host, result)
        data = None

    if data:
        # to debug because of api
        logger.debug('Parsing results from %s' % url)
        try:
            provider = data['feed']['link']
        except KeyError:
            provider = 'rss_%s' % feednr
        if not dispname:
            dispname = provider
        logger.debug("rss %s returned %i %s" % (provider, len(data.entries), plural(len(data.entries), "result")))
        for post in data.entries:
            title = None
            magnet = None
            size = 0
            torrent = None
            nzb = None
            url = None
            tortype = 'torrent'

            if 'title' in post:
                title = post.title
            if 'links' in post:
                for f in post.links:
                    if 'x-bittorrent' in f['type']:
                        size = f['length']
                        torrent = f['href']
                        break
                    elif 'x-nzb' in f['type']:
                        size = f['length']
                        nzb = f['href']
                        break
                    elif f['href'].startswith('magnet'):
                        magnet = f['href']
                        if 'length' in f:
                            size = f['length']
                        break

            if 'torrent_magneturi' in post:
                magnet = post.torrent_magneturi

            if torrent:
                url = torrent
                tortype = 'torrent'

            if magnet:
                if not url or (url and lazylibrarian.CONFIG.get_bool('PREFER_MAGNET')):
                    url = magnet
                    tortype = 'magnet'

            if nzb:  # prefer nzb over torrent/magnet
                url = nzb
                tortype = 'nzb'

            if not url:
                if 'link' in post:
                    url = post.link

            tor_date = 'Fri, 01 Jan 1970 00:00:00 +0100'
            if 'newznab_attr' in post:
                if post.newznab_attr['name'] == 'usenetdate':
                    tor_date = post.newznab_attr['value']

            if not size:
                size = 1000
            if title and url:
                results.append({
                    'tor_prov': provider,
                    'tor_title': title,
                    'tor_url': url,
                    'tor_size': str(size),
                    'tor_date': tor_date,
                    'tor_feed': feednr,
                    'tor_type': tortype,
                    'priority': priority,
                    'dispname': dispname,
                    'types': types,
                    'label': label,
                })
    else:
        logger.debug('No data returned from %s' % host)
    if test:
        return len(results)
    return results


def cancel_search_type(search_type: str, error_msg: str, provider: ConfigDict):
    """ See if errorMsg contains a known error response for an unsupported search function
    depending on which searchType. If it does, disable that searchtype for the relevant provider
    return True if cancelled
    """
    errorlist = ['no such function', 'unknown parameter', 'unknown function', 'bad_gateway',
                 'bad request', 'bad_request', 'incorrect parameter', 'does not support']

    errormsg = make_unicode(error_msg).lower()

    if (provider['BOOKSEARCH'] and search_type in ["book", "shortbook", 'titlebook']) or \
            (provider['AUDIOSEARCH'] and search_type in ["audio", "shortaudio"]):
        match = False
        for item in errorlist:
            if item in errormsg:
                match = True
                break

        if match:
            if search_type in ["book", "shortbook", 'titlebook']:
                msg = 'BOOKSEARCH'
            elif search_type in ["audio", "shortaudio"]:
                msg = 'AUDIOSEARCH'
            else:
                msg = ''

            if msg:
                count = 0
                # CFG2DO p3 Quite inelegant duplication here
                for provider in lazylibrarian.CONFIG.providers('NEWZNAB'):
                    while count < len(provider):
                        if provider['HOST'] == provider['HOST']:
                            if not provider['MANUAL']:
                                logger.error("Disabled %s=%s for %s" % (msg, provider[msg], provider['DISPNAME']))
                                provider[msg] = ""
                                lazylibrarian.CONFIG.save_config_and_backup_old(section=provider['NAME'])
                                return True
                        count += 1
                for provider in lazylibrarian.CONFIG.providers('TORZNAB'):
                    while count < len(provider):
                        if provider['HOST'] == provider['HOST']:
                            if not provider['MANUAL']:
                                logger.error("Disabled %s=%s for %s" % (msg, provider[msg], provider['DISPNAME']))
                                provider[msg] = ""
                                lazylibrarian.CONFIG.save_config_and_backup_old(section=provider['NAME'])
                                return True
                        count += 1
            logger.error('Unable to disable searchtype [%s] for %s' % (search_type, provider['DISPNAME']))
    return False


def newznab_plus(book:Dict, provider:ConfigDict, search_type:str, search_mode=None, test=False):
    """
    Generic NewzNabplus query function
    takes in host+key+type and returns the result set regardless of who
    based on site running NewzNab+
    ref http://usenetreviewz.com/nzb-sites/
    """

    host = provider['HOST']
    api_key = provider['API']
    logger.debug('SearchType [%s] with Host [%s] mode [%s] using api [%s] for item [%s]' % (
        search_type, host, search_mode, api_key, str(book)))

    results = []

    params = return_search_structure(provider, api_key, book, search_type, search_mode)

    if params:
        if not str(host[:4]) == "http":
            host = 'http://' + host
        if host[-1:] == '/':
            host = host[:-1]
        if host[-4:] == '/api':
            url = host + '?' + urlencode(params)
        else:
            url = host + '/api?' + urlencode(params)

        sterm = make_unicode(book['searchterm'])

        rootxml = None
        logger.debug("URL = %s" % url)
        result, success = fetch_url(url, raw=True)

        if test:
            try:
                result = result.decode('utf-8')
            except UnicodeDecodeError:
                result = result.decode('latin-1')
            except AttributeError:
                pass

            if result.startswith('<') and result.endswith('/>') and "error code" in result:
                result = result[1:-2]
                success = False
            if not success:
                logger.debug(result)
                return success, result

        if success:
            try:
                rootxml = ElementTree.fromstring(result)
            except Exception as e:
                logger.error('Error parsing data from %s: %s %s' % (host, type(e).__name__, str(e)))
                logger.debug(repr(result))
                rootxml = None
                success = False
        else:
            try:
                result = result.decode('utf-8')
            except UnicodeDecodeError:
                result = result.decode('latin-1')
            except AttributeError:
                pass

            if not result or result == "''":
                result = "Got an empty response"
            logger.error('Error reading data from %s: %s' % (host, result))

        if not success:
            # maybe the host doesn't support the search type
            cancelled = cancel_search_type(search_type, result, provider)
            if not cancelled:  # it was some other problem
                block_provider(provider['HOST'], result)

        if rootxml is not None:
            # to debug because of api
            logger.debug('Parsing results from <a href="%s">%s</a>' % (url, host))
            if rootxml.tag == 'error':
                # noinspection PyTypeChecker
                errormsg = rootxml.get('description', default='unknown error')
                errormsg = errormsg[:200]  # sometimes get huge error messages from jackett
                logger.error("%s - %s" % (host, errormsg))
                # maybe the host doesn't support the search type
                cancelled = cancel_search_type(search_type, errormsg, provider)
                if not cancelled:  # it was some other problem
                    block_provider(provider['HOST'], errormsg)

                if test and search_type == 'book' and cancelled:
                    return newznab_plus(book, provider, 'generalbook', search_mode, test)
            else:
                channel = rootxml.find('channel')
                if channel:
                    for item in channel:
                        if 'apilimits' in str(item):
                            limits = item
                            apimax = limits.get('apimax')
                            if apimax:
                                provider.set_int('APILIMIT', int(apimax))
                            apicurrent = limits.get('apicurrent')
                            if apicurrent:
                                provider['APICOUNT'] = apicurrent
                            logger.debug("%s used %s of %s" % (provider['DISPNAME'],
                                                               provider['APICOUNT'],
                                                               provider['APILIMIT']))
                            break
                resultxml = rootxml.iter('item')
                nzbcount = 0
                maxage = lazylibrarian.CONFIG.get_int('USENET_RETENTION')
                for nzb in resultxml:
                    try:
                        thisnzb = return_results_by_search_type(book, nzb, host, search_mode, provider.get_int('DLPRIORITY'))
                        thisnzb['dispname'] = provider['DISPNAME']
                        if search_type in ['book', 'shortbook', 'titlebook']:
                            thisnzb['booksearch'] = provider['BOOKSEARCH']

                        if 'seeders' in thisnzb:
                            if 'SEEDERS' not in provider:
                                # might have provider in newznab instead of torznab slot?
                                logger.warn("%s does not support seeders" % provider['DISPNAME'])
                            else:
                                # its torznab, check if minimum seeders relevant
                                if thisnzb['seeders'].get_int() >= provider.get_int('SEEDERS'):
                                    nzbcount += 1
                                    results.append(thisnzb)
                                else:
                                    logger.debug('Rejecting %s has %s %s' % (thisnzb['nzbtitle'],
                                                                             thisnzb['seeders'],
                                                                             plural(thisnzb['seeders'], "seeder")))
                        else:
                            # its newznab, check if too old
                            if not maxage:
                                nzbcount += 1
                                results.append(thisnzb)
                            else:
                                # example nzbdate format: Mon, 27 May 2013 02:12:09 +0200
                                nzbdate = thisnzb['nzbdate']
                                try:
                                    parts = nzbdate.split(' ')
                                    nzbage = age('%04d-%02d-%02d' % (int(parts[3]), month2num(parts[2]),
                                                                     int(parts[1])))
                                except Exception as e:
                                    logger.warn('Unable to get age from [%s] %s %s' %
                                                (thisnzb['nzbdate'], type(e).__name__, str(e)))
                                    nzbage = 0
                                if nzbage <= maxage:
                                    nzbcount += 1
                                    results.append(thisnzb)
                                else:
                                    logger.debug('%s is too old (%s %s)' % (thisnzb['nzbtitle'],
                                                                            nzbage, plural(nzbage, "day")))

                    except IndexError:
                        logger.debug('No results from %s for %s' % (host, sterm))
                logger.debug('Found %s results at %s for: %s' % (nzbcount, host, sterm))
        else:
            logger.debug('No data returned from %s for %s' % (host, sterm))
        if test:
            return len(results), host
    return True, results


def return_search_structure(provider: ConfigDict, api_key, book, search_type, search_mode):
    params = None
    if search_type in ["book", "shortbook", 'titlebook']:
        authorname, bookname = get_searchterm(book, search_type)
        bookname = no_umlauts(bookname)
        if provider['BOOKSEARCH'] and provider['BOOKCAT']:  # if specific booksearch, use it
            if provider['BOOKSEARCH'] == 'bibliotik':
                params = {
                    "t": provider['GENERALSEARCH'],
                    "apikey": api_key,
                    "q": make_utf8bytes("@title %s @authors %s" % (bookname, authorname))[0],
                    "cat": provider['BOOKCAT']
                }
            else:
                params = {
                    "t": provider['BOOKSEARCH'],
                    "apikey": api_key,
                    "title": make_utf8bytes(unaccented(bookname))[0],
                    "author": make_utf8bytes(authorname)[0],
                    "cat": provider['BOOKCAT']
                }
        elif provider['GENERALSEARCH'] and provider['BOOKCAT']:  # if not, try general search
            params = {
                "t": provider['GENERALSEARCH'],
                "apikey": api_key,
                "q": make_utf8bytes("%s %s" % (authorname, bookname))[0],
                "cat": provider['BOOKCAT']
            }
    elif search_type in ["audio", "shortaudio"]:
        authorname, bookname = get_searchterm(book, search_type)
        bookname = no_umlauts(bookname)
        if provider['AUDIOSEARCH'] and provider['AUDIOCAT']:  # if specific audiosearch, use it
            params = {
                "t": provider['AUDIOSEARCH'],
                "apikey": api_key,
                "title": make_utf8bytes(bookname)[0],
                "author": make_utf8bytes(authorname)[0],
                "cat": provider['AUDIOCAT']
            }
        elif provider['GENERALSEARCH'] and provider['AUDIOCAT']:  # if not, try general search
            params = {
                "t": provider['GENERALSEARCH'],
                "apikey": api_key,
                "q": make_utf8bytes("%s %s" % (authorname, bookname))[0],
                "cat": provider['AUDIOCAT']
            }
    elif search_type == "mag":
        if provider['MAGSEARCH'] and provider['MAGCAT']:  # if specific magsearch, use it
            params = {
                "t": provider['MAGSEARCH'],
                "apikey": api_key,
                "cat": provider['MAGCAT'],
                "q": make_utf8bytes(book['searchterm'].replace(':', ''))[0],
            }
        elif provider['GENERALSEARCH'] and provider['MAGCAT']:
            params = {
                "t": provider['GENERALSEARCH'],
                "apikey": api_key,
                "cat": provider['MAGCAT'],
                "q": make_utf8bytes(book['searchterm'].replace(':', ''))[0],
            }
    else:
        if provider['GENERALSEARCH']:
            if "shortgeneral" in search_type:
                searchterm = unaccented(book['searchterm'].split('(')[0], only_ascii=False, umlauts=False)
                searchterm = searchterm.replace('/', '_').replace('#', '_').replace(':', '')
            elif 'title' in search_type:
                _, searchterm = get_searchterm(book, search_type)
                searchterm = unaccented(searchterm.replace(':', ''), only_ascii=False, umlauts=False)
            else:
                searchterm = unaccented(book['searchterm'], only_ascii=False, umlauts=False)
                searchterm = searchterm.replace('/', '_').replace('#', '_').replace(':', '')
            params = {
                "t": provider['GENERALSEARCH'],
                "apikey": api_key,
                "q": make_utf8bytes(searchterm)[0],
            }
    if params:
        if provider['EXTENDED']:
            extends = provider['EXTENDED'].split('&')
            if extends[0] in ['1', '0']:
                params["extended"] = extends[0]
            if '=' not in extends[0]:
                extends.pop(0)
            for item in extends:
                try:
                    key, value = item.split('=')
                    params[key] = value
                except ValueError:
                    pass

        logger.debug('%s Search parameters set to %s' % (search_mode, str(params)))
    else:
        logger.debug('%s No matching search parameters for %s' % (search_mode, search_type))

    return params


def return_results_by_search_type(book: Dict, nzbdetails, host=None, search_mode=None, priority=0) -> Dict:
    """
    # searchType has multiple query params for t=, which return different results sets.
    # books have a dedicated check, so will use that.
    # mags don't so will have more generic search term.
    # http://newznab.readthedocs.org/en/latest/misc/api/#predefined-categories
    # results when searching for t=book
    #    <item>
    #       <title>David Gemmell - Troy 03 - Fall of Kings</title>
    #       <guid isPermaLink="true">
    #           https://www.usenet-crawler.com/details/091c8c0e18ca34201899b91add52e8c0
    #       </guid>
    #       <link>
    #           https://www.usenet-crawler.com/getnzb/091c8c0e18ca34201899b91add52e8c0.nzb&i=155518&r=78c0509
    #       </link>
    #       <comments>
    # https://www.usenet-crawler.com/details/091c8c0e18ca34201899b91add52e8c0#comments
    #       </comments>
    #       <pubDate>Fri, 11 Jan 2013 16:49:34 +0100</pubDate>
    #       <category>Books > Ebook</category>
    #       <description>David Gemmell - Troy 03 - Fall of Kings</description>
    #       <enclosure url="https://www.usenet-crawler.com/getnzb/091c8c0e18ca34201899b91add52e8c0.nzb&i=155518&r=78c0>
    #       <newznab:attr name="category" value="7000"/>
    #       <newznab:attr name="category" value="7020"/>
    #       <newznab:attr name="size" value="4909563"/>
    #       <newznab:attr name="guid" value="091c8c0e18ca34201899b91add52e8c0"/>
    #       </item>
    #
    # t=search results
    # <item>
    #   <title>David Gemmell - [Troy 03] - Fall of Kings</title>
    #   <guid isPermaLink="true">
    #       https://www.usenet-crawler.com/details/5d7394b2386683d079d8bd8f16652b18
    #   </guid>
    #   <link>
    #       https://www.usenet-crawler.com/getnzb/5d7394b2386683d079d8bd8f16652b18.nzb&i=155518&r=78c0509bc6bb9174
    #   </link>
    #   <comments>
    # https://www.usenet-crawler.com/details/5d7394b2386683d079d8bd8f16652b18#comments
    #   </comments>
    #   <pubDate>Mon, 27 May 2013 02:12:09 +0200</pubDate>
    #   <category>Books > Ebook</category>
    #   <description>David Gemmell - [Troy 03] - Fall of Kings</description>
    #   <enclosure url="https://www.usenet-crawler.com/getnzb/5d7394b2386683d079d8bd8f16652b18.nzb&i=155518&r=78c05>
    #   <newznab:attr name="category" value="7000"/>
    #   <newznab:attr name="category" value="7020"/>
    #   <newznab:attr name="size" value="4909563"/>
    #   <newznab:attr name="guid" value="5d7394b2386683d079d8bd8f16652b18"/>
    #   <newznab:attr name="files" value="2"/>
    #   <newznab:attr name="poster" value="nerdsproject@gmail.com (N.E.R.Ds)"/>
    #   <newznab:attr name="grabs" value="0"/>
    #   <newznab:attr name="comments" value="0"/>
    #   <newznab:attr name="password" value="0"/>
    #   <newznab:attr name="usenetdate" value="Fri, 11 Mar 2011 13:45:15 +0100"/>
    #   <newznab:attr name="group" value="alt.binaries.e-book.flood"/>
    # </item>
    # -------------------------------TORZNAB RETURN DATA-- book ---------------------------------------------
    # <item>
    #  <title>Tom Holt - Blonde Bombshell (Dystop; SFX; Humour) ePUB+MOBI</title>
    #  <guid>https://getstrike.net/torrents/1FDBE6466738EED3C7FD915E1376BA0A63088D4D</guid>
    #  <comments>https://getstrike.net/torrents/1FDBE6466738EED3C7FD915E1376BA0A63088D4D</comments>
    #  <pubDate>Sun, 27 Sep 2015 23:10:56 +0200</pubDate>
    #  <size>24628</size>
    #  <description>Tom Holt - Blonde Bombshell (Dystop; SFX; Humour) ePUB+MOBI</description>
    #  <link>http://192.168.2.2:9117/dl/strike/pkl4u83iz41up73m4zsigqsd4zyie50r/aHR0cHM6Ly9nZXRzdHJpa2UubmV0L3RvcnJl
    #  bnRzL2FwaS9kb3dubG9hZC8xRkRCRTY0NjY3MzhFRUQzQzdGRDkxNUUxMzc2QkEwQTYzMDg4RDRELnRvcnJlbnQ1/t.torrent</link>
    #  <category>8000</category>
    #  <enclosure url="http://192.168.2.2:9117/dl/strike/pkl4u83iz41up73m4zsigqsd4zyie50r/aHR0cHM6Ly9nZXRzdHJpa2UubmV
    #  0L3RvcnJlbnRzL2FwaS9kb3dubG9hZC8xRkRCRTY0NjY3MzhFRUQzQzdGRDkxNUUxMzc2QkEwQTYzMDg4RDRELnRvcnJlbnQ1/t.torrent"
    #  length="24628" type="application/x-bittorrent" />
    #  <torznab:attr name="magneturl" value="magnet:?xt=urn:btih:1FDBE6466738EED3C7FD915E1376BA0A63088D4D&amp;
    #  dn=Tom+Holt+-+Blonde+Bombshell+(Dystop%3B+SFX%3B+Humour)+ePUB%2BMOBI&amp;tr=udp://open.demonii.com:1337&amp;
    #  tr=udp://tracker.coppersurfer.tk:6969&amp;tr=udp://tracker.leechers-paradise.org:6969&amp;
    #  tr=udp://exodus.desync.com:6969" />
    #  <torznab:attr name="seeders" value="1" />
    #  <torznab:attr name="peers" value="2" />
    #  <torznab:attr name="infohash" value="1FDBE6466738EED3C7FD915E1376BA0A63088D4D" />
    #  <torznab:attr name="minimumratio" value="1" />
    #  <torznab:attr name="minimumseedtime" value="172800" />
    # </item>
    # ---------------------------------------- magazine ----------------------------------------
    # <item>
    #  <title>Linux Format Issue 116 - KDE Issue</title>
    #  <guid>https://getstrike.net/torrents/f3fc8df4fdd850132072a435a7d112d6c9d77d16</guid>
    #  <comments>https://getstrike.net/torrents/f3fc8df4fdd850132072a435a7d112d6c9d77d16</comments>
    #  <pubDate>Wed, 04 Mar 2009 01:57:20 +0100</pubDate>
    #  <size>1309195</size>
    #  <description>Linux Format Issue 116 - KDE Issue</description>
    #  <link>http://192.168.2.2:9117/dl/strike/pkl4u83iz41up73m4zsigqsd4zyie50r/aHR0cHM6Ly9nZXRzdHJpa2UubmV0L3R
    #  vcnJlbnRzL2FwaS9kb3dubG9hZC9mM2ZjOGRmNGZkZDg1MDEzMjA3MmE0MzVhN2QxMTJkNmM5ZDc3ZDE2LnRvcnJlbnQ1/t.torrent</link>
    #  <enclosure url="http://192.168.2.2:9117/dl/strike/pkl4u83iz41up73m4zsigqsd4zyie50r/aHR0cHM6Ly9nZXRzdHJpa2Uubm
    #  V0L3RvcnJlbnRzL2FwaS9kb3dubG9hZC9mM2ZjOGRmNGZkZDg1MDEzMjA3MmE0MzVhN2QxMTJkNmM5ZDc3ZDE2LnRvcnJlbnQ1/t.torrent"
    #  length="1309195" type="application/x-bittorrent" />
    #  <torznab:attr name="magneturl" value="magnet:?xt=urn:btih:f3fc8df4fdd850132072a435a7d112d6c9d77d16&amp;
    #  dn=Linux+Format+Issue+116+-+KDE+Issue&amp;tr=udp://open.demonii.com:1337&amp;tr=udp://tracker.coppersurfer.
    #  tk:6969&amp;tr=udp://tracker.leechers-paradise.org:6969&amp;tr=udp://exodus.desync.com:6969" />
    #  <torznab:attr name="seeders" value="2" />
    #  <torznab:attr name="peers" value="3" />
    #  <torznab:attr name="infohash" value="f3fc8df4fdd850132072a435a7d112d6c9d77d16" />
    #  <torznab:attr name="minimumratio" value="1" />
    #  <torznab:attr name="minimumseedtime" value="172800" />
    #  </item>
    """

    nzbtitle = ''
    nzbdate = ''
    nzburl = ''
    nzbsize = 0
    seeders = None

    n = 0
    while n < len(nzbdetails):
        tag = str(nzbdetails[n].tag).lower()

        if tag == 'title':
            nzbtitle = nzbdetails[n].text
        elif tag == 'size':
            nzbsize = nzbdetails[n].text
        elif tag == 'pubdate':
            nzbdate = nzbdetails[n].text
        elif tag == 'link':
            if not nzburl or (nzburl and not lazylibrarian.CONFIG.get_bool('PREFER_MAGNET')):
                nzburl = nzbdetails[n].text
        elif nzbdetails[n].attrib.get('name') == 'magneturl':
            nzburl = nzbdetails[n].attrib.get('value')
        elif nzbdetails[n].attrib.get('name') == 'size':
            nzbsize = nzbdetails[n].attrib.get('value')
        elif nzbdetails[n].attrib.get('name') == 'seeders':
            seeders = nzbdetails[n].attrib.get('value')
        n += 1

    result_fields = {
        'bookid': book['bookid'],
        'nzbprov': host,
        'nzbtitle': nzbtitle,
        'nzburl': nzburl,
        'nzbdate': nzbdate,
        'nzbsize': nzbsize,
        'nzbmode': search_mode,
        'priority': priority
    }
    if seeders is not None:  # only if torznab
        result_fields['seeders'] = check_int(seeders, 0)

    logger.debug('Result fields from NZB are ' + str(result_fields))
    return result_fields
