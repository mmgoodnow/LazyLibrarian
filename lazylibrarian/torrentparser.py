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
import re
import traceback
from urllib.parse import quote, urlencode

import lib.feedparser as feedparser
from bs4 import BeautifulSoup
from lazylibrarian.cache import fetch_url
from lazylibrarian.config2 import CONFIG
from lazylibrarian.formatter import plural, unaccented, make_unicode, size_in_bytes, url_fix, \
    make_utf8bytes
from lazylibrarian.telemetry import TELEMETRY


def torrent_tpb(book=None, test=False):
    logger = logging.getLogger(__name__)
    errmsg = ''
    provider = "torrent_tpb"
    host = CONFIG['TPB_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host + "/search/")

    cat = 0  # 601=ebooks, 602=comics, 102=audiobooks, 0=all, no mag category so use 600=other
    if 'library' in book:
        if book['library'] == 'AudioBook':
            cat = 102
        elif book['library'] == 'eBook':
            cat = 601
        elif book['library'] == 'comic':
            cat = 602
        elif book['library'] == 'magazine':
            cat = 600

    sterm = make_unicode(book['searchterm'])

    page = 0
    results = []
    minimumseeders = CONFIG.get_int('TPB_SEEDERS') - 1
    next_page = True

    while next_page:

        search_url = providerurl + "%s/%s/99/%s" % (quote(make_utf8bytes(book['searchterm'])[0]), page, cat)
        next_page = False
        result, success = fetch_url(search_url)

        if not success:
            # may return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s" % (provider, sterm))
                if test:
                    return 0  # no results but no error
            else:
                logger.debug(search_url)
                logger.debug('Error fetching data from %s: %s' % (provider, result))
                errmsg = result
                TELEMETRY.record_usage_data("tpbError")
            result = False

        if result:
            logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
            soup = BeautifulSoup(result, 'html5lib')
            # tpb uses a named table
            table = soup.find('table', id='searchResult')
            if table:
                rows = table.find_all('tr')
            else:
                logger.debug("No table found in results")
                rows = []

            if len(rows) > 1:
                rows = rows[1:]  # first row is headers
            for row in rows:
                td = row.find_all('td')
                if len(td) > 2:
                    try:
                        prov_page = ''
                        new_soup = BeautifulSoup(str(td[1]), 'html5lib')
                        link = new_soup.find("a")
                        magnet = link.get("href")
                        title = link.text
                        if 'detLink' in str(td[1]):
                            prov_page = str(td[1]).split('detLink')[1].split('href="', 1)[1].split('"')[0]
                        try:
                            seeders = int(td[2].text.replace(',', ''))
                        except ValueError:
                            seeders = 0

                        # no point in asking for magnet link if not enough seeders
                        if minimumseeders < seeders:
                            # some tpb proxies return absolute path, some return relative
                            if magnet.startswith('http'):
                                magurl = magnet
                            else:
                                magurl = '%s/%s' % (host, magnet)
                            result, success = fetch_url(magurl)
                            if not success:
                                logger.debug('Error fetching url %s, %s' % (magurl, result))
                            else:
                                magnet = None
                                new_soup = BeautifulSoup(result, 'html5lib')
                                for link in new_soup.find_all('a'):
                                    output = link.get('href')
                                    if output and output.startswith('magnet'):
                                        magnet = output
                                        break
                            if not magnet or not title:
                                logger.debug('Missing magnet or title')
                            else:
                                size = td[1].text.split(', Size ')[1].split('iB')[0]
                                size = size.replace('&nbsp;', '')
                                size = size_in_bytes(size)
                                res = {
                                    'bookid': book.get('bookid', 'test'),
                                    'tor_prov': provider,
                                    'tor_title': title,
                                    'tor_url': magnet,
                                    'tor_size': str(size),
                                    'tor_type': 'magnet',
                                    'priority': CONFIG['TPB_DLPRIORITY'],
                                    'prov_page': prov_page
                                }
                                # dates are either mm dd yyyy or mm dd hh:mm if yyyy is this year
                                try:
                                    tor_date = td[1].text.split('Uploaded ')[1].split(',')[0]
                                    m = tor_date[:2]
                                    d = tor_date[3:5]
                                    y = tor_date[-4:]
                                    if ':' in y:
                                        t = tor_date[-6:]
                                        res['tor_date'] = "%s-%s%s" % (m, d, t)
                                    else:
                                        res['tor_date'] = "%s-%s-%s" % (y, m, d)
                                except IndexError:
                                    pass

                                results.append(res)
                                logger.debug('Found %s. Size: %s: %s' % (title, size, magnet))
                                next_page = True
                        else:
                            logger.debug('Found %s but %s %s' % (title, seeders, plural(seeders, "seeder")))
                    except Exception as e:
                        logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                        logger.debug('%s: %s' % (provider, traceback.format_exc()))
                        TELEMETRY.record_usage_data("tpbParserError")

        if test:
            logger.debug("Test found %i %s from %s for %s" % (len(results), plural(len(results),
                                                              "result"), provider, sterm))
            return len(results)

        if 0 < CONFIG.get_int('MAX_PAGES') < page:
            logger.warning('Maximum results page search reached, still more results available')
            next_page = False
        else:
            page += 1

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    return results, errmsg


def torrent_kat(book=None, test=False):
    logger = logging.getLogger(__name__)
    errmsg = ''
    provider = "torrent_kat"
    host = CONFIG['KAT_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host + "/usearch/" + quote(make_utf8bytes(book['searchterm'])[0]))

    params = {
        "category": "books",
        "field": "seeders",
        "sorder": "desc"
    }
    search_url = providerurl + "/?%s" % urlencode(params)

    sterm = make_unicode(book['searchterm'])

    result, success = fetch_url(search_url)
    if not success:
        # seems torrent_kat returns 404 if no results, not really an error
        if '404' in result:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            if test:
                return False
        else:
            logger.debug(search_url)
            logger.debug('Error fetching data from %s: %s' % (provider, result))
            errmsg = result
            TELEMETRY.record_usage_data("katError")
        result = False

    results = []

    if result:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
        minimumseeders = CONFIG.get_int('KAT_SEEDERS') - 1
        soup = BeautifulSoup(result, 'html5lib')
        rows = []
        try:
            table = soup.find_all('table')[1]  # un-named table
            if table:
                rows = table.find_all('tr')
        except IndexError:  # no results table in result page
            logger.debug("No table found in results")
            rows = []

        if len(rows) > 1:
            rows = rows[1:]  # first row is headers

        for row in rows:
            td = row.find_all('td')
            if len(td) > 4:
                try:
                    # some mirrors of kat return multiple text items, some just the title
                    try:
                        title = str(td[0]).split('class="cellMainLink"')[1].split('>', 1)[1].split('</a>')[0]
                    except IndexError:
                        title = td[0].text
                    title = re.sub('<[^<]+?>', '', title).strip()  # remove embedded html tags
                    title = unaccented(title, only_ascii=False, umlauts=False)
                    try:
                        prov_page = host + \
                                    str(td[0]).split('class="torrentname"')[1].split('href="', 1)[1].split('"')[0]
                    except IndexError:
                        prov_page = ''
                    # kat can return magnet or torrent or both.
                    magnet = ''
                    url = ''
                    mode = 'torrent'
                    try:
                        magnet = 'magnet' + str(td[0]).split('href="magnet')[1].split('"')[0]
                        mode = 'magnet'
                    except IndexError:
                        pass
                    try:
                        url = 'http' + str(td[0]).split('href="http')[1].split('.torrent?')[0] + '.torrent'
                        mode = 'torrent'
                    except IndexError:
                        pass

                    if not url or (magnet and url and CONFIG.get_bool('PREFER_MAGNET')):
                        url = magnet
                        mode = 'magnet'

                    if prov_page and not url:
                        prov_result, success = fetch_url(prov_page)
                        if success:
                            try:
                                url = 'magnet' + prov_result.split('href="magnet', 1)[1].split('"')[0]
                                mode = 'magnet'
                            except IndexError:
                                pass
                    try:
                        size = str(td[1].text).replace('&nbsp;', '').upper()
                        size = size_in_bytes(size)
                    except ValueError:
                        size = 0

                    try:
                        seeders = int(td[4].text.replace(',', ''))
                    except ValueError:
                        seeders = 0

                    if not url or not title:
                        logger.debug('Missing url or title')
                    elif minimumseeders < seeders:
                        results.append({
                            'bookid': book.get('bookid', 'test'),
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': mode,
                            'priority': CONFIG['KAT_DLPRIORITY'],
                            'prov_page': prov_page
                        })
                        logger.debug('Found %s. Size: %s' % (title, size))
                    else:
                        logger.debug('Found %s but %s %s' % (title, seeders, plural(seeders, "seeder")))
                except Exception as e:
                    logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                    logger.debug('%s: %s' % (provider, traceback.format_exc()))
                    TELEMETRY.record_usage_data("katParserError")

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    if test:
        return len(results)
    return results, errmsg


def torrent_lime(book=None, test=False):
    logger = logging.getLogger(__name__)
    errmsg = ''
    provider = "Limetorrent"
    host = CONFIG['LIME_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    params = {
        "q": make_utf8bytes(book['searchterm'])[0]
    }
    providerurl = url_fix(host + "/searchrss/other")
    search_url = providerurl + "?%s" % urlencode(params)

    sterm = make_unicode(book['searchterm'])

    data, success = fetch_url(search_url)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in data:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            if test:
                return False
        else:
            logger.debug(search_url)
            logger.debug('Error fetching data from %s: %s' % (provider, data))
            errmsg = data
            TELEMETRY.record_usage_data("limeError")
        data = False

    results = []

    minimumseeders = CONFIG.get_int('LIME_SEEDERS') - 1
    if data:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
        d = feedparser.parse(data)
        if len(d.entries):
            for item in d.entries:
                try:
                    title = unaccented(item['title'], only_ascii=False, umlauts=False)
                    try:
                        seeders = item['description']
                        seeders = int(seeders.split('Seeds:')[1].split(' ,')[0].replace(',', '').strip())
                    except (IndexError, ValueError):
                        seeders = 0

                    size = item['size']
                    try:
                        size = int(size)
                    except ValueError:
                        size = 0

                    try:
                        pubdate = item['published']
                    except KeyError:
                        pubdate = None

                    url = None
                    for link in item['links']:
                        if 'x-bittorrent' in link['type']:
                            url = link['url']

                    if not url or not title:
                        logger.debug('No url or title found')
                    elif minimumseeders < seeders:
                        res = {
                            'bookid': book.get('bookid', 'test'),
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': 'torrent',
                            'priority': CONFIG['LIME_DLPRIORITY']
                        }
                        if pubdate:
                            res['tor_date'] = pubdate
                        results.append(res)
                        logger.debug('Found %s. Size: %s' % (title, size))
                    else:
                        logger.debug('Found %s but %s %s' % (title, seeders, plural(seeders, "seeder")))

                except Exception as e:
                    if 'forbidden' in str(e).lower():
                        # may have ip based access limits
                        logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                    else:
                        logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                        logger.debug('%s: %s' % (provider, traceback.format_exc()))
                        TELEMETRY.record_usage_data("limeParserError")

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    if test:
        return len(results)
    return results, errmsg


def torrent_tdl(book=None, test=False):
    logger = logging.getLogger(__name__)
    errmsg = ''
    provider = "torrentdownloads"
    host = CONFIG['TDL_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host)

    params = {
        "type": "search",
        "cid": "2",
        "search": make_utf8bytes(book['searchterm'])[0]
    }
    search_url = providerurl + "/rss.xml?%s" % urlencode(params)

    sterm = make_unicode(book['searchterm'])

    data, success = fetch_url(search_url)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in data:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            if test:
                return False
        else:
            logger.debug(search_url)
            logger.debug('Error fetching data from %s: %s' % (provider, data))
            errmsg = data
            TELEMETRY.record_usage_data("tdlError")
        data = False

    results = []

    minimumseeders = CONFIG.get_int('TDL_SEEDERS') - 1
    if data:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
        d = feedparser.parse(data)
        if len(d.entries):
            for item in d.entries:
                try:
                    title = item['title']
                    seeders = int(item['seeders'].replace(',', ''))
                    link = item['link']
                    size = int(item['size'])
                    url = None

                    try:
                        pubdate = item['published']
                    except KeyError:
                        pubdate = None

                    if link and minimumseeders < seeders:
                        # no point requesting the magnet link if not enough seeders
                        # torrent_tdl gives us a relative link
                        result, success = fetch_url(providerurl + link)
                        if success:
                            new_soup = BeautifulSoup(result, 'html5lib')
                            for link in new_soup.find_all('a'):
                                output = link.get('href')
                                if output and output.startswith('magnet'):
                                    url = output
                                    break

                        if not url or not title:
                            logger.debug('Missing url or title')
                        else:
                            res = {
                                'bookid': book.get('bookid', 'test'),
                                'tor_prov': provider,
                                'tor_title': title,
                                'tor_url': url,
                                'tor_size': str(size),
                                'tor_type': 'magnet',
                                'priority': CONFIG['TDL_DLPRIORITY']
                            }
                            if pubdate:
                                res['tor_date'] = pubdate
                            logger.debug('Found %s. Size: %s' % (title, size))
                            results.append(res)
                    else:
                        logger.debug('Found %s but %s %s' % (title, seeders, plural(seeders, "seeder")))

                except Exception as e:
                    logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                    logger.debug('%s: %s' % (provider, traceback.format_exc()))
                    TELEMETRY.record_usage_data("tdlParserError")

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    if test:
        return len(results)
    return results, errmsg
