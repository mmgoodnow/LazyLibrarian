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


import traceback

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.cache import fetchURL
from lazylibrarian.formatter import plural, unaccented, makeUnicode, size_in_bytes, url_fix, \
    replace_all, getList, month2num, check_year
from lib.six import PY2
# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_parse import quote, urlencode, quote_plus

try:
    from fuzzywuzzy import fuzz
except ImportError:
    from lib.fuzzywuzzy import fuzz

try:
    import html5lib
    from bs4 import BeautifulSoup
except ImportError:
    if PY2:
        from lib.bs4 import BeautifulSoup
    else:
        from lib3.bs4 import BeautifulSoup

if PY2:
    import lib.feedparser as feedparser
else:
    import lib3.feedparser as feedparser


def TRF(book=None, test=False):
    errmsg = ''
    provider = "Torrof"
    host = lazylibrarian.CONFIG['TRF_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    # '0': '', '1': '', '2': '', '3': '', '4': '', '5': '', '6': '', '7': '', '8': '', '9': '',
    dictrepl = {'...': '', '.': ' ', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', ' + ': ' ', '"': '',
                ',': ' ', '*': '', '(': '', ')': '', '[': '', ']': '', '#': '', '\'': '',
                ':': '', '!': '', '-': ' ', r'\s\s': ' '}

    cat = 'Book'
    if 'library' in book:
        if book['library'] == 'AudioBook':
            cat = 'Audio'
        elif book['library'] == 'eBook':
            cat = 'Book'
        elif book['library'] == 'magazine':
            cat = 'Magazine'
        elif book['library'] == 'comic':
            cat = 'Comic'

    sterm = makeUnicode("%s %s" % (book['searchterm'], cat))

    results = []
    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1

    searchURL = "%s/%s" % (host, quote_plus("%s %s" % (book['searchterm'], cat)))

    result, success = fetchURL(searchURL)

    if not success:
        # may return 404 if no results, not really an error
        if '404' in result:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            success = True
        else:
            logger.debug(searchURL)
            logger.debug('Error fetching data from %s: %s' % (provider, result))
            errmsg = result
        result = False

    if test:
        return success

    if result:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
        soup = BeautifulSoup(result, 'html5lib')

        try:
            table = soup.find_all('table')[1]  # un-named table
        except IndexError:
            table = None

        if table:
            rows = table.find_all('tr')
        else:
            rows = []

        if len(rows) > 3:
            rows = rows[1:]  # first row is headers
        for row in rows:
            td = row.find_all('td')
            if len(td) > 2:
                rejected = False
                dl_type = td[0].text
                if not dl_type:
                    rejected = 'no type'
                elif 'Direct'in dl_type:
                    rejected = 'direct'
                elif cat not in dl_type:
                    rejected = 'wrong cat'
                elif cat != 'Audio' and 'Audio' in dl_type:
                    rejected = 'audio'

                if not rejected:
                    resultTitle = unaccented(replace_all(td[1].text, dictrepl)).strip()
                    match = fuzz.token_set_ratio(book['searchterm'], resultTitle)

                    if match > 90:
                        try:
                            magnet = None
                            title = ''
                            seeders = 0
                            size = 0
                            age = ''
                            new_url = "%s%s" % (host, str(td[1]).split('href="')[1].split('"')[0])
                            result, success = fetchURL(new_url)
                            if not success:
                                logger.debug('Error fetching url %s, %s' % (new_url, result))
                            else:
                                new_soup = BeautifulSoup(result, 'html5lib')
                                for link in new_soup.find_all('a'):
                                    output = link.get('href')
                                    if output and output.startswith('magnet'):
                                        magnet = output
                                        break
                                if magnet:
                                    for link in new_soup.find_all('li'):
                                        data = link.get('title')
                                        if data:
                                            if 'Seeder' in data:
                                                seeders = int(link.text.strip().split(' ')[0])
                                            elif 'Size' in data:
                                                size = size_in_bytes(link.text.strip())
                                            elif ' age' in data:
                                                age = link.text.strip()
                                    title = new_soup.find("h1")
                                    if title:
                                        title = title.text
                                        title = title.replace('\n', '').strip()
                                        title = ' '.join(title.split())

                            # no point in asking for magnet link if not enough seeders
                            if minimumseeders < seeders:
                                if not magnet or not title:
                                    logger.debug('Missing magnet or title')
                                else:
                                    size = td[1].text.split(', Size ')[1].split('iB')[0]
                                    size = size.replace('&nbsp;', '')
                                    size = size_in_bytes(size)
                                    res = {
                                        'bookid': book['bookid'],
                                        'tor_prov': provider,
                                        'tor_title': title,
                                        'tor_url': magnet,
                                        'tor_size': str(size),
                                        'tor_type': 'magnet',
                                        'priority': lazylibrarian.CONFIG['TRF_DLPRIORITY']
                                    }
                                    # date style: Dec 2015
                                    if age:
                                        m = 0
                                        y = 0
                                        words = getList(age)
                                        for word in words:
                                            val = month2num(word)
                                            if val:
                                                m = val
                                            else:
                                                val = check_year(word)
                                                if val:
                                                    y = val
                                        if y and m:
                                            res['tor_date'] = "%d-%02d-01" % (y, m)

                                    results.append(res)

                                    logger.debug('Found %s. Size: %s: %s' % (title, size, magnet))

                            else:
                                logger.debug('Found %s, %s:%s, but %s seeder%s' % (title, size, age, seeders,
                                                                                   plural(seeders)))
                        except Exception as e:
                            logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                            logger.debug('%s: %s' % (provider, traceback.format_exc()))

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))
    return results, errmsg


def TPB(book=None, test=False):
    errmsg = ''
    provider = "TPB"
    host = lazylibrarian.CONFIG['TPB_HOST']
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

    sterm = makeUnicode(book['searchterm'])

    page = 0
    results = []
    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
    next_page = True

    while next_page:

        searchURL = providerurl + "%s/%s/99/%s" % (quote(book['searchterm']), page, cat)

        next_page = False
        result, success = fetchURL(searchURL)

        if not success:
            # may return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s" % (provider, sterm))
                success = True
            else:
                logger.debug(searchURL)
                logger.debug('Error fetching data from %s: %s' % (provider, result))
                errmsg = result
            result = False

        if test:
            return success

        if result:
            logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
            soup = BeautifulSoup(result, 'html5lib')
            # tpb uses a named table
            table = soup.find('table', id='searchResult')
            if table:
                rows = table.find_all('tr')
            else:
                rows = []

            if len(rows) > 1:
                rows = rows[1:]  # first row is headers
            for row in rows:
                td = row.find_all('td')
                if len(td) > 2:
                    try:
                        new_soup = BeautifulSoup(str(td[1]), 'html5lib')
                        link = new_soup.find("a")
                        magnet = link.get("href")
                        title = link.text
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
                            result, success = fetchURL(magurl)
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
                                    'bookid': book['bookid'],
                                    'tor_prov': provider,
                                    'tor_title': title,
                                    'tor_url': magnet,
                                    'tor_size': str(size),
                                    'tor_type': 'magnet',
                                    'priority': lazylibrarian.CONFIG['TPB_DLPRIORITY']
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
                            logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))
                    except Exception as e:
                        logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                        logger.debug('%s: %s' % (provider, traceback.format_exc()))

        if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < page:
            logger.warn('Maximum results page search reached, still more results available')
            next_page = False
        else:
            page += 1

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))
    return results, errmsg


def KAT(book=None, test=False):
    errmsg = ''
    provider = "KAT"
    host = lazylibrarian.CONFIG['KAT_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host + "/usearch/" + quote(book['searchterm']))

    params = {
        "category": "books",
        "field": "seeders",
        "sorder": "desc"
    }
    searchURL = providerurl + "/?%s" % urlencode(params)

    sterm = makeUnicode(book['searchterm'])

    result, success = fetchURL(searchURL)
    if not success:
        # seems KAT returns 404 if no results, not really an error
        if '404' in result:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            success = True
        else:
            logger.debug(searchURL)
            logger.debug('Error fetching data from %s: %s' % (provider, result))
            errmsg = result
        result = False

    if test:
        return success

    results = []

    if result:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
        minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
        soup = BeautifulSoup(result, 'html5lib')
        rows = []
        try:
            table = soup.find_all('table')[1]  # un-named table
            if table:
                rows = table.find_all('tr')
        except IndexError:  # no results table in result page
            rows = []

        if len(rows) > 1:
            rows = rows[1:]  # first row is headers

        for row in rows:
            td = row.find_all('td')
            if len(td) > 3:
                try:
                    title = unaccented(td[0].text)
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

                    if not url or (magnet and url and lazylibrarian.CONFIG['PREFER_MAGNET']):
                        url = magnet
                        mode = 'magnet'

                    try:
                        size = str(td[1].text).replace('&nbsp;', '').upper()
                        size = size_in_bytes(size)
                    except ValueError:
                        size = 0
                    try:
                        seeders = int(td[3].text.replace(',', ''))
                    except ValueError:
                        seeders = 0

                    if not url or not title:
                        logger.debug('Missing url or title')
                    elif minimumseeders < seeders:
                        results.append({
                            'bookid': book['bookid'],
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': mode,
                            'priority': lazylibrarian.CONFIG['KAT_DLPRIORITY']
                        })
                        logger.debug('Found %s. Size: %s' % (title, size))
                    else:
                        logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))
                except Exception as e:
                    logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                    logger.debug('%s: %s' % (provider, traceback.format_exc()))

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))
    return results, errmsg


def WWT(book=None, test=False):
    errmsg = ''
    provider = "WorldWideTorrents"
    host = lazylibrarian.CONFIG['WWT_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host + "/torrents-search.php")

    sterm = makeUnicode(book['searchterm'])

    cat = 0  # 0=all, 36=ebooks, 50=comics, 52=mags, 56=audiobooks
    if 'library' in book:
        if book['library'] == 'AudioBook':
            cat = 56
        elif book['library'] == 'eBook':
            cat = 36
        elif book['library'] == 'comic':
            cat = 50
        elif book['library'] == 'magazine':
            cat = 52

    page = 0
    results = []
    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
    next_page = True

    while next_page:
        params = {
            "search": book['searchterm'],
            "page": page,
            "cat": cat
        }
        searchURL = providerurl + "/?%s" % urlencode(params)

        next_page = False
        result, success = fetchURL(searchURL)
        if not success:
            # might return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s" % (provider, sterm))
                success = True
            elif '503' in result:
                logger.warn("Cloudflare bot detection? %s: %s" % (provider, result))
                logger.warn("Try unblocking %s from a browser" % providerurl)
                success = True
            else:
                logger.debug(searchURL)
                logger.debug('Error fetching data from %s: %s' % (provider, result))
                errmsg = result
            result = False

        if test:
            return success

        if result:
            logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
            soup = BeautifulSoup(result, 'html5lib')
            rows = []
            try:
                tables = soup.find_all('table')  # un-named table
                table = tables[2]
                if table:
                    rows = table.find_all('tr')
            except IndexError:  # no results table in result page
                rows = []

            if len(rows) > 1:
                rows = rows[1:]  # first row is headers

            for row in rows:
                td = row.find_all('td')
                if len(td) > 3:
                    try:
                        title = unaccented(td[0].text)
                        # can return magnet or torrent or both.
                        magnet = ''
                        url = ''
                        mode = 'torrent'
                        try:
                            magnet = 'magnet' + str(td[0]).split('href="magnet')[1].split('"')[0]
                            mode = 'magnet'
                        except IndexError:
                            pass
                        try:
                            url = url_fix(host + '/download.php') + \
                                          str(td[0]).split('href="download.php')[1].split('.torrent"')[0] + '.torrent'
                            mode = 'torrent'
                        except IndexError:
                            pass

                        if not url or (magnet and url and lazylibrarian.CONFIG['PREFER_MAGNET']):
                            url = magnet
                            mode = 'magnet'

                        try:
                            size = str(td[1].text).replace('&nbsp;', '').upper()
                            size = size_in_bytes(size)
                        except ValueError:
                            size = 0
                        try:
                            seeders = int(td[2].text.replace(',', ''))
                        except ValueError:
                            seeders = 0

                        if not url or not title:
                            logger.debug('Missing url or title')
                        elif minimumseeders < seeders:
                            results.append({
                                'bookid': book['bookid'],
                                'tor_prov': provider,
                                'tor_title': title,
                                'tor_url': url,
                                'tor_size': str(size),
                                'tor_type': mode,
                                'priority': lazylibrarian.CONFIG['WWT_DLPRIORITY']
                            })
                            logger.debug('Found %s. Size: %s' % (title, size))
                            next_page = True
                        else:
                            logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))
                    except Exception as e:
                        logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                        logger.debug('%s: %s' % (provider, traceback.format_exc()))
        page += 1
        if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < page:
            logger.warn('Maximum results page search reached, still more results available')
            next_page = False

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))
    return results, errmsg


def EXTRA(book=None, test=False):
    errmsg = ''
    provider = "Extratorrent"
    host = lazylibrarian.CONFIG['EXTRA_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host + "/rss")

    params = {
        "type": "search",
        "s_cat": "2",
        "search": book['searchterm']
    }
    searchURL = providerurl + "/?%s" % urlencode(params)

    sterm = makeUnicode(book['searchterm'])

    data, success = fetchURL(searchURL)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in data:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            success = True
        else:
            logger.debug('Error fetching data from %s: %s' % (provider, data))
            errmsg = data
        data = False

    if test:
        return success

    results = []

    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
    if data:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
        d = feedparser.parse(data)
        if len(d.entries):
            for item in d.entries:
                try:
                    title = unaccented(item['title'])

                    try:
                        seeders = int(item['seeders'].replace(',', ''))
                    except ValueError:
                        seeders = 0

                    try:
                        size = int(item['size'])
                    except ValueError:
                        size = 0

                    url = None
                    for link in item['links']:
                        if 'x-bittorrent' in link['type']:
                            url = link['href']

                    if not url or not title:
                        logger.debug('No url or title found')
                    elif minimumseeders < seeders:
                        results.append({
                            'bookid': book['bookid'],
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': 'torrent',
                            'priority': lazylibrarian.CONFIG['EXTRA_DLPRIORITY']
                        })
                        logger.debug('Found %s. Size: %s' % (title, size))
                    else:
                        logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))

                except Exception as e:
                    logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                    logger.debug('%s: %s' % (provider, traceback.format_exc()))

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))

    return results, errmsg


def ZOO(book=None, test=False):
    errmsg = ''
    provider = "zooqle"
    host = lazylibrarian.CONFIG['ZOO_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host + "/search")

    params = {
        "q": book['searchterm'],
        "category": "books",
        "fmt": "rss"
    }
    searchURL = providerurl + "?%s" % urlencode(params)

    sterm = makeUnicode(book['searchterm'])

    data, success = fetchURL(searchURL)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in data:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            success = True
        else:
            logger.debug(searchURL)
            logger.debug('Error fetching data from %s: %s' % (provider, data))
            errmsg = data
        data = False

    if test:
        return success

    results = []

    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
    if data:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
        d = feedparser.parse(data)
        if len(d.entries):
            for item in d.entries:
                try:
                    title = unaccented(item['title'])
                    seeders = int(item['torrent_seeds'].replace(',', ''))
                    link = item['links'][1]['href']
                    size = int(item['links'][1]['length'])
                    magnet = item['torrent_magneturi']

                    url = None
                    mode = 'torrent'
                    if link:
                        url = link
                        mode = 'torrent'
                    if magnet:
                        if not url or (url and lazylibrarian.CONFIG['PREFER_MAGNET']):
                            url = magnet
                            mode = 'magnet'

                    if not url or not title:
                        logger.debug('No url or title found')
                    elif minimumseeders < seeders:
                        results.append({
                            'bookid': book['bookid'],
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': mode,
                            'priority': lazylibrarian.CONFIG['ZOO_DLPRIORITY']
                        })
                        logger.debug('Found %s. Size: %s' % (title, size))
                    else:
                        logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))

                except Exception as e:
                    if 'forbidden' in str(e).lower():
                        # looks like zooqle has ip based access limits
                        logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                    else:
                        logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                        logger.debug('%s: %s' % (provider, traceback.format_exc()))

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))

    return results, errmsg


def LIME(book=None, test=False):
    errmsg = ''
    provider = "Limetorrent"
    host = lazylibrarian.CONFIG['LIME_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    params = {
        "q": book['searchterm']
    }
    providerurl = url_fix(host + "/searchrss/other")
    searchURL = providerurl + "?%s" % urlencode(params)

    sterm = makeUnicode(book['searchterm'])

    data, success = fetchURL(searchURL)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in data:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            success = True
        else:
            logger.debug(searchURL)
            logger.debug('Error fetching data from %s: %s' % (provider, data))
            errmsg = data
        data = False

    if test:
        return success

    results = []

    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
    if data:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
        d = feedparser.parse(data)
        if len(d.entries):
            for item in d.entries:
                try:
                    title = unaccented(item['title'])
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
                            'bookid': book['bookid'],
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': 'torrent',
                            'priority': lazylibrarian.CONFIG['LIME_DLPRIORITY']
                        }
                        if pubdate:
                            res['tor_date'] = pubdate
                        results.append(res)
                        logger.debug('Found %s. Size: %s' % (title, size))
                    else:
                        logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))

                except Exception as e:
                    if 'forbidden' in str(e).lower():
                        # may have ip based access limits
                        logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                    else:
                        logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                        logger.debug('%s: %s' % (provider, traceback.format_exc()))

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))

    return results, errmsg


def TDL(book=None, test=False):
    errmsg = ''
    provider = "torrentdownloads"
    host = lazylibrarian.CONFIG['TDL_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    providerurl = url_fix(host)

    params = {
        "type": "search",
        "cid": "2",
        "search": book['searchterm']
    }
    searchURL = providerurl + "/rss.xml?%s" % urlencode(params)

    sterm = makeUnicode(book['searchterm'])

    data, success = fetchURL(searchURL)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in data:
            logger.debug("No results found from %s for %s" % (provider, sterm))
            success = True
        else:
            logger.debug(searchURL)
            logger.debug('Error fetching data from %s: %s' % (provider, data))
            errmsg = data
        data = False

    if test:
        return success

    results = []

    minimumseeders = int(lazylibrarian.CONFIG['NUMBEROFSEEDERS']) - 1
    if data:
        logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
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
                        # TDL gives us a relative link
                        result, success = fetchURL(providerurl+link)
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
                                'bookid': book['bookid'],
                                'tor_prov': provider,
                                'tor_title': title,
                                'tor_url': url,
                                'tor_size': str(size),
                                'tor_type': 'magnet',
                                'priority': lazylibrarian.CONFIG['TDL_DLPRIORITY']
                            }
                            if pubdate:
                                res['tor_date'] = pubdate
                            logger.debug('Found %s. Size: %s' % (title, size))
                            results.append(res)
                    else:
                        logger.debug('Found %s but %s seeder%s' % (title, seeders, plural(seeders)))

                except Exception as e:
                    logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                    logger.debug('%s: %s' % (provider, traceback.format_exc()))

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))

    return results, errmsg
