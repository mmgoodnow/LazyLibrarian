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
import time

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.cache import fetchURL
from lazylibrarian.formatter import plural, formatAuthorName, makeUnicode, size_in_bytes, url_fix, \
    makeUTF8bytes, seconds_to_midnight, check_int

from lib.six import PY2
# noinspection PyUnresolvedReferences
from lib.six.moves.urllib_parse import urlparse, urlencode

try:
    import html5lib
    from bs4 import BeautifulSoup
except ImportError:
    if PY2:
        from lib.bs4 import BeautifulSoup
    else:
        from lib3.bs4 import BeautifulSoup


# noinspection PyProtectedMember
def redirect_url(genhost, url):
    """ libgen.io might have dns blocked, but user can bypass using genhost 93.174.95.27 in config
        libgen might send us a book url that still contains http://libgen.io/  or /libgen.io/
        so we might need to redirect it to users genhost setting """

    myurl = urlparse(url)
    if myurl.netloc.lower() != 'libgen.io':
        return url

    host = urlparse(genhost)

    # genhost http://93.174.95.27 -> scheme http, netloc 93.174.95.27, path ""
    # genhost 93.174.95.27 -> scheme "", netloc "", path 93.174.95.27
    if host.netloc:
        if host.netloc.lower() != 'libgen.io':
            # noinspection PyArgumentList
            myurl = myurl._replace(**{"netloc": host.netloc})
            logger.debug('Redirected libgen.io to [%s]' % host.netloc)
    elif host.path:
        if host.path.lower() != 'libgen.io':
            # noinspection PyArgumentList
            myurl = myurl._replace(**{"netloc": host.netloc})
            logger.debug('Redirected libgen.io to [%s]' % host.netloc)
    return myurl.geturl()


def bok_sleep():
    limit = check_int(lazylibrarian.CONFIG['SEARCH_RATELIMIT'], 0)
    if limit:
        time_now = time.time()
        delay = time_now - lazylibrarian.LAST_ZLIBRARY
        if delay < limit:
            sleep_time = limit - delay
            time.sleep(sleep_time)
        lazylibrarian.LAST_ZLIBRARY = time_now


def BOK(book=None, prov=None, test=False):
    errmsg = ''
    provider = "zlibrary"
    if not prov:
        prov = 'BOK'
    if lazylibrarian.providers.ProviderIsBlocked(provider):
        return [], "ProviderIsBlocked"

    host = lazylibrarian.CONFIG[prov + '_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    sterm = makeUnicode(book['searchterm'])
    results = []
    page = 1
    removed = 0
    next_page = True
    if test:
        book['bookid'] = '0'

    while next_page:
        params = {
            "q": makeUTF8bytes(book['searchterm'])[0]
        }
        if page > 1:
            params['page'] = page

        providerurl = url_fix(host + "/s/")
        searchURL = providerurl + "?%s" % urlencode(params)

        next_page = False
        result, success = fetchURL(searchURL)
        if not success:
            # may return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s, got 404 for %s" % (provider, sterm,
                                                                                  searchURL))
                if not test:
                    success = True
            elif '111' in result:
                # may have ip based access limits
                logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                errmsg = result
                delay = check_int(lazylibrarian.CONFIG['BLOCKLIST_TIMER'], 3600)
                lazylibrarian.providers.BlockProvider(provider, errmsg, delay=delay)
            else:
                logger.debug(searchURL)
                logger.debug('Error fetching page data from %s: %s' % (provider, result))
                errmsg = result
            result = ''

        if len(result):
            logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
            try:
                rows = []
                if 'class="fuzzyMatchesLine"' in result:
                    subset = result.split('class="fuzzyMatchesLine"')[0]
                    soup = BeautifulSoup(subset, "html5lib")
                    try:
                        rows = soup.find_all('table', {"class": "resItemTable"})
                    except IndexError:
                        rows = []

                if not rows and not results:  # nothing found in earlier pages or before the cutoff line
                    soup = BeautifulSoup(result, "html5lib")
                    try:
                        rows = soup.find_all('table', {"class": "resItemTable"})
                    except IndexError:
                        rows = []

                for row in rows:
                    url = None
                    newsoup = BeautifulSoup(str(row), 'html5lib')
                    title = newsoup.find('h3', itemprop='name').text
                    for tr in newsoup.find_all('tr'):
                        for a in tr.find_all('a'):
                            link = a['href']
                            if link:
                                url = host + link
                                break
                    author = newsoup.find('a', itemprop='author').text
                    detail = newsoup.find("div", {"class": "bookProperty property__file"}).text
                    try:
                        extn, size = detail.split('\n')[-2].strip().split(',')
                        extn = extn.lower()
                        size = size_in_bytes(size.upper())
                    except IndexError:
                        extn = ''
                        size = 0

                    if url:
                        res, succ = fetchURL(url)
                        if succ:
                            try:
                                newsoup = BeautifulSoup(res, "html5lib")
                                a = newsoup.find('a', {"class": "dlButton"})
                                if not a:
                                    link = ''
                                    delay = 0
                                    msg = ''
                                    if 'WARNING' in res and '24 hours' in res:
                                        msg = res.split('WARNING')[1].split('24 hours')[0]
                                        msg = 'WARNING' + msg + '24 hours'
                                        delay = seconds_to_midnight()
                                    if 'Too many requests' in res:
                                        msg = res
                                        delay = check_int(lazylibrarian.CONFIG['BLOCKLIST_TIMER'], 3600)
                                    if delay:
                                        lazylibrarian.providers.BlockProvider(provider, msg, delay=delay)
                                        logger.warn(msg)
                                else:
                                    link = a['href']
                                if link and len(link) > 2:
                                    url = host + link
                                else:
                                    logger.debug("Link unavailable for %s" % title)
                                    url = None
                                    removed += 1
                            except Exception as e:
                                logger.error("An error occurred parsing %s in the %s parser: %s" %
                                             (url, provider, str(e)))
                                logger.debug('%s: %s' % (provider, traceback.format_exc()))
                                url = None

                    if url:
                        if author:
                            title = author.strip() + ' ' + title.strip()
                        if extn:
                            title = title + '.' + extn

                        results.append({
                            'bookid': book['bookid'],
                            'tor_prov': provider,
                            'tor_title': title,
                            'tor_url': url,
                            'tor_size': str(size),
                            'tor_type': 'direct',
                            'priority': lazylibrarian.CONFIG[prov + '_DLPRIORITY']
                        })
                        logger.debug('Found %s, Size %s' % (title, size))
                    next_page = True

            except Exception as e:
                logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                logger.debug('%s: %s' % (provider, traceback.format_exc()))

        if test:
            logger.debug("Test found %s result%s (%s removed)" % (len(results), plural(len(results)), removed))
            return success

        page += 1
        if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < page:
            logger.warn('Maximum results page search reached, still more results available')
            next_page = False
        else:
            bok_sleep()

        if lazylibrarian.providers.ProviderIsBlocked(provider):
            errmsg = "ProviderIsBlocked"
            next_page = False

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))
    return results, errmsg


def GEN(book=None, prov=None, test=False):
    errmsg = ''
    provider = "libgen.io"
    if not prov:
        prov = 'GEN'
    if lazylibrarian.providers.ProviderIsBlocked(provider):
        return [], "ProviderIsBlocked"
    host = lazylibrarian.CONFIG[prov + '_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    search = lazylibrarian.CONFIG[prov + '_SEARCH']
    if not search:
        search = 'search.php'
    if search[0] == '/':
        search = search[1:]

    sterm = makeUnicode(book['searchterm'])

    page = 1
    results = []
    next_page = True
    maxresults = 100
    if test:
        book['bookid'] = '0'
        maxresults = 25

    while next_page:
        if 'index.php' in search:
            params = {
                "s": makeUTF8bytes(book['searchterm'])[0],
                "f_lang": "All",
                "f_columns": 0,
                "f_ext": "All"
            }
        elif 'search.php' in search:
            params = {
                "view": "simple",
                "open": 0,
                "phrase": 0,
                "column": "def",
                "lg_topic": "libgen",
                "res": maxresults,
                "req": makeUTF8bytes(book['searchterm'])[0]
            }
        else:  # elif 'fiction' in search:
            params = {
                "q": makeUTF8bytes(book['searchterm'])[0]
            }

        if page > 1:
            params['page'] = page

        providerurl = url_fix(host + "/%s" % search)
        searchURL = providerurl + "?%s" % urlencode(params)

        next_page = False
        result, success = fetchURL(searchURL)
        if not success:
            # may return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s, got 404 for %s" % (provider, sterm,
                                                                                  searchURL))
                if not test:
                    success = True
            elif '111' in result:
                # looks like libgen has ip based access limits
                logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                errmsg = result
                delay = check_int(lazylibrarian.CONFIG['BLOCKLIST_TIMER'], 3600)
                lazylibrarian.providers.BlockProvider(provider, errmsg, delay=delay)
            else:
                logger.debug(searchURL)
                logger.debug('Error fetching page data from %s: %s' % (provider, result))
                errmsg = result
            result = False

        if result:
            logger.debug('Parsing results from <a href="%s">%s</a>' % (searchURL, provider))
            try:
                soup = BeautifulSoup(result, 'html5lib')
                rows = []

                try:
                    tables = soup.find_all('table', rules='rows')  # the last table with rules=rows
                    if not tables:
                        tables = soup.find_all('table')
                    if tables:
                        rows = tables[-1].find_all('tr')
                except IndexError:  # no results table in result page
                    rows = []

                if len(rows) > 1:  # skip table headers
                    rows = rows[1:]

                logger.debug("libgen returned %s row%s" % (len(rows), plural(len(rows))))
                for row in rows:
                    author = ''
                    title = ''
                    size = ''
                    extn = ''
                    td = row.find_all('td')
                    links = []

                    if ('fiction' in search or 'index.php' in search) and len(td) > 3:
                        try:
                            author = formatAuthorName(td[0].text)
                            title = td[2].text
                            newsoup = None
                            if '/' in td[4].text:
                                extn = td[4].text.split('/')[0].strip()
                                size = td[4].text.split('/')[1].strip()
                                newsoup = BeautifulSoup(str(td[5]), 'html5lib')
                            elif '(' in td[4].text:
                                extn = td[4].text.split('(')[0].strip()
                                size = td[4].text.split('(')[1].split(')')[0]
                                newsoup = BeautifulSoup(str(td[4]), 'html5lib')
                            size = size.upper()
                            if newsoup:
                                data = newsoup.find_all('a')
                                for d in data:
                                    links.append(d.get('href'))
                        except IndexError as e:
                            logger.debug('Error parsing libgen fiction results: %s' % str(e))
                            pass

                    elif 'search.php' in search and len(td) > 8:
                        # Non-fiction
                        try:
                            author = formatAuthorName(td[1].text)
                            title = td[2].text
                            size = td[7].text.upper()
                            extn = td[8].text
                            td = td[9:-1]
                            for lnk in td:
                                newsoup = BeautifulSoup(str(lnk), 'html5lib')
                                data = newsoup.find_all('a')
                                for d in data:
                                    links.append(d.get('href'))
                        except IndexError as e:
                            logger.debug('Error parsing libgen search.php results; %s' % str(e))
                            pass

                    size = size_in_bytes(size)

                    if links and title:
                        if author:
                            title = author.strip() + ' ' + title.strip()
                        if extn:
                            title = title + '.' + extn

                        success = False
                        bookresult = None
                        url = None
                        for link in links:
                            if link.startswith('http'):
                                url = redirect_url(host, link)
                            else:
                                if "/index.php?" in link:
                                    link = 'md5' + link.split('md5')[1]
                                if "/ads.php?" in link:
                                    url = url_fix(host + "/" + link)
                                else:
                                    url = url_fix(host + "/ads.php?" + link)

                            if "booksdescr.org" in url:
                                # booksdescr is a direct link to book
                                logger.debug(url)
                                success = True
                                break

                            # redirect page for other sources [libgen.me, library1.org, booksdl.org]
                            bookresult, success = fetchURL(url)
                            if not success:
                                logger.debug('Error fetching link data from %s: %s' % (provider, bookresult))
                                logger.debug(url)
                            else:
                                break

                        if success and bookresult:
                            try:
                                new_soup = BeautifulSoup(bookresult, 'html5lib')
                                for link in new_soup.find_all('a'):
                                    output = link.get('href')
                                    if output:
                                        if '/get.php' in output or '/download/' in output or \
                                                '/book/' in output or '/fiction/' in output or \
                                                '/main/' in output:
                                            if output.startswith('http'):
                                                url = output
                                                break
                                            else:
                                                nhost = urlparse(url)
                                                nurl = urlparse(output)
                                                # noinspection PyProtectedMember
                                                nurl = nurl._replace(**{"scheme": nhost.scheme})
                                                # noinspection PyProtectedMember
                                                nurl = nurl._replace(**{"netloc": nhost.netloc})
                                                url = nurl.geturl()
                                                break
                                if url:
                                    if not url.startswith('http'):
                                        url = url_fix(host + url)
                                    else:
                                        url = redirect_url(host, url)
                                    logger.debug("Download URL: %s" % url)
                            except Exception as e:
                                logger.error('%s parsing bookresult: %s' % (type(e).__name__, str(e)))
                                url = None

                        if url:
                            results.append({
                                'bookid': book['bookid'],
                                'tor_prov': provider + '/' + search,
                                'tor_title': title,
                                'tor_url': url,
                                'tor_size': str(size),
                                'tor_type': 'direct',
                                'priority': lazylibrarian.CONFIG[prov + '_DLPRIORITY']
                            })
                            logger.debug('Found %s, Size %s' % (title, size))
                        next_page = True

            except Exception as e:
                logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                logger.debug('%s: %s' % (provider, traceback.format_exc()))

        if test:
            logger.debug("Test found %s result%s" % (len(results), plural(len(results))))
            if not len(results):
                return False
            return success

        page += 1
        if 0 < lazylibrarian.CONFIG['MAX_PAGES'] < page:
            logger.warn('Maximum results page search reached, still more results available')
            next_page = False

        # try to detect libgen mirrors not honouring "page="
        if results:
            last_result_url = results[-1]['tor_url']
            cnt = 0
            for item in results:
                if item['tor_url'] == last_result_url:
                    cnt += 1
                if cnt > 1:
                    break
            if cnt > 1:
                logger.warn('Duplicate results page found from provider')
                next_page = False
        else:
            logger.warn('No results found from provider')
            next_page = False

    logger.debug("Found %i result%s from %s for %s" % (len(results), plural(len(results)), provider, sterm))
    return results, errmsg
