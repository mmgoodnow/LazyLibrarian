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

import time
import traceback
from urllib.parse import urlparse, urlencode

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.cache import fetch_url
from lazylibrarian.formatter import plural, format_author_name, make_unicode, size_in_bytes, url_fix, \
    make_utf8bytes, seconds_to_midnight

import html5lib
from bs4 import BeautifulSoup


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
            # noinspection PyArgumentList,PyProtectedMember
            myurl = myurl._replace(**{"netloc": host.netloc})
            logger.debug('Redirected libgen.io to [%s]' % host.netloc)
    elif host.path:
        if host.path.lower() != 'libgen.io':
            # noinspection PyArgumentList,PyProtectedMember
            myurl = myurl._replace(**{"netloc": host.netloc})
            logger.debug('Redirected libgen.io to [%s]' % host.netloc)
    return myurl.geturl()


def bok_sleep():
    time_now = time.time()
    delay = time_now - lazylibrarian.TIMERS['LAST_BOK']
    limit = lazylibrarian.CONFIG.get_int('SEARCH_RATELIMIT')
    # make sure bok leaves at least a 2-second delay between calls to prevent "Too many requests from your IP"
    if limit < 2.0:
        limit = 2.0
    if delay < limit:
        sleep_time = limit - delay
        lazylibrarian.TIMERS['SLEEP_BOK'] += sleep_time
        if lazylibrarian_log.LOGLEVEL & logger.log_cache:
            logger.debug("B-OK sleep %.3f, total %.3f" % (sleep_time, lazylibrarian.TIMERS['SLEEP_BOK']))
        time.sleep(sleep_time)
    lazylibrarian.TIMERS['LAST_BOK'] = time_now


def direct_bok(book=None, prov=None, test=False):
    errmsg = ''
    provider = "zlibrary"
    if not prov:
        prov = 'BOK'
    if lazylibrarian.providers.provider_is_blocked(provider):
        if test:
            return False
        return [], "provider is already blocked"

    bok_today = lazylibrarian.bok_dlcount()[0]
    if bok_today and bok_today >= lazylibrarian.CONFIG.get_int(prov + '_DLLIMIT'):
        if test:
            return False
        return [], "download limit reached"

    host = lazylibrarian.CONFIG[prov + '_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    sterm = make_unicode(book['searchterm'])
    results = []
    page = 1
    removed = 0
    next_page = True
    if test:
        book['bookid'] = '0'

    while next_page:
        params = {
            "q": make_utf8bytes(book['searchterm'])[0]
        }
        if page > 1:
            params['page'] = page

        providerurl = url_fix(host + "/s/")
        search_url = providerurl + "?%s" % urlencode(params)

        next_page = False
        bok_sleep()
        result, success = fetch_url(search_url)
        if not success or len(result) < 100:  # may return a "blocked" message
            # may return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s, got 404 for %s" % (provider, sterm,
                                                                                  search_url))
                if test:
                    return 0
            elif '111' in result:
                # may have ip based access limits
                logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                errmsg = result
                lazylibrarian.providers.block_provider(provider, errmsg)
            else:
                logger.debug(search_url)
                logger.debug('Error fetching page data from %s: %s' % (provider, result))
                errmsg = result
            if test:
                return False
            return results, errmsg

        if len(result):
            logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
            try:
                rows = []
                if 'class="fuzzyMatchesLine"' in result:
                    logger.debug("Skipping fuzzy matches for %s" % book['searchterm'])
                    subset = result.split('class="fuzzyMatchesLine"')[0]
                    soup = BeautifulSoup(subset, "html5lib")
                    try:
                        rows = soup.find_all('table', {"class": "resItemTable"})
                    except IndexError:
                        logger.debug("No table found in results")
                        rows = []

                if not rows and not results:  # nothing found in earlier pages or before the cutoff line
                    soup = BeautifulSoup(result, "html5lib")
                    try:
                        rows = soup.find_all('table', {"class": "resItemTable"})
                    except IndexError:
                        logger.debug("No table found in results")
                        rows = []

                logger.debug("Found %s rows for %s" % (len(rows), book['searchterm']))
                for row in rows:
                    if lazylibrarian.providers.provider_is_blocked(provider):
                        next_page = False
                        break
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
                        res = detail.split('\n')[-1].strip().split(',')
                        extn = res[0].lower()
                        size = res[-1]
                        if len(res) == 3:
                            size = "%s.%s" % (res[1], res[2])
                        size = size_in_bytes(size.upper())
                    except (IndexError, ValueError):
                        extn = ''
                        size = 0

                    if url:
                        bok_sleep()
                        res, succ = fetch_url(url)
                        if succ:
                            try:
                                newsoup = BeautifulSoup(res, "html5lib")
                                a = newsoup.find('a', {"class": "dlButton"})
                                if not a:
                                    link = ''
                                    if 'WARNING' in res and '24 hours' in res:
                                        msg = res.split('WARNING')[1].split('24 hours')[0]
                                        msg = 'WARNING' + msg + '24 hours'
                                        count, oldest = lazylibrarian.bok_dlcount()
                                        if count and count >= lazylibrarian.CONFIG.get_int(prov + '_DLLIMIT'):
                                            # rolling 24hr delay if limit reached
                                            delay = oldest + 24*60*60 - time.time()
                                        else:
                                            delay = seconds_to_midnight()
                                        lazylibrarian.providers.block_provider(provider, msg, delay=delay)
                                        logger.warn(msg)
                                        url = None
                                    elif 'Too many requests' in res:
                                        lazylibrarian.providers.block_provider(provider, res)
                                        logger.warn(res)
                                        url = None
                                else:
                                    link = a.get('href')
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
            logger.debug("Test found %s %s (%s removed)" % (len(results), plural(len(results), "result"), removed))
            return len(results)

        page += 1
        if 0 < lazylibrarian.CONFIG.get_int('MAX_PAGES') < page:
            logger.warn('Maximum results page search reached, still more results available')
            next_page = False
        else:
            bok_sleep()

        if lazylibrarian.providers.provider_is_blocked(provider):
            errmsg = "provider_is_blocked"
            next_page = False

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    return results, errmsg


def direct_bfi(book=None, prov=None, test=False):
    errmsg = ''
    provider = "BookFi"
    if not prov:
        prov = 'BFI'
    if lazylibrarian.providers.provider_is_blocked(provider):
        if test:
            return False
        return [], "provider_is_blocked"

    host = lazylibrarian.CONFIG['BFI_HOST']
    if not host.startswith('http'):
        host = 'http://' + host

    sterm = make_unicode(book['searchterm'])
    results = []
    removed = 0
    if test:
        book['bookid'] = '0'

    params = {
        "q": make_utf8bytes(book['searchterm'])[0]
    }

    providerurl = url_fix(host + "/s/")
    search_url = providerurl + "?%s" % urlencode(params)

    result, success = fetch_url(search_url)
    if not success:
        # may return 404 if no results, not really an error
        if '404' in result:
            logger.debug("No results found from %s for %s, got 404 for %s" % (provider, sterm,
                                                                              search_url))
            if test:
                return 0
        elif '111' in result:
            # may have ip based access limits
            logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
            errmsg = result
            lazylibrarian.providers.block_provider(provider, errmsg)
        else:
            logger.debug(search_url)
            logger.debug('Error fetching page data from %s: %s' % (provider, result))
            errmsg = result
        if test:
            return False
        return results, errmsg

    if len(result):
        logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
        try:
            soup = BeautifulSoup(result, "html5lib")
            try:
                rows = soup.find_all('div', {"class": "resItemBox"})
            except IndexError:
                logger.debug("No item box found in results")
                rows = []

            for row in rows:
                if lazylibrarian.providers.provider_is_blocked(provider):
                    break
                rowsoup = BeautifulSoup(str(row), 'html5lib')
                title = rowsoup.find('h3', itemprop='name').text
                link = rowsoup.find('a', {"class": "ddownload"})
                url = link['href']

                if '(' in link.text:
                    extn = link.text.split('(')[1].split(')')[0].lower()
                else:
                    extn = ''
                author = rowsoup.find('a', itemprop='author').text

                try:
                    detail = rowsoup.find("span", itemprop='inLanguage').find_parent().text
                    size = detail.split('\n')[0]
                    size = size_in_bytes(size.upper())
                except (IndexError, AttributeError):
                    size = 0

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

        except Exception as e:
            logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
            logger.debug('%s: %s' % (provider, traceback.format_exc()))

    if test:
        logger.debug("Test found %s %s (%s removed)" % (len(results), plural(len(results), "result"), removed))
        return len(results)

    if lazylibrarian.providers.provider_is_blocked(provider):
        errmsg = "provider_is_blocked"

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    return results, errmsg


def direct_gen(book=None, prov=None, test=False):
    errmsg = ''
    host = ''
    search = ''
    priority = 0
    provider = "libgen"
    if not prov:
        prov = 'GEN_0'
    if lazylibrarian.providers.provider_is_blocked(prov):
        if test:
            return False
        return [], "provider_is_blocked"
    for entry in lazylibrarian.CONFIG.providers('GEN'):
        if entry['NAME'].lower() == prov.lower():
            host = entry['HOST']
            if not host.startswith('http'):
                host = 'http://' + host
            search = entry['SEARCH']
            if not search:
                search = 'search.php'
            if search[0] == '/':
                search = search[1:]
            priority = entry['DLPRIORITY']
            break

    if not host:
        return [], "Unknown Provider [%s]" % prov

    sterm = make_unicode(book['searchterm'])

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
                "f_lang": "All",
                "f_columns": 0,
                "f_ext": "All"
            }
            # for index.php, default to s=
            if "?req=" in search or "&req=" in search:
                search = search.replace("?req=", "").replace("&req=", "")
                params['req'] = make_utf8bytes(book['searchterm'])[0]
            else:
                if "?s=" in search or "&s=" in search:
                    search = search.replace("?req=", "").replace("&req=", "")
                params["s"] = make_utf8bytes(book['searchterm'])[0]
        elif 'search.php' in search:
            params = {
                "view": "simple",
                "open": 0,
                "phrase": 0,
                "column": "def",
                "lg_topic": "libgen",
                "res": maxresults
            }
            # for search.php, default to req=
            if "?s=" in search or "&s=" in search:
                search = search.replace("?s=", "").replace("&s=", "")
                params['s'] = make_utf8bytes(book['searchterm'])[0]
            else:
                if "?req=" in search or "&req=" in search:
                    search = search.replace("?req=", "").replace("&req=", "")
                params["req"] = make_utf8bytes(book['searchterm'])[0]
        elif 'comic' in search:
            params = {
                "s": make_utf8bytes(book['searchterm'])[0]
            }
        else:  # elif 'fiction' in search:
            params = {
                "q": make_utf8bytes(book['searchterm'])[0]
            }

        if page > 1:
            params['page'] = page

        providerurl = url_fix(host + "/%s" % search)
        search_url = providerurl + "?%s" % urlencode(params)

        next_page = False
        result, success = fetch_url(search_url)
        if not success:
            # may return 404 if no results, not really an error
            if '404' in result:
                logger.debug("No results found from %s for %s, got 404 for %s" % (provider, sterm,
                                                                                  search_url))
            if test:
                return 0
            elif '111' in result:
                # looks like libgen has ip based access limits
                logger.error('Access forbidden. Please wait a while before trying %s again.' % provider)
                errmsg = result
                lazylibrarian.providers.block_provider(prov, errmsg)
            else:
                logger.debug(search_url)
                logger.debug('Error fetching page data from %s: %s' % (provider, result))
                errmsg = result
            if test:
                return False
            return results, errmsg

        if len(result):
            logger.debug('Parsing results from <a href="%s">%s</a>' % (search_url, provider))
            try:
                soup = BeautifulSoup(result, 'html5lib')
                rows = []

                try:
                    if 'comic' in search:
                        tables = soup.find_all('table', align='center')
                    else:
                        tables = soup.find_all('table', rules='rows')  # the last table with rules=rows
                    if not tables:
                        tables = soup.find_all('table')
                    if tables:
                        # all rows from the last matching table
                        rows = tables[-1].find_all('tr')
                except IndexError:  # no results table in result page
                    logger.debug("No table found in results")
                    rows = []

                if len(rows) > 1:  # skip table headers
                    rows = rows[1:]

                logger.debug("libgen returned %s %s" % (len(rows), plural(len(rows), "row")))
                for row in rows:
                    author = ''
                    title = ''
                    size = ''
                    extn = ''
                    td = row.find_all('td')
                    links = []

                    if td and 'comic' in search:
                        try:
                            if 'FILE' in str(td[-1]):
                                newsoup = BeautifulSoup(str(td[1]), 'html5lib')
                                data = newsoup.find_all('a')
                                for d in data:
                                    links.append(d.get('href'))
                                title = td[3].text.strip()
                                issue = ''
                                year = ''
                                publisher = ''
                                language = ''
                                for f in range(4, len(td)-1):
                                    if 'Issue: ' in td[f].text:
                                        issue = td[f].text.split('Issue: ')[1].strip()
                                    elif 'Year: ' in td[f].text:
                                        year = td[f].text.split('Year: ')[1].strip()
                                    elif 'Publisher: ' in td[f].text:
                                        publisher = td[f].text.split('Publisher: ')[1].strip()
                                    elif 'Language: ' in td[f].text:
                                        language = td[f].text.split('Language: ')[1].strip()
                                    elif not size:
                                        if '<br' in str(td[f]) and td[f].text[0].isdigit():
                                            size = str(td[f]).split('>')[1].split('<br')[0]
                                            extn = str(td[f]).split('<br')[1].split('>')[1].split('<')[0]
                                    logger.debug("Title: %s Issue:%s Year:%s Pub:%s Lang:%s Size: %s" %
                                                 (title, issue, year, publisher, language, size))
                        except Exception as e:
                            logger.debug('Error parsing libgen comic results: %s' % str(e))
                            pass

                    elif ('fiction' in search or 'index.php' in search) and len(td) > 3:
                        try:
                            author = format_author_name(td[0].text)
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
                            author = format_author_name(td[1].text)
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
                            if link.startswith('magnet'):
                                url = link
                            elif "comic" in search or "booksdescr.org" in link:
                                # booksdescr is a direct link to book
                                url = link
                                if not url.startswith('http'):
                                    url = url_fix(host + "/" + url)
                                    logger.debug(url)
                                success = True
                                break
                            elif link.startswith('http'):
                                url = redirect_url(host, link)
                            else:
                                if "/index.php?" in link:
                                    link = 'md5' + link.split('md5')[1]
                                if "/ads.php?" in link:
                                    url = url_fix(host + "/" + link)
                                else:
                                    url = url_fix(host + "/ads.php?" + link)

                            # redirect page for other sources [libgen.me, library1.org, booksdl.org]
                            bookresult, success = fetch_url(url)
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
                                    url = make_unicode(url)
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
                                'priority': priority
                            })
                            logger.debug('Found %s, Size %s' % (title, size))
                        next_page = True

            except Exception as e:
                logger.error("An error occurred in the %s parser: %s" % (provider, str(e)))
                logger.debug('%s: %s' % (provider, traceback.format_exc()))

            if test:
                logger.debug("Test found %s %s" % (len(results), plural(len(results), "result")))
                return len(results)

        page += 1
        if 0 < lazylibrarian.CONFIG.get_int('MAX_PAGES') < page:
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

    logger.debug("Found %i %s from %s for %s" % (len(results), plural(len(results), "result"), provider, sterm))
    return results, errmsg
