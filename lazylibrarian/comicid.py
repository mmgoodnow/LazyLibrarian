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


import re
import string

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.cache import html_request, gb_json_request
from lazylibrarian.formatter import check_int, check_year, replace_all

try:
    import urllib3
    import requests
except ImportError:
    import lib.requests as requests

from lib.six import PY2
try:
    import html5lib
    from bs4 import BeautifulSoup
except ImportError:
    if PY2:
        from lib.bs4 import BeautifulSoup
    else:
        from lib3.bs4 import BeautifulSoup


def getIssueNum(words, skipped):
    # try to extract issue number from a list of words if not in skipped list
    # this is so we can tell 007 in "james bond 007" is not an issue number
    for word in words:
        if word.startswith('#') and len(word) > 1:
            try:
                return int(word[1:])
            except ValueError:
                pass

    for l in (3, 2, 1):
        for word in words:
            if len(word) == l and word not in skipped:
                try:
                    return int(word)
                except ValueError:
                    pass
    return ''


def nameWords(name):
    # sanitize for better matching
    # strip all ascii and non-ascii quotes/apostrophes
    dic = {u'\u2018': "", u'\u2019': "", u'\u201c': '', u'\u201d': '', "'": "", '"': ''}
    name = replace_all(name, dic)
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    name = regex.sub(' ', name)
    tempwords = name.lower().split()
    # merge initials together into one "word" for matching
    namewords = []
    buildword = ''
    for word in tempwords:
        if len(word) == 1 and not word.isdigit():
            buildword = "%s%s." % (buildword, word)
        else:
            if buildword:
                if len(buildword) == 2:
                    buildword = buildword[0]
                namewords.append(buildword)
                buildword = ''
            namewords.append(word)
    return namewords


def titleWords(words):
    titlewords = []
    # Extract title from filename by skipping leading numbers
    # and stopping when we reach the next number (volume, issue, year)
    for word in words:
        if not titlewords:
            if not word[-1].isdigit():
                titlewords.append(word)
        elif word[-1].isdigit():
            break
        elif len(word) > 1:
            titlewords.append(word)
    return titlewords


def cv_identify(fname, best=True):
    apikey = lazylibrarian.CONFIG['CV_APIKEY']
    if not apikey:
        logger.warn("Please obtain an apikey from https://comicvine.gamespot.com/api/")
        return []

    words = nameWords(fname)
    titlewords = titleWords(words)
    minmatch = 1
    # comicvine sometimes misses matches if we include too many words??
    # we can either use less words, or scrape the html...
    matchwords = '+'.join(titlewords)
    if '+' in matchwords:
        minmatch = 2

    choices = []
    results = []
    offset = 0
    next_page = True
    while next_page:
        if offset:
            off = "&offset=%s" % offset
        else:
            off = ''

        url = 'https://comicvine.gamespot.com/api/volumes/?api_key=%s' % apikey
        url += '&format=json&sort=name:asc&filter=name:%s%s' % (matchwords, off)
        res, in_cache = gb_json_request(url)

        if not res:
            next_page = False
        else:
            results = res['results']
            offset = res['offset']
            total = res['number_of_total_results']
            paged = res['number_of_page_results']
            for item in results:
                title = item['name']
                publisher = item['publisher']
                if publisher:
                    publisher = publisher['name']
                else:
                    publisher = ''
                start = item['start_year']
                link = item['site_detail_url'].replace('\\', '')
                count = item['count_of_issues']
                first = item.get('first_issue', 0)
                if first:
                    first = check_int(first.get('issue_number'), 0)
                last = item.get('last_issue', 0)
                if last:
                    last = check_int(last.get('issue_number'), 0)
                seriesid = item['id']
                description = item['description']
                if description is None:
                    description = ""
                else:
                    soup = BeautifulSoup(description, "html5lib")
                    description = soup.text

                choices.append({"title": title,
                                "publisher": publisher,
                                "start": start,
                                "count": count,
                                "first": first,
                                "last": last,
                                "seriesid": "CV%s" % seriesid,
                                "description": description,
                                "searchterm": matchwords,
                                "link": link
                                })
            if paged and len(choices) < total:
                offset += paged
                next_page = True
            else:
                next_page = False

    if not best:
        return choices

    if choices:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
            logger.debug('Found %i possible for %s' % (len(choices), fname))
        results = []
        year = 0
        # do we have a year to narrow it down
        for w in words:
            if check_year(w):
                year = w
                break

        for item in choices:
            wordcount = 0
            noise = 0
            rejected = False
            namewords = nameWords(item['title'])

            for w in namewords:
                if w in words:
                    wordcount += 1
                else:
                    noise += 1

            if year and item["start"] > year:  # series not started yet
                rejected = True

            issue = getIssueNum(words, namewords)
            if issue and (issue < check_int(item["first"], 0) or issue > check_int(item["last"], 0)):
                rejected = True

            missing = 0
            for w in titlewords:
                if w not in nameWords(item['title']):
                    missing += 1

            if not rejected and wordcount >= minmatch:
                results.append([wordcount, noise, missing, item, issue])

        results = sorted(results, key=lambda x: (-x[0], -(check_int(x[3]["start"], 0)), x[1]))

    if results:
        return results[0]

    if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
        logger.debug('No api match for %s, trying websearch' % fname)
    # fortunately comicvine sorts the resuts and gives us "best match first"
    # so we only scrape the first page (could add &page=2)
    url = 'https://comicvine.gamespot.com/search/?i=volume&q=%s' % matchwords
    data, in_cache = html_request(url)
    if not data:
        logger.warn('No match for %s' % fname)
        return []

    choices = get_volumes_from_search(data)
    if choices:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
            logger.debug('Found %i possible for %s' % (len(choices), fname))
        results = []
        year = 0
        # do we have a year to narrow it down
        for w in words:
            if check_year(w):
                year = w
                break

        for item in choices:
            wordcount = 0
            noise = 0
            rejected = False
            namewords = nameWords(item['title'])

            for w in namewords:
                if w in words:
                    wordcount += 1
                else:
                    noise += 1

            if year and item["start"] > year:  # series not started yet
                rejected = True

            issue = getIssueNum(words, namewords)

            missing = 0
            for w in titlewords:
                if w not in nameWords(item['title']):
                    missing += 1

            if not rejected and wordcount >= minmatch:
                results.append([wordcount, noise, missing, item, issue])

        results = sorted(results, key=lambda x: (-x[0], x[1], -(check_int(x[3]["start"], 0))))

    if results:
        return results[0]

    logger.warn('No match for %s' % fname)
    return []


def get_volumes_from_search(page_content):
    # Return list of volumes for the Comics Series
    choices = []
    soup = BeautifulSoup(page_content, "html5lib")
    h2 = soup.find('h2', class_='header-border')
    if h2:
        matchwords = h2.span.text
    else:
        matchwords = ''

    extracted_volumes = soup.find_all('ul', class_='search-results')
    for item in extracted_volumes:
        try:
            title = item.find('h3').text.strip('\n').strip().strip('\n')
            info = item.find('p').text.strip('\n').strip().strip('\n')
            href = item.find('a', href=True)['href']
            seriesid = href.rsplit('-', 1)[1].strip('/')
            publisher = info.split('(')[-1].split(')')[0]
            start = info.split('(')[0].split(' ', 1)[1].strip()
            count = info.split('(')[1].split(' ')[0]
            match = True
        except IndexError:
            title = ''
            publisher = ''
            href = ''
            seriesid = ''
            start = ''
            count = ''
            match = False

        if match:
            first = 0
            last = 0
            description = ''

            choices.append({"title": title,
                            "publisher": publisher,
                            "start": start,
                            "count": count,
                            "first": first,
                            "last": last,
                            "seriesid": "CV%s" % seriesid,
                            "description": description,
                            "searchterm": matchwords,
                            "link": 'https://comicvine.gamespot.com' + href[0]
                            })
    return choices


def remove_attributes_from_link(link_list, publisher=None):
    # Remove attributes from links in a list and return "clean" list
    clean_link_list = []
    for link in link_list:
        new_link = re.sub(r"(\?.+)", "", link)
        if publisher:
            clean_link_list.append([new_link, publisher])
        else:
            clean_link_list.append(new_link)
    return clean_link_list


def get_series_links_from_search(page_content):
    # Return list of links for the Comics Series
    series_links = []
    soup = BeautifulSoup(page_content, "html5lib")
    res = soup.find_all('div', class_='content-cover')
    extracted_series_links = []
    for item in res:
        extracted_series_links.append(item.find('a', href=True)['href'])
    clean_series_links = remove_attributes_from_link(extracted_series_links)
    series_links.extend(clean_series_links)
    return series_links


def get_series_detail_from_search(page_content):
    # Return details for the Comics Series
    series_detail = {}
    soup = BeautifulSoup(page_content, "html5lib")
    series_detail['publisher'] = soup.find('h3', class_="name").text.strip('\n').strip()
    series_detail['title'] = soup.find('h1', itemprop='name').text
    series_detail['description'] = soup.find('div', itemprop='description').text
    issues = soup.find('div', class_="list Issues")
    series_detail['issues'] = issues.find_all('h6')
    return series_detail


def cx_identify(fname, best=True):
    res = []
    words = nameWords(fname)
    titlewords = titleWords(words)
    minmatch = 1
    matchwords = '+'.join(titlewords)
    if '+' in matchwords:
        minmatch = 2

    url = 'https://www.comixology.com/search/series?search=%s' % matchwords
    data, in_cache = html_request(url)

    if not data:
        logger.warn('No match for %s' % fname)
        return []

    series_links = get_series_links_from_search(data)
    for link in series_links:
        page_number = 1
        next_page = True
        first = 0
        last = 0
        start = ''

        while next_page:
            if page_number == 1:
                data, in_cache = html_request(link)
            else:
                data, in_cache = html_request(link+'?Issues_pg=%s' % page_number)
            soup = BeautifulSoup(data, "html5lib")
            try:
                pager = soup.find('div', class_="list Issues").find(
                                  'div', class_="pager-text").text.strip('\n').strip()
            except AttributeError:
                pager = None

            if pager:
                # eg '1 TO 18 OF 27'
                pager_words = pager[-1].split()
                if pager_words[2] == pager_words[4]:
                    next_page = False
                else:
                    next_page = True
                    page_number += 1
            else:
                next_page = False

            if data:
                series_detail = get_series_detail_from_search(data)
                for item in series_detail['issues']:
                    # noinspection PyBroadException
                    try:
                        num = item.split('#')[1].split(' ')[0]
                        num = check_int(num, 0)
                        if not first:
                            first = num
                        else:
                            first = min(first, num)
                        last = max(last, num)
                    except Exception:
                        pass
                try:
                    start = series_detail['title'].rsplit('(', 1)[1].split('-')[0]
                except IndexError:
                    pass

                series_detail['seriesid'] = "CX%s" % link.rsplit('/', 1)[1]
                series_detail['start'] = start
                series_detail['first'] = first
                series_detail['last'] = last
                series_detail['searchterm'] = matchwords
                series_detail['link'] = link
                series_detail.pop('issues')
                res.append(series_detail)

    if not best:
        return res

    choices = []
    if res:
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
            logger.debug('Found %i possible for %s' % (len(res), fname))
        year = 0
        # do we have a year to narrow it down
        for w in words:
            if check_year(w):
                year = w
                break

        for item in res:
            wordcount = 0
            noise = 0
            missing = 0
            y1 = 0
            y2 = 0
            rejected = False
            if year:  # get year or range from title
                for y in nameWords(item['title']):
                    if check_year(y):
                        if not y1:
                            y1 = y
                        else:
                            y2 = y
                            if y1 > y2:
                                y0 = y2
                                y2 = y1
                                y1 = y0
                            break

            for w in nameWords(item['title']):
                if w in words:
                    if check_year(w):
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
                            logger.debug('Match %s year %s' % (item['title'], year))
                    else:
                        wordcount += 1
                else:
                    if check_year(w):
                        if y1 and y2 and int(y1) <= int(year) <= int(y2):
                            if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
                                logger.debug('Match %s (%s is between %s-%s)' % (item['title'], year, y1, y2))
                            rejected = False
                            break
                        elif y1 and not y2 and int(year) >= int(y1):
                            if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
                                logger.debug('Accept %s (%s is in %s-)' % (item['title'], year, y1))
                            rejected = False
                            break
                        else:
                            if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
                                logger.debug('Rejecting %s, need %s' % (item['title'], year))
                            rejected = True
                            noise += 1
                            break
                    else:
                        noise += 1

            for w in titlewords:
                if w not in nameWords(item['title']):
                    missing += 1

            issue = getIssueNum(words, nameWords(item['title'].split('(')[0]))
            if year and item["start"] > year:  # series not started yet
                rejected = True

            if not rejected and wordcount >= minmatch:
                if (missing + noise)/2 >= wordcount:
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_magdates:
                        logger.debug("Rejecting %s (noise %s)" % (item['title'], missing + noise))
                else:
                    choices.append([wordcount, noise, missing, item, issue])

        if choices:
            choices = sorted(choices, key=lambda x: (-x[0], x[1]))
            return choices[0]

    logger.warn('No match for %s' % fname)
    return []
