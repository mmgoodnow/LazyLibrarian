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


import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.formatter import get_list, unaccented_bytes, unaccented, plural, date_format
from lazylibrarian.providers import iterate_over_rss_sites, iterate_over_torrent_sites, iterate_over_newznab_sites, \
    iterate_over_direct_sites, iterate_over_irc_sites
from lib.thefuzz import fuzz

from six import PY2
# noinspection PyUnresolvedReferences
from six.moves.urllib_parse import quote_plus


def search_item(item=None, bookid=None, cat=None):
    """
    Call all active search providers to search for item
    return a list of results, each entry in list containing percentage_match, title, provider, size, url
    item = searchterm to use for general search
    bookid = link to data for book/audio searches
    cat = category to search [general, book, audio]
    """
    results = []

    if not item:
        return results

    book = {}
    if PY2:
        searchterm = unaccented_bytes(item, only_ascii=False, umlauts=False)
    else:
        searchterm = unaccented(item, only_ascii=False, umlauts=False)

    book['searchterm'] = searchterm
    if bookid:
        book['bookid'] = bookid
    else:
        book['bookid'] = searchterm

    if cat in ['book', 'audio']:
        db = database.DBConnection()
        cmd = 'SELECT authorName,bookName,bookSub from books,authors WHERE books.AuthorID=authors.AuthorID'
        cmd += ' and bookID=?'
        match = db.match(cmd, (bookid,))
        if match:
            book['authorName'] = match['authorName']
            book['bookName'] = match['bookName']
            book['bookSub'] = match['bookSub']
        else:
            logger.debug('Forcing general search')
            cat = 'general'

    nprov = lazylibrarian.use_nzb() + lazylibrarian.use_tor() + lazylibrarian.use_rss()
    nprov += lazylibrarian.use_direct() + lazylibrarian.use_irc()
    logger.debug('Searching %s %s (%s) for %s' % (nprov, plural(nprov, "provider"), cat, searchterm))

    if lazylibrarian.use_nzb():
        resultlist, nprov = iterate_over_newznab_sites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.use_tor():
        resultlist, nprov = iterate_over_torrent_sites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.use_direct():
        resultlist, nprov = iterate_over_direct_sites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.use_irc():
        resultlist, nprov = iterate_over_irc_sites(book, cat)
        if nprov:
            results += resultlist
    if lazylibrarian.use_rss():
        resultlist, nprov, dltypes = iterate_over_rss_sites()
        if nprov and dltypes != 'M':
            results += resultlist

    # reprocess to get consistent results
    searchresults = []
    for item in results:
        provider = ''
        title = ''
        url = ''
        size = ''
        date = ''
        mode = ''
        if 'dispname' in item:
            provider = item['dispname']
        elif 'nzbprov' in item:
            provider = item['nzbprov']
        elif 'tor_prov' in item:
            provider = item['tor_prov']
        elif 'rss_prov' in item:
            provider = item['rss_prov']
        if 'nzbtitle' in item:
            title = item['nzbtitle']
        if 'nzburl' in item:
            url = item['nzburl']
        if 'nzbsize' in item:
            size = item['nzbsize']
        if 'nzbdate' in item:
            date = item['nzbdate']
        if 'nzbmode' in item:
            mode = item['nzbmode']
        if 'tor_title' in item:
            title = item['tor_title']
        if 'tor_url' in item:
            url = item['tor_url']
        if 'tor_size' in item:
            size = item['tor_size']
        if 'tor_date' in item:
            date = item['tor_date']
        if 'tor_type' in item:
            mode = item['tor_type']

        if title and provider and mode and url:
            # Not all results have a date or a size
            if not size:
                size = '1000'
            if date:
                date = date_format(date)
            url = url.encode('utf-8')
            if mode == 'torznab':
                # noinspection PyTypeChecker
                if url.startswith(b'magnet'):
                    mode = 'magnet'

            # calculate match percentage - torrents might have words_with_underscore_separator
            score = fuzz.token_set_ratio(searchterm, title.replace('_', ' '))
            # lose a point for each extra word in the title so we get the closest match
            words = len(get_list(searchterm))
            words -= len(get_list(title))
            score -= abs(words)
            if score >= 40:  # ignore wildly wrong results?
                result = {'score': score, 'title': title, 'provider': provider, 'size': size, 'date': date,
                          'url': quote_plus(url), 'mode': mode}

                searchresults.append(result)

    logger.debug('Found %s %s results for %s' % (len(searchresults), cat, searchterm))
    return searchresults
