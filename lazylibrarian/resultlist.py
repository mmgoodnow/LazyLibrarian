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
#
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

import logging
import traceback

from lazylibrarian import database
from lazylibrarian.config2 import CONFIG
from lazylibrarian.common import only_punctuation
from lazylibrarian.scheduling import schedule_job, SchedulerCommand
from lazylibrarian.downloadmethods import nzb_dl_method, tor_dl_method, \
    direct_dl_method, irc_dl_method
from lazylibrarian.formatter import unaccented, replace_all, get_list, now, check_int
from lazylibrarian.notifiers import notify_snatch, custom_notify_snatch
from lazylibrarian.providers import get_searchterm

from thefuzz import fuzz


def process_result_list(resultlist, book, searchtype, source):
    """ Separated this out into two functions
        1. get the "best" match
        2. if over match threshold, send it to downloader
        This lets us try several searchtypes and stop at the first successful one
        and we can combine results from tor/nzb searches in one task
        Return 0 if not found, 1 if already snatched, 2 if we found it
    """
    match = find_best_result(resultlist, book, searchtype, source)
    if match:
        score = match[0]
        # resultTitle = match[1]
        # newValueDict = match[2]
        # controlValueDict = match[3]
        # dlpriority = match[4]

        if score < CONFIG.get_int('MATCH_RATIO'):
            return 0
        return download_result(match, book)
    return 0


def find_best_result(resultlist, book, searchtype, source):
    """ resultlist: collated results from search providers
        book:       the book we want to find
        searchtype: book, magazine, shortbook, audiobook etc.
        source:     nzb, tor, rss, direct
        return:     highest scoring match, or None if no match
    """
    # noinspection PyBroadException
    logger = logging.getLogger(__name__)
    loggerfuzz = logging.getLogger('special.fuzz')
    db = database.DBConnection()
    try:
        # '0': '', '1': '', '2': '', '3': '', '4': '', '5': '', '6': '', '7': '', '8': '', '9': '',
        dictrepl = {'...': '', '.': ' ', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', ' + ': ' ', '"': '',
                    ',': ' ', '*': '', '(': '', ')': '', '[': '', ']': '', '#': '', '\'': '',
                    ':': '', '!': '', '-': ' ', r'\s\s': ' '}

        dic = {'...': '', '.': ' ', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', ' + ': ' ', '"': '',
               ',': '', '*': '', ':': '.', ';': '', '\'': ''}

        if source == 'rss':
            author, title = get_searchterm(book, searchtype)
        else:
            author = unaccented(replace_all(book['authorName'], dic), only_ascii=False)
            title = unaccented(replace_all(book['bookName'], dic), only_ascii=False, umlauts=False)

        if 'short' in searchtype and '(' in title:
            title = title.split('(')[0].strip()

        if book['library'] == 'AudioBook':
            reject_list = get_list(CONFIG['REJECT_AUDIO'], ',')
            maxsize = CONFIG.get_int('REJECT_MAXAUDIO')
            minsize = CONFIG.get_int('REJECT_MINAUDIO')
            auxinfo = 'AudioBook'

        else:  # elif book['library'] == 'eBook':
            reject_list = get_list(CONFIG['REJECT_WORDS'], ',')
            maxsize = CONFIG.get_int('REJECT_MAXSIZE')
            minsize = CONFIG.get_int('REJECT_MINSIZE')
            auxinfo = 'eBook'

        if source == 'nzb':
            prefix = 'nzb'
        else:  # rss and libgen return same names as torrents
            prefix = 'tor_'

        logger.debug('Searching %s %s results for best %s match' % (len(resultlist), source, auxinfo))

        matches = []
        ignored_messages = []
        for res in resultlist:
            result_title = unaccented(replace_all(res[prefix + 'title'], dictrepl),
                                      only_ascii=False, umlauts=False).strip()
            result_title = ' '.join(result_title.split())  # remove extra whitespace
            only_title = result_title.replace(author, '')
            if not only_title or only_punctuation(only_title):
                book_match = fuzz.token_set_ratio(title, result_title)
            else:
                book_match = fuzz.token_set_ratio(title.replace(author, ''), only_title)
            if 'booksearch' in res and res['booksearch'] == 'bibliotik':
                # bibliotik only returns book title, not author name
                loggerfuzz.debug("bibliotik, ignoring author fuzz")
                author_match = 100
            else:
                author_match = fuzz.token_set_ratio(author, result_title)

            loggerfuzz.debug("%s author/book Match: %s/%s %s at %s" %
                             (source.upper(), author_match, book_match, result_title, res[prefix + 'prov']))

            rejected = False

            url = res[prefix + 'url']
            if not url:
                rejected = True
                logger.debug("Rejecting %s, no URL found" % result_title)

            if not rejected and CONFIG.get_bool('BLACKLIST_FAILED'):
                cmd = "SELECT * from wanted WHERE NZBurl=? and Status='Failed'"
                args = (url,)
                if res.get('tor_type', '') == 'irc':
                    cmd += " and NZBTitle=?"
                    args += (res['tor_title'],)
                blacklisted = db.match(cmd, args)
                if blacklisted:
                    logger.debug("Rejecting %s, url blacklisted (Failed) at %s" %
                                 (res[prefix + 'title'], blacklisted['NZBprov']))
                    rejected = True
                if not rejected:
                    blacklisted = db.match("SELECT * from wanted WHERE NZBprov=? and NZBtitle=? and Status='Failed'",
                                           (res[prefix + 'prov'], res[prefix + 'title']))
                    if blacklisted:
                        logger.debug("Rejecting %s, title blacklisted (Failed) at %s" %
                                     (res[prefix + 'title'], blacklisted['NZBprov']))
                        rejected = True

            if not rejected and CONFIG.get_bool('BLACKLIST_PROCESSED'):
                cmd = "SELECT * from wanted WHERE NZBurl=?"
                args = (url,)
                if res.get('tor_type', '') == 'irc':
                    cmd += " and NZBTitle=?"
                    args += (res['tor_title'],)
                blacklisted = db.match(cmd, args)
                if blacklisted:
                    logger.debug("Rejecting %s, url blacklisted (%s) at %s" %
                                 (res[prefix + 'title'], blacklisted['Status'], blacklisted['NZBprov']))
                    rejected = True
                if not rejected:
                    blacklisted = db.match('SELECT * from wanted WHERE NZBprov=? and NZBtitle=?',
                                           (res[prefix + 'prov'], res[prefix + 'title']))
                    if blacklisted:
                        logger.debug("Rejecting %s, title blacklisted (%s) at %s" %
                                     (res[prefix + 'title'], blacklisted['Status'], blacklisted['NZBprov']))
                        rejected = True

            if not rejected and source == 'rss':
                if searchtype in ['book', 'shortbook'] and 'E' not in res['types']:
                    rejected = True
                    ignore_msg = "Ignoring %s for eBook" % res[prefix + 'prov']
                    if ignore_msg not in ignored_messages:
                        ignored_messages.append(ignore_msg)
                        logger.debug(ignore_msg)
                if 'audio' in searchtype and 'A' not in res['types']:
                    rejected = True
                    ignore_msg = "Ignoring %s for AudioBook" % res[prefix + 'prov']
                    if ignore_msg not in ignored_messages:
                        ignored_messages.append(ignore_msg)
                        logger.debug(ignore_msg)
                if 'mag' in searchtype and 'M' not in res['types']:
                    rejected = True
                    ignore_msg = "Ignoring %s for Magazine" % res[prefix + 'prov']
                    if ignore_msg not in ignored_messages:
                        ignored_messages.append(ignore_msg)
                        logger.debug(ignore_msg)

            if not rejected:
                if source == 'irc':
                    if not url.startswith('!'):
                        rejected = True
                        logger.debug("Rejecting %s, invalid nick [%s]" % (res[prefix + 'title'], url))
                else:
                    if not url.startswith('http') and not url.startswith('magnet'):
                        rejected = True
                        logger.debug("Rejecting %s, invalid URL [%s]" % (res[prefix + 'title'], url))

            if not rejected:
                for word in reject_list:
                    if word in get_list(result_title.lower()) and word not in get_list(author.lower()) \
                            and word not in get_list(title.lower()):
                        rejected = True
                        logger.debug("Rejecting %s, contains %s" % (result_title, word))
                        break

            size_temp = check_int(res[prefix + 'size'], 1000)  # Need to cater for when this is NONE (Issue 35)
            size = round(float(size_temp) / 1048576, 2)

            if not rejected and maxsize and size > maxsize:
                rejected = True
                logger.debug("Rejecting %s, too large (%sMb)" % (result_title, size))

            if not rejected and minsize and size < minsize:
                rejected = True
                logger.debug("Rejecting %s, too small (%sMb)" % (result_title, size))

            if not rejected:
                bookid = book['bookid']

                if source == 'nzb':
                    mode = res.get('nzbmode', '')  # nzb, torznab
                else:
                    mode = res.get('tor_type', '')  # torrent, magnet, nzb(from rss), direct, irc

                control_value_dict = {"NZBurl": url}
                new_value_dict = {
                    "NZBprov": res[prefix + 'prov'],
                    "BookID": bookid,
                    "NZBdate": now(),  # when we asked for it
                    "NZBsize": size,
                    "NZBtitle": res[prefix + 'title'],  # was resultTitle,
                    "NZBmode": mode,
                    "AuxInfo": auxinfo,
                    "Label": res.get('label', ''),
                    "Status": "Matched"
                }
                if source == 'irc':
                    new_value_dict['NZBprov'] = res['tor_feed']
                    new_value_dict['NZBtitle'] = res[prefix + 'title']

                if author_match >= CONFIG.get_int('MATCH_RATIO'):
                    score = book_match
                else:
                    score = (book_match + author_match) / 2  # as a percentage
                # lose a point for each unwanted word in the title so we get the closest match
                # but for rss ignore anything at the end in square braces [keywords, genres etc]
                if source == 'rss':
                    wordlist = get_list(result_title.rsplit('[', 1)[0].lower())
                else:
                    wordlist = get_list(result_title.lower())
                words = [x for x in wordlist if x not in get_list(author.lower())]
                words = [x for x in words if x not in get_list(title.lower())]
                typelist = ''

                if new_value_dict['AuxInfo'] == 'eBook':
                    words = [x for x in words if x not in get_list(CONFIG['EBOOK_TYPE'])]
                    typelist = get_list(CONFIG['EBOOK_TYPE'])
                elif new_value_dict['AuxInfo'] == 'AudioBook':
                    words = [x for x in words if x not in get_list(CONFIG['AUDIOBOOK_TYPE'])]
                    typelist = get_list(CONFIG['AUDIOBOOK_TYPE'])
                score -= len(words)
                # prioritise titles that include the ebook types we want
                # add more points for booktypes nearer the left in the list
                # eg if epub, mobi, pdf  add 3 points if epub found, 2 for mobi, 1 for pdf
                booktypes = [x for x in wordlist if x in typelist]
                if booktypes:
                    typelist = list(reversed(typelist))
                    for item in booktypes:
                        for i in [i for i, x in enumerate(typelist) if x == item]:
                            score += i + 1

                matches.append([score, new_value_dict, control_value_dict, res['priority']])

        if matches:
            highest = max(matches, key=lambda s: (s[0], s[3]))
            score = highest[0]
            new_value_dict = highest[1]
            # controlValueDict = highest[2]
            dlpriority = highest[3]

            if score < CONFIG.get_int('MATCH_RATIO'):
                logger.info('Nearest match (%s%%): %s using %s search for %s %s' %
                            (score, new_value_dict['NZBtitle'], searchtype, book['authorName'], book['bookName']))
            else:
                logger.info('Best match (%s%%): %s using %s search, %s priority %s' %
                            (score, new_value_dict['NZBtitle'], searchtype, new_value_dict['NZBprov'], dlpriority))
            return highest
        else:
            logger.debug("No %s found for [%s] using searchtype %s" % (source, book["searchterm"], searchtype))
        return None
    except Exception:
        logger.error('Unhandled exception in find_best_result: %s' % traceback.format_exc())
    finally:
        db.close()


def download_result(match, book):
    """ match:  best result from search providers
        book:   book we are downloading (needed for reporting author name)
        return: 0 if failed to snatch
                1 if already snatched
                2 if we snatched it
    """
    # noinspection PyBroadException
    logger = logging.getLogger(__name__)
    db = database.DBConnection()
    try:
        new_value_dict = match[1]
        control_value_dict = match[2]

        # It's possible to get book and wanted tables "Snatched" status out of sync
        # for example if a user marks a book as "Wanted" after a search task snatches it and before postprocessor runs
        # so check status in both tables here
        snatched = db.match("SELECT BookID from wanted WHERE BookID=? and AuxInfo=? and Status='Snatched'",
                            (new_value_dict["BookID"], new_value_dict["AuxInfo"]))
        if snatched:
            logger.debug('%s %s %s already marked snatched in wanted table' %
                         (new_value_dict["AuxInfo"], book['authorName'], book['bookName']))
            return 1  # someone else already found it

        if new_value_dict["AuxInfo"] == 'eBook':
            snatched = db.match("SELECT BookID from books WHERE BookID=? and Status='Snatched'",
                                (new_value_dict["BookID"],))
        else:
            snatched = db.match("SELECT BookID from books WHERE BookID=? and AudioStatus='Snatched'",
                                (new_value_dict["BookID"],))
        if snatched:
            logger.debug('%s %s %s already marked snatched in book table' %
                         (new_value_dict["AuxInfo"], book['authorName'], book['bookName']))
            return 1  # someone else already found it

        db.upsert("wanted", new_value_dict, control_value_dict)
        label = new_value_dict.get('Label', '')
        if new_value_dict['NZBmode'] == 'direct':
            snatch, res = direct_dl_method(new_value_dict["BookID"], new_value_dict["NZBtitle"],
                                           control_value_dict["NZBurl"], new_value_dict["AuxInfo"],
                                           new_value_dict['NZBprov'])
        elif new_value_dict['NZBmode'] == 'irc':
            snatch, res = irc_dl_method(new_value_dict["BookID"], new_value_dict["NZBtitle"],
                                        control_value_dict["NZBurl"], new_value_dict["AuxInfo"],
                                        new_value_dict['NZBprov'])
        elif new_value_dict['NZBmode'] in ["torznab", "torrent", "magnet"]:
            snatch, res = tor_dl_method(new_value_dict["BookID"], new_value_dict["NZBtitle"],
                                        control_value_dict["NZBurl"], new_value_dict["AuxInfo"], label)
        elif new_value_dict['NZBmode'] == 'nzb':
            snatch, res = nzb_dl_method(new_value_dict["BookID"], new_value_dict["NZBtitle"],
                                        control_value_dict["NZBurl"], new_value_dict["AuxInfo"], label)
        else:
            res = 'Unhandled NZBmode [%s] for %s' % (new_value_dict['NZBmode'], control_value_dict["NZBurl"])
            logger.error(res)
            snatch = 0

        if snatch:
            logger.info('Downloading %s %s from %s' %
                        (new_value_dict["AuxInfo"], new_value_dict["NZBtitle"], new_value_dict["NZBprov"]))
            custom_notify_snatch("%s %s" % (new_value_dict["BookID"], new_value_dict['AuxInfo']))
            notify_snatch("%s %s from %s at %s" %
                          (new_value_dict["AuxInfo"], new_value_dict["NZBtitle"],
                           CONFIG.disp_name(new_value_dict["NZBprov"]), now()))
            # at this point we could add NZBprov to the blocklist with a short timeout, a second or two?
            # This would implement a round-robin search system. Blocklist with an incremental counter.
            # If number of active providers == number blocklisted, so no unblocked providers are left,
            # either sleep for a while, or unblock the one with the lowest counter.
            schedule_job(SchedulerCommand.START, target='PostProcessor')
            return 2  # we found it
        else:
            db.action("UPDATE wanted SET status='Failed',DLResult=? WHERE NZBurl=?",
                      (res, control_value_dict["NZBurl"]))
        return 0
    except Exception:
        logger.error('Unhandled exception in download_result: %s' % traceback.format_exc())
        return 0
    finally:
        db.close()
