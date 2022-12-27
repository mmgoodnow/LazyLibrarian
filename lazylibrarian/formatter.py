#  This file is part of Lazylibrarian.
# coding: utf-8
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
from hashlib import md5
import os
import re
import unicodedata
import threading
from typing import Optional, List

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.logger import lazylibrarian_log
from urllib.parse import quote_plus, quote, urlsplit, urlunsplit


# dict to remove/replace characters we don't want in a filename - this might be too strict?
namedic = {'<': '', '>': '', '...': '', ' & ': ' ', ' = ': ' ', '?': '', '$': 's', '|': '',
           ' + ': ' ', '"': '', ',': '', '*': '', ':': '', ';': '', '\'': '', '//': '/',
           '\\\\': '\\'}

#   ä ö ü Ä Ö Ü ß
umlaut_dict = {u'\xe4': 'ae', u'\xf6': 'oe', u'\xfc': 'ue', u'\xc4': 'Ae', u'\xd6': 'Oe', u'\xdc': 'Ue', u'\xdf': 'ss'}

apostrophe_dic = {u'\u0060': "'", u'\u2018': u"'", u'\u2019': u"'", u'\u201c': u'"', u'\u201d': u'"'}


# noinspection PyDeprecation
def thread_name(name=None) -> str:
    if name:
        threading.current_thread().name = name
        return name
    else:
        return threading.current_thread().name


def sanitize(name):
    """
    Sanitizes a string so it can be used as a file name, normalized as Unicode
    Returns a sanitized string
    """
    if not name:
        return ''
    filename = make_unicode(name)
    # replace non-ascii quotes with regular ones
    filename = replace_all(filename, apostrophe_dic)
    # strip characters we don't want in a filename
    filename = replace_all(filename, namedic)
    # Remove all characters below code point 32
    filename = u"".join(c for c in filename if 31 < ord(c))
    filename = unicodedata.normalize('NFC', filename)
    # windows filenames can't end in space or dot
    while filename and filename[-1] in '. ':
        filename = filename[:-1]
    return filename


def versiontuple(versionstring):
    """
    Convert a version string into 3 part tuple without using packaging.version.parse
    as we can't be sure that's installed
    Note this assumes bugfix component starts with a digit eg 1.2.3-beta4
    and drops any extension part, eg 1.2.3-beta4 becomes (1, 2, 3)
    which is not strictly correct so we need to check for version > too_low
    rather than >= lowest acceptable
    """
    major = 0
    minor = 0
    bugfix = 0
    parts = versionstring.split('.', 2)
    nparts = len(parts)
    if nparts:
        major = check_int(parts[0], 0)
    if nparts > 1:
        minor = check_int(parts[1], 0)
    if nparts > 2:
        part = ''
        for i in parts[2]:
            if i.isdigit():
                part += i
            else:
                break
        bugfix = check_int(part, 0)
    return major, minor, bugfix


def url_fix(s, charset='utf-8'):
    """
    Return the argument so it's valid in a web context
    """
    if not isinstance(s, str):
        s = s.decode(charset)
    scheme, netloc, path, qs, anchor = urlsplit(s)
    path = quote(path, '/%')
    qs = quote_plus(qs, ':&=')
    return urlunsplit((scheme, netloc, path, qs, anchor))


def book_series(bookname):
    """
    Try to get a book series/seriesnum from a bookname, or return empty string
    See if book is in multiple series first, if so return first one
    eg "The Shepherds Crown (Discworld, #41; Tiffany Aching, #5)"
    if no match, try single series, eg Mrs Bradshaws Handbook (Discworld, #40.5)
    Returns seriesname, number if a series, otherwise "", ""
    """
    # \(            Must have (
    # ([\S\s]+      followed by a group of one or more non whitespace
    # [^)])        not ending in )
    # ,? #?         followed by optional comma, then space optional hash
    # (             start next group
    # \d+           must have one or more digits
    # \.?           then optional decimal point, (. must be escaped)
    # -?            optional dash for a range
    # \d{0,}        zero or more digits
    # [;,]          a semicolon or comma if multiple series
    # )             end group
    series = ""
    seriesnum = ""

    # First handle things like "(Book 3: series name)"
    if ':' in bookname:
        # change to "(series name, Book 3)"
        if bookname[0] == "(" and bookname[-1] == ")":
            parts = bookname[1:-1].split(':', 1)
            if parts[0][-1].isdigit():
                bookname = '(%s, %s)' % (parts[1], parts[0])

    # These are words that don't indicate a following series name/number eg "FIRST 3 chapters"
    non_series_words = ['series', 'unabridged', 'volume', 'phrase', 'from', 'chapters', 'season',
                        'the first', 'includes', 'paperback', 'first', 'books', 'large print', 'of',
                        'rrp', '2 in', '&', 'v.']

    # First look for multi series
    result = re.search(r"\(([\S\s]+[^)]),? #?(\d+\.?-?\d*[;,])", bookname)
    if result:
        series = result.group(1)
        while series[-1] in ',)':
            series = series[:-1]
        seriesnum = result.group(2)
        while seriesnum[-1] in ';,':
            seriesnum = seriesnum[:-1]
    else:
        result = re.search(r"\(([\S\s]+[^)]),? #?(\d+\.?-?\d*)", bookname)
        if result:
            series = result.group(1)
            while series[-1] in ',)':
                series = series[:-1]
            seriesnum = result.group(2)

    for word in [' novel', ' book', ' part', ' -']:
        if series and series.lower().endswith(word):
            series = series[:-len(word)]
            break

    for word in non_series_words:
        if series.lower().startswith(word):
            return "", ""

    series = clean_name(unaccented(series, only_ascii=False)).strip()
    seriesnum = seriesnum.strip()
    if series.lower().strip('.') == 'vol':
        series = ''
    if series.lower().strip('.').endswith('vol'):
        series = series.strip('.')
        series = series[:-3].strip()
    return series, seriesnum



def now():
    dtnow = datetime.datetime.now()
    return dtnow.strftime("%Y-%m-%d %H:%M:%S")


def today():
    dtnow = datetime.datetime.now()
    return dtnow.strftime("%Y-%m-%d")


def seconds_to_midnight():
    """Get the number of seconds to midnight."""
    tomorrow = datetime.datetime.now() + datetime.timedelta(1)
    midnight = datetime.datetime(year=tomorrow.year, month=tomorrow.month,
                                 day=tomorrow.day, hour=0, minute=0, second=0)
    return (midnight - datetime.datetime.now()).seconds


def age(histdate):
    """
    Return how many days since histdate
    histdate = yyyy-mm-dd
    return 0 for today, or if invalid histdate
    """
    return datecompare(today(), histdate)


def check_year(num, past=1850, future=1):
    # See if num looks like a valid year
    # for a magazine allow forward dated by a year, eg Jan 2017 issues available in Dec 2016
    n = check_int(num, 0)
    if past < n <= int(now()[:4]) + future:
        return n
    return 0


def nzbdate2format(nzbdate):
    """
    Returns an "nzb date" in yyyy-mm-dd format
    Returns 1970-01-01 if the date can't be parsed
    """
    try:
        mmname = nzbdate.split()[2].zfill(2)
        day = nzbdate.split()[1]
        # nzbdates are mostly english short month names, but not always
        month = month2num(mmname)
        if month == 0:
            month = 1  # hopefully won't hit this, but return a default value rather than error
        year = nzbdate.split()[3]
        return "%s-%02d-%s" % (year, month, day)
    except IndexError:
        return "1970-01-01"


def date_format(datestr, formatstr="$Y-$m-$d"):
    # return date formatted in requested style
    # $d	Day of the month as a zero-padded decimal number
    # $b	Month as abbreviated name
    # $B	Month as full name
    # $m	Month as a zero-padded decimal number
    # $y	Year without century as a zero-padded decimal number
    # $Y	Year with century as a decimal number
    # datestr are stored in lazylibrarian as YYYY-MM-DD or YYYY-MM-DD HH:MM:SS or nnnn for issue number

    # Dates from providers are in various formats, need to consolidate them so we can sort...
    # Newznab/Torznab Tue, 23 Aug 2016 17:33:26 +0100
    # LimeTorrent 13 Nov 2014 05:01:18 +0200
    # torrent_tpb 04-25 23:46 or 2018-04-25
    # openlibrary May 1995 or June 20, 2008
    # sometimes one "word" eg 28Dec2008
    # We could use dateutil module but it's not standard library and we only have a few formats
    # so we can roll our own. To make it simple we'll ignore timezone and seconds

    if not datestr:
        return ''

    if datestr.isdigit():  # just issue number or year
        return datestr

    dateparts = datestr.split(' +')[0].split(';')[0].replace(
        '-', ' ').replace(':', ' ').replace(',', ' ').replace('/', ' ').split()
    if len(dateparts) == 1:  # one "word" might need splitting
        dateparts = []
        word = ''
        digits = True
        for c in datestr:
            if digits and c.isdigit():
                word += c
            elif not digits and not c.isdigit():
                word += c
            elif word:
                dateparts.append(word)
                word = c
                digits = not digits
            else:
                word = c
                digits = c.isdigit()
        if word:
            dateparts.append(word)

    if len(dateparts) == 7:  # Tue, 23 Aug 2016 17:33:26
        _, d, m, y, hh, mm, _ = dateparts
    elif len(dateparts) == 6:
        if check_year(dateparts[0]):  # YYYY-MM-DD HH:MM:SS
            y, m, d, hh, mm, _ = dateparts
        elif check_year(dateparts[1]):  # MM-YYYY-DD HH:MM:SS
            m, y, d, hh, mm, _ = dateparts
        elif check_year(dateparts[2]):  # 13 Nov 2014 05:01:18
            d, m, y, hh, mm, _ = dateparts
        else:
            d, m, y, hh, mm = 0, 0, 0, 0, 0
    elif len(dateparts) == 5:  # 2018-04-25 23:46
        y, m, d, hh, mm = dateparts
    elif len(dateparts) == 4:  # 04-25 23:46 (this year)
        m, d, hh, mm = dateparts
        y = now()[:4]
    elif len(dateparts) == 3:  # 2018-04-25 or June 20 2008 or 20 June 2008
        if check_year(dateparts[0]):
            y, m, d = dateparts
        else:
            if dateparts[0].isdigit():
                d, m, y = dateparts
            else:
                m, d, y = dateparts
        hh = '00'
        mm = '00'
    elif len(dateparts) == 2:  # May 1995
        m, y = dateparts
        d = '01'
        hh = '00'
        mm = '00'
    else:
        d, m, y, hh, mm = 0, 0, 0, 0, 0

    if not m:
        lazylibrarian.logger.warn("Unrecognised datestr [%s]" % datestr)
        return datestr

    try:
        _ = int(m)
    except ValueError:
        m = "%02d" % month2num(m)

    datestr = "%s-%s-%s %s:%s:00" % (y, m.zfill(2), d.zfill(2), hh, mm)
    if not formatstr:
        if len(dateparts) == 1:
            return datestr[:4]  # only year
        return datestr[:11]  # default yyyy-mm-dd

    # noinspection PyBroadException
    try:
        return formatstr.replace(
            '$Y', datestr[0:4]).replace(
            '$y', datestr[2:4]).replace(
            '$m', datestr[5:7]).replace(
            '$d', datestr[8:10]).replace(
            '$B', lazylibrarian.MONTHNAMES[int(datestr[5:7])][0].title()).replace(
            '$b', lazylibrarian.MONTHNAMES[int(datestr[5:7])][1].title())
    except Exception:
        lazylibrarian.logger.error("Invalid datestr [%s]" % datestr)
        return datestr


def month2num(month):
    """
    Return a month number
     - given a month name (long or short) in requested locales
     - or given a season name (only in English)
    """

    month = unaccented(month).lower()
    for f in range(1, 13):
        if month in lazylibrarian.MONTHNAMES[f]:
            return f

    if month == "winter":
        return 1
    elif month == "spring":
        return 4
    elif month == "summer":
        return 7
    elif month in ["fall", "autumn"]:
        return 10
    elif month == "christmas":
        return 12
    else:
        return 0


def datecompare(nzbdate, control_date):
    """
    Return how many days between two dates given in yy-mm-dd format or yyyy-mm-dd format
    or zero if error (not a valid date)
    """
    try:
        y1, m1, d1 = (int(x) for x in nzbdate.split('-'))
        y2, m2, d2 = (int(x) for x in control_date.split('-'))
        if y1 < 100:
            y1 += 1900
        if y2 < 100:
            y2 += 1900
        date1 = datetime.date(y1, m1, d1)
        date2 = datetime.date(y2, m2, d2)
        dtage = date1 - date2
        return dtage.days
    except ValueError:
        return 0


def plural(var, phrase=""):
    """
    Convenience function for pluralising log messages
    if var = 1 return phrase unchanged
    if var is anything else return phrase + 's'
    so book -> books, seeder -> seeders  etc
    or return translation
    so copy -> copies, entry -> entries  etc
    """
    translates = {
                    'copy': 'copies',
                    'entry': 'entries',
                    'shelf': 'shelves',
                    'series': 'series',
                    'is': 'are',
                 }
    if check_int(var, 0) == 1:
        return phrase
    res = translates.get(phrase, '')
    if res:
        return res
    return '%ss' % phrase


def check_int(var, default, positive=True):
    """
    Return an integer representation of var
    or return default value if var is not a positive integer
    """
    try:
        res = int(var)
        if positive and res < 0:
            return default
        return res
    except (ValueError, TypeError):
        try:
            return int(default)
        except (ValueError, TypeError):
            return 0


def check_float(var, default):
    """
    Return a float representation of var
    or return default value if var is not a float
    """
    try:
        return float(var)
    except (ValueError, TypeError):
        try:
            return float(default)
        except (ValueError, TypeError):
            return 0.0


def human_size(num):
    num = check_int(num, 0)
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if abs(num) < 1024.0:
            return "%3.2f%s" % (num, unit)
        num /= 1024.0
    return "%.2f%s" % (num, 'PiB')


def size_in_bytes(size):
    """
    Take a size string with units eg 10 Mb, 5.3Kb
    and return integer bytes
    """
    if not size:
        return 0
    mult = 1
    try:
        if 'K' in size:
            size = size.split('K')[0]
            mult = 1024
        elif 'M' in size:
            size = size.split('M')[0]
            mult = 1024 * 1024
        elif 'G' in size:
            size = size.split('G')[0]
            mult = 1024 * 1024 * 1024
        size = int(float(size) * mult)
    except (ValueError, IndexError):
        size = 0
    return size


def md5_utf8(txt):
    if isinstance(txt, str):
        txt = txt.encode('utf-8')
    # noinspection PyDeprecation
    return md5(txt).hexdigest()


# Special character hex range:
# CP850: 0x80-0xA5 (fortunately not used in ISO-8859-15) aka latin-1
# UTF-8: 1st hex code 0xC2-0xC3 followed by a 2nd hex code 0xA1-0xFF
# ISO-8859-15: 0xA6-0xFF
# The function will detect if string contains a special character
# If there is special character, detects if it is a UTF-8, CP850 or ISO-8859-15 encoding
def make_utf8bytes(txt):
    name = make_bytestr(txt)
    # parse to detect if CP850/ISO-8859-15 is used
    # and return tuple of bytestring encoded in utf-8, detected encoding
    for idx in range(len(name)):
        # /!\ detection is done 2char by 2char for UTF-8 special character
        ch = chr(name[idx])
        if idx < (len(name) - 1):
            chx = chr(name[idx + 1])
            # Detect UTF-8
            if ((ch == '\xC2') | (ch == '\xC3')) & ((chx >= '\xA0') & (chx <= '\xFF')):
                return name, 'UTF-8'
        # Detect CP850 or Windows CP1252 (latin-1)
        if (ch >= '\x80') & (ch <= '\xA5'):
            name = name.decode('cp850')
            return name.encode('utf-8'), 'CP850'
        # Detect ISO-8859-15 (latin-9)
        if (ch >= '\xA6') & (ch <= '\xFF'):
            name = name.decode('iso-8859-15')
            return name.encode('utf-8'), 'ISO-8859-15'
    return name, ''


_encodings = ['utf-8', 'iso-8859-15', 'cp850']

def make_unicode(txt) -> str:
    # convert a bytestring to unicode, don't know what encoding it might be so try a few
    # it could be a file on a windows filesystem, unix...
    if isinstance(txt, str):  # nothing to do if already unicode
        return txt
    if txt is None:
        return txt
    if not isinstance(txt, bytes):  # list, int etc
        txt = str(txt)
        if isinstance(txt, str):
            return txt
    if lazylibrarian.SYS_ENCODING.lower() not in _encodings:
        _encodings.insert(0, lazylibrarian.SYS_ENCODING)
    for encoding in _encodings:
        try:
            return txt.decode(encoding)
        except (UnicodeError, LookupError):
            pass
    lazylibrarian.logger.debug("Unable to decode name [%s]" % repr(txt))
    return txt


def make_bytestr(txt):
    """
    Turn text into a binary byte string
    """
    if isinstance(txt, bytes):  # nothing to do if already bytestring
        return txt
    if txt is None:
        return txt
    if not isinstance(txt, str):  # list, int etc
        txt = str(txt)
        if isinstance(txt, bytes):
            return txt
    if lazylibrarian.SYS_ENCODING.lower() not in _encodings:
        _encodings.insert(0, lazylibrarian.SYS_ENCODING)
    for encoding in _encodings:
        try:
            return txt.encode(encoding)
        except UnicodeError:
            pass
    lazylibrarian.logger.debug("Unable to encode name [%s]" % repr(txt))
    return txt


def is_valid_isbn(isbn):
    """
    Return True if parameter looks like a valid isbn
    either 13 digits, 10 digits, or 9 digits followed by 'x'
    """
    if not isbn:
        return False
    isbn = isbn.replace('-', '').replace(' ', '')
    if len(isbn) == 13 and isbn.isdigit():
         return True
    if len(isbn) == 10 and isbn[:9].isdigit(): # Validate checksum
        xsum = 0
        for i in range(9):
            xsum += check_int(isbn[i], 0) * (10-i)
        if isbn[9] in "Xx":
            xsum += 10
        else:
            xsum += check_int(isbn[9], 0)
        return xsum % 11 == 0
    return False


def is_valid_type(filename: str, extensions: List[str], extras='jpg, opf') -> bool:
    """
    Check if filename has an extension we can process.
    returns True or False
    """
    type_list = extensions + get_list(extras)
    extn = os.path.splitext(filename)[1].lstrip('.')
    return extn and extn.lower() in type_list


def get_list(st, c=None):
    """
    Split a string/unicode into a list on whitespace or plus or comma
    or single character split eg filenames with spaces split on comma only
    Returns list of same type as st
    """
    lst = []
    if st:
        if c is not None and len(c) == 1:
            x = st.split(c)
            for item in x:
                lst.append(item.strip())
        else:
            st = st.replace(',', ' ').replace('+', ' ')
            lst = ' '.join(st.split()).split()
    return lst


# noinspection PyArgumentList
def safe_unicode(obj, *args):
    """ return the unicode representation of obj """
    return str(obj, *args)

def split_title(author, book):
    """
    Strips the author name from book title and
    returns the book name part split into (name, subtitle and series)
    """
    from lazylibrarian.config2 import CONFIG
    if lazylibrarian_log.LOGLEVEL & logger.log_matching:
        lazylibrarian.logger.debug("%s [%s]" % (author, book))
    bookseries = ''
    splitlist = get_list(CONFIG['IMP_NOSPLIT'], ',')
    # Strip author from title, eg Tom Clancy: Ghost Protocol
    if book.startswith(author + ':'):
        book = book.split(author + ':')[1].strip()
    brace = book.rfind('(') + 1
    if brace and book.endswith(')'):
        # if title ends with words in braces, split on last brace
        # as this always seems to be a subtitle or series info
        # If there is a digit before the closing brace assume it's series
        # eg Abraham Lincoln: Vampire Hunter (Abraham Lincoln: Vampire Hunter, #1)
        # unless the part in braces is one word, eg (TM) or (Annotated) or (Unabridged)
        if book[-2].isdigit():
            # separate out the series part
            book, bookseries = book.rsplit('(', 1)
            bookseries = bookseries.strip(')')
            book = book.strip().rstrip(':').strip()
        else:
            parts = book.rsplit('(', 1)
            parts[1] = '(' + parts[1]
            bookname = parts[0].strip()
            booksub = parts[1].rstrip(':').strip()
            if booksub.find(')'):
                for item in splitlist:
                    if f"({item})" == booksub.lower():
                        booksub = ""
            if lazylibrarian_log.LOGLEVEL & logger.log_matching:
                lazylibrarian.logger.debug("[%s][%s][%s]" % (bookname, booksub, bookseries))
            return bookname, booksub, bookseries

    # if not (words in braces at end of string)
    # split subtitle on first ':'
    colon = book.find(':') + 1
    bookname = book
    booksub = ''
    if colon:
        parts = book.split(':', 1)
        bookname = parts[0].strip()
        booksub = parts[1].rstrip(':').strip()
        bookname_lower = bookname.lower()
        booksub_lower = booksub.lower()
        for item in splitlist:
            if item and booksub_lower.startswith(item) or bookname_lower.startswith(item):
                bookname = book
                booksub = ''
                break
    if lazylibrarian_log.LOGLEVEL & logger.log_matching:
        lazylibrarian.logger.debug("Name[%s] Sub[%s] Series[%s]" % (bookname, booksub, bookseries))
    return bookname, booksub, bookseries


def format_author_name(author: str, config: Optional[ConfigDict]=None) -> str:
    if not config:  # TODO: Fix this hack when config is properly parameterised everywhere
        from lazylibrarian.config2 import CONFIG
        config = CONFIG

    """ get authorname in a consistent format """
    author = make_unicode(author)
    # if multiple authors assume the first one is primary
    if '& ' in author:
        author = author.split('& ')[0].strip()
    if "," in author:
        postfix = get_list(config['NAME_POSTFIX'])
        words = author.split(',')
        if len(words) == 2:
            # Need to handle names like "L. E. Modesitt, Jr." or "J. Springmann, Phd"
            # use an exceptions list for now, there might be a better way...
            if words[1].strip().strip('.').strip('_').lower() in postfix:
                surname = words[1].strip()
                forename = words[0].strip()
            else:
                # guess its "surname, forename" or "surname, initial(s)" so swap them round
                forename = words[1].strip()
                surname = words[0].strip()
            if author != forename + ' ' + surname:
                if lazylibrarian_log.LOGLEVEL & logger.log_fuzz:
                    lazylibrarian.logger.debug('Formatted authorname [%s] to [%s %s]' %
                                               (author, forename, surname))
                author = forename + ' ' + surname
    # reformat any initials, we want to end up with L.E. Modesitt Jr
    if len(author) > 2 and author[1] in '. ':
        surname = author
        forename = ''
        while len(surname) > 2 and surname[1] in '. ':
            forename = forename + surname[0] + '.'
            surname = surname[2:].strip()
        if author != forename + ' ' + surname:
            if lazylibrarian_log.LOGLEVEL & logger.log_fuzz:
                lazylibrarian.logger.debug('Stripped authorname [%s] to [%s %s]' %
                                           (author, forename, surname))
            author = forename + ' ' + surname

    res = ' '.join(author.split())  # ensure no extra whitespace
    if res.isupper() or res.islower():
        res = res.title()
    return res


def sort_definite(title: str, config: Optional[ConfigDict]=None) -> str:
    if not config:  # TODO: Fix this hack when config is properly parameterised everywhere
        from lazylibrarian.config2 import CONFIG
        config = CONFIG
    """
    Return the sort string for a title, moving prefixes
    we want to ignore to the end, like The or A
    """
    words = get_list(title)
    if len(words) < 2:
        return title
    word = words.pop(0)
    if word.lower() in get_list(config['NAME_DEFINITE']):
        return ' '.join(words) + ', ' + word
    return title


def surname_first(authorname: str, postfixes: List[str]) -> str:
    """ Swap authorname round into surname, forenames for display and sorting"""
    words = get_list(authorname)
    if len(words) < 2:
        return authorname
    res = words.pop()

    if res.strip('.').lower() in postfixes:
        res = words.pop() + ' ' + res
    return res + ', ' + ' '.join(words)


def clean_name(name, extras=None):
    if not name:
        return u''

    if extras and "'" in extras:
        name = replace_all(name, apostrophe_dic)

    valid_name_chars = u"-_.() %s" % extras
    cleaned = u''.join(c for c in name if c in valid_name_chars or c.isalnum())
    cleaned = cleaned.strip()
    if cleaned:
        return cleaned
    return name


def no_umlauts(s: str, config: Optional[ConfigDict]=None) -> str:
    if not config:  # TODO: Fix this hack when config is properly parameterised everywhere
        from lazylibrarian.config2 import CONFIG
        config = CONFIG
    if 'de' not in get_list(config['IMP_PREFLANG']):
        return s

    s = replace_all(s, umlaut_dict)
    res = ''
    # long form to split out the accents
    s = unicodedata.normalize('NFKD', s)
    # replace any diacritic 0x308 (umlaut) with 'e' so 'ü': 'ue', 'Ä': 'Ae'
    for c in s:
        if unicodedata.combining(c) and ord(c) == 0x308:
            res += 'e'
        else:
            res += c
    # back to short form for consistency
    return unicodedata.normalize('NFC', res)


def unaccented(str_or_unicode, only_ascii=True, umlauts=True):
    if not str_or_unicode:
        return u''
    return make_unicode(unaccented_bytes(str_or_unicode, only_ascii=only_ascii, umlauts=umlauts))


def unaccented_bytes(str_or_unicode, only_ascii=True, umlauts=True):
    if not str_or_unicode:
        return ''.encode('ASCII')  # ensure bytestring for python3
    # use long form to separate out the accents into combining type
    try:
        cleaned = unicodedata.normalize('NFKD', str_or_unicode)
    except TypeError:
        cleaned = unicodedata.normalize('NFKD', str_or_unicode.decode('utf-8', 'replace'))

    if not umlauts:
        cleaned = no_umlauts(cleaned)

    # turn accented chars into non-accented
    stripped = u''.join([c for c in cleaned if not unicodedata.combining(c)])
    # replace all non-ascii quotes/apostrophes with ascii ones eg "Collector's"
    dic = apostrophe_dic
    # Other characters not converted by unicodedata.combining
    # c6 Ae, d0 Eth, d7 multiply, d8 Ostroke, de Thorn, df sharpS
    dic.update({u'\xc6': 'A', u'\xd0': 'D', u'\xd7': '*', u'\xd8': 'O', u'\xde': 'P', u'\xdf': 's'})
    # e6 ae, f0 eth, f7 divide, f8 ostroke, fe thorn
    dic.update({u'\xe6': 'a', u'\xf0': 'o', u'\xf7': '/', u'\xf8': 'o', u'\xfe': 'p'})
    stripped = replace_all(stripped, dic)
    if only_ascii is not False:
        # now get rid of any other non-ascii
        if only_ascii is True:  # just strip out
            stripped = stripped.encode('ASCII', 'ignore')
        else:  # replace with specified char (use '_' for goodreads author names)
            stripped = stripped.encode('ASCII', 'replace')  # replaces with '?'
            stripped = stripped.replace(b'?', make_bytestr(str(only_ascii)[0]))

    stripped = stripped.strip()
    if not stripped:
        stripped = str_or_unicode
    return stripped  # return bytestring


def replace_all(text, dic):
    if not text:
        return ''
    for item in dic:
        text = text.replace(item, dic[item])
    return text


def replace_quotes_with(text, char):
    """
    Replaces every occurrence of quote characters in "text"
    with char - which can be blank to remove them
    """
    if not text:
        return ''
    replaces = re.compile('[%s]' % re.escape(''.join(lazylibrarian.common.quotes)))
    return replaces.sub(char, text)


def disp_name(provider: str) -> str:
    # TODO: Move this method to a place where config2 can be imported
    """
    Strange function. Returns the display name of a provider that
    matches the host name provided as parameter, if any.
    If not, returns the host name provided, shortened if too long.
    """
    from lazylibrarian import configdefs
    from lazylibrarian.config2 import CONFIG
    provname = ''
    for name, definitions in configdefs.ARRAY_DEFS.items():
        key = definitions[0] # Primary key for this array type
        array = CONFIG.providers(name)
        if array and not provname:
            for index, config in array._configs.items():
                if config[key].strip('/') == provider:
                    provname = config['DISPNAME']
                    break

    if not provname:
        provname = provider
    if len(provname) > 20:
        while len(provname) > 20 and '/' in provname:
            provname = provname.split('/', 1)[1]
        provname = provname.replace('/', ' ')
    return provname
