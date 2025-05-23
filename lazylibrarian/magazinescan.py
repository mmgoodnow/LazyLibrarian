#  This file is part of Lazylibrarian.
#  Lazylibrarian is free software, you can redistribute it and/or modify
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
import logging
import os
import re
import threading
import traceback
import uuid
from hashlib import sha1
from shutil import copyfile

import lazylibrarian
from lazylibrarian import database
from lazylibrarian.config2 import CONFIG
from lazylibrarian.filesystem import DIRS, path_isfile, path_isdir, syspath, path_exists, walk, setperm, make_dirs, \
    safe_move, get_directory
from lazylibrarian.formatter import get_list, plural, make_bytestr, replace_all, check_year, unaccented, sanitize, \
    replacevars
from lazylibrarian.images import create_mag_cover


def create_id(issuename=None):
    hash_id = sha1(make_bytestr(issuename)).hexdigest()
    # logger.debug('Issue %s Hash: %s' % (issuename, hashID))
    return hash_id


def magazine_scan(title=None):
    logger = logging.getLogger(__name__)
    loggermatching = logging.getLogger('special.matching')
    lazylibrarian.MAG_UPDATE = 1

    db = database.DBConnection()
    # noinspection PyBroadException
    try:
        mag_path = CONFIG['MAG_DEST_FOLDER']
        if CONFIG.get_bool('MAG_RELATIVE'):
            mag_path = os.path.join(get_directory('eBook'), mag_path)

        onetitle = title
        if onetitle and '$Title' in mag_path:
            mag_path = mag_path.replace('$Title', onetitle)

        while '$' in mag_path:
            mag_path = os.path.dirname(mag_path)

        if CONFIG.get_bool('FULL_SCAN') and not onetitle:
            mags = db.select('select * from Issues')
            # check all the issues are still there, delete entry if not
            for mag in mags:
                title = mag['Title']
                issuedate = mag['IssueDate']
                issuefile = mag['IssueFile']

                if issuefile and not path_isfile(issuefile):
                    db.action('DELETE from Issues where issuefile=?', (issuefile,))
                    logger.info(f'Issue {title} - {issuedate} deleted as not found on disk')
                    control_value_dict = {"Title": title}
                    new_value_dict = {
                        "LastAcquired": None,  # clear magazine dates
                        "IssueDate": None,  # we will fill them in again later
                        "LatestCover": None,
                        "IssueStatus": "Skipped"  # assume there are no issues now
                    }
                    db.upsert("magazines", new_value_dict, control_value_dict)
                    logger.debug(f'Magazine {title} details reset')

            # now check the magazine titles and delete any with no issues
            if CONFIG.get_bool('MAG_DELFOLDER'):
                mags = db.select('SELECT Title,count(Title) as counter from issues group by Title')
                for mag in mags:
                    title = mag['Title']
                    issues = mag['counter']
                    if not issues:
                        logger.debug(f'Magazine {title} deleted as no issues found')
                        db.action('DELETE from magazines WHERE Title=?', (title,))

        logger.info(f" Checking [{mag_path}] for {CONFIG['MAG_TYPE']}")

        booktypes = ''
        count = -1
        booktype_list = get_list(CONFIG['MAG_TYPE'])
        for book_type in booktype_list:
            count += 1
            if count == 0:
                booktypes = book_type
            else:
                booktypes = f"{booktypes}|{book_type}"

        # massage the MAG_DEST_FILE config parameter into something we can use
        # with regular expression matching
        # only escape the non-alpha characters as python 3.7 reserves escaped alpha
        match_string = ''
        matchto = CONFIG['MAG_DEST_FILE']
        for char in matchto:
            if not char.isalpha():
                match_string += '\\'
            match_string = match_string + char

        match = match_string.replace(
            "\\$IssueDate", "(?P<issuedate>.*?)").replace(
            "\\$Title", "(?P<title>.*?)") + r'\.[' + booktypes + ']'
        loggermatching.debug(f"Pattern [{match}]")

        # noinspection PyBroadException
        try:
            pattern = re.compile(match, re.VERBOSE | re.IGNORECASE)
        except Exception as e:
            logger.error(f"Pattern failed for [{matchto}] {str(e)}")
            pattern = None

        if pattern:
            for rootdir, _, filenames in walk(mag_path):
                for fname in filenames:
                    # maybe not all magazines will be pdf?
                    if CONFIG.is_valid_booktype(fname, booktype='mag'):
                        issuedate = ''
                        issuefile = os.path.join(rootdir, fname)  # full path to issue.pdf
                        # noinspection PyBroadException
                        try:
                            match = db.match('SELECT Title,IssueDate from issues WHERE IssueFile=?', (issuefile,))
                            if match:
                                title = match['Title']
                                issuedate = match['IssueDate']
                            if not match:
                                match = pattern.match(fname)
                                if match:
                                    title = match.group("title").strip()
                                    issuedate = match.group("issuedate").strip()
                                    loggermatching.debug(f"Title pattern [{title}][{issuedate}] {fname}")
                                    if title.isdigit():
                                        match = False
                                    else:
                                        parent = os.path.basename(rootdir).strip()
                                        if parent.lower() == title.lower():
                                            # assume folder name is in users preferred case
                                            title = parent
                                        match = True
                                if not match:
                                    logger.debug(f"Title pattern match failed for [{fname}]")
                        except Exception:
                            match = False

                        if not match:
                            title = os.path.basename(rootdir).strip()
                            issuedate = ''

                        dic = {'.': ' ', '-': ' ', '/': ' ', '_': ' ', '(': '', ')': '', '[': ' ', ']': ' ',
                               '#': '# '}
                        datetype = ''

                        # is this magazine already in the database?
                        cmd = ("SELECT Title,LastAcquired,IssueDate,MagazineAdded,CoverPage,DateType,Language "
                               "from magazines WHERE Title=? COLLATE NOCASE")
                        mag_entry = db.match(cmd, (title,))
                        if mag_entry:
                            datetype = mag_entry['DateType']
                        if not datetype:
                            datetype = ''

                        if issuedate:
                            exploded = replace_all(issuedate, dic).split()
                            issuenum_type, issuedate, year = lazylibrarian.searchmag.get_issue_date(exploded,
                                                                                                    datetype=datetype)
                            loggermatching.debug(f"Date style [{issuenum_type}][{issuedate}][{year}]")
                            if issuenum_type:
                                if issuedate.isdigit() and 'I' in datetype:
                                    issuedate = issuedate.zfill(4)
                                    if 'Y' in datetype:
                                        issuedate = year + issuedate
                            else:
                                issuedate = ''

                        if not issuedate:
                            exploded = replace_all(fname, dic).split()
                            issuenum_type, issuedate, year = lazylibrarian.searchmag.get_issue_date(exploded,
                                                                                                    datetype=datetype)
                            loggermatching.debug(f"Filename date style [{issuenum_type}][{issuedate}][{year}]")
                            if issuenum_type:
                                if issuedate.isdigit() and 'I' in datetype:
                                    issuedate = issuedate.zfill(4)
                                    if 'Y' in datetype:
                                        issuedate = year + issuedate
                            else:
                                issuedate = ''

                        if not issuedate:
                            logger.warning(f"Invalid name format for [{fname}]")
                            continue

                        mtime = os.path.getmtime(syspath(issuefile))
                        iss_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))

                        logger.debug(f"Found {title} Issue {issuedate}")

                        if not mag_entry:
                            # need to add a new magazine to the database
                            # title = title.title()
                            control_value_dict = {"Title": title}
                            new_value_dict = {
                                "Reject": None,
                                "Status": "Active",
                                "MagazineAdded": None,
                                "LastAcquired": None,
                                "LatestCover": None,
                                "IssueDate": None,
                                "IssueStatus": "Skipped",
                                "Regex": None,
                                "CoverPage": 1,
                                "Language": "en",
                            }
                            logger.debug(f"Adding magazine {title}")
                            db.upsert("magazines", new_value_dict, control_value_dict)
                            magissuedate = None
                            magazineadded = None
                            maglastacquired = None
                            magcoverpage = 1
                            maglanguage = "en"
                        else:
                            title = mag_entry['Title']
                            maglastacquired = mag_entry['LastAcquired']
                            magissuedate = mag_entry['IssueDate']
                            magazineadded = mag_entry['MagazineAdded']
                            magissuedate = str(magissuedate).zfill(4)
                            magcoverpage = mag_entry['CoverPage']
                            maglanguage = mag_entry['Language']

                        if CONFIG.get_bool('MAG_RENAME'):
                            filedate = issuedate
                            if issuedate and issuedate.isdigit():
                                if len(issuedate) == 8:
                                    if check_year(issuedate[:4]):
                                        filedate = f'Issue {int(issuedate[4:])} {issuedate[:4]}'
                                    else:
                                        filedate = f'Vol {int(issuedate[:4])} Iss {int(issuedate[4:])}'
                                elif len(issuedate) == 12:
                                    filedate = f'Vol {int(issuedate[4:8])} Iss {int(issuedate[8:])} {issuedate[:4]}'
                                else:
                                    filedate = str(issuedate).zfill(4)

                            extn = os.path.splitext(fname)[1]
                            newfname = f"{format_issue_name(CONFIG['MAG_DEST_FILE'], title, issuedate)}{extn}"
                            new_path = format_issue_name(CONFIG['MAG_DEST_FOLDER'], title, filedate)
                            if CONFIG.get_bool('MAG_RELATIVE'):
                                new_path = os.path.join(get_directory('eBook'), new_path)

                            newissuefile = os.path.join(new_path, newfname)
                            # check for windows case-insensitive
                            if os.name == 'nt' and newissuefile.lower() == issuefile.lower():
                                newissuefile = issuefile
                            if newissuefile != issuefile:
                                if not path_isdir(new_path):
                                    make_dirs(new_path)
                                logger.debug(f"Rename {repr(issuefile)} -> {repr(newissuefile)}")
                                try:
                                    newissuefile = safe_move(issuefile, newissuefile)
                                except Exception as e:
                                    logger.error(str(e))

                                for e in ['.jpg', '.opf']:
                                    if path_exists(issuefile.replace(extn, e)):
                                        safe_move(issuefile.replace(extn, e), newissuefile.replace(extn, e))

                                # check for any empty directories
                                try:
                                    os.rmdir(os.path.dirname(issuefile))
                                except OSError:
                                    pass
                                issuefile = newissuefile

                        issuedate = str(issuedate).zfill(4)  # for sorting issue numbers

                        # is this issue already in the database?
                        issue_id = create_id(f"{title} {issuedate}")
                        iss_entry = db.match('SELECT Title,IssueFile,Cover from issues WHERE Title=? and IssueDate=?',
                                             (title, issuedate))

                        new_entry = False
                        myhash = uuid.uuid4().hex
                        if not iss_entry or iss_entry['IssueFile'] != issuefile:
                            coverfile = create_mag_cover(issuefile, pagenum=magcoverpage, refresh=new_entry)
                            if coverfile:
                                hashname = os.path.join(DIRS.CACHEDIR, 'magazine', f'{myhash}.jpg')
                                copyfile(coverfile, hashname)
                                setperm(hashname)
                                cover = f'cache/magazine/{myhash}.jpg'
                            else:
                                cover = 'data/images/nocover.jpg'
                            new_entry = True  # new entry or name changed
                            if not iss_entry:
                                logger.debug(f"Adding issue {title} {issuedate}")
                            else:
                                logger.debug(f"Updating issue {title} {issuedate}")
                            control_value_dict = {"Title": title, "IssueDate": issuedate}
                            new_value_dict = {
                                "IssueAcquired": iss_acquired,
                                "IssueID": issue_id,
                                "IssueFile": issuefile,
                                "Cover": cover
                            }
                            db.upsert("Issues", new_value_dict, control_value_dict)
                        else:
                            logger.debug(f"Issue {title} {issuedate} already exists")
                            cover = iss_entry['Cover']

                        ignorefile = os.path.join(os.path.dirname(issuefile), '.ll_ignore')
                        try:
                            with open(syspath(ignorefile), 'w', encoding='utf-8') as f:
                                f.write(u"magazine")
                        except IOError as e:
                            logger.warning(f"Unable to create/write to ignorefile: {str(e)}")

                        if not CONFIG.get_bool('IMP_MAGOPF'):
                            logger.debug('create_mag_opf is disabled')
                        else:
                            if CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                                authors = title
                            else:
                                authors = 'magazines'
                            lazylibrarian.postprocess.create_mag_opf(issuefile, authors, title, issuedate,
                                                                     issue_id, maglanguage, overwrite=new_entry)

                        # see if this issues date values are useful
                        control_value_dict = {"Title": title}
                        if not mag_entry:  # new magazine, this is the only issue
                            # controlValueDict = {"Title": title.title()}
                            new_value_dict = {
                                "MagazineAdded": iss_acquired,
                                "LastAcquired": iss_acquired,
                                "LatestCover": cover,
                                "IssueDate": issuedate,
                                "IssueStatus": "Open"
                            }
                            db.upsert("magazines", new_value_dict, control_value_dict)
                        else:
                            # Set magazine_issuedate to issuedate of most recent issue we have
                            # Set latestcover to most recent issue cover
                            # Set magazine_added to acquired date of the earliest issue we have
                            # Set magazine_lastacquired to acquired date of most recent issue we have
                            # acquired dates are read from magazine file timestamps
                            new_value_dict = {"IssueStatus": "Open"}
                            if not magazineadded or iss_acquired < magazineadded:
                                new_value_dict["MagazineAdded"] = iss_acquired
                            if not maglastacquired or iss_acquired > maglastacquired:
                                new_value_dict["LastAcquired"] = iss_acquired
                            if not magissuedate or magissuedate == 'None' or issuedate >= magissuedate:
                                new_value_dict["IssueDate"] = issuedate
                                new_value_dict["LatestCover"] = cover
                            db.upsert("magazines", new_value_dict, control_value_dict)

            if CONFIG.get_bool('FULL_SCAN') and not onetitle:
                magcount = db.match("select count(*) from magazines")
                isscount = db.match("select count(*) from issues")
                logger.info(
                    f"Magazine scan complete, found {magcount['count(*)']} "
                    f"{plural(magcount['count(*)'], 'magazine')}, "
                    f"{isscount['count(*)']} {plural(isscount['count(*)'], 'issue')}")
            else:
                logger.info("Magazine scan complete")
        lazylibrarian.MAG_UPDATE = 0

    except Exception:
        lazylibrarian.MAG_UPDATE = 0
        logger.error(f'Unhandled exception in magazine_scan: {traceback.format_exc()}')
    finally:
        if 'MAGAZINE_SCAN' in threading.current_thread().name:
            threading.current_thread().name = 'WEBSERVER'
        db.close()


def format_issue_name(base, mag_title, issue_date):
    logger = logging.getLogger(__name__)
    mydict = {'Title': mag_title, 'IssueDate': issue_date, 'IssueYear': '', 'IssueMonth': '',
              'IssueVol': '', 'IssueNum': '', 'IssueDay': ''}
    # issue_date is yyyy-mm-dd
    # or yyyyiiii or vvvviiii or yyyyvvvviiii or iiii
    if issue_date.isdigit():
        if len(issue_date) == 8:
            if check_year(issue_date[:4]):
                mydict['IssueYear'] = issue_date[:4]
                mydict['IssueNum'] = issue_date[4:]
            else:
                mydict['IssueVol'] = issue_date[:4]
                mydict['IssueNum'] = issue_date[4:]
        elif len(issue_date) == 12:
            mydict['IssueYear'] = issue_date[:4]
            mydict['IssueVol'] = issue_date[4:8]
            mydict['IssueNum'] = issue_date[8:]
        else:
            mydict['IssueNum'] = issue_date.zfill(4)
    elif ' ' not in issue_date:  # check it's not preformatted eg "Vol xxxx Issue yyyy"
        mydict['IssueYear'] = issue_date[:4]
        mydict['IssueNum'] = issue_date[5:7]
        mydict['IssueDay'] = issue_date[8:]
    if mydict['IssueNum'] and mydict['IssueNum'].isdigit():
        if 0 < int(mydict['IssueNum']) < 13:
            # monthnames for this month, eg ["January", "Jan", "enero", "ene"]
            # could change language here to match CONFIG['Date_Lang'] by changing the final [1]
            mydict['IssueMonth'] = lazylibrarian.MONTHNAMES[0][int(mydict['IssueNum'])][1]

    if base == CONFIG['MAG_DEST_FOLDER']:
        valid_format = True
    else:
        valid_format = False
        if '$IssueDate' in base:
            valid_format = True
        if '$IssueYear' in base and '$IssueNum' in base:
            if mydict['IssueYear'] and mydict['IssueNum']:
                valid_format = True
                if mydict['IssueDay'] and mydict['IssueDay'] != '01' and '$IssueDay' not in base:
                    valid_format = False
        if '$IssueVol' in base and '$IssueNum' in base:
            if mydict['IssueVol'] and mydict['IssueNum']:
                valid_format = True
        if '$IssueYear' in base and '$IssueMonth' in base:
            if mydict['IssueYear'] and mydict['IssueMonth']:
                valid_format = True
                if mydict['IssueDay'] and mydict['IssueDay'] != '01' and '$IssueDay' not in base:
                    valid_format = False

    if valid_format:
        issue_name = replacevars(base, mydict)
    else:
        logger.debug(f"Invalid format {base}:{mag_title}:{issue_date}")
        issue_name = f"{mag_title} - {issue_date}"
    issue_name = unaccented(issue_name, only_ascii=False)
    issue_name = sanitize(issue_name)
    return issue_name
