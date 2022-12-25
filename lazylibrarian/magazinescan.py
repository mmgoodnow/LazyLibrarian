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

import datetime
import os
import re
import traceback
import uuid
from hashlib import sha1
from shutil import copyfile

import lazylibrarian
from lazylibrarian import database, logger
from lazylibrarian.filesystem import DIRS, path_isfile, path_isdir, syspath, path_exists, walk, setperm, make_dirs, \
    safe_move, get_directory
from lazylibrarian.formatter import get_list, is_valid_booktype, plural, make_bytestr, \
    replace_all, check_year
from lazylibrarian.images import create_mag_cover
from lazylibrarian.logger import lazylibrarian_log


def create_id(issuename=None):
    hash_id = sha1(make_bytestr(issuename)).hexdigest()
    # logger.debug('Issue %s Hash: %s' % (issuename, hashID))
    return hash_id


def magazine_scan(title=None):
    lazylibrarian.MAG_UPDATE = 1

    # noinspection PyBroadException
    try:
        db = database.DBConnection()
        mag_path = lazylibrarian.CONFIG['MAG_DEST_FOLDER']
        if lazylibrarian.CONFIG.get_bool('MAG_RELATIVE'):
            mag_path = os.path.join(get_directory('eBook'), mag_path)

        onetitle = title
        if onetitle and '$Title' in mag_path:
            mag_path = mag_path.replace('$Title', onetitle)

        while '$' in mag_path:
            mag_path = os.path.dirname(mag_path)

        if lazylibrarian.CONFIG.get_bool('FULL_SCAN') and not onetitle:
            mags = db.select('select * from Issues')
            # check all the issues are still there, delete entry if not
            for mag in mags:
                title = mag['Title']
                issuedate = mag['IssueDate']
                issuefile = mag['IssueFile']

                if issuefile and not path_isfile(issuefile):
                    db.action('DELETE from Issues where issuefile=?', (issuefile,))
                    logger.info('Issue %s - %s deleted as not found on disk' % (title, issuedate))
                    control_value_dict = {"Title": title}
                    new_value_dict = {
                        "LastAcquired": None,  # clear magazine dates
                        "IssueDate": None,  # we will fill them in again later
                        "LatestCover": None,
                        "IssueStatus": "Skipped"  # assume there are no issues now
                    }
                    db.upsert("magazines", new_value_dict, control_value_dict)
                    logger.debug('Magazine %s details reset' % title)

            # now check the magazine titles and delete any with no issues
            if lazylibrarian.CONFIG.get_bool('MAG_DELFOLDER'):
                mags = db.select('SELECT Title,count(Title) as counter from issues group by Title')
                for mag in mags:
                    title = mag['Title']
                    issues = mag['counter']
                    if not issues:
                        logger.debug('Magazine %s deleted as no issues found' % title)
                        db.action('DELETE from magazines WHERE Title=?', (title,))

        logger.info(' Checking [%s] for %s' % (mag_path, lazylibrarian.CONFIG['MAG_TYPE']))

        booktypes = ''
        count = -1
        booktype_list = get_list(lazylibrarian.CONFIG['MAG_TYPE'])
        for book_type in booktype_list:
            count += 1
            if count == 0:
                booktypes = book_type
            else:
                booktypes = booktypes + '|' + book_type

        # massage the MAG_DEST_FILE config parameter into something we can use
        # with regular expression matching
        # only escape the non-alpha characters as python 3.7 reserves escaped alpha
        match_string = ''
        matchto = lazylibrarian.CONFIG['MAG_DEST_FILE']
        for char in matchto:
            if not char.isalpha():
                match_string = match_string + '\\'
            match_string = match_string + char

        match = match_string.replace(
            "\\$IssueDate", "(?P<issuedate>.*?)").replace(
            "\\$Title", "(?P<title>.*?)") + r'\.[' + booktypes + ']'

        # noinspection PyBroadException
        try:
            pattern = re.compile(match, re.VERBOSE | re.IGNORECASE)
        except Exception as e:
            logger.error("Pattern failed for [%s] %s" % (matchto, str(e)))
            pattern = None

        if pattern:
            for rootdir, _, filenames in walk(mag_path):
                for fname in filenames:
                    # maybe not all magazines will be pdf?
                    if is_valid_booktype(fname, booktype='mag'):
                        issuedate = ''
                        # noinspection PyBroadException
                        try:
                            match = pattern.match(fname)
                            if match:
                                title = match.group("title").strip()
                                issuedate = match.group("issuedate").strip()
                                if lazylibrarian_log.LOGLEVEL & logger.log_matching:
                                    logger.debug("Title pattern [%s][%s]" % (title, issuedate))
                                if title.isdigit():
                                    match = False
                                else:
                                    parent = os.path.basename(rootdir).strip()
                                    if parent.lower() == title.lower():
                                        # assume folder name is in users preferred case
                                        title = parent
                                    match = True
                            if not match:
                                logger.debug("Title pattern match failed for [%s]" % fname)
                        except Exception:
                            match = False

                        if not match:
                            title = os.path.basename(rootdir).strip()
                            issuedate = ''

                        dic = {'.': ' ', '-': ' ', '/': ' ', '+': ' ', '_': ' ', '(': '', ')': '', '[': ' ', ']': ' ',
                               '#': '# '}
                        datetype = ''

                        # is this magazine already in the database?
                        cmd = 'SELECT Title,LastAcquired,IssueDate,MagazineAdded,CoverPage,DateType from magazines '
                        cmd += 'WHERE Title=? COLLATE NOCASE'
                        mag_entry = db.match(cmd, (title,))
                        if mag_entry:
                            datetype = mag_entry['DateType']
                        if not datetype:
                            datetype = ''

                        if issuedate:
                            exploded = replace_all(issuedate, dic).split()
                            regex_pass, issuedate, year = lazylibrarian.searchmag.get_issue_date(exploded,
                                                                                                 datetype=datetype)
                            if lazylibrarian_log.LOGLEVEL & logger.log_matching:
                                logger.debug("Date regex [%s][%s][%s]" % (regex_pass, issuedate, year))
                            if regex_pass:
                                if issuedate.isdigit() and 'I' in datetype:
                                    issuedate = issuedate.zfill(4)
                                    if 'Y' in datetype:
                                        issuedate = year + issuedate
                            else:
                                issuedate = ''

                        if not issuedate:
                            exploded = replace_all(fname, dic).split()
                            regex_pass, issuedate, year = lazylibrarian.searchmag.get_issue_date(exploded,
                                                                                                 datetype=datetype)
                            if lazylibrarian_log.LOGLEVEL & logger.log_matching:
                                logger.debug("File regex [%s][%s][%s]" % (regex_pass, issuedate, year))
                            if regex_pass:
                                if issuedate.isdigit() and 'I' in datetype:
                                    issuedate = issuedate.zfill(4)
                                    if 'Y' in datetype:
                                        issuedate = year + issuedate
                            else:
                                issuedate = ''

                        if not issuedate:
                            logger.warn("Invalid name format for [%s]" % fname)
                            continue

                        issuefile = os.path.join(rootdir, fname)  # full path to issue.pdf
                        mtime = os.path.getmtime(syspath(issuefile))
                        iss_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))

                        logger.debug("Found %s Issue %s" % (title, issuedate))

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
                                "CoverPage": 1
                            }
                            logger.debug("Adding magazine %s" % title)
                            db.upsert("magazines", new_value_dict, control_value_dict)
                            magissuedate = None
                            magazineadded = None
                            maglastacquired = None
                            magcoverpage = 1
                        else:
                            title = mag_entry['Title']
                            maglastacquired = mag_entry['LastAcquired']
                            magissuedate = mag_entry['IssueDate']
                            magazineadded = mag_entry['MagazineAdded']
                            magissuedate = str(magissuedate).zfill(4)
                            magcoverpage = mag_entry['CoverPage']

                        if lazylibrarian.CONFIG.get_bool('MAG_RENAME'):
                            filedate = issuedate
                            if issuedate and issuedate.isdigit():
                                if len(issuedate) == 8:
                                    if check_year(issuedate[:4]):
                                        filedate = 'Issue %d %s' % (int(issuedate[4:]), issuedate[:4])
                                    else:
                                        filedate = 'Vol %d Iss %d' % (int(issuedate[:4]), int(issuedate[4:]))
                                elif len(issuedate) == 12:
                                    filedate = 'Vol %d Iss %d %s' % (int(issuedate[4:8]), int(issuedate[8:]),
                                                                     issuedate[:4])
                                else:
                                    filedate = str(issuedate).zfill(4)

                            extn = os.path.splitext(fname)[1]
                            # suppress the "-01" day on monthly magazines
                            # if re.match(r'\d+-\d\d-01', str(filedate)):
                            #    filedate = filedate[:-3]

                            newfname = lazylibrarian.CONFIG['MAG_DEST_FILE'].replace('$Title', title).replace(
                                                                                     '$IssueDate', filedate)
                            newfname = newfname + extn

                            new_path = lazylibrarian.CONFIG['MAG_DEST_FOLDER'].replace('$Title', title).replace(
                                                                                       '$IssueDate', filedate)
                            if lazylibrarian.CONFIG.get_bool('MAG_RELATIVE'):
                                new_path = os.path.join(get_directory('eBook'), new_path)

                            newissuefile = os.path.join(new_path, newfname)
                            # check for windows case-insensitive
                            if os.name == 'nt' and newissuefile.lower() == issuefile.lower():
                                newissuefile = issuefile
                            if newissuefile != issuefile:
                                if not path_isdir(new_path):
                                    make_dirs(new_path)
                                logger.debug("Rename %s -> %s" % (repr(issuefile), repr(newissuefile)))
                                newissuefile = safe_move(issuefile, newissuefile)
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
                        issue_id = create_id("%s %s" % (title, issuedate))
                        iss_entry = db.match('SELECT Title,IssueFile,Cover from issues WHERE Title=? and IssueDate=?',
                                             (title, issuedate))

                        new_entry = False
                        myhash = uuid.uuid4().hex
                        if not iss_entry or iss_entry['IssueFile'] != issuefile:
                            coverfile = create_mag_cover(issuefile, pagenum=magcoverpage, refresh=new_entry)
                            if coverfile:
                                hashname = os.path.join(DIRS.CACHEDIR, 'magazine', '%s.jpg' % myhash)
                                copyfile(coverfile, hashname)
                                setperm(hashname)
                                cover = 'cache/magazine/%s.jpg' % myhash
                            else:
                                cover = 'data/images/nocover.jpg'
                            new_entry = True  # new entry or name changed
                            if not iss_entry:
                                logger.debug("Adding issue %s %s" % (title, issuedate))
                            else:
                                logger.debug("Updating issue %s %s" % (title, issuedate))
                            control_value_dict = {"Title": title, "IssueDate": issuedate}
                            new_value_dict = {
                                "IssueAcquired": iss_acquired,
                                "IssueID": issue_id,
                                "IssueFile": issuefile,
                                "Cover": cover
                            }
                            db.upsert("Issues", new_value_dict, control_value_dict)
                        else:
                            logger.debug("Issue %s %s already exists" % (title, issuedate))
                            cover = iss_entry['Cover']

                        ignorefile = os.path.join(os.path.dirname(issuefile), '.ll_ignore')
                        try:
                            with open(syspath(ignorefile), 'w', encoding='utf-8') as f:
                                f.write(u"magazine")
                        except IOError as e:
                            logger.warn("Unable to create/write to ignorefile: %s" % str(e))

                        if not lazylibrarian.CONFIG.get_bool('IMP_MAGOPF'):
                            logger.debug('create_mag_opf is disabled')
                        else:
                            if lazylibrarian.CONFIG.get_bool('IMP_CALIBRE_MAGTITLE'):
                                authors = title
                            else:
                                authors = 'magazines'
                            lazylibrarian.postprocess.create_mag_opf(issuefile, authors, title, issuedate,
                                                                     issue_id, overwrite=new_entry)

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

            if lazylibrarian.CONFIG.get_bool('FULL_SCAN') and not onetitle:
                magcount = db.match("select count(*) from magazines")
                isscount = db.match("select count(*) from issues")
                logger.info("Magazine scan complete, found %s %s, %s %s" %
                            (magcount['count(*)'], plural(magcount['count(*)'], "magazine"),
                             isscount['count(*)'], plural(isscount['count(*)'], "issue")))
            else:
                logger.info("Magazine scan complete")
        lazylibrarian.MAG_UPDATE = 0

    except Exception:
        lazylibrarian.MAG_UPDATE = 0
        logger.error('Unhandled exception in magazine_scan: %s' % traceback.format_exc())
