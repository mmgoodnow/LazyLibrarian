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
import traceback
import uuid
from shutil import copyfile

import lazylibrarian
from lazylibrarian import database, logger
from lazylibrarian.comicid import cv_identify, cx_identify, comic_metadata, cv_issue, cx_issue
from lazylibrarian.common import walk, setperm, path_isfile, syspath, namedic
from lazylibrarian.formatter import is_valid_booktype, plural, check_int, now, get_list, unaccented, replace_all
from lazylibrarian.images import create_mag_cover
from lazylibrarian.postprocess import create_comic_opf
from six import PY2
if PY2:
    from io import open


def comic_scan(comicid=None):
    lazylibrarian.COMIC_UPDATE = 1
    title = ''
    # noinspection PyBroadException
    try:
        db = database.DBConnection()
        if comicid:
            mags = db.match('select Title from comics WHERE ComicID=?', (comicid,))
            if mags:
                title = mags['Title']
        mag_path = lazylibrarian.CONFIG['COMIC_DEST_FOLDER']
        if title and '$Title' in mag_path:
            comic_name = unaccented(replace_all(title, namedic), only_ascii=False)
            mag_path = mag_path.replace('$Title', comic_name)
            onetitle = comic_name
        else:
            onetitle = None

        while '$' in mag_path:
            mag_path = os.path.dirname(mag_path)

        if lazylibrarian.CONFIG['COMIC_RELATIVE']:
            mag_path = os.path.join(lazylibrarian.directory('eBook'), mag_path)
        if PY2:
            mag_path = mag_path.encode(lazylibrarian.SYS_ENCODING)

        if lazylibrarian.CONFIG['FULL_SCAN'] and not onetitle:
            cmd = 'select Title,IssueID,IssueFile,comics.ComicID from comics,comicissues '
            cmd += 'WHERE comics.ComicID = comicissues.ComicID'
            mags = db.select(cmd)
            # check all the issues are still there, delete entry if not
            for mag in mags:
                title = mag['Title']
                issueid = mag['IssueID']
                comicid = mag['ComicID']
                issuefile = mag['IssueFile']

                if issuefile and not path_isfile(issuefile):
                    db.action('DELETE from comicissues where issuefile=?', (issuefile,))
                    logger.info('Issue %s - %s deleted as not found on disk' % (title, issueid))

                    control_value_dict = {"ComicID": comicid}
                    new_value_dict = {
                        "LastAcquired": None,  # clear magazine dates
                        "LatestIssue": None,  # we will fill them in again later
                        "LatestCover": None,
                        "IssueStatus": "Skipped"  # assume there are no issues now
                    }
                    db.upsert("comics", new_value_dict, control_value_dict)
                    logger.debug('Comic %s (%s) details reset' % (title, comicid))

            # now check the comic titles and delete any with no issues
            if lazylibrarian.CONFIG['COMIC_DELFOLDER']:
                cmd = 'select Title,ComicID,(select count(*) as counter from comicissues '
                cmd += 'where comics.comicid = comicissues.comicid) as issues from comics order by Title'
                mags = db.select(cmd)
                for mag in mags:
                    title = mag['Title']
                    comicid = mag['ComicID']
                    issues = mag['issues']
                    if not issues:
                        logger.debug('Comic %s deleted as no issues found' % title)
                        db.action('DELETE from comics WHERE ComicID=?', (comicid,))

        logger.info(' Checking [%s] for %s' % (mag_path, lazylibrarian.CONFIG['COMIC_TYPE']))

        for rootdir, _, filenames in walk(mag_path):
            for fname in filenames:
                if is_valid_booktype(fname, booktype='comic'):
                    title = ''
                    issue = ''
                    start = ''
                    publisher = ''
                    searchterm = ''
                    issuelink = ''
                    comicid = ''
                    issuedescription = ''
                    contributors = ''
                    aka = ''
                    res = comic_metadata(os.path.join(rootdir, fname))
                    if res:
                        title = res.get('Series')
                        issue = str(check_int(res.get('Number'), 0))
                        comicid = res.get('ComicID')
                        if title and issue and comicid:
                            publisher = res.get('Publisher')
                            start = res.get('Year')
                            searchterm = title
                            issuelink = res.get('Web')
                            issuedescription = res.get('Summary')
                            logger.debug("Metadata found %s (%s) Issue %s" % (title, comicid, issue))

                    res = cv_identify(fname)
                    if not res:
                        res = cx_identify(fname)
                    if res:
                        if not comicid:
                            comicid = res[3]['seriesid']
                        elif comicid and res[3]['seriesid'] and comicid != res[3]['seriesid']:
                            # stick with comicid from metadata and use identify result as aka
                            aka = res[3]['seriesid']

                        if not issue:
                            issue = str(res[4])
                        if not title:
                            title = res[3]['title']
                        if not publisher:
                            publisher = res[3]['publisher']
                        if not start:
                            start = res[3]['start']
                        if not searchterm:
                            searchterm = res[3]['searchterm']
                        first = res[3]['first']
                        last = res[3]['last']
                        serieslink = res[3]['link']
                        seriesdescription = res[3]['description']
                        logger.debug("Found %s (%s) Issue %s" % (title, comicid, issue))

                        control_value_dict = {"ComicID": comicid}
                        # is this comic already in the database?
                        mag_entry = db.match('SELECT * from comics WHERE ComicID=?', (comicid,))
                        if mag_entry:
                            if aka:
                                akas = get_list(mag_entry['aka'])
                                if aka not in akas:
                                    logger.debug("Adding aka %s to %s" % (aka, comicid))
                                    akas.append(aka)
                                    new_value_dict = {"aka": ','.join(akas)}
                                    db.upsert("comics", new_value_dict, control_value_dict)
                        else:
                            mag_entry = db.match('SELECT * from comics WHERE aka LIKE "%' + aka + '%"')
                            if mag_entry:
                                comicid = mag_entry['ComicID']  # use existing comicid
                        if not mag_entry:
                            # need to add a new comic to the database
                            new_value_dict = {
                                "Title": title,
                                "Status": "Active",
                                "Added": now(),
                                "LastAcquired": None,
                                "Updated": now(),
                                "LatestIssue": issue,
                                "IssueStatus": "Skipped",
                                "LatestCover": None,
                                "Start": start,
                                "First": first,
                                "Last": last,
                                "Publisher": publisher,
                                "SearchTerm": searchterm,
                                "Link": serieslink,
                                "Description": seriesdescription,
                                "aka": aka
                            }
                            logger.debug("Adding comic %s (%s)" % (title, comicid))
                            db.upsert("comics", new_value_dict, control_value_dict)
                            lastacquired = None
                            latestissue = issue
                            added = None
                        else:
                            lastacquired = mag_entry['LastAcquired']
                            latestissue = mag_entry['LatestIssue']
                            added = mag_entry['Added']

                        # is this issue already in the database?
                        iss_entry = db.match('SELECT IssueFile from comicissues WHERE ComicID=? and IssueID=?',
                                             (comicid, issue))
                        issuefile = os.path.join(rootdir, fname)  # full path to issue.cbr
                        mtime = os.path.getmtime(issuefile)
                        iss_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))
                        myhash = uuid.uuid4().hex

                        if not iss_entry or (iss_entry['IssueFile'] != issuefile):
                            new_value_dict = {
                                "IssueAcquired": iss_acquired,
                                "IssueFile": issuefile
                            }
                            if not iss_entry:
                                logger.debug("Adding issue %s %s" % (title, issue))
                                coverfile = create_mag_cover(issuefile, refresh=True)
                                if coverfile and path_isfile(coverfile):
                                    hashname = os.path.join(lazylibrarian.CACHEDIR, 'comic', '%s.jpg' % myhash)
                                    copyfile(coverfile, hashname)
                                    setperm(hashname)
                                    new_value_dict['Cover'] = 'cache/comic/%s.jpg' % myhash
                                else:
                                    new_value_dict['Cover'] = 'images/nocover.png'
                                new_value_dict['Description'] = issuedescription
                                new_value_dict['Link'] = issuelink
                            else:
                                logger.debug("Updating issue %s %s" % (title, issue))
                            if not issuedescription or not issuelink or not contributors:
                                # get issue details from series page
                                res = ''
                                if comicid.startswith('CV'):
                                    res = cv_issue(comicid[2:], issue)
                                elif comicid.startswith('CX'):
                                    res = cx_issue(serieslink, issue)
                                if res:  # type: dict
                                    for item in ['Description', 'Link', 'Contributors']:
                                        # noinspection PyTypeChecker
                                        if res[item]:
                                            # noinspection PyTypeChecker
                                            new_value_dict[item] = res[item]

                            control_value_dict = {"ComicID": comicid, "IssueID": issue}
                            db.upsert("comicissues", new_value_dict, control_value_dict)
                            if not iss_entry:
                                dest_path, global_name = os.path.split(issuefile)
                                global_name = os.path.splitext(global_name)[0]
                                data = control_value_dict
                                data.update(new_value_dict)
                                data['Title'] = title
                                data['Publisher'] = publisher
                                if not lazylibrarian.CONFIG['IMP_COMICOPF']:
                                    logger.debug('create_comic_opf is disabled')
                                else:
                                    _ = create_comic_opf(dest_path, data, global_name, overwrite=True)

                        ignorefile = os.path.join(os.path.dirname(issuefile), '.ll_ignore')
                        try:
                            with open(syspath(ignorefile), 'w', encoding='utf-8') as f:
                                f.write(u'comic')
                        except IOError as e:
                            logger.warn("Unable to create/write to ignorefile: %s" % str(e))

                        # see if this issues date values are useful
                        control_value_dict = {"ComicID": comicid}
                        if not mag_entry:  # new magazine, this is the only issue
                            new_value_dict = {
                                "Added": iss_acquired,
                                "LastAcquired": iss_acquired,
                                "LatestCover": 'cache/comic/%s.jpg' % myhash,
                                "LatestIssue": latestissue,
                                "IssueStatus": "Open"
                            }
                            db.upsert("comics", new_value_dict, control_value_dict)
                        else:
                            # Set magazine_issuedate to issuedate of most recent issue we have
                            # Set latestcover to most recent issue cover
                            # Set magazine_added to acquired date of earliest issue we have
                            # Set magazine_lastacquired to acquired date of most recent issue we have
                            # acquired dates are read from magazine file timestamps
                            new_value_dict = {"IssueStatus": "Open"}
                            if not added or iss_acquired < added:
                                new_value_dict["Added"] = iss_acquired
                            if not lastacquired or iss_acquired > lastacquired:
                                new_value_dict["LastAcquired"] = iss_acquired

                            if not latestissue or issue >= latestissue:
                                new_value_dict["LatestIssue"] = issue
                                new_value_dict["LatestCover"] = 'cache/comic/%s.jpg' % myhash
                            db.upsert("comics", new_value_dict, control_value_dict)
                    else:
                        logger.debug("No match for %s" % fname)
        if lazylibrarian.CONFIG['FULL_SCAN'] and not onetitle:
            magcount = db.match("select count(*) from comics")
            isscount = db.match("select count(*) from comicissues")
            logger.info("Comic scan complete, found %s %s, %s %s" %
                        (magcount['count(*)'], plural(magcount['count(*)'], "comic"),
                         isscount['count(*)'], plural(isscount['count(*)'], "issue")))
        else:
            logger.info("Comic scan complete")
        lazylibrarian.COMIC_UPDATE = 0

    except Exception:
        lazylibrarian.COMIC_UPDATE = 0
        logger.error('Unhandled exception in comic_scan: %s' % traceback.format_exc())
