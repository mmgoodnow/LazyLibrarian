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
from lazylibrarian.common import walk, setperm, path_isfile, syspath
from lazylibrarian.formatter import is_valid_booktype, plural, check_int, now
from lazylibrarian.images import createMagCover
from lazylibrarian.postprocess import createComicOPF
from lib.six import PY2


def comicScan(comicid=None):
    lazylibrarian.COMIC_UPDATE = 1
    title = ''
    # noinspection PyBroadException
    try:
        myDB = database.DBConnection()
        if comicid:
            mags = myDB.match('select Title from comics WHERE ComicID=?', (comicid,))
            if mags:
                title = mags['Title']
        mag_path = lazylibrarian.CONFIG['COMIC_DEST_FOLDER']
        if title and '$Title' in mag_path:
            mag_path = mag_path.replace('$Title', title)
            onetitle = title
        else:
            onetitle = None

        while '$' in mag_path:
            mag_path = os.path.dirname(mag_path)

        if lazylibrarian.CONFIG['COMIC_RELATIVE']:
            mag_path = os.path.join(lazylibrarian.DIRECTORY('eBook'), mag_path)
        if PY2:
            mag_path = mag_path.encode(lazylibrarian.SYS_ENCODING)

        if lazylibrarian.CONFIG['FULL_SCAN'] and not onetitle:
            cmd = 'select Title,IssueID,IssueFile,comics.ComicID from comics,comicissues '
            cmd += 'WHERE comics.ComicID = comicissues.ComicID'
            mags = myDB.select(cmd)
            # check all the issues are still there, delete entry if not
            for mag in mags:
                title = mag['Title']
                issueid = mag['IssueID']
                comicid = mag['ComicID']
                issuefile = mag['IssueFile']

                if issuefile and not path_isfile(issuefile):
                    myDB.action('DELETE from comicissues where issuefile=?', (issuefile,))
                    logger.info('Issue %s - %s deleted as not found on disk' % (title, issueid))

                    controlValueDict = {"ComicID": comicid}
                    newValueDict = {
                        "LastAcquired": None,  # clear magazine dates
                        "LatestIssue": None,  # we will fill them in again later
                        "LatestCover": None,
                        "IssueStatus": "Skipped"  # assume there are no issues now
                    }
                    myDB.upsert("comics", newValueDict, controlValueDict)
                    logger.debug('Comic %s (%s) details reset' % (title, comicid))

            # now check the comic titles and delete any with no issues
            if lazylibrarian.CONFIG['COMIC_DELFOLDER']:
                cmd = 'select Title,ComicID,(select count(*) as counter from comicissues '
                cmd += 'where comics.comicid = comicissues.comicid) as issues from comics order by Title'
                mags = myDB.select(cmd)
                for mag in mags:
                    title = mag['Title']
                    comicid = mag['ComicID']
                    issues = mag['issues']
                    if not issues:
                        logger.debug('Comic %s deleted as no issues found' % title)
                        myDB.action('DELETE from comics WHERE ComicID=?', (comicid,))

        logger.info(' Checking [%s] for %s' % (mag_path, lazylibrarian.CONFIG['COMIC_TYPE']))

        for rootdir, _, filenames in walk(mag_path):
            for fname in filenames:
                if is_valid_booktype(fname, booktype='comic'):
                    title = ''
                    issue = ''
                    start = ''
                    first = ''
                    last = ''
                    publisher = ''
                    searchterm = ''
                    serieslink = ''
                    issuelink = ''
                    comicid = ''
                    seriesdescription = ''
                    issuedescription = ''
                    contributors = ''
                    aka = []
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
                        if comicid and comicid != res[3]['seriesid']:
                            aka = comicid
                        comicid = res[3]['seriesid']

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
                    if res:
                        controlValueDict = {"ComicID": comicid}

                        # is this comic already in the database?
                        mag_entry = myDB.match('SELECT * from comics WHERE ComicID=?', (comicid,))
                        if not mag_entry:
                            # need to add a new comic to the database
                            newValueDict = {
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
                                "aka": ', '.join(aka)
                            }
                            logger.debug("Adding comic %s (%s)" % (title, comicid))
                            myDB.upsert("comics", newValueDict, controlValueDict)
                            lastacquired = None
                            latestissue = issue
                            added = None
                        else:
                            lastacquired = mag_entry['LastAcquired']
                            latestissue = mag_entry['LatestIssue']
                            added = mag_entry['Added']

                        # is this issue already in the database?
                        iss_entry = myDB.match('SELECT IssueFile from comicissues WHERE ComicID=? and IssueID=?',
                                               (comicid, issue))
                        issuefile = os.path.join(rootdir, fname)  # full path to issue.cbr
                        mtime = os.path.getmtime(issuefile)
                        iss_acquired = datetime.date.isoformat(datetime.date.fromtimestamp(mtime))
                        myhash = uuid.uuid4().hex

                        if not iss_entry or (iss_entry['IssueFile'] != issuefile):
                            newValueDict = {
                                "IssueAcquired": iss_acquired,
                                "IssueFile": issuefile
                            }
                            if not iss_entry:
                                logger.debug("Adding issue %s %s" % (title, issue))
                                coverfile = createMagCover(issuefile, refresh=True)
                                if coverfile and path_isfile(coverfile):
                                    hashname = os.path.join(lazylibrarian.CACHEDIR, 'comic', '%s.jpg' % myhash)
                                    copyfile(coverfile, hashname)
                                    setperm(hashname)
                                    newValueDict['Cover'] = 'cache/comic/%s.jpg' % myhash
                                else:
                                    newValueDict['Cover'] = 'images/nocover.png'
                                newValueDict['Description'] = issuedescription
                                newValueDict['Link'] = issuelink
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
                                            newValueDict[item] = res[item]

                            controlValueDict = {"ComicID": comicid, "IssueID": issue}
                            myDB.upsert("comicissues", newValueDict, controlValueDict)
                            if not iss_entry:
                                dest_path, global_name = os.path.split(issuefile)
                                global_name = os.path.splitext(global_name)[0]
                                data = controlValueDict
                                data.update(newValueDict)
                                data['Title'] = title
                                data['Publisher'] = publisher
                                if not lazylibrarian.CONFIG['IMP_COMICOPF']:
                                    logger.debug('createComicOPF is disabled')
                                else:
                                    _ = createComicOPF(dest_path, data, global_name, overwrite=True)

                        ignorefile = os.path.join(os.path.dirname(issuefile), '.ll_ignore')
                        try:
                            with open(syspath(ignorefile), 'a') as f:
                                f.write('comic')
                        except IOError as e:
                            logger.warn("Unable to create/write to ignorefile: %s" % str(e))

                        # see if this issues date values are useful
                        controlValueDict = {"ComicID": comicid}
                        if not mag_entry:  # new magazine, this is the only issue
                            newValueDict = {
                                "Added": iss_acquired,
                                "LastAcquired": iss_acquired,
                                "LatestCover": 'cache/comic/%s.jpg' % myhash,
                                "LatestIssue": latestissue,
                                "IssueStatus": "Open"
                            }
                            myDB.upsert("comics", newValueDict, controlValueDict)
                        else:
                            # Set magazine_issuedate to issuedate of most recent issue we have
                            # Set latestcover to most recent issue cover
                            # Set magazine_added to acquired date of earliest issue we have
                            # Set magazine_lastacquired to acquired date of most recent issue we have
                            # acquired dates are read from magazine file timestamps
                            newValueDict = {"IssueStatus": "Open"}
                            if not added or iss_acquired < added:
                                newValueDict["Added"] = iss_acquired
                            if not lastacquired or iss_acquired > lastacquired:
                                newValueDict["LastAcquired"] = iss_acquired

                            if not latestissue or issue >= latestissue:
                                newValueDict["LatestIssue"] = issue
                                newValueDict["LatestCover"] = 'cache/comic/%s.jpg' % myhash
                            myDB.upsert("comics", newValueDict, controlValueDict)
                    else:
                        logger.debug("No match for %s" % fname)
        if lazylibrarian.CONFIG['FULL_SCAN'] and not onetitle:
            magcount = myDB.match("select count(*) from comics")
            isscount = myDB.match("select count(*) from comicissues")
            logger.info("Comic scan complete, found %s %s, %s %s" %
                        (magcount['count(*)'], plural(magcount['count(*)'], "comic"),
                         isscount['count(*)'], plural(isscount['count(*)'], "issue")))
        else:
            logger.info("Comic scan complete")
        lazylibrarian.COMIC_UPDATE = 0

    except Exception:
        lazylibrarian.COMIC_UPDATE = 0
        logger.error('Unhandled exception in comicScan: %s' % traceback.format_exc())
