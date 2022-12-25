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


import os
import re
import traceback

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.common import safe_move, multibook, only_punctuation, opf_file
from lazylibrarian.filesystem import path_isdir, syspath, remove_file, listdir
from lazylibrarian.formatter import plural, is_valid_booktype, check_int, get_list, \
    make_unicode, sort_definite, surname_first, sanitize
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.opfedit import opf_read

try:
    from lib.tinytag import TinyTag
except ImportError:
    TinyTag = None


def id3read(filename):
    mydict = {}
    if not TinyTag:
        logger.warn("TinyTag library not available")
        return mydict

    filename = syspath(filename)
    # noinspection PyBroadException
    try:
        res = TinyTag.is_supported(filename)
    except Exception:
        res = False
    if not res:
        logger.warn("TinyTag:unsupported [%s]" % filename)
        return mydict

    # noinspection PyBroadException
    try:
        id3r = TinyTag.get(filename)
        artist = id3r.artist
        composer = id3r.composer
        album = id3r.album
        albumartist = id3r.albumartist
        title = id3r.title
        track = id3r.track
        track_total = id3r.track_total
        comment = id3r.comment

        if artist:
            artist = artist.strip().rstrip('\x00')
        else:
            artist = ''
        if composer:
            composer = composer.strip().rstrip('\x00')
        else:
            composer = ''
        if album:
            album = album.strip().rstrip('\x00')
        else:
            album = ''
        if albumartist:
            albumartist = albumartist.strip().rstrip('\x00')
        else:
            albumartist = ''

        if lazylibrarian_log.LOGLEVEL & logger.log_libsync:
            for tag in ['filename', 'artist', 'albumartist', 'composer', 'album', 'title', 'track',
                        'track_total', 'comment']:
                logger.debug("id3r.%s [%s]" % (tag, eval(tag)))

        if artist == 'None':
            artist = ''
        if albumartist == 'None':
            albumartist = ''
        if composer == 'None':
            composer = ''

        db = database.DBConnection()

        # Commonly used tags, eg plex:
        # ARTIST  Author or Author, Narrator
        # ALBUMARTIST     Author
        # COMPOSER    Narrator
        author = ''
        narrator = ''
        if albumartist:
            author = albumartist
            if composer:
                narrator = composer
        elif len(artist.split(',')) == 2:
            author, narrator = artist.split(',')
        elif artist and composer and artist != composer:
            author = artist
            narrator = composer

        # finally override with any opf values found
        opffile = opf_file(os.path.dirname(filename))
        if os.path.exists(opffile):
            opf_template, replaces = opf_read(opffile)
            remove_file(opf_template)
            for item in replaces:
                if item[0] == 'author':
                    author = item[1]
                elif item[0] == 'narrator':
                    narrator = item[1]

        if not author:
            # if artist exists in library, probably author, though might get one author narrating anothers books?
            if artist and db.match("select * from authors where authorname=?", (artist,)):
                author = artist
            elif albumartist and db.match("select * from authors where authorname=?", (albumartist,)):
                author = albumartist
            elif composer and db.match("select * from authors where authorname=?", (composer,)):
                author = composer
            elif artist:
                author = artist
            elif albumartist:
                author = albumartist
            elif composer:
                author = composer

        if author and type(author) is list:
            lst = ', '.join(author)
            logger.debug("id3reader author list [%s]" % lst)
            author = author[0]  # if multiple authors, just use the first one

        mydict['artist'] = artist
        mydict['composer'] = composer
        mydict['album'] = album
        mydict['albumartist'] = albumartist
        mydict['title'] = title
        mydict['track'] = track
        mydict['track_total'] = track_total
        mydict['comment'] = comment

        mydict['author'] = author
        mydict['narrator'] = narrator
        if not title:
            mydict['title'] = album

        for item in mydict:
            mydict[item] = make_unicode(mydict[item])

    except Exception:
        logger.error("tinytag error %s" % traceback.format_exc())
    return mydict


def audio_parts(folder, bookname, authorname):
    parts = []
    cnt = 0
    audio_file = ''
    abridged = ''
    tokmatch = ''
    total = 0
    wholebook = ''
    for f in listdir(folder):
        if is_valid_booktype(f, booktype='audiobook'):
            # if no number_period or number_space in filename assume its whole-book
            if not re.findall(r'\d+\b', f):
                wholebook = f
            else:
                cnt += 1
                audio_file = f
                try:
                    audio_path = os.path.join(folder, f)
                    id3r = id3read(audio_path)
                    artist = id3r['artist']
                    composer = id3r['composer']
                    albumartist = id3r['albumartist']
                    book = id3r['album']
                    title = id3r['title']
                    comment = id3r['comment']
                    track = id3r['track']
                    total = id3r['track_total']

                    track = check_int(track, 0)
                    total = check_int(total, 0)

                    author = id3r['author']
                    if not book:
                        book = id3r['title']

                    if author and book:
                        parts.append([track, book, author, f])
                    if not abridged:
                        # unabridged is sometimes shortened to unabr.
                        for tag in [book, title, albumartist, artist, composer, comment]:
                            if tag and 'unabr' in tag.lower():
                                abridged = 'Unabridged'
                                break
                    if not abridged:
                        for tag in [book, title, albumartist, artist, composer, comment]:
                            if tag and 'abridged' in tag.lower():
                                abridged = 'Abridged'
                                break

                except Exception as e:
                    logger.error("id3tag %s %s" % (type(e).__name__, str(e)))
                    pass
                finally:
                    if not abridged:
                        if audio_file and 'unabr' in audio_file.lower():
                            abridged = 'Unabridged'
                            break
                    if not abridged:
                        if audio_file and 'abridged' in audio_file.lower():
                            abridged = 'Abridged'
                            break

    if cnt == 1 and not parts:  # single file audiobook with number but no tags
        parts = [[1, bookname, authorname, audio_file]]

    logger.debug("Audiobook found %s %s" % (cnt, plural(cnt, "part")))

    if cnt == 0 and wholebook:  # only single file audiobook, no part files
        cnt = 1
        parts = [[1, bookname, authorname, wholebook]]

    failed = False
    try:
        if cnt != len(parts):
            logger.warn("%s: Incorrect number of parts (found %i from %i)" % (bookname, len(parts), cnt))
            failed = True

        if total and total != cnt:
            logger.warn("%s: Reported %i parts, got %i" % (bookname, total, cnt))
            failed = True

        # check all parts have the same author and title
        if len(parts) > 1:
            book = parts[0][1]
            author = parts[0][2]
            for part in parts:
                if part[1] != book:
                    logger.warn("%s: Inconsistent title: [%s][%s]" % (bookname, part[1], book))
                    failed = True

                if part[2] != author:
                    logger.warn("%s: Inconsistent author: [%s][%s]" % (bookname, part[2], author))
                    failed = True

        # do we have any track info from id3 tags
        if failed or parts[0][0] == 0:
            if failed:
                logger.debug("No usable track info from id3")
            else:
                logger.debug("No track info from id3")

            if len(parts) == 1:
                return parts, failed, '', abridged

            failed = False
            # try to extract part information from filename. Search for token style of part 1 in this order...
            for token in [' 001.', ' 01.', ' 1.', ' 001 ', ' 01 ', ' 1 ', '001', '01']:
                if tokmatch:
                    break
                for part in parts:
                    if token in part[3]:
                        tokmatch = token
                        break
            if tokmatch:  # we know the numbering style, get numbers for the other parts
                cnt = 0
                while cnt < len(parts):
                    cnt += 1
                    if tokmatch == ' 001.':
                        pattern = ' %s.' % str(cnt).zfill(3)
                    elif tokmatch == ' 01.':
                        pattern = ' %s.' % str(cnt).zfill(2)
                    elif tokmatch == ' 1.':
                        pattern = ' %s.' % str(cnt)
                    elif tokmatch == ' 001 ':
                        pattern = ' %s ' % str(cnt).zfill(3)
                    elif tokmatch == ' 01 ':
                        pattern = ' %s ' % str(cnt).zfill(2)
                    elif tokmatch == ' 1 ':
                        pattern = ' %s ' % str(cnt)
                    elif tokmatch == '001':
                        pattern = '%s' % str(cnt).zfill(3)
                    else:
                        pattern = '%s' % str(cnt).zfill(2)
                    # standardise numbering of the parts
                    for part in parts:
                        if pattern in part[3]:
                            part[0] = cnt
                            break
    except Exception as e:
        logger.error(str(e))

    logger.debug("Checking numbering of %s %s" % (len(parts), plural(len(parts), 'part')))
    parts.sort(key=lambda x: x[0])
    # check all parts are present
    cnt = 0
    while cnt < len(parts):
        if cnt and parts[cnt][0] == cnt:
            logger.error("%s: Duplicate part %i found" % (bookname, cnt))
            failed = True
            break
        if parts[cnt][0] != cnt + 1:
            logger.warn('%s: No part %i found, "%s" for token "%s" %s' % (bookname, cnt + 1, parts[cnt][0],
                                                                          tokmatch, parts[cnt][3]))
            failed = True
            break
        cnt += 1
    return parts, failed, tokmatch, abridged


def audio_rename(bookid, rename=False, playlist=False):
    """
    :param bookid: book to process
    :param rename: rename to match audiobook filename pattern
    :param playlist: generate a playlist for popup
    :return: filename of part 01 of the audiobook
    """
    if rename:
        if '$Part' not in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FILE']:
            logger.error("Unable to rename, no $Part in AUDIOBOOK_DEST_FILE")
            return
        if '$Title' not in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FILE'] and \
                '$SortTitle' not in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FILE']:
            logger.error("Unable to rename, no $Title or $SortTitle in AUDIOBOOK_DEST_FILE")
            return ''

    db = database.DBConnection()
    cmd = 'select AuthorName,BookName,AudioFile from books,authors where books.AuthorID = authors.AuthorID and bookid=?'
    exists = db.match(cmd, (bookid,))
    if exists:
        book_filename = exists['AudioFile']
        if book_filename:
            r = os.path.dirname(book_filename)
        else:
            logger.debug("No filename for %s in audio_rename" % bookid)
            return ''
    else:
        logger.debug("Invalid bookid in audio_rename %s" % bookid)
        return ''

    if not TinyTag:
        logger.warn("TinyTag library not available")
        return ''

    parts, failed, _, abridged = audio_parts(r, exists['BookName'], exists['AuthorName'])

    if failed or not parts:
        return exists['AudioFile']

    if abridged:
        abridged = ' (%s)' % abridged
    # if we get here, looks like we have all the parts needed to rename properly
    seriesinfo = name_vars(bookid, abridged)
    logger.debug(str(seriesinfo))
    dest_path = seriesinfo['AudioFolderName']
    dest_dir = lazylibrarian.directory('Audio')
    dest_path = os.path.join(dest_dir, dest_path)
    # check for windows case-insensitive
    if os.name == 'nt' and r.lower() == dest_path.lower():
        dest_path = r
    if rename and r != dest_path:
        try:
            logger.debug("Moving folder [%s] to [%s]" % (repr(r), repr(dest_path)))
            dest_path = safe_move(r, dest_path)
            r = dest_path
            book_filename = os.path.join(r, os.path.basename(book_filename))
        except Exception as why:
            if not path_isdir(dest_path):
                logger.error('Unable to create directory %s: %s' % (dest_path, why))

    if playlist:
        try:
            playlist = open(os.path.join(r, 'playlist.ll'), "w")
        except Exception as why:
            logger.error('Unable to create playlist in %s: %s' % (r, why))
            playlist = None

    if len(parts) == 1:
        part = parts[0]
        namevars = name_vars(bookid, abridged)
        bookfile = namevars['AudioSingleFile']
        if not bookfile:
            bookfile = "%s - %s" % (exists['AuthorName'], exists['BookName'])
        out_type = os.path.splitext(part[3])[1]
        outfile = bookfile + out_type
        if playlist:
            if rename:
                playlist.write("%s\n" % make_unicode(outfile))
            else:
                playlist.write("%s\n" % make_unicode(part[3]))
        if rename:
            n = os.path.join(make_unicode(r), make_unicode(outfile))
            o = os.path.join(make_unicode(r), make_unicode(part[3]))
            # check for windows case-insensitive
            if os.name == 'nt' and n.lower() == o.lower():
                n = o
            if o != n:
                try:
                    n = safe_move(o, n)
                    book_filename = n  # return part 1 of set
                    logger.debug('%s: audio_rename [%s] to [%s]' % (exists['BookName'], o, n))
                except Exception as e:
                    logger.error('Unable to rename [%s] to [%s] %s %s' % (o, n, type(e).__name__, str(e)))
    else:
        for part in parts:
            pattern = seriesinfo['AudioFile']
            pattern = pattern.replace(
                '$Part', str(part[0]).zfill(len(str(len(parts))))).replace(
                '$Total', str(len(parts)))
            pattern = ' '.join(pattern.split()).strip()
            pattern = pattern + os.path.splitext(part[3])[1]
            if rename:
                pattern = sanitize(pattern)

            if playlist:
                if rename:
                    playlist.write("%s\n" % make_unicode(pattern))
                else:
                    playlist.write("%s\n" % make_unicode(part[3]))
            if rename:
                n = os.path.join(make_unicode(r), make_unicode(pattern))
                o = os.path.join(make_unicode(r), make_unicode(part[3]))
                # check for windows case-insensitive
                if os.name == 'nt' and n.lower() == o.lower():
                    n = o
                if o != n:
                    try:
                        n = safe_move(o, n)
                        if part[0] == 1:
                            book_filename = n  # return part 1 of set
                        logger.debug('%s: audio_rename [%s] to [%s]' % (exists['BookName'], o, n))
                    except Exception as e:
                        logger.error('Unable to rename [%s] to [%s] %s %s' % (o, n, type(e).__name__, str(e)))
    if playlist:
        playlist.close()
    return book_filename


def stripspaces(pathname):
    # windows doesn't allow directory names to end in a space or a period
    # but allows starting with a period (not sure about starting with a space, but it looks messy anyway)
    parts = pathname.split(os.sep)
    new_parts = []
    for part in parts:
        while part and part[-1] in ' .':
            part = part[:-1]
        part = part.lstrip(' ')
        new_parts.append(part)
    pathname = os.sep.join(new_parts)
    return pathname


def book_rename(bookid):
    db = database.DBConnection()
    cmd = 'select AuthorName,BookName,BookFile from books,authors where books.AuthorID = authors.AuthorID and bookid=?'
    exists = db.match(cmd, (bookid,))

    if not exists:
        msg = "Invalid bookid in book_rename %s" % bookid
        logger.debug(msg)
        return '', msg

    f = exists['BookFile']
    if not f:
        msg = "No filename for %s in BookRename" % bookid
        logger.debug(msg)
        return '', msg

    if not os.path.isfile(f):
        msg = "Missing source file for %s in BookRename" % bookid
        logger.debug(msg)
        return '', msg

    r = os.path.dirname(f)
    if not lazylibrarian.CONFIG.get_bool('CALIBRE_RENAME'):
        try:
            # noinspection PyTypeChecker
            calibreid = r.rsplit('(', 1)[1].split(')')[0]
            if not calibreid.isdigit():
                calibreid = ''
        except IndexError:
            calibreid = ''

        if calibreid:
            msg = '[%s] looks like a calibre directory: not renaming book' % os.path.basename(r)
            logger.debug(msg)
            return f, msg

    reject = multibook(r)
    if reject:
        msg = "Not renaming %s, found multiple %s" % (f, reject)
        logger.debug(msg)
        return f, msg

    namevars = name_vars(bookid)
    dest_path = namevars['FolderName']
    dest_dir = lazylibrarian.directory('eBook')
    dest_path = os.path.join(dest_dir, dest_path)
    dest_path = stripspaces(dest_path)
    oldpath = r

    new_basename = namevars['BookFile']
    if ' / ' in new_basename:  # used as a separator in goodreads omnibus
        msg = "book_rename [%s] looks like an omnibus? Not renaming" % new_basename
        logger.warn(msg)
        return f, msg

    if oldpath != dest_path:
        try:
            dest_path = safe_move(oldpath, dest_path)
            logger.debug("book_rename folder %s to %s" % (oldpath, dest_path))
        except Exception as why:
            if not path_isdir(dest_path):
                msg = 'Unable to create directory %s: %s' % (dest_path, why)
                logger.error(msg)
                return f, msg

    book_basename, _ = os.path.splitext(os.path.basename(f))

    if book_basename == new_basename:
        return f, "No change"
    else:
        msg = ''
        # only rename bookname.type, bookname.jpg, bookname.opf, not cover.jpg or metadata.opf
        for fname in listdir(dest_path):
            extn = ''
            if is_valid_booktype(fname, booktype='ebook'):
                extn = os.path.splitext(fname)[1]
            elif fname.endswith('.opf') and not fname == 'metadata.opf':
                extn = '.opf'
            elif fname.endswith('.jpg') and not fname == 'cover.jpg':
                extn = '.jpg'
            if extn:
                ofname = os.path.join(dest_path, fname)
                nfname = os.path.join(dest_path, new_basename + extn)
                # check for windows case-insensitive
                if os.name == 'nt' and nfname.lower() == ofname.lower():
                    nfname = ofname
                if ofname != nfname:
                    try:
                        nfname = safe_move(ofname, nfname)
                        m = "book_rename file %s to %s " % (ofname, nfname)
                        logger.debug(m)
                        msg += m
                        oldname = os.path.join(oldpath, fname)
                        if oldname == exists['BookFile']:  # if we renamed/moved the preferred file, return new name
                            f = nfname
                    except Exception as e:
                        m = 'Unable to rename [%s] to [%s] %s %s ' % (ofname, nfname, type(e).__name__, str(e))
                        logger.error(m)
                        msg += m
        return f, msg


def name_vars(bookid, abridged=''):
    """ Return name variables for a bookid as a dict of formatted strings
        The strings are configurable, but by default...
        Series returns ( Lord of the Rings 2 )
        FmtName returns Lord of the Rings (with added Num part if that's not numeric, eg Lord of the Rings Book One)
        FmtNum  returns Book #1 -    (or empty string if no numeric part)
        so you can combine to make Book #1 - Lord of the Rings
        PadNum is zero padded numeric part or empty string
        SerName and SerNum are the unformatted base strings
        PubYear is the publication year of the book or empty string
        SerYear is the publication year of the first book in the series or empty string
        """
    mydict = {}
    seriesnum = ''
    seriesname = ''

    db = database.DBConnection()

    if bookid == 'test':
        seriesid = '66175'
        serieslist = ['3']
        pubyear = '1955'
        seryear = '1954'
        seriesname = 'The Lord of the Rings'
        mydict['Author'] = 'J.R.R. Tolkien'
        mydict['Title'] = 'The Fellowship of the Ring'
        mydict['SortAuthor'] = surname_first(mydict['Author'])
        mydict['SortTitle'] = sort_definite(mydict['Title'])
        mydict['Part'] = '1'
        mydict['Total'] = '3'
        res = {}
    else:
        cmd = 'SELECT SeriesID,SeriesNum from member,books WHERE books.bookid = member.bookid and books.bookid=?'
        res = db.match(cmd, (bookid,))
        if res:
            seriesid = res['SeriesID']
            serieslist = get_list(res['SeriesNum'])

            cmd = 'SELECT BookDate from member,books WHERE books.bookid = member.bookid and SeriesNum=1 and SeriesID=?'
            res_date = db.match(cmd, (seriesid,))
            if res_date:
                seryear = res_date['BookDate']
                if not seryear or seryear == '0000':
                    seryear = ''
                seryear = seryear[:4]
            else:
                seryear = ''
        else:
            seriesid = ''
            serieslist = []
            seryear = ''

        cmd = 'SELECT BookDate from books WHERE bookid=?'
        res_date = db.match(cmd, (bookid,))
        if res_date:
            pubyear = res_date['BookDate']
            if not pubyear or pubyear == '0000':
                pubyear = ''
            pubyear = pubyear[:4]  # googlebooks sometimes has month or full date
        else:
            pubyear = ''

    # might be "Book 3.5" or similar, just get the numeric part
    while serieslist:
        seriesnum = serieslist.pop()
        seriesnum = seriesnum.lstrip('#')
        try:
            _ = float(seriesnum)
            break
        except ValueError:
            seriesnum = ''
            pass

    padnum = ''
    if res and seriesnum == '':
        # couldn't figure out number, keep everything we got, could be something like "Book Two"
        serieslist = res['SeriesNum']
    elif seriesnum.isdigit():
        padnum = str(int(seriesnum)).zfill(2)
    else:
        try:
            padnum = str(float(seriesnum))
            if padnum[1] == '.':
                padnum = '0' + padnum
        except (ValueError, IndexError):
            padnum = ''

    if seriesid and bookid != 'test':
        cmd = 'SELECT SeriesName from series WHERE seriesid=?'
        res = db.match(cmd, (seriesid,))
        if res:
            seriesname = res['SeriesName']
            if seriesnum == '':
                # add what we got back to end of series name
                if seriesname and serieslist:
                    seriesname = "%s %s" % (seriesname, serieslist)

    seriesname = ' '.join(seriesname.split())  # strip extra spaces
    if only_punctuation(seriesname):  # but don't return just whitespace or punctuation
        seriesname = ''

    if seriesname:
        fmtname = lazylibrarian.CONFIG['FMT_SERNAME'].replace('$SerName', seriesname).replace(
                                                              '$PubYear', pubyear).replace(
                                                              '$SerYear', seryear).replace(
                                                              '$$', ' ')
    else:
        fmtname = ''

    if only_punctuation(fmtname):
        fmtname = ''

    if seriesnum != '':  # allow 0
        fmtnum = lazylibrarian.CONFIG['FMT_SERNUM'].replace('$SerNum', seriesnum).replace(
                                                            '$PubYear', pubyear).replace(
                                                            '$SerYear', seryear).replace(
                                                            '$PadNum', padnum).replace('$$', ' ')
    else:
        fmtnum = ''

    if only_punctuation(fmtnum):
        fmtnum = ''

    if fmtnum != '' or fmtname:
        fmtseries = lazylibrarian.CONFIG['FMT_SERIES'].replace('$SerNum', seriesnum).replace(
                                                             '$SerName', seriesname).replace(
                                                             '$PadNum', padnum).replace(
                                                             '$PubYear', pubyear).replace(
                                                             '$SerYear', seryear).replace(
                                                             '$FmtName', fmtname).replace(
                                                             '$FmtNum', fmtnum).replace('$$', ' ')
    else:
        fmtseries = ''

    if only_punctuation(fmtseries):
        fmtseries = ''

    mydict['FmtName'] = fmtname
    mydict['FmtNum'] = fmtnum
    mydict['Series'] = fmtseries
    mydict['PadNum'] = padnum
    mydict['SerName'] = seriesname
    mydict['SerNum'] = seriesnum
    mydict['PubYear'] = pubyear
    mydict['SerYear'] = seryear
    mydict['Abridged'] = abridged

    if bookid != 'test':
        cmd = 'select AuthorName,BookName from books,authors where books.AuthorID = authors.AuthorID and bookid=?'
        exists = db.match(cmd, (bookid,))
        if exists:
            mydict['Author'] = exists['AuthorName']
            mydict['Title'] = exists['BookName']
            mydict['SortAuthor'] = surname_first(mydict['Author'])
            mydict['SortTitle'] = sort_definite(mydict['Title'])
        else:
            mydict['Author'] = ''
            mydict['Title'] = ''
            mydict['SortAuthor'] = ''
            mydict['SortTitle'] = ''

    mydict['FolderName'] = stripspaces(sanitize(replacevars(lazylibrarian.CONFIG['EBOOK_DEST_FOLDER'],
                                                            mydict)))
    mydict['AudioFolderName'] = stripspaces(sanitize(replacevars(lazylibrarian.CONFIG['AUDIOBOOK_DEST_FOLDER'],
                                                                 mydict)))
    mydict['BookFile'] = stripspaces(sanitize(replacevars(lazylibrarian.CONFIG['EBOOK_DEST_FILE'],
                                                          mydict)))
    mydict['AudioFile'] = stripspaces(sanitize(replacevars(lazylibrarian.CONFIG['AUDIOBOOK_DEST_FILE'],
                                                           mydict))).replace('sPart',
                                                                             '$Part').replace('sTotal',
                                                                                              '$Total')
    mydict['AudioSingleFile'] = stripspaces(sanitize(replacevars(lazylibrarian.CONFIG['AUDIOBOOK_SINGLE_FILE'],
                                                                 mydict))).replace('sPart',
                                                                                   '$Part').replace('sTotal',
                                                                                                    '$Total')
    return mydict


def replacevars(base, mydict):
    for item in ['$Author', '$SortAuthor', '$Title', '$SortTitle', '$Series', '$FmtName', '$FmtNum',
                 '$SerName', '$SerNum', '$PadNum', '$PubYear', '$SerYear', '$Part', '$Total',
                 '$Abridged']:
        if item[1:] in mydict:
            base = base.replace(item, mydict[item[1:]].replace(os.sep, '_'))
    return base.replace('$$', ' ')
