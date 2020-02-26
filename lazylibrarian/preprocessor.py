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

from __future__ import print_function
from __future__ import with_statement

import os
import subprocess

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.common import listdir, path_exists
from lazylibrarian.formatter import makeBytestr, check_int, getList

try:
    from tinytag import TinyTag
except ImportError:
    try:
        from lib.tinytag import TinyTag
    except ImportError:
        TinyTag = None
try:
    # noinspection PyProtectedMember
    from PyPDF3 import PdfFileWriter, PdfFileReader
except ImportError:
    try:
        # noinspection PyProtectedMember
        from lib.PyPDF3 import PdfFileWriter, PdfFileReader
    except ImportError:
        PdfFileWriter = None
        PdfFileReader = None


def preprocess_ebook(bookfolder):
    ebook_convert = lazylibrarian.CONFIG['EBOOK_CONVERT']
    if not path_exists(ebook_convert):
        logger.error("%s not found" % ebook_convert)
        return

    logger.debug("Preprocess ebook %s" % bookfolder)
    sourcefile = None
    created = ''
    for fname in listdir(bookfolder):
        filename, extn = os.path.splitext(fname)
        if extn.lower() == '.epub':
            sourcefile = fname
            break
        elif extn.lower() in ['.mobi', '.azw3']:
            sourcefile = fname
            break

    logger.debug("Wanted formats: %s" % lazylibrarian.CONFIG['EBOOK_WANTED_FORMATS'])
    if not sourcefile:
        logger.error("No suitable sourcefile found in %s" % bookfolder)
        return

    basename, source_extn = os.path.splitext(sourcefile)
    wanted_formats = getList(lazylibrarian.CONFIG['EBOOK_WANTED_FORMATS'])
    for ftype in wanted_formats:
        if not path_exists(os.path.join(bookfolder, basename + '.' + ftype)):
            logger.debug("No %s" % ftype)
            params = [ebook_convert, os.path.join(bookfolder, sourcefile),
                      os.path.join(bookfolder, basename + '.' + ftype)]
            if ftype == 'mobi':
                params.extend(['--output-profile', 'kindle'])
            try:
                _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
                if created:
                    created += ' '
                created += ftype
            except Exception as e:
                logger.error("%s" % e)
                logger.error(repr(params))
                return
        else:
            logger.debug("Found %s" % ftype)

    if lazylibrarian.CONFIG['DELETE_OTHER_FORMATS']:
        if lazylibrarian.CONFIG['KEEP_OPF']:
            wanted_formats.append('opf')
        if lazylibrarian.CONFIG['KEEP_JPG']:
            wanted_formats.append('jpg')
        for fname in listdir(bookfolder):
            filename, extn = os.path.splitext(fname)
            if not extn or extn.lstrip('.').lower() not in wanted_formats:
                logger.debug("Deleting %s" % fname)
                try:
                    os.remove(os.path.join(bookfolder, fname))
                except OSError:
                    pass
    if created:
        logger.debug("Created %s from %s" % (created, source_extn))
    else:
        logger.debug("No extra ebook formats created")


def preprocess_audio(bookfolder, authorname, bookname):
    if not lazylibrarian.CONFIG['CREATE_SINGLEAUDIO'] and not lazylibrarian.CONFIG['WRITE_AUDIOTAGS']:
        return

    ffmpeg = lazylibrarian.CONFIG['FFMPEG']
    if not path_exists(ffmpeg):
        logger.error("%s not found" % ffmpeg)
        return

    if not TinyTag:
        logger.error("TinyTag not found")
        return

    logger.debug("Preprocess audio %s %s %s" % (bookfolder, authorname, bookname))
    # this produces a single file audiobook
    ffmpeg_params = ['-f', 'concat', '-safe', '0', '-i',
                     os.path.join(bookfolder, 'partslist.ll'), '-f', 'ffmetadata',
                     '-i', os.path.join(bookfolder, 'metadata.ll'), '-map_metadata', '1',
                     '-id3v2_version', '3']
    cnt = 0
    parts = []
    total = 0
    author = ''
    book = ''
    audio_file = ''
    out_type = ''
    for f in listdir(bookfolder):
        extn = os.path.splitext(f)[1].lstrip('.')
        if extn.lower() in getList(lazylibrarian.CONFIG['AUDIOBOOK_TYPE']):
            cnt += 1
            audio_file = f
            try:
                audio_path = os.path.join(bookfolder, f)
                performer = ''
                composer = ''
                albumartist = ''
                book = ''
                track = 0
                total = 0
                if TinyTag.is_supported(audio_path):
                    id3r = TinyTag.get(audio_path)
                    performer = id3r.artist
                    composer = id3r.composer
                    albumartist = id3r.albumartist
                    book = id3r.album
                    track = id3r.track
                    total = id3r.track_total

                    track = check_int(track, 0)
                    total = check_int(total, 0)

                    if performer:
                        performer = performer.strip()
                    if composer:
                        composer = composer.strip()
                    if book:
                        book = book.strip()
                    if albumartist:
                        albumartist = albumartist.strip()

                if composer:  # if present, should be author
                    author = composer
                elif performer:  # author, or narrator if composer == author
                    author = performer
                elif albumartist:
                    author = albumartist
                if author and book:
                    parts.append([track, book, author, f])
                if track == 1:
                    out_type = extn
            except Exception as e:
                logger.debug("tinytag %s %s" % (type(e).__name__, str(e)))
                pass

    logger.info("%s found %s audiofiles" % (book, cnt))

    if cnt == 1 and not parts:  # single file audiobook with no tags
        parts = [[1, book, author, audio_file]]

    if cnt != len(parts):
        logger.error("%s: Incorrect number of parts (found %i from %i)" % (book, len(parts), cnt))
        return

    if total and total != cnt:
        logger.error("%s: Reported %i parts, got %i" % (book, total, cnt))
        return

    if cnt == 1:
        logger.info("Only one audio file found, nothing to merge")
        return

    # check all parts have the same author and title
    if len(parts) > 1:
        for part in parts:
            if part[1] != book:
                logger.error("%s: Inconsistent title: [%s][%s]" % (book, part[1], book))
                return
            if part[2] != author:
                logger.error("%s: Inconsistent author: [%s][%s]" % (book, part[2], author))
                return

    # do we have any track info (value is 0 if not)
    tokmatch = ''
    if parts[0][0] == 0:
        # try to extract part information from filename. Search for token style of part 1 in this order...
        for token in [' 001.', ' 01.', ' 1.', ' 001 ', ' 01 ', ' 1 ', '01']:
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
                else:
                    pattern = '%s' % str(cnt).zfill(2)
                # standardise numbering of the parts
                for part in parts:
                    if pattern in part[3]:
                        part[0] = cnt
                        break

    parts.sort(key=lambda x: x[0])
    # check all parts are present
    cnt = 0
    while cnt < len(parts):
        if parts[cnt][0] != cnt + 1:
            logger.error("%s: No part %i found" % (book, cnt + 1))
            return
        cnt += 1

    # if we get here, looks like we have all the parts
    with open(os.path.join(bookfolder, 'partslist.ll'), 'wb') as f:
        for part in parts:
            f.write("file '%s'" % makeBytestr(part[3]))
            if lazylibrarian.CONFIG['WRITE_AUDIOTAGS'] and authorname and bookname:
                if tokmatch or (part[2] != authorname) or (part[1] != bookname):
                    extn = os.path.splitext(part[3])[1]
                    params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                              '-y', '-c:a', 'copy', '-metadata', "album=%s" % bookname,
                              '-metadata', "artist=%s" % authorname,
                              '-metadata', "track=%s" % part[0],
                              os.path.join(bookfolder, "tempaudio%s" % extn)]
                    try:
                        _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
                        os.remove(os.path.join(bookfolder, part[3]))
                        os.rename(os.path.join(bookfolder, "tempaudio%s" % extn),
                                  os.path.join(bookfolder, part[3]))
                        logger.debug("Metadata written to %s" % part[3])
                    except Exception as e:
                        logger.error(str(e))
                        return

    if lazylibrarian.CONFIG['CREATE_SINGLEAUDIO']:
        params = [ffmpeg, '-i', os.path.join(bookfolder, parts[0][3]),
                  '-f', 'ffmetadata', '-y', os.path.join(bookfolder, 'metadata.ll')]
        try:
            _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
            logger.debug("Metadata written to metadata.ll")
        except Exception as e:
            logger.error(str(e))
            return

        params = [ffmpeg]
        params.extend(ffmpeg_params)
        params.extend(getList(lazylibrarian.CONFIG['AUDIO_OPTIONS']))
        params.append('-y')
        if not out_type:
            out_type = 'mp3'
        outfile = "%s - %s.%s" % (author, book, out_type)
        params.append(os.path.join(bookfolder, outfile))

        try:
            logger.debug("Processing %d files" % len(parts))
            _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
        except Exception as e:
            logger.error(str(e))
            return

        logger.info("%d files merged into %s" % (len(parts), outfile))
        os.remove(os.path.join(bookfolder, 'partslist.ll'))
        os.remove(os.path.join(bookfolder, 'metadata.ll'))
        if not lazylibrarian.CONFIG['KEEP_SEPARATE_AUDIO']:
            logger.debug("Removing %d part files" % len(parts))
            for part in parts:
                os.remove(os.path.join(bookfolder, part[3]))


def preprocess_magazine(bookfolder, cover=0):
    logger.debug("Preprocess magazine %s cover=%s" % (bookfolder, cover))
    if not lazylibrarian.CONFIG['SWAP_COVERPAGE']:
        return

    if cover < 2:
        return

    if not PdfFileWriter:
        logger.error("PdfFileWriter not found")
        return

    try:
        sourcefile = None
        for fname in listdir(bookfolder):
            filename, extn = os.path.splitext(fname)
            if extn.lower() == '.pdf':
                sourcefile = fname
                break

        if not sourcefile:
            logger.error("No suitable sourcefile found in %s" % bookfolder)
            return

        cover -= 1  # zero based page count
        fname = os.path.join(bookfolder, sourcefile)
        output = PdfFileWriter()
        f = open(fname, "rb")
        input1 = PdfFileReader(f)
        cnt = input1.getNumPages()
        output.addPage(input1.getPage(cover))
        p = 0
        while p < cnt:
            if p != cover:
                output.addPage(input1.getPage(p))
            p = p + 1
        with open(fname + 'new', "wb") as outputStream:
            output.write(outputStream)
        logger.debug("%s has %d pages. Cover from page %d" % (fname, cnt, cover + 1))
        f.close()
        os.remove(fname)
        os.rename(fname + 'new', fname)
    except Exception as e:
        logger.error(str(e))
