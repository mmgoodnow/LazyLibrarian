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
from lazylibrarian.bookrename import audio_parts
from lazylibrarian.common import listdir, path_exists
from lazylibrarian.formatter import getList, makeUnicode

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
    if not ebook_convert:
        logger.error("Check config setting for ebook-convert")
        return

    try:
        params = [ebook_convert, "--version"]
        res = subprocess.check_output(params, stderr=subprocess.STDOUT)
        res = makeUnicode(res).strip().split("(")[1].split(")")[0]
        logger.debug("Found ebook-convert version %s" % res)
        convert_ver = res
    except Exception as e:
        logger.debug("ebook-convert --version failed: %s %s" % (type(e).__name__, str(e)))
        convert_ver = ''

    logger.debug("Preprocess ebook %s" % bookfolder)
    sourcefile = None
    created = ''
    for fname in listdir(bookfolder):
        filename, extn = os.path.splitext(fname)
        if extn.lower() == '.epub':
            sourcefile = fname
            break
    if not sourcefile:
        for fname in listdir(bookfolder):
            filename, extn = os.path.splitext(fname)
            if extn.lower() in ['.mobi', '.azw3']:
                sourcefile = fname
                break

    if not sourcefile:
        logger.error("No suitable sourcefile found in %s" % bookfolder)
        return

    basename, source_extn = os.path.splitext(sourcefile)
    logger.debug("Wanted formats: %s" % lazylibrarian.CONFIG['EBOOK_WANTED_FORMATS'])
    wanted_formats = getList(lazylibrarian.CONFIG['EBOOK_WANTED_FORMATS'])
    for ftype in wanted_formats:
        if not path_exists(os.path.join(bookfolder, basename + '.' + ftype)):
            logger.debug("No %s" % ftype)
            params = [ebook_convert, os.path.join(bookfolder, sourcefile),
                      os.path.join(bookfolder, basename + '.' + ftype)]
            if ftype == 'mobi':
                params.extend(['--output-profile', 'kindle'])
            if convert_ver:
                try:
                    _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10), stderr=subprocess.STDOUT)
                    if created:
                        created += ' '
                    created += ftype
                except Exception as e:
                    logger.error("%s" % e)
                    logger.error(repr(params))
                    return
            else:
                logger.warn("Unable to create %s" % ftype)
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
    if not ffmpeg:
        logger.error("Check config setting for ffmpeg")
        return
    try:
        params = [ffmpeg, "-version"]
        res = subprocess.check_output(params, stderr=subprocess.STDOUT)
        res = makeUnicode(res).strip().split("Copyright")[0].split()[-1]
        logger.debug("Found ffmpeg version %s" % res)
        ff_ver = res
    except Exception as e:
        logger.debug("ffmpeg -version failed: %s %s" % (type(e).__name__, str(e)))
        ff_ver = ''

    logger.debug("Preprocess audio %s %s %s" % (bookfolder, authorname, bookname))
    partslist = os.path.join(bookfolder, "partslist.ll")
    metadata = os.path.join(bookfolder, "metadata.ll")

    # this is to work around an ffmpeg oddity...
    if os.path.__name__ == 'ntpath':
        partslist = partslist.replace("\\", "/")
        metadata = metadata.replace("\\", "/")

    # this produces a single file audiobook
    ffmpeg_params = ['-f', 'concat', '-safe', '0', '-i', partslist, '-f', 'ffmetadata',
                     '-i', metadata, '-map_metadata', '1', '-id3v2_version', '3']

    parts, failed, token, _ = audio_parts(bookfolder, bookname, authorname)

    if failed:
        return
    if len(parts) == 1:
        logger.info("Only one audio file found, nothing to merge")
        return

    # if we get here, looks like we have all the parts
    out_type = os.path.splitext(parts[0][3])[1]
    with open(os.path.join(bookfolder, "partslist.ll"), 'w') as f:
        for part in parts:
            f.write("file '%s'\n" % part[3])
            if ff_ver and lazylibrarian.CONFIG['WRITE_AUDIOTAGS'] and authorname and bookname:
                if token or (part[2] != authorname) or (part[1] != bookname):
                    extn = os.path.splitext(part[3])[1]
                    params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                              '-y', '-c:a', 'copy', '-metadata', "album=%s" % bookname,
                              '-metadata', "artist=%s" % authorname,
                              '-metadata', "track=%s" % part[0],
                              os.path.join(bookfolder, "tempaudio%s" % extn)]
                    try:
                        _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                    stderr=subprocess.STDOUT)
                        os.remove(os.path.join(bookfolder, part[3]))
                        os.rename(os.path.join(bookfolder, "tempaudio%s" % extn),
                                  os.path.join(bookfolder, part[3]))
                        logger.debug("Metadata written to %s" % part[3])
                    except Exception as e:
                        logger.error(str(e))
                        return

    if ff_ver and lazylibrarian.CONFIG['CREATE_SINGLEAUDIO']:
        params = [ffmpeg, '-i', os.path.join(bookfolder, parts[0][3]),
                  '-f', 'ffmetadata', '-y', os.path.join(bookfolder, "metadata.ll")]
        try:
            _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10), stderr=subprocess.STDOUT)
            logger.debug("Metadata written to metadata.ll")
        except Exception as e:
            logger.error(str(e))
            return

        params = [ffmpeg]
        params.extend(ffmpeg_params)
        params.extend(getList(lazylibrarian.CONFIG['AUDIO_OPTIONS']))
        params.append('-y')

        outfile = "%s - %s%s" % (authorname, bookname, out_type)
        params.append(os.path.join(bookfolder, outfile))

        res = ''
        try:
            logger.debug("Merging %d files" % len(parts))
            res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10), stderr=subprocess.STDOUT)
        except Exception as e:
            logger.error(str(e))
            if res:
                logger.error(res)
            return

        logger.info("%d files merged into %s" % (len(parts), outfile))
        os.remove(os.path.join(bookfolder, "partslist.ll"))
        os.remove(os.path.join(bookfolder, "metadata.ll"))
        if not lazylibrarian.CONFIG['KEEP_SEPARATEAUDIO']:
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
