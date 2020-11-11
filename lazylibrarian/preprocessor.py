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
from lazylibrarian import logger, database
from lazylibrarian.bookrename import audio_parts
from lazylibrarian.common import listdir, path_exists, safe_copy, safe_move, remove, calibre_prg
from lazylibrarian.formatter import getList, makeUnicode, check_int, human_size
from lazylibrarian.images import shrinkMag

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

try:
    from lib.tinytag import TinyTag
except ImportError:
    TinyTag = None


def preprocess_ebook(bookfolder):
    logger.debug("Preprocess ebook %s" % bookfolder)
    ebook_convert = calibre_prg('ebook-convert')
    if not ebook_convert:
        logger.error("No ebook-convert found")
        return

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
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_postprocess:
                logger.debug(str(params))
            try:
                if os.name != 'nt':
                    _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                stderr=subprocess.STDOUT)
                else:
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

    if wanted_formats and lazylibrarian.CONFIG['DELETE_OTHER_FORMATS']:
        if lazylibrarian.CONFIG['KEEP_OPF']:
            wanted_formats.append('opf')
        if lazylibrarian.CONFIG['KEEP_JPG']:
            wanted_formats.append('jpg')
        for fname in listdir(bookfolder):
            filename, extn = os.path.splitext(fname)
            if not extn or extn.lstrip('.').lower() not in wanted_formats:
                logger.debug("Deleting %s" % fname)
                remove(os.path.join(bookfolder, fname))
    if created:
        logger.debug("Created %s from %s" % (created, source_extn))
    else:
        logger.debug("No extra ebook formats created")


def preprocess_audio(bookfolder, bookid=0, authorname='', bookname='', merge=None, tag=None):
    if merge is None:
        merge = lazylibrarian.CONFIG['CREATE_SINGLEAUDIO']
    if tag is None:
        tag = lazylibrarian.CONFIG['WRITE_AUDIOTAGS']
    if not merge and not tag:
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

    if failed or not parts:
        return
    if len(parts) == 1:
        logger.info("Only one audio file found, nothing to merge")
        return

    # if we get here, looks like we have all the parts
    out_type = os.path.splitext(parts[0][3])[1]
    # output file will be the same type as the first input file
    # unless the user supplies a -f parameter to override it
    if '-f ' in lazylibrarian.CONFIG['AUDIO_OPTIONS']:
        out_type = lazylibrarian.CONFIG['AUDIO_OPTIONS'].split('-f ')[1].split(',')[0].split(' ')[0]
        out_type = '.' + out_type

    b_to_a = False
    if out_type == '.m4b':
        # ffmpeg doesn't like m4b extension so rename to m4a
        b_to_a = True
        out_type = '.m4a'
    parts_mod = []
    for part in parts:
        if part[3].endswith('.m4b'):
            b_to_a = True
            new_name = part[3].replace('.m4b', '.m4a')
            os.rename(os.path.join(bookfolder, part[3]), os.path.join(bookfolder, new_name))
            parts_mod.append([part[0], part[1], part[2], new_name])
        else:
            parts_mod.append(part)
    parts = parts_mod

    with open(os.path.join(bookfolder, "partslist.ll"), 'w') as f:
        for part in parts:
            f.write("file '%s'\n" % part[3])
            if ff_ver and tag and authorname and bookname:
                if token or (part[2] != authorname) or (part[1] != bookname):
                    extn = os.path.splitext(part[3])[1]
                    params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                              '-y', '-c:a', 'copy', '-metadata', "album=%s" % bookname,
                              '-metadata', "artist=%s" % authorname,
                              '-metadata', "track=%s" % part[0],
                              os.path.join(bookfolder, "tempaudio%s" % extn)]
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_postprocess:
                        params.append('-report')
                        logger.debug(str(params))
                    try:
                        if os.name != 'nt':
                            _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                        stderr=subprocess.STDOUT)
                        else:
                            _ = subprocess.check_output(params, stderr=subprocess.STDOUT)

                        remove(os.path.join(bookfolder, part[3]))
                        os.rename(os.path.join(bookfolder, "tempaudio%s" % extn),
                                  os.path.join(bookfolder, part[3]))
                        logger.debug("Metadata written to %s" % part[3])
                    except subprocess.CalledProcessError as e:
                        logger.error("%s: %s" % (type(e).__name__, str(e)))
                        return
                    except Exception as e:
                        logger.error("%s: %s" % (type(e).__name__, str(e)))
                        return

    if ff_ver and merge:
        params = [ffmpeg, '-i', os.path.join(bookfolder, parts[0][3]),
                  '-f', 'ffmetadata', '-y', os.path.join(bookfolder, "metadata.ll")]
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_postprocess:
            params.append('-report')
            logger.debug(str(params))
        try:
            if os.name != 'nt':
                _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10), stderr=subprocess.STDOUT)
            else:
                _ = subprocess.check_output(params, stderr=subprocess.STDOUT)

            logger.debug("Metadata written to metadata.ll")
        except subprocess.CalledProcessError as e:
            logger.error("%s: %s" % (type(e).__name__, str(e)))
        except Exception as e:
            logger.error("%s: %s" % (type(e).__name__, str(e)))
            return

        params = [ffmpeg]
        params.extend(ffmpeg_params)
        params.extend(getList(lazylibrarian.CONFIG['AUDIO_OPTIONS']))
        params.append('-y')

        outfile = "%s - %s%s" % (authorname, bookname, out_type)
        params.append(os.path.join(bookfolder, outfile))
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_postprocess:
            params.append('-report')
            logger.debug(str(params))
        res = ''
        try:
            logger.debug("Merging %d files" % len(parts))
            if os.name != 'nt':
                res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10), stderr=subprocess.STDOUT)
            else:
                res = subprocess.check_output(params, stderr=subprocess.STDOUT)

        except subprocess.CalledProcessError as e:
            logger.error("%s: %s" % (type(e).__name__, str(e)))
            return
        except Exception as e:
            logger.error("%s: %s" % (type(e).__name__, str(e)))
            if res:
                logger.error(res)
            return

        if b_to_a:
            if outfile.endswith('.m4a'):
                new_name = outfile.replace('.m4a', '.m4b')
                os.rename(os.path.join(bookfolder, outfile), os.path.join(bookfolder, new_name))
                outfile = new_name
            for part in parts:
                if part[3].endswith('.m4a'):
                    new_name = part[3].replace('.m4a', '.m4b')
                    os.rename(os.path.join(bookfolder, part[3]), os.path.join(bookfolder, new_name))

        logger.info("%d files merged into %s" % (len(parts), outfile))
        extn = os.path.splitext(outfile)[1]

        params = [ffmpeg, '-i', os.path.join(bookfolder, outfile),
                  '-y', '-c:a', 'copy',
                  '-metadata', 'track="1/1"']

        myDB = database.DBConnection()
        match = myDB.match('SELECT * from books WHERE bookid=?', (bookid,))
        audio_path = os.path.join(bookfolder, parts[0][3])
        if tag and match and TinyTag and TinyTag.is_supported(audio_path):
            id3r = TinyTag.get(audio_path)
            artist = id3r.artist
            composer = id3r.composer
            album_artist = id3r.albumartist
            album = id3r.album
            title = id3r.title
            # "unused" locals are used in eval() statement below
            # noinspection PyUnusedLocal
            comment = id3r.comment
            # noinspection PyUnusedLocal
            author = authorname
            # noinspection PyUnusedLocal
            media_type = "Audiobook"
            # noinspection PyUnusedLocal
            genre = match['BookGenre']
            # noinspection PyUnusedLocal
            description = match['BookDesc']
            # noinspection PyUnusedLocal
            date = match['BookDate']
            if date == '0000':
                # noinspection PyUnusedLocal
                date = ''
            if artist:
                # noinspection PyUnusedLocal
                artist = artist.strip().rstrip('\x00')
            if composer:
                # noinspection PyUnusedLocal
                composer = composer.strip().rstrip('\x00')
            if album:
                # noinspection PyUnusedLocal
                album = album.strip().rstrip('\x00')
            if album_artist:
                # noinspection PyUnusedLocal
                album_artist = album_artist.strip().rstrip('\x00')
            if match['SeriesDisplay']:
                series = match['SeriesDisplay'].split('<br>')[0].strip()
                if series and title and '$SerName' in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FILE']:
                    title = "%s (%s)" % (title, series)
                    outfile, extn = os.path.splitext(outfile)
                    outfile = "%s (%s)%s" % (outfile, series, extn)

            params.extend(['-metadata', "title=%s" % title])
            for item in ['artist', 'album_artist', 'composer', 'album', 'author',
                         'date', 'comment', 'description', 'genre', 'media_type']:
                value = eval(item)
                if value:
                    params.extend(['-metadata', "%s=%s" % (item, value)])
        else:
            params.extend(['-metadata', "album=%s" % bookname,
                           '-metadata', "artist=%s" % authorname])
        params.append(os.path.join(bookfolder, "tempaudio%s" % extn))
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_postprocess:
            params.append('-report')
            logger.debug(str(params))
        try:
            if os.name != 'nt':
                _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                            stderr=subprocess.STDOUT)
            else:
                _ = subprocess.check_output(params, stderr=subprocess.STDOUT)

            remove(os.path.join(bookfolder, outfile))
            os.rename(os.path.join(bookfolder, "tempaudio%s" % extn),
                      os.path.join(bookfolder, outfile))
            logger.debug("Metadata written to %s" % outfile)
        except subprocess.CalledProcessError as e:
            logger.error("%s: %s" % (type(e).__name__, str(e)))
            return
        except Exception as e:
            logger.error("%s: %s" % (type(e).__name__, str(e)))
            return

        remove(os.path.join(bookfolder, "partslist.ll"))
        remove(os.path.join(bookfolder, "metadata.ll"))
        if not lazylibrarian.CONFIG['KEEP_SEPARATEAUDIO']:
            logger.debug("Removing %d part files" % len(parts))
            for part in parts:
                remove(os.path.join(bookfolder, part[3]))


def preprocess_magazine(bookfolder, cover=0):
    logger.debug("Preprocess magazine %s cover=%s" % (bookfolder, cover))
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

        dpi = check_int(lazylibrarian.CONFIG['SHRINK_MAG'], 0)
        if not dpi and not (lazylibrarian.CONFIG['SWAP_COVERPAGE'] and cover >= 2):
            return

        # reordering or shrinking pages is quite slow if the source is on a networked drive
        # so work on a local copy, then move it over.
        original = os.path.join(bookfolder, sourcefile)
        srcfile = safe_copy(original, os.path.join(lazylibrarian.CACHEDIR, sourcefile))

        if dpi:
            logger.debug("Resizing %s to %s dpi" % (srcfile, dpi))
            shrunkfile = shrinkMag(srcfile, dpi)
            old_size = os.stat(srcfile).st_size
            if shrunkfile:
                new_size = os.stat(shrunkfile).st_size
            else:
                new_size = 0
            logger.debug("New size %s, was %s" % (human_size(new_size), human_size(old_size)))
            if new_size and new_size < old_size:
                remove(srcfile)
                os.rename(shrunkfile, srcfile)
            elif shrunkfile:
                remove(shrunkfile)

        if lazylibrarian.CONFIG['SWAP_COVERPAGE'] and cover >= 2:
            if not PdfFileWriter:
                logger.error("PdfFileWriter not found")
            else:
                output = PdfFileWriter()
                with open(srcfile, "rb") as f:
                    cover -= 1  # zero based page count
                    input1 = PdfFileReader(f)
                    cnt = input1.getNumPages()
                    output.addPage(input1.getPage(cover))
                    p = 0
                    while p < cnt:
                        if p != cover:
                            output.addPage(input1.getPage(p))
                        p = p + 1
                    with open(srcfile + 'new', "wb") as outputStream:
                        output.write(outputStream)
                logger.debug("%s has %d pages. Cover from page %d" % (srcfile, cnt, cover + 1))
                try:
                    sz = os.stat(srcfile + 'new').st_size
                except Exception as e:
                    sz = 0
                    logger.warn("Unable to get size of %s: %s" % (srcfile + 'new', str(e)))
                if sz:
                    remove(srcfile)
                    newcopy = safe_move(srcfile + 'new', original + 'new')
                    os.rename(newcopy, original)
                    return
        safe_move(srcfile, original)
    except Exception as e:
        logger.error(str(e))
