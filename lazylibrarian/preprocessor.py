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
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.filesystem import DIRS, remove_file, path_exists
from lazylibrarian.bookrename import audio_parts, name_vars, id3read
from lazylibrarian.common import listdir, safe_copy, safe_move, calibre_prg, setperm, zip_audio
from lazylibrarian.formatter import get_list, make_unicode, check_int, human_size, now, check_float
from lazylibrarian.images import shrink_mag

from PyPDF3 import PdfFileWriter, PdfFileReader


def preprocess_ebook(bookfolder):
    logger.debug("Preprocess ebook %s" % bookfolder)
    ebook_convert = calibre_prg('ebook-convert')
    if not ebook_convert:
        logger.error("No ebook-convert found")
        return

    sourcefile = None
    created = ''
    for fname in listdir(bookfolder):
        _, extn = os.path.splitext(fname)
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
    wanted_formats = get_list(lazylibrarian.CONFIG['EBOOK_WANTED_FORMATS'])
    for ftype in wanted_formats:
        if not path_exists(os.path.join(bookfolder, basename + '.' + ftype)):
            logger.debug("No %s" % ftype)
            params = [ebook_convert, os.path.join(bookfolder, sourcefile),
                      os.path.join(bookfolder, basename + '.' + ftype)]
            if ftype == 'mobi':
                params.extend(['--output-profile', 'kindle'])
            if lazylibrarian_log.LOGLEVEL & logger.log_postprocess:
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

    if wanted_formats and lazylibrarian.CONFIG.get_bool('DELETE_OTHER_FORMATS'):
        if lazylibrarian.CONFIG.get_bool('KEEP_OPF'):
            wanted_formats.append('opf')
        if lazylibrarian.CONFIG.get_bool('KEEP_JPG'):
            wanted_formats.append('jpg')
        for fname in listdir(bookfolder):
            filename, extn = os.path.splitext(fname)
            if not extn or extn.lstrip('.').lower() not in wanted_formats:
                logger.debug("Deleting %s" % fname)
                remove_file(os.path.join(bookfolder, fname))
    if created:
        logger.debug("Created %s from %s" % (created, source_extn))
    else:
        logger.debug("No extra ebook formats created")


def preprocess_audio(bookfolder, bookid=0, authorname='', bookname='', merge=None, tag=None, zipp=None):
    if merge is None:
        merge = lazylibrarian.CONFIG.get_bool('CREATE_SINGLEAUDIO')
    if tag is None:
        tag = lazylibrarian.CONFIG.get_bool('WRITE_AUDIOTAGS')
    if zipp is None:
        zipp = lazylibrarian.CONFIG.get_bool('ZIP_AUDIOPARTS')
    if not merge and not tag and not zipp:
        return

    ffmpeg = lazylibrarian.CONFIG['FFMPEG']
    if not ffmpeg:
        logger.error("Check config setting for ffmpeg")
        return
    ff_ver = lazylibrarian.FFMPEGVER
    if not ff_ver:
        try:
            params = [ffmpeg, "-version"]
            res = subprocess.check_output(params, stderr=subprocess.STDOUT)
            res = make_unicode(res).strip().split("Copyright")[0].split()[-1]
            logger.debug("Found ffmpeg version %s" % res)
            ff_ver = res
        except Exception as e:
            logger.debug("ffmpeg -version failed: %s %s" % (type(e).__name__, str(e)))
            ff_ver = ''
        finally:
            lazylibrarian.FFMPEGVER = ff_ver

    ff_aac = lazylibrarian.FFMPEGAAC
    if ff_ver and not ff_aac:
        try:
            params = [ffmpeg, "-codecs"]
            if os.name != 'nt':
                res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                              stderr=subprocess.STDOUT)
            else:
                res = subprocess.check_output(params, stderr=subprocess.STDOUT)
            res = make_unicode(res)
            for lyne in res.split('\n'):
                if 'AAC' in lyne:
                    ff_aac = lyne.strip().split(' ')[0]
                    break
        except Exception as e:
            logger.debug("ffmpeg -codecs failed: %s %s" % (type(e).__name__, str(e)))
            ff_aac = ''
        finally:
            lazylibrarian.FFMPEGAAC = ff_aac

    logger.debug("Preprocess audio %s %s %s" % (bookfolder, authorname, bookname))
    if zipp:
        _ = zip_audio(bookfolder, bookname, bookid)

    partslist = os.path.join(bookfolder, "partslist.ll")
    metadata = os.path.join(bookfolder, "metadata.ll")

    # this is to work around an ffmpeg oddity...
    if os.path.__name__ == 'ntpath':
        partslist = partslist.replace("\\", "/")
        metadata = metadata.replace("\\", "/")

    # this produces a single file audiobook
    ffmpeg_params = ['-f', 'concat', '-safe', '0', '-i', partslist, '-f', 'ffmetadata',
                     '-i', metadata, '-map_metadata', '1', '-id3v2_version', '3']

    parts, failed, token, abridged = audio_parts(bookfolder, bookname, authorname)

    if failed or not parts:
        return

    namevars = name_vars(bookid, abridged)

    # if we get here, looks like we have all the parts
    # output file will be the same type as the first input file
    # unless the user supplies a parameter to override it
    if lazylibrarian.CONFIG['FFMPEG_OUT']:
        out_type = '.' + lazylibrarian.CONFIG['FFMPEG_OUT'].lower().lstrip('.')
    else:
        out_type = os.path.splitext(parts[0][3])[1]

    if '-f ' in lazylibrarian.CONFIG['AUDIO_OPTIONS']:
        force_type = '.' + lazylibrarian.CONFIG['AUDIO_OPTIONS'].split('-f ', 1)[1].split(',')[0].split(' ')[0].strip()
    else:
        force_type = ''

    force_mp4 = False
    if out_type in ['.m4b', '.m4a', '.aac', '.mp4']:
        force_mp4 = True
        if 'D' not in ff_aac or 'E' not in ff_aac:
            logger.warn("Your version of ffmpeg does not report supporting read/write aac (%s) trying anyway" %
                        ff_aac)
    # else:  # should we force mp4 if input is mp4 but output is mp3?
    #     for part in parts:
    #         if os.path.splitext(part[3])[1] in ['.m4b', '.m4a', '.aac', '.mp4']:
    #             force_mp4 = True
    #            break

    if force_mp4 and force_type != 'mp4':
        if force_type:
            pre, post = lazylibrarian.CONFIG['AUDIO_OPTIONS'].split('-f ', 1)
            post = post.lstrip()
            post = post[len(force_type) + 1:]
            ffmpeg_options = pre + '-f mp4 ' + post
        else:
            ffmpeg_options = lazylibrarian.CONFIG['AUDIO_OPTIONS'] + ' -f mp4'
        logger.debug("ffmpeg options: %s" % ffmpeg_options)
    else:
        ffmpeg_options = lazylibrarian.CONFIG['AUDIO_OPTIONS']

    with open(partslist, 'w', encoding="utf-8") as f:
        for part in parts:
            f.write("file '%s'\n" % part[3])

            if lazylibrarian.CONFIG.get_bool('KEEP_SEPARATEAUDIO') and ff_ver and tag and authorname and bookname:
                if token or (part[2] != authorname) or (part[1] != bookname):
                    extn = os.path.splitext(part[3])[1]
                    params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                              '-y', '-c:a', 'copy', '-metadata', "album=%s" % bookname,
                              '-metadata', "artist=%s" % authorname,
                              '-metadata', "track=%s" % part[0],
                              os.path.join(bookfolder, "tempaudio%s" % extn)]
                    if lazylibrarian_log.LOGLEVEL & logger.log_postprocess:
                        params.append('-report')
                        logger.debug(str(params))
                        ffmpeg_env = os.environ.copy()
                        ffmpeg_env["FFREPORT"] = "file=" + \
                            DIRS.get_tmpfilename("ffmpeg-tag-%s.log" % now().replace(':', '-').replace(' ', '-'))
                    else:
                        ffmpeg_env = None
                    try:
                        if os.name != 'nt':
                            _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                        stderr=subprocess.STDOUT, env=ffmpeg_env)
                        else:
                            _ = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

                        remove_file(os.path.join(bookfolder, part[3]))
                        os.rename(os.path.join(bookfolder, "tempaudio%s" % extn),
                                  os.path.join(bookfolder, part[3]))
                        logger.debug("Metadata written to %s" % part[3])
                    except subprocess.CalledProcessError as e:
                        logger.error("%s: %s" % (type(e).__name__, str(e)))
                        return
                    except Exception as e:
                        logger.error("%s: %s" % (type(e).__name__, str(e)))
                        return

    bookfile = namevars['AudioSingleFile']
    if not bookfile:
        bookfile = "%s - %s" % (authorname, bookname)
    outfile = bookfile + out_type

    if ff_ver and merge:
        if len(parts) == 1:
            logger.info("Only one audio file found, nothing to merge")
        else:
            # read metadata from first file
            params = [ffmpeg, '-i', os.path.join(bookfolder, parts[0][3]),
                      '-f', 'ffmetadata', '-y', metadata]
            if lazylibrarian_log.LOGLEVEL & logger.log_postprocess:
                params.append('-report')
                logger.debug(str(params))
                ffmpeg_env = os.environ.copy()
                ffmpeg_env["FFREPORT"] = "file=" + \
                    DIRS.get_tmpfilename("ffmpeg-meta-%s.log" % now().replace(':', '-').replace(' ', '-'))
            else:
                ffmpeg_env = None
            try:
                if os.name != 'nt':
                    _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                stderr=subprocess.STDOUT, env=ffmpeg_env)
                else:
                    _ = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

                logger.debug("Metadata written to metadata.ll")
            except subprocess.CalledProcessError as e:
                logger.error("%s: %s" % (type(e).__name__, str(e)))
            except Exception as e:
                logger.error("%s: %s" % (type(e).__name__, str(e)))
                return

            part_durations = []
            for part in parts:
                params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                          '-f', 'ffmetadata', '-y', os.path.join(bookfolder, "partmeta.ll")]
                if lazylibrarian_log.LOGLEVEL & logger.log_postprocess:
                    params.append('-report')
                    logger.debug(str(params))
                    ffmpeg_env = os.environ.copy()
                    ffmpeg_env["FFREPORT"] = "file=" + \
                        DIRS.get_tmpfilename("ffmpeg-part-%s.log" % now().replace(':', '-').replace(' ', '-'))
                else:
                    ffmpeg_env = None
                try:
                    if os.name != 'nt':
                        res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                      stderr=subprocess.STDOUT, env=ffmpeg_env)
                    else:
                        res = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

                    res = res.decode('utf-8')
                    if 'Duration: ' in res:
                        try:
                            duration = res.split('Duration: ', 1)[1].split(',')[0]
                            h, m, s = duration.split(':')
                            secs = check_float(s, 0) + (check_int(m, 0) * 60) + (check_int(h, 0) * 3600)
                            part_durations.append([part[0], secs])
                            logger.debug("Part %s, duration %s" % (part[0], secs))
                        except IndexError:
                            pass

                except subprocess.CalledProcessError as e:
                    logger.error("%s: %s" % (type(e).__name__, str(e)))
                    return
                except Exception as e:
                    logger.error("%s: %s" % (type(e).__name__, str(e)))
                    return

            if part_durations:
                part_durations.sort(key=lambda x: x[0])
                start = 0
                with open(metadata, 'r', encoding="utf-8") as f:
                    with open(os.path.join(bookfolder, "newmetadata.ll"), 'w', encoding="utf-8") as o:
                        for lyne in f.readlines():
                            if not lyne.startswith('[CHAPTER]') and not lyne.startswith('TIMEBASE='):
                                if not lyne.startswith('START=') and not lyne.startswith('END='):
                                    if not lyne.startswith('title='):
                                        o.write(lyne)
                    remove_file(metadata)
                    os.rename(os.path.join(bookfolder, "newmetadata.ll"), metadata)

                with open(metadata, 'a', encoding="utf-8") as f:
                    for item in part_durations:
                        if item[0]:
                            f.write("[CHAPTER]\nTIMEBASE=1/1000\n")
                            f.write("START=%s\n" % int(start))
                            start = start + (1000 * item[1])
                            f.write("END=%s\n" % int(start))
                            f.write("title=Chapter %s\n" % item[0])

            params = [ffmpeg]
            params.extend(ffmpeg_params)
            params.extend(get_list(ffmpeg_options))
            params.append('-y')
            params.append(os.path.join(bookfolder, outfile))
            if lazylibrarian_log.LOGLEVEL & logger.log_postprocess:
                params.append('-report')
                logger.debug(str(params))
                ffmpeg_env = os.environ.copy()
                ffmpeg_env["FFREPORT"] = "file=" + \
                    DIRS.get_tmpfilename("ffmpeg-merge-%s.log" % now().replace(':', '-').replace(' ', '-'))
            else:
                ffmpeg_env = None
            res = ''
            try:
                logger.debug("Merging %d files" % len(parts))
                if os.name != 'nt':
                    res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                  stderr=subprocess.STDOUT, env=ffmpeg_env)
                else:
                    res = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

            except subprocess.CalledProcessError as e:
                logger.error("%s: %s" % (type(e).__name__, str(e)))
                return
            except Exception as e:
                logger.error("%s: %s" % (type(e).__name__, str(e)))
                if res:
                    logger.error(res)
                return

            logger.info("%d files merged into %s" % (len(parts), outfile))

        if tag:
            extn = os.path.splitext(outfile)[1]
            params = [ffmpeg, '-i', os.path.join(bookfolder, outfile),
                      '-y', '-c:a', 'copy',
                      '-metadata', 'track="1/1"']

            db = database.DBConnection()
            match = db.match('SELECT * from books WHERE bookid=?', (bookid,))
            audio_path = os.path.join(bookfolder, parts[0][3])
            if match:
                id3r = id3read(audio_path)
                if not match['Narrator'] and id3r['narrator']:
                    db.action("UPDATE books SET Narrator=? WHERE BookID=?", (id3r['narrator'], bookid))
                # noinspection PyUnusedLocal
                artist = id3r['artist']
                # noinspection PyUnusedLocal
                composer = id3r['composer']
                # noinspection PyUnusedLocal
                album_artist = id3r['albumartist']
                # noinspection PyUnusedLocal
                album = id3r['album']
                # title = id3r.title
                # "unused" locals are used in eval() statement below
                # noinspection PyUnusedLocal
                comment = id3r['comment']
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

                if bookfile:
                    title = bookfile
                else:
                    title = "%s - %s" % (authorname, bookname)
                    if match['SeriesDisplay']:
                        series = match['SeriesDisplay'].split('<br>')[0].strip()
                        if series and '$SerName' in lazylibrarian.CONFIG['AUDIOBOOK_DEST_FILE']:
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
                               '-metadata', "artist=%s" % authorname,
                               '-metadata', "title=%s" % bookfile])

            tempfile = os.path.join(bookfolder, "tempaudio%s" % extn)
            if extn == '.m4b':
                # some versions of ffmpeg will not add tags to m4b files, but they will add them to m4a
                b2a = True
                tempfile = tempfile.replace('.m4b', '.m4a')
            else:
                b2a = False

            params.append(tempfile)
            if lazylibrarian_log.LOGLEVEL & logger.log_postprocess:
                params.append('-report')
                logger.debug(str(params))
                ffmpeg_env = os.environ.copy()
                ffmpeg_env["FFREPORT"] = "file=" + \
                    DIRS.get_tmpfilename("ffmpeg-merge_tag-%s.log" % now().replace(':', '-').replace(' ', '-'))
            else:
                ffmpeg_env = None
            try:
                if os.name != 'nt':
                    _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                stderr=subprocess.STDOUT, env=ffmpeg_env)
                else:
                    _ = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

                outfile = os.path.join(bookfolder, outfile)
                remove_file(outfile)
                if b2a:
                    tempfile.replace('.m4a', '.m4b')
                os.rename(tempfile, outfile)
                logger.debug("Metadata written to %s" % outfile)
            except subprocess.CalledProcessError as e:
                logger.error("%s: %s" % (type(e).__name__, str(e)))
                return
            except Exception as e:
                logger.error("%s: %s" % (type(e).__name__, str(e)))
                return

        if not lazylibrarian.CONFIG.get_bool('KEEP_SEPARATEAUDIO') and len(parts) > 1:
            logger.debug("Removing %d part files" % len(parts))
            for part in parts:
                remove_file(os.path.join(bookfolder, part[3]))

    remove_file(partslist)
    remove_file(metadata)


def preprocess_magazine(bookfolder, cover=0):
    logger.debug("Preprocess magazine %s cover=%s" % (bookfolder, cover))
    try:
        sourcefile = None
        for fname in listdir(bookfolder):
            _, extn = os.path.splitext(fname)
            if extn.lower() == '.pdf':
                sourcefile = fname
                break

        if not sourcefile:
            logger.error("No suitable sourcefile found in %s" % bookfolder)
            return

        dpi = lazylibrarian.CONFIG.get_int('SHRINK_MAG')
        cover = check_int(cover, 0)

        if not dpi and not (lazylibrarian.CONFIG.get_bool('SWAP_COVERPAGE') and cover > 1):
            logger.debug("No preprocessing required")
            return

        # reordering or shrinking pages is quite slow if the source is on a networked drive
        # so work on a local copy, then move it over.
        original = os.path.join(bookfolder, sourcefile)
        srcfile = safe_copy(original, os.path.join(DIRS.CACHEDIR, sourcefile))

        if dpi:
            logger.debug("Resizing %s to %s dpi" % (srcfile, dpi))
            shrunkfile = shrink_mag(srcfile, dpi)
            old_size = os.stat(srcfile).st_size
            if shrunkfile:
                new_size = os.stat(shrunkfile).st_size
            else:
                new_size = 0
            logger.debug("New size %s, was %s" % (human_size(new_size), human_size(old_size)))
            if new_size and new_size < old_size:
                remove_file(srcfile)
                os.rename(shrunkfile, srcfile)
                _ = setperm(srcfile)
            elif shrunkfile:
                remove_file(shrunkfile)

        if lazylibrarian.CONFIG.get_bool('SWAP_COVERPAGE') and cover > 1:
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
                    remove_file(srcfile)
                    newcopy = safe_move(srcfile + 'new', original + 'new')
                    os.rename(newcopy, original)
                    _ = setperm(original)
                    return
        safe_move(srcfile, original)
        _ = setperm(original)
    except Exception as e:
        logger.error(str(e))
