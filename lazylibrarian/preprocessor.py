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
import subprocess
import logging
from urllib.parse import unquote_plus

import lazylibrarian
from lazylibrarian.config2 import CONFIG
from lazylibrarian import database
from lazylibrarian.filesystem import DIRS, remove_file, path_exists, listdir, setperm, safe_move, safe_copy
from lazylibrarian.bookrename import audio_parts, name_vars, id3read
from lazylibrarian.common import calibre_prg, zip_audio
from lazylibrarian.formatter import get_list, make_unicode, check_int, human_size, now, check_float, plural
from lazylibrarian.images import shrink_mag, coverswap, valid_pdf


def preprocess_ebook(bookfolder):
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    logger.debug(f"Preprocess ebook {bookfolder}")
    ebook_convert = calibre_prg('ebook-convert')
    if not ebook_convert:
        logger.error("No ebook-convert found")
        return False

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
        logger.error(f"No suitable sourcefile found in {bookfolder}")
        return False

    basename, source_extn = os.path.splitext(sourcefile)
    logger.debug(f"Wanted formats: {CONFIG['EBOOK_WANTED_FORMATS']}")
    wanted_formats = get_list(CONFIG['EBOOK_WANTED_FORMATS'])
    for ftype in wanted_formats:
        if not path_exists(os.path.join(bookfolder, basename + '.' + ftype)):
            logger.debug(f"No {ftype}")
            params = [ebook_convert, os.path.join(bookfolder, sourcefile),
                      os.path.join(bookfolder, basename + '.' + ftype)]
            if ftype == 'mobi':
                params.extend(['--output-profile', 'kindle'])
            loggerpostprocess.debug(str(params))
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
                logger.error(f"{e}")
                logger.error(repr(params))
                return False
        else:
            logger.debug(f"Found {ftype}")

    if wanted_formats and CONFIG.get_bool('DELETE_OTHER_FORMATS'):
        if CONFIG.get_bool('KEEP_OPF'):
            wanted_formats.append('opf')
        if CONFIG.get_bool('KEEP_JPG'):
            wanted_formats.append('jpg')
        for fname in listdir(bookfolder):
            filename, extn = os.path.splitext(fname)
            if not extn or extn.lstrip('.').lower() not in wanted_formats:
                logger.debug(f"Deleting {fname}")
                remove_file(os.path.join(bookfolder, fname))
    if created:
        logger.debug(f"Created {created} from {source_extn}")
    else:
        logger.debug("No extra ebook formats created")
    return True


def preprocess_audio(bookfolder, bookid=0, authorname='', bookname='', merge=None, tag=None, zipp=None):
    logger = logging.getLogger(__name__)
    loggerpostprocess = logging.getLogger('special.postprocess')
    if merge is None:
        merge = CONFIG.get_bool('CREATE_SINGLEAUDIO')
    if tag is None:
        tag = CONFIG.get_bool('WRITE_AUDIOTAGS')
    if zipp is None:
        zipp = CONFIG.get_bool('ZIP_AUDIOPARTS')
    if not merge and not tag and not zipp:
        return True  # nothing to do

    ffmpeg = CONFIG['FFMPEG']
    if not ffmpeg:
        logger.error("Check config setting for ffmpeg")
        return False
    ff_ver = lazylibrarian.FFMPEGVER
    if not ff_ver:
        try:
            params = [ffmpeg, "-version"]
            res = subprocess.check_output(params, stderr=subprocess.STDOUT)
            res = make_unicode(res).strip().split("Copyright")[0].split()[-1]
            logger.debug(f"Found ffmpeg version {res}")
            ff_ver = res
        except Exception as e:
            logger.debug(f"ffmpeg -version failed: {type(e).__name__} {str(e)}")
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
            logger.debug(f"ffmpeg -codecs failed: {type(e).__name__} {str(e)}")
            ff_aac = ''
        finally:
            lazylibrarian.FFMPEGAAC = ff_aac

    logger.debug(f"Preprocess audio {bookfolder} {authorname} {bookname}")
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
        return False

    namevars = name_vars(bookid, abridged)

    # if we get here, looks like we have all the parts
    # output file will be the same type as the first input file
    # unless the user supplies a parameter to override it
    out_type = ''
    if CONFIG['FFMPEG_OUT']:
        out_type = '.' + CONFIG['FFMPEG_OUT'].lower().lstrip('.')
        unquoted_type = unquote_plus(out_type)
        for token in ['<', '>', '=', '"']:
            if token in unquoted_type:
                logger.warning(f'Cannot set output type, contains "{token}"')
    if not out_type:
        out_type = os.path.splitext(parts[0][3])[1]

    if '-f ' in CONFIG['AUDIO_OPTIONS']:
        force_type = '.' + CONFIG['AUDIO_OPTIONS'].split('-f ', 1)[1].split(',')[0].split(' ')[0].strip()
    else:
        force_type = ''

    force_mp4 = False
    if out_type in ['.m4b', '.m4a', '.aac', '.mp4']:
        force_mp4 = True
        if 'D' not in ff_aac or 'E' not in ff_aac:
            logger.warning(f"Your version of ffmpeg does not report supporting read/write aac ({ff_aac}) trying anyway")
    # else:  # should we force mp4 if input is mp4 but output is mp3?
    #     for part in parts:
    #         if os.path.splitext(part[3])[1] in ['.m4b', '.m4a', '.aac', '.mp4']:
    #             force_mp4 = True
    #            break

    if force_mp4 and force_type != 'mp4':
        if force_type:
            pre, post = CONFIG['AUDIO_OPTIONS'].split('-f ', 1)
            post = post.lstrip()
            post = post[len(force_type) + 1:]
            ffmpeg_options = pre + '-f mp4 ' + post
        else:
            ffmpeg_options = CONFIG['AUDIO_OPTIONS'] + ' -f mp4'
        logger.debug(f"ffmpeg options: {ffmpeg_options}")
    else:
        ffmpeg_options = CONFIG['AUDIO_OPTIONS']

    with open(partslist, 'w', encoding="utf-8") as f:
        for part in parts:
            f.write(f"file '{part[3]}'\n")

            if CONFIG.get_bool('KEEP_SEPARATEAUDIO') and ff_ver and tag and authorname and bookname:
                if token or (part[2] != authorname) or (part[1] != bookname):
                    extn = os.path.splitext(part[3])[1]
                    params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                              '-y', '-c:a', 'copy', '-metadata', f"album={bookname}",
                              '-metadata', f"artist={authorname}",
                              '-metadata', f"track={part[0]}",
                              os.path.join(bookfolder, f"tempaudio{extn}")]
                    if loggerpostprocess.isEnabledFor(logging.DEBUG):
                        params.append('-report')
                        logger.debug(str(params))
                        ffmpeg_env = os.environ.copy()
                        ffmpeg_env["FFREPORT"] = "file=" + \
                            DIRS.get_tmpfilename(f"ffmpeg-tag-{now().replace(':', '-').replace(' ', '-')}.log")
                    else:
                        ffmpeg_env = None
                    try:
                        if os.name != 'nt':
                            _ = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                        stderr=subprocess.STDOUT, env=ffmpeg_env)
                        else:
                            _ = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

                        remove_file(os.path.join(bookfolder, part[3]))
                        os.rename(os.path.join(bookfolder, f"tempaudio{extn}"),
                                  os.path.join(bookfolder, part[3]))
                        logger.debug(f"Metadata written to {part[3]}")
                    except subprocess.CalledProcessError as e:
                        logger.error(f"{type(e).__name__}: {str(e)}")
                        return False
                    except Exception as e:
                        logger.error(f"{type(e).__name__}: {str(e)}")
                        return False

    bookfile = namevars['AudioSingleFile']
    if not bookfile:
        bookfile = f"{authorname} - {bookname}"
    outfile = bookfile + out_type

    if ff_ver and merge:
        if len(parts) == 1 and out_type == os.path.splitext(parts[0][3])[1]:
            logger.info("Only one audio file found, nothing to merge")
        else:
            # read metadata from first file
            params = [ffmpeg, '-i', os.path.join(bookfolder, parts[0][3]),
                      '-f', 'ffmetadata', '-y', metadata]
            if loggerpostprocess.isEnabledFor(logging.DEBUG):
                params.append('-report')
                logger.debug(str(params))
                ffmpeg_env = os.environ.copy()
                ffmpeg_env["FFREPORT"] = "file=" + \
                    DIRS.get_tmpfilename(f"ffmpeg-meta-{now().replace(':', '-').replace(' ', '-')}.log")
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
                logger.error(f"{type(e).__name__}: {str(e)}")
                return False
            except Exception as e:
                logger.error(f"{type(e).__name__}: {str(e)}")
                return False

            part_durations = []
            for part in parts:
                params = [ffmpeg, '-i', os.path.join(bookfolder, part[3]),
                          '-f', 'ffmetadata', '-y', os.path.join(bookfolder, "partmeta.ll")]
                if loggerpostprocess.isEnabledFor(logging.DEBUG):
                    params.append('-report')
                    logger.debug(str(params))
                    ffmpeg_env = os.environ.copy()
                    ffmpeg_env["FFREPORT"] = "file=" + \
                        DIRS.get_tmpfilename(f"ffmpeg-part-{now().replace(':', '-').replace(' ', '-')}.log")
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
                            logger.debug(f"Part {part[0]}, duration {secs}")
                        except IndexError:
                            pass

                except subprocess.CalledProcessError as e:
                    logger.error(f"{type(e).__name__}: {str(e)}")
                    return False
                except Exception as e:
                    logger.error(f"{type(e).__name__}: {str(e)}")
                    return False

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
                            f.write(f"START={int(start)}\n")
                            start = start + (1000 * item[1])
                            f.write(f"END={int(start)}\n")
                            f.write(f"title=Chapter {item[0]}\n")

            params = [ffmpeg]
            params.extend(ffmpeg_params)
            params.extend(get_list(ffmpeg_options))
            params.append('-y')
            params.append(os.path.join(bookfolder, outfile))
            if loggerpostprocess.isEnabledFor(logging.DEBUG):
                params.append('-report')
                logger.debug(str(params))
                ffmpeg_env = os.environ.copy()
                ffmpeg_env["FFREPORT"] = "file=" + \
                    DIRS.get_tmpfilename(f"ffmpeg-merge-{now().replace(':', '-').replace(' ', '-')}.log")
            else:
                ffmpeg_env = None
            res = ''
            try:
                logger.debug(f"Merging {len(parts)} files")
                if os.name != 'nt':
                    res = subprocess.check_output(params, preexec_fn=lambda: os.nice(10),
                                                  stderr=subprocess.STDOUT, env=ffmpeg_env)
                else:
                    res = subprocess.check_output(params, stderr=subprocess.STDOUT, env=ffmpeg_env)

            except subprocess.CalledProcessError as e:
                logger.error(f"{type(e).__name__}: {str(e)}")
                return False
            except Exception as e:
                logger.error(f"{type(e).__name__}: {str(e)}")
                if res:
                    logger.error(res)
                return False

            if len(parts) > 1:
                logger.info(f"{len(parts)} files merged into {outfile}")
            else:
                logger.info(f"Source {os.path.splitext(parts[0][3])[1]} file rewritten to {outfile}")

        if tag:
            try:
                extn = os.path.splitext(outfile)[1]
                params = [ffmpeg, '-i', os.path.join(bookfolder, outfile),
                          '-y', '-c:a', 'copy',
                          '-metadata', 'track=0']

                db = database.DBConnection()
                try:
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
                            title = f"{authorname} - {bookname}"
                            if match['SeriesDisplay']:
                                series = match['SeriesDisplay'].split('<br>')[0].strip()
                                if series and '$SerName' in CONFIG['AUDIOBOOK_DEST_FILE']:
                                    title = f"{title} ({series})"
                                    outfile, extn = os.path.splitext(outfile)
                                    outfile = f"{outfile} ({series}){extn}"
                        params.extend({'-metadata', f"title={title}"})
                        for item in ['artist', 'album_artist', 'composer', 'album', 'author',
                                     'date', 'comment', 'description', 'genre', 'media_type']:
                            value = eval(item)
                            if value:
                                params.extend(['-metadata', f"{item}={value}"])
                    else:
                        params.extend(['-metadata', f"album={bookname}",
                                       '-metadata', f"artist={authorname}",
                                       '-metadata', f"title={bookfile}"])
                finally:
                    db.close()

                tempfile = os.path.join(bookfolder, f"tempaudio{extn}")
                if extn == '.m4b':
                    # some versions of ffmpeg will not add tags to m4b files, but they will add them to m4a
                    b2a = True
                    tempfile = tempfile.replace('.m4b', '.m4a')
                else:
                    b2a = False

                params.append(tempfile)
                if loggerpostprocess.isEnabledFor(logging.DEBUG):
                    params.append('-report')
                    logger.debug(str(params))
                    ffmpeg_env = os.environ.copy()
                    ffmpeg_env["FFREPORT"] = "file=" + \
                        DIRS.get_tmpfilename(f"ffmpeg-merge_tag-{now().replace(':', '-').replace(' ', '-')}.log")
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
                    logger.debug(f"Metadata written to {outfile}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"{type(e).__name__}: {str(e)}")
                    return False
                except Exception as e:
                    logger.error(f"{type(e).__name__}: {str(e)}")
                    return False
            except Exception as e:
                logger.error(f" Error writing tags to files: {e}")
                return False

        if not CONFIG.get_bool('KEEP_SEPARATEAUDIO') and len(parts):
            logger.debug(f"Removing {len(parts)} part {plural(len(parts), 'file')}")
            for part in parts:
                remove_file(os.path.join(bookfolder, part[3]))

    remove_file(partslist)
    remove_file(metadata)
    return True


def preprocess_magazine(bookfolder, cover=0):
    logger = logging.getLogger(__name__)
    logger.debug(f"Preprocess magazine {bookfolder} cover={cover}")
    try:
        sourcefile = None
        for fname in listdir(bookfolder):
            _, extn = os.path.splitext(fname)
            if extn.lower() == '.pdf':
                sourcefile = fname
                break

        if not sourcefile:
            msg = f"No suitable sourcefile found in {bookfolder}"
            logger.error(msg)
            return False, msg

        if not valid_pdf(os.path.join(bookfolder, sourcefile)):
            msg = f"Invalid pdf {sourcefile} in {bookfolder}"
            return False, msg

        dpi = CONFIG.get_int('SHRINK_MAG')
        cover = check_int(cover, 0)

        if not dpi and not (CONFIG.get_bool('SWAP_COVERPAGE') and cover > 1):
            logger.debug("No preprocessing required")
            return True, ''

        # reordering or shrinking pages is quite slow if the source is on a networked drive
        # so work on a local copy, then move it over.
        original = os.path.join(bookfolder, sourcefile)
        try:
            srcfile = safe_copy(original, os.path.join(DIRS.CACHEDIR, sourcefile))
        except Exception as e:
            logger.warning(f"Failed to copy source file: {str(e)}")
            return False, str(e)
        if dpi:
            logger.debug(f"Resizing {srcfile} to {dpi} dpi")
            shrunkfile = shrink_mag(srcfile, dpi)
            old_size = os.stat(srcfile).st_size
            if shrunkfile:
                new_size = os.stat(shrunkfile).st_size
            else:
                new_size = 0
            logger.debug(f"New size {human_size(new_size)}, was {human_size(old_size)}")
            if new_size and new_size < old_size:
                remove_file(srcfile)
                os.rename(shrunkfile, srcfile)
                _ = setperm(srcfile)
            elif shrunkfile:
                remove_file(shrunkfile)

        if CONFIG.get_bool('SWAP_COVERPAGE') and cover > 1:
            coverswap(srcfile, cover)

        safe_move(srcfile, original)
        _ = setperm(original)
    except Exception as e:
        logger.error(str(e))
        return False, str(e)
    return True, ''
