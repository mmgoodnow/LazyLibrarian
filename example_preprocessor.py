#!/usr/bin/python
# The parameter list is type, folder, authorname, bookname
# where "type" is one of 'ebook', 'audiobook', 'magazine', 'comic', 'test'
# and "folder" is the folder ready to be processed
# and authorname, bookname are optional, only used for audio tags
# This example uses "ebook-convert" from calibre to make sure we have both epub and mobi of the new book.
# and "ffmpeg" to produce a single file audiobook and/or write id3 tags
# Note it is not fully error trapped, just a basic working example.
# Error messages appear as errors in the lazylibrarian log
# Anything you print to stdout appears as debug messages in the log
# The exit code and messages get passed back to the "test" button
# Always exit zero on success, non-zero on fail

import os
import subprocess
import sys
import time

try:
    from lib.tinytag import TinyTag
except ImportError:
    TinyTag = None

if sys.version_info[0] == 3:
    text_type = str
else:
    text_type = unicode

# eBook options
###########################################################################
preprocess_ebook = True
converter = "ebook-convert"  # if not in your "path", put the full pathname here
wanted_formats = ['.epub', '.mobi']
keep_opf = True
keep_jpg = True
delete_others = False  # use with care, deletes everything except wanted formats (and opf/jpg if keep is True)
###########################################################################
# audiobook options
write_singlefile = True
write_tags = True
ffmpeg = 'ffmpeg'  # if not in your "path", put the full pathname here
audio_options = ['-ab', '320k']
keep_original_audiofiles = True
audiotypes = ['mp3', 'flac', 'm4a', 'm4b']


###########################################################################
# should not need to alter anything below here
###########################################################################


def makeBytestr(txt):
    # convert unicode to bytestring, needed for os.walk and os.listdir
    # listdir falls over if given unicode startdir and a filename in a subdir can't be decoded to ascii
    if not txt:
        return b''
    elif not isinstance(txt, text_type):  # nothing to do if already bytestring
        return txt
    for encoding in ['utf-8', 'latin-1']:
        try:
            txt = txt.encode(encoding)
            return txt
        except UnicodeError:
            pass
    return txt


def makeUnicode(txt):
    # convert a bytestring to unicode, don't know what encoding it might be so try a few
    # it could be a file on a windows filesystem, unix...
    if not txt:
        return u''
    elif isinstance(txt, text_type):
        return txt
    for encoding in ['utf-8', 'latin-1']:
        try:
            txt = txt.decode(encoding)
            return txt
        except UnicodeError:
            pass
    return txt


def check_int(var, default, positive=True):
    """
    Return an integer representation of var
    or return default value if var is not a positive integer
    """
    try:
        res = int(var)
        if positive and res < 0:
            return default
        return res
    except (ValueError, TypeError):
        try:
            return int(default)
        except (ValueError, TypeError):
            return 0


def main():
    authorname = ''
    bookname = ''
    if len(sys.argv) < 3:
        sys.stderr.write("Invalid parameters (%s) assume test\n" % len(sys.argv))
        booktype = 'test'
        bookfolder = ''
    else:
        booktype = sys.argv[1]
        bookfolder = sys.argv[2]
        if len(sys.argv) == 5:
            authorname = sys.argv[3]
            bookname = sys.argv[4]

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'preprocessor.log'), 'a') as pplog:
        pplog.write("%s: %s %s\n" % (time.ctime(), booktype, bookfolder))
        if not booktype or booktype not in ['ebook', 'audiobook', 'magazine', 'test']:
            sys.stderr.write("%s %s\n" % ("Invalid booktype", booktype))
            pplog.write("%s: %s %s\n" % (time.ctime(), "Invalid booktype", booktype))
            exit(0)
        if not os.path.exists(bookfolder) and booktype != 'test':
            sys.stderr.write("%s %s\n" % ("Invalid bookfolder", bookfolder))
            pplog.write("%s: %s %s\n" % (time.ctime(), "Invalid bookfolder", bookfolder))
            exit(1)

        if booktype == 'test':
            print("Preprocessor test")
            if not os.path.exists(bookfolder):
                bookfolder = os.path.dirname(os.path.abspath(__file__))

        if booktype in ['ebook', 'test']:
            if booktype == 'ebook' and not preprocess_ebook:
                print("ebook preprocessing is disabled")
                exit(0)
            sourcefile = None
            source_extn = None
            created = ''
            for fname in os.listdir(makeBytestr(bookfolder)):
                fname = makeUnicode(fname)
                filename, extn = os.path.splitext(fname)
                if extn == '.epub':
                    sourcefile = fname
                    break
                elif extn == '.mobi':
                    sourcefile = fname
                    break

            pplog.write("Wanted formats: %s\n" % str(wanted_formats))
            if not sourcefile:
                if booktype == 'test':
                    print("No suitable sourcefile found in %s" % bookfolder)
                else:
                    sys.stderr.write("%s %s\n" % ("No suitable sourcefile found in", bookfolder))
                pplog.write("%s: %s %s\n" % (time.ctime(), "No suitable sourcefile found in", bookfolder))
            else:
                sourcefile = makeBytestr(sourcefile)
                basename, source_extn = os.path.splitext(sourcefile)
                for ftype in wanted_formats:
                    if not os.path.exists(os.path.join(bookfolder, basename + ftype)):
                        pplog.write("No %s\n" % ftype)
                        params = [converter, os.path.join(bookfolder, sourcefile),
                                  os.path.join(bookfolder, basename + ftype)]
                        if ftype == '.mobi':
                            params.extend(['--output-profile', 'kindle'])
                        try:
                            _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
                            if created:
                                created += ' '
                            created += ftype
                        except Exception as e:
                            sys.stderr.write("%s\n" % e)
                            pplog.write("%s: %s\n" % (time.ctime(), e))
                            exit(1)
                    else:
                        pplog.write("Found %s\n" % ftype)

                if delete_others:
                    if keep_opf:
                        wanted_formats.append('.opf')
                    if keep_jpg:
                        wanted_formats.append('.jpg')
                    for fname in os.listdir(makeBytestr(bookfolder)):
                        fname = makeUnicode(fname)
                        filename, extn = os.path.splitext(fname)
                        if not extn or extn.lower() not in wanted_formats:
                            if booktype == 'test':
                                print("Would delete %s" % fname)
                                pplog.write("Would delete %s\n" % fname)
                            else:
                                print("Deleting %s" % fname)
                                pplog.write("Deleting %s\n" % fname)
                                try:
                                    os.remove(os.path.join(bookfolder, fname))
                                except OSError:
                                    pass
            if created:
                print("Created %s from %s" % (created, source_extn))
                pplog.write("%s: Created %s from %s\n" % (time.ctime(), created, source_extn))
            else:
                print("No extra ebook formats created")
                pplog.write("%s: No extra ebook formats created\n" % time.ctime())

        elif booktype == 'audiobook':
            if not write_singlefile and not write_tags:
                print("audiobook preprocessing is disabled")
                exit(0)

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
            for f in os.listdir(makeBytestr(bookfolder)):
                f = makeUnicode(f)
                extn = os.path.splitext(f)[1].lstrip('.')
                if extn and extn.lower() in audiotypes:
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
                        print("tinytag %s %s" % (type(e).__name__, str(e)))
                        pass

            pplog.write("%s found %s audiofiles\n" % (book, cnt))

            if cnt == 1 and not parts:  # single file audiobook with no tags
                parts = [[1, book, author, audio_file]]

            if cnt != len(parts):
                print("%s: Incorrect number of parts (found %i from %i)" % (book, len(parts), cnt))
                exit(1)

            if total and total != cnt:
                print("%s: Reported %i parts, got %i" % (book, total, cnt))
                exit(1)

            if cnt == 1:
                print("Only one audio file found, nothing to merge")
                exit(0)

            # check all parts have the same author and title
            if len(parts) > 1:
                for part in parts:
                    if part[1] != book:
                        print("%s: Inconsistent title: [%s][%s]" % (book, part[1], book))
                        exit(1)
                    if part[2] != author:
                        print("%s: Inconsistent author: [%s][%s]" % (book, part[2], author))
                        exit(1)

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
                    print("%s: No part %i found" % (book, cnt + 1))
                    exit(1)
                cnt += 1

            # if we get here, looks like we have all the parts
            with open(os.path.join(bookfolder, 'partslist.ll'), 'wb') as f:
                for part in parts:
                    f.write("file '%s'\n" % makeBytestr(part[3]))
                    if write_tags and authorname and bookname:
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
                                print("Metadata written to %s" % part[3])
                            except Exception as e:
                                sys.stderr.write("%s\n" % e)

            if write_singlefile:
                params = [ffmpeg, '-i', os.path.join(bookfolder, parts[0][3]),
                          '-f', 'ffmetadata', '-y', os.path.join(bookfolder, 'metadata.ll')]
                try:
                    _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
                    print("Metadata written to file")
                except Exception as e:
                    sys.stderr.write("%s\n" % e)
                    pplog.write("%s: %s\n" % (time.ctime(), e))
                    exit(1)

                params = [ffmpeg]
                params.extend(ffmpeg_params)
                params.extend(audio_options)
                params.append('-y')
                if not out_type:
                    out_type = 'mp3'
                outfile = "%s - %s.%s" % (author, book, out_type)
                params.append(os.path.join(bookfolder, outfile))

                try:
                    msg = "Processing %d files" % len(parts)
                    print(msg)
                    pplog.write("%s: %s\n" % (time.ctime(), msg))
                    _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
                except Exception as e:
                    sys.stderr.write("%s\n" % e)
                    pplog.write("%s: %s\n" % (time.ctime(), e))
                    exit(1)

                msg = "%d files merged into %s" % (len(parts), outfile)
                print(msg)
                pplog.write("%s: %s\n" % (time.ctime(), msg))
                os.remove(os.path.join(bookfolder, 'partslist.ll'))
                os.remove(os.path.join(bookfolder, 'metadata.ll'))
                if not keep_original_audiofiles:
                    msg = "Removing %d part files" % len(parts)
                    print(msg)
                    pplog.write("%s: %s\n" % (time.ctime(), msg))
                    for part in parts:
                        os.remove(os.path.join(bookfolder, part[3]))

        elif booktype in ['magazine', 'comic']:
            # maybe you want to split the pages and turn them into jpeg like a comic?
            print("This example preprocessor only preprocesses eBooks and audiobooks")

    exit(0)


if __name__ == "__main__":
    main()
