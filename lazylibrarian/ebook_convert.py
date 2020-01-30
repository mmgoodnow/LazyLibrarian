#!/usr/bin/python
import sys
import os
import subprocess
import lazylibrarian


def convert(input_file, output_format):
    """
    Converts an eBook file to a different format using calibre's ebook-convert command
    :param input_file: Filepath of the book to be converted
    :param output_format: The format to output (eg. mobi, azw3)
    :return: Filepath of the converted book
    """

    # Check to see if calibredb exists. If it does not, we cannot find ebook-convert, and we raise an error
    if not lazylibrarian.CONFIG['IMP_CALIBREDB']:
        sys.stderr.write("Error, No calibredb configured")
        raise ValueError("No calibredb configured")

    calibredb = lazylibrarian.CONFIG['IMP_CALIBREDB']

    if lazylibrarian.CONFIG['CALIBRE_USE_SERVER']:
        ebook_directory = lazylibrarian.CONFIG['CALIBRE_SERVER']
    else:
        ebook_directory = lazylibrarian.DIRECTORY('eBook')

    converter = os.path.join(os.path.dirname(calibredb), 'ebook-convert')

    basename, extn = os.path.splitext(input_file)

    # Strip leading dot from output format
    output_format = output_format.strip('.')

    try:
        calibreid = basename.rsplit('(', 1)[1].split(')')[0]
        if not calibreid.isdigit():
            calibreid = ''
    except IndexError:
        calibreid = ''

    if not ebook_directory or not calibredb:
        calibreid = ''

    params = [converter, input_file, basename + '.' + output_format]
    try:
        _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
        if calibreid:  # tell calibre about the new format
            params = [calibredb, "add_format", "--with-library", "%s" % ebook_directory]

            # Add user authentication if provided
            if lazylibrarian.CONFIG['CALIBRE_USE_SERVER'] and lazylibrarian.CONFIG['CALIBRE_USER'] and \
                    lazylibrarian.CONFIG['CALIBRE_PASS']:
                params.extend(['--username', lazylibrarian.CONFIG['CALIBRE_USER'],
                               '--password', lazylibrarian.CONFIG['CALIBRE_PASS']])

            params.extend([calibreid, "%s" % basename + '.' + output_format])
            _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
        return basename + '.' + output_format
    except Exception as e:
        sys.stderr.write("%s\n" % e)
        raise Exception(e)
