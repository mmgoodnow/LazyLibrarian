#!/usr/bin/python
import sys
import os
import subprocess

from lazylibrarian.common import calibre_prg
from lazylibrarian.config2 import CONFIG
from lazylibrarian.filesystem import get_directory


def convert(input_file, output_format):
    """
    Converts an eBook file to a different format using calibre's ebook-convert command
    :param input_file: Filepath of the book to be converted
    :param output_format: The format to output (eg. mobi, azw3)
    :return: Filepath of the converted book
    """

    converter = calibre_prg('ebook-convert')
    if not converter:
        sys.stderr.write("Error, No ebook-convert found")
        raise ValueError("No ebook-convert found")

    calibredb = calibre_prg('calibredb')
    if not calibredb:
        sys.stderr.write("Error, No calibredb found")
        raise ValueError("No calibredb found")

    if CONFIG.get_bool('CALIBRE_USE_SERVER'):
        ebook_directory = CONFIG['CALIBRE_SERVER']
    else:
        ebook_directory = get_directory('eBook')

    basename, extn = os.path.splitext(input_file)

    # Strip leading dot from output format
    output_format = output_format.strip('.')

    try:
        # noinspection PyTypeChecker
        calibreid = basename.rsplit('(', 1)[1].split(')')[0]
        if not calibreid.isdigit():
            calibreid = ''
    except IndexError:
        calibreid = ''

    if not ebook_directory:
        calibreid = ''

    params = [converter, input_file, basename + '.' + output_format]
    try:
        _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
        if calibreid:  # tell calibre about the new format
            params = [calibredb, "add_format", "--with-library", "%s" % ebook_directory]

            # Add user authentication if provided
            if CONFIG.get_bool('CALIBRE_USE_SERVER') and CONFIG['CALIBRE_USER'] and \
                    CONFIG['CALIBRE_PASS']:
                params.extend(['--username', CONFIG['CALIBRE_USER'],
                               '--password', CONFIG['CALIBRE_PASS']])

            params.extend([calibreid, "%s" % basename + '.' + output_format])
            _ = subprocess.check_output(params, stderr=subprocess.STDOUT)
        return basename + '.' + output_format
    except Exception as e:
        sys.stderr.write("%s\n" % e)
        raise Exception(e)
