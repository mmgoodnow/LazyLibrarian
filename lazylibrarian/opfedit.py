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

#  basic opf editor, only edit "our" fields, leave everything else unchanged

import os
import lazylibrarian
from lazylibrarian import logger

try:
    from html import escape  # python 3.x
except ImportError:
    from cgi import escape  # python 2.x


def opf_read(filename):
    if not os.path.exists(filename):
        return '', []
    keys = ['<dc:title>', '<dc:language>', '<dc:publisher>', '<dc:date>']
    subject = '<dc:subject>'
    scheme = '<dc:identifier'
    creator = '<dc:creator'
    content = '<meta content="'
    description = '<dc:description>'
    enddesc = description.replace('<', '</')
    desc = ''
    subjects = 0
    creators = 0
    replaces = []
    outfile = filename + '_mod'
    f = open(outfile, 'w')
    with open(filename, 'r') as i:
        data = i.readlines()
    try:
        for lyne in data:
            if description in lyne:
                desc = lyne
            elif desc:
                desc += lyne
            if enddesc in lyne:
                new_lyne, token, value = extract_key(description, desc)
                replaces.append((token, value))
                f.write(new_lyne)
                desc = ''
            elif not desc:
                new_lyne = ''
                for key in keys:
                    if key in lyne:
                        new_lyne, token, value = extract_key(key, lyne)
                        replaces.append((token, value))
                        break

                if subject in lyne:
                    new_lyne, token, value = extract_key(subject, lyne)
                    indexed = "subject_%02d" % subjects
                    subjects += 1
                    new_lyne = new_lyne.replace('$$subject$$', '$$%s$$' % indexed)
                    replaces.append((indexed, value))

                if scheme in lyne:
                    new_lyne, token, value = extract_scheme(lyne)
                    replaces.append((token, value))

                if creator in lyne:
                    indexed = "_%02d" % creators
                    creators += 1
                    new_lyne, role, fileas, value = extract_creator(lyne)
                    replaces.append(('role' + indexed, role))
                    replaces.append(('creator' + indexed, value))
                    found = False
                    for item in replaces:
                        if item[0] == 'fileas':
                            found = True
                            break
                    if not found:
                        replaces.append(('fileas', fileas))
                    new_lyne = new_lyne.replace('$$role$$', '$$role%s$$' % indexed).replace(
                                                '$$creator$$', '$$creator%s$$' % indexed)

                if content in lyne:
                    new_lyne, token, value = extract_content(content, lyne)
                    replaces.append((token, value))

                if not new_lyne:
                    new_lyne = lyne
                f.write(new_lyne)
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_matching:
            items = []
            for item in replaces:
                items.append(item[0])
            logger.debug(','.join(items))
    except Exception as e:
        logger.debug(str(e))
    finally:
        return outfile, replaces


def extract_key(key, data):
    ident = key.split(':', 1)[1].split('>')[0]
    endkey = key.replace('<', '</')
    value = data.split(key, 1)[1].split(endkey, 1)[0]
    return data.replace(key + value + endkey, key + '$$' + ident + '$$' + endkey), ident, value


def extract_scheme(data):
    ident = data.split('scheme="', 1)[1].split('"')[0]
    value = data.split('>', 1)[1].split('<')[0]
    return data.replace('>' + value + '<', '>$$%s$$<' % ident), ident, value


def extract_content(key, data):
    ident = data.split('name="', 1)[1].split('"')[0]
    value = data.split(key, 1)[1].split('"')[0]
    return data.replace(key + value + '"', key + '$$%s$$"' % ident), ident, value


def extract_creator(data):
    fileas = data.split('file-as="', 1)[1].split('"')[0]
    role = data.split('role="', 1)[1].split('"')[0]
    value = data.split('>', 1)[1].split('<')[0]
    return data.replace('>' + value + '<', '>$$creator$$<').replace(
        'role="' + role + '"', 'role="$$role$$"').replace(
        'file-as="' + fileas + '"', 'file-as="$$fileas$$"'), role, fileas, value


def opf_write(filename, replaces):
    if not os.path.exists(filename):
        return ''
    with open(filename, 'r') as i:
        data = i.readlines()
        outfile = filename + '_mod'
        f = open(outfile, 'w')
        for lyne in data:
            for item in replaces:
                lyne = lyne.replace('$$%s$$' % item[0], escape(item[1]))
            f.write(lyne)
    return outfile
