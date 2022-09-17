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


def opf_read(filename):
    keys = ['<dc:title>', '<dc:language>', '<dc:publisher>', '<dc:date>', '<dc:description>']
    schemes = ['<dc:identifier']
    creators = ['<dc:creator']
    contents = ['<meta content="']
    with open(filename, 'r') as i:
        data = i.readlines()

    replaces = []
    outfile = filename + '_mod'
    f = open(outfile, 'w')
    for lyne in data:
        new_lyne = ''
        for key in keys:
            if key in lyne:
                new_lyne, token, value = extract_key(key, lyne)
                replaces.append((token, value))
                break
        for scheme in schemes:
            if scheme in lyne:
                new_lyne, token, value = extract_scheme(lyne)
                replaces.append((token, value))
                break
        for creator in creators:
            if creator in lyne:
                new_lyne, role, fileas, value = extract_creator(lyne)
                replaces.append(('role', role))
                replaces.append(('fileas', fileas))
                replaces.append(('creator', value))
                break
        for content in contents:
            if content in lyne:
                new_lyne, token, value = extract_content(content, lyne)
                replaces.append((token, value))
                break
        if not new_lyne:
            new_lyne = lyne
        f.write(new_lyne)

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
    with open(filename, 'r') as i:
        data = i.readlines()
        outfile = filename + '_mod'
        f = open(outfile, 'w')
        for lyne in data:
            for item in replaces:
                lyne = lyne.replace('$$%s$$' % item[0], item[1])
            f.write(lyne)
    return outfile
