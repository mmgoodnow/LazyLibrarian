#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
#  This file is part of Lazylibrarian.
#
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

import tarfile

archivename = 'backup.tgz'

if tarfile.is_tarfile(archivename):
    try:
        with tarfile.open(archivename) as tar:
            tar.extractall()
    except Exception as e:
        print('error', 'Failed to unpack tarfile %s (%s): %s' %
              (type(e).__name__, archivename, str(e)))
else:
    print("Invalid archive")
exit(0)
