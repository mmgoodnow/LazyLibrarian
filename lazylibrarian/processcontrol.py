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

# Purpose:
#   Provider functionality related to process control

import inspect
import os


def get_info_on_caller(depth=1, filenamewithoutpath=True, filenamewithoutext=True) -> (str, str, int):
    """ Return file and module name, plus line number, for the caller that called this.
    To get an even earlier caller, use a higher value for depth. If depth is too high,
    return '', '', 0 to indicate error.
    If filenamewithoutpath, return just the filename, otherwise include the full path.
    If filenamewithoutext, return just the base name, otherwise include the extension. """
    depth += 1  # We want to look at a level deeper than this call
    if len(inspect.stack()) > depth >= 1:
        caller_info = inspect.getframeinfo(inspect.stack()[depth][0])
        filename = os.path.basename(caller_info.filename) if filenamewithoutpath else caller_info.filename
        if filenamewithoutext:
            filename, _ = os.path.splitext(filename)
        caller_function = caller_info.function
        lineno = caller_info.lineno
        return filename, caller_function, lineno
    else:
        return '', '', 0
