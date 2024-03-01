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
#  along with Lazylibrarian.  if not, see <http://www.gnu.org/licenses/>.

########
# Author: /u/anonymous_rocketeer
# THIS PROGRAM COMES WITH ABSOLUTELY NO WARRANTY
# Please download only public domain books ;)
########

"""
A Simple Ebook downloading bot.
Much credit to Joel Rosdahl for his irc package:
https://github.com/jaraco/irc
This code is of debatable quality, but as far as I can tell, it works.
Use at your own risk
"""

import time
import struct
import os
import zipfile
import logging
import irc.bot
import irc.strings
from irc.client import ip_numstr_to_quad
import shlex
from jaraco.stream import buffer
import irc.client
import threading

import lazylibrarian
from lazylibrarian.blockhandler import BLOCKHANDLER
from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.formatter import today, size_in_bytes, md5_utf8, check_int
from lazylibrarian.filesystem import DIRS, path_isfile, syspath, remove_file

# Prevents a common UnicodeDecodeError when downloading from many sources that don't use utf-8
irc.client.ServerConnection.buffer_class = buffer.LenientDecodingLineBuffer

searchtimeout = 60
dltimeout = 360


class IrcBot(irc.bot.SingleServerIRCBot):
    def __init__(self, searchterm,  localfolder, channel, nickname, filename, server, port=6667, searchtype="@search",):
        port = check_int(port, 6667)
        irc.bot.SingleServerIRCBot.__init__(
            self, [(server, port)], nickname, nickname)
        self.channel = channel
        self.searchterm = searchterm
        self.received_bytes = 0
        self.havebook = False
        self.localfolder = localfolder
        self.logger = logging.getLogger(__name__)
        self.dlcommslogger = logging.getLogger('special.dlcomms')
        self.searchtype = searchtype
        self.download = self.searchtype.startswith('!')
        self.timer = None
        self.filename = filename
        self.file = None
        self.my_dcc = None

    def on_nicknameinuse(c, e):  # handle username conflicts
        c.nick(c.get_nickname() + "_")

    def on_welcome(self, c, e):
        self.dlcommslogger.debug("on_welcome")
        c.join(self.channel)
        self.timer = threading.Timer(searchtimeout, self.handle_timeout)
        self.timer.start()
        self.connection.privmsg(self.channel, self.searchtype + " " + self.searchterm)

        if not self.download:
            self.logger.debug("Searching ...\n")
        else:
            self.logger.debug("Downloading ...\n")

    def handle_timeout(self):
        self.logger.debug("No search results found")
        self.timer.cancel()
        self.die()

    def on_ctcp(self, connection, event):
        self.dlcommslogger.debug("on_ctcp")
        # Handle the actual download
        payload = event.arguments[1]
        parts = shlex.split(payload)

        if len(parts) != 5:  # Check if it's a DCC SEND
            return  # If not, we don't care what it is

        self.dlcommslogger.debug("Receiving Data:")
        self.timer.cancel()

        self.logger.debug(payload)
        command, filename, peer_address, peer_port, size = parts
        if command != "SEND":
            return
        self.logger.debug("peer sending file on port " + str(peer_port))
        # self.filename = os.path.basename(filename)
        self.filename = self.localfolder + "/" + self.filename
        self.logger.debug("writing file " + self.filename)
        self.file = open(self.filename, "wb")
        peer_address = irc.client.ip_numstr_to_quad(peer_address)
        peer_port = int(peer_port)
        self.my_dcc = self.dcc("raw")
        self.my_dcc.connect(peer_address, peer_port)

    def on_dccmsg(self, connection, event):
        data = event.arguments[0]
        self.file.write(data)
        self.received_bytes = self.received_bytes + len(data)
        self.my_dcc.send_bytes(struct.pack("!I", self.received_bytes))

    def on_dcc_disconnect(self, connection, event):
        self.file.close()
        self.logger.debug("Received file %s (%d bytes).\n" % (self.filename, self.received_bytes))
        self.timer.cancel()
        self.die()  # end program when the book disconnect finishes

    def search(self, searchterm):
        self.connection.privmsg(self.channel, searchterm)


def irc_query(provider: ConfigDict, filename, searchterm, searchtype, cache=True):
    logger = logging.getLogger(__name__)
    cachelogger = logging.getLogger('special.cache')
    if BLOCKHANDLER.is_blocked(provider['SERVER']):
        msg = "%s is blocked" % provider['SERVER']
        logger.warning(msg)
        return ''

    if not searchtype:
        searchtype = provider['SEARCH']

    cache_location = os.path.join(DIRS.CACHEDIR, "IRCCache")
    if cache:
        if searchterm:
            myhash = md5_utf8(provider['SERVER'] + provider['CHANNEL'] + searchterm)
        else:
            myhash = md5_utf8(provider['SERVER'] + provider['CHANNEL'] + searchtype)
        valid_cache = False
        hashfilename = os.path.join(cache_location, myhash + ".irc")
        # cache results so we can do multiple searches for the same author
        # or multiple search types for a book without hammering the irc provider
        # expire cache after 2 hours, there might be new additions
        expiry = check_int(lazylibrarian.IRC_CACHE_EXPIRY, 2 * 3600)

        if path_isfile(hashfilename):
            cache_modified_time = os.stat(hashfilename).st_mtime
            time_now = time.time()
            if cache_modified_time < time_now - expiry:
                # Cache entry is too old, delete it
                cachelogger.debug("Expiring %s" % myhash)
                remove_file(hashfilename)
            else:
                valid_cache = True

        if valid_cache:
            lazylibrarian.CACHE_HIT = int(lazylibrarian.CACHE_HIT) + 1
            cachelogger.debug("CacheHandler: Returning CACHED response %s for %s" % (hashfilename,
                                                                                     searchterm))
            return

        lazylibrarian.CACHE_MISS = int(lazylibrarian.CACHE_MISS) + 1
    else:
        hashfilename = ''

    bot = IrcBot(searchterm, cache_location, provider['CHANNEL'], provider['BOTNICK'], filename, provider['SERVER'], None, searchtype)
    bot.start()


def irc_results(provider: ConfigDict, fname,):
    # Open the zip file, extract the txt
    # for each line that starts with !
    # user is first word
    # filename is rest up to ::INFO:: or "\r"
    # if ::INFO:: in line, following word is size including unit
    # if \r- in line last two words are size/unit
    logger = logging.getLogger(__name__)
    results = []
    tor_date = today()
    logger.debug("Checking results in %s" % fname)
    if fname and zipfile.is_zipfile(fname):
        try:
            data = zipfile.ZipFile(fname)
            if data:
                our_member = None
                for member in data.namelist():
                    if '.txt' in member.lower():
                        our_member = member
                        break

                if our_member:
                    with data.open(our_member) as ourfile:
                        new_line = '!'
                        while new_line:
                            new_line = ourfile.readline()
                            lyne = new_line.decode('utf-8').rstrip()
                            if lyne.startswith('!'):
                                user, remainder = lyne.split(' ', 1)
                                filename = ''
                                size = ''
                                if '::INFO::' in remainder:
                                    filename, size = remainder.split('::INFO::', 1)
                                elif '\r-' in remainder:
                                    filename, remainder = remainder.split('\r-', 1)
                                    words = remainder.strip().split()
                                    size = words[-2]
                                    units = words[-1]
                                    size = size + units

                                if filename and size:
                                    filename = filename.strip()
                                    size = size_in_bytes(str(size))

                                    results.append({
                                        'tor_prov': provider['SERVER'],
                                        'tor_title': filename,
                                        'tor_url': user,
                                        'tor_size': str(size),
                                        'tor_date': tor_date,
                                        'tor_feed': provider['NAME'],
                                        'tor_type': 'irc',
                                        'priority': provider['DLPRIORITY'],
                                        'dispname': provider['DISPNAME'],
                                        'types': provider['DLTYPES'],
                                    })
                else:
                    logger.error("No results file found in %s" % fname)
            else:
                logger.error("No zip data in %s" % fname)
        except Exception as e:
            logger.error("Error reading results: %s" % str(e))
    return results
