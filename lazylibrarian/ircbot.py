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

import socket
import time
import struct
import os
import tarfile
from lib.six import PY2
import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.formatter import today, size_in_bytes, makeBytestr, replace_all, md5_utf8
from lazylibrarian.common import namedic
try:
    import zipfile
except ImportError:
    if PY2:
        import lib.zipfile as zipfile
    else:
        import lib3.zipfile as zipfile
try:
    from fuzzywuzzy import fuzz
except ImportError:
    from lib.fuzzywuzzy import fuzz


def ip_numstr_to_quad(num):
    """
    Convert an IP number as an integer given in ASCII
    representation to an IP address string.
    >>> ip_numstr_to_quad('3232235521')
    '192.168.0.1'
    >>> ip_numstr_to_quad(3232235521)
    '192.168.0.1'
    """
    packed = struct.pack('>L', int(num))
    bts = struct.unpack('BBBB', packed)
    return ".".join(map(str, bts))


class IRC:

    irc = socket.socket()

    def __init__(self):
        self.ver = "LazyLibrarian ircbot version 2020-01-29 (https://gitlab.com/LazyLibrarian)"
        self.server = ""
        self.nick = ""
        # Define the socket
        self.irc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.irc.settimeout(10)

    def send(self, server, channel, msg):
        # Transfer data
        try:
            self.irc.send(makeBytestr("PRIVMSG " + channel + " " + msg + "\n"))
        except Exception as e:
            logger.debug("Exception sending %s" % msg)
            logger.debug(str(e))
            lazylibrarian.providers.BlockProvider(server, msg, 600)

    def ison(self, msg):
        self.irc.send(makeBytestr("ISON " + msg + "\n"))

    def pong(self, msg):
        reply = msg.replace('PING', 'PONG')
        logger.debug("Reply: %s" % reply)
        self.irc.send(makeBytestr(reply + "\n"))

    def version(self):
        reply = "VERSION " + self.ver
        logger.debug("Reply: %s" % reply)
        self.irc.send(makeBytestr(reply + "\n"))

    def join(self, channel):
        self.irc.send(makeBytestr("JOIN " + channel + "\n"))

    def part(self, channel):
        self.irc.send(makeBytestr("PART " + channel + " :Bye\n"))

    def connect(self, server, port, botnick="", botpass=""):
        # Connect to the server
        logger.debug("Connecting to: " + server)
        logger.debug(self.ver)
        try:
            email = lazylibrarian.CONFIG['ADMIN_EMAIL']
            self.irc.connect((server, port))
            self.server = server
            self.nick = botnick
            if botnick:
                logger.debug("Sending auth for %s" % botnick)
                # Perform user authentication
                self.irc.send(makeBytestr("USER " + botnick + " " + botnick + " " + botnick + " :LazyLibrarian\n"))
                self.irc.send(makeBytestr("NICK " + botnick + "\n"))
                if botpass and email:
                    logger.debug("Sending nickserv")
                    self.irc.send(makeBytestr("NICKSERV REGISTER " + botpass + " " + email + "\n"))
                    time.sleep(2)
                    self.irc.send(makeBytestr("NICKSERV IDENTIFY " + botpass + "\n"))
                    time.sleep(2)
        except Exception:
            raise

    def get_response(self):
        # Get the response
        try:
            reply = self.irc.recv(2040)
        except socket.timeout:
            logger.debug("response timeout")
            reply = ''
        # should really put the data in a queue/buffer so we can handle split lines
        # for now we'll just split on newline and ignore the last line not being complete
        try:
            resp = reply.decode("UTF-8")
        except UnicodeDecodeError:
            resp = reply.decode("latin-1")
        except AttributeError:
            resp = reply
        lynes = resp.split('\n')
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            if len(lynes) == 1 and not lynes[0]:
                logger.warn("Empty response")
            else:
                logger.debug("Received %s lines" % len(lynes))
        for lyne in lynes:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                if self.nick in lyne:
                    logger.debug(lyne)
            if 'PING ' in lyne:
                self.pong(lyne)
            elif 'VERSION ' in lyne:
                self.version()
        return lynes


def ircConnect(server, port, channel, botnick, botpass):
    if lazylibrarian.providers.ProviderIsBlocked(server):
        logger.warn("%s is blocked" % channel)
        return None
    retries = 0
    maxretries = 3
    irc = IRC()
    e = ''
    while retries < maxretries:
        try:
            if '114' in str(e):  # already in progress
                time.sleep(10)
            else:
                irc.connect(server, port, botnick, botpass)
            while retries < maxretries:
                try:
                    lynes = irc.get_response()
                    for lyne in lynes:
                        if "All connections in use" in lyne:
                            logger.warn(lyne)
                            return None

                        if botnick in lyne:
                            if "433" in lyne:  # ERR_NICKNAMEINUSE
                                botnick += '_'
                                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                                    logger.debug("Trying NICK %s" % botnick)
                                break
                            elif "Welcome to" in lyne:
                                irc.join(channel)
                                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                                    logger.debug("Sent JOIN %s" % channel)
                                return irc
                except socket.timeout:
                    logger.warn("Reply timed out")
                    retries += 1
        except Exception as e:
            logger.error(e)
            retries += 1
    return None


def ircSearch(irc, server, channel, searchstring, cmd=":@search"):
    if lazylibrarian.providers.ProviderIsBlocked(server):
        msg = "%s is blocked" % channel
        logger.warn(msg)
        return False, msg

    cacheLocation = os.path.join(lazylibrarian.CACHEDIR, "IRCCache")
    if searchstring:
        myhash = md5_utf8(server + channel + searchstring)
    else:
        myhash = md5_utf8(server + channel + cmd)
    valid_cache = False
    hashfilename = os.path.join(cacheLocation, myhash + ".irc")
    expiry = 2 * 24 * 60 * 60  # expire cache after this many seconds

    if os.path.isfile(hashfilename):
        cache_modified_time = os.stat(hashfilename).st_mtime
        time_now = time.time()
        if cache_modified_time < time_now - expiry:
            # Cache entry is too old, delete it
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
                logger.debug("Expiring %s" % myhash)
            os.remove(hashfilename)
        else:
            valid_cache = True

    if valid_cache:
        lazylibrarian.CACHE_HIT = int(lazylibrarian.CACHE_HIT) + 1
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
            logger.debug("CacheHandler: Returning CACHED response %s for %s" % (hashfilename,
                                                                                searchstring))
        with open(hashfilename, "rb") as cachefile:
            data = cachefile.read()
        return hashfilename, data

    lazylibrarian.CACHE_MISS = int(lazylibrarian.CACHE_MISS) + 1
    received_data = b''
    status = ""
    cmd_sent = time.time()
    last_cmd = ''
    last_search_cmd = ''
    last_search_time = 0
    retries = 0
    maxretries = 3
    abortafter = 60
    ratelimit = 2
    pingcheck = 0

    while status != "finished":
        try:
            lynes = irc.get_response()
        except socket.timeout:
            logger.warn("Timed out, status [%s]" % status)
            retries += 1
            lynes = ''
            if status == "":
                logger.debug("Rejoining %s" % channel)
                irc.join(channel)
                cmd_sent = time.time()
                last_cmd = 'socket timeout re-join %s' % channel
            if status == "waiting":
                new_cmd = cmd + " " + searchstring
                if new_cmd == last_search_cmd:
                    # about to repeat search, ensure not too soon
                    pause_until = last_search_time + abortafter
                    pause = int(pause_until - time.time())
                    if pause > 0:
                        logger.debug("Waiting %ssec before resending search" % pause)
                        while pause_until > time.time():
                            _ = irc.get_response()  # listen and handle ping
                irc.send(server, channel, new_cmd)
                cmd_sent = time.time()
                last_cmd = "socket timeout resend %s" % new_cmd
                logger.debug(new_cmd)
                last_search_time = cmd_sent
                last_search_cmd = new_cmd
        except socket.error as e:
            logger.error("Socket error: %s" % str(e))
            # if disconnected need to reconnect and rejoin channel
            return False, str(e)

        for lyne in lynes:
            if len(lynes) == 1 and not lyne:
                if last_cmd:
                    logger.debug("Empty response to %s" % last_cmd)
                    time.sleep(ratelimit)
                    retries += 1
                else:
                    status = ""
                    irc.join(channel)
                    last_cmd = 'Empty response, rejoin %s' % channel
                    cmd_sent = time.time()

            elif 'KICK' in lyne:
                logger.debug("Kick: %s" % lyne.rsplit(':', 1)[1])
                lazylibrarian.providers.BlockProvider(server, "Kick", 600)
                return False, "Kick"

            elif '404' in lyne:  # cannot send to channel
                status = ""
                logger.debug("[%s] Rejoining %s" % (
                    lyne.rsplit(':', 1)[1], channel))
                time.sleep(ratelimit)
                irc.join(channel)
                last_cmd = '404 rejoin %s' % channel
                cmd_sent = time.time()

            elif "PRIVMSG" in lyne and channel in lyne and "hello" in lyne:
                irc.send(server, channel, "Hello!")
                logger.debug("Sent HELLO")

            if status == "joined":
                new_cmd = cmd + " " + searchstring
                if new_cmd == last_search_cmd:
                    # about to repeat search, ensure not too soon
                    pause_until = last_search_time + abortafter
                    pause = int(pause_until - time.time())
                    if pause > 0:
                        logger.debug("Waiting %ssec before resending search" % pause)
                        while pause_until > time.time():
                            _ = irc.get_response()  # listen and handle ping
                irc.send(server, channel, new_cmd)
                cmd_sent = time.time()
                last_cmd = new_cmd
                status = "waiting"
                logger.debug("Asking %s for %s" % (cmd, searchstring))
                last_search_cmd = new_cmd
                last_search_time = cmd_sent

            elif status == "waiting":
                if len(lyne.split("matches")) > 1:
                    titlefuzz = fuzz.partial_ratio(lyne, searchstring)
                    logger.debug("fuzz %s%% for %s" % (titlefuzz, searchstring))
                    if titlefuzz >= lazylibrarian.CONFIG['NAME_RATIO']:
                        res = lyne.split("matches")[0].split()
                        try:
                            matches = int(res[-1])
                        except ValueError:
                            matches = 0
                        logger.debug("Found %d matches" % matches)
                        if not matches:
                            status = "finished"
                elif 'Request Denied' in lyne:
                    titlefuzz = fuzz.partial_ratio(lyne, searchstring)
                    logger.debug("fuzz %s%% for %s" % (titlefuzz, searchstring))
                    if titlefuzz >= lazylibrarian.CONFIG['NAME_RATIO']:
                        try:
                            msg = lyne.split("PRIVMSG")[1].split('\n')[0]
                        except IndexError:
                            msg = lyne
                        logger.warn("Request Denied by %s" % cmd)
                        logger.debug(msg)
                        # irc.part(channel)
                        return False, msg

            elif channel in lyne and status == "":
                status = "joined"
                logger.debug("Joined %s" % channel)

            if "PRIVMSG" in lyne and "DCC SEND" in lyne:
                res = lyne.split("DCC SEND")[1].split('\n')[0].split()
                size = res[-1]
                peer_port = res[-2]
                peer_address = res[-3]
                filename = ' '.join(res[:-3])
                filename = filename.strip('"')
                logger.debug("%s %s %s %s" % (filename, ip_numstr_to_quad(peer_address), peer_port, size))
                filesize = int(size.strip('\x01'))

                peeraddress = socket.gethostbyname(peer_address)
                peerport = int(peer_port)
                peersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peersocket.settimeout(30)
                try:
                    peersocket.connect((peeraddress, peerport))
                    status = "connected"
                    logger.debug("Connected to %s" % peeraddress)
                except socket.error as x:
                    logger.warn("Couldn't connect to socket: %s" % x)

                if status == "connected":
                    while len(received_data) < filesize:
                        try:
                            new_data = peersocket.recv(2 ** 14)
                        except socket.error:
                            # The server hung up.
                            logger.warn("Connection reset by peer")
                            new_data = ''
                            status = ""
                            retries += 1
                        if not new_data:
                            # Read nothing: connection must be down.
                            logger.warn("Connection reset by peer")
                            status = ""
                            retries += 1
                        else:
                            received_data += new_data
                            if len(received_data) >= filesize:
                                peersocket.close()
                                logger.debug("Completed, got %s" % len(received_data))
                                # status = "finished"
                                # irc.part(channel)
                                logger.debug("CacheHandler: Storing %s" % hashfilename)
                                with open(hashfilename, "wb") as cachefile:
                                    cachefile.write(received_data)
                                return hashfilename, received_data
                            else:
                                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                                    logger.debug("Got %s of %s" % (len(received_data), size))
                                peersocket.send(struct.pack("!I", len(received_data)))
                        if retries > maxretries:
                            msg = "Aborting download, too many retries"
                            logger.warn(msg)
                            return False, msg

                        # check every few seconds so we don't miss a ping from irc server
                        if time.time() > pingcheck + 10:
                            try:
                                # read and handle any PING, discard anything else
                                _ = irc.get_response()
                            except socket.timeout:
                                logger.warn("Timed out on main channel")
                            pingcheck = time.time()

        if time.time() - cmd_sent > abortafter:
            logger.warn("No response in %ssec from %s" % (abortafter, last_cmd))
            status = ""
            retries += 1
        if retries > maxretries:
            msg = "Aborting, too many retries"
            logger.warn(msg)
            return False, msg

    logger.debug("CacheHandler: Storing %s" % hashfilename)
    with open(hashfilename, "wb") as cachefile:
        cachefile.write(received_data)
    return hashfilename, received_data


def ircResults(provider, fname, data, irc=None):
    # Open the zip file, extract the txt
    # for each line that starts with !
    # user is first word
    # filename is rest up to ::INFO:: or "\r"
    # if ::INFO:: in line, following word is size including unit
    # if \r- in line last two words are size/unit
    results = []
    tor_date = today()
    fname = replace_all(fname, namedic)
    outfile = os.path.join(lazylibrarian.CACHEDIR, fname)
    if not os.path.isfile(outfile):
        with open(outfile, "wb") as f:
            f.write(data)
        logger.debug("Written %s" % outfile)
    else:
        if zipfile.is_zipfile(outfile):
            data = zipfile.ZipFile(outfile)
        elif tarfile.is_tarfile(outfile):
            data = tarfile.TarFile(outfile)
        elif lazylibrarian.UNRARLIB == 1 and lazylibrarian.RARFILE.is_rarfile(outfile):
            data = lazylibrarian.RARFILE.RarFile(outfile)
        elif lazylibrarian.UNRARLIB == 2:
            # noinspection PyBroadException
            try:
                data = lazylibrarian.RARFILE(outfile)
            except Exception:
                data = None  # not a rar archive
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
                logger.error("No results.txt found in %s" % outfile)
    if results:
        if irc:
            retries = 0
            maxretries = 8
            userlist = []
            for item in results:
                userlist.append(item['tor_url'].lstrip('!'))
            userlist = set(userlist)
            users = ' '.join(userlist)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug("Checking for %s online" % len(userlist))
            irc.ison(users)
            online = ''
            while not online:
                try:
                    lynes = irc.get_response()
                except socket.timeout:
                    logger.warn("Timed out waiting for ison response")
                    lynes = []

                for lyne in lynes:
                    if '303' in lyne:  # RPL_ISON
                        res = lyne.split('303')[1]
                        if ':' in res:
                            res = res.split(':')[1]
                        else:
                            logger.warn("Unexpected ISON reply: [%s]" % lyne)
                        online = res.split()
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                            logger.debug("Found %s online" % len(online))
                        if len(userlist) == len(online):
                            return results

                retries += 1
                if retries >= maxretries:
                    msg = "Ignoring ison, too many retries"
                    logger.warn(msg)
                    return results

            oldresults = results
            results = []
            stripped = 0
            offline = userlist.difference(online)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug("Offline: %s" % ' '.join(offline))
            for entry in oldresults:
                if entry['tor_url'].lstrip('!') in offline:
                    stripped += 1
                else:
                    results.append(entry)
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug("Stripped %s results from %s users not online" %
                             (stripped, len(offline)))
    return results
