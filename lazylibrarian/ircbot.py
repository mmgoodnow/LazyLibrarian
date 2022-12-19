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
import zipfile

import lazylibrarian
from lazylibrarian import logger, database
from lazylibrarian.configtypes import ConfigDict
from lazylibrarian.formatter import today, size_in_bytes, make_bytestr, md5_utf8, check_int
from lazylibrarian.common import path_isfile, syspath, remove


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


def valid_ip(s):
    a = s.split('.')
    if len(a) != 4:
        return False
    for x in a:
        if not x.isdigit():
            return False
        i = int(x)
        if i < 0 or i > 255:
            return False
    return True


class IRC:

    irc = socket.socket()

    def __init__(self):
        self.email = "https://gitlab.com/LazyLibrarian/LazyLibrarian"
        self.name = 'eBook ircBot'
        db = database.DBConnection()
        res = db.match('SELECT name,email from users where username="Admin" COLLATE NOCASE')
        if res:
            self.email = res['email']
            if not self.email:
                self.email = lazylibrarian.CONFIG['ADMIN_EMAIL']
            if not self.email:
                logger.warn("No admin email, using default")
            if res['name']:
                self.name = res['name']
            else:
                logger.warn("No admin name, using default")
        else:
            logger.warn("No admin user, expect problems")

        self.ver = "eBook ircBot version 2022-05-15 (" + self.email + ")"
        self.server = ""
        self.nick = ""
        # Define the socket
        self.irc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.irc.settimeout(10)

    def send(self, server, channel, msg):
        # Transfer data
        try:
            if msg.startswith(':'):
                msg = msg[1:]
            send_string = 'PRIVMSG %s :%s\n' % (channel, msg)
            logger.debug(send_string)
            self.irc.send(make_bytestr(send_string))
        except Exception as e:
            logger.debug("Exception sending %s" % msg)
            logger.debug(str(e))
            lazylibrarian.providers.block_provider(server, msg, 600)

    def ison(self, msg):
        try:
            self.irc.send(make_bytestr("ISON " + msg + "\n"))
        except Exception as e:
            logger.debug("Exception sending %s" % msg)
            logger.debug(str(e))
            lazylibrarian.providers.block_provider(self.server, msg, 600)

    def pong(self, msg):
        reply = msg.replace('PING', 'PONG')
        logger.debug(reply)
        try:
            self.irc.send(make_bytestr(reply + "\n"))
        except Exception as e:
            logger.debug("Exception sending %s" % msg)
            logger.debug(str(e))
            lazylibrarian.providers.block_provider(self.server, msg, 600)

    def version(self):
        reply = "VERSION " + self.ver
        logger.debug(reply)
        try:
            self.irc.send(make_bytestr(reply + "\n"))
        except Exception as e:
            logger.debug("Exception sending %s" % reply)
            logger.debug(str(e))
            lazylibrarian.providers.block_provider(self.server, reply, 600)

    def join(self, channel):
        try:
            self.irc.send(make_bytestr("JOIN " + channel + "\n"))
        except Exception as e:
            msg = "Exception sending JOIN %s" % channel
            logger.debug(msg)
            logger.debug(str(e))
            lazylibrarian.providers.block_provider(self.server, msg, 600)

    def leave(self, provider: ConfigDict):
        if provider.get_connection():
            self.irc.send(make_bytestr("PART " + provider['CHANNEL'] + " :Bye\n"))
            pause_until = time.time() + 2
            while pause_until > time.time():
                _ = self.get_response('leaving pause')  # listen and handle ping
            self.irc.send(make_bytestr("QUIT\n"))
            provider.set_connection(None)

    def connect(self, server, port, botnick="", botpass=""):
        # Connect to the server
        logger.debug("Connecting to: " + server)
        logger.debug(self.ver)
        try:
            self.irc.connect((server, port))
            self.server = server
            self.nick = botnick
            if botnick:
                logger.debug("Sending auth for %s" % botnick)
                # Perform user authentication, username, hostname, servername, realname
                self.irc.send(make_bytestr("USER " + botnick + " " + botnick + " " + botnick + " :%s\n" % self.name))
                self.irc.send(make_bytestr("NICK " + botnick + "\n"))
                if botpass and self.email:
                    logger.debug("Sending nickserv")
                    self.irc.send(make_bytestr("NICKSERV REGISTER " + botpass + " " + self.email + "\n"))
                    time.sleep(2)
                    self.irc.send(make_bytestr("NICKSERV IDENTIFY " + botpass + "\n"))
                    time.sleep(2)
        except Exception:
            raise

    def get_response(self, why=''):
        # Get the response
        try:
            reply = self.irc.recv(2040)
        except socket.timeout:
            logger.debug("response timeout %s" % why)
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
                logger.warn("Empty response %s" % why)
        for lyne in lynes:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                if lyne:
                    logger.debug(lyne)
            elif "NOTICE" in lyne:
                logger.debug(lyne)
            if lyne.startswith('PING '):
                self.pong(lyne)
            elif lyne.startswith('VERSION '):
                self.version()
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug("%s line response to %s" % (len(lynes), why))
        return lynes


def irc_connect(provider: ConfigDict, retries=10):
    if lazylibrarian.providers.provider_is_blocked(provider['SERVER']):
        logger.warn("%s is blocked" % provider['SERVER'])
        return None

    irc = provider.get_connection()
    if irc:
        logger.debug("Trying existing connection to %s" % provider['SERVER'])
        try:
            res = irc.get_response("existing connection")
            if res:
                irc.join(provider['CHANNEL'])
                _ = irc.get_response("join")
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                    logger.debug("Sent JOIN %s" % provider['CHANNEL'])
                return irc
        except Exception as e:
            logger.debug("Existing connection failed: %s" % str(e))
            provider.set_connection(None)
            # if the attempt to join failed it will block us,
            # and we were not blocked before the attempt
            for entry in lazylibrarian.PROVIDER_BLOCKLIST:
                if entry["name"] == provider['SERVER']:
                    lazylibrarian.PROVIDER_BLOCKLIST.remove(entry)
    retried = 0
    irc = IRC()
    e = ''
    botnick = provider['BOTNICK']
    while retried < retries:
        if ' 114 ' in str(e):  # already in progress
            logger.debug(str(e))
            time.sleep(5)
        else:
            irc.connect(provider['SERVER'], 6667, botnick, provider['BOTPASS'])
        while retried < retries:
            try:
                lynes = irc.get_response("connect, retried=%s" % retried)
                for lyne in lynes:
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms and lyne:
                        logger.debug(lyne)
                    if "All connections in use" in lyne:
                        logger.warn(lyne)
                        return None

                    if botnick in lyne:
                        if "433" in lyne:  # ERR_NICKNAMEINUSE
                            botnick += '_'
                            logger.debug("Trying NICK %s" % botnick)
                            break
                        elif "001" in lyne:  # Welcome
                            logger.debug("Got Welcome message")
                            # wait for welcome messages to finish...
                            logger.debug("Waiting 5sec before joining %s" % provider['CHANNEL'])
                            pause_until = time.time() + 5
                            while pause_until > time.time():
                                _ = irc.get_response('welcome pause')  # listen and handle ping
                            irc.join(provider['CHANNEL'])
                            _ = irc.get_response("join")
                            provider.set_connection(irc)
                            return irc
                retried += 1  # welcome not found yet
            except socket.timeout:
                logger.warn("Reply timed out")
                retried += 1
            except Exception as e:
                logger.error(str(e))
                return None
    logger.debug("Connect failed")
    return None


def irc_search(provider: ConfigDict, searchstring, cmd="", cache=True, retries=10):
    if lazylibrarian.providers.provider_is_blocked(provider['SERVER']):
        msg = "%s is blocked" % provider['SERVER']
        logger.warn(msg)
        return '', msg

    if not cmd:
        cmd = provider['SEARCH']

    if cache:
        cache_location = os.path.join(lazylibrarian.CACHEDIR, "IRCCache")
        if searchstring:
            myhash = md5_utf8(provider['SERVER'] + provider['CHANNEL'] + searchstring)
        else:
            myhash = md5_utf8(provider['SERVER'] + provider['CHANNEL'] + cmd)
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
                if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
                    logger.debug("Expiring %s" % myhash)
                remove(hashfilename)
            else:
                valid_cache = True

        if valid_cache:
            lazylibrarian.CACHE_HIT = int(lazylibrarian.CACHE_HIT) + 1
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_cache:
                logger.debug("CacheHandler: Returning CACHED response %s for %s" % (hashfilename,
                                                                                    searchstring))
            with open(syspath(hashfilename), "rb") as cachefile:
                data = cachefile.read()
            return hashfilename, data

        lazylibrarian.CACHE_MISS = int(lazylibrarian.CACHE_MISS) + 1
    else:
        hashfilename = ''

    received_data = b''
    status = ""
    cmd_sent = time.time()
    last_cmd = ''
    last_search_cmd = ''
    retried = 0
    abortafter = 90
    ratelimit = 2
    pingcheck = 0
    filename = ''

    irc = provider.get_connection()
    if not irc:
        irc = irc_connect(provider)

    if not irc:
        provider.set_connection(None)
        return '', "Failed to connect to %s" % provider['SERVER']

    while status != "finished":
        try:
            lynes = irc.get_response("irc_search %s" % status)
        except socket.timeout:
            logger.warn("Timed out, status [%s]" % status)
            retried += 1
            lynes = ''
            if status == "":
                logger.debug("Rejoining %s" % provider['CHANNEL'])
                try:
                    irc.join(provider['CHANNEL'])
                except Exception as e:
                    logger.debug(str(e))
                    return '', str(e)
                cmd_sent = time.time()
                last_cmd = 'socket timeout re-join %s' % provider['CHANNEL']
            if status == "waiting":
                new_cmd = cmd + " " + searchstring
                if new_cmd == last_search_cmd:
                    # about to repeat search, ensure not too soon
                    pause_until = provider.get_int('LAST_SEARCH_TIME') + abortafter
                    pause = int(pause_until - time.time())
                    if pause > 0:
                        logger.debug("Waiting %ssec before resending search" % pause)
                        while pause_until > time.time():
                            _ = irc.get_response('re-send pause')  # listen and handle ping
                irc.send(provider['SERVER'], provider['CHANNEL'], new_cmd)
                cmd_sent = time.time()
                last_cmd = "socket timeout resend %s" % new_cmd
                logger.debug(new_cmd)
                provider.set_int('LAST_SEARCH_TIME', int(time.time()))
                last_search_cmd = new_cmd
        except socket.error as e:
            logger.error("Socket error: %s" % str(e))
            # if disconnected need to reconnect and rejoin channel
            return '', str(e)

        if not lynes or len(lynes) == 1 and not lynes[0]:
            if last_cmd:
                logger.debug("Empty response to %s" % last_cmd)
                time.sleep(ratelimit)
            else:
                status = ""
                try:
                    irc.join(provider['CHANNEL'])
                except Exception as e:
                    logger.debug(str(e))
                    return '', str(e)
                last_cmd = 'Empty response, rejoin %s' % provider['CHANNEL']
                cmd_sent = time.time()
            retried += 1

        for lyne in lynes:
            if 'KICK' in lyne:
                msg = "Kick: %s" % lyne.rsplit(':', 1)[1]
                logger.debug(msg)
                lazylibrarian.providers.block_provider(provider['SERVER'], "Kick", 600)
                return '', msg

            elif ' 474 ' in lyne:  # banned
                msg = "Banned: %s" % lyne.rsplit(':', 1)[1]
                logger.debug(msg)
                lazylibrarian.providers.block_provider(provider['SERVER'], "Banned", 24*60*60)
                return '', msg

            elif ' 404 ' in lyne:  # cannot send to channel
                status = ""
                logger.debug("[%s] Rejoining %s" % (
                    lyne, provider['CHANNEL']))
                time.sleep(ratelimit)
                irc.join(provider['CHANNEL'])
                last_cmd = '404 rejoin %s' % provider['CHANNEL']
                cmd_sent = time.time()

            elif "PRIVMSG" in lyne and provider['CHANNEL'] in lyne and "hello" in lyne:
                irc.send(provider['SERVER'], provider['CHANNEL'], "Hello!")
                logger.debug("Sent HELLO")

            if status == "joined":
                new_cmd = cmd + " " + searchstring
                if new_cmd == last_search_cmd:
                    # about to repeat search, ensure not too soon
                    pause_until = provider.get_int('LAST_SEARCH_TIME') + abortafter
                    pause = int(pause_until - time.time())
                    if pause > 0:
                        logger.debug("Waiting %ssec before resending search" % pause)
                        while pause_until > time.time():
                            _ = irc.get_response('join/resend pause')  # listen and handle ping
                else:
                    # just joined, wait for welcome messages to finish...
                    logger.debug("Waiting 5sec before sending search")
                    pause_until = time.time() + 5
                    while pause_until > time.time():
                        _ = irc.get_response('joined pause')  # listen and handle ping

                logger.debug("Sending %s" % new_cmd)
                irc.send(provider['SERVER'], provider['CHANNEL'], new_cmd)
                last_cmd = new_cmd
                status = "waiting"
                last_search_cmd = new_cmd
                provider.set_int('LAST_SEARCH_TIME', int(time.time()))

            elif status == "waiting":
                if len(lyne.split("matches")) > 1:
                    res = lyne.split("matches")[0].split()
                    try:
                        matches = int(res[-1])
                        logger.debug("Found %d matches" % matches)
                        if not matches:
                            return '', 'No matches'
                    except ValueError:
                        return '', 'ValueError: %s' % lyne
                    status = "pending"
                elif 'search' in lyne and 'accepted' in lyne:
                    logger.debug("Search accepted by %s" % cmd)
                elif 'Request Accepted' in lyne:
                    logger.debug("Request accepted by %s" % cmd)
                elif 'Request Denied' in lyne or 'Search denied' in lyne:
                    try:
                        msg = lyne.split("PRIVMSG")[1].split('\n')[0]
                    except IndexError:
                        msg = lyne
                    if 'Request Denied' in lyne:
                        logger.warn("Request Denied by %s" % cmd)
                    else:
                        logger.warn("Search Denied by %s" % cmd)
                    logger.debug(msg)
                    irc.leave(provider)
                    if 'you already have' not in msg.lower():
                        return '', msg

            elif provider['CHANNEL'] in lyne and status == "":
                status = "joined"
                logger.debug("Joined %s" % provider['CHANNEL'])

            if "PRIVMSG" in lyne and "DCC SEND" in lyne:
                res = lyne.split("DCC SEND")[1].split('\n')[0].split()
                size = res[-1]
                peer_port = res[-2]
                peer_address = res[-3]
                filename = ' '.join(res[:-3])
                filename = filename.strip('"')
                try:
                    if valid_ip(peer_address):
                        peeraddress = peer_address
                    elif peer_address.isdigit():
                        peeraddress = ip_numstr_to_quad(peer_address)
                    else:
                        peeraddress = socket.gethostbyname(peer_address)
                except Exception as e:
                    logger.debug("Failed to convert peer_address [%s] %s" % (peer_address, str(e)))
                    peeraddress = peer_address

                logger.debug("%s %s %s %s" % (filename, peeraddress, peer_port, size))
                filesize = int(size.strip('\x01'))

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
                            retried += 1
                        if not new_data:
                            # Read nothing: connection must be down.
                            logger.warn("Connection reset by peer")
                            status = ""
                            retried += 1
                        else:
                            received_data += new_data
                            if len(received_data) >= filesize:
                                peersocket.close()
                                logger.debug("Completed, got %s" % len(received_data))
                                status = "finished"
                            else:
                                if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                                    logger.debug("Got %s of %s" % (len(received_data), filesize))
                                peersocket.send(struct.pack("!I", len(received_data)))
                        if status != "finished":
                            if retried > retries:
                                msg = "Aborting download, too many retries"
                                logger.warn(msg)
                                irc.leave(provider)
                                return '', msg

                            # check every few seconds so we don't miss a ping from irc server
                            if time.time() > pingcheck + 10:
                                try:
                                    # read and handle any PING, discard anything else
                                    _ = irc.get_response("pingcheck")
                                except socket.timeout:
                                    logger.warn("Timed out on main channel")
                                pingcheck = time.time()

        if status != "finished":
            if time.time() - cmd_sent > abortafter:
                msg = "No response in %ssec from %s" % (abortafter, last_cmd)
                logger.warn(msg)
                irc.leave(provider)
                return '', msg

    if cache:
        # still cache if empty response (no matches)
        # so we don't need to ask again
        logger.debug("CacheHandler: Storing %s" % hashfilename)
        with open(syspath(hashfilename), "wb") as cachefile:
            cachefile.write(received_data)
        return hashfilename, received_data
    return filename, received_data


def irc_leave(provider: ConfigDict):
    if provider.get_connection():
        provider.get_connection().leave(provider)


def irc_results(provider: ConfigDict, fname, retries=5):
    # Open the zip file, extract the txt
    # for each line that starts with !
    # user is first word
    # filename is rest up to ::INFO:: or "\r"
    # if ::INFO:: in line, following word is size including unit
    # if \r- in line last two words are size/unit
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

    if results:
        irc = provider.get_connection()
        if irc:
            retried = 0
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
                    lynes = irc.get_response("ison")
                except socket.timeout:
                    logger.warn("Timed out waiting for ison response")
                    lynes = []

                for lyne in lynes:
                    if ' 303 ' in lyne:  # RPL_ISON
                        res = lyne.split(' 303 ')[1]
                        if ':' in res:
                            res = res.rsplit(':')[1]
                        else:
                            logger.warn("Unexpected ISON reply: [%s]" % lyne)
                        online = res.split()
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                            logger.debug("Found %s online" % len(online))
                        if len(userlist) == len(online):
                            return results
                        elif not len(online):
                            return []

                retried += 1
                if retried >= retries:
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
        else:
            logger.debug("Not checking online status for results")
    return results
