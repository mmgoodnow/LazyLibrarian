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

import socket
import time
import struct
import os
import tarfile
from lib.six import PY2
import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.formatter import today, size_in_bytes, makeBytestr
try:
    import zipfile
except ImportError:
    if PY2:
        import lib.zipfile as zipfile
    else:
        import lib3.zipfile as zipfile


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
        # Define the socket
        self.irc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.irc.settimeout(10)

    def send(self, channel, msg):
        # Transfer data
        self.irc.send(makeBytestr("PRIVMSG " + channel + " " + msg + "\n"))

    def join(self, channel):
        self.irc.send(makeBytestr("JOIN " + channel + "\n"))

    def part(self, channel):
        self.irc.send(makeBytestr("PART " + channel + " :Bye\n"))

    def connect(self, server, port, botnick, botpass):
        # Connect to the server
        logger.debug("Connecting to: " + server)
        try:
            email = lazylibrarian.CONFIG['ADMIN_EMAIL']
            self.irc.connect((server, port))
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
        time.sleep(1)
        # Get the response
        reply = self.irc.recv(2040)
        # should really put the data in a queue/buffer so we can handle split lines
        # for now we'll just split on newline and ignore the last line not being complete
        try:
            resp = reply.decode("UTF-8")
        except UnicodeDecodeError:
            resp = reply.decode("latin-1")
        if resp.find('PING :') != -1:
            self.irc.send(makeBytestr('PONG ' + resp.split('PING :')[1]))
            logger.debug("Sent PONG %s" % resp.split('PING :')[1])
        return resp


def ircConnect(server, port, channel, botnick, botpass):
    retries = 0
    maxretries = 3
    irc = IRC()
    while retries < maxretries:
        try:
            irc.connect(server, port, botnick, botpass)
            while retries < maxretries:
                try:
                    text = irc.get_response()
                    lynes = text.split('\n')
                    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                        logger.debug("Received %s lines" % len(lynes))
                    for lyne in lynes:
                        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                            logger.debug(lyne)
                        if botnick in lyne:
                            if "already in use" in lyne:
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


def ircSearch(irc, channel, searchstring, cmd=":@search"):
    filename = ""
    received_data = b''
    status = ""
    cmd_sent = time.time()
    retries = 0
    maxretries = 3
    abortafter = 30
    text = ''

    # if cmd.startswith('!') can we check if they are online?
    while status != "finished":
        try:
            text = irc.get_response()
        except socket.timeout:
            logger.warn("Timed out, status [%s]" % status)
            retries += 1

            if status == "":
                logger.debug("Rejoining %s" % channel)
                irc.join(channel)
                cmd_sent = time.time()
            if status == "waiting":
                irc.send(channel, cmd + " " + searchstring)
                cmd_sent = time.time()
                logger.debug("Resent %s" % cmd + " " + searchstring)

        lynes = text.split('\n')
        if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
            logger.debug("[%s] Received %s lines" % (status, len(lynes)))
        for lyne in lynes:
            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                logger.debug(lyne)
            if channel in lyne and status == "":
                status = "joined"
                logger.debug("Joined %s" % channel)

            if "PRIVMSG" in lyne and channel in lyne and "hello" in lyne:
                irc.send(channel, "Hello!")
                logger.debug("Sent HELLO")

            if status == "joined":
                irc.send(channel, cmd + " " + searchstring)
                cmd_sent = time.time()
                status = "waiting"
                logger.debug("Asking %s for %s" % (cmd, searchstring))

            if status == "waiting":
                if searchstring in lyne:
                    if len(lyne.split("matches")) > 1:
                        res = lyne.split("matches")[0].split()
                        try:
                            matches = int(res[-1])
                        except ValueError:
                            matches = 0
                        logger.debug("Found %d matches" % matches)
                        if not matches:
                            status = "finished"
                    if 'Request Denied' in lyne:
                        try:
                            msg = lyne.split("PRIVMSG")[1].split('\n')[0]
                        except IndexError:
                            msg = lyne
                        logger.warn("Request Denied by %s" % cmd)
                        logger.debug(msg)
                        # irc.part(channel)
                        return False, msg

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
                            if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
                                logger.debug("Got %s of %s" % (len(received_data), size))
                            if len(received_data) >= filesize:
                                peersocket.close()
                                logger.debug("Got %s of %s" % (len(received_data), size))
                                status = "finished"
                                # irc.part(channel)
                            else:
                                peersocket.send(struct.pack("!I", len(received_data)))

        if time.time() - cmd_sent > abortafter:
            logger.warn("No response from %s" % cmd)
            status = ""
            retries += 1
        if retries > maxretries:
            msg = "Aborting, too many retries"
            logger.warn(msg)
            return False, msg
    return filename, received_data


def ircResults(provider, fname, data):
    # Open the zip file, extract the txt
    # for each line that starts with !
    # user is first word
    # filename is rest up to ::INFO:: or linefeed
    # if ::INFO:: in line, following word is size including unit
    # if next line starts with - last two words are size/unit
    results = []
    tor_date = today()
    outfile = os.path.join(lazylibrarian.CACHEDIR, fname)
    with open(outfile, "wb") as f:
        f.write(data)
    logger.debug("Written %s" % outfile)

    if outfile:
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
            r = b''
            for member in data.namelist():
                if '.txt' in member.lower():
                    r = data.read(member)
                    break
            lynes = r.split(b'\n')
            ln = 0
            while ln < len(lynes):
                filename = ''
                size = 0
                user = ''
                lyne = lynes[ln].decode('utf-8').strip('\r')
                if lyne.startswith('!'):
                    user, newlyne = lyne.split(' ', 1)
                    if '::INFO::' in newlyne:
                        filename, size = newlyne.split('::INFO::', 1)
                    elif ln + 1 < len(lynes):
                        filename = newlyne
                        newlyne = lynes[ln + 1].decode('utf-8').strip('\r')
                        if newlyne.startswith('-'):
                            words = newlyne.strip('\r').split()
                            size = words[-2]
                            units = words[-1]
                            size = size + units
                        else:
                            filename = ''
                    else:
                        filename = newlyne
                        size = '0'
                ln += 1
                if filename:
                    filename = filename.strip()
                    size = size_in_bytes(size)

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
    os.remove(outfile)
    print(results)
    return results


"""
## IRC Config
server = "eu.undernet.org"
channel = "#bookz"
server = "irc.irchighway.net"
channel = "#ebooks"
botnick = "lazylib0001"
botpass = "1htrf19"
"""