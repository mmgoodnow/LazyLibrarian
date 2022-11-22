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
#   Handle basic, anonymous telemetry gathering for LL, to help prioritise
#   future development.

import datetime
import json
from collections import defaultdict
from lazylibrarian import config

class LazyTelemetry(object):
    """Handles basic telemetry gathering for LazyLibrarian, helping
    developers prioritise future development"""

    _instance = None
    _data = {
        "server": {},
        "config": {},
        "usage": defaultdict(int),
    }

    # Singleton; no __init__ method
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LazyTelemetry, cls).__new__(cls)
            # More initialization goes initialization here
            cls._boottime = datetime.datetime.now()
        return cls._instance

    def ensure_server_id(self, _config):
        """ Get unique, anonymous ID for this installation """
        server = self._data["server"]
        if "id" not in server:
            id = _config['SERVER_ID'] if 'SERVER_ID' in _config else None
            if not id:
                import uuid
                id = uuid.uuid4().hex
                _config['SERVER_ID'] = id
            server["id"] = id

        return server["id"]

    def clear_id(self, _config):
        """ Clear the server ID. Only needed for testing purposes. """
        server = self._data["server"]
        del server['id']
        _config['SERVER_ID'] = ''

    def get_server_telemetry(self):
        return self._data["server"]

    def get_config_telemetry(self):
        return self._data["config"]

    def get_usage_telemetry(self):
        return self._data["usage"]

    def set_install_data(self, _config):
        """ Update telemetry with data bout the installation """
        self.ensure_server_id(_config) # Make sure it has an ID
        server = self.get_server_telemetry()
        up = datetime.datetime.now() - self._boottime
        server["uptime_seconds"] = round(up.total_seconds())
        server["install_type"] = _config['INSTALL_TYPE']
        server["version"] = _config['CURRENT_VERSION']

    def set_config_data(self, _config):
        import lazylibrarian # To get access to the _PROV objects

        cfg_telemetry = self.get_config_telemetry()
        ## Idea: Could add Telemetry Type column to each config item

        # Record whether particular on/off features are configured
        for key in [
            # General
            'USER_ACCOUNTS', 'EBOOK_TAB', 'COMIC_TAB', 'SERIES_TAB', 
            'AUDIO_TAB', 'MAG_TAB', 'SHOW_GENRES', 
            'BOOK_IMG', 'MAG_IMG', 'COMIC_IMG', 'AUTHOR_IMG',
            'API_ENABLED',
            # Downloaders
            'NZB_DOWNLOADER_SABNZBD', 'NZB_DOWNLOADER_NZBGET', 'USE_SYNOLOGY', 
            'NZB_DOWNLOADER_BLACKHOLE',
            'TOR_DOWNLOADER_DELUGE', 'TOR_DOWNLOADER_TRANSMISSION', 
            'TOR_DOWNLOADER_RTORRENT', 'TOR_DOWNLOADER_UTORRENT', 
            'TOR_DOWNLOADER_QBITTORRENT', 'TOR_DOWNLOADER_BLACKHOLE',
            # Providers
            # Processing
            'CALIBRE_USE_SERVER', 'OPF_TAGS',
            # Notifiers
            'USE_TWITTER', 'USE_BOXCAR', 'USE_PUSHBULLET', 'USE_PUSHOVER',
            'USE_ANDROIDPN', 'USE_TELEGRAM', 'USE_PROWL', 'USE_GROWL',
            'USE_SLACK', 'USE_CUSTOM', 'USE_EMAIL', 
            ]:
            cfg_telemetry[key] = _config[key] > 0

        # Record the actual config of these features
        for key in ['BOOK_API']:
            cfg_telemetry[key] = _config[key]

        # Record whether these are configured differently from the default
        default = {}
        for key in ['GR_API', 'GB_API', 'LT_DEVKEY', 'IMP_PREFLANG', 
            'IMP_CALIBREDB', 'DOWNLOAD_DIR', 'ONE_FORMAT', 'API_KEY']:
            _, _, default = config.CONFIG_DEFINITIONS[key]
            cfg_telemetry[key] = _config[key] != default

        # Count how many of each provider are configured
        for provider in [(lazylibrarian.NEWZNAB_PROV, "NEWZNAB"), 
            (lazylibrarian.TORZNAB_PROV, "TORZNAB"),
            (lazylibrarian.RSS_PROV, "RSS"),
            (lazylibrarian.IRC_PROV, "IRC"),
            (lazylibrarian.RSS_PROV, "RSS"),
            (lazylibrarian.GEN_PROV, "GEN"),
            ]:
            count = sum([1 for prov in provider[0] if prov["ENABLED"] and prov["HOST"]])
            cfg_telemetry[provider[1]] = count

        # Count how many Apprise notifications are configured
        count = sum([1 for prov in lazylibrarian.APPRISE_PROV if prov["URL"]])
        cfg_telemetry["APPRISE"] = count
       

    def record_usage_data(self, counter):
        usg = self.get_usage_telemetry()
        assert(not any([c in counter for c in ' "=']), "Counter must be plain text")
        usg[counter] += 1

    def get_json(self, pretty=False):
        return json.dumps(obj=self._data, indent = 4 if pretty else None)

    def submit_data(self, config):
        pass

