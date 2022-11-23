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


from urllib.parse import urlencode

import requests

import lazylibrarian
from lazylibrarian import logger
from lazylibrarian.common import proxy_list
from lazylibrarian.formatter import check_int, make_utf8bytes, versiontuple


def check_link():
    # connection test, check host/port
    auth, _ = sab_nzbd(nzburl='auth')
    if not auth:
        return "Unable to talk to sab_nzbd, check HOST/PORT/SUBDIR"
    vers, _ = sab_nzbd(nzburl='version')
    if not vers or 'version' not in vers:
        lazylibrarian.SAB_VER = (0, 0, 0)
        vers = {'version': 'unknown'}
    else:
        lazylibrarian.SAB_VER = versiontuple(vers['version'])
        logger.debug("SAB version tuple %s" % str(lazylibrarian.SAB_VER))
    # check apikey is valid
    cats, _ = sab_nzbd(nzburl='get_cats')  # type: dict
    if not cats:
        return "Unable to talk to sab_nzbd, check APIKEY"
    # check category exists
    if lazylibrarian.CONFIG['SAB_CAT']:
        if 'categories' not in cats or not len(cats['categories']):
            return "Failed to get sab_nzbd categories: %s" % str(cats)
        if lazylibrarian.CONFIG['SAB_CAT'].split(',')[0] not in cats['categories']:
            return "sab_nzbd: Unknown category [%s]\nValid categories:\n%s" % (
                    lazylibrarian.CONFIG['SAB_CAT'], str(cats['categories']))
    return "sab_nzbd connection successful, version %s" % vers['version']


def sab_nzbd(title=None, nzburl=None, remove_data=False, search=None, nzo_ids=None, library='eBook', label=''):

    if nzburl in ['delete', 'delhistory', 'pause'] and title == 'unknown':
        res = '%s function unavailable in this version of sabnzbd, no nzo_ids' % nzburl
        logger.debug(res)
        return False, res

    hostname = lazylibrarian.CONFIG['SAB_HOST']
    port = check_int(lazylibrarian.CONFIG['SAB_PORT'], 0)
    if not hostname or not port:
        res = 'Invalid sabnzbd host or port, check your config'
        logger.error(res)
        return False, res

    hostname = hostname.rstrip('/')
    if not hostname.startswith("http://") and not hostname.startswith("https://"):
        hostname = 'http://' + hostname

    host = "%s:%s" % (hostname, port)

    if lazylibrarian.CONFIG['SAB_SUBDIR']:
        host = host + "/" + lazylibrarian.CONFIG['SAB_SUBDIR'].strip('/')

    params = {}

    if nzburl in ['auth', 'get_cats', 'version']:
        # connection test
        params['mode'] = nzburl
        params['output'] = 'json'
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        title = 'LL.(%s)' % nzburl
    elif nzburl == 'queue':
        params['mode'] = 'queue'
        params['limit'] = '50'
        params['output'] = 'json'
        if search:
            params['search'] = search
        if nzo_ids:
            params['nzo_ids'] = nzo_ids
        if lazylibrarian.CONFIG['SAB_CAT']:
            if label:
                params['category'] = label
            else:
                params['category'] = lazylibrarian.downloadmethods.use_label('SABNZBD', library)
        if lazylibrarian.CONFIG['SAB_USER']:
            params['ma_username'] = lazylibrarian.CONFIG['SAB_USER']
        if lazylibrarian.CONFIG['SAB_PASS']:
            params['ma_password'] = lazylibrarian.CONFIG['SAB_PASS']
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        title = 'LL.(Queue)'
    elif nzburl == 'history':
        params['mode'] = 'history'
        params['limit'] = '50'
        params['output'] = 'json'
        if search:
            params['search'] = search
        if nzo_ids:
            params['nzo_ids'] = nzo_ids
        if lazylibrarian.CONFIG['SAB_CAT']:
            if label:
                params['category'] = label
            else:
                params['category'] = lazylibrarian.downloadmethods.use_label('SABNZBD', library)
        if lazylibrarian.CONFIG['SAB_USER']:
            params['ma_username'] = lazylibrarian.CONFIG['SAB_USER']
        if lazylibrarian.CONFIG['SAB_PASS']:
            params['ma_password'] = lazylibrarian.CONFIG['SAB_PASS']
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        title = 'LL.(History)'
    elif nzburl == 'delete':
        # only deletes tasks if still in the queue, ie NOT completed tasks
        params['mode'] = 'queue'
        params['output'] = 'json'
        params['name'] = nzburl
        params['value'] = make_utf8bytes(title)[0]
        if lazylibrarian.CONFIG['SAB_USER']:
            params['ma_username'] = lazylibrarian.CONFIG['SAB_USER']
        if lazylibrarian.CONFIG['SAB_PASS']:
            params['ma_password'] = lazylibrarian.CONFIG['SAB_PASS']
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        if remove_data:
            params['del_files'] = 1
        title = 'LL.(Delete) ' + title
    elif nzburl == 'delhistory':
        params['mode'] = 'history'
        params['output'] = 'json'
        params['name'] = 'delete'
        params['value'] = make_utf8bytes(title)[0]
        if lazylibrarian.CONFIG['SAB_USER']:
            params['ma_username'] = lazylibrarian.CONFIG['SAB_USER']
        if lazylibrarian.CONFIG['SAB_PASS']:
            params['ma_password'] = lazylibrarian.CONFIG['SAB_PASS']
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        if remove_data:
            params['del_files'] = 1
        title = 'LL.(DelHistory) ' + title
    elif nzburl == 'pause':
        params['mode'] = 'queue'
        params['output'] = 'json'
        params['name'] = 'pause'
        params['value'] = nzo_ids
        if lazylibrarian.CONFIG['SAB_USER']:
            params['ma_username'] = lazylibrarian.CONFIG['SAB_USER']
        if lazylibrarian.CONFIG['SAB_PASS']:
            params['ma_password'] = lazylibrarian.CONFIG['SAB_PASS']
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        title = 'LL.(Pause) ' + title
    else:
        params['mode'] = 'addurl'
        params['output'] = 'json'
        if nzburl:
            params['name'] = make_utf8bytes(nzburl)[0]
        if title:
            params['nzbname'] = make_utf8bytes(title)[0]
        if lazylibrarian.CONFIG['SAB_USER']:
            params['ma_username'] = lazylibrarian.CONFIG['SAB_USER']
        if lazylibrarian.CONFIG['SAB_PASS']:
            params['ma_password'] = lazylibrarian.CONFIG['SAB_PASS']
        if lazylibrarian.CONFIG['SAB_API']:
            params['apikey'] = lazylibrarian.CONFIG['SAB_API']
        if lazylibrarian.CONFIG['SAB_CAT']:
            if label:
                params['category'] = label
            else:
                params['cat'] = lazylibrarian.downloadmethods.use_label('SABNZBD', library)
        if lazylibrarian.CONFIG['USENET_RETENTION']:
            params["maxage"] = lazylibrarian.CONFIG['USENET_RETENTION']

# FUTURE-CODE
#    if lazylibrarian.SAB_PRIO:
#        params["priority"] = lazylibrarian.SAB_PRIO
#    if lazylibrarian.SAB_PP:
#        params["script"] = lazylibrarian.SAB_SCRIPT

    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('sab params: %s' % repr(params))

    url = host + "/api?" + urlencode(params)

    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug('Request url for <a href="%s">sab_nzbd</a>' % url)
    proxies = proxy_list()
    try:
        timeout = check_int(lazylibrarian.CONFIG['HTTP_TIMEOUT'], 30)
        if url.startswith('https') and lazylibrarian.CONFIG['SSL_VERIFY']:
            r = requests.get(url, timeout=timeout, proxies=proxies,
                             verify=lazylibrarian.CONFIG['SSL_CERTS'] if lazylibrarian.CONFIG['SSL_CERTS'] else True)
        else:
            r = requests.get(url, timeout=timeout, proxies=proxies, verify=False)
        result = r.json()
    except requests.exceptions.Timeout:
        res = "Timeout connecting to SAB with URL: %s" % url
        logger.error(res)
        return False, res
    except Exception as e:
        res = "Unable to connect to SAB with URL: %s, %s:%s" % (url, type(e).__name__, str(e))
        logger.error(res)
        return False, res
    if lazylibrarian.LOGLEVEL & lazylibrarian.log_dlcomms:
        logger.debug("Result text from SAB: " + str(result))

    if title and title.startswith('LL.('):
        return result, ''

    if result['status'] is True:
        logger.info("%s sent to SAB successfully." % title)
        # sab versions earlier than 0.8.0 don't return nzo_ids
        if 'nzo_ids' in result:
            if result['nzo_ids']:  # check its not empty
                return result['nzo_ids'][0], ''
        return 'unknown', ''
    elif result['status'] is False:
        res = "SAB returned Error: %s" % result['error']
        logger.error(res)
        return False, res
    else:
        res = "Unknown error: %s" % str(result)
        logger.error(res)
        return False, res
