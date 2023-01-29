# Web server for LazyLibrarian telemetry server

import logging
import time
from io import StringIO

from bottle import route, run, request
import telemetrydb


@route('/')
def hello():
    logger.debug(f"Hello request for /")
    return "LazyLibrarian Telemetry Server"


@route('/stats/<stat_type:re:[a-z]+>')
def stats(stat_type):
    logger.debug(f"Getting stats for {stat_type}")
    valid_types = ['usage', 'configs', 'servers', 'switches', 'params', 'all']
    if not stat_type or stat_type not in valid_types:
        return "Valid stats types: %s" % str(valid_types)
    result = _read_from_db(stat_type)
    return result


@route('/csv/servers/<interval>')
def get_csv_server(interval):
    logger.debug(f"Getting server counts for {interval}")
    try:
        actual = telemetrydb.IntervalLength[interval.upper()]
    except KeyError:
        return "Valid intervals are hour, day, week and month"
    result = _read_csv('servers', actual)
    # Return a comma-separated list
    s = StringIO()
    s.write('date,reports\n')
    s.writelines([",".join(row) + '\n' for row in result])
    return s.getvalue()


@route('/help')
def server_status():
    logger.debug("Showing help")
    return """
    <html>
    <body>
    <h1>LazyLibrarian Telemetry Server</h1>
    <p>Please use one of the following:</p>
    <ul>
        <li><b>/status</b> - to get server status/uptime</li>
        <li><b>/stats/</b>[type] - show stats of given type (no type to list options)</li>
        <li><b>/send</b>?xxx - to send telemetry data (LL only)</li>
        <li><b>/help</b> - this page</li>
    <p>
    </body>
    </html>
    """


@route('/status')
def server_status():
    logger.debug("Getting server status")
    uptime = time.time() - _starttime
    pretty = pretty_approx_time(int(uptime))
    return {'status': 'online', 'servertime': pretty, 'received': _received, 'success': _success}


@route('/send', method='GET')
def process_telemetry():
    global _received, _success
    _received += 1

    # Expect data to be sent as...
    # ?server="{...}"&config="{...}"&usage="{...}"
    logger.info(f"Receiving {len(request.query_string)} bytes of telemetry data")

    data = request.query.dict
    logger.debug(f"Processing telemetry {data}")
    if 1 <= len(data) <= 4 and 'server' in data.keys():
        # In addition to data, we may also have a timeout parameter
        try:
            logger.info(f"Add to database ({len(data)} elements) from {data['server']}")
            status = _add_to_db(data)
            _success += 1
        except Exception as e:
            status = f"Error processing telemetry data: {str(e)}"
            logger.warning(status)
    else:
        status = "Invalid data received"
        logger.warning(status)
    return {'status': status}


def run_server(add_to_db, read_from_db, read_csv):
    global logger, _add_to_db, _read_from_db, _read_csv

    logger = logging.getLogger(__name__)
    port = 9174
    logger.info(f"Starting web server on port {port}")
    _add_to_db = add_to_db  # Method handler
    _read_from_db = read_from_db  # Method handler
    _read_csv = read_csv
    run(host='0.0.0.0', port=port, debug=True, quiet=True)


def pretty_approx_time(seconds: int) -> str:
    """ Return a string representing the parameter in a nice human-readable (approximate) way """
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    locals_ = locals()
    magnitudes_str = ("{n} {magnitude}".format(n=int(locals_[magnitude]), magnitude=magnitude)
                      for magnitude in ("days", "hours", "minutes", "seconds") if locals_[magnitude])
    return ", ".join(magnitudes_str)


_starttime = time.time()
_add_to_db = None
_read_from_db = None
_read_csv = None
_received = 0
_success = 0
logger: logging.Logger

