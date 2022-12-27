# Web server for LazyLibrarian telemetry server

from bottle import route, run, request
import time
import logging
from lazylibrarian.formatter import pretty_approx_time

@route('/')
def hello():
    logger.debug(f"Hello request for /")
    return "LazyLibrarian Telemetry Server"

@route('/stats/<type:re:[a-z]+>')
def stats(type):
    logger.debug(f"Getting stats for {type}")
    return f"Something needs to happen here: {type}"

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
        <li><b>/stats/</b>[type] - show stats of given type - to come</li>
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
    pretty = pretty_approx_time(uptime)
    return {'status':'online', 'servertime':pretty, 'received': _received, 'success': _success}

@route('/send', method='GET')
def process_telemetry():
    global _received, _success
    _received += 1

    # Expect data to be sent as...
    # ?server="{...}"&config="{...}"&usage="{...}"
    logger.info(f"Receiving {len(request.query_string)} bytes of telemetry data")

    data = request.query.dict
    logger.debug(f"Processing telemetry {data}")
    if len(data) >= 1 and len(data) <=4 and 'server' in data.keys():
        # In addition to data, we may also have a timeout parameter
        try:
            logger.debug(f"Add to database ({len(data)} elements)")
            status = _add_to_db(data)
            _success += 1
        except Exception as e:
            status = f"Error processing telemetry data: {str(e)}"
            logger.warning(status)
    else:
        status = "Invalid data received"
        logger.warning(status)
    return {'status': status}

def run_server(add_to_db):
    global logger, _add_to_db

    logger = logging.getLogger(__name__)
    PORT = 9174
    logger.info(f"Starting web server on port {PORT}")
    _add_to_db = add_to_db # Method handler
    run(host='0.0.0.0', port=PORT, debug=True, quiet=True)


_starttime = time.time()
_add_to_db = None
_received = 0
_success = 0
logger = None

