# Basic telemetry server for LazyLibrarian
#
# Listens for telemetry data from LL installations, and stores the
# data in a MySQL database.
#
# Requires MySQL running somewhere
#
# Config in telemetry.ini

from bottle import route, run, request
import telemetryserver.telemetrydb
import datetime
import json

def pretty_time_delta(seconds):
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if seconds < 1:seconds = 1
    locals_ = locals()
    magnitudes_str = ("{n} {magnitude}".format(n=int(locals_[magnitude]), magnitude=magnitude)
                    for magnitude in ("days", "hours", "minutes", "seconds") if locals_[magnitude])
    return ", ".join(magnitudes_str)    

telemetry_db = None
starttime = datetime.datetime.now()

@route('/')
def hello():
    return "LazyLibrarian Telemetry Server"

@route('/stats/<type:re:[a-z]+>')
def stats(type):
    return f"Something needs to happen here: {type}"

@route('/status')
def server_status():
    uptime = datetime.datetime.now() - starttime
    pretty = pretty_time_delta(uptime.total_seconds())
    return {'status':'online', 'servertime':pretty}

@route('/send', method='GET')
def process_telemetry():
    # Expect data to be sent as...
    # ?server="{...}"&config="{...}"&usage="{...}"
    data = request.query.dict
    if len(data) >= 1 and len(data) <=4 and 'server' in data.keys():
        # In addition to data, we may also have a timeout parameter
        try:
            status = telemetry_db.add_telemetry(data)
        except Exception as e:
            status = f"Error processing telemetry data: {str(e)}"
    else:
        status = "Invalid telemetry data"
    return {'status': status}

# Routine used for testing
def test_loadjson():
    f = open('./unittests/testdata/telemetry-sample.json')
    try:
        loadedjson = json.load(f)
    finally:
        f.close()
    return loadedjson

# Open the database and start the server
# Parse command line
# Read config file
Config = None
# Create a logger
telemetry_db = telemetryserver.telemetrydb.TelemetryDB()
if telemetry_db.initialize():
    try:
        run(host='localhost', port=9174, debug=True)
    finally:
        telemetry_db = None # Closes the database
        TS = None


## Sample test URL:
#http://localhost:9174/send?server={%22id%22:%22ABC%22,%22uptime_seconds%22:16,%22install_type%22:%22%22,%22version%22:%22%22,%22os%22:%22nt%22}&config={%22switches%22:%22EBOOK_TAB%20COMIC_TAB%20SERIES_TAB%20BOOK_IMG%20MAG_IMG%20COMIC_IMG%20AUTHOR_IMG%20API_ENABLED%20CALIBRE_USE_SERVER%20OPF_TAGS%20%22,%22params%22:%22IMP_CALIBREDB%20DOWNLOAD_DIR%20API_KEY%20%22,%22BOOK_API%22:%22OpenLibrary%22,%22NEWZNAB%22:1,%22TORZNAB%22:0,%22RSS%22:0,%22IRC%22:0,%22GEN%22:0,%22APPRISE%22:0}&usage={%22APIgetHelp%22:20,%22web-test%22:10,%22Download!NZB%22:1}
