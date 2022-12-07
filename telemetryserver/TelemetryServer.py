# Basic telemetry server for LazyLibrarian
#
# Listens for telemetry data from LL installations, and stores the
# data in a MySQL database.
#
# Requires MySQL running somewhere
#
# Config in telemetry.ini

import server

TS = server.TelemetryServer()
TS.initialize()
try:
    TS.start()
finally:
    TS.stop()

## Sample test URL:
#http://localhost:9174/send?server={%22id%22:%22ABC%22,%22uptime_seconds%22:16,%22install_type%22:%22%22,%22version%22:%22%22,%22os%22:%22nt%22}&config={%22switches%22:%22EBOOK_TAB%20COMIC_TAB%20SERIES_TAB%20BOOK_IMG%20MAG_IMG%20COMIC_IMG%20AUTHOR_IMG%20API_ENABLED%20CALIBRE_USE_SERVER%20OPF_TAGS%20%22,%22params%22:%22IMP_CALIBREDB%20DOWNLOAD_DIR%20API_KEY%20%22,%22BOOK_API%22:%22OpenLibrary%22,%22NEWZNAB%22:1,%22TORZNAB%22:0,%22RSS%22:0,%22IRC%22:0,%22GEN%22:0,%22APPRISE%22:0}&usage={%22APIgetHelp%22:20,%22web-test%22:10,%22Download!NZB%22:1}
