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

## Sample test URL - native
#http://localhost:9174/send?server={"id":"ABC","install_type":"","version":"","os":"nt","uptime_seconds":0,"python_ver":"3.11.0 (main, Oct 24 2022, 18:26:48) [MSC v.1933 64 bit (AMD64)]"}&config={"switches":"EBOOK_TAB COMIC_TAB SERIES_TAB BOOK_IMG MAG_IMG COMIC_IMG AUTHOR_IMG API_ENABLED CALIBRE_USE_SERVER OPF_TAGS ","params":"IMP_CALIBREDB DOWNLOAD_DIR API_KEY ","BOOK_API":"OpenLibrary","NEWZNAB":1,"TORZNAB":0,"RSS":0,"IRC":0,"GEN":0,"APPRISE":1}&usage={"API/getHelp":2,"web/test":1,"Download/NZB":1}
# Sample test URL - made web friendly:
#http://localhost:9174/send?server={%22id%22:%22ABC%22,%22install_type%22:%22%22,%22version%22:%22%22,%22os%22:%22nt%22,%22uptime_seconds%22:0,%22python_ver%22:%223.11.0%20(main,%20Oct%2024%202022,%2018:26:48)%20[MSC%20v.1933%2064%20bit%20(AMD64)]%22}&config={%22switches%22:%22EBOOK_TAB%20COMIC_TAB%20SERIES_TAB%20BOOK_IMG%20MAG_IMG%20COMIC_IMG%20AUTHOR_IMG%20API_ENABLED%20CALIBRE_USE_SERVER%20OPF_TAGS%20%22,%22params%22:%22IMP_CALIBREDB%20DOWNLOAD_DIR%20API_KEY%20%22,%22BOOK_API%22:%22OpenLibrary%22,%22NEWZNAB%22:1,%22TORZNAB%22:0,%22RSS%22:0,%22IRC%22:0,%22GEN%22:0,%22APPRISE%22:1}&usage={%22API/getHelp%22:2,%22web/test%22:1,%22Download/NZB%22:1}
