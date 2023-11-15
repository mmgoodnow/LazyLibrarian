To help with identifying and fixing issues, please include as much information as possible, including:
### LazyLibrarian version number (at the bottom of config page)
### Operating system used (windows, mac, linux, NAS type)
### Interface in use (default=bookstrap)
### Which api (Goodreads, GoogleBooks, both)
### Source of your LazyLibrarian installation (git, zip, snap, flatpak, rpm, deb, docker (which), 3rd party package)
### Relevant debug log with api keys and any passwords redacted

Please note - usually a single line of log is not sufficient. The lines just before the error occurs can give useful context and greatly assist with debugging.

### There is a built-in debug log creator on the logs page which makes it easy to provide this information
* To use it, first go to the config page and make sure logging is set to DEBUG and the box "Redact files save to disc" is checked. Other debug options allow focussing in on different areas. Leave these unchecked unless advised to enable them by lazylibrarian support.   
* Go and do whatever you need to recreate the error  
* Go back to the log page and press "Get support zip". This option is only available if redaction is enabled to prevent disclosing user info (passwords etc). It will create a zip file containing a redacted log and system/config info to assist in identifying the problem.  
* You can now turn debug logging off again if you want  
* Attach the zip file to your bug report.   
