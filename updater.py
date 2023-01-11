#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
#
# Should be run from the lazylibrarian program folder
# Call with lazylibrarian python interpreter and pid of lazylibrarian, eg 
# updater.py /usr/local/bin/python3.9 1234567
# 
# this will install library dependencies to the currently running lazylibrarian python interpreter
# and if successful it will terminate lazylibrarian by pid and trigger an upgrade from git/source, then remove bundled libraries
# Note: it currently does not restart lazylibrarian after the upgrade
#
# Alternatively run manually from the lazylibrarian program folder with no args
# Lazylibrarian should be stopped before running updater, and the default python interpreter will be used
#
import signal
import sys
import subprocess
import os
import shutil

bundled_libs = [
    'bs4',
    'html5lib',
    'webencodings',
    'requests',
    'urllib3',
    'cherrypy',
    'cherrypy_cors.py',
    'httpagentparser',
    'mako',
    'httplib2',
    'PyPDF3',
    'magic',
    'thefuzz',
    'deluge_client'
]

dependencies = [
    'bs4',
    'html5lib',
    'webencodings',
    'requests',
    'urllib3',
    'pyOpenSSL',
    'cherrypy',
    'cherrypy_cors',
    'httpagentparser',
    'mako',
    'httplib2',
    'Pillow',
    'apprise',
    'PyPDF3',
    'python_magic',
    'thefuzz[speedup]',
    'deluge_client',
]

if len(sys.argv) == 3:
    parent = sys.argv[2]
    executable = sys.argv[1]
else:
    parent = ''
    executable = 'python3'

print('Installing dependencies')
failures = ''
for item in dependencies:
    print(item)
    try:
        reply = subprocess.run([executable, '-m', 'pip', 'install', item], check=True, capture_output=True, text=True).stdout
        print(reply)
    except subprocess.CalledProcessError as e:
        failures += ' ' + item
        print(str(e))

if failures:
    print("Unable to continue, failed to install%s" % failures)
    exit(0)

print("Updating LazyLibrarian")
# if everything installed ok, force a lazylibrarian update from git/source
if parent:
    os.kill(parent, signal.SIGTERM)
try:
    reply = subprocess.run([executable, 'LazyLibrarian.py', '--update'], check=True, capture_output=True, text=True).stdout
    print(reply)
except subprocess.CalledProcessError as e:
    print(str(e))
    exit(0)

print("Tidying up")
# if updated ok remove any redundant residual files
# shouldn't need to manually remove these files for a git install
# as the git update will delete any files removed from the project.
# A source install (zip, tar.gz) overwrites old versions of files, 
# but doesn't delete any removed files, so we have to do it...
cwd = os.getcwd()
for item in bundled_libs:
    f = os.path.join(cwd, item)
    # might have already been deleted
    if os.path.isdir(f):
        shutil.rmtree(f)
        print("Removed old copy of ", item)
    if os.path.isfile(f):
        os.remove(f)
        print("Removed old copy of ", item)
  
