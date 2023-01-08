#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import subprocess
import os
import shutil

print('Installing requirements')
try:
    reply = subprocess.run([sys.executable, '-m', 'pip', 'install', '.'], check=True, capture_output=True, text=True).stdout
except subprocess.CalledProcessError as e:
    print(str(e))
    exit(0)

print("Updating LazyLibrarian")
# if everything installed ok, force a lazylibrarian update from git/source
try:
    reply = subprocess.run([sys.executable, 'LazyLibrarian.py', '--update'], check=True, capture_output=True, text=True).stdout
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
for item in [
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
    'thefuzz',
    'magic',
    'deluge_client'
]:
    f = os.path.join(cwd, item)
    # might have already been deleted
    if os.path.isdir(f):
        shutil.rmtree(f)
        print("Removed ", item)
    if os.path.isfile(f):
        os.remove(f)
        print("Removed ", item)
  
