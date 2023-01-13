#  This file is part of Lazylibrarian.
#
# Purpose:
#   Remove bundled libraries and use a system version if available or installable
#   Only do this if not on docker as we can't modify the contents of a docker container
#   Record the results in a file. Don't keep trying to find/install libraries
# Constraint:
#   Should not depend on any other LazyLibrarian files, only standard python system libraries

import sys
import subprocess
import os
import shutil
import importlib


dependencies = [
    # pip name, bundled name, aka
    ('bs4', '', ''),
    ('html5lib', '', ''),
    ('webencodings', '', ''),
    ('requests', '', ''), 
    ('urllib3', '', ''),
    ('pyOpenSSL', None, 'OpenSSL'),
    ('cherrypy', '', ''),
    ('cherrypy_cors', 'cherrypy_cors.py', ''),
    ('httpagentparser', '', ''),
    ('mako', '', ''),
    ('httplib2', '', ''),
    ('Pillow', None, 'PIL'),
    ('apprise', None, ''),
    ('PyPDF3', '', ''),
    ('python_magic', 'magic', 'magic'),
    ('thefuzz', '', ''),
    ('Levenshtein', None, ''),
    ('deluge_client', '', ''),
]

def unbundle_libraries(dependencies, testing=False):
    docker = '/config' in sys.argv and sys.argv[0].startswith('/app/')
    bypass_file = os.path.join(os.getcwd(), 'unbundled.libs')
    removed = []
    if not docker and (testing or not os.path.isfile(bypass_file)):
        bundled = {}
        distro = {}
        missing = []
        for item in dependencies:
            if item[1] is not None:  # there may be a bundled version
                name = item[0]
                for finder in sys.meta_path:
                    if hasattr(finder, 'find_spec'):
                        spec = finder.find_spec(importlib.util.resolve_name(name, None), None)
                        if spec is not None:
                            if 'LazyLibrarian' in spec.origin:
                                bundled[name] = spec.origin
                            else:
                                distro[name] = spec.origin

        current_dir = sys.path.pop(0)  # don't look in current working directory
        for item in dependencies:
            name = item[2] if item[2] else item[0]
            if name not in distro:
                spec = None
                for finder in sys.meta_path:
                    if hasattr(finder, 'find_spec'):
                        spec = finder.find_spec(importlib.util.resolve_name(name, None), None)
                        if spec is not None:
                            distro[name] = spec.origin
                            break
                if not spec:
                    missing.append(name)

        for item in missing:
            try:
                _ = subprocess.run([sys.executable, '-m', 'pip', 'install', item], check=True,
                                   capture_output=True, text=True).stdout
                distro[item] = 'new install'
                missing.remove(item)
            except subprocess.CalledProcessError as e:
                print(str(e))

        if missing:
            print("Failed to install %s" % str(missing))

        deletable = []
        for item in dependencies:
            if item[1] is not None:
                if item[2] and item[2] in distro or item[0] in distro:
                    deletable.append(item[1] if item[1] else item[0])

        cwd = os.getcwd()
        for item in deletable:
            f = os.path.join(cwd, item)
            # might have already been deleted
            if os.path.isdir(f):
                if not testing:
                    shutil.rmtree(f)
                print("Removed bundled", item)
                removed.append(item)
            if os.path.isfile(f):
                if not testing:
                    os.remove(f)
                print("Removed bundled", item)
                removed.append(item)
        if not testing:
            with open(bypass_file, 'w') as f:
                f.write(str(removed))
        sys.path.insert(0, current_dir)
    return removed