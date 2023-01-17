#  This file is part of Lazylibrarian.
#
# Purpose:
#   Remove bundled libraries and use a system version if available or installable
#   Only do this if not on docker as we can't modify the contents of a docker container
#   Record the results in a file. Don't keep trying to find/install libraries
# Constraint:
#   Should not depend on any other LazyLibrarian files, only standard python system libraries

import os
import shutil
import subprocess
import sys
from importlib.util import resolve_name
from pathlib import Path
from typing import Dict, List, Tuple

ll_dependencies = (
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
)


def unbundle_libraries(dependencies: List[Tuple[str, str, str]] = ll_dependencies) -> List[str]:
    """ Attempt to unbundle the dependencies passed, stored in subdirs off where basefile resides.
    Returns a list of dependent libraries that were removed in the process.
    Saves a file called unbundled.libs with these names; if this file exists, this routine does nothing.
    """
    docker = '/config' in sys.argv and sys.argv[0].startswith('/app/')
    # This file must be in the lazylibrarian/ directory; the parent dir is the main one
    basedir = str(Path(__file__).parent.parent.resolve())
    bypass_file = os.path.join(basedir, 'unbundled.libs')
    removed = []
    if not docker and not os.path.isfile(bypass_file):
        bundled, distro = get_library_locations(basedir, dependencies)
        distro = install_missing_libraries(bundled, distro)
        deletable = calc_libraries_to_delete(dependencies, distro)
        removed = delete_libraries(basedir, deletable)
        with open(bypass_file, 'w') as f:
            f.write(str(removed))
    return removed


def get_library_locations(basedir: str, dependencies: List[Tuple[str, str, str]]) -> (
        str, Dict[str, str], Dict[str, str]):
    """ Go through dependencies, return two dicts:
    1) A dict of dependencies where we need to use the bundled version, and
    2) a dict of dependencies where we can use a separate installation
    """
    paths = sys.path
    bundled = {}
    distro = {}
    for item in dependencies:
        if item[1] is not None:  # there may be a bundled version
            name = item[0]
            for finder in sys.meta_path:
                if hasattr(finder, 'find_spec'):
                    spec = finder.find_spec(resolve_name(name, None), None)
                    if spec is not None:
                        if basedir in spec.origin:
                            bundled[name] = spec.origin
                        else:
                            distro[name] = spec.origin

    # Look again, but not in the base LL directory where the bundles are
    removed_base = False
    for index, item in enumerate(paths):
        if item == basedir:
            paths.pop(index)
            removed_base = True
    try:
        for item in dependencies:
            name = item[2] if item[2] else item[0]
            if name not in distro:
                for finder in sys.meta_path:
                    if hasattr(finder, 'find_spec'):
                        spec = finder.find_spec(resolve_name(name, None), None)
                        if spec is not None:
                            distro[name] = spec.origin
                            break
    finally:
        if removed_base:
            sys.path.insert(0, basedir)
    return bundled, distro


def install_missing_libraries(bundled: Dict[str, str], distro: Dict[str, str]) -> Dict[str, str]:
    """ Attempt to install the libraries that are in 'bundled' but not in 'distro'.
    Return an updated dict of libraries that can now be unbundled. """
    for item in bundled:
        if item not in distro:
            print("Installing %s" % item)  # TODO: warn about potentially long installs here
            try:
                res = subprocess.run([sys.executable, '-m', 'pip', 'install', item], check=True,
                                     capture_output=True, text=True).stdout
                distro[item] = 'new install'
                print(res)
            except subprocess.CalledProcessError as e:
                print(f'Error: {str(e)}')
    return distro


def calc_libraries_to_delete(dependencies: List[Tuple[str, str, str]], distro: Dict[str, str]) -> List[str]:
    """ Returns a list of libraries from dependencies that can be deleted, because
    they can be satisfied by a library in the distro list. """
    deletable = []
    for item in dependencies:
        # Unfold item for readability
        (pipname, bundledname, aka) = item
        if aka and aka in distro or pipname in distro:
            deletable.append(bundledname if bundledname else pipname)
    return deletable


def delete_libraries(basedir: str, deletable: List[str]) -> List[str]:
    """ Delete all of the libraries in deletable, return a list of those deleted """
    removed = []
    for item in deletable:
        f = os.path.join(basedir, item)
        # might have already been deleted
        if os.path.isdir(f):
            shutil.rmtree(f)
            print("Removed bundled", item)
            removed.append(item)
        if os.path.isfile(f):
            os.remove(f)
            print("Removed bundled", item)
            removed.append(item)
    return removed
