#!/bin/bash
# Script called by gitlab CI engine during test phases
# Removes optional python modules if they are there

python3 -m pip uninstall pillow lxml soupsieve Levenshtein apprise requests pyopenssl urllib3 -y
