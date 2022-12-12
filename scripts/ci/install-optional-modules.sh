#!/bin/sh
# Script called by gitlab CI engine during test phases
# Installs optional python modules

python3 -m pip install pillow lxml soupsieve Levenshtein apprise requests pyopenssl urllib3
