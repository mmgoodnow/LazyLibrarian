#!/bin/bash
# Script called by gitlab CI engine during test phases
# Installs python modules required for LL and unit testing

# Minimal modules
python3 -m pip install charset_normalizer certifi idna pillow
# Build/testing modules
python3 -m pip install pytest mock pytest_order pytest-cov coverage pytest-profiling build