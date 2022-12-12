#!/usr/bin/env bash
# Script called by gitlab CI engine during test phases

coverage run --source=lazylibrarian/ -m pytest --junitxml=lltest.xml ./unittests/test*.py
#coverage run --source=lazylibrarian/ -m pytest  --profile -v --junitxml=lltest.xml ./unittests/test*.py
coverage report
coverage xml