# Utility functions for LazyLibrarian telemetry server

import json


# Routine used for testing
def test_loadjson():
    f = open('./unittests/testdata/telemetry-sample.json')
    try:
        loadedjson = json.load(f)
    finally:
        f.close()
    return loadedjson
