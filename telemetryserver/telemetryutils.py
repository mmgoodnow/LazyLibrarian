# Utility functions for LazyLibrarian telemetry server

import json

def pretty_approx_time(seconds):
    """ Return a string representing the parameter in a nice human readable (approximate) way """
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if seconds < 1:seconds = 1
    locals_ = locals()
    magnitudes_str = ("{n} {magnitude}".format(n=int(locals_[magnitude]), magnitude=magnitude)
                    for magnitude in ("days", "hours", "minutes", "seconds") if locals_[magnitude])
    return ", ".join(magnitudes_str)    

# Routine used for testing
def test_loadjson():
    f = open('./unittests/testdata/telemetry-sample.json')
    try:
        loadedjson = json.load(f)
    finally:
        f.close()
    return loadedjson
