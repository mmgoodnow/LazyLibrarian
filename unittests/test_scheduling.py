#  This file is part of Lazylibrarian.
#
# Purpose:
#   Test functions in scheduling.py

import unittesthelpers

import lazylibrarian
from lazylibrarian import scheduling
import datetime


class SchedulingTest(unittesthelpers.LLTestCase):

    def test_next_run_time(self):
        testnow = datetime.datetime(2023, 11, 30, 17, 42, 31)
        testcases = [
            (datetime.timedelta(seconds=0), "0 seconds" ),
            (datetime.timedelta(seconds=1), "1 second" ),
            (datetime.timedelta(seconds=10), "10 seconds" ),
            (datetime.timedelta(seconds=60), "60 seconds" ),
            (datetime.timedelta(seconds=100), "2 minutes" ),
            (datetime.timedelta(minutes=1), "60 seconds" ),
            (datetime.timedelta(hours=1), "60 minutes" ),
            (datetime.timedelta(hours=10), "10 hours" ),
            (datetime.timedelta(days=1, seconds=1), "2 days" ),
            (datetime.timedelta(days=1, hours=1), "2 days" ),
            (datetime.timedelta(hours=100), "4 days" ),
            (datetime.timedelta(days=1), "24 hours" ),
            (datetime.timedelta(hours=25), "2 days" ),
            (datetime.timedelta(days=2), "2 days" ),
            (datetime.timedelta(days=100), "100 days" ),
            (datetime.timedelta(days=1000), "1000 days" ),
            (datetime.timedelta(weeks=5, days=7), "42 days" ),
            # Failure case: Don't supply increments of less than a second
            (datetime.timedelta(days=1, microseconds=1), "0 seconds" ),
        ]
        for case in testcases:
            delta = case[0]
            nrt = scheduling.next_run_time(str(testnow+delta), testnow)
            self.assertEqual(nrt, case[1])
