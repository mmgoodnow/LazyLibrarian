#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the processcontrol.py module
import os
import time

from lazylibrarian import logger
from lazylibrarian.processcontrol import get_info_on_caller, get_process_memory, track_resource_usage, get_cpu_use, PSUTIL
from unittests.unittesthelpers import LLTestCase


def some_function(path: bool, ext: bool) -> (str, str, int):
    """ A function that wants to know who called it """
    return get_info_on_caller(not path, not ext)


class TestProcessControl(LLTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setDoAll(False)
        logger.RotatingLogger.SHOW_LINE_NO = False
        return super().setUpClass()

    def some_method(self, depth: int, path: bool, ext: bool) -> (str, str, int):
        """ A method that wants to know who called it """
        return get_info_on_caller(depth, not path, not ext)

    def method_calling_method(self, depth: int, path: bool, ext: bool):
        return self.some_method(depth, path, ext)

    @classmethod
    def some_classmethod(cls, path: bool, ext: bool) -> (str, str, int):
        """ A class method that wants to know who called it """
        return get_info_on_caller(not path, not ext)

    def test_get_info_on_caller(self):
        def assertresults(fullpath: bool, ext: bool, expectedfunction: str):
            if fullpath:
                self.assertTrue(os.path.sep in filename)
            expectedfilename = 'test_processcontrol.py' if ext else 'test_processcontrol'
            self.assertEndsWith(filename, expectedfilename)
            self.assertEqual(function, expectedfunction)
            self.assertGreater(lineno, 10)  # Just not 0

        # Test calling a function:
        filename, function, lineno = some_function(False, False)
        assertresults(False, False, 'test_get_info_on_caller')

        # Test calling a method:
        filename, function, lineno = self.some_method(1, False, False)
        assertresults(False, False, 'test_get_info_on_caller')

        # Test calling a class method:
        filename, function, lineno = self.some_classmethod(False, False)
        assertresults(False, False, 'test_get_info_on_caller')

        # Test calling another level deep
        filename, function, lineno = self.method_calling_method(0, False, False)
        assertresults(False, False, 'some_method')
        filename, function, lineno = self.method_calling_method(1, False, False)
        assertresults(False, False, 'method_calling_method')
        filename, function, lineno = self.method_calling_method(2, False, False)
        assertresults(False, False, 'test_get_info_on_caller')

        # Test returning the extension
        filename, function, lineno = self.some_method(1, False, True)
        assertresults(False, True, 'test_get_info_on_caller')

        # Test returning the full filename
        filename, function, lineno = self.some_method(1, True, True)
        assertresults(True, True, 'test_get_info_on_caller')

        # Test returning the full filename but no extension
        filename, function, lineno = self.some_method(1, True, False)
        assertresults(True, False, 'test_get_info_on_caller')

        # Test level errors:
        filename, function, lineno = self.some_method(-1, False, False)
        self.assertEqual(filename, '')
        self.assertEqual(function, '')
        self.assertEqual(lineno, 0)

        filename, function, lineno = self.some_method(10000000, False, False)
        self.assertEqual(filename, '')
        self.assertEqual(function, '')
        self.assertEqual(lineno, 0)

    def test_get_process_memory(self):
        ok, mem = get_process_memory()
        if ok:
            self.assertGreater(mem, 0)
        else:
            self.assertEqual(mem, 0)

    def test_get_cpu_usage(self):
        ok, cpu = get_cpu_use()
        if ok:
            self.assertTrue('Unknown' not in cpu)
        else:
            self.assertTrue('Unknown' in cpu)

    @track_resource_usage
    def use_some_resource(self):
        time.sleep(0.1)
        x = dict()
        for i in range(10000):
            x[i] = f'Hello, {i}'
        return x

    def test_track_resource_usage(self):
        self.set_loglevel(2)  # Need debug logging
        with self.assertLogs('lazylibrarian.logger', 'DEBUG') as cm:
            _ = self.use_some_resource()

        if PSUTIL:
            self.assertEqual(len(cm.output), 1)
            # Sample output:
            # 'DEBUG:lazylibrarian.logger:MainThread : processcontrol.py:wrapper : use_some_resource: memory before: 87,011,328, after: 88,526,848, consumed: 1,515,520; exec time: 0:00:00.103080'
            logparts = cm.output[0].split(' ')
            self.assertNotEqual('0', logparts[11])  # Memory consumed
            self.assertNotEqual('0:00:00.000000', logparts[14])  # Time taken > 0
        else:
            self.assertEqual(cm.output, [
                'DEBUG:lazylibrarian.logger:MainThread : processcontrol.py:wrapper : psutil is not installed'])
