#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the processcontrol.py module
import os

from lazylibrarian.processcontrol import get_info_on_caller
from unittesthelpers import LLTestCase


def some_function(path: bool, ext: bool) -> (str, str, int):
    """ A function that wants to know who called it """
    return get_info_on_caller(not path, not ext)


class TestProcessControl(LLTestCase):

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
