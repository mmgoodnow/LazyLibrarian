#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the configtypes module

from collections import Counter
import logging

from lazylibrarian import configtypes
from lazylibrarian.configenums import Access, TimeUnit, OnChangeReason
from unittests.unittesthelpers import LLTestCase


# noinspection PyBroadException
class Config2Test(LLTestCase):

    def test_ConfigItem(self):
        ci = configtypes.ConfigItem(section='Section', key='Key', default=17)
        self.assertFalse(ci.is_key('Section'))
        self.assertTrue(ci.is_key('Key'))
        self.assertEqual(ci.get_full_name(), 'SECTION.KEY')
        self.assertTrue(ci.is_enabled())  # 17 is True
        self.assertTrue(ci.is_default())
        self.assertFalse(ci.set_int(10), 'Cannot set a value on a ConfigItem')
        self.assertFalse(ci.set_from_ui('34'), 'Cannot set ConfigItem value')
        self.assertFalse(ci.set_bool(False), 'Cannot set ConfigItem value')
        self.assertTrue(ci.reset_to_default(), 'Can reset to default because no change happens')
        self.assertTrue(ci.is_default(), 'Should still be default')
        self.assertEqual(ci.get_save_str(), '17')
        self.assertEqual(ci.get_list(), ['17'])
        self.assertEqual(ci.get_schedule_name(), '')
        self.assertIsNone(ci.get_connection())
        self.assertTrue(ci.do_persist())
        self.assertFalse(ci.is_new)

        nsci = configtypes.ConfigItem(section='', key='MyKey', default='', persist=False)
        self.assertTrue(nsci.is_key('MyKey'))
        self.assertEqual(nsci.get_full_name(), 'MYKEY')
        self.assertFalse(nsci.is_enabled())  # '' is False
        self.assertFalse(nsci.set_from_ui(''), 'No change as it is already default')
        self.assertEqual(nsci.get_list(), [''])
        self.assertFalse(nsci.get_force_lower())
        self.assertFalse(nsci.do_persist())

        newci = configtypes.ConfigItem(section='Section', key='Key', default=17, is_new=True)
        self.assertTrue(newci.is_new)

    def test_ConfigStr(self):
        """ Tests for ConfigStr class """
        self.set_loglevel(logging.DEBUG)
        ci = configtypes.ConfigStr('Section', 'StrValue', 'Default')
        self.assertEqual(ci.get_str(), 'Default')
        self.assertEqual(str(ci), 'Default')

        ci.set_str('Override')
        self.assertEqual(ci.get_str(), 'Override')
        self.assertEqual(ci.get_default(), 'Default')
        self.assertFalse(ci.get_force_lower())

        with self.assertLogs('root', level=logging.WARNING) as cm:
            ci.set_int(2)  # Write Error
            self.assertEqual(ci.get_int(), 0)  # Read Error
            ci.set_bool(True)  # Write Error
            self.assertEqual(ci.get_bool(), False)  # Read Error
        self.assertListEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Cannot set config[STRVALUE] to 2: incorrect type',
            'WARNING:lazylibrarian.configtypes:Type error reading config[STRVALUE] (Override)',
            'WARNING:lazylibrarian.configtypes:Cannot set config[STRVALUE] to True: incorrect type',
            'WARNING:lazylibrarian.configtypes:Type error reading config[STRVALUE] (Override)'
        ])

        expected = Counter({Access.READ_OK: 3, Access.WRITE_OK: 1, Access.WRITE_ERR: 2, Access.READ_ERR: 2})
        self.single_access_compare(ci.accesses, expected, [], 'Basic String Config not working as expected')

        ci = configtypes.ConfigStr('S1', 'Key', 'Def', force_lower=True)
        self.assertTrue(ci.get_force_lower())
        self.assertEqual(ci.get_str(), 'Def')  # force_lower not applied to the default value
        self.assertTrue(ci.set_str('Hello, World'))
        self.assertEqual(ci.get_str(), 'hello, world')

    def test_ConfigInt(self):
        """ Tests for ConfigInt class """
        ci = configtypes.ConfigInt('Section', 'IntValue', 42)
        self.set_loglevel(logging.DEBUG)
        with self.assertLogs('root', level='INFO') as cm:
            self.assertEqual(ci.get_int(), 42)
            self.assertEqual(ci.get_str(), '42')
            self.assertEqual(ci.get_bool(), False)  # Read Error

            ci.set_str('Override')  # Write Error
            self.assertEqual(ci.get_str(), '42')

            ci.set_int(2)
            self.assertEqual(ci.get_int(), 2)
            self.assertEqual(str(ci), '2')
            ci.set_bool(True)  # Write Error
            self.assertEqual(ci.get_bool(), False)  # Read Error

            self.assertTrue(ci.set_from_ui('123'))
            self.assertEqual(ci.get_int(), 123)
            self.assertTrue(ci.set_from_ui('abc'))  # Works, but sets it to default. As expected?
            self.assertEqual(ci.get_int(), 42)
            self.assertFalse(ci.set_from_ui('False'))  # Not changed, remains default
            self.assertEqual(ci.get_int(), 42)

        self.assertListEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Type error reading config[INTVALUE] (42)',
            'WARNING:lazylibrarian.configtypes:Cannot set config[INTVALUE] to Override: incorrect type',
            'WARNING:lazylibrarian.configtypes:Cannot set config[INTVALUE] to True: incorrect type',
            'WARNING:lazylibrarian.configtypes:Type error reading config[INTVALUE] (2)'
        ])
        expected = Counter({Access.READ_OK: 8, Access.WRITE_OK: 3, Access.WRITE_ERR: 2, Access.READ_ERR: 2})
        self.single_access_compare(ci.accesses, expected, [], 'Basic Int Config not working as expected')

    def test_ConfigRangedInt(self):
        """ Tests for ConfigRangedInt class """
        ci = configtypes.ConfigRangedInt('Section', 'RangedIntValue', 42, 10, 1000)
        self.set_loglevel(logging.DEBUG)
        with self.assertLogs('root', level='INFO') as cm:
            self.assertEqual(int(ci), 42)
            ci.set_int(5)  # Write Error
            self.assertEqual(ci.get_int(), 42)
            ci.set_int(1100)  # Write Error
            self.assertEqual(int(ci), 42)
            ci.set_int(100)
            self.assertEqual(int(ci), 100)

        self.assertListEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Cannot set config[RANGEDINTVALUE] to 5',
            'WARNING:lazylibrarian.configtypes:Cannot set config[RANGEDINTVALUE] to 1100'
        ])
        expected = Counter({Access.READ_OK: 4, Access.WRITE_OK: 1, Access.WRITE_ERR: 2})
        self.single_access_compare(ci.accesses, expected, [], 'Ranged Int Config not working as expected')

    def test_ConfigPerm(self):
        """ Tests for ConfigPerm class """
        ci = configtypes.ConfigPerm('Section', 'PermissionValue', '0o777')
        self.assertEqual(ci.get_int(), 0o777)
        self.assertEqual(str(ci), '0o777')

        self.set_loglevel(logging.DEBUG)
        with self.assertLogs('root', level='INFO') as cm:
            ci.set_int(1000000)  # Write Error
            self.assertEqual(int(ci), 0o777)
            ci.set_int(-8)  # Write Error
            self.assertEqual(int(ci), 0o777)
            ci.set_int(57)  # Fine, if awkward
            self.assertEqual(int(ci), 57)
            ci.set_str('0o321')
            self.assertEqual(int(ci), 0o321)

        self.assertListEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Cannot set config[PERMISSIONVALUE] to 0o3641100',
            'WARNING:lazylibrarian.configtypes:Cannot set config[PERMISSIONVALUE] to -0o10'
        ])
        expected = Counter({Access.READ_OK: 6, Access.WRITE_OK: 2, Access.WRITE_ERR: 2})
        self.single_access_compare(ci.accesses, expected, [], 'Permission config not working as expected')

    def test_ConfigBool(self):
        """ Tests for ConfigBool class """
        ci = configtypes.ConfigBool('Section', 'BoolValue', True)
        self.set_loglevel(logging.DEBUG)
        with self.assertLogs('root', level='INFO') as cm:
            self.assertEqual(ci.get_int(), 1)  # We can read bools as int
            self.assertEqual(ci.get_str(), '1')
            self.assertEqual(ci.get_bool(), True)
            self.assertEqual(int(ci), 1)  # We can read bools as default int
            ci.set_int(2)  # ok, writes as True/1

            ci.set_str('Override')  # Write Error

            self.assertEqual(ci.get_str(), '1')
            self.assertEqual(ci.get_save_str(), 'True')
            self.assertEqual(ci.get_bool(), True)
            self.assertEqual(int(ci), 1)
            self.assertTrue(ci.is_enabled())
            ci.set_bool(False)
            self.assertEqual(ci.get_bool(), False)
            self.assertEqual(ci.get_str(), '')
            self.assertEqual(ci.get_save_str(), 'False')
            self.assertFalse(ci.is_enabled())
        self.assertListEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Cannot set config[BOOLVALUE] to Override: incorrect type'
        ])
        expected = Counter({Access.READ_OK: 13, Access.WRITE_OK: 1, Access.WRITE_ERR: 1})
        self.single_access_compare(ci.accesses, expected, [], 'Basic Bool Config not working as expected')

    def test_ConfigURL(self):
        """ Tests for ConfigURL class """
        goodurls = [
            ('google', 'https://www.google.com'),
            ('ftp', "ftp://ftp.example.com"),
            ('localip', "http://192.168.1.1"),
        ]
        badurls = [
            ('invalid_spaces', "not a URL"),
            ('invalid_proto', "httpss://www.google.com"),
            ('invalid_domain', "htt://.com"),
        ]
        for url in goodurls:
            urlitem = configtypes.ConfigURL(section='', key=url[0], default=url[1])
            self.assertEqual(urlitem.get_url(), url[1])
            self.assertEqual(urlitem.get_str(), url[1])

        for url in badurls:
            with self.assertRaises(RuntimeError):
                _ = configtypes.ConfigURL(section='', key=url[0], default=url[1])  # Format error->Abort
            urlitem = configtypes.ConfigURL(section='', key=url[0], default='')
            self.assertFalse(urlitem.set_str(url[1]))  # Format error, not RuntimeError
            self.assertEqual(urlitem.get_url(), '')
            self.assertFalse(urlitem.set_int(17))  # No, can't use an int

        self.maxDiff = None

    def test_ConfigFolder(self):
        """ Tests for ConfigFolder class """
        foldernames = [
            '',
            '/forward/slash',
            '\\windows\\style',
            'C:\\Windows/confused/style',
            '$Author/$Template'
        ]
        import os
        osname = os.name
        try:
            # Pretend it's Windows
            os.name = 'nt'
            for name in foldernames:
                cf = configtypes.ConfigFolder('', '', name)
                self.assertFalse('/' in str(cf), f'Expect no forward slashes in Windows: {name} -> {str(cf)}')
                self.assertFalse('\\' in cf.get_save_str(), f'Expect no \\ in save strings: {name} -> {str(cf)}')

            # Pretend it's not Windows:
            os.name = 'linux'
            for name in foldernames:
                cf = configtypes.ConfigFolder('', '', name)
                self.assertFalse('\\' in str(cf), f'Expect no backslashes in Linux: {name} -> {str(cf)}')
                self.assertFalse('\\' in cf.get_save_str(), f'Expect no \\ in save strings: {name} -> {str(cf)}')
        finally:
            os.name = osname

    def test_ConfigScheduler(self):
        """ Tests for config holding scheduler information """
        ci = configtypes.ConfigScheduler('', '', 'Test', 10, TimeUnit.MIN, 'run',
                                         'unittests.unittesthelpers.false_method', 'Description', needs_provider=False)
        self.assertEqual(ci.get_schedule_name(), 'Test', 'Schedule name not stored correctly')
        self.assertEqual(ci.get_int(), 10, 'Schedule interval not stored correctly')
        self.assertIsNotNone(ci.get_method(), 'Cannot find schedule method to run')
        with self.assertLogs('root', level='INFO') as cm:
            ci.set_int(10000000)  # Value too large, should have no effect
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Cannot set config[] to 10000000'])
        self.assertEqual(ci.get_int(), 10, 'Schedule interval not stored correctly')
        self.assertEqual((0, 10), ci.get_hour_min_interval())

        # Interval < 5 minutes
        ci = configtypes.ConfigScheduler('', '', 'Test', 1, TimeUnit.MIN, 'run',
                                         'unittests.unittesthelpers.false_method', 'Description', needs_provider=False)
        self.assertEqual((0, 5), ci.get_hour_min_interval())

        # More than an hour, less than 10
        ci = configtypes.ConfigScheduler('', '', 'Test', 65, TimeUnit.MIN, 'run',
                                         'unittests.unittesthelpers.false_method', 'Description', needs_provider=False)
        self.assertEqual((0, 65), ci.get_hour_min_interval())

        # More than 10 hours
        ci = configtypes.ConfigScheduler('', '', 'Test', 700, TimeUnit.MIN, 'run',
                                         'unittests.unittesthelpers.false_method', 'Description', needs_provider=False)
        self.assertEqual((11, 0), ci.get_hour_min_interval())

        # More than a day in hours
        ci = configtypes.ConfigScheduler('', '', 'Test', 28, TimeUnit.HOUR, 'run',
                                         'unittests.unittesthelpers.false_method', 'Description', needs_provider=False)
        self.assertEqual((28, 0), ci.get_hour_min_interval())

        # 2 days becomes hours
        ci = configtypes.ConfigScheduler('', '', 'Test', 2, TimeUnit.DAY, 'run',
                                         'unittests.unittesthelpers.false_method', 'Description', needs_provider=False)
        self.assertEqual((48, 0), ci.get_hour_min_interval())

        try:
            _ = configtypes.ConfigScheduler('', '', '', 10, TimeUnit.HOUR, 'run', '', '', True)
            self.assertTrue(False, 'Expected RuntimeError to be raised because schedule is empty')
        except RuntimeError:
            pass  # This is what we expect

    def test_ConfigDownloadTypes(self):
        """ Test the ConfigDownloadTypes, which can only be A,C,E,M or combinations """
        with self.assertLogs('root', level='INFO') as cm:
            cdt = configtypes.ConfigDownloadTypes('', '', 'E')
            self.assertEqual(cdt.get_csv(), 'E')
            cdt.set_str('M,A')
            self.assertEqual(cdt.get_csv(), 'M,A')

            cdt.set_str('M,A,X')  # Write error, value doesn't change
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.configtypes:Cannot set config[] to M,A,X'
        ])
        self.assertEqual(cdt.get_csv(), 'M,A')

    def test_ConfigEmail(self):
        ci = configtypes.ConfigEmail('', 'mail', '')
        self.assertTrue(ci.set_str('testing@test.com'))
        self.assertEqual(ci.get_str(), 'testing@test.com')

        self.assertTrue(ci.set_str('bob+fred@aol.co.uk'))
        self.assertEqual(ci.get_str(), 'bob+fred@aol.co.uk')
        self.assertFalse(ci.set_str('justastring.co.uk'))
        self.assertEqual(ci.get_str(), 'bob+fred@aol.co.uk')
        self.assertFalse(ci.set_str('toomany@alsome@bob.com'))
        self.assertEqual(ci.get_str(), 'bob+fred@aol.co.uk')

    def test_ConfigConnection(self):
        ci = configtypes.ConfigConnection('', 'connection')
        self.assertIsNone(ci.get_connection())
        self.assertFalse(ci.do_persist())
        self.assertTrue(ci.set_connection(101))  # A connection can be anything
        self.assertEqual(ci.get_connection(), 101)  # A connection can be anything

    def test_read_write_count(self):
        someint = configtypes.ConfigInt(section='', key='someint', default=41)
        self.assertEqual(someint.get_read_count(), 0, 'someint has not yet been read')
        self.assertEqual(someint.get_write_count(), 0)
        self.assertEqual(someint.get_int(), 41)
        self.assertTrue(someint.set_int(123))
        self.assertEqual(someint.get_int(), 123)
        self.assertEqual(someint.get_read_count(), 2, 'Expected 2 reads')
        self.assertEqual(someint.get_write_count(), 1, 'Expected 1 write')

    @classmethod
    def onchangesample(cls, value: str, reason: OnChangeReason):
        cls.changed_value = value

    def test_onchange(self):
        ci = configtypes.ConfigStr('', 'StrTest', '123')
        ci.set_onchange(self.onchangesample)
        ci.set_str('abc')
        self.assertEqual(ci.get_str(), 'abc')
        self.assertEqual(self.changed_value, 'abc')
