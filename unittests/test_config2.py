
#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the new config2 module

from collections import Counter

from unittesthelpers import LLTestCase
import lazylibrarian
from lazylibrarian import config2, configdefs, configtypes, logger, LOGLEVEL
from lazylibrarian.common import syspath

# Ini files used for testing load/save functions.
# If these change, many test cases need to be updated. Run to find out which ones
SMALL_INI_FILE = './unittests/testdata/testconfig-defaults.ini'
COMPLEX_INI_FILE = './unittests/testdata/testconfig-complex.ini'

class Config2Test(LLTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.setConfigFile('No Config File*')
        super().setDoAll(False)
        logger.RotatingLogger.SHOW_LINE_NO = False # Hack used to make tests more robust
        return super().setUpClass()

    def test_Logging(self):
        """ Test that we can test for log events """
        lazylibrarian.LOGLEVEL = 1
        # Test checking that a single message can be captured
        with self.assertLogs('lazylibrarian.logger', level='ERROR') as cm:
            logger.error('test error')
        self.assertEqual(cm.output,
         ['ERROR:lazylibrarian.logger:MainThread : test_config2.py:test_Logging : test error']
         ,'Did not log a message as expected'
        )

        # Check more error levels, but with LOGLEVEL=1, debug messages are ignored
        with self.assertLogs('lazylibrarian.logger', level='DEBUG') as cm:
            logger.error('test error')
            logger.warn('test warn')
            logger.info('test info')
            logger.debug('test debug')
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : test_config2.py:test_Logging : test error',
            'WARNING:lazylibrarian.logger:MainThread : test_config2.py:test_Logging : test warn',
            'INFO:lazylibrarian.logger:MainThread : test_config2.py:test_Logging : test info'
        ]
         ,'Expected an error, a warning and an info message'
        )

        # Test capturing debug messages
        lazylibrarian.LOGLEVEL = 2
        with self.assertLogs('lazylibrarian.logger', level='DEBUG') as cm:
            logger.info('test info')
            logger.debug('test debug')
        self.assertEqual(cm.output, [
            'INFO:lazylibrarian.logger:MainThread : test_config2.py:test_Logging : test info',
            'DEBUG:lazylibrarian.logger:MainThread : test_config2.py:test_Logging : test debug'
        ]
         ,'Expected an info and a debug message'
        )

    def test_ConfigStr(self):
        """ Tests for ConfigStr class """
        lazylibrarian.LOGLEVEL = 2
        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            ci = configtypes.ConfigStr('Section', 'StrValue', 'Default')
            self.assertEqual(ci.get_str(), 'Default')
            self.assertEqual(str(ci), 'Default')

            ci.set_str('Override')
            self.assertEqual(ci.get_str(), 'Override')

        with self.assertLogs('lazylibrarian.logger', level='WARN') as cm:
            ci.set_int(2)                          # Write Error
            self.assertEqual(ci.get_int(), 0)      # Read Error
            ci.set_bool(True)                      # Write Error
            self.assertEqual(ci.get_bool(), False) # Read Error
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_type_mismatch : Cannot set config[StrValue] to 2: incorrect type',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_read : Type error reading config[StrValue] (Override)',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_type_mismatch : Cannot set config[StrValue] to True: incorrect type',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_read : Type error reading config[StrValue] (Override)',
        ])

        expected = Counter({'read_ok': 3, 'write_ok': 1, 'write_error': 2, 'read_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Basic String Config not working as expected')

    def test_ConfigInt(self):
        """ Tests for ConfigInt class """
        ci = configtypes.ConfigInt('Section', 'IntValue', 42)
        lazylibrarian.LOGLEVEL = 2
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            self.assertEqual(ci.get_int(), 42)
            self.assertEqual(ci.get_str(), '42')
            self.assertEqual(ci.get_bool(), False) # Read Error

            ci.set_str('Override')                 # Write Error
            self.assertEqual(ci.get_str(), '42')

            ci.set_int(2)
            self.assertEqual(ci.get_int(), 2)
            self.assertEqual(str(ci), '2')
            ci.set_bool(True)                      # Write Error
            self.assertEqual(ci.get_bool(), False) # Read Error

        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_read : Type error reading config[IntValue] (42)',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_type_mismatch : Cannot set config[IntValue] to Override: incorrect type',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_type_mismatch : Cannot set config[IntValue] to True: incorrect type',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_read : Type error reading config[IntValue] (2)',
        ])
        expected = Counter({'read_ok': 5, 'write_ok': 1, 'write_error': 2, 'read_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Basic Int Config not working as expected')

    def test_ConfigRangedInt(self):
        """ Tests for ConfigRangedInt class """
        ci = configtypes.ConfigRangedInt('Section', 'RangedIntValue', 42, 10, 1000)
        lazylibrarian.LOGLEVEL = 2
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            self.assertEqual(int(ci), 42)
            ci.set_int(5)                          # Write Error
            self.assertEqual(ci.get_int(), 42)
            ci.set_int(1100)                       # Write Error
            self.assertEqual(int(ci), 42)
            ci.set_int(100)
            self.assertEqual(int(ci), 100)

        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_set : Cannot set config[RangedIntValue] to 5',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_set : Cannot set config[RangedIntValue] to 1100',
        ])
        expected = Counter({'read_ok': 4, 'write_ok': 1, 'write_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Ranged Int Config not working as expected')

    def test_ConfigPerm(self):
        """ Tests for ConfigPerm class """
        ci = configtypes.ConfigPerm('Section', 'PermissionValue', '0o777')
        self.assertEqual(ci.get_int(), 0o777)
        self.assertEqual(str(ci), '0o777')

        lazylibrarian.LOGLEVEL = 2
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            ci.set_int(1000000)                     # Write Error
            self.assertEqual(int(ci), 0o777)
            ci.set_int(-8)                          # Write Error
            self.assertEqual(int(ci), 0o777)
            ci.set_int(57)                          # Fine, if awkward
            self.assertEqual(int(ci), 57)
            ci.set_str('0o321')
            self.assertEqual(int(ci), 0o321)

        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_set : Cannot set config[PermissionValue] to 0o3641100',
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_set : Cannot set config[PermissionValue] to -0o10',
        ])
        expected = Counter({'read_ok': 6, 'write_ok': 2, 'write_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Permission config working as expected')

    def test_ConfigBool(self):
        """ Tests for ConfigBool class """
        ci = configtypes.ConfigBool('Section', 'BoolValue', True)
        lazylibrarian.LOGLEVEL = 2
        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            self.assertEqual(ci.get_int(), 1)      # We can read bools as int
            self.assertEqual(ci.get_str(), 'True')
            self.assertEqual(ci.get_bool(), True)
            self.assertEqual(int(ci), 1)           # We can read bools as default int
            ci.set_int(2)                          # ok, writes as True/1

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            ci.set_str('Override')                 # Write Error

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            self.assertEqual(ci.get_str(), 'True')
            self.assertEqual(ci.get_bool(), True)
            self.assertEqual(int(ci), 1)
            ci.set_bool(False)
            self.assertEqual(ci.get_bool(), False)
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_type_mismatch : Cannot set config[BoolValue] to Override: incorrect type',
        ])
        expected = Counter({'read_ok': 8, 'write_ok': 1, 'write_error': 1})
        self.do_access_compare(ci.accesses, expected, 'Basic Bool Config not working as expected')

    def test_ConfigURL(self):
        """ Tests for ConfigURL class """
        cfg = config2.LLConfigHandler()
        goodurls = [
            ('google', 'https://www.google.com'),
            ('ftp', "ftp://ftp.example.com"),
            ('localip', "http://192.168.1.1"),
        ]
        badurls = [
            ('invalid_spaces', "not a URL"),
            ('invalid_proto', "httpss://www.google.com"),
            ('invalid_domain', "http://.com"),
        ]

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            for url in goodurls:
                cfg.set_url(url[0], configtypes.URLstr(url[1]))

                goturl = cfg.get_url(url[0])
                self.assertEqual(goturl, url[1])
                self.assertEqual(type(goturl), str)

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            for url in badurls:
                cfg.set_url(url[0], configtypes.URLstr(url[1]))  # Format error
                goturl = cfg.get_url(url[0])                     # Read error
                self.assertEqual(goturl, '')
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[invalid_spaces]: format_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[invalid_spaces]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[invalid_proto]: format_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[invalid_proto]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[invalid_domain]: format_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[invalid_domain]: read_error'
        ])

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

    def test_ConfigScheduleInterval(self):
        """ Tests for config holding scheduler information """
        ci = configtypes.ConfigScheduleInterval('', '', 'Test', 10)
        self.assertEqual(ci.get_schedule_name(), 'Test', 'Schedule name not stored correctly')
        self.assertEqual(ci.get_int(), 10, 'Schedule interval not stored correctly')
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            ci.set_int(100000) # Value too large, should have no effect
        self.assertEqual(cm.output,
            ['WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_set : Cannot set config[] to 100000'])
        self.assertEqual(ci.get_int(), 10, 'Schedule interval not stored correctly')

        try:
            ci = configtypes.ConfigScheduleInterval('', '', '', 10)
            self.assertTrue(False, 'Expected RuntimeError to be raised because schedule is empty')
        except RuntimeError:
            pass # This is what we expect

    def test_ConfigDownloadTypes(self):
        """ Test the ConfigDownloadTypes, which can only be A,C,E,M or combinations """
        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            cdt = configtypes.ConfigDownloadTypes('', '', 'E')
            self.assertEqual(cdt.get_csv(), 'E')
            cdt.set_str('M,A')
            self.assertEqual(cdt.get_csv(), 'M,A')

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            cdt.set_str('M,A,X') # Write error, value doesn't change
        self.assertEqual(cm.output,
            ['WARNING:lazylibrarian.logger:MainThread : configtypes.py:_on_set : Cannot set config[] to M,A,X'])
        self.assertEqual(cdt.get_csv(), 'M,A')

    def set_basic_test_values(self, cfg: config2.LLConfigHandler):
        """ Helper function, sets some basic config values """
        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            cfg.set_str('somestr', 'abc')
            cfg.set_int('someint', 123)
            cfg.set_int('someint', 45)
            cfg.set_bool('abool', False)
            cfg.set_bool('boo', True)
            email = configtypes.Email('name@gmail.com')
            cfg.set_email('mail', email)

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            cfg.set_email('mail2', configtypes.Email('name@gmailmissingcom')) # Format Error
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[mail2]: format_error'
        ])

    def test_compare_basic_configs(self):
        """ Test that we can compare basic configs and tell if they differ """
        cfg1 = config2.LLConfigHandler()
        cfg2 = config2.LLConfigHandler()

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            self.set_basic_test_values(cfg1)
            self.set_basic_test_values(cfg2)

            self.assertTrue(config2.are_equivalent(cfg1, cfg2))

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            cfg1.set_int('a-new-int', 1)
            self.assertFalse(config2.are_equivalent(cfg1, cfg2))
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : config2.py:are_configdicts_equivalent : Array lengths differ: 6 != 5',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:are_equivalent : Base configs differ'
        ])

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            cfg2.set_int('a-new-int', 1)
            self.assertTrue(config2.are_equivalent(cfg1, cfg2))

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            cfg2.set_str('another-str', 'help')
            self.assertFalse(config2.are_equivalent(cfg1, cfg2))
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : config2.py:are_configdicts_equivalent : Array lengths differ: 6 != 7',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:are_equivalent : Base configs differ'
        ])

    def do_access_compare(self, got, expected, error):
        """ Helper function, validates that two access lists are the same """
        self.assertEqual(len(got), len(expected))
        for key in got:
            eac = expected[key]
            self.assertEqual(got[key], eac, f'[{key}]: {error}')

    def test_basic_types(self):
        """ Tests basic config types inside a ConfigHandler """
        cfg = config2.LLConfigHandler()
        self.set_basic_test_values(cfg)

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            self.assertEqual('abc', cfg.get_str('somestr'))
            self.assertEqual('abc', cfg['somestr'])
            self.assertEqual(45, cfg.get_int('someint'))
            self.assertEqual('45', cfg['someint'])
            self.assertEqual('name@gmail.com', cfg.get_email('mail'))
            self.assertFalse(cfg.get_bool('abool'))
            self.assertTrue(cfg.get_bool('boo'))
            self.assertEqual('True', cfg['boo'])

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            self.assertEqual('', cfg.get_email('mail2')) # Read Error
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[mail2]: read_error'
        ])

    def do_csv_ops(self, cfg: config2.LLConfigHandler):
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            cfg.set_csv('csv', configtypes.CSVstr('allan,bob,fred'))
            cfg.set_csv('csv2', configtypes.CSVstr(''))
            cfg.set_csv('csv3', configtypes.CSVstr(',,test')) # Format error
            cfg.set_csv('csv4', configtypes.CSVstr('"fred" bob and alice,test')) # Format error
            cfg.set_csv('csv5', configtypes.CSVstr('single'))
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[csv3]: format_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[csv4]: format_error'
        ])

    def test_csv(self):
        """ Test ConfigCSV handling """
        cfg = config2.LLConfigHandler()
        self.do_csv_ops(cfg)

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            self.assertEqual('allan,bob,fred', cfg.get_csv('csv'))
            self.assertEqual('', cfg.get_csv('csv2'))
            self.assertEqual('single', cfg.get_csv('csv5'))

        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            self.assertEqual('', cfg.get_csv('csv3')) # Read error
            self.assertEqual('', cfg.get_csv('csv4')) # Read error
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[csv3]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[csv4]: read_error'
        ])

    def test_read_error_counters(self):
        """ Test that read error counters are correct in lots of cases """
        cfg = config2.LLConfigHandler()
        self.test_csv()

        # Try to access non-existing keys
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            self.assertEqual('', cfg.get_str('does-not-exist'))
            self.assertEqual(0, cfg.get_int('does-not-exist'))
            self.assertEqual(False, cfg.get_bool('does-not-exist'))
            self.assertEqual('', cfg.get_csv('also-does-not'))
            self.assertEqual('', cfg['KeyDoesNotExist'])
        self.assertEqual(cm.output, [
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[does-not-exist]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[does-not-exist]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[does-not-exist]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[also-does-not]: read_error',
            'ERROR:lazylibrarian.logger:MainThread : config2.py:_handle_access_error : Config[KeyDoesNotExist]: read_error'
        ])

        ecs = cfg.get_error_counters()
        expectedecs = {
            'KeyDoesNotExist': Counter({'read_error': 1}),
            'does-not-exist': Counter({'read_error': 3}),
            'also-does-not': Counter({'read_error': 1})
        }
        self.do_access_compare(ecs, expectedecs, 'Errors  not as expected')

    def test_access_counters(self):
        """ Test that read/create counters work correctly when there are no errors """
        cfg = config2.LLConfigHandler()

        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            self.do_csv_ops(cfg)
            self.set_basic_test_values(cfg)

            # Access some of these items
            self.assertEqual('abc', cfg.get_str('somestr'))
            for _ in range(3):
                self.assertEqual(45, cfg.get_int('someint'))
            self.assertEqual('name@gmail.com', cfg.get_email('mail'))
            self.assertTrue(cfg.get_bool('boo'))

            self.assertEqual('allan,bob,fred', cfg.get_csv('csv'))
            for _ in range(3):
                self.assertEqual('single', cfg.get_csv('csv5'))

        acs = cfg.get_all_accesses()
        expectedacs = {
            'csv': Counter({'create_ok': 1, 'read_ok': 1}),
            'csv2': Counter({'create_ok': 1}),
            'csv5': Counter({'read_ok': 3, 'create_ok': 1}),
            'somestr': Counter({'create_ok': 1, 'read_ok': 1}),
            'someint': Counter({'read_ok': 3, 'create_ok': 1, 'write_ok': 1}),
            'abool': Counter({'create_ok': 1}),
            'boo': Counter({'create_ok': 1, 'read_ok': 1}),
            'mail': Counter({'create_ok': 1, 'read_ok': 1})
        }
        self.do_access_compare(acs, expectedacs, 'Access patterns not as expected')

        cfg.clear_access_counters()
        acs = cfg.get_all_accesses()
        self.do_access_compare(acs, {}, 'Clearing all access patterns did not work')

    def test_LLdefaults(self):
        """ Test setting the default LL config """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS)
        self.assertEqual(len(cfg.config), len(configdefs.BASE_DEFAULTS), 'Maybe there is a duplicate entry in BASE_DEFAULTS')
        self.do_access_compare({}, cfg.get_all_accesses(), 'There should be no changes from defaults')
        self.assertEqual(cfg.get_str('AUTH_TYPE'), 'BASIC')

    def test_force_lower(self):
        """ Test various string configss that have force_lower and make sure they are. """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=COMPLEX_INI_FILE)

        for key, item in cfg.config.items():
            if item.get_force_lower():
                self.assertEqual(item.get_str(), str(item).lower(), f'force_lower has not worked for {key}')

        has_uppercase = cfg['API_KEY']
        self.assertNotEqual(has_uppercase, has_uppercase.lower())

    def test_configread_nodefs_defaultini(self):
        """ Test reading a near-default ini file, but without base definitions """
        with self.assertLogs('lazylibrarian.logger', level='INFO') as cm:
            # Because no defaults are loaded, every item will case a warning
            cfg = config2.LLConfigHandler(defaults=None, configfile=SMALL_INI_FILE)
        self.assertEqual(cm.output, [
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.EBOOK_DIR in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.AUDIO_DIR in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.DOWNLOAD_DIR in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.ALTERNATE_DIR in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.TESTDATA_DIR in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.LOGLEVEL in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.NO_IPV6 in config',
            'WARNING:lazylibrarian.logger:MainThread : config2.py:_load_section : Unknown option GENERAL.SSL_VERIFY in config'
        ])
        acs = cfg.get_all_accesses()
        self.do_access_compare(acs, {}, 'Loading ini without defaults should not load anything')

    def test_configread_defaultini(self):
        """ Test reading a near-default ini file, with all of the base definitions loads """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=SMALL_INI_FILE)
        acs = cfg.get_all_accesses()
        expectedacs = {
            'GENERAL.LOGLEVEL': Counter({'write_ok': 1}),
            'GENERAL.NO_IPV6': Counter({'write_ok': 1}),
            'GENERAL.EBOOK_DIR': Counter({'write_ok': 1}),
            'GENERAL.AUDIO_DIR': Counter({'write_ok': 1}),
            'GENERAL.ALTERNATE_DIR': Counter({'write_ok': 1}),
            'GENERAL.TESTDATA_DIR': Counter({'write_ok': 1}),
            'GENERAL.DOWNLOAD_DIR': Counter({'write_ok': 1})
         }
        self.do_access_compare(acs, expectedacs, 'Loading ini file did not modify the expected values')

    def test_configread_nondefault(self):
        """ Test reading a more complex config.ini file """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=COMPLEX_INI_FILE)
        acs = cfg.get_all_accesses()
        expectedacs = {
            "GENERAL.LOGDIR": Counter({'write_ok': 1}),
            "GENERAL.LOGLIMIT": Counter({'write_ok': 1}),
            "GENERAL.LOGFILES": Counter({'write_ok': 1}),
            "GENERAL.LOGSIZE": Counter({'write_ok': 1}),
            "GENERAL.LOGLEVEL": Counter({'write_ok': 1}),
            "GENERAL.MAG_TAB": Counter({'write_ok': 1}),
            "GENERAL.COMIC_TAB": Counter({'write_ok': 1}),
            "GENERAL.AUDIO_TAB": Counter({'write_ok': 1}),
            "GENERAL.API_ENABLED": Counter({'write_ok': 1}),
            "GENERAL.API_KEY": Counter({'write_ok': 1}),
            "GENERAL.IMP_CALIBREDB": Counter({'write_ok': 1}),
            "GENERAL.CALIBRE_USE_SERVER": Counter({'write_ok': 1}),
            "GENERAL.CALIBRE_SERVER": Counter({'write_ok': 1}),
            "GENERAL.IMP_NOSPLIT": Counter({'write_ok': 1}),
            "TELEMETRY.SERVER_ID": Counter({'write_ok': 1}),
            "GENERAL.EBOOK_DIR": Counter({'write_ok': 1}),
            "GENERAL.AUDIO_DIR": Counter({'write_ok': 1}),
            "GENERAL.ALTERNATE_DIR": Counter({'write_ok': 1}),
            "GENERAL.TESTDATA_DIR": Counter({'write_ok': 1}),
            "GENERAL.DOWNLOAD_DIR": Counter({'write_ok': 1}),
            "POSTPROCESS.AUDIOBOOK_DEST_FOLDER": Counter({'write_ok': 1}),
            "NEWZNAB.0.DISPNAME": Counter({'write_ok': 1}),
            "NEWZNAB.0.ENABLED": Counter({'write_ok': 1}),
            "NEWZNAB.0.HOST": Counter({'write_ok': 1}),
            "NEWZNAB.0.API": Counter({'write_ok': 1}),
            "NEWZNAB.0.GENERALSEARCH": Counter({'write_ok': 1}),
            "NEWZNAB.0.BOOKSEARCH": Counter({'write_ok': 1}),
            "NEWZNAB.0.BOOKCAT": Counter({'write_ok': 1}),
            "NEWZNAB.0.UPDATED": Counter({'write_ok': 1}),
            "NEWZNAB.0.APILIMIT": Counter({'write_ok': 1}),
            "NEWZNAB.0.RATELIMIT": Counter({'write_ok': 1}),
            "NEWZNAB.0.DLTYPES": Counter({'write_ok': 1}),
            "NEWZNAB.1.DISPNAME": Counter({'write_ok': 1}),
            'NEWZNAB.1.HOST': Counter({'write_ok': 1, 'read_ok': 1}),
            'APPRISE.0.NAME': Counter({'write_ok': 1}),
            'APPRISE.0.DISPNAME': Counter({'write_ok': 1}),
            'APPRISE.0.SNATCH': Counter({'write_ok': 1}),
            'APPRISE.0.DOWNLOAD': Counter({'write_ok': 1}),
            'APPRISE.0.URL': Counter({'write_ok': 1, 'read_ok': 1}),
        }
        self.do_access_compare(acs, expectedacs, 'Loading complex ini file did not modify the expected values')

    def test_configread_nondefault_access(self):
        """ Test accessing a more complex config.ini file """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=COMPLEX_INI_FILE)

        self.assertEqual(cfg.get_array_entries('APPRISE'), 2, 'Expected two entries for APPRISE')
        self.assertEqual(cfg.get_array_entries('NEWZNAB'), 3, 'Expected two entries for NEWZNAB')
        self.assertEqual(cfg.get_array_entries('RSS'), 1, 'Expected one empty entry for RSS')
        self.assertEqual(cfg.get_array_entries('DOESNOTEXIST'), 0, 'Expected no entries')

        NEWZNAB = cfg.get_array_dict('NEWZNAB', 0)
        self.assertIsNotNone(NEWZNAB, 'Expected to get a NEWZNAB object')
        if NEWZNAB:
            self.assertEqual(NEWZNAB['DISPNAME'].get_str(), 'NZBtester', 'NEWZNAB.0.DISPNAME not loaded correctly')
            self.assertEqual(str(NEWZNAB['DISPNAME']), 'NZBtester', 'Default string return on array is not working')
            self.assertTrue(NEWZNAB['ENABLED'].get_bool(), 'NEWZNAB.0.ENABLED not loaded correctly')
            self.assertEqual(NEWZNAB['APILIMIT'].get_int(), 12345, 'NEWZNAB.0.APILIMIT not loaded correctly')

    def remove_test_file(self, filename) -> bool:
        """ Remove a file used for testing. Returns True if a file was removed """
        import os
        try:
            os.remove(filename)
            return True
        except OSError as e:
            self.assertEqual(e.errno, 2, 'Error removing test file is not as expected')
            return False

    def test_save_config(self):
        """ Test saving config file """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=SMALL_INI_FILE)
        try:
            TESTFILE = 'test-small.ini'
            count = cfg.save_config(TESTFILE, False) # Save only non-default values
            self.assertEqual(count, 7, 'Saving default config.ini has unexpected # of changes')
            cfgnew = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=TESTFILE)
            self.assertTrue(config2.are_equivalent(cfg, cfgnew), f'Save error: {TESTFILE} is not the same as original file!')
        finally:
            self.assertEqual(self.remove_test_file('test-small.ini'), True, 'Could not remove test-small.ini')

        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=COMPLEX_INI_FILE)
        with self.assertLogs('lazylibrarian.logger', level='WARN'):
            count = cfg.save_config('?*/\\invalid<>file', False) # Save only non-default values
        self.assertEqual(count, -1, 'Should not be able to save to invalid file name')
        try:
            TESTFILE = 'test-changed.ini'
            count = cfg.save_config(TESTFILE, False) # Save only non-default values
            self.assertEqual(count, 39, 'Saving config.ini has unexpected # of non-default items')
            cfgnew = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=TESTFILE)
            self.assertTrue(config2.are_equivalent(cfg, cfgnew), f'Save error: {TESTFILE} is not the same as original file!')
        finally:
            self.assertEqual(self.remove_test_file(TESTFILE), True, 'Could not remove test-changed.ini')

        try:
            TESTFILE = 'test-all.ini'
            count = cfg.save_config(TESTFILE, True) # Save everything.
            self.assertEqual(count, 512, 'Saving config.ini has unexpected total # of items')
            cfgnew = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=TESTFILE)
            self.assertTrue(config2.are_equivalent(cfg, cfgnew), f'Save error: {TESTFILE} is not the same as original file!')
        finally:
            self.assertEqual(self.remove_test_file(TESTFILE), True, 'Could not remove test-all.ini')

    def test_save_config_and_backup_old(self):
        """ Test saving config file while keeping the old one as a .bak file """
        import os.path, shutil
        TEST_FILE = syspath('./unittests/testdata/test.ini')
        shutil.copyfile(COMPLEX_INI_FILE, TEST_FILE)
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=TEST_FILE)

        # delete potential backup file before starting
        backupfile = syspath(cfg.configfilename + '.bak')
        self.remove_test_file(backupfile)

        try:
            with self.assertNoLogs('lazylibrarian.logger', level='WARN'): # Expect only INFO messages
                count = cfg.save_config_and_backup_old(False)
            self.assertEqual(count, 39, 'Saving config.ini has unexpected total # of items')
            self.assertTrue(os.path.isfile(backupfile), 'Backup file does not exist')

            cfgbak = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=backupfile)
            self.assertTrue(config2.are_equivalent(cfg, cfgbak), '.bak file is not the same as original file!')

            # Verify that it works when .bak file exists as well:
            with self.assertNoLogs('lazylibrarian.logger', level='WARN'): # Expect only INFO messages
                count = cfg.save_config_and_backup_old(False)
            self.assertEqual(count, 39, 'Saving config.ini has unexpected total # of items')
            self.assertTrue(self.remove_test_file(backupfile), 'Could not delete backup file')

        finally:
            self.remove_test_file(TEST_FILE)
            self.remove_test_file(backupfile)

    def test_post_load_fixup(self):
        """ Verify that the post_load_fixup routine does the right thing """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS)

        import lazylibrarian
        # Ensure we are in a known state
        lazylibrarian.SHOW_EBOOK = 0
        lazylibrarian.SHOW_AUDIO = 1
        lazylibrarian.SHOW_MAGS = 0
        lazylibrarian.SHOW_COMICS = 0

        # Set some values that trigger warnings/fixes
        import os
        for fname in ['EBOOK_DEST_FILE', 'MAG_DEST_FILE', 'AUDIOBOOK_DEST_FILE', 'AUDIOBOOK_SINGLE_FILE']:
            value = cfg.config[fname].get_str() + os.sep # These will be removed in post
            cfg.config[fname].set_str(value)
        cfg.config['HTTP_LOOK'].set_str('default')

        # Set some values that cause changes
        cfg.config['EBOOK_TAB'].set_bool(True)
        cfg.config['AUDIO_TAB'].set_bool(False)
        cfg.config['MAG_TAB'].set_bool(True)
        cfg.config['COMIC_TAB'].set_bool(True)
        cfg.config['HOMEPAGE'].set_str('AudioBooks')
        cfg.config['SSL_CERTS'].set_str('dir-doesnot-exist')


        with self.assertLogs('lazylibrarian.logger', level='WARN') as cm:
            warnings = cfg.post_load_fixup()
        # Do not test for specific messages as they depend on the OS
        self.assertEqual(len(cm.output), 6, 'Unexpected # of log messages')
        self.assertEqual(cfg.config['LOGDIR'].get_str()[-4:], 'Logs', 'LOGDIR not set')
        self.assertEqual(str(cfg.config['AUDIOBOOK_DEST_FOLDER']), str(cfg.config['EBOOK_DEST_FOLDER']))
        self.assertEqual(str(cfg.config['HTTP_LOOK']), 'bookstrap')

        self.assertTrue(lazylibrarian.SHOW_EBOOK)
        self.assertFalse(lazylibrarian.SHOW_AUDIO)
        self.assertTrue(lazylibrarian.SHOW_MAGS)
        self.assertTrue(lazylibrarian.SHOW_COMICS)

        self.assertEqual(str(cfg.config['HOMEPAGE']), '', 'HOMEPAGE cannot be audio if that is disabled')
        self.assertEqual(warnings, 6, 'Unexpected # of warnings from fixup')

        # Second run with different inputs
        for fname in ['EBOOK_DEST_FILE', 'MAG_DEST_FILE', 'AUDIOBOOK_DEST_FILE', 'AUDIOBOOK_SINGLE_FILE']:
            cfg.config[fname].reset_to_default()
        cfg.config['HOMEPAGE'].set_str('eBooks')
        with self.assertNoLogs('lazylibrarian.logger', level='INFO'):
            warnings = cfg.post_load_fixup()
        self.assertEqual(str(cfg.config['HOMEPAGE']), 'eBooks', 'Should not have changed HOMEPAGE')
        self.assertEqual(warnings, 0, 'Expected no warnings here')

    def test_array_entry_usage(self):
        """ Verify that array entries can be added to and deleted """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=COMPLEX_INI_FILE)
        #with self.assertLogs('lazylibrarian.logger', level='WARN'):

        array = cfg.get_array('NOPEDOESNOTEXIST')
        self.assertIsNone(array, 'Non-existent array type must not be found')

        self.assertEqual(cfg.get_array_entries('APPRISE'), 2, 'This test assumes 2 APPRISE entries')
        array = cfg.get_array('APPRISE')
        self.assertIsNotNone(array, 'APPRISE array must exist')
        if array:
            self.assertTrue(array.is_in_use(0), 'This test assumes there is an Apprise[0] entry in use')
            self.assertFalse(array.is_in_use(1), 'This test assumes there is an empty Apprise[1] entry')
            # A user is removing the URL from the first APPRISE entry, making it invalid
            array[0]['URL'].set_str('')
            self.assertFalse(array.is_in_use(0), 'An empty URL should mean this item is not in use!')

            # A user adds a URL to the formerly empty item, making it valid
            array[1]['URL'].set_str('http://testing')
            self.assertTrue(array.is_in_use(1), 'The entry should now be in use as the URL is not empty')

            # We now save, clean up empty items and rename them
            array.cleanup_for_save()

        # Re-get the array and make sure it's valid
        self.assertEqual(cfg.get_array_entries('APPRISE'), 1, 'There should be just one entry left')
        array = cfg.get_array('APPRISE')
        if array:
            self.assertTrue(array.is_in_use(0), 'The renumbering did not work correctly')
            array.ensure_empty_end_item()
            self.assertEqual(cfg.get_array_entries('APPRISE'), 2, 'We should now have two entries')
            self.assertFalse(array.is_in_use(1), 'The last entry must be empty at this stage')






