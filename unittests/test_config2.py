
#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the new config2 module

from collections import Counter

from unittesthelpers import LLTestCase
from lazylibrarian import config2, configdefs, configtypes
from lazylibrarian.common import syspath

# Ini files used for testing load/save functions. 
# If these change, many test cases need to be updated. Run to find out which ones :)
DEFAULT_INI_FILE = './unittests/testdata/testconfig-defaults.ini' 
NONDEF_INI_FILE = './unittests/testdata/testconfig-nondefault.ini'

class Config2Test(LLTestCase):
    
    @classmethod
    def setUpClass(cls) -> None:
        cls.setConfigFile('No Config File*')
        super().setDoAll(False)
        return super().setUpClass()

    def test_ConfigStr(self):
        ci = configtypes.ConfigStr('Section', 'StrValue', 'Default')
        self.assertEqual(ci.get_str(), 'Default')
        self.assertEqual(str(ci), 'Default')

        ci.set_str('Override')
        self.assertEqual(ci.get_str(), 'Override')

        ci.set_int(2)                          # Write Error
        self.assertEqual(ci.get_int(), 0)      # Read Error
        ci.set_bool(True)                      # Write Error
        self.assertEqual(ci.get_bool(), False) # Read Error

        expected = Counter({'read_ok': 3, 'write_ok': 1, 'write_error': 2, 'read_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Basic String Config not working as expected')
        
    def test_ConfigInt(self):
        ci = configtypes.ConfigInt('Section', 'IntValue', 42)
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

        expected = Counter({'read_ok': 5, 'write_ok': 1, 'write_error': 2, 'read_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Basic Int Config not working as expected')
        
    def test_ConfigRangedInt(self):
        ci = configtypes.ConfigRangedInt('Section', 'RangedIntValue', 42, 10, 1000)
        self.assertEqual(int(ci), 42)

        ci.set_int(5)                          # Write Error
        self.assertEqual(ci.get_int(), 42)

        ci.set_int(1100)                       # Write Error
        self.assertEqual(int(ci), 42)

        ci.set_int(100)                       
        self.assertEqual(int(ci), 100)

        expected = Counter({'read_ok': 4, 'write_ok': 1, 'write_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Ranged Int Config not working as expected')
        
    def test_ConfigPerm(self):
        ci = configtypes.ConfigPerm('Section', 'PermissionValue', 0o777)
        self.assertEqual(ci.get_int(), 0o777)
        self.assertEqual(str(ci), '0o777')

        ci.set_int(1000000)                     # Write Error
        self.assertEqual(int(ci), 0o777)

        ci.set_int(-8)                          # Write Error
        self.assertEqual(int(ci), 0o777)

        ci.set_int(57)                          # Fine, if awkward
        self.assertEqual(int(ci), 57)

        ci.set_str('0o321')
        self.assertEqual(int(ci), 0o321)

        expected = Counter({'read_ok': 6, 'write_ok': 2, 'write_error': 2})
        self.do_access_compare(ci.accesses, expected, 'Permission config working as expected')
        
    def test_ConfigBool(self):
        ci = configtypes.ConfigBool('Section', 'BoolValue', True)
        self.assertEqual(ci.get_int(), 1)      # We can read bools as int
        self.assertEqual(ci.get_str(), 'True')
        self.assertEqual(ci.get_bool(), True)  
        self.assertEqual(int(ci), 1)           # We can read bools as default int

        ci.set_str('Override')                 # Write Error
        self.assertEqual(ci.get_str(), 'True')

        ci.set_int(2)                          # ok, writes as True/1
        self.assertEqual(ci.get_bool(), True)      
        self.assertEqual(int(ci), 1)      
        ci.set_bool(False)                      
        self.assertEqual(ci.get_bool(), False) 

        expected = Counter({'read_ok': 8, 'write_ok': 1, 'write_error': 1})
        self.do_access_compare(ci.accesses, expected, 'Basic Bool Config not working as expected')
        
    def test_ConfigURL(self):
        cfg = config2.LLConfigHandler()
        testurl = [
            ('google', 'https://www.google.com', True),
            ('ftp', "ftp://ftp.example.com", True),
            ('localip', "http://192.168.1.1", True),
            ('invalid_spaces', "not a URL", False),
            ('invalid_proto', "httpss://www.google.com", False),
            ('invalid_domain', "http://.com", False),
        ]
        for url in testurl:
            cfg.set_url(url[0], configtypes.URLstr(url[1]))

        for url in testurl:
            goturl = cfg.get_url(url[0])
            if url[2]:
                self.assertEqual(goturl, url[1])
                self.assertEqual(type(goturl), str)
            else:
                self.assertEqual(goturl, '')

    def set_basic_test_values(self, cfg: config2.LLConfigHandler):
        cfg.set_str('somestr', 'abc')
        cfg.set_int('someint', 123)
        cfg.set_int('someint', 45)
        cfg.set_bool('abool', False)
        cfg.set_bool('boo', True)
        email = configtypes.Email('name@gmail.com')
        cfg.set_email('mail', email)
        cfg.set_email('mail2', configtypes.Email('name@gmailmissingcom'))

    def test_compare_basic_configs(self):
        """ Test that we can compare basic configs and tell if they differ """
        cfg1 = config2.LLConfigHandler()
        cfg2 = config2.LLConfigHandler()

        self.set_basic_test_values(cfg1)
        self.set_basic_test_values(cfg2)

        self.assertTrue(config2.are_equivalent(cfg1, cfg2))

        cfg1.set_int('a-new-int', 1)
        self.assertFalse(config2.are_equivalent(cfg1, cfg2))

        cfg2.set_int('a-new-int', 1)
        self.assertTrue(config2.are_equivalent(cfg1, cfg2))

        cfg2.set_int('another-str', 'help')
        self.assertFalse(config2.are_equivalent(cfg1, cfg2))

    def do_access_compare(self, got, expected, error):
        self.assertEqual(len(got), len(expected))
        for key in got:
            eac = expected[key]
            self.assertEqual(got[key], eac, f'[{key}]: {error}')

    def test_basic_types(self):
        cfg = config2.LLConfigHandler()
        self.set_basic_test_values(cfg)

        self.assertEqual('abc', cfg.get_str('somestr'))
        self.assertEqual('abc', cfg['somestr'])
        self.assertEqual(45, cfg.get_int('someint'))
        self.assertEqual('45', cfg['someint'])
        self.assertEqual('name@gmail.com', cfg.get_email('mail'))
        self.assertEqual('', cfg.get_email('mail2'))
        self.assertFalse(cfg.get_bool('abool'))
        self.assertTrue(cfg.get_bool('boo'))
        self.assertEqual('True', cfg['boo'])

    def do_csv_ops(self, cfg: config2.LLConfigHandler):
        cfg.set_csv('csv', configtypes.CSVstr('allan,bob,fred'))
        cfg.set_csv('csv2', configtypes.CSVstr(''))
        cfg.set_csv('csv3', configtypes.CSVstr(',,test'))
        cfg.set_csv('csv4', configtypes.CSVstr('"fred" bob and alice,test'))
        cfg.set_csv('csv5', configtypes.CSVstr('single'))

    def test_csv(self):
        cfg = config2.LLConfigHandler()
        self.do_csv_ops(cfg)

        self.assertEqual('allan,bob,fred', cfg.get_csv('csv'))
        self.assertEqual('', cfg.get_csv('csv2'))
        self.assertEqual('', cfg.get_csv('csv3'))
        self.assertEqual('', cfg.get_csv('csv4'))
        self.assertEqual('single', cfg.get_csv('csv5'))

    def test_error_counters(self):
        cfg = config2.LLConfigHandler()
        self.test_csv()

        # Try to access non-existing keys
        self.assertEqual('', cfg.get_str('does-not-exist'))
        self.assertEqual(0, cfg.get_int('does-not-exist'))
        self.assertEqual(False, cfg.get_bool('does-not-exist'))
        self.assertEqual('', cfg.get_csv('also-does-not'))

        self.assertEqual('', cfg['KeyDoesNotExist'])

        ecs = cfg.get_error_counters()
        expectedecs = {
            'KeyDoesNotExist': Counter({'read_error': 1}), 
            'does-not-exist': Counter({'read_error': 3}), 
            'also-does-not': Counter({'read_error': 1})
        }
        self.do_access_compare(ecs, expectedecs, 'Errors  not as expected')

    def test_access_counters(self):
        cfg = config2.LLConfigHandler()
        self.do_csv_ops(cfg)
        self.set_basic_test_values(cfg)

        # Access some of these items
        self.assertEqual('abc', cfg.get_str('somestr'))
        for _ in range(3):
            self.assertEqual(45, cfg.get_int('someint'))
        self.assertEqual('name@gmail.com', cfg.get_email('mail'))
        self.assertTrue(cfg.get_bool('boo'))

        self.assertEqual('allan,bob,fred', cfg.get_csv('csv'))
        self.assertEqual('', cfg.get_csv('csv3'))
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
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS)
        self.assertEqual(len(cfg.config), len(configdefs.BASE_DEFAULTS), 'Maybe there is a duplicate entry in BASE_DEFAULTS')
        self.do_access_compare({}, cfg.get_all_accesses(), 'There should be no changes from defaults')
        self.assertEqual(cfg.get_str('AUTH_TYPE'), 'BASIC')

    def test_configread_nodefs_defaultini(self):
        """ Test reading a near-default ini file, but without base definitions """
        cfg = config2.LLConfigHandler(defaults=None, configfile=DEFAULT_INI_FILE)
        acs = cfg.get_all_accesses()
        self.do_access_compare(acs, {}, 'Loading ini without defaults should not load anything')

    def test_configread_defaultini(self):
        """ Test reading a near-default ini file, with all of the base definitions loads """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=DEFAULT_INI_FILE)
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
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=NONDEF_INI_FILE)
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
            "POSTPROCESS.EBOOK_DEST_FOLDER": Counter({'write_ok': 1}),
            "POSTPROCESS.AUDIOBOOK_DEST_FOLDER": Counter({'write_ok': 1}),
            "POSTPROCESS.COMIC_DEST_FOLDER": Counter({'write_ok': 1}),
            "POSTPROCESS.MAG_DEST_FOLDER": Counter({'write_ok': 1}),
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
            "NEWZNAB.1.GENERALSEARCH": Counter({'write_ok': 1}),
            "NEWZNAB.1.BOOKSEARCH": Counter({'write_ok': 1}),
            "NEWZNAB.1.DLTYPES": Counter({'write_ok': 1}),
            "TORZNAB.0.DISPNAME": Counter({'write_ok': 1}),
            "TORZNAB.0.GENERALSEARCH": Counter({'write_ok': 1}),
            "TORZNAB.0.BOOKSEARCH": Counter({'write_ok': 1}),
            "TORZNAB.0.DLTYPES": Counter({'write_ok': 1}),
            "RSS_.0.DISPNAME": Counter({'write_ok': 1}),
            "GEN_.0.DISPNAME": Counter({'write_ok': 1}),
            "IRC_.0.DISPNAME": Counter({'write_ok': 1}),
            "APPRISE_.0.NAME": Counter({'write_ok': 1}),
            "APPRISE_.0.DISPNAME": Counter({'write_ok': 1}),
        }
        self.do_access_compare(acs, expectedacs, 'Loading complex ini file did not modify the expected values')

    def test_configread_nondefault_access(self):
        """ Test accessing a more complex config.ini file """
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=NONDEF_INI_FILE)

        self.assertEqual(cfg.get_array_entries('APPRISE'), 1, 'Expected one entry for APPRISE') 
        self.assertEqual(cfg.get_array_entries('NEWZNAB'), 2, 'Expected two entries for NEWZNAB') 
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
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=DEFAULT_INI_FILE)
        try:
            count = cfg.save_config('test-small.ini', False) # Save only non-default values
            self.assertEqual(count, 7, 'Saving default config.ini has unexpected # of changes')
        finally:
            self.assertEqual(self.remove_test_file('test-small.ini'), True, 'Could not remove test-small.ini')

        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=NONDEF_INI_FILE)
        count = cfg.save_config('?*/\\invalid<>file', False) # Save only non-default values
        self.assertEqual(count, -1, 'Should not be able to save to invalid file name')
            
        try:
            count = cfg.save_config('test-changed.ini', False) # Save only non-default values
            self.assertEqual(count, 48, 'Saving config.ini has unexpected # of non-default items')
        finally:
            self.assertEqual(self.remove_test_file('test-changed.ini'), True, 'Could not remove test-changed.ini')

        try:
            count = cfg.save_config('test-all.ini', True) # Save everything.
            self.assertEqual(count, 559, 'Saving config.ini has unexpected total # of items')
        finally:
            self.assertEqual(self.remove_test_file('test-all.ini'), True, 'Could not remove test-all.ini')

    def test_save_config_and_backup_old(self):
        """ Test saving config file while keeping the old one as a .bak file """
        import os.path, shutil
        TEST_FILE = syspath('./unittests/testdata/test.ini')
        shutil.copyfile(NONDEF_INI_FILE, TEST_FILE)
        cfg = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=TEST_FILE)

        # delete potential backup file before starting
        backupfile = syspath(cfg.configfilename + '.bak')
        self.remove_test_file(backupfile)

        try:
            count = cfg.save_config_and_backup_old(False) 
            self.assertEqual(count, 48, 'Saving config.ini has unexpected total # of items')
            self.assertTrue(os.path.isfile(backupfile), 'Backup file does not exist')

            cfgbak = config2.LLConfigHandler(defaults=configdefs.BASE_DEFAULTS, configfile=backupfile)
            self.assertTrue(config2.are_equivalent(cfg, cfgbak), '.bak file is not the same as original file!')

            # Verify that it works when .bak file exists as well:
            count = cfg.save_config_and_backup_old(False) 
            self.assertEqual(count, 48, 'Saving config.ini has unexpected total # of items')
            self.assertTrue(self.remove_test_file(backupfile), 'Could not delete backup file')

        finally:
            self.remove_test_file(TEST_FILE)
            self.remove_test_file(backupfile)
