#  This file is part of Lazylibrarian.
#
# Purpose:
#   Testing the config module, making sure configs are read and written correctly

import unittesthelpers
import lazylibrarian
from lazylibrarian import config

class ConfigTest(unittesthelpers.LLTestCase):
 
    # Initialisation code that needs to run only once
    @classmethod
    def setUpClass(cls) -> None:
        cls.setConfigFile('an invalid file name*')
        super().setDoAll(False)
        return super().setUpClass()

    # Is CONFIG_DEFINITIONS valid?
    def test_CONFIG_DEFINITIONS(self):
        # Are all default keys in upper case?
        for key in config.CONFIG_DEFINITIONS.keys():
            self.assertEqual(key, key.upper(), r'Default key {key} is not UPPER CASE')

        # Are all CONFIG_DEFINITIONS typed correctly?
        for key in config.CONFIG_DEFINITIONS.keys():
            dtype, _, default = config.CONFIG_DEFINITIONS[key]
            self.assertTrue(dtype in ['int', 'bool', 'str'], f"{dtype} is not a valid type for {key}")
            if dtype == 'int':
                self.assertIsInstance(default, int, f'Default for {key} is not int: {default}')
            if dtype == 'str':
                self.assertIsInstance(default, str, f'Default for {key} is not str: {default}')
            if dtype == 'bool':
                if type(default) == int:
                    self.assertIn(default, [0, 1], f'Default for {key} is not 0 or 1: {default}')
                else:
                    self.assertIsInstance(default, bool, f'Default for {key} is not bool: {default}')

        # Are all of these values low case by default?
        for key in config.FORCE_LOWER:
            _, _, default = config.CONFIG_DEFINITIONS[key]
            self.assertEqual(default, default.lower(),
                f'Default value for {key} is not in lower case: {default}')


    # Are all the config lists properly defined in CONFIG_DEFINITIONS?
    def test_CONFIG_XXX(self):
        def validate_list(cfg):
            for key in cfg:
                self.assertIn(key, config.CONFIG_DEFINITIONS, 
                    f'key {key} is not defined in CONFIG_DEFINITIONS')

        validate_list(config.CONFIG_GIT)
        validate_list(config.CONFIG_NONWEB)
        validate_list(config.FORCE_LOWER)


    # Make sure all config settings are their defaults
    def test_default_CONFIG(self):
        # Config valus here are modified after loading config.ini
        MODIFIED_KEYS = ['LOGDIR', 'EBOOK_DEST_FOLDER', 'AUDIOBOOK_DEST_FOLDER', 'COMIC_DEST_FOLDER', 'MAG_DEST_FOLDER']

        for key in lazylibrarian.CONFIG.keys() - MODIFIED_KEYS:
            _, _, default = config.CONFIG_DEFINITIONS[key]
            value = lazylibrarian.CONFIG[key]
            self.assertEqual(value, default, 
                f'CONFIG value for {key}: {value} != {default}')

    # Test that the CFG object mirrors CONFIG even when config.ini didn't exist
    def test_default_CFG_sections(self):
        sections = []
        for key in config.CONFIG_DEFINITIONS.keys():
            _, section, _ = config.CONFIG_DEFINITIONS[key]
            if not section in sections:
                sections.append(section)

        for section in sections:
            self.assertIn(section, lazylibrarian.CFG.sections())
        # There are 6 "array" sections where CFG gets an additional item added
        self.assertEqual(len(sections) + 6, len(lazylibrarian.CFG.sections()))


# Functions still to test:
#   check_ini_section
#   check_setting
#   readConfigFile
#   config_read (with/without reload)
#   config_write (full/partial)
#   add_newz_slot
#   add_torz_slot
#   add_rss_slot
#   add_irc_slot
#   add_gen_slot
#   add_apprise_slot
