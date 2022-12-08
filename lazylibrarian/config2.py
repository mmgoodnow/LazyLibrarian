#  This file will be part of Lazylibrarian.
#
# Purpose:
#   Type-aware handling config.ini, access to its properties, etc.
#   Intended to entirely replace the previous file, config.py, as
#   well as many global variables

from typing import Dict, List, TypedDict, Tuple, Type
from copy import deepcopy
from configparser import ConfigParser
from collections import Counter
from os import path, sep

import lazylibrarian
from lazylibrarian.configtypes import ConfigItem, ConfigStr, ConfigBool, ConfigInt, ConfigEmail, ConfigCSV, \
    ConfigURL, Email, CSVstr, URLstr, ValidStrTypes, ValidTypes
from lazylibrarian.configdefs import ARRAY_DEFS
from lazylibrarian import logger
from lazylibrarian.formatter import thread_name
from lazylibrarian.common import syspath, path_exists

ConfigDict = Dict[str, ConfigItem]

""" Main configuration handler for LL """
class LLConfigHandler():
    config: ConfigDict
    arrays: Dict[Tuple[str, int], ConfigDict]
    errors: Dict[str, Counter]
    configfilename: str

    def __init__(self, defaults: List[ConfigItem]|None=None, configfile: str|None=None):
        self.config = dict()
        self.errors = dict()
        self.arrays = dict()
        self._copydefaults(self.config, defaults)

        if configfile:
            self.configfilename = configfile
            parser = ConfigParser()
            parser.optionxform = lambda optionstr: optionstr.upper()
            parser.read(configfile)
            for section in parser.sections():
                if section[-1:].isdigit(): 
                    self._load_array_section(section, parser)
                else:
                    self._load_section(section, parser, self.config)
        else:
            self.configfilename = ''

    def _copydefaults(self, config: ConfigDict, defaults: List[ConfigItem]|None=None, index: int|None=None):
        """ Copy the default values and settings for the given config """
        if defaults:
            for config_item in defaults:
                key = config_item.key
                config[key] = deepcopy(config_item)
                if index != None and index >= 0: # It's an array
                    config[key].section = config[key].section % index
                        
    def _load_section(self, section:str, parser:ConfigParser, config: ConfigDict):
        """ Load a section of an ini file """
        for option in parser.options(section):
            if option in config:
                config_item = config[option]
                if not config_item.update_from_parser(parser, option):
                    logger.warn(f"Error loading {section}.{option} as {parser.get(section, option)}")
            else:
                logger.warn(f"Unknown option {section}.{option} in config")

    def _load_array_section(self, section:str, parser:ConfigParser):
        """ Load a section of an ini file, where that section is part of an array """
        arrayname = section[:-1].upper() # Assume we have < 10 items! 
        index = int(section[-1:])
        defaults = ARRAY_DEFS[arrayname] if arrayname in ARRAY_DEFS else None
        if defaults: 
            logger.debug(f"Loading array {arrayname} index {index}")
            if not (arrayname, index) in self.arrays:
                self.arrays[(arrayname, index)] = dict()
            self._copydefaults(self.arrays[(arrayname, index)], defaults, index)
            array = self.arrays[(arrayname, index)]
            self._load_section(section, parser, array)
        else:
            logger.warn(f"Cannot load array {section}: Undefined")

    def get_config(self, section: str, key: str) -> ConfigItem|None:
        if key in self.config:
            
            return 
        return None

    """ Handle array entries """
    def get_array_entries(self, wantname: str) -> int:
        """ Return number of entries in a particular array config """
        rc = 0
        if not wantname:
            return rc
        for (name, _), _ in self.arrays.items():
            if name == wantname or name[:len(wantname)] == wantname:
                rc += 1
        return rc

    def get_array_dict(self, wantname: str, wantindex: int) -> ConfigDict|None:
        """ Return the complete config for an entry, like ('APPRISE', 0) """
        if (wantname, wantindex) in self.arrays:
            return self.arrays[(wantname, wantindex)]
        else:
            return None

    """ Plain strings """
    def get_str(self, key: str) -> str:
        if key in self.config:
            return self.config[key].get_str()
        else:
            self._handle_access_error(key, 'read_error')
            return ''

    def __getitem__(self, __name: str) -> str:
        """ Make it possible to use CONFIG['name'] to access a string config directly """
        return self.get_str(__name) 

    def set_str(self, key: str, value: str):
        if key in self.config:
            self.config[key].set_str(value)
        else:
            self.create_str_key(ConfigStr, key, value)

    """ Integers """
    def get_int(self, key: str) -> int:
        if key in self.config:
            return self.config[key].get_int()
        else:
            self._handle_access_error(key, 'read_error')
            return 0

    def set_int(self, key: str, value: int):
        if key in self.config:
            self.config[key].set_int(value)
        else:
            self.config[key] = ConfigInt('', key, 0, is_new=True)
            self.set_int(key, value)

    """ Booleans (0/1, False/True) """
    def get_bool(self, key: str) -> bool:
        if key in self.config:
            return self.config[key].get_bool()
        else:
            self._handle_access_error(key, 'read_error')
            return False

    def set_bool(self, key: str, value: bool):
        if key in self.config:
            self.config[key].set_bool(value)
        else:
            self.config[key] = ConfigBool('', key, False, is_new=True)
            self.set_bool(key, value)

    """ Email addresses """
    def get_email(self, key: str) -> Email:
        return Email(self.get_str(key))

    def set_email(self, key: str, value: Email):
        if key in self.config:
            self.config[key].set_str(value)
        else:
            self.create_str_key(ConfigEmail, key, value)

    """ CSV strings """
    def get_csv(self, key: str) -> CSVstr:
        return CSVstr(self.get_str(key))

    def set_csv(self, key: str, value: CSVstr):
        if key in self.config:
            self.config[key].set_str(value)
        else:
            self.create_str_key(ConfigCSV, key, value)

    """ URL strings """
    def get_url(self, key: str) -> URLstr:
        return URLstr(self.get_str(key))

    def set_url(self, key: str, value: URLstr):
        if key in self.config:
            self.config[key].set_str(value)
        else:
            self.create_str_key(ConfigURL, key, value)

    def create_str_key(self, aclass: Type[ConfigItem], key: str, value: ValidStrTypes):
        """ Function for creating new config items on the fly. Should be rare in LL. """    
        new_entry = aclass('', key, '', is_new=True)
        if new_entry.is_valid_value(value):
            self.config[key] = new_entry
            self.config[key].set_str(value)
        else:
            self._handle_access_error(key, 'format_error')

    def _handle_access_error(self, key: str, status: str):
        """ Handle accesses to invalid keys """
        if key not in self.errors:
            self.errors[key] = Counter()
        self.errors[key][status] += 1
        logger.error(f"Config[{key}]: {status}")

    def get_error_counters(self) -> Dict[str, Counter]:
        """ Get a list of all access errors """
        return self.errors

    def get_all_accesses(self) -> Dict[str, Counter]:
        """ Get a list of all config values that have been accessed  """
        result = dict()
        for key, value in self.config.items():
            a = value.get_accesses()
            if len(a):
                if value.section:
                    result[f"{value.section.upper()}.{key}"] = a
                else:
                    result[f"{key}"] = a

        for (name, index), config in self.arrays.items():
            for key, value in config.items():
                a = value.get_accesses()
                if len(a):
                    result[f"{name}.{index}.{key}"] = a

        return result
    
    def clear_access_counters(self):
        """ Clear all counters. Might be useful after sending telemetry etc """
        all = self.get_all_accesses()
        for _, item in all.items():
            item.clear()

    def save_config(self, filename: str, save_all: bool=False):
        """ 
        Save the configuration to a new file. Return number of items stored, -1 if error.
        If save_all, saves all possible config items. If False, saves only changed items
        """

        def add_to_parser(parser, sectionname, item) -> int:
            """ Add item to parser, return 1 if added, 0 if ignored """
            if save_all or not item.is_default():
                if not sectionname in parser:
                    parser[sectionname] = {}
                parser[sectionname][key] = str(item)
                return 1
            else:
                return 0

        parser = ConfigParser()
        parser.optionxform = lambda optionstr: optionstr.lower()

        count = 0
        for key, item in self.config.items():
            count += add_to_parser(parser, item.section, item)

        for (name, inx), array in self.arrays.items():
            sectionname = f"{name}{inx}"
            for key, item in array.items():
                count += add_to_parser(parser, sectionname, item)

        try:
            with open(filename, "w") as configfile:
                parser.write(configfile)
            return count
        except Exception as e:
            logger.warn(f'Error saving config file {filename}: {type(e).__name__} {str(e)}')
            return -1

    def save_config_and_backup_old(self, save_all: bool=False) -> int:
        """ 
        Renames the old config file to .bak and saves new config file. 
        Return number of items stored, -1 if error.
        """

        if not self.configfilename:
            logger.error('Cannot save and backup config without a filename')
            return -1

        currentname = thread_name()
        thread_name("CONFIG2_WRITE")
        try:
            logger.info(f'Saving configuration to {self.configfilename}')
            savecount = self.save_config(syspath(self.configfilename + '.new'), save_all)
            if savecount == 0:
                return 0
            else:
                import os

                msg = ''
                try:
                    os.remove(syspath(self.configfilename + '.bak'))
                except OSError as e:
                    if e.errno != 2:  # doesn't exist is ok
                        msg = '{} {}{} {} {}'.format(type(e).__name__, 'deleting backup file:', self.configfilename, '.bak', e.strerror)
                        logger.warn(msg)
                try:
                    os.rename(syspath(self.configfilename), syspath(self.configfilename + '.bak'))
                except OSError as e:
                    if e.errno != 2:  # doesn't exist is ok as wouldn't exist until first save
                        msg = '{} {} {} {}'.format('Unable to backup config file:', self.configfilename, type(e).__name__, e.strerror)
                        logger.warn(msg)
                try:
                    os.rename(syspath(self.configfilename + '.new'), syspath(self.configfilename))
                except OSError as e:
                    msg = '{} {} {} {}'.format('Unable to rename new config file:', self.configfilename, type(e).__name__, e.strerror)
                    logger.warn(msg)

                if not msg:
                    msg = f'Config file {self.configfilename} has been saved with {savecount} items'
                    logger.info(msg)
                    return savecount
                else:
                    return -1
        finally:
            thread_name(currentname)

    def post_load_fixup(self) -> int:
        """ 
        Perform post-load operations specific to LL.
        Returns 0 if ok, otherwise number of warnings
        """
        warnings = 0
        logger.debug('Performing post-load fixup on config')
        if str(self.config['LOGDIR']) == '':
            self.config['LOGDIR'].set_str(path.join(lazylibrarian.DATADIR, 'Logs'))

        if str(self.config['AUDIOBOOK_DEST_FOLDER']) == 'None':
            self.config['AUDIOBOOK_DEST_FOLDER'].set_str(self.config['EBOOK_DEST_FOLDER'].get_str())

        for fname in ['EBOOK_DEST_FILE', 'MAG_DEST_FILE', 'AUDIOBOOK_DEST_FILE', 'AUDIOBOOK_SINGLE_FILE']:
            if sep in self.config[fname].get_str():
                logger.warn('Please check your %s setting, contains "%s"' % (fname, sep))
                warnings += 1

        if str(self.config['HTTP_LOOK']) in ['legacy', 'default']:
            logger.warn('configured interface is deprecated, new features are in bookstrap')
            self.config['HTTP_LOOK'].set_str('bookstrap')
            warnings += 1

        lazylibrarian.SHOW_EBOOK = 1 if self.config['EBOOK_TAB'].get_bool() else 0
        lazylibrarian.SHOW_AUDIO = 1 if self.config['AUDIO_TAB'].get_bool() else 0
        lazylibrarian.SHOW_MAGS = 1 if self.config['MAG_TAB'].get_bool() else 0
        lazylibrarian.SHOW_COMICS = 1 if self.config['COMIC_TAB'].get_bool() else 0

        if  str(self.config['HOMEPAGE']) == 'eBooks' and not lazylibrarian.SHOW_EBOOK or \
            str(self.config['HOMEPAGE']) == 'AudioBooks' and not lazylibrarian.SHOW_AUDIO or \
            str(self.config['HOMEPAGE']) == 'Magazines' and not lazylibrarian.SHOW_MAGS or \
            str(self.config['HOMEPAGE']) == 'Comics' and not lazylibrarian.SHOW_COMICS or \
            str(self.config['HOMEPAGE']) == 'Series' and not lazylibrarian.SHOW_SERIES:
            self.config['HOMEPAGE'].set_str('')
        
        if self.config['SSL_CERTS'].get_str() != '' and not path_exists(str(self.config['SSL_CERTS'])):
            logger.warn("SSL_CERTS [%s] not found" % str(self.config['SSL_CERTS']))
            self.config['SSL_CERTS'].set_str('')
            warnings += 1

        return warnings


def are_equivalent(cfg1: LLConfigHandler, cfg2: LLConfigHandler) -> bool:
    """ Check that the two configs are logically equivalent by comparing all the keys and values """

    def are_configdicts_equivalent(cd1: ConfigDict, cd2: ConfigDict) -> bool:
        if not cd1 or not cd2:
            return False
        if len(cd1) != len(cd2):
            return False
        for key, item1 in cd1.items():
            if key in cd2.keys():
                if cd2[key].value != item1.value:
                    return False
            else:
                return False
        return True


    if not cfg1 or not cfg2: # Both need to exist
        return False 

    # Compare base configs
    if not are_configdicts_equivalent(cfg1.config, cfg2.config): 
        return False

    # Compare array configs
    if len(cfg1.arrays) != len(cfg2.arrays):
        return False

    for (name, inx), cd1 in cfg1.arrays.items():
        try:
            cd2 = cfg2.arrays[(name, inx)]
        except:
            return False
        if not are_configdicts_equivalent(cd1, cd2):
            return False

    return True
