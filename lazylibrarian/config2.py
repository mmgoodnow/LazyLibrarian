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

from lazylibrarian.configtypes import ConfigItem, ConfigStr, ConfigBool, ConfigInt, ConfigEmail, ConfigCSV, \
    Email, CSVstr, ValidStrTypes, ValidTypes
from lazylibrarian.configdefs import ARRAY_DEFS
from lazylibrarian import logger

ConfigDict = Dict[str, ConfigItem]

""" Main configuration handler for LL """
class LLConfigHandler():
    config: ConfigDict
    arrays: Dict[Tuple[str, int], ConfigDict]
    errors: Dict[str, Counter]

    def __init__(self, defaults: List[ConfigItem]|None=None, configfile: str|None=None):
        self.config = dict()
        self.errors = dict()
        self.arrays = dict()
        self._copydefaults(self.config, defaults)

        if configfile:
            parser = ConfigParser()
            parser.read(configfile)
            for section in parser.sections():
                if section[-1:].isdigit(): 
                    self._load_array_section(section, parser)
                else:
                    self._load_section(section, parser, self.config)

    def _copydefaults(self, config: ConfigDict, defaults: List[ConfigItem]|None=None, index: int|None=None):
        """ Copy the default values and settings for the given config """
        if defaults:
            for config_item in defaults:
                key = config_item.key
                config[key] = deepcopy(config_item)
                if index != None and index >= 0:
                    config[key].section = config[key].section % index
                        
    def _load_section(self, section:str, parser:ConfigParser, config: ConfigDict):
        """ Load a section of an ini file """
        for option in parser.options(section):
            option = option.upper()
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

    def create_str_key(self, aclass: Type[ConfigItem], key: str, value: ValidStrTypes):
        """ Function for creating new config items on the fly. Should be rare in LL. """    
        if aclass.is_valid_value(value):
            self.config[key] = aclass('', key, '', is_new=True)
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
                    result[f"{value.section}.{key}"] = a
                else:
                    result[f"{key}"] = a

        for (name, index), config in self.arrays.items():
            for key, value in config.items():
                a = value.get_accesses()
                if len(a):
                    result[f"{name}.{index}.{key}"] = a

        return result
    