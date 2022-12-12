#  This file will be part of Lazylibrarian.
#
# Purpose:
#   Type-aware handling config.ini, access to its properties, etc.
#   Intended to entirely replace the previous file, config.py, as
#   well as many global variables

from typing import Dict, List, Type, MutableMapping, Optional
from copy import deepcopy
from configparser import ConfigParser
from collections import Counter, OrderedDict
from os import path, sep

import lazylibrarian
from lazylibrarian.configtypes import ConfigItem, ConfigStr, ConfigBool, ConfigInt, ConfigEmail, ConfigCSV, \
    ConfigURL, Email, CSVstr, URLstr, ValidStrTypes
from lazylibrarian.configdefs import DefaultArrayDef, ARRAY_DEFS
from lazylibrarian import logger, database
from lazylibrarian.formatter import thread_name
from lazylibrarian.common import syspath, path_exists
from lazylibrarian.scheduling import schedule_job

ConfigDict = Dict[str, ConfigItem]

class ArrayConfig():
    """ Handle an array-config, such as for a list of notifiers """
    _name: str    # e.g. 'APPRISE'
    _secstr: str  # e.g. 'APPRISE_%i'
    _primary: str # e.g. 'URL'
    _configs: Dict[int, ConfigDict]
    _defaults: List[ConfigItem]

    def __init__(self, arrayname: str, defaults: DefaultArrayDef):
        self._name = arrayname
        self._primary = defaults[0]  # Name of the primary key for this item
        self._secstr = defaults[1]   # Name of the section string template
        self._defaults = defaults[2] # All the entries
        self._configs = OrderedDict()

    def setupitem_at(self, index: int):
        for config_item in self._defaults:
            key = config_item.key.upper()
            if not index in self._configs:
                self._configs[index] = dict()
            self._configs[index][key] = deepcopy(config_item)
            self._configs[index][key].section = self.get_section_str(index)

    def has_index(self, index: int) -> bool:
        return index in self._configs.keys()

    # Allow ArrayConfig to be accessed as an indexed list
    def __len__(self) -> int:
        return len(self._configs)

    def __getitem__(self, index: int) -> ConfigDict:
        return self._configs[index]

    def is_in_use(self, index: int) -> bool:
        """ Check if the index'th item is in use, or spare """
        if index > len(self):
            return False
        config = self[index]
        if self._primary in config:
            return self._configs[index][self._primary].get_str() != ''
        else:
            return False

    def get_section_str(self, index: int) -> str:
        return self._secstr % index

    def ensure_empty_end_item(self):
        """ Ensure there is an empty/unused item at the end of the list """
        if len(self._configs) == 0 or self.is_in_use(len(self._configs)-1):
            self.setupitem_at(len(self))

    def cleanup_for_save(self):
        """ Clean out empty items and renumber items from 0 """
        keepcount = 0
        for index in range(0,len(self)):
            if not self.is_in_use(index):
                del self._configs[index]
            else:
                keepcount += 1

        renum = 0
        # Because we use an OrderedDict, items will be in numeric order
        for number in self._configs:
            if number > renum:
                config = self._configs.pop(number)
                # Update the section key in each item
                for name, item in config.items():
                    item.section = self.get_section_str(renum)
                # Update the key of the dict entry
                self._configs[renum] = config
            renum += 1

        # Validate that this worked, it's a bit iffy
        if keepcount != len(self):
            logger.error(f'Internal error cleaning up {self._name}')
        for index in range(0,len(self)):
            config = self._configs[index]
            for name, item in config.items():
                if item.section != self.get_section_str(index):
                    logger.error(f'Internal error in {self._name}:{name}')

""" Main configuration handler for LL """
class LLConfigHandler():
    config: ConfigDict # simple (str, value)
    arrays: Dict[str, ArrayConfig] # (section, array)
    errors: Dict[str, Counter]
    configfilename: str

    def __init__(self, defaults: Optional[List[ConfigItem]]=None, configfile: Optional[str]=None):
        self.config = dict()
        self.errors = dict()
        self.arrays = dict()
        self._copydefaults(self.config, defaults)

        if configfile:
            self.configfilename = configfile
            parser = ConfigParser(dict_type=CaseInsensitiveDict)
            parser.optionxform = lambda optionstr: optionstr.upper()
            parser.read(configfile)
            for section in parser.sections():
                if section[-1:].isdigit():
                    self._load_array_section(section, parser)
                else:
                    self._load_section(section, parser, self.config)
        else:
            self.configfilename = ''
        self.ensure_arrays_have_empty_item()

    def _copydefaults(self, config: ConfigDict, defaults: Optional[List[ConfigItem]]=None):
        """ Copy the default values and settings for the given config """
        if defaults:
            for config_item in defaults:
                key = config_item.key.upper()
                config[key] = deepcopy(config_item)

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
        if arrayname[-1:] == '_':
            arrayname = arrayname[:-1]
        index = int(section[-1:])
        defaults = ARRAY_DEFS[arrayname] if arrayname in ARRAY_DEFS else None
        if defaults:
            logger.debug(f"Loading array {arrayname} index {index}")
            if not arrayname in self.arrays:
                self.arrays[arrayname] = ArrayConfig(arrayname, defaults)

            self.arrays[arrayname].setupitem_at(index)
            array = self.arrays[arrayname][index]
            self._load_section(section, parser, array)
        else:
            logger.warn(f"Cannot load array {section}: Undefined")

    def ensure_arrays_have_empty_item(self):
        """ Make sure every array has an empty item for users to configure """
        for name in ARRAY_DEFS:
            if not name in self.arrays:
                self.arrays[name] = ArrayConfig(name, ARRAY_DEFS[name])
            self.arrays[name].ensure_empty_end_item()

    """ Handle array entries """
    def get_array_entries(self, wantname: str) -> int:
        """ Return number of entries in a particular array config """
        rc = 0
        if not wantname:
            return rc
        for name in self.arrays.keys():
            if name[:len(wantname)] == wantname:
                rc += len(self.arrays[name])
        return rc

    def get_array(self, wantname: str) -> Optional[ArrayConfig]:
        """ Return the config for an array, like 'APPRISE', or None """
        if wantname in self.arrays:
            return self.arrays[wantname]
        else:
            return None

    def get_array_dict(self, wantname: str, wantindex: int) -> Optional[ConfigDict]:
        """ Return the complete config for an entry, like ('APPRISE', 0) """
        if wantname in self.arrays and self.arrays[wantname].has_index(wantindex):
            return self.arrays[wantname][wantindex]
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

        for name, array in self.arrays.items():
            for index, config in array._configs.items():
                for key, item in config.items():
                    a = item.get_accesses()
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

        def add_to_parser(parser: ConfigParser, sectionname: str, item: ConfigItem) -> int:
            """ Add item to parser, return 1 if added, 0 if ignored """
            if save_all or not item.is_default():
                if not sectionname in parser:
                    parser[sectionname] = {}
                parser[sectionname][key] = item.get_save_str()
                return 1
            else:
                return 0

        for _, array in self.arrays.items():
            array.cleanup_for_save()
        try:
            parser = ConfigParser()
            parser.optionxform = lambda optionstr: optionstr.lower()

            count = 0
            for key, item in self.config.items():
                count += add_to_parser(parser, item.section, item)

            for name, array in self.arrays.items():
                for inx in range(0, len(array)):
                    config = array[inx]
                    sectionname = array.get_section_str(inx)
                    for key, item in config.items():
                        count += add_to_parser(parser, sectionname, item)

            try:
                with open(filename, "w") as configfile:
                    parser.write(configfile)
                return count
            except Exception as e:
                logger.warn(f'Error saving config file {filename}: {type(e).__name__} {str(e)}')
                return -1
        finally:
            for _, array in self.arrays.items():
                array.ensure_empty_end_item()

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
            self.post_save_actions()

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

    def post_save_actions(self):
        """ Run activities after saving, such as rescheduling jobs that may have changed """
        for _, item in self.config.items():
            schedule = item.get_schedule_name()
            if schedule:
                logger.debug(f"Restarting job {schedule}, interval {item.get_int()}")
                schedule_job('Restart', schedule)

        if self.config['NO_SINGLE_BOOK_SERIES'].get_bool():
            logger.debug("Deleting single-book series from database")
            db = database.DBConnection()
            db.action('DELETE from series where total=1')
            db.close()

def are_equivalent(cfg1: LLConfigHandler, cfg2: LLConfigHandler) -> bool:
    """ Check that the two configs are logically equivalent by comparing all the keys and values """

    def are_configdicts_equivalent(cd1: ConfigDict, cd2: ConfigDict) -> bool:
        if not cd1 or not cd2:
            logger.warn(f"Arrays don't exist {not cd1}, {not cd2}")
            return False
        if len(cd1) != len(cd2):
            logger.warn(f"Array lengths differ: {len(cd1)} != {len(cd2)}")
            return False
        for key, item1 in cd1.items():
            if key in cd2.keys():
                if cd2[key].value != item1.value:
                    logger.warn(f"Array values for [{key}]: {item1.value} != {cd2[key].value}")
                    return False
            else:
                logger.warn(f"Array key [{key}] missing in array 2")
                return False
        return True


    if not cfg1 or not cfg2: # Both need to exist
        return False

    # Compare base configs
    if not are_configdicts_equivalent(cfg1.config, cfg2.config):
        logger.warn(f"Base configs differ")
        return False

    # Compare array configs
    if len(cfg1.arrays) != len(cfg2.arrays):
        logger.warn(f"Number of array configs differ")
        return False

    for name, array in cfg1.arrays.items():
        for inx in range(0, len(array)):
            cd1 = array[inx]
            try:
                cd2 = cfg2.arrays[name][inx]
            except:
                logger.warn(f"Error retrieving array config {name}.{inx}")
                return False
            if not are_configdicts_equivalent(cd1, cd2):
                logger.warn(f"Array configs differ in {name}.{inx}")
                return False

    return True

### This is to have section names be case insensitive.
### Built from https://stackoverflow.com/questions/49755480/case-insensitive-sections-in-configparser
class CaseInsensitiveDict(MutableMapping):
    """ Ordered case insensitive mutable mapping class. """
    def __init__(self, *args, **kwargs):
        self._d = OrderedDict(*args, **kwargs)
        self._convert_keys()
    def _convert_keys(self):
        for k in list(self._d.keys()):
            v = self._d.pop(k)
            self._d.__setitem__(k, v)
    def __len__(self):
        return len(self._d)
    def __iter__(self):
        return iter(self._d)
    def __setitem__(self, k, v):
        self._d[k.upper()] = v
    def __getitem__(self, k):
        return self._d[k.upper()]
    def __delitem__(self, k):
        del self._d[k.upper()]
    def copy(self):
        return CaseInsensitiveDict(self._d.copy())
