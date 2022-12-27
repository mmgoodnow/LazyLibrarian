#  This file is part of Lazylibrarian.
#
# Purpose:
#   Type-aware handling config.ini, access to its properties, etc.
#   Intended to entirely replace the previous file, config.py, as
#   well as many global variables

from typing import Dict, List, Optional, Generator, Tuple
from configparser import ConfigParser
from collections import Counter
from os import path, sep
import os
import shutil
import sys
import re

import lazylibrarian
from lazylibrarian.configtypes import ConfigItem, ConfigBool, Access, CaseInsensitiveDict, ConfigDict, ConfigScheduler
from lazylibrarian.configarray import ArrayConfig
from lazylibrarian.configdefs import BASE_DEFAULTS, ARRAY_DEFS, configitem_from_default
from lazylibrarian import logger, database
from lazylibrarian.logger import lazylibrarian_log
from lazylibrarian.formatter import thread_name, plural
from lazylibrarian.filesystem import DIRS, syspath, path_exists
from lazylibrarian.blockhandler import BLOCKHANDLER

""" Main configuration handler for LL """
class LLConfigHandler(ConfigDict):
    arrays: Dict[str, ArrayConfig] # (section, array)
    configfilename: str
    REDACTLIST: List[str]

    def __init__(self, defaults: Optional[List[ConfigItem]]=None, configfile: Optional[str]=None):
        super().__init__()
        self.arrays = dict()
        self.defaults = defaults
        self._copydefaults(defaults)
        self.configfilename = ''
        self.load_configfile(configfile)

    def load_configfile(self, configfile: Optional[str]=None):
        if self.configfilename:
            # Clear existing before loading another setup, brute force.
            super().clear()
            self.arrays.clear()
            # Re-setup the defaults
            self._copydefaults(self.defaults)
        if configfile:
            self.configfilename = configfile
            parser = ConfigParser(dict_type=CaseInsensitiveDict)
            parser.optionxform = lambda optionstr: optionstr.upper()
            parser.read(syspath(configfile))
            for section in parser.sections():
                if section[-1:].isdigit():
                    self._load_array_section(section, parser)
                else:
                    self._load_section(section, parser, self)
            self._update_redactlist()
        else:
            self.configfilename = ''
        self.ensure_arrays_have_empty_item()

    def _copydefaults(self, defaults: Optional[List[ConfigItem]]=None):
        """ Copy the default values and settings for the given config """
        if defaults:
            for config_item in defaults:
                key = config_item.key.upper()
                self.config[key] = configitem_from_default(config_item)

    @staticmethod
    def _load_section(section:str, parser:ConfigParser, config: ConfigDict):
        """ Load a section of an ini file """
        for option in parser.options(section):
            if option in config:
                config_item = config.get_item(option)
                if not config_item or not config_item.update_from_parser(parser, option):
                    logger.warn(f"Error loading {section}.{option} as {parser.get(section, option)}")
            else:
                logger.warn(f"Unknown option {section}.{option} in config")

    def _load_array_section(self, section:str, parser:ConfigParser):
        """ Load a section of an ini file, where that section is part of an array """
        arrayname, index = re.split(r'(^\D+)', section)[1:]
        arrayname = arrayname.upper()
        if arrayname[-1:] == '_':
            arrayname = arrayname[:-1]
        index = int(index)
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
        found = False
        if not wantname:
            return rc
        wantname = wantname.upper()
        for name in self.arrays.keys():
            if name[:len(wantname)] == wantname:
                rc += len(self.arrays[name])
                found = True

        if not found:
            self._handle_access_error(wantname, Access.READ_ERR)
        return rc

    def get_array(self, wantname: str) -> Optional[ArrayConfig]:
        """ Return the config for an array, like 'APPRISE', or None """
        if wantname.upper() in self.arrays:
            return self.arrays[wantname.upper()]
        else:
            return None

    def get_array_dict(self, wantname: str, wantindex: int) -> Optional[ConfigDict]:
        """ Return the complete config for an entry, like ('APPRISE', 0) """
        if wantname in self.arrays and self.arrays[wantname.upper()].has_index(wantindex):
            return self.arrays[wantname.upper()][wantindex]
        else:
            return None

    def get_array_str(self, __array: str, index: int, __key: str) -> str:
        """ Access a single array string config directly """
        return self.arrays[__array.upper()][index][__key.upper()]

    def providers(self, name: str) -> ArrayConfig:
        """ Return an iterable list of providers """
        array = self.get_array(name)
        if array:
            return array
        else:
            self._handle_access_error(name, Access.READ_ERR)
            raise Exception(f'Cannot iterate over non-existent array {name}')

    def get_schedulers(self, name:str='') -> Generator[Tuple[str, ConfigScheduler], None, None]:
        """ Return an iterable list of schedulers that matches name, or all """
        for key, item in self.config.items():
            if name == '' or name.lower() in key.lower():
                if isinstance(item, ConfigScheduler):
                    yield key, item

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
        """ Clear all counters. Used after sending telemetry or saving config """
        for _, value in self.config.items():
            value.get_accesses().clear()
        self.clear_error_counters()

        for _, array in self.arrays.items():
            for _, config in array._configs.items():
                for _, item in config.items():
                    item.get_accesses().clear()
                config.clear_error_counters()

    def update_providers_from_UI(self, kwargs):
        """ Update all provider arrays with a settings array from the web UI.
            Assumes that all UI settings are of the form section_num_setting=value """
        for pname, array in self.arrays.items():
            for inx, config in array._configs.items():
                for key, item in config.items():
                    setting = f'{pname.lower()}_{inx}_{key.lower()}'
                    value = kwargs.get(setting)
                    if value is not None:
                        item.set_from_ui(value)
                    elif isinstance(item, ConfigBool): # Bools that are not listed are False
                        item.set_from_ui(False)

    def reset_to_default(self, keys: List[str]):
        for key in keys:
            item = self.get_item(key)
            if item:
                item.reset_to_default()
            else:
                logger.warn(f'Cannot reset value of {key} as it does not exist')

    def scheduler_can_run(self, scheduler: ConfigScheduler) -> bool:
        """ Return True if the scheduler's requirements are satisfied """
        ok = scheduler.get_int() > 0 # 0 means schedule is disabled
        if ok and scheduler.needs_provider:
            ok = self.use_any()
        if ok and scheduler.run_name == 'GRSYNC': # Special case, should maybe add option to object
            ok = self.get_bool('GR_SYNC')
        if ok and scheduler.run_name == 'TELEMETRYSEND': # Special case for telemetry
            ok = self.config['TELEMETRY_ENABLE'].get_bool()
        return ok

    def save_config(self, filename: str, save_all: bool=False):
        """
        Save the configuration to a new file. Return number of items stored, -1 if error.
        If save_all, saves all possible config items. If False, saves only changed items
        """

        def add_to_parser(aparser: ConfigParser, asectionname: str, aitem: ConfigItem) -> int:
            """ Add item to parser, return 1 if added, 0 if ignored """
            if aitem.do_persist() and (save_all or not aitem.is_default()):
                if not asectionname in aparser:
                    aparser[asectionname] = {}
                aparser[asectionname][key] = aitem.get_save_str()
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

    def save_config_and_backup_old(self, save_all: bool=False, section:Optional[str]=None, restart_jobs:bool=False) -> int:
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
            logger.debug(f'Saving configuration to {self.configfilename}')
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
                    if section:
                        msg = f'Config file {self.configfilename} has been saved with {savecount} items (Triggered by {section})'
                    else:
                        msg = f'Config file {self.configfilename} has been saved with {savecount} items'
                    logger.info(msg)
                    return savecount
                else:
                    return -1
        finally:
            from lazylibrarian.telemetry import record_usage_data
            record_usage_data('Config/Save')
            thread_name(currentname)
            # Only clear counters if we save the entire config
            clear:bool = False if section and section != '' else True
            self.post_save_actions(restart_jobs=restart_jobs, clear_counters=clear)

    def post_load_fixup(self) -> int:
        """
        Perform post-load operations specific to LL.
        Returns 0 if ok, otherwise number of warnings
        """
        warnings = 0
        logger.debug('Performing post-load fixup on config')
        DIRS.set_config(self)
        DIRS.ensure_log_dir()

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

    @staticmethod
    def get_mako_versionfile():
        return path.join(DIRS.get_mako_cachedir(), 'python_version.txt')

    def post_save_actions(self, restart_jobs: bool=True, clear_counters: bool=False):
        """ Run activities after saving, such as rescheduling jobs that may have changed """
        lazylibrarian_log.update_loglevel()
        # Clean the mako cache if the interface has changed
        interface = self.config['HTTP_LOOK']
        if interface.get_writes() > 0: # It's changed
            mako_dir = DIRS.get_mako_cachedir()
            logger.debug("Clearing mako cache")
            shutil.rmtree(mako_dir)
            os.makedirs(mako_dir)
            version_file = self.get_mako_versionfile()
            with open(version_file, 'w') as fp:
                fp.write(sys.version.split()[0] + ':' + interface.get_str())

        # Restart all scheduled jobs since the schedules may have changed
        if restart_jobs:
            from lazylibrarian.scheduling import schedule_job
            for _, item in self.config.items():
                schedule = item.get_schedule_name()
                if schedule and isinstance(item, ConfigScheduler):
                    if self.scheduler_can_run(item):
                        logger.debug(f"Restarting job {schedule}, interval {item.get_int()}")
                        schedule_job('Restart', schedule)
                    else:
                        logger.debug(f"Stopping job {schedule}")
                        schedule_job('Stop', schedule)

        # Clean up the database if needed (Does this really belong here?)
        if self.config['NO_SINGLE_BOOK_SERIES'].get_bool():
            logger.debug("Deleting single-book series from database")
            db = database.DBConnection()
            db.action('DELETE from series where total=1')
            db.close()

        # Update the redact list since things may have changed
        self._update_redactlist()

        if clear_counters:
            # Clear all access counters, so we can tell if something has changed later
            self.clear_access_counters()

    def create_access_summary(self, saveto:str='') -> Dict:
        """ For debugging: Create a summary of all accesses, potentially
        highlighting places where config2 is used incorrectly or where things
        are highly inefficient """

        # Collate all access attempts to keys that exist
        access_summary = {}
        for a in Access:
            access_summary[a.name] = []

        for key, value in self.config.items():
            accesses = value.get_accesses()
            for aname, count in accesses.items():
                access_summary[aname.name].append((key, count)) # Accesstype = (key, counter)
        for key, errors in self.get_error_counters().items():
            for ename, count in errors.items():
                access_summary[ename.name].append((key, count))

        for name, array in self.arrays.items(): # e.g. Apprise
            for index, config in array._configs.items(): # e.g. Each Apprise
                # Add the normal access items
                for key, item in config.items():
                    accesses = item.get_accesses()
                    for aname, count in accesses.items():
                        access_summary[aname.name].append((f"{name}.{index}.{key}", count)) # Accesstype = (key, counter)
                # Add any key error items
                for key, errors in config.get_error_counters().items():
                    for ename, count in errors.items():
                        access_summary[ename.name].append((f"{name}.{index}.{key}", count))

        if saveto:
            self.save_access_summary(saveto, access_summary)

        return access_summary

    @staticmethod
    def save_access_summary(saveto: str, access_summary):
        """ For debugging: Create a summary of all config accesses by type """

        file = open(saveto,"w")
        try:
            file.write(f'*** Config Item Access Summary ***\n')
            for sumtype, summary in access_summary.items():
                if len(summary) > 0:
                    file.writelines(f'Access type: {sumtype}\n')
                    for line in summary:
                        #Format:  NameOfKey--------------------- Count--
                        file.writelines(f'  {line[0]:30}: {line[1]:7}\n')
        finally:
            file.close()


    def _update_redactlist(self):
        """ Update REDACTLIST after config changes """

        self.REDACTLIST = []
        wordlist = ['PASS', 'TOKEN', 'SECRET', '_API', '_USER', '_DEV']
        if self.get_bool('HOSTREDACT'):
            wordlist.append('_HOST')
        for key in self.config.keys():
            if key not in ['BOOK_API', 'GIT_USER', 'SINGLE_USER']:
                for word in wordlist:
                    if word in key and self[key]:
                        self.REDACTLIST.append(u"%s" % self[key])
        for key in ['EMAIL_FROM', 'EMAIL_TO', 'SSL_CERTS']:
            if self[key]:
                self.REDACTLIST.append(u"%s" % self[key])

        for name, definitions in ARRAY_DEFS.items():
            key = definitions[0] # Primary key for this array type
            array = self.get_array(name)
            if array:
                for inx, config in array._configs.items():
                    if config[key]:
                        self.REDACTLIST.append(f"{config[key]}")
                    if 'API' in config:
                        if config['API']:
                            self.REDACTLIST.append(f"{config['API']}")

        logger.debug("Redact list has %d %s" % (len(self.REDACTLIST),
                                                plural(len(self.REDACTLIST), "entry")))

    def get_all_types_list(self) -> List[str]:
        """ Return a list of extensions that includes all types of items """
        return self.get_list('MAG_TYPE') + self.get_list('AUDIOBOOK_TYPE') + \
            self.get_list('EBOOK_TYPE') + self.get_list('COMIC_TYPE')

    def use_any(self, rss=True) -> bool:
        """ Checks for TOR, NZB, Direct and IRC providers (optionally also RSS) """
        ok = bool(self.use_tor() or self.use_nzb() or self.use_direct() or self.use_irc())
        if not ok and rss:
            ok = bool(self.use_rss())
        return ok

    def total_active_providers(self) -> int:
        """ Count total number of valid providers of type TOR, NZB, RSS, Direct and IRC """
        return self.use_tor() + self.use_nzb() + self.use_rss() + self.use_direct() + self.use_irc()


    def count_in_use(self, provider: str, wishlist: Optional[bool] = None) -> int:
        """ Returns # of providers named provider are in use """
        from lazylibrarian.providers import wishlist_type
        count = 0
        if provider in self.arrays:
            array = self.get_array(provider)
            if array:
                for inx in range(0, len(array)):
                    host = array.primary_host(inx)
                    ok = array.is_in_use(inx) and not BLOCKHANDLER.is_blocked(host)
                    if wishlist is not None:
                        ok = ok and wishlist_type(host) == wishlist
                    if ok:
                        count += 1
        return count

    def use_rss(self) -> int:
        """ Returns number of RSS providers that are not wishlists, and are not blocked """
        return self.count_in_use('RSS', wishlist=False)

    def use_irc(self) -> int:
        """ Returns number of IRC active providers that are not blocked """
        return self.count_in_use('IRC')

    def use_wishlist(self) -> int:
        """Returns number of RSS providers that are wishlists and not blocked """
        return self.count_in_use('RSS', wishlist=True)

    def use_nzb(self) -> int:
        """ Returns number of nzb active providers that are not blocked
        (Includes Newznab and Torznab providers) """
        return self.count_in_use('NEWZNAB') + self.count_in_use('TORZNAB')

    def use_tor(self) -> int:
        """ Returns number of TOR providers that are not blocked """
        count = 0
        for provider in ['KAT', 'WWT', 'TPB', 'ZOO', 'LIME', 'TDL', 'TRF']:
            if self.get_bool(provider) and not BLOCKHANDLER.is_blocked(provider):
                count += 1
        return count

    def use_direct(self) -> int:
        """ Returns number of enabled direct book providers """
        count = self.count_in_use('GEN')
        if self.get_bool('BOK') and not BLOCKHANDLER.is_blocked('BOK'):
             count += 1
        if self.get_bool('BFI') and not BLOCKHANDLER.is_blocked('BFI'):
             count += 1
        return count

    def disp_name(self, provider: str) -> str:
        """
        Strange function. Returns the display name of a provider that
        matches the host name provided as parameter, if any.
        If not, returns the host name provided, shortened if too long.
        """
        provname = ''
        # Iterate over each type of provider
        for name, definitions in ARRAY_DEFS.items():
            key = definitions[0] # Primary key for this array type
            array = self.providers(name)
            if array and not provname: # If we have not yet got a result...
                 for config in array:
                     if config[key].strip('/') == provider:
                        provname = config['DISPNAME']
                        break

        if not provname:
            provname = provider
        if len(provname) > 20:
            while len(provname) > 20 and '/' in provname:
                provname = provname.split('/', 1)[1]
            provname = provname.replace('/', ' ')
        return provname

### Global configuration holder
CONFIG = LLConfigHandler(defaults=BASE_DEFAULTS)

### Global config related methods that are not part of the config object

def are_equivalent(cfg1: LLConfigHandler, cfg2: LLConfigHandler) -> bool:
    """ Check that the two configs are logically equivalent by comparing all the keys and values """

    def are_configdicts_equivalent(checkcd1: ConfigDict, checkcd2: ConfigDict) -> bool:
        if not checkcd1 or not checkcd2:
            logger.warn(f"Arrays don't exist {not checkcd1}, {not checkcd2}")
            return False
        if len(checkcd1) != len(checkcd2):
            logger.warn(f"Array lengths differ: {len(checkcd1)} != {len(checkcd2)}")
            return False
        for key, item1 in checkcd1.items():
            if key in checkcd2.keys():
                item2 = checkcd2.get_item(key)
                if not item2:
                    logger.warn(f"Array values for [{key}]: {item1.value}: key does not exist")
                elif item2.value != item1.value:
                    logger.warn(f"Array values for [{key}]: {item1.value} != {item2.value}")
                    return False
            else:
                logger.warn(f"Array key [{key}] missing in array 2")
                return False
        return True


    if not cfg1 or not cfg2: # Both need to exist
        return False

    # Compare base configs
    if not are_configdicts_equivalent(cfg1, cfg2):
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
