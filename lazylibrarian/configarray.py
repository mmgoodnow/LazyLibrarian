#  This file will be part of Lazylibrarian.
#
# Purpose:
#   Handle array-configs, such as providers and notifiers

from typing import Dict, List
from collections import OrderedDict

from lazylibrarian.configtypes import ConfigItem, ConfigDict
from lazylibrarian.configdefs import DefaultArrayDef, configitem_from_default
from lazylibrarian import logger

class ArrayConfig:
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
                self._configs[index] = ConfigDict(self.get_section_str(index))
            item = self._configs[index].set_item(key, configitem_from_default(config_item))
            item.section = self.get_section_str(index)
            if key == 'NAME': # Override name with section name
                item.value = item.section

    def has_index(self, index: int) -> bool:
        return index in self._configs.keys()

    # Allow ArrayConfig to be accessed as an indexed list
    def __len__(self) -> int:
        return len(self._configs)

    def __getitem__(self, index: int) -> ConfigDict:
        return self._configs[index]

    def primary_host(self, index: int) -> str:
        if index > len(self):
            return ''
        config = self[index]
        if self._primary in config:
            return self._configs[index][self._primary]
        else:
            return ''

    # Allow ArrayConfig to be iterated over, ignoring the index
    def __iter__(self):
        return self._configs.values().__iter__()

    def is_in_use(self, index: int) -> bool:
        """ Check if the index'th item is in use, or spare """
        if index > len(self):
            return False
        return self.primary_host(index) != ''

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

