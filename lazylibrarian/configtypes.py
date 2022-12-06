
#  This file is part of Lazylibrarian.
#
# Purpose:
#    Defines all of the different types of configs that can be
#    found in LazyLibrarian's config.ini (or eventually DB)

from typing import NewType, Dict, Union, List, Type
from enum import Enum
from configparser import ConfigParser
from collections import Counter
from re import match

from lazylibrarian import logger

### Type aliases to distinguish types of string
Email = NewType('Email', str)
CSVstr = NewType('CSV', str)
ValidIntTypes = int
ValidStrTypes =  Union[str, Email, CSVstr]
ValidTypes = Union[ValidStrTypes, ValidIntTypes, bool]

""" Simple wrapper classes for config values of different types """
class ConfigItem():
    section: str
    key: str
    default: ValidTypes
    value: ValidTypes
    accesses: Counter
    is_new: bool

    def __init__(self, section: str, key: str, default: ValidTypes, is_new: bool=False):
        self.section = section
        self.key = key
        self.default = default
        self.value = default
        self.accesses = Counter()
        self.is_new = is_new

    def get_default(self) -> ValidTypes:
        return self.default

    def is_default(self) -> bool:
        return self.value == self.default

    def update_from_parser(self, parser: ConfigParser, name: str) -> bool:
        return self.set_str(parser.get(self.section, name))

    def get_str(self) -> str:
        self._on_read(True)
        return str(self.value) # Everything can be a string

    def __str__(self) -> str:  # Make it the default when accessing the object
        return self.get_str()

    def set_str(self, value: str) -> bool: 
        return False

    def get_int(self) -> int:
        self._on_read(False)
        return 0

    def set_int(self, value: int) -> bool:
        return False

    def get_bool(self) -> bool:
        self._on_read(False)
        return False

    def set_bool(self, value: bool) -> bool:
        return False

    @classmethod
    def is_valid_value(cls, value: ValidTypes) -> bool:
        return True

    def _on_read(self, ok: bool) -> bool:
        if ok:
            self.accesses['read_ok'] += 1
            logger.debug(f"Read config[{self.key}]={self.value}")
        else:
            self.accesses['read_error'] += 1
            logger.debug(f"Type error reading config[{self.key}] ({self.value})")
        return ok

    def _on_set(self, value: ValidTypes) -> bool:
        if self.is_valid_value(value):
            if self.is_new:
                self.accesses['create_ok'] += 1
                self.is_new = False
            elif self.value != value:
                # Don't count a write if the value does not change
                self.accesses['write_ok'] += 1
            self.value = value
            logger.debug(f"Set config[{self.key}]={value}")
            return True
        else:
            self.accesses['write_error'] += 1
            logger.warn(f"Cannot set config[{self.key}] to {value}")
            return False

    def _on_type_mismatch(self, value) -> bool:
        self.accesses['write_error'] += 1
        logger.warn(f"Cannot set config[{self.key}] to {value}: incorrect type")
        return False
    
    def get_accesses(self):
        return self.accesses

class ConfigStr(ConfigItem):
    """ A config item that is a plan string """
    def set_str(self, value: str) -> bool:
        return self._on_set(value)

    def set_int(self, value: int) -> bool:
        return self._on_type_mismatch(value)

    def set_bool(self, value: int) -> bool:
        return self._on_type_mismatch(value)

class ConfigInt(ConfigItem):
    """ A config item that is an int """
    def __init__(self, section: str, key: str, default: int, is_new: bool=False):
        super().__init__(section, key, default, is_new)

    def get_int(self) -> int:
        if self._on_read(type(self.value) == int):
            return int(self.value)
        else:
            return 0

    def set_int(self, value: int) -> bool:
        return self._on_set(value)

    def set_str(self, value: str) -> bool:
        return self._on_type_mismatch(value)

    def set_bool(self, value: bool) -> bool:
        return self._on_type_mismatch(value)

    def update_from_parser(self, parser: ConfigParser, name: str) -> bool:
        try:
            value = parser.getint(self.section, name, fallback=0)
        except:
            value = 0
        return self.set_int(value)

class ConfigBool(ConfigInt):
    """ A config item that is a bool """
    def __init__(self, section: str, key: str, default: bool|int, is_new: bool=False):
        super().__init__(section, key, default, is_new)

    def get_bool(self) -> bool:
        if self._on_read(type(self.value) == bool):
            return bool(self.value)
        else:
            return False

    def set_bool(self, value: bool|int) -> bool:
        return self._on_set(value)

    def set_int(self, value: int) -> bool:
        return self._on_type_mismatch(value)

    def set_str(self, value: str) -> bool:
        return self._on_type_mismatch(value)

    def update_from_parser(self, parser: ConfigParser, name: str) -> bool:
        return self.set_bool(parser.getboolean(self.section, name, fallback=False))

class ConfigEmail(ConfigStr):
    """ A config item that is a string that must be a valid email address """
    def get_email(self) -> Email:
        return Email(self.get_str())

    @classmethod
    def is_valid_value(cls, value: ValidTypes) -> bool:
        value = str(value)
        # Regular expression pattern to match email addresses
        pattern = r"^[\w.-]+@[\w.-]+\.[\w]+$"

        # Check if email matches pattern
        if match(pattern, value):
            # Check if local part of email is not too long
            if len(value.split("@")[0]) <= 64:
                # Check if domain name of email is not too long
                return len(value.split("@")[1]) <= 255
        return False

class ConfigCSV(ConfigStr):
    """ A config item that is a string that must be a valid CSV """
    def get_csv(self) -> CSVstr:
        return CSVstr(self.get_str())

    @classmethod
    def is_valid_value(cls, value: ValidTypes) -> bool:
        if isinstance(value, str):
            if value == '':
                return True
            else:
                # Check if the string only contains alphanumeric characters, commas, and spaces
                if all(c.isalnum() or c == ',' or c == ' ' for c in value):
                    # Split the string by the comma and check if the resulting parts are not empty
                    parts = value.split(',')
                    return all(part.strip() for part in parts)
        return False
