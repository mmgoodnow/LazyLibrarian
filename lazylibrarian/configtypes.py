
#  This file is part of Lazylibrarian.
#
# Purpose:
#    Defines all of the different types of configs that can be
#    found in LazyLibrarian's config.ini (or eventually DB)

from typing import NewType, Dict, Union, List, Type
from enum import Enum
from configparser import ConfigParser
from collections import Counter
from re import match, compile, IGNORECASE

from lazylibrarian import logger

### Type aliases to distinguish types of string
Email = NewType('Email', str)
CSVstr = NewType('CSV', str)
URLstr = NewType('URL', str)
ValidIntTypes = Union[int, bool]
ValidStrTypes =  Union[str, Email, CSVstr, URLstr]
ValidTypes = Union[ValidStrTypes, ValidIntTypes]

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
        self.accesses = Counter()
        self.is_new = is_new
        if self.is_valid_value(default):
            self.value = default
        else:
            raise RuntimeError(f'Cannot initialize {section}.{key} as {default}')

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

    def get_force_lower(self):
        return False

    def get_int(self) -> int:
        self._on_read(False)
        return 0

    def __int__(self) -> int:  # Make it the default when accessing the object as int
        return self.get_int()

    def set_int(self, value: int) -> bool:
        return False

    def get_bool(self) -> bool:
        self._on_read(False)
        return False

    def set_bool(self, value: bool) -> bool:
        return False

    def is_valid_value(self, value: ValidTypes) -> bool:
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
    """ A config item that is a string """
    def __init__(self, section: str, key: str, default: str, force_lower: bool=False, is_new: bool=False):
        self.force_lower = force_lower
        super().__init__(section, key, default, is_new)

    def set_str(self, value: str) -> bool:
        if self.force_lower:
            return self._on_set(value.lower())
        else:
            return self._on_set(value)

    def get_force_lower(self):
        return self.force_lower

    def set_int(self, value: int) -> bool:
        return self._on_type_mismatch(value)

    def set_bool(self, value: int) -> bool:
        return self._on_type_mismatch(value)

class ConfigInt(ConfigItem):
    """ A config item that is an int """
    def __init__(self, section: str, key: str, default: int, is_new: bool=False):
        super().__init__(section, key, default, is_new)

    def get_int(self) -> int:
        if self._on_read(type(self.value) in [int, bool]):
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

class ConfigRangedInt(ConfigInt):
    """ An int config item that must be in a particular range """
    def __init__(self, section: str, key: str, default: int, 
        range_min: int, range_max: int, is_new: bool=False):
        self.range_min = range_min
        self.range_max = range_max
        super().__init__(section, key, default, is_new)

    def is_valid_value(self, value: ValidTypes) -> bool:
        return int(value) >= self.range_min and int(value) <= self.range_max

class ConfigPerm(ConfigInt):
    """ Represents UNIX file permissions. Emitted as an Octal string """
    def __init__(self, section: str, key: str, default: int, is_new: bool=False):
        super().__init__(section, key, default, is_new)

    def get_str(self) -> str:
        self._on_read(True)
        return oct(int(self.value))

    def set_str(self, value: str) -> bool:
        # It's an int, but can be set with an octal string
        return self._on_set(value)

    def get_int(self) -> int:
        if self._on_read(type(self.value) in [int, str]):
            if type(self.value) == int:
                return int(self.value)
            else:
                return int(str(self.value), 8)
        else:
            return int(self.default)

    def is_valid_value(self, value: ValidTypes) -> bool:
        try:
            if type(value) == str:
                value = oct(int(str(value), 8)) # Should now be a valid Oct string
            elif type(value) == int:
                value = oct(int(value))
            else:
                return False

            if value[:2] != '0o':
                return False

            intval = int(value[2:], 8)
            return intval >= 0 and intval <= 0o777
        except ValueError:
            return False

class ConfigBool(ConfigInt):
    """ A config item that is a bool """
    def __init__(self, section: str, key: str, default: bool|int, is_new: bool=False):
        super().__init__(section, key, default, is_new)

    def get_bool(self) -> bool:
        if self._on_read(type(self.value) in [bool, int]): # We're ok with ints
            return bool(self.value)
        else:
            return False

    def set_bool(self, value: bool|int) -> bool:
        return self._on_set(value)

    def set_int(self, value: int) -> bool:
        return self.set_bool(bool(value))

    def set_str(self, value: str) -> bool:
        return self._on_type_mismatch(value)

    def update_from_parser(self, parser: ConfigParser, name: str) -> bool:
        return self.set_bool(parser.getboolean(self.section, name, fallback=False))
        
class ConfigEmail(ConfigStr):
    """ A config item that is a string that must be a valid email address """
    def __init__(self, section: str, key: str, default: str, is_new: bool=False):
        return super().__init__(section, key, default, force_lower=True, is_new=is_new)

    def get_email(self) -> Email:
        return Email(self.get_str())

    def is_valid_value(self, value: ValidTypes) -> bool:
        value = str(value)
        if value == '':
            return True
        else:
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

    def is_valid_value(self, value: ValidTypes) -> bool:
        if isinstance(value, str):
            if value == '':
                return True
            else:
                # Check if the string only contains alphanumeric characters, and select symbols
                if all(c.isalnum() or c in ', !-+#' for c in value):
                    # Split the string by the comma and check if the resulting parts are not empty
                    parts = value.split(',')
                    return all(part.strip() for part in parts)
        return False

class ConfigURL(ConfigStr):
    """ A config item that is a string that must be a valid URL """
    def get_url(self) -> URLstr:
        return URLstr(self.get_str())

    def set_str(self, value: str):
        value = value.rstrip('/')
        super().set_str(value)

    def is_valid_value(self, value: ValidTypes) -> bool:
        if isinstance(value, str):
            if value == '':
                return True
            else:
                regex = compile(
                    r'^(?:http|ftp)s?://' # http:// or https://
                    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                    r'localhost|' #localhost...
                    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                    r'(?::\d+)?' # optional port
                    r'(?:/?|[/?]\S+)$', IGNORECASE)
                
                # check if the URL matches the regular expression
                return regex.match(value) is not None
        return False
