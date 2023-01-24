#  This file is part of Lazylibrarian.
#
# Purpose:
#    Defines the enums used in the Config system

from enum import Enum


# Types of access
class Access(Enum):
    READ_OK = 'read_ok'
    WRITE_OK = 'write_ok'
    READ_ERR = 'read_error'
    WRITE_ERR = 'write_error'
    CREATE_OK = 'create_ok'
    FORMAT_ERR = 'format_error'


# Schedule intervals
class TimeUnit(Enum):
    MIN = 'min'
    HOUR = 'hour'
    DAY = 'day'


# Reason for change event firing
class OnChangeReason(Enum):
    SETTING = 'setting'
    COPYING = 'copying'
