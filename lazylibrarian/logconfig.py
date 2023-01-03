#  This file is part of Lazylibrarian.
#
# Purpose:
#    Handles logging configuration, stored in logging.yaml but with some
#    overrides from config.ini

import logging
import logging.config
from typing import Dict

import yaml

from lazylibrarian.filesystem import DIRS


def read_log_config(yamlname='logging.yaml', fixfilenames=False) -> Dict:
    with open(yamlname, "r") as stream:
        try:
            logsettings = yaml.safe_load(stream)
            for handler in logsettings['handlers']:
                settings = logsettings['handlers'][handler]
                if fixfilenames and 'filename' in settings:
                    filename = settings['filename']
                    logsettings['handlers'][handler]['filename'] = DIRS.get_logfile(filename)
            logging.config.dictConfig(logsettings)
            return logsettings
        except yaml.YAMLError as exc:
            print(f"YAML error reading logging config: {str(exc)}")
        except Exception as e:
            raise RuntimeError(f"Error reading logging config, exiting: {str(e)}")


def enable_logger(logname: str, enabled: bool = True) -> logging.Logger:
    """ Enable/disable the logger named logname, return the logger """
    logger = logging.getLogger(logname)
    # Use the disabled property of the logger, though the logic is reversed
    logger.disabled = not enabled
    return logger
