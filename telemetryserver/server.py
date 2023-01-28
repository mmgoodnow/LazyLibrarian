# Server object for LazyLibrarian telemetry server

import yaml
import logging
import logging.config
import configparser
from bottle import Bottle, request, response
import telemetrydb
import telemetryweb

import datetime
from functools import wraps


def bottle_to_logger(fn):
    """ Helper function, making Bottle logging go to our logger """

    @wraps(fn)
    def _log_to_logger(*args, **kwargs):
        request_time = datetime.datetime.now()
        actual_response = fn(*args, **kwargs)
        # modify this to log exactly what you need:
        _logger.info('BOB %s %s %s %s %s' % (request.remote_addr,
                                             request_time,
                                             request.method,
                                             request.url,
                                             response.status))
        return actual_response

    return _log_to_logger


class TelemetryServer:
    config: configparser.ConfigParser

    def __init__(self):
        self._telemetry_db = None

    def initialize(self):
        # Parse command line
        # Read config file
        self.config = configparser.ConfigParser()
        self.config.read('telemetry.ini')

        self._initlogger()
        app = Bottle()
        app.install(bottle_to_logger)

    def _initlogger(self):
        with open("tslogging.yaml", "r") as stream:
            try:
                logsettings = yaml.safe_load(stream)
                logging.config.dictConfig(logsettings)
            except yaml.YAMLError as exc:
                print(f"YAML error reading logging config: {str(exc)}")
            except Exception as e:
                print(f"Error reading logging config: {str(e)}")
        self.logger = logging.getLogger(__name__)
        self.logger.disabled = False
        self.logger.info('Starting LazyLibrarian telemetry server')

    def start(self):
        # Open the database and start the server
        self._telemetry_db = telemetrydb.TelemetryDB(self.config)
        self.logger.debug('Initializing database')
        if self._telemetry_db.initialize():
            self.logger.debug('Run server')
            telemetryweb.run_server(self._telemetry_db.add_telemetry, self._telemetry_db.read_telemetry,
                                    self._telemetry_db.read_csv)

    def stop(self):
        self.logger.debug('Stopping server')
        self._telemetry_db = None  # Closes the database


_logger = logging.getLogger(__name__)
