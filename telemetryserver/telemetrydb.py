# Database for holding LazyLibrarian telemetry data
#
# Requires MySQL to be installed

import mysql.connector
import mysql.connector.errorcode
import time
import datetime
import json
import logging
import configparser

class TelemetryDB():
    """ Handler for the LL telemetry database """
    connection = None

    def __init__(self, config: configparser.ConfigParser):
        self.host = config.get('database', 'host', fallback='localhost')
        self.user = config.get('database', 'user', fallback='root')
        self.password = config.get('database', 'password', fallback='secret789')
        self.DBName = config.get('database', 'dbname', fallback='LazyTelemetry')
        self.retries = int(config.get('database', 'retries', fallback=3))

        self.logger = logging.getLogger(__name__)

    def __del__(self):
        if self.connection:
            self.connection.commit()
            self.connection.close()
        self.connection = None

    def _connect(self):
        """ Connect to the database, return cursor """
        if not self.connection:
            self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.DBName
            )

        return self.connection.cursor()

    def connect_to_db_patiently(self) -> bool:
        retries = 0
        self.logger.debug(f'Will try DB connection #{self.retries} times')
        while retries < self.retries:
            self.logger.debug(f'Connecting to database, try #{retries}')
            try:
                cursor = self._connect()
                if cursor:
                    return True
            except Exception as e:
                retries += 1
                self.logger.info(f'DB error {str(e)}: wait, then retry')
                time.sleep(5)
                dberror = str(e)

        self.logger.error(f"Could not connect to database after {retries} attempts, exiting")
        return False

    def ensure_db_exists(self):
        cursor = self._connect()
        self.logger.debug('Ensuring database exists')
        try:
            try:
                cursor.execute(f"CREATE DATABASE {self.DBName};")
            except mysql.connector.Error as e:
                if e.errno == mysql.connector.errorcode.ER_DB_CREATE_EXISTS:
                    pass # self is ok
        finally:
            cursor.close()

    def ensure_column(self, cursor, tablename, column):
        try:
            alter_statement = f"ALTER TABLE {tablename} ADD COLUMN {column};"
            cursor.execute(alter_statement)
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_DUP_FIELDNAME:
                pass # We expect self most of the time
            else:
                self.logger.error(f"Unexpected error creating column {tablename}.{column}: {e.errno}")
        except Exception as e:
            self.logger.error(f"Unexpected exception creating column: {str(e)}")

    def ensure_table(self, tablename, columns):
        self.logger.debug(f'Ensuring table {tablename}')
        cursor = self._connect()
        try:
            create_it = True
            try:
                self.logger.debug(f'Get existing table info')
                cursor.execute(f"SHOW TABLES like '{tablename}';")
                for tbl in cursor:
                    if tbl[0] == tablename:
                        create_it = False
            except mysql.connector.Error as e:
                if e.errno == mysql.connector.errorcode.ER_NO_SUCH_TABLE:
                    create_it = True
                else:
                    self.logger.debug(f'Table {tablename} exists')

            if create_it:
                create_statement = f"CREATE TABLE {tablename} (id INT AUTO_INCREMENT PRIMARY KEY);"
                self.logger.debug(f'Execute {create_statement}')
                try:
                    cursor.execute(create_statement)
                    self.logger.debug(f'Created table ok')
                except Exception as e:
                    self.logger.error(f'Error creating table {tablename}: {str(e)}')

            [self.ensure_column(cursor, tablename, column) for column in columns]
        finally:
            cursor.close()


    def ensure_db_schema(self):
        self.logger.info('Ensuring database schema is correct')
        self.ensure_table("ll_servers",[
            "serverid       VARCHAR(50) NOT NULL",
            "os             VARCHAR(50)",
            "first_seen     DATETIME NOT NULL",
            "last_seen      DATETIME NOT NULL",
            "last_uptime    INT NOT NULL",
            "longest_uptime INT NOT NULL",
            "ll_version     VARCHAR(50)",
            "ll_installtype VARCHAR(20)",
        ])
        self.ensure_table("ll_configs",[
            "serverid       VARCHAR(50) NOT NULL",
            "datetime       DATETIME NOT NULL",
            "switches       VARCHAR(255)",
            "params         VARCHAR(255)",
            "book_api       VARCHAR(50)",
            "newznab        INT NOT NULL",
            "torznab        INT NOT NULL",
            "rss            INT NOT NULL",
            "irc            INT NOT NULL",
            "gen            INT NOT NULL",
            "apprise        INT NOT NULL",
        ])
        self.ensure_table("ll_telemetry",[
            "serverid       VARCHAR(50) NOT NULL",
            "datetime       DATETIME NOT NULL",
        ])

    def _update_server_data(self, server):
        """ Returns current datetime string """
        self.logger.debug(f'Store server data {server}')
        try:
            nowstr = f"'{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"
            cursor = self._connect()
            try:
                entry = None
                # Load existing row, if it exists
                stmt = f"""SELECT last_seen, longest_uptime FROM ll_servers WHERE serverid = '{server["id"]}';"""
                cursor.execute(stmt)
                for row in cursor:
                    entry = row

                if entry:
                    longest_up = max(server["uptime_seconds"], entry[1])
                    stmt = f"""UPDATE ll_servers SET
                        last_seen = {nowstr}, last_uptime = {server["uptime_seconds"]}, os = '{server["os"]}',
                        longest_uptime = {longest_up}, ll_version = '{server["version"]}', ll_installtype = '{server["install_type"]}'"""
                    cursor.execute(stmt)
                else:
                    stmt = (f"""INSERT INTO ll_servers
                        (serverid, os, first_seen, last_seen, last_uptime, longest_uptime, ll_version, ll_installtype)
                        VALUES
                        ('{server["id"]}', '{server["os"]}', {nowstr}, {nowstr}, {server["uptime_seconds"]}, {server["uptime_seconds"]},
                        '{server["version"]}', '{server["install_type"]}') """)
                    cursor.execute(stmt)
            finally:
                cursor.close()

        except Exception as e:
            raise Exception(f"Error updating server data: {str(e)}")

        return nowstr

    def _add_config_data(self, serverid, nowstr, config):
        self.logger.debug(f'Store config data {config}')
        try:
            cursor = self._connect()
            try:
                keys = "serverid, datetime, "
                params = f"'{serverid}', {nowstr}, "
                keys = keys + ", ".join([key.lower() for key in config.keys()])
                params = params + ", ".join([f"'{str(value)}'" for value in config.values()])

                stmt = f"INSERT INTO ll_configs ({keys}) VALUES ({params})"
                cursor.execute(stmt)
            finally:
                cursor.close()
        except Exception as e:
            raise Exception(f"Error updating config data: {str(e)}")

    def _add_usage_data(self, serverid, nowstr, usage):
        self.logger.debug(f'Store usage data {usage}')
        try:
            cursor = self._connect()
            try:
                keys = "serverid, datetime"
                params = f"'{serverid}', {nowstr}"

                # Create a column for each usage data provided
                for key in usage.keys():
                    columnname = "".join(c for c in key if c.isalpha())
                    columnspec = f"{columnname} INT"
                    self.ensure_column(cursor, "ll_telemetry", columnspec)
                    keys += ", " + columnname
                    params += ", " + str(usage[key])

                stmt = f"INSERT INTO ll_telemetry ({keys}) VALUES ({params})"
                cursor.execute(stmt)
            finally:
                cursor.close()
        except Exception as e:
            raise Exception(f"Error updating telemetry data: {str(e)}")

    def add_telemetry(self, telemetry_data):
        """ Add telemetry received from LL to the database
        Returns status string """
        self.logger.debug(f'Parsing telemetry data {telemetry_data}')
        server = None
        config = None
        serverid = None
        usage = None
        try:
            server = list2dict(telemetry_data['server'])
            serverid = server["id"] if 'id' in server.keys() else None
            if not serverid:
                return "Need server ID in telemetry data"
            config = list2dict(telemetry_data['config']) if 'config' in telemetry_data.keys() else None
            usage  = list2dict(telemetry_data['usage'])  if 'usage' in telemetry_data.keys() else None
        except Exception as e:
            if not server:
                return "No server data in json, aborting"

        try:
            now = self._update_server_data(server)
            processed = ['server']
            if config:
                self._add_config_data(serverid, now, config)
                processed.append('config')
            if usage:
                self._add_usage_data(serverid, now, usage)
                processed.append('usage')

            self.connection.commit()
            self.logger.debug('Processed data ok')
            return f"ok. Processed {processed}"
        except Exception as e:
            self.logger.error(f'Error processing data {str(e)}')
            return str(e)

    def initialize(self):
        """ Initialize the database.
        Returns True if all is well. """
        if not self.connect_to_db_patiently():
            return False
        self.logger.info('Initializing database')
        try:
            self.ensure_db_exists()
            self.ensure_db_schema()
            return True
        except mysql.connector.Error as e:
            self.logger.error(f"Database error: {e}")
        return False

# Helper functions

def list2dict(obj):
    """ Turn a list object holding a string into a dict """
    if isinstance(obj, list):
        return json.loads(obj[0])
    else:
        return obj

