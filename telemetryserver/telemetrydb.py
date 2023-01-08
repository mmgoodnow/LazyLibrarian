# Database for holding LazyLibrarian telemetry data
#
# 
import configparser
import datetime
import json
import logging
import sqlite3
import time


class TelemetryDB:
    """ Handler for the LL telemetry database """
    connection = None

    def __init__(self, config: configparser.ConfigParser):
        self.host = config.get('database', 'host', fallback='localhost')
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
            self.connection = sqlite3.connect('/data/' + self.DBName,
                                              detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            self.connection.execute("PRAGMA temp_store = 2")  # memory
            self.connection.row_factory = sqlite3.Row
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

        self.logger.error(f"Could not connect to database after {retries} attempts, exiting")
        return False

    def ensure_db_exists(self):
        self.logger.debug('Ensuring database exists')
        cursor = None
        try:
            cursor = self._connect()
        except Exception as e:
            self.logger.error(f"Unexpected exception creating database: {str(e)}")
        finally:
            if cursor:
                cursor.close()

    def ensure_column(self, cursor, tablename, column):
        try:
            columns = cursor.execute('PRAGMA table_info(%s)' % tablename).fetchall()
            to_create = False
            if not columns:  # check for no such table
                to_create = not any(item[1].split()[0] == column.split()[0] for item in columns)
            if to_create:
                alter_statement = f"ALTER TABLE {tablename} ADD COLUMN {column};"
                self.logger.debug(f'Execute {alter_statement}')
                cursor.execute(alter_statement)
        except Exception as e:
            self.logger.error(f"Unexpected error creating column {tablename}.{column}: {str(e)}")

    def ensure_table(self, tablename, columns):
        self.logger.debug(f'Ensuring table {tablename}')
        cursor = self._connect()
        try:
            create_statement = f"CREATE TABLE IF NOT EXISTS {tablename} (rowid INTEGER PRIMARY KEY);"
            self.logger.debug(f'Execute {create_statement}')
            try:
                cursor.execute(create_statement)
                self.logger.info(f'Created table ok')
            except Exception as e:
                self.logger.error(f'Error creating table {tablename}: {str(e)}')

            [self.ensure_column(cursor, tablename, column) for column in columns]
        finally:
            cursor.close()

    def ensure_db_schema(self):
        self.logger.info('Ensuring database schema is correct')
        self.ensure_table("ll_servers", [
            "serverid       VARCHAR(50) NOT NULL",
            "os             VARCHAR(50)",
            "first_seen     TIMESTAMP NOT NULL",
            "last_seen      TIMESTAMP NOT NULL",
            "last_uptime    INT NOT NULL",
            "longest_uptime INT NOT NULL",
            "ll_version     VARCHAR(50)",
            "ll_installtype VARCHAR(20)",
            "python_ver     VARCHAR(200)",
        ])
        self.ensure_table("ll_configs", [
            "serverid       VARCHAR(50) NOT NULL",
            "datetime       TIMESTAMP NOT NULL",
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
        self.ensure_table("ll_telemetry", [
            "serverid       VARCHAR(50) NOT NULL",
            "datetime       TIMESTAMP NOT NULL",
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
                        longest_uptime = {longest_up}, ll_version = '{server["version"]}', 
                        ll_installtype = '{server["install_type"]}', python_ver='{server["python_ver"]}'
                        WHERE serverid = '{server["id"]}'"""
                    cursor.execute(stmt)
                else:
                    stmt = (f"""INSERT INTO ll_servers
                        (serverid, os, first_seen, last_seen, last_uptime, longest_uptime, ll_version, 
                        ll_installtype, python_ver)
                        VALUES
                        ('{server["id"]}', '{server["os"]}', {nowstr}, {nowstr}, {server["uptime_seconds"]},
                            {server["uptime_seconds"]}, {server["version"]}', '{server["install_type"]}', 
                            {server["python_ver"]}') """)
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

    def read_telemetry(self, telemetry_data):
        """ Read telemetry data, returns data as json """
        self.logger.debug(f'Reading telemetry data {telemetry_data}')
        result = {}
        cursor = None
        try:
            cursor = self._connect()
            if telemetry_data in ['usage', 'all']:
                last_hour_date_time = datetime.datetime.now() - datetime.timedelta(minutes=60)
                stmt = f"SELECT COUNT(*) as  last_hour from ll_telemetry where datetime > '{last_hour_date_time}'"
                res = cursor.execute(stmt).fetchone()
                last_hour = res[0]
                last_day_date_time = datetime.datetime.now() - datetime.timedelta(hours=24)
                stmt = f"SELECT COUNT(*) as  last_day from ll_telemetry where datetime > '{last_day_date_time}'"
                res = cursor.execute(stmt).fetchone()
                last_day = res[0]
                last_week_date_time = datetime.datetime.now() - datetime.timedelta(days=7)
                stmt = f"SELECT COUNT(*) as  last_week from ll_telemetry where datetime > '{last_week_date_time}'"
                res = cursor.execute(stmt).fetchone()
                last_week = res[0]
                last_month_date_time = datetime.datetime.now() - datetime.timedelta(days=28)
                stmt = f"SELECT COUNT(*) as  last_month from ll_telemetry where datetime > '{last_month_date_time}'"
                res = cursor.execute(stmt).fetchone()
                last_month = res[0]
                stmt = f"SELECT COUNT(*) from ll_telemetry"
                res = cursor.execute(stmt).fetchone()
                all_time = res[0]
                result['usage'] = {'Last_Hour': last_hour, 'Last_Day': last_day, 'Last_Week': last_week,
                                   'Last_Four_Weeks': last_month, 'All_Time': all_time}
            if telemetry_data in ['servers', 'all']:
                versions = {}
                for key in ['python_ver', 'll_version', 'll_installtype']:
                    stmt = f"select distinct {key} from ll_servers"
                    res = cursor.execute(stmt).fetchall()
                    for item in res:
                        stmt = f"select count(*) as count from ll_servers where {key} = '{item[0]}'"
                        tot = cursor.execute(stmt).fetchone()
                        versions[item[0]] = tot[0]
                result['servers'] = versions
            if telemetry_data in ['switches', 'params', 'all']:
                last_4wks_date_time = datetime.datetime.now() - datetime.timedelta(days=28)
                if telemetry_data == 'all':
                    telemetry_types = ['switches', 'params']
                else:
                    telemetry_types = [telemetry_data]
                for telemetry_type in telemetry_types:
                    results = {}
                    stmt = f"""select {telemetry_type} as data from ll_configs,ll_servers 
                                where ll_configs.serverid = ll_servers.serverid and datetime = last_seen 
                                and last_seen >= '{last_4wks_date_time}'"""
                    configs = cursor.execute(stmt).fetchall()
                    for conf in configs:
                        for item in conf[0].split():
                            if item not in results:
                                results[item] = 1
                            else:
                                results[item] = results[item] + 1
                    result[telemetry_type] = results
            if telemetry_data in ['configs', 'all']:
                configs = {}
                last_4wks_date_time = datetime.datetime.now() - datetime.timedelta(days=28)
                stmt = f"""select distinct book_api from ll_configs,ll_servers where 
                            ll_configs.serverid = ll_servers.serverid and datetime = last_seen 
                            and last_seen >= '{last_4wks_date_time}'"""
                res = cursor.execute(stmt).fetchall()
                for item in res:
                    stmt = f"""select count(*) as count from ll_configs,ll_servers where book_api = '{item[0]}' and 
                                ll_configs.serverid = ll_servers.serverid and datetime = last_seen 
                                and last_seen >= '{last_4wks_date_time}'"""
                    tot = cursor.execute(stmt).fetchone()
                    configs[item[0]] = tot[0]
                for key in ['newznab', 'torznab', 'rss', 'irc', 'gen', 'apprise']:
                    stmt = f"""select count(*) from ll_configs,ll_servers where {key} > 0 and 
                                ll_configs.serverid = ll_servers.serverid and datetime = last_seen 
                                and last_seen >= '{last_4wks_date_time}'"""
                    tot = cursor.execute(stmt).fetchone()
                    configs[key] = tot[0]
                result['configs'] = configs
        except Exception as e:
            raise Exception(f"Error reading data: {str(e)}")
        finally:
            if cursor:
                cursor.close()
        if not result:
            result = f"{telemetry_data} Not implemented yet"
        return json.dumps(result)

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
            usage = list2dict(telemetry_data['usage']) if 'usage' in telemetry_data.keys() else None
        except Exception as e:
            if not server:
                return f"No server data in json, aborting. {str(e)}"

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
        except Exception as e:
            self.logger.error(f"Database error: {e}")
        return False


# Helper functions

def list2dict(obj):
    """ Turn a list object holding a string into a dict """
    if isinstance(obj, list):
        return json.loads(obj[0])
    else:
        return obj
