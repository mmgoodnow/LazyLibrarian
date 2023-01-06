#  This file is part of Lazylibrarian.
#
#  Lazylibrarian is free software':'you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Lazylibrarian is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Lazylibrarian.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import with_statement

import os
import sqlite3
import threading
import time
import traceback
import logging

# DO NOT import from common in this module, circular import
from lazylibrarian.filesystem import DIRS, syspath

db_lock = threading.Lock()


class DBConnection:
    def __init__(self):
        try:
            self.connection = sqlite3.connect(DIRS.get_dbfile(), 20)
            # journal disabled since we never do rollbacks
            self.connection.execute("PRAGMA journal_mode = WAL")
            # sync less often as using WAL mode
            self.connection.execute("PRAGMA synchronous = NORMAL")
            # 32mb of cache
            self.connection.execute("PRAGMA cache_size=-%s" % (32 * 1024))
            # for cascade deletes
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA temp_store = 2")  # memory
            self.connection.row_factory = sqlite3.Row
            self.dblog = syspath(DIRS.get_logfile('database.log'))
            self.logger = logging.getLogger(__name__)
            self.dbcommslogger = logging.getLogger('special.dbcomms')
        except Exception as e:
            self.logger.debug(str(e))
            self.logger.debug(DIRS.get_dbfile())
            self.logger.debug(str(os.stat(DIRS.get_dbfile())))

    def close(self):
        self.dbcommslogger.debug('close')
        with db_lock:
            self.connection.close()

    def commit(self):
        self.dbcommslogger.debug('commit')
        with db_lock:
            self.connection.commit()

    # wrapper function with lock
    def action(self, query, args=None, suppress=None):
        if not query:
            return None
        with db_lock:
            return self._action(query, args, suppress)

    # do not use directly, use through action() or upsert() which add lock
    def _action(self, query: str, args=None, suppress=None):
        sql_result = None
        attempt = 0
        start = time.time()
        while attempt < 5:
            try:
                if not args:
                    # context manager adds commit() on success or rollback() on exception
                    with self.connection:
                        sql_result = self.connection.execute(query)
                else:
                    with self.connection:
                        sql_result = self.connection.execute(query, args)

                elapsed = time.time() - start
                self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                break

            except sqlite3.OperationalError as e:
                if "unable to open database file" in str(e) or "database is locked" in str(e):
                    elapsed = time.time() - start
                    self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                    self.dbcommslogger.debug(f'Database Error {str(e)}')

                    self.logger.warning('Database Error: %s' % e)
                    self.logger.error("Failed db query: [%s]" % query)
                    time.sleep(1)
                else:
                    elapsed = time.time() - start
                    self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                    self.dbcommslogger.debug(f'Database OperationalError {str(e)}')

                    self.logger.error('Database OperationalError: %s' % e)
                    self.logger.error("Failed query: [%s]" % query)
                    raise

            except sqlite3.IntegrityError as e:
                # we could ignore unique errors in sqlite by using "insert or ignore into" statements
                # but this would also ignore null values as we can't specify which errors to ignore :-(
                # Also the python interface to sqlite only returns english text messages, not error codes
                elapsed = time.time() - start
                msg = str(e).lower()
                if suppress and 'UNIQUE' in suppress and ('not unique' in msg or 'unique constraint failed' in msg):
                    self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                    self.dbcommslogger.debug(f'Suppressed {msg}')
                    self.connection.commit()
                    break
                else:
                    self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                    self.dbcommslogger.debug(f'IntegrityError: {msg}')

                    self.logger.error('Database IntegrityError: %s' % e)
                    self.logger.error("Failed query: [%s]" % query)
                    self.logger.error("Failed args: [%s]" % str(args))
                    raise

            except sqlite3.DatabaseError as e:
                elapsed = time.time() - start
                self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                self.dbcommslogger.debug(f'DatabaseError: {str(e)}')

                self.logger.error('Fatal error executing %s :%s: %s' % (query, args, e))
                self.logger.error("%s" % traceback.format_exc())
                raise

            except Exception as e:
                elapsed = time.time() - start
                self.dbcommslogger.debug(f'#{attempt} {elapsed:.4f} {query} [{args}]')
                self.dbcommslogger.debug(f'CatchallError: {str(e)}')

                self.logger.error('Exception executing %s :: %s' % (query, e))
                raise

            finally:
                attempt += 1

        return sql_result

    def match(self, query, args=None):
        try:
            # if there are no results, action() returns None and .fetchone() fails
            sql_results = self.action(query, args).fetchone()
        except sqlite3.Error:
            return []
        if not sql_results:
            return []

        return sql_results

    def select(self, query, args=None):
        try:
            # if there are no results, action() returns None and .fetchall() fails
            sql_results = self.action(query, args).fetchall()
        except sqlite3.Error:
            return []
        if not sql_results:
            return []

        return sql_results

    @staticmethod
    def gen_params(my_dict):
        return [x + " = ?" for x in list(my_dict.keys())]

    def upsert(self, table_name, value_dict, key_dict):
        with db_lock:
            changes_before = self.connection.total_changes

            query = "UPDATE " + table_name + " SET " + ", ".join(self.gen_params(value_dict)) + \
                    " WHERE " + " AND ".join(self.gen_params(key_dict))

            self._action(query, list(value_dict.values()) + list(key_dict.values()))

            # This version of upsert is not thread safe, each action() is thread safe,
            # but it's possible for another thread to jump in between the
            # UPDATE and INSERT statements, so we use suppress=unique to log any conflicts
            # -- update -- should be thread safe now, threading lock moved

            if self.connection.total_changes == changes_before:
                query = "INSERT INTO " + table_name + " ("
                query += ", ".join(list(value_dict.keys()) + list(key_dict.keys())) + ") VALUES ("
                query += ", ".join(["?"] * len(list(value_dict.keys()) + list(key_dict.keys()))) + ")"
                self._action(query, list(value_dict.values()) + list(key_dict.values()), suppress="UNIQUE")
