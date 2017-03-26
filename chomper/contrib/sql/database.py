from chomper.exceptions import NotConfigured
from chomper.support.replay import Replayable

try:
    from orator import DatabaseManager as BaseDatabaseManager
    from orator.connections import PostgresConnection as BasePostgresConnection
    from orator.connections import MySQLConnection as BaseMySQLConnection
    from orator.connections import SQLiteConnection as BaseSQLiteConnection
    from orator.connectors.connection_factory import ConnectionFactory
    from orator.query import QueryBuilder
except ImportError:
    raise NotConfigured('Orator library must be installed to use the SQL module.')


class DatabaseManager(BaseDatabaseManager):

    def set_connection_config(self, name, connection):
        if name not in self._config:
            self._config[name] = connection


class SQLiteConnection(BaseSQLiteConnection):

    def table_columns(self, table_name):
        columns = self.select('PRAGMA table_info(%s)', (table_name, ))
        return [column[1] for column in columns]


class MySQLConnection(BaseMySQLConnection):

    def table_columns(self, table_name):
        columns = self.select('SHOW COLUMNS FROM %s', (table_name, ))
        return [column[0] for column in columns]


class PostgresConnection(BasePostgresConnection):

    def table_columns(self, table_name):
        sql = 'SELECT column_name FROM information_schema.columns WHERE table_name = %s'
        columns = self.select(sql, (table_name, ))
        return [column[0] for column in columns]


ConnectionFactory.register_connection('sqlite', SQLiteConnection)
ConnectionFactory.register_connection('mysql', MySQLConnection)
ConnectionFactory.register_connection('postgres', PostgresConnection)
ConnectionFactory.register_connection('pgsql', PostgresConnection)

manager = DatabaseManager({})

Query = Replayable(QueryBuilder)
