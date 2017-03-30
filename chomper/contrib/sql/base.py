import types
import six

from ...exceptions import NotConfigured
from ...support.replay import replay
from ...support.generative import GenerativeBase, generative
from ...config import config
from ...items import Item, Field
from .database import manager

try:
    from orator.support.collection import Collection
    from orator.connectors.postgres_connector import DictRow
except ImportError:
    raise NotConfigured('Orator library is required to use the SQL module.')


class DatabaseNotConfigured(NotConfigured):
    pass


class SqlBase(GenerativeBase):

    DEFAULT_PORTS = {
        'sqlite': None,
        'mysql': 3306,
        'postgres': 5432,
        'pgsql': 5432
    }

    _connection_name_template = '%(driver)s__%(host)s__%(database)s__%(user)s'

    def __init__(self, *args, **kwargs):
        self._set_connection(kwargs.pop('database', None))
        self._table = kwargs.pop('table', None)
        super(SqlBase, self).__init__()

    @property
    def manager(self):
        return manager

    @generative
    def database(self, database):
        if isinstance(database, six.string_types):
            self._set_connection(database)
        elif isinstance(database, dict):
            self._set_connection(self._connection_name_template % database, database)
        else:
            raise ValueError('Database must either be a config section module, '
                             'or a dict containing connection config.')

    @generative
    def table(self, table):
        self._table = table

    def _set_connection(self, connection_name=None, connection_config=None):
        if connection_name is None:
            connection_name = self._get_default_connection_name()

        if connection_config is None:
            connection_config = self._get_connection_config(connection_name)

        self._connection_name = connection_name
        self._connection_config = connection_config

        if self._connection_name and self._connection_config:
            manager.set_connection_config(self._connection_name, self._connection_config)

    def _get_default_connection_name(self):
        return config.get('sql', 'default', None)

    def _get_connection_config(self, connection_name=None):
        if not isinstance(connection_name, six.string_types):
            return None

        if not config.has_section(connection_name):
            raise DatabaseNotConfigured(
                'Database config for connection name "%s" could not be found'
                % connection_name)

        driver = config.get(connection_name, 'driver')

        if not driver:
            raise DatabaseNotConfigured(
                'Database driver name was missing from connection configuration. '
                'The driver should be either sqlite, mysql, postgres or pgsql.'
                % connection_name)

        return dict(
            driver=driver,
            host=config.get(connection_name, 'host', '127.0.0.1'),
            port=config.get(connection_name, 'port', self.DEFAULT_PORTS[driver]),
            database=config.get(connection_name, 'database'),
            user=config.get(connection_name, 'user'),
            password=config.get(connection_name, 'password'),
            prefix=config.get(connection_name, 'prefix', ''),
            log_queries=config.getboolean(connection_name, 'log_queries', False)
        )

    def _get_connection(self):
        return manager.connection(self._connection_name)

    def _get_query(self):
        return self._get_connection().query()

    def _run_query(self, query, **context):
        return replay(query, self._get_query(), self._signature_filter(context))

    def _get_table_columns(self, table_name):
        return self._get_connection().table_columns(table_name)

    @classmethod
    def _iter_results(cls, results):
        """
        Iterate over rows in an Orator results collection.
        """
        if isinstance(results, types.GeneratorType):
            # Chunked query
            for result_chunk in results:
                for result in result_chunk:
                    yield result
        elif isinstance(results, Collection):
            # Collection of rows
            for result in results:
                yield result
        else:
            # Single row
            yield results

    def _signature_filter(self, context):
        def signature_filter(signature):
            item = context.get('item')

            def _filter(args, key, value):
                if isinstance(value, Item):
                    args[key] = item
                if isinstance(value, Field):
                    args[key] = item[value]

            for key, value in enumerate(signature.args):
                _filter(signature.args, key, value)

            for key, value in six.iteritems(signature.kwargs):
                _filter(signature.kwargs, key, value)

            return signature

        return signature_filter
