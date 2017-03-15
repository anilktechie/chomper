from __future__ import absolute_import

import six
from orator.connectors.postgres_connector import DictRow

from chomper import Item, config
from chomper.items import Field
from chomper.feeders import Feeder
from chomper.exporters import Exporter
from chomper.processors import Processor, item_processor, field_processor
from chomper.exceptions import NotConfigured
from chomper.support.replay import Replayable, replay

try:
    from orator import DatabaseManager as BaseDatabaseManager
    from orator.query import QueryBuilder
    from orator.support.collection import Collection
except ImportError:
    raise NotConfigured('Orator library is required to use the SQL module.')


class DatabaseManager(BaseDatabaseManager):

    def set_connection_config(self, name, connection):
        if name not in self._config:
            self._config[name] = connection


database = DatabaseManager({})
Query = Replayable(QueryBuilder)


class SqlMixin(object):

    driver_name = None
    connection_name = None

    def __init__(self, *args, **kwargs):
        default_connection_name = getattr(self.__class__, 'connection_name')
        default_connection_config = config.getdict(default_connection_name)

        self.connection_name = kwargs.pop('connection_name', default_connection_name)
        self.connection_config = kwargs.pop('connection', default_connection_config)

        if 'driver' not in self.connection_config:
            self.connection_config['driver'] = self.driver_name

        database.set_connection_config(self.connection_name, self.connection_config)

        super(SqlMixin, self).__init__(*args, **kwargs)

    def get_connection(self):
        return database.connection(self.connection_name)

    def run_query(self, query, **context):
        live_query = self.get_connection().query()
        return replay(query, live_query, self._signature_filter(context))

    @staticmethod
    def row_to_dict(obj):
        """
        Convert a Orator DictRow object to a dict
        """
        if isinstance(object, dict):
            return obj
        if not isinstance(obj, DictRow):
            raise ValueError('Attempted to convert non DictRow object to dict.')
        return dict((k, v) for k, v in obj.items())

    @classmethod
    def iter_results(cls, results):
        """
        Iterate over rows in an Orator results collection.

        Also needs to handle chuncked queries.
        """
        for result in results:
            if isinstance(result, Collection):
                for _result in result:
                    yield cls.row_to_dict(_result)
            else:
                yield cls.row_to_dict(result)

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


class SqlProcessor(SqlMixin, Processor):

    pass


class SqlExporter(SqlMixin, Exporter):

    pass


class SqlFeeder(SqlMixin, Feeder):

    def __init__(self, query, chunk_size=100, *args, **kwargs):
        super(SqlFeeder, self).__init__(*args, **kwargs)

        # Do we need to build a query from the table name?
        if isinstance(query, six.string_types):
            query = Query().from_(query)
            if not chunk_size or chunk_size < 1:
                query.get()
            else:
                query.chunk(chunk_size)

        if not isinstance(query, Query):
            raise ValueError('SqlFeeder table must be a query object or a table name.')

        self.query = query

    def __call__(self, item=None):
        return self.feed(item)

    def feed(self, item):
        results = self.run_query(self.query, item=item)
        for result in self.iter_results(results):
            yield self._load_item(result)

    def _load_item(self, row):
        return Item(**dict((k, v) for k, v in row.items()))


class SqlTruncator(SqlMixin):

    def __init__(self, table, *args, **kwargs):
        super(SqlTruncator, self).__init__(*args, **kwargs)
        self.table = table

    def __call__(self, item):
        self.run_query(Query().from_(self.table).truncate(), item=item)
        return item


class SqlInserter(SqlExporter):

    def __init__(self, table, id_field='id', *args, **kwargs):
        super(SqlInserter, self).__init__(*args, **kwargs)
        self.table = table
        self.id_field = id_field

    def export(self, item):
        _id = self.run_query(Query().from_(self.table).insert_get_id(item), item=item)
        if self.id_field:
            item[self.id_field] = _id
        return item


class SqlUpdater(SqlExporter):

    def __init__(self, query, *args, **kwargs):
        super(SqlUpdater, self).__init__(*args, **kwargs)
        self.query = query

    def export(self, item):
        self.run_query(self.query, item=item)
        return item


class SqlUpserter(SqlExporter):

    def __init__(self, table, identifiers=('id',), columns=None, listeners=None, *args, **kwargs):
        if isinstance(identifiers, six.string_types):
            identifiers = [identifiers]

        if isinstance(columns, six.string_types):
            columns = [columns]

        # If columns are not provided try to update all columns that exist on the table
        if not columns:
            columns = self._table_columns(table)

        self.table = table
        self.identifiers = identifiers
        self.columns = columns

        # Find any change listeners in kwargs (Eg. on_update, on_insert, on_title_change)
        self.listeners = {key: kwargs.pop(key) for key in kwargs.keys() if key[:3] == 'on_'}
        if listeners is not None:
            self.listeners.update(listeners)

        super(SqlUpserter, self).__init__(*args, **kwargs)

    def export(self, item):
        pass

    def _table_columns(self, table):
        raise NotImplementedError()


class SqlAssigner(SqlProcessor):

    def __init__(self, selector, query, **kwargs):
        super(SqlAssigner, self).__init__(selector, **kwargs)
        self.query = query

    @item_processor()
    def assign_item(self, item):
        result = self.run_query(self.query, item=item)

        if isinstance(result, Collection):
            try:
                result = result[0]
            except IndexError:
                self.logger.warning('Query returned more then one result. Only the first result will be used.')

        if isinstance(result, DictRow):
            result = self.row_to_dict(result)

        if isinstance(result, dict):
            for key, value in six.iteritems(result):
                item[key] = value
        else:
            self.logger.warning('Could not assign query result to item as the query did not return a result.')

        return item

    @field_processor()
    def assign_field(self, key, value, item):
        result = self.run_query(self.query, item=item)

        if isinstance(result, Collection):
            result = list(result)

        if isinstance(result, DictRow):
            result = self.row_to_dict(result)

        return key, result


class PostgresFeeder(SqlFeeder):

    connection_name = 'postgres'
    driver_name = 'postgres'


class PostgresInserter(SqlInserter):

    connection_name = 'postgres'
    driver_name = 'postgres'


class PostgresTruncator(SqlTruncator):

    connection_name = 'postgres'
    driver_name = 'postgres'


class PostgresUpdater(SqlUpdater):

    connection_name = 'postgres'
    driver_name = 'postgres'


class PostgresUpserter(SqlUpserter):

    connection_name = 'postgres'
    driver_name = 'postgres'


class PostgresAssigner(SqlAssigner):

    connection_name = 'postgres'
    driver_name = 'postgres'
