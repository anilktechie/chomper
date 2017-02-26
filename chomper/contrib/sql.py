from __future__ import absolute_import

import six

from chomper import Item, config
from chomper.items import Field
from chomper.feeders import Feeder
from chomper.exporters import Exporter
from chomper.processors import Processor
from chomper.exceptions import NotConfigured
from chomper.support import SignatureInterceptor

try:
    from orator import DatabaseManager
    from orator.query import QueryBuilder as OratorQueryBuilder
    from orator.support.collection import Collection
except ImportError:
    raise NotConfigured('Orator library is required to use the SQL module.')


db_config = {
    'pgsql': {
        'driver': 'pgsql',
        'database': 'test',
        'user': 'postgres',
        'password': 'postgres',
        'log_queries': True
    }
}


BUILDER_METHODS = (
    'select', 'select_raw', 'add_select', 'distinct', 'from_',

    'join', 'join_where', 'left_join', 'left_join_where', 'right_join', 'right_join_where',

    'where', 'or_where', 'where_raw', 'or_where_raw', 'where_between', 'or_where_between', 'where_not_between',
    'or_where_not_between', 'where_exists', 'or_where_exists', 'where_not_exists', 'or_where_not_exists', 'where_in',
    'or_where_in', 'where_not_in', 'or_where_not_in', 'where_null', 'or_where_null', 'where_not_null',
    'or_where_not_null', 'where_date', 'where_day', 'where_month', 'where_year',

    'group_by', 'having', 'or_having', 'having_raw', 'or_having_raw',

    'order_by', 'latest', 'oldest', 'order_by_raw', 'offset', 'skip', 'limit', 'take',

    'find', 'pluck', 'first', 'get', 'chunk', 'lists', 'implode',

    'exists', 'count', 'min', 'max', 'sum', 'avg', 'aggregate',

    'insert', 'insert_get_id',

    'update', 'increment', 'decrement',

    'delete', 'truncate',

    'raw'
)

UNSUPPORTED_BUILDER_METHODS = (
    'select_sub', 'where_nested', 'for_page', 'union', 'union_all', 'lock', 'lock_for_update', 'shared_lock',
    'to_sql', 'paginate', 'simple_paginate',
)


@six.add_metaclass(SignatureInterceptor)
class Query(OratorQueryBuilder):
    """
    Wrapper around the Orator query builder

    Allows us to use item field references and expressions when building database queries.
    """

    __intercept__ = BUILDER_METHODS

    def __init__(self, table, connection=None):
        # Init the Orator QueryBuilder without the connection, grammar and processor
        # We just need to make sure the connection is set before executing any database operations
        super(Query, self).__init__(None, None, None)

        self._context = dict()

        self.table = table
        self.from_(self.table)

        if connection:
            self.set_connection(connection)

    def set_connection(self, connection):
        self._connection = connection
        self._grammar = connection.get_query_grammar()
        self._processor = connection.get_post_processor()

    def filter_signature(self, signature, context):
        item = context.item

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


class SqlFeeder(Feeder):

    def __init__(self, query):
        if isinstance(query, six.string_types):
            table = Query(query)
            table.get()

        if not isinstance(query, Query):
            raise ValueError('SqlFeeder table must be a query object or a table name.')

        self.query = query

    def __call__(self, item=None):
        db = DatabaseManager(db_config)
        self.query.set_connection(db.connection())
        self.query.enable_execution()
        items = self.feed(item)
        self.query.disable_execution()
        return items

    def feed(self, item):
        results = self.query.execute(item=item)
        return self.iter_results(results)

    def iter_results(self, results):
        for result in results:
            # If the result is a collection then this is a chunked query
            if isinstance(result, Collection):
                for _result in result:
                    yield self.load_item(_result)
            else:
                yield self.load_item(result)

    def load_item(self, row):
        return Item(**dict((k, v) for k, v in row.items()))


class SqlAssigner(Processor):

    def __init__(self, selector):
        super(SqlAssigner, self).__init__(selector)
        raise NotImplementedError()


class SqlTruncator(object):

    def __init__(self, table):
        self.query = Query(table)
        self.query.truncate()

    def __call__(self, item):
        self.query.enable_execution()
        self.query.execute(item=item)
        self.query.disable_execution()
        return item


class SqlInserter(Exporter):

    def __init__(self, table, id_field='id'):
        self.table = table
        self.id_field = id_field

    def export(self, item):
        query = Query(self.table)
        query.insert_get_id(item)

        query.enable_execution()
        _id = query.execute(item=item)
        query.disable_execution()

        if self.id_field:
            item[self.id_field] = _id

        return item


class SqlUpdater(Exporter):

    def __init__(self, query):
        self.query = query

    def export(self, item):
        self.query.enable_execution()
        self.query.execute(item=item)
        self.query.disable_execution()
        return item


class SqlUpserter(Exporter):

    def __init__(self, table, identifiers=None, columns=None, _listeners=None, **listeners):
        """
        Upsert rows in a Postgres database

        NOTE: this upsert implementation isn't perfect; it is susceptible to some race conditions
            (see: https://www.depesz.com/2012/06/10/why-is-upsert-so-complicated/)

        TODO: batch upsert using Postgres COPY from a csv file
            (example: https://gist.github.com/luke/5697511)

        :param table: The database table name
        :param identifiers: List of column names used to identify an item as unique (used in SQL where clause)
        :param columns: List of columns to be updated. By default all fields on the item are updated (if match columns)
        """
        if isinstance(identifiers, six.string_types):
            identifiers = [identifiers]

        if isinstance(columns, six.string_types):
            columns = [columns]

        # If columns are not provided try to update all columns that exist on the table
        if not columns:
            columns = self._table_columns(table)

        # By default, try to update on the item's id
        if not identifiers:
            self.identifiers = ['id']

        self.table = table
        self.identifiers = identifiers
        self.columns = columns

        # Find any change listeners in kwargs (Eg. on_update, on_insert, on_title_change)
        self.listeners = {key: listeners.pop(key) for key in listeners.keys() if key[:3] == 'on_'}
        if _listeners is not None:
            self.listeners.update(_listeners)

    def export(self, item):
        pass

    def _table_columns(self, table):
        sql = 'SELECT column_name FROM information_schema.columns WHERE table_name = %s'
