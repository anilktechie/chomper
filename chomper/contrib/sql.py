from __future__ import absolute_import

import six

from chomper import Item, config
from chomper.items import Field
from chomper.feeders import Feeder
from chomper.exporters import Exporter
from chomper.processors import Processor
from chomper.exceptions import NotConfigured
from chomper.support.replay import Replayable

try:
    from orator import DatabaseManager
    from orator.query import QueryBuilder
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


Query = Replayable(QueryBuilder)


class SqlAction(object):

    def run_query(self, query, **context):
        db = DatabaseManager(db_config)
        live_query = db.connection().query()
        return query.replay(live_query, self._signature_filter(context))

    def iter_results(self, results):
        for result in results:
            # If the result is a collection then this is a chunked query
            if isinstance(result, Collection):
                for _result in result:
                    yield self._load_item(_result)
            else:
                yield self._load_item(result)

    def _load_item(self, row):
        return Item(**dict((k, v) for k, v in row.items()))

    def _signature_filter(self, context):
        def signature_filter(signature):
            item = context['item']

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


class SqlFeeder(Feeder, SqlAction):

    def __init__(self, query):
        if isinstance(query, six.string_types):
            query = Query().from_(query).get()

        if not isinstance(query, Query):
            raise ValueError('SqlFeeder table must be a query object or a table name.')

        self.query = query

    def __call__(self, item=None):
        return self.feed(item)

    def feed(self, item):
        results = self.run_query(self.query)
        return self.iter_results(results)


class SqlAssigner(Processor, SqlAction):

    def __init__(self, selector):
        super(SqlAssigner, self).__init__(selector)
        raise NotImplementedError()


class SqlTruncator(SqlAction):

    def __init__(self, table):
        self.table = table

    def __call__(self, item):
        self.run_query(Query().from_(self.table).truncate(), item=item)
        return item


class SqlInserter(Exporter, SqlAction):

    def __init__(self, table, id_field='id'):
        self.table = table
        self.id_field = id_field

    def export(self, item):
        _id = self.run_query(Query().from_(self.table).insert_get_id(item), item=item)
        if self.id_field:
            item[self.id_field] = _id
        return item


class SqlUpdater(Exporter, SqlAction):

    def __init__(self, query):
        self.query = query

    def export(self, item):
        self.run_query(self.query, item=item)
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
