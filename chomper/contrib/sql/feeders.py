from ... import Item
from ...loaders import Feeder
from ...support.generative import generative
from .base import SqlBase
from .database import Query


class SqlFeederBase(SqlBase, Feeder):

    def feed(self, item):
        raise NotImplementedError()

    def _row_to_item(self, row):
        return Item(**dict((k, v) for k, v in row.items()))


class TableFeeder(SqlFeederBase):

    def __init__(self, table, *args, **kwargs):
        super(TableFeeder, self).__init__(*args, **kwargs)
        self._table = table
        self._chunk_size = kwargs.pop('chunk', 100)

    def feed(self, item):
        results = self._run_query(self._build_query(), item=item)
        for result in self._iter_results(results):
            yield self._row_to_item(result)

    @generative
    def chunk(self, size):
        if not isinstance(size, int) or size < 0:
            raise TypeError('Chunk size must be an positive integer (use 0 to disable chunks)')
        self._chunk_size = size

    def _build_query(self):
        query = Query().from_(self._table)
        if not self._chunk_size:
            query.get()
        else:
            query.chunk(self._chunk_size)
        return query


class QueryFeeder(SqlFeederBase):

    def __init__(self, query, *args, **kwargs):
        super(QueryFeeder, self).__init__(*args, **kwargs)

        if not isinstance(query, Query):
            raise TypeError('QueryFeed query must be a Query instance')

        self._query = query

    def feed(self, item):
        results = self._run_query(self._query, item=item)
        for result in self._iter_results(results):
            yield self._row_to_item(result)
