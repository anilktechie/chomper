import six

from ...processors import Processor, field_processor, item_processor
from .base import SqlBase
from .database import Query

from orator.support.collection import Collection


class SqlProcessorBase(SqlBase, Processor):

    pass


class QueryAssigner(SqlProcessorBase):

    def __init__(self, selector, query, **kwargs):
        if not isinstance(query, Query):
            raise TypeError('QueryFeed query must be a Query instance')

        self.query = query

        super(QueryAssigner, self).__init__(selector, **kwargs)

    @item_processor()
    def assign_item(self, item):
        result = self.run_query(self.query, item=item)

        if isinstance(result, Collection):
            try:
                result = result[0]
            except IndexError:
                self.logger.warning('Query returned more then one result. Only the first result will be used.')

        result = result.serialize()

        if isinstance(result, dict):
            for key, value in six.iteritems(result):
                item[key] = value
        else:
            self.logger.warning('Could not assign query result to item as the query did not return a result.')

        return item

    @field_processor()
    def assign_field(self, key, value, item):
        result = self._run_query(self.query, item=item)
        result.serialize()
        return key, result
