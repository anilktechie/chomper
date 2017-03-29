from datetime import datetime
from pytz import timezone
import six

from ...exporters import Exporter
from ...utils import smart_invoke
from ...support.generative import generative
from .base import SqlBase
from .database import Query


class SqlExporterBase(SqlBase, Exporter):

    def __init__(self, *args, **kwargs):
        super(SqlExporterBase, self).__init__(*args, **kwargs)

        self._identifiers = kwargs.pop('identifiers', [])
        self._columns = kwargs.pop('columns', None)
        self._protected_columns = kwargs.pop('protected', None)

        self._set_id_field = False
        self._id_field = False
        self._id_column = False

        self._table_columns = []

        self._insert_query = None

        self._timestamps = False
        self._created_at_column = None
        self._updated_at_column = None
        self._timestamps_timezone = None

    def export(self, item):
        raise NotImplementedError()

    @generative
    def identifiers(self, identifiers):
        if isinstance(identifiers, six.string_types):
            identifiers = [identifiers]
        self._identifiers = identifiers

    @generative
    def columns(self, columns):
        if isinstance(columns, six.string_types):
            columns = [columns]
        self._columns = columns

    @generative
    def protected(self, columns):
        if isinstance(columns, six.string_types):
            columns = [columns]
        self._protected_columns = columns

    @generative
    def id_field(self, id_field='id', id_column='id'):
        self._set_id_field = True
        self._id_field = id_field
        self._id_column = id_column

    @generative
    def timestamps(self, timezone='UTC', created_at='created_at', updated_at='updated_at'):
        self._timestamps = True
        self._created_at_column = created_at
        self._updated_at_column = updated_at
        self._timestamps_timezone = timezone

    def _build_select_query(self, item):
        q = Query().from_(self._table)
        q = self._build_where_clause(q, item)
        return q.first()

    def _build_update_query(self, item):
        q = Query().from_(self._table)
        q = self._build_where_clause(q, item)
        return q.update(self._prepare_fields(item, timestamps=self._timestamps))

    def _build_insert_query(self, item):
        data = self._prepare_fields(item, timestamps=self._timestamps, inserting=True)
        return Query().from_(self._table).insert_get_id(data)

    def _build_where_clause(self, query, item):
        try:
            for ident in self._identifiers:
                query = query.where(ident, '=', item[ident])
        except TypeError:
            raise ValueError('Cannot build query where clause without identifier columns.')
        else:
            return query

    def _get_exportable_fields(self):
        """
        Get a list of the columns in the database table that we can insert/update.

        If a list of columns has been provided they will be used, otherwise inspect
        the table schema to get a list of available columns.

        Also remove any fields that have been marked as protected by the user.
        """
        if self._columns:
            columns = self._columns
        elif self._table_columns:
            columns = self._table_columns
        else:
            columns = self._get_table_columns(self._table)
            self._table_columns = columns

        if self._protected_columns:
            columns = [column for column in columns if column not in self._protected_columns]

        return columns

    def _prepare_fields(self, item, timestamps=False, inserting=False):
        """
        Return a dict of data that only contains fields that can be safely inserted into the table
        """
        fields = self._get_exportable_fields()
        data = dict((key, value) for key, value in six.iteritems(item) if key in fields)

        if timestamps:
            data[self._updated_at_column] = self._get_timestamp()

        if timestamps and inserting:
            data[self._created_at_column] = self._get_timestamp()

        return data

    def _get_timestamp(self):
        return datetime.now(timezone(self._timestamps_timezone))


class Inserter(SqlExporterBase):

    def __init__(self, table=None, *args, **kwargs):
        super(Inserter, self).__init__(*args, **kwargs)
        self._table = table
        self._insert_query = None

    def export(self, item):
        insert = self._insert_query if self._insert_query else self._build_insert_query(item)
        inserted_id = self._run_query(insert)

        if inserted_id and self._set_id_field:
            item[self._id_field] = inserted_id

        return item

    @generative
    def insert(self, query):
        self._insert_query = query


class Updater(SqlExporterBase):

    def __init__(self, table, *args, **kwargs):
        super(Updater, self).__init__(*args, **kwargs)
        self._table = table
        self._update_query = None

    def export(self, item):
        update = self._update_query if self._update_query else self._build_update_query(item)
        self._run_query(update)
        return item

    @generative
    def update(self, query):
        self._update_query = query


class Upserter(SqlExporterBase):
    """
    Upsert rows in a SQL database

    NOTE: this upsert implementation isn't perfect; it is susceptible to some race conditions
        (see: https://www.depesz.com/2012/06/10/why-is-upsert-so-complicated/)

    TODO: batch upsert using Postgres COPY from a csv file
        (example: https://gist.github.com/luke/5697511)

    :param table: Name of the database table to upsert row in
    :param database: Database connection name, should reference a section in your config
    :param identifiers: List of column names used to identify an item as unique (used in SQL where clause)
    :param columns: List of columns to be updated. By default all fields on the item are updated
    :param protected: List of columns that should never be modified
    :param id_field: Name of the id field, used to update the item when a new row is inserted
        (None to disable adding the id to the item after an insert)
    """

    EVENT_UPDATE = 'update'
    EVENT_INSERT = 'insert'
    EVENT_CHANGE = 'change'

    def __init__(self, table=None, *args, **kwargs):
        super(Upserter, self).__init__(*args, **kwargs)
        self._table = table
        self._select_query = None
        self._insert_query = None
        self._update_query = None
        self._listeners = {}

    def export(self, item):
        select = self._select_query if self._select_query else self._build_select_query(item)
        result = self._run_query(select)

        if result:
            update = self._update_query if self._update_query else self._build_update_query(item)
            self._run_query(update)
            if self._set_id_field:
                item[self._id_field] = result.get(self._id_column)
        else:
            insert = self._insert_query if self._insert_query else self._build_insert_query(item)
            inserted_id = self._run_query(insert)
            if inserted_id and self._set_id_field:
                item[self._id_field] = inserted_id

        self._notify_change_listeners(item, result)

        return item

    @generative
    def select(self, query):
        self._select_query = query

    @generative
    def insert(self, query):
        self._insert_query = query

    @generative
    def update(self, query):
        self._update_query = query

    @generative
    def on(self, event, field, listener=None):
        if event not in [self.EVENT_INSERT, self.EVENT_UPDATE, self.EVENT_CHANGE]:
            raise ValueError('Unsupported event type "%s"' % event)

        if listener is None:
            listener = field
            event_key = event
        else:
            event_key = '%s.%s' % (event, field)

        if not callable(listener):
            raise ValueError('Event listener must be callable')

        try:
            self._listeners[event_key].update([listener])
        except KeyError:
            self._listeners[event_key] = {listener}

    def _notify_change_listeners(self, current, previous=None):
        """
        Call any change listeners

        :param current: The current state in the database
        :param previous: The database state of the row before the upsert
        """
        triggered_listeners = []

        if previous:
            triggered_listeners.append(self.EVENT_UPDATE)
        else:
            triggered_listeners.append(self.EVENT_INSERT)

        if not previous:
            triggered_listeners += ['%s.%s' % (self.EVENT_INSERT, key) for key in current.keys()]
            triggered_listeners += ['%s.%s' % (self.EVENT_CHANGE, key) for key in current.keys()]
        else:
            for key, value in six.iteritems(current):
                if key not in previous or not value == previous[key]:
                    triggered_listeners += [
                        '%s.%s' % (self.EVENT_CHANGE, key),
                        '%s.%s' % (self.EVENT_UPDATE, key)
                    ]

        for listener_name in triggered_listeners:
            self._invoke_change_listener(listener_name, [current, previous])

    def _invoke_change_listener(self, listener_name, listener_args):
        if listener_name not in self._listeners:
            return
        listeners = self._listeners[listener_name]
        for listener in listeners:
            smart_invoke(listener, listener_args)


class Truncator(SqlBase):

    def __init__(self, table=None, *args, **kwargs):
        super(Truncator, self).__init__(*args, **kwargs)
        self._table = table

    def __call__(self, item=None):
        self._run_query(Query().from_(self._table).truncate())
        return item
