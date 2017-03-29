from datetime import datetime
from pytz import timezone

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock

from . import SqlTestCaseBase
from chomper import Item
from chomper.contrib.sql import Query, TableFeeder, QueryFeeder, Inserter, Updater, Upserter, Truncator


class SqlFeederTest(SqlTestCaseBase):

    @classmethod
    def setUpClass(cls):
        cls._execute_sql_file('tests/fixtures/test_sql_postgres_feeder.sql')

    def test_postgres_table_feeder(self):
        feeder = TableFeeder('feeder_test')
        items = feeder()

        result = next(items)
        self.assertEqual(result.first_name, 'Jeff')
        self.assertEqual(result.last_name, 'Winger')
        self.assertEqual(result.age, 32)

        result = next(items)
        self.assertEqual(result.first_name, 'Annie')
        self.assertEqual(result.last_name, 'Edison')
        self.assertEqual(result.age, 23)

    def test_postgres_query_feeder(self):
        query = Query().from_('feeder_test').where('age', '<', 30).get()
        feeder = QueryFeeder(query)
        items = feeder()

        result = next(items)
        self.assertEqual(result.first_name, 'Annie')
        self.assertEqual(result.age, 23)

        result = next(items)
        self.assertEqual(result.first_name, 'Britta')
        self.assertEqual(result.age, 28)


class SqlInserterTest(SqlTestCaseBase):

    @classmethod
    def setUpClass(cls):
        cls._execute_sql_file('tests/fixtures/test_sql_postgres_inserter.sql')

    def test_postgres_inserter(self):
        inserter = Inserter('inserter_test')

        item = inserter(Item(first_name='Jeff', age=32, missing_column=True))
        item_db = self.db.select('SELECT * from inserter_test WHERE first_name = %s', ('Jeff', ))[0]

        self.assertEqual(item_db[1], 'Jeff')
        self.assertEqual(item_db[2], None)
        self.assertEqual(item_db[3], 32)

    def test_postgres_inserter_columns(self):
        inserter = Inserter().table('inserter_test').columns(['first_name', 'last_name'])

        item = inserter(Item(first_name='Annie', last_name='Edison', age=23))
        item_db = self.db.select('SELECT * from inserter_test WHERE first_name = %s', ('Annie',))[0]

        self.assertEqual(item_db[1], 'Annie')
        self.assertEqual(item_db[2], 'Edison')
        self.assertEqual(item_db[3], None)


class SqlUpdaterTest(SqlTestCaseBase):

    @classmethod
    def setUpClass(cls):
        cls._execute_sql_file('tests/fixtures/test_sql_postgres_updater.sql')

    def test_postgres_updater(self):
        updater = Updater('updater_test').identifiers('first_name')

        updater(Item(first_name='Jeff', last_name='Winger', age=32))
        item_db = self.db.select('SELECT * from updater_test WHERE first_name = %s', ('Jeff', ))[0]

        self.assertEqual(item_db[2], 'Winger')
        self.assertEqual(item_db[3], 32)


class SqlUpserterTest(SqlTestCaseBase):

    def setUp(self):
        self._execute_sql_file('tests/fixtures/test_sql_postgres_upserter.sql')

    def test_postgres_upserter(self):
        upserter = Upserter('upserter_test').identifiers('first_name').timestamps()

        item1 = Item(first_name='Jeff', last_name='Winger', age=32)
        item2 = Item(first_name='Annie', last_name='Edison', age=23, missing_column=True)
        upserter(item1)
        upserter(item2)

        rows = self.db.select('SELECT * from upserter_test')

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1], item1.first_name)
        self.assertEqual(rows[0][2], item1.last_name)
        self.assertEqual(rows[0][3], item1.age)
        self.assertEqual(rows[1][1], item2.first_name)
        self.assertEqual(rows[1][2], item2.last_name)
        self.assertEqual(rows[1][3], item2.age)

    def test_postgres_upserter_timestamps(self):
        started_at = datetime.now(timezone('UTC'))
        upserter = Upserter('upserter_test').identifiers('first_name').timestamps()

        upserter(Item(first_name='Jeff'))
        upserter(Item(first_name='Annie'))

        rows = self.db.select('SELECT * from upserter_test')

        self.assertEqual(rows[0][4], datetime(2017, 1, 1, 0, 0, 0, 0, timezone('UTC')))
        self.assertNotEqual(rows[0][5], datetime(2017, 1, 1, 0, 0, 0, 0, timezone('UTC')))
        self.assertTrue(rows[0][5] >= started_at)
        self.assertTrue(rows[1][4] >= started_at)
        self.assertTrue(rows[1][5] >= started_at)

    def test_postgres_upserter_id_field(self):
        upserter = Upserter('upserter_test').identifiers('first_name').id_field('person_id')

        item_update = Item(first_name='Jeff')
        item_insert = Item(first_name='Annie')
        upserter(item_update)
        upserter(item_insert)

        self.assertTrue('person_id' in item_update)
        self.assertTrue('person_id' in item_insert)
        self.assertTrue(isinstance(item_update.person_id, int))
        self.assertTrue(isinstance(item_insert.person_id, int))

    def test_postgres_upserter_mapping(self):
        upserter = Upserter('upserter_test').identifiers('first_name').mapping({
            'last_name': 'family_name'
        })

        item = Item(first_name='Jeff', family_name='Edison', age=123)
        upserter(item)

        rows = self.db.select('SELECT * from upserter_test')
        self.assertEqual(rows[0][2], 'Edison')
        self.assertEqual(rows[0][3], None, msg='Should not update unmapped column')

    def test_postgres_upserter_mapping_without_restrict(self):
        upserter = Upserter('upserter_test').identifiers('first_name').mapping({
            'last_name': 'family_name'
        }, restrict=False)

        item = Item(first_name='Jeff', family_name='Edison', age=123)
        upserter(item)

        rows = self.db.select('SELECT * from upserter_test')
        self.assertEqual(rows[0][2], 'Edison')
        self.assertEqual(rows[0][3], 123)

    def test_postgres_upserter_insert_listener(self):
        on_insert = MagicMock()
        upserter = Upserter('upserter_test').identifiers('first_name').on('insert', on_insert)

        item = Item(first_name='Annie', last_name='Edison', age=23)
        upserter(item)

        on_insert.assert_called_once_with(item)

    def test_postgres_upserter_update_listener(self):
        on_update = MagicMock()
        upserter = Upserter('upserter_test').identifiers('first_name').on('update', on_update)

        item = Item(first_name='Jeff', last_name='Winger', age=32)
        upserter(item)

        on_update.assert_called_once_with(item)

    def test_postgres_upserter_field_change_listener(self):
        on_change_first_name = MagicMock()
        on_change_last_name = MagicMock()
        on_change_age = MagicMock()

        upserter = Upserter('upserter_test').identifiers('first_name')\
            .on('change', 'first_name', on_change_first_name)\
            .on('change', 'last_name', on_change_last_name)\
            .on('change', 'age', on_change_age)

        item = Item(first_name='Jeff', last_name='Winger', age=32)
        upserter(item)

        on_change_first_name.assert_not_called()
        on_change_last_name.assert_called_once_with(item)
        on_change_age.assert_called_once_with(item)


class SqlTruncatorTest(SqlTestCaseBase):

    @classmethod
    def setUpClass(cls):
        cls._execute_sql_file('tests/fixtures/test_sql_postgres_truncator.sql')

    def test_postgres_truncator(self):
        truncator = Truncator('truncator_test')
        truncator()
        rows = self.db.select('SELECT * from truncator_test')
        self.assertEqual(len(rows), 0)
