import unittest

from chomper import Item
from chomper.feeders import ListFeeder, CsvFeeder, JsonFeeder, JsonLinesFeeder


class ReadersTest(unittest.TestCase):

    def test_list_feeder(self):
        data = [
            {
                'name': 'Jeff Winger',
                'age': 32
            },
            {
                'name': 'Annie Edison',
                'age': 24
            }
        ]

        feeder = ListFeeder(data)
        items = feeder()
        item1 = next(items)
        item2 = next(items)
        self.assertEqual(item1.name, data[0]['name'])
        self.assertEqual(item1.age, data[0]['age'])
        self.assertEqual(item2.name, data[1]['name'])
        self.assertEqual(item2.age, data[1]['age'])
        self.assertRaises(StopIteration, next, items)

    def test_csv_feeder(self):
        feeder = CsvFeeder('tests/fixtures/data.csv', ['name', 'age'], skip=1)
        items = feeder()

        item1 = next(items)
        self.assertTrue(isinstance(item1, Item))
        self.assertEqual(item1.name, 'Jeff Winger')
        self.assertEqual(item1.age, '32')

        item2 = next(items)
        self.assertTrue(isinstance(item2, Item))

        item3 = next(items)
        self.assertTrue(isinstance(item3, Item))

        self.assertRaises(StopIteration, next, items)

    def test_json_feeder(self):
        feeder = JsonFeeder('tests/fixtures/data.json')
        items = feeder()

        item1 = next(items)
        self.assertTrue(isinstance(item1, Item))
        self.assertEqual(item1.name, 'Jeff Winger')
        self.assertEqual(item1.age, 32)

        item2 = next(items)
        self.assertTrue(isinstance(item2, Item))

        item3 = next(items)
        self.assertTrue(isinstance(item3, Item))

        self.assertRaises(StopIteration, next, items)

    def test_json_lines_feeder(self):
        feeder = JsonLinesFeeder('tests/fixtures/data.jsonlines')
        items = feeder()

        item1 = next(items)
        self.assertTrue(isinstance(item1, Item))
        self.assertEqual(item1.name, 'Jeff Winger')
        self.assertEqual(item1.age, 32)

        item2 = next(items)
        self.assertTrue(isinstance(item2, Item))

        item3 = next(items)
        self.assertTrue(isinstance(item3, Item))

        self.assertRaises(StopIteration, next, items)
