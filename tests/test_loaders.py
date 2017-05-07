import unittest
from mock import MagicMock

from chomper.loaders import Csv, Json


class CsvLoadersTest(unittest.TestCase):

    def test_loader_csv_string(self):
        loader = Csv('load')
        loader.push = MagicMock()
        loader('Jeff,34\nAnnie,25')
        self.assertEqual(loader.push.call_count, 2)
        self.assertListEqual(loader.push.call_args_list[0][0][0], ['Jeff', '34'])
        self.assertListEqual(loader.push.call_args_list[1][0][0], ['Annie', '25'])

    def test_loader_csv_list(self):
        loader = Csv('load')
        loader.push = MagicMock()
        loader([
            'Jeff,34',
            'Annie,25'
        ])
        self.assertEqual(loader.push.call_count, 2)
        self.assertListEqual(loader.push.call_args_list[0][0][0], ['Jeff', '34'])
        self.assertListEqual(loader.push.call_args_list[1][0][0], ['Annie', '25'])

    def test_loader_csv_generator(self):
        loader = Csv('load')
        loader.push = MagicMock()
        loader((row for row in [
            'Jeff,34',
            'Annie,25'
        ]))
        self.assertEqual(loader.push.call_count, 2)
        self.assertListEqual(loader.push.call_args_list[0][0][0], ['Jeff', '34'])
        self.assertListEqual(loader.push.call_args_list[1][0][0], ['Annie', '25'])

    def test_loader_csv_columns(self):
        loader = Csv('load').columns(['name', 'age'])
        loader.push = MagicMock()
        loader('Jeff,34')
        self.assertDictEqual(loader.push.call_args_list[0][0][0], dict(name='Jeff', age='34'))

    def test_loader_csv_columns_exclude(self):
        loader = Csv('load').columns(['name', None])
        loader.push = MagicMock()
        loader('Jeff,34')
        self.assertDictEqual(loader.push.call_args_list[0][0][0], dict(name='Jeff'))

    def test_loader_csv_skip(self):
        loader = Csv('load').skip(2)
        loader.push = MagicMock()
        loader('\n\nJeff,34')
        self.assertEqual(loader.push.call_count, 1)
        self.assertListEqual(loader.push.call_args_list[0][0][0], ['Jeff', '34'])

    def test_loader_csv_column_types(self):
        loader = Csv('load').types([str, int, None])
        loader.push = MagicMock()
        loader('Jeff,34,')
        self.assertListEqual(loader.push.call_args_list[0][0][0], ['Jeff', 34, None])


class JsonLoaderTest(unittest.TestCase):

    def test_loader_json_string(self):
        loader = Json('load')
        loader.push = MagicMock()
        loader('{"name": "Jeff", "age": 34}')
        self.assertEqual(loader.push.call_count, 1)
        self.assertDictEqual(loader.push.call_args_list[0][0][0], dict(name='Jeff', age=34))

    def test_loader_json_string_array(self):
        loader = Json('load')
        loader.push = MagicMock()
        loader('[1, 2]')
        self.assertEqual(loader.push.call_count, 1)
        self.assertListEqual(loader.push.call_args_list[0][0][0], [1, 2])

    def test_loader_json_iterable(self):
        loader = Json('load')
        loader.push = MagicMock()
        loader((data for data in [
            '{"name": "Jeff", "age": 34}',
            '{"name": "Annie", "age": 25}',
        ]))
        self.assertEqual(loader.push.call_count, 2)
        self.assertDictEqual(loader.push.call_args_list[0][0][0], dict(name='Jeff', age=34))
        self.assertDictEqual(loader.push.call_args_list[1][0][0], dict(name='Annie', age=25))
