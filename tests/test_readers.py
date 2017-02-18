import unittest
import responses

from chomper.readers import Resource, FileReader, HttpReader


data_json_contents = """[
  {
    "name": "Jeff Winger",
    "age": 32
  },
  {
    "name": "Annie Edison",
    "age": 24
  },
  {
    "name": "Britta Perry",
    "age": 27
  }
]
"""


class ReadersTest(unittest.TestCase):

    def test_resource_parse_uri_scheme(self):
        uris = (
            ('http://hostname.com/path/file.csv', 'http'),
            ('https://hostname.com/path/file.csv', 'https'),

            ('ftp://hostname.com/path/file.csv', 'ftp'),

            ('/home/user/file.csv', 'file'),
            ('./directory/file.csv', 'file'),
            ('directory/file.csv', 'file'),
            ('file:///home/user/file.csv', 'file'),

            ('s3://bucket/key', 's3'),
            ('s3://user:pass@bucket/key', 's3'),
        )

        for uri, scheme in uris:
            res = Resource(uri)
            self.assertEqual(res.scheme, scheme)

    def test_file_reader(self):
        reader = FileReader.from_uri('tests/fixtures/data.json', lines=False)
        reader_lines = FileReader.from_uri('tests/fixtures/data.jsonlines')

        reader_data = reader.read()
        self.assertEqual(next(reader_data), data_json_contents)
        self.assertRaises(StopIteration, next, reader_data)

        reader_lines_data = reader_lines.read()
        self.assertEqual(next(reader_lines_data), '{ "name": "Jeff Winger", "age": 32 }')
        self.assertEqual(next(reader_lines_data), '{ "name": "Annie Edison", "age": 24 }')
        self.assertEqual(next(reader_lines_data), '{ "name": "Britta Perry", "age": 27 }')
        self.assertRaises(StopIteration, next, reader_lines_data)

    @responses.activate
    def test_http_reader(self):
        url = 'http://example.com/data.json'
        url_lines = 'http://example.com/data.jsonlines'

        responses.add(responses.GET, url, body=open('tests/fixtures/data.json').read())
        responses.add(responses.GET, url_lines, body=open('tests/fixtures/data.jsonlines').read())

        reader = HttpReader.from_uri(url, lines=False)
        reader_lines = HttpReader.from_uri(url_lines)

        reader_data = reader.read()
        self.assertEqual(next(reader_data), data_json_contents)
        self.assertRaises(StopIteration, next, reader_data)

        reader_lines_data = reader_lines.read()
        self.assertEqual(next(reader_lines_data), '{ "name": "Jeff Winger", "age": 32 }')
        self.assertEqual(next(reader_lines_data), '{ "name": "Annie Edison", "age": 24 }')
        self.assertEqual(next(reader_lines_data), '{ "name": "Britta Perry", "age": 27 }')
        self.assertRaises(StopIteration, next, reader_lines_data)
