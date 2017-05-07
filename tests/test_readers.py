import types
import unittest
import responses
from mock import MagicMock

from chomper.readers import *


class FileReaderTest(unittest.TestCase):

    def test_read_file(self):
        node = File('read_file').path('tests/fixtures/data.json').lines(False)
        node.push = MagicMock()
        node(None)
        args, kwargs = node.push.call_args
        self.assertEqual(node.push.call_count, 1)
        self.assertTrue(isinstance(args[0], type('')))

    def test_read_file_lines(self):
        node = File('read_file').path('tests/fixtures/data.jsonlines').lines()
        node.push = MagicMock()
        node(None)
        args, kwargs = node.push.call_args
        self.assertEqual(node.push.call_count, 1)
        self.assertTrue(isinstance(args[0], types.GeneratorType))
        self.assertEqual(next(args[0]), '{ "name": "Jeff Winger", "age": 32 }')
        self.assertEqual(next(args[0]), '{ "name": "Annie Edison", "age": 24 }')


class HttpReaderTest(unittest.TestCase):

    @responses.activate
    def test_get_url(self):
        url = 'http://example.com/data.json'
        responses.add(responses.GET, url, body=open('tests/fixtures/data.json').read())
        node = Http('read_url').get(url).lines(False)
        node.push = MagicMock()
        node(None)

        args, kwargs = node.push.call_args
        self.assertEqual(node.push.call_count, 1)
        self.assertTrue(isinstance(args[0], type('')))

    @responses.activate
    def test_get_url_lines(self):
        url = 'http://example.com/data.jsonlines'
        responses.add(responses.GET, url, body=open('tests/fixtures/data.jsonlines').read())
        node = Http('read_url').get(url).lines()
        node.push = MagicMock()
        node(None)
        args, kwargs = node.push.call_args
        self.assertEqual(node.push.call_count, 1)
        self.assertTrue(isinstance(args[0], types.GeneratorType))
        self.assertEqual(next(args[0]).decode('ascii'), '{ "name": "Jeff Winger", "age": 32 }')
