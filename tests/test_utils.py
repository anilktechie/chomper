import unittest

from chomper import Item
from chomper.utils import type_name, path_split, path_get, path_set, path_del, path_exists


class UtilsTest(unittest.TestCase):

    def _get_item(self):
        return Item({
            'users': [
                {
                    'name': 'Jeff',
                    'age': 32
                },
                {
                    'name': 'Annie',
                    'age': 23
                }
            ]
        })

    def test_type_name(self):
        self.assertEqual(type_name({}), 'dict')
        self.assertEqual(type_name('test'), 'string')
        self.assertEqual(type_name(1), 'number')
        self.assertEqual(type_name(0), 'number')
        self.assertEqual(type_name(0.001), 'number')
        self.assertEqual(type_name([]), 'list')
        self.assertEqual(type_name(()), 'tuple')
        self.assertEqual(type_name(None), 'none')

    def test_split_path(self):
        self.assertEqual(path_split(''), [])
        self.assertEqual(path_split(None), [])
        self.assertEqual(path_split(True), [])
        self.assertEqual(path_split('users'), ['users'])
        self.assertEqual(path_split('users[0]'), ['users', 0])
        self.assertEqual(path_split('users.0'), ['users', '0'])
        self.assertEqual(path_split('users.0.0[0]'), ['users', '0', '0', 0])
        self.assertEqual(path_split('users[0].address.city'), ['users', 0, 'address', 'city'])
        self.assertEqual(path_split('users[0][1][2].name'), ['users', 0, 1, 2, 'name'])

    def test_path_get(self):
        item = self._get_item()
        self.assertEqual(path_get('this.is.a.bad.path', item), None)
        self.assertEqual(path_get('this.is.a.bad.path', item, 'Jeff'), 'Jeff')
        self.assertEqual(path_get('users[0].name', item), 'Jeff')

    def test_path_set(self):
        item1 = self._get_item()
        path_set('users[0].name', item1, 'Jeff Winger')
        self.assertEqual(item1.users[0]['name'], 'Jeff Winger')

        item2 = self._get_item()
        path_set('users[0]', item2, 'Jeff Winger')
        self.assertEqual(item2.users[0], 'Jeff Winger')

        item3 = self._get_item()
        path_set('users', item3, None)
        self.assertEqual(item3.users, None)

    def test_path_del(self):
        item1 = self._get_item()
        path_del('users[0].name', item1)
        self.assertFalse('name' in item1.users[0])

        item2 = self._get_item()
        path_del('users[1]', item2)
        self.assertEqual(len(item2.users), 1)

    def test_path_exists(self):
        item = self._get_item()
        self.assertTrue(path_exists('', item))
        self.assertTrue(path_exists('users', item))
        self.assertTrue(path_exists('users[0]', item))
        self.assertTrue(path_exists('users[1]', item))
        self.assertTrue(path_exists('users[1].name', item))
        self.assertFalse(path_exists('users.0', item))
        self.assertFalse(path_exists('users[2]', item))
