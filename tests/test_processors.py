import unittest

from chomper import Item
from chomper.exceptions import ItemNotImportable
from chomper.processors import Defaulter, Assigner, ItemDropper, FieldDropper, ValueFilter, ValueMapper, KeyMapper, \
    FieldPicker, FieldOmitter


class ProcessorsTest(unittest.TestCase):

    def test_defaulter(self):
        process = Defaulter(dict(hello='world'))
        process_func = Defaulter(lambda i: dict(world=i.hello))

        self.assertEquals(process(Item()).hello, 'world')
        self.assertEquals(process(Item(hello2='world2')).hello2, 'world2')
        self.assertEquals(process(Item(hello='universe')).hello, 'universe')
        self.assertEquals(process(Item(hello=False)).hello, False)
        self.assertEquals(process(Item(hello=0)).hello, 0)
        self.assertEquals(process(Item(hello=None)).hello, 'world')
        self.assertEquals(process_func(Item(hello='world')).world, 'world')
        self.assertRaises(ValueError, Defaulter, {})
        self.assertRaises(ValueError, Defaulter, None)

    def test_assigner(self):
        process = Assigner(Item.hello, 'world')
        process_func = Assigner(Item.hello, lambda: 'world')

        self.assertEqual(process(Item()).hello, 'world')
        self.assertEqual(process(Item(hello=True)).hello, 'world')
        self.assertEqual(process_func(Item()).hello, 'world')

    def test_item_dropper(self):
        process = ItemDropper(Item.hello == 'world')

        self.assertRaises(ItemNotImportable, process, Item(hello='world'))

        item = Item(hello='universe')
        self.assertEqual(process(item), item)

    def test_field_dropper(self):
        process = FieldDropper(Item.hello, Item.planet.is_in(['earth', 'mars']))

        item1 = Item(planet='earth', hello='earth')
        item2 = Item(planet='pluto', hello='pluto')

        self.assertFalse(hasattr(process(item1), 'hello'))
        self.assertTrue(hasattr(process(item2), 'hello'))

    def test_value_filter(self):
        process = ValueFilter(Item.hello, lambda val: 'hello ' + val)
        self.assertEqual(process(Item(hello='world')).hello, 'hello world')
        self.assertFalse(hasattr(process(Item()), 'hello'))

    def test_value_mapper(self):
        mapping = {
            'hello': 'world',
            'world': 'hello'
        }

        process = ValueMapper(Item.field1, mapping)
        item = Item(field1='hello', field2='hello')
        self.assertEquals(process(item).field1, 'world')
        self.assertEquals(process(item).field2, 'hello')

        process_func = ValueMapper(Item.field1, lambda: mapping)
        item_func = Item(field1='hello', field2='hello')
        self.assertEquals(process_func(item_func).field1, 'world')
        self.assertEquals(process_func(item_func).field2, 'hello')

        self.assertRaises(ValueError, ValueMapper, Item.field, [])
        self.assertRaises(ValueError, ValueMapper, Item.field, None)

    def test_key_mapper(self):
        mapping = {
            'field1': 'field2',
            'field3': 'field4',
            'field5': 'field6'
        }

        process = KeyMapper(mapping)
        item = Item(field1='value', field3='value', field7='value')
        processed_item = process(item)

        self.assertFalse(hasattr(processed_item, 'field1'))
        self.assertTrue(hasattr(processed_item, 'field2'))

        self.assertFalse(hasattr(processed_item, 'field3'))
        self.assertTrue(hasattr(processed_item, 'field4'))

        self.assertFalse(hasattr(processed_item, 'field5'))
        self.assertFalse(hasattr(processed_item, 'field6'))

        self.assertTrue(hasattr(processed_item, 'field7'))

        process_func = KeyMapper(lambda: mapping)
        item_func = Item(field1='value')
        processed_item_func = process_func(item_func)

        self.assertFalse(hasattr(processed_item_func, 'field1'))
        self.assertTrue(hasattr(processed_item_func, 'field2'))

    def test_field_picker(self):
        process = FieldPicker(Item.field1, Item.field2)
        item = Item(field1='value', field2='value', field3='value')
        processed_item = process(item)

        self.assertTrue(hasattr(processed_item, 'field1'))
        self.assertTrue(hasattr(processed_item, 'field2'))
        self.assertFalse(hasattr(processed_item, 'field3'))

    def test_field_omitter(self):
        process = FieldOmitter(Item.field1, Item.field2)
        item = Item(field1='value', field2='value', field3='value')
        processed_item = process(item)

        self.assertFalse(hasattr(processed_item, 'field1'))
        self.assertFalse(hasattr(processed_item, 'field2'))
        self.assertTrue(hasattr(processed_item, 'field3'))
