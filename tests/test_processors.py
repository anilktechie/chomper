import unittest

from chomper import Item
from chomper.exceptions import DropItem
from chomper.processors import Defaulter, Assigner, Dropper, Filter, Mapper, Picker, Omitter, item_processor, \
    field_processor


class ProcessorsTest(unittest.TestCase):

    def test_item_processor_decorator(self):
        def processor():
            pass

        dec = item_processor()
        func = dec(processor)

        self.assertTrue(func.is_processor)
        self.assertEqual(func.accept_types, ('item',))

    def test_field_processor_decorator(self):
        def processor():
            pass

        dec_all = field_processor()
        dec_dict = field_processor(dict)
        dec_multiple = field_processor(str, None)

        func_all = dec_all(processor)
        all_types = func_all.accept_types
        self.assertTrue(func_all.is_processor)
        self.assertTrue('string' in all_types)
        self.assertTrue('dict' in all_types)
        self.assertTrue('list' in all_types)
        self.assertTrue('none' in all_types)
        self.assertTrue('boolean' in all_types)
        self.assertTrue('item' not in all_types)

        func_dict = dec_dict(processor)
        self.assertTrue(func_dict.is_processor)
        self.assertEqual(func_dict.accept_types, ('dict',))

        func_multiple = dec_multiple(processor)
        self.assertTrue(func_multiple.is_processor)
        self.assertEqual(func_multiple.accept_types, ('string', 'none',))


    def test_defaulter(self):
        process = Defaulter(Item, dict(hello='world'))
        self.assertEquals(process(Item()).hello, 'world')
        self.assertEquals(process(Item(hello2='world2')).hello2, 'world2')
        self.assertEquals(process(Item(hello='universe')).hello, 'universe')
        self.assertEquals(process(Item(hello=False)).hello, False)
        self.assertEquals(process(Item(hello=0)).hello, 0)
        self.assertEquals(process(Item(hello=None)).hello, 'world')

    def test_defaulter_func(self):
        process_func = Defaulter(Item, lambda: dict(hello='world'))
        self.assertEquals(process_func(Item()).hello, 'world')
        self.assertEquals(process_func(Item(hello=None)).hello, 'world')
        self.assertEquals(process_func(Item(hello='universe')).hello, 'universe')

    def test_defaulter_dict_field(self):
        defaults = dict(country='Australia')
        process = Defaulter(Item.address, defaults)
        self.assertEquals(process(Item(address=None)).address, defaults)
        self.assertEquals(process(Item(address={})).address, defaults)
        self.assertEquals(process(Item(address=dict(city='Brisbane'))).address['country'], 'Australia')

    def test_defaulter_string_field(self):
        process = Defaulter(Item.foo, 'bar')
        self.assertEquals(process(Item(foo=None)).foo, 'bar')

    def test_defaulter_number_field(self):
        process = Defaulter(Item.one, 1)
        self.assertEquals(process(Item(one=None)).one, 1)
        self.assertEquals(process(Item(one=0)).one, 0)

    def test_defaulter_multiple_fields(self):
        process = Defaulter([Item.foo, Item.bar], True)
        processed = process(Item(baz=False))
        self.assertTrue(processed.foo)
        self.assertTrue(processed.bar)
        self.assertFalse(processed.baz)

    def test_assigner(self):
        process = Assigner(Item.hello, 'world')
        process_func = Assigner(Item.hello, lambda: 'world')

        self.assertEqual(process(Item()).hello, 'world')
        self.assertEqual(process(Item(hello=True)).hello, 'world')
        self.assertEqual(process_func(Item()).hello, 'world')

    def test_dropper_item(self):
        process = Dropper(Item, Item.hello == 'world')

        self.assertRaises(DropItem, process, Item(hello='world'))

        item = Item(hello='universe')
        self.assertEqual(process(item), item)

    def test_dropper_field(self):
        process = Dropper(Item.hello, Item.planet.is_in(['earth', 'mars']))

        item1 = Item(planet='earth', hello='earth')
        self.assertFalse('hello' in process(item1))

        item2 = Item(planet='pluto', hello='pluto')
        self.assertTrue('hello' in process(item2))

    def test_value_filter(self):
        process = Filter(Item.hello, lambda val: 'hello ' + val)
        self.assertEqual(process(Item(hello='world')).hello, 'hello world')
        self.assertTrue(process(Item()).hello is None)

    def test_value_mapper(self):
        mapping = {
            'hello': 'world',
            'world': 'hello'
        }

        process = Mapper(Item.field1, mapping)
        item = Item(field1='hello', field2='hello')
        self.assertEquals(process(item).field1, 'world')
        self.assertEquals(process(item).field2, 'hello')

        process_func = Mapper(Item.field1, lambda: mapping)
        item_func = Item(field1='hello', field2='hello')
        self.assertEquals(process_func(item_func).field1, 'world')
        self.assertEquals(process_func(item_func).field2, 'hello')

    def test_key_mapper(self):
        mapping = {
            'field1': 'field2',
            'field3': 'field4',
            'field5': 'field6'
        }

        process = Mapper(Item, mapping, target=Mapper.KEYS)
        item = Item(field1='value', field3='value', field7='value')
        processed_item = process(item)

        self.assertFalse(hasattr(processed_item, 'field1'))
        self.assertTrue(hasattr(processed_item, 'field2'))

        self.assertFalse(hasattr(processed_item, 'field3'))
        self.assertTrue(hasattr(processed_item, 'field4'))

        self.assertFalse(hasattr(processed_item, 'field5'))
        self.assertFalse(hasattr(processed_item, 'field6'))

        self.assertTrue(hasattr(processed_item, 'field7'))

        process_func = Mapper(Item, lambda: mapping, target=Mapper.KEYS)
        item_func = Item(field1='value')
        processed_item_func = process_func(item_func)

        self.assertFalse(hasattr(processed_item_func, 'field1'))
        self.assertTrue(hasattr(processed_item_func, 'field2'))

    def test_picker(self):
        process = Picker(Item, [Item.field1, Item.field2])
        item = Item(field1='value', field2='value', field3='value')
        processed_item = process(item)

        self.assertTrue(hasattr(processed_item, 'field1'))
        self.assertTrue(hasattr(processed_item, 'field2'))
        self.assertFalse(hasattr(processed_item, 'field3'))

    def test_field_omitter(self):
        process = Omitter(Item, [Item.field1, Item.field2])
        item = Item(field1='value', field2='value', field3='value')
        processed_item = process(item)

        self.assertFalse(hasattr(processed_item, 'field1'))
        self.assertFalse(hasattr(processed_item, 'field2'))
        self.assertTrue(hasattr(processed_item, 'field3'))
