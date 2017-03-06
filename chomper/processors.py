import six
import inspect
import logging
import json

from chomper.items import Item, Selector
from chomper.exceptions import ImporterMethodNotFound, DropField, DropItem
from chomper.utils import smart_invoke, type_name


ITEM_TYPE = 'item'
VALID_FIELD_PROCESSOR_TYPES = ('dict', 'list', 'string', 'number', 'boolean', 'none', )


def item_processor():
    def _mark_func(func):
        func.is_processor = True
        func.accept_types = (ITEM_TYPE,)
        return func
    return _mark_func


def field_processor(*types):
    def _mark_func(func):
        _types = []

        for _type in types:
            try:
                name = type_name(_type)
                assert name in VALID_FIELD_PROCESSOR_TYPES
            except (AssertionError, TypeError):
                raise ValueError('Unsupported field type for processor "%s"' % func.__name__)
            else:
                _types.append(name)

        func.is_processor = True
        func.accept_types = tuple(_types) if len(_types) else VALID_FIELD_PROCESSOR_TYPES
        return func

    return _mark_func


class ProcessorRegistry(type):

    def __init__(cls, name, bases, atts):
        cls._create_processor_map()
        super(ProcessorRegistry, cls).__init__(name, bases, atts)

    def _create_processor_map(cls):
        processor_map = dict((_type, []) for _type in list(VALID_FIELD_PROCESSOR_TYPES) + [ITEM_TYPE])

        for types, name in cls._get_processor_methods():
            for _type in types:
                processor_map[_type].append(name)

        cls.PROCESSORS = processor_map

    def _get_processor_methods(cls):
        for name, method in inspect.getmembers(cls, predicate=lambda m: inspect.ismethod(m) or inspect.isfunction(m)):
            is_processor = getattr(method, 'is_processor', False)
            types = getattr(method, 'accept_types', [])
            has_types = len(types) > 0
            if is_processor and has_types:
                yield types, name


@six.add_metaclass(ProcessorRegistry)
class Processor(object):
    """
    Base class for all pipeline processors
    """

    PROCESSORS = None

    def __init__(self, selector, name=None, importer=None):
        self.selector = Selector(selector)
        self.name = name if name else self.__class__.__name__
        self.importer = importer

    def __call__(self, item, importer=None):
        # TODO: this seems like a bad idea
        self.importer = importer
        if not self.should_process(item):
            return item
        item = self.before_process(item)
        item = self.process(item)
        return self.after_process(item)

    @property
    def logger(self):
        return logging.getLogger(type(self).__name__)

    def should_process(self, item):
        return True

    def before_process(self, item):
        return item

    def process(self, item):
        item = self._run_item_processors(item)
        item = self._run_field_processors(item)
        return item

    def after_process(self, item):
        return item

    def _run_item_processors(self, item):
        if Item in self.selector:
            for processor_name in self._get_type_processors(Item):
                processor = getattr(self, processor_name)
                item = processor(item)
        return item

    def _run_field_processors(self, item):
        for field in self.selector.iterfields():
            key = field.get_key()
            value = field.get_value(item)
            key_changed = False

            try:
                for processor_name in self._get_type_processors(type(value)):
                    processor = getattr(self, processor_name)
                    result = processor(key, value, item)
                    if isinstance(result, tuple):
                        _key = key
                        key, value = result
                        key_changed = True if _key != key else key_changed
                    else:
                        value = result
            except DropField:
                del item[field]
            else:
                if key_changed:
                    del item[field]
                    field.replace_key(key)
                item[field] = value

        return item

    def _valid_type(self, _type):
        return _type in VALID_FIELD_PROCESSOR_TYPES

    def _get_type_processors(self, _type):
        try:
            return self.PROCESSORS[type_name(_type)]
        except (TypeError, KeyError):
            return []

    def _is_callable(self, func):
        from chomper.importers import ImporterMethod
        return callable(func) or isinstance(func, ImporterMethod)

    def _resolve_arg(self, value, args=None):
        if self._is_callable(value):
            return self._invoke_func(value, args)
        return value

    def _invoke_func(self, func, args):
        from chomper.importers import ImporterMethod

        if isinstance(func, ImporterMethod):
            if not self.importer:
                raise ImporterMethodNotFound('Importer methods cannot be used on processors that are not '
                                             'associated with an importer.')
            func = self.importer.get_method(func)

        if not callable(func):
            raise ValueError('Filter must be callable')

        return smart_invoke(func, args)


class Defaulter(Processor):
    """
    Set default values for fields on an item.

    Defaults will only be set if the field has not yet been defined or is "None".
    All other falsy values will be skipped.
    """

    def __init__(self, selector, defaults, **kwargs):
        super(Defaulter, self).__init__(selector, **kwargs)
        self.defaults = defaults

    @item_processor()
    def process_item(self, item):
        return self._set_defaults(self._get_defaults(item), item)

    @field_processor(dict)
    def process_dict(self, key, value, item):
        return key, self._set_defaults(self._get_defaults(item), value)

    @field_processor(list, str, int, float, None)
    def process_other(self, key, value, item):
        return key, value if value is not None else self._get_defaults(item)

    def _get_defaults(self, item):
        return self._resolve_arg(self.defaults, [item])

    def _set_defaults(self, defaults, obj):
        if not isinstance(defaults, dict):
            raise ValueError('Cannot assign default values as the source in not a dict.')

        if obj is None:
            obj = dict()

        for key, default in six.iteritems(defaults):
            if key not in obj or obj[key] is None:
                obj[key] = default

        return obj


class Assigner(Processor):
    """
    Define a field on an item and assign it's value

    Value can be either static or a callable
    """

    def __init__(self, selector, value, **kwargs):
        super(Assigner, self).__init__(selector, **kwargs)
        self.value = value

    @field_processor()
    def assign_value(self, key, value, item):
        return key, self._resolve_arg(self.value, [item])


class Dropper(Processor):
    """
    Drop an item or field if the expression evaluates true
    """

    def __init__(self, selector, expression, **kwargs):
        super(Dropper, self).__init__(selector, **kwargs)
        self.expression = expression

    @item_processor()
    def drop_item(self, item):
        if item.eval(self.expression):
            raise DropItem()
        return item

    @field_processor()
    def drop_field(self, key, value, item):
        if item.eval(self.expression):
            raise DropField()
        return key, value


class Filter(Processor):
    """
    Filter a single field value on an item

    Action is skipped if the field is not defined on the item
    """

    def __init__(self, selector, _filter, **kwargs):
        super(Filter, self).__init__(selector, **kwargs)
        self.filter = _filter

    @field_processor()
    def filter_value(self, key, value, item):
        if value is None:
            self.logger.info('Could not filter value as the field did not exist on the item.')
            return key, value
        else:
            return key, self._invoke_func(self.filter, [value, item])


class Mapper(Processor):
    """
    Map the values of an item field

    Mapping must be a dict (or a callable that returns one)
    Mapping dict keys are the search value; mapping values are the replacement
    """

    KEYS = 'keys'
    VALUES = 'values'

    def __init__(self, selector, mapping, target=VALUES, **kwargs):
        super(Mapper, self).__init__(selector, **kwargs)
        self.mapping = mapping
        self.target = target

    @item_processor()
    def map_item(self, item):
        mapping = self._resolve_arg(self.mapping, [item])
        return self._map_object(mapping, item)

    @field_processor(dict, list)
    def map_dict_or_list(self, key, value, item):
        mapping = self._resolve_arg(self.mapping, [item])
        return key, self._map_object(mapping, value)

    @field_processor(str, int, float)
    def map_other(self, key, value, item):
        mapping = self._resolve_arg(self.mapping, [item])
        try:
            if self.target == self.KEYS:
                key = mapping[key]
            else:
                value = mapping[value]
        except KeyError:
            pass
        return key, value

    def _map_object(self, mapping, obj):
        try:
            items = six.iteritems(obj)
        except AttributeError:
            items = enumerate(obj)

        for key, value in items:
            try:
                if self.target == self.KEYS:
                    map_key = mapping[key]
                    key_val = obj[key]
                    del obj[key]
                    obj[map_key] = key_val
                else:
                    obj[key] = mapping[value]
            except KeyError:
                continue
        return obj


class Picker(Processor):
    """
    Returns the item what only contains the provided fields

    TODO: add option to flatten the selected fields?
    TODO: Add support for dict and list fields
    """

    def __init__(self, selector, fields, **kwargs):
        super(Picker, self).__init__(selector, **kwargs)
        self.fields = fields

    @item_processor()
    def pick_item_fields(self, item):
        _item = Item()
        for field in self.fields:
            if field in item:
                _item[field] = item[field]
        return _item


class Omitter(Processor):
    """
    Returns the item with the provided fields removed

    TODO: Add support for dict and list fields
    """

    def __init__(self, selector, fields, **kwargs):
        super(Omitter, self).__init__(selector, **kwargs)
        self.fields = fields

    @item_processor()
    def omit_item_fields(self, item):
        for field in self.fields:
            if field in item:
                del item[field]
        return item


class Logger(Processor):

    def __init__(self, selector, level=logging.DEBUG, **kwargs):
        super(Logger, self).__init__(selector, kwargs)
        self.level = level

    @item_processor()
    def log_item(self, item):
        self.logger.log(self.level, json.dumps(item, indent=4, sort_keys=True))
        return item
