import six

from .utils import AttrDict, path_split, path_get, path_set, path_del, path_exists


OPS = ['EQ', 'NE', 'LT', 'LTE', 'GT', 'GTE', 'IN', 'NOT_IN']

OP = AttrDict(dict((op, op) for op in OPS))

OP_FN = AttrDict(
    EQ=lambda l, r: l == r,
    NE=lambda l, r: l != r,
    LT=lambda l, r: l < r,
    LTE=lambda l, r: l <= r,
    GT=lambda l, r: l > r,
    GTE=lambda l, r: l >= r,
    IN=lambda l, r: l in r,
    NOT_IN=lambda l, r: l not in r
)

OP_SQL = AttrDict(
    EQ='=',
    NE='!=',
    LT='<',
    LTE='<=',
    GT='>',
    GTE='>=',
    IN='IN',
    NOT_IN='NOT IN'
)


class Expression(object):
    """
    Represents the values and operator in a simple binary expression

    E.g. 1 < 10
    """

    def __init__(self, left, op, right):
        super(Expression, self).__init__()
        self.left = left
        self.op = op
        self.right = right

    def __repr__(self):
        return 'Expression(%s %s %s)' % (self.left, self.op, self.right)


class Field(object):
    """
    Represents a field on an item, used to create expressions for item values
    """

    def __init__(self, name):
        self._path = name

    def __repr__(self):
        return 'Field(%s)' % self._path

    def get_path(self):
        return self._path

    def get_key(self):
        return path_split(self.get_path()).pop()

    def replace_key(self, key):
        _path = path_split(self.get_path())
        self._path = _path[:-1] + [key]

    def get_value(self, item):
        return item[self]

    def __getattr__(self, key):
        if key.startswith('__') and key.endswith('__'):
            return super(Field, self).__getattr__(key)
        else:
            self._path = '%s.%s' % (self._path, key)
            return self

    def __getitem__(self, key):
        if isinstance(key, six.string_types):
            self._path = '%s.%s' % (self._path, key)
        elif isinstance(key, int):
            self._path = '%s.[%d]' % (self._path, key)
        elif isinstance(key, slice):
            # TODO: add support for field slices
            raise ValueError('Field slices are not supported.')
        return self

    def __eq__(self, right):
        return Expression(self, OP.EQ, right)

    def __ne__(self, right):
        return Expression(self, OP.NE, right)

    def __lt__(self, right):
        return Expression(self, OP.LT, right)

    def __le__(self, right):
        return Expression(self, OP.LTE, right)

    def __gt__(self, right):
        return Expression(self, OP.GT, right)

    def __ge__(self, right):
        return Expression(self, OP.GTE, right)

    def is_in(self, right):
        return Expression(self, OP.IN, right)

    def not_in(self, right):
        return Expression(self, OP.NOT_IN, right)

    def is_not_in(self, right):
        return self.not_in(right)

    def map(self, mapping, **kwargs):
        from .processors import Mapper
        return Mapper(self, mapping, **kwargs)

    def filter(self, func):
        from .processors import Filter
        return Filter(self, func)

    def assign(self, value, **kwargs):
        from .processors import Assigner
        return Assigner(self, value, **kwargs)

    def set(self, *args, **kwargs):
        return self.assign(*args, **kwargs)

    def drop(self, expression):
        from .processors import Dropper
        return Dropper(self, expression)


class ItemMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        return super(ItemMetaclass, mcs).__new__(mcs, name, bases, attrs)

    def __getattr__(self, key):
        return Field(key)

    def __setattr__(self, key, value):
        from .processors import Assigner
        return Assigner(getattr(Item, key), value)

    @staticmethod
    def defaults(defaults, **kwargs):
        from .processors import Defaulter
        return Defaulter(Item, defaults, **kwargs)

    @staticmethod
    def drop(expression):
        from .processors import Dropper
        return Dropper(Item, expression)

    @staticmethod
    def map(mapping, **kwargs):
        from .processors import Mapper
        return Mapper(Item, mapping, **kwargs)

    @staticmethod
    def pick(fields, **kwargs):
        from .processors import Picker
        return Picker(fields, **kwargs)

    @staticmethod
    def omit(fields, **kwargs):
        from .processors import Omitter
        return Omitter(fields, **kwargs)

    @staticmethod
    def log(*args, **kwargs):
        from .processors import Logger
        return Logger(Item, *args, **kwargs)


@six.add_metaclass(ItemMetaclass)
class Item(AttrDict):

    def __repr__(self):
        return 'Item(%s)' % dict(self)

    def __getitem__(self, key):
        if isinstance(key, Field):
            return path_get(key.get_path(), self)
        else:
            return super(Item, self).__getitem__(key)

    def __getattribute__(self, key):
        if isinstance(key, Field):
            return path_get(key.get_path(), self)
        else:
            return super(Item, self).__getattribute__(key)

    def __setitem__(self, key, value):
        if isinstance(key, Field):
            path_set(key.get_path(), self, value)
        else:
            super(Item, self).__setitem__(key, value)

    def __setattr__(self, key, value):
        if isinstance(key, Field):
            path_set(key.get_path(), self, value)
        else:
            super(Item, self).__setattr__(key, value)

    def __delitem__(self, key):
        if isinstance(key, Field):
            path_del(key.get_path(), self)
        else:
            super(Item, self).__delitem__(key)

    def __delattr__(self, key):
        if isinstance(key, Field):
            path_del(key.get_path(), self)
        else:
            super(Item, self).__delattr__(key)

    def __contains__(self, key):
        if isinstance(key, Field):
            return path_exists(key.get_path(), self)
        else:
            return super(Item, self).__contains__(key)

    def eval(self, expression):
        def _val(value):
            if isinstance(value, Field):
                return self[value]
            else:
                return value

        try:
            fn = OP_FN[expression.op]
        except KeyError:
            raise ValueError('Invalid expression operator "%s"' % expression.op)

        return fn(_val(expression.left), _val(expression.right))


class Selector(object):

    def __init__(self, selection):
        if not isinstance(selection, list):
            selection = [selection]

        self.item = False
        self.fields = []

        for selector in selection:
            if isinstance(selector, Field):
                self.fields.append(selector)
            elif selector == Item:
                self.item = True
            else:
                # TODO should we handle field path strings here too?
                continue

    def __contains__(self, field):
        if field == Item or isinstance(field, Item):
            return self.item
        elif isinstance(field, Field):
            return field in self.fields
        else:
            # TODO should we handle field path strings here too?
            return False

    def iterfields(self):
        for field in self.fields:
            yield field
