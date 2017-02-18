from six import add_metaclass

from .processors import *
from .utils import AttrDict


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
        self.name = name

    def __repr__(self):
        return 'Field(%s)' % self.name

    def get_name(self):
        return self.name

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

    def map(self, mapping):
        return ValueMapper(self, mapping)

    def filter(self, func):
        return ValueFilter(self, func)

    def assign(self, value, **kwargs):
        return Assigner(self, value, **kwargs)

    def set(self, *args, **kwargs):
        return self.assign(*args, **kwargs)

    def drop(self, expression):
        return FieldDropper(self, expression)


class ItemMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        return super(ItemMetaclass, mcs).__new__(mcs, name, bases, attrs)

    def __getattr__(self, key):
        return Field(key)

    def __setattr__(self, key, value):
        return Assigner(getattr(self, key), value)

    @staticmethod
    def defaults(*args, **kwargs):
        return Defaulter(*args, **kwargs)

    @staticmethod
    def drop(expression):
        return ItemDropper(expression)

    @staticmethod
    def map(*args, **kwargs):
        return KeyMapper(*args, **kwargs)

    @staticmethod
    def pick(**fields):
        return FieldPicker(fields)

    @staticmethod
    def omit(**fields):
        return FieldOmitter(fields)


@add_metaclass(ItemMetaclass)
class Item(AttrDict):

    def __repr__(self):
        return 'Item(%s)' % dict(self)

    def __getitem__(self, key):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__getitem__(key)

    def __getattribute__(self, key):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__getattribute__(key)

    def __setitem__(self, key, value):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__setitem__(key, value)

    def __setattr__(self, key, value):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__setattr__(key, value)

    def __delitem__(self, key):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__delitem__(key)

    def __delattr__(self, key):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__delattr__(key)

    def __contains__(self, key):
        if isinstance(key, Field):
            key = key.name
        return super(Item, self).__contains__(key)

    def eval(self, expression):
        def _val(value):
            if isinstance(value, Field):
                try:
                    return self[value.name]
                except KeyError:
                    return None
            else:
                return value

        try:
            fn = OP_FN[expression.op]
        except KeyError:
            raise ValueError('Invalid expression operator "%s"' % expression.op)

        return fn(_val(expression.left), _val(expression.right))
