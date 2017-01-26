from six import add_metaclass

from .utils import AttrDict


OPS = ['EQ', 'NE', 'LT', 'LTE', 'GT', 'GTE', 'IN', 'NOT_IN']

OP = AttrDict({op: op for op in OPS})

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
    IN='in',
    NOT_IN='not in'
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


class ItemMetaclass(type):

    def __getattr__(self, name):
        return Field(name)


@add_metaclass(ItemMetaclass)
class Item(AttrDict):

    def __repr__(self):
        return 'Item(%s)' % self.__dict__

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


class Meta(AttrDict):
    """
    Item meta objects are used to allow the sharing data between pipeline actions.
    """

    @classmethod
    def copy_from(cls, source):
        return cls(**source)

    def __repr__(self):
        return '%s, Meta(%s)' % (super(Meta, self).__repr__(), self.__dict__)
