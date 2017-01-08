from collections import MutableMapping


class ItemMeta(MutableMapping):
    """
    Item meta objects are used to allow the sharing data between pipeline actions.
    """

    def __init__(self, *args, **kwargs):
        self.__dict__.update(*args, **kwargs)

    @classmethod
    def copy_from(cls, meta):
        return cls(**meta.__dict__)

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return '%s, ItemMeta(%s)' % (super(ItemMeta, self).__repr__(), self.__dict__)
