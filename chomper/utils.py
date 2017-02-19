import inspect
import re
import six


class AttrDict(dict):
    """
    Dict-like object to allow accessing keys as attributes

    Note: Causes a memory leak in Python < 2.7.4 and Python3 < 3.2.3
    http://bugs.python.org/issue1469629
    """

    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def smart_invoke(func, args):
    """
    Invoke the provided function / callable with the correct number of arguments
    """
    if not (isinstance(args, list) or isinstance(args, tuple)):
        args = [args]

    try:
        spec = inspect.getargspec(func)
    except TypeError:
        # inspect.getargspec does not support callable objects be default
        if hasattr(func, '__call__'):
            spec = inspect.getargspec(func.__call__)
        else:
            raise

    # Remove the "self" / "cls" arg from the spec
    spec_args = [arg for arg in spec.args if arg not in ['self', 'cls']]

    return func(*args[:len(spec_args)])


def path_split(path_str):
    """
    Split a path string into a list of keys

    Eg. 'users[0].address.city' => ['users', 0, 'address', 'city']
    """
    if not path_str or not isinstance(path_str, six.string_types):
        return []

    if '.' not in path_str and '[' not in path_str:
        return [path_str]

    # Spilt this path on the "." character or an int wrapped in square braces
    keys = [key for key in re.split(r'\.|(\[\d+\])', path_str) if key]
    # If the key is wrapped in square braces then we parse it as an int, otherwise leave as string
    return [int(key[1:-1]) if re.match(r'^\[\d+\]$', key) else key for key in keys]


def path_get(path, obj, default=None):
    ret = obj
    for key in path_split(path):
        try:
            ret = ret[key]
        except (KeyError, AttributeError, TypeError, IndexError):
            ret = default
            break
    return ret


def path_set(path, obj, value):
    keys = path_split(path)
    last_key = keys.pop()
    for key in keys:
        try:
            obj = obj[key]
        except (KeyError, AttributeError, TypeError, IndexError):
            break
    else:
        try:
            obj[last_key] = value
        except KeyError:
            pass


def path_del(path, obj):
    keys = path_split(path)
    last_key = keys.pop()
    for key in keys:
        try:
            obj = obj[key]
        except (KeyError, AttributeError, TypeError, IndexError):
            break
    else:
        try:
            del obj[last_key]
        except KeyError:
            pass


def path_exists(path, obj):
    for key in path_split(path):
        try:
            obj = obj[key]
        except (KeyError, AttributeError, TypeError, IndexError):
            return False
    else:
        return True
