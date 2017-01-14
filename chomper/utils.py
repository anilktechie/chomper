import inspect


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
