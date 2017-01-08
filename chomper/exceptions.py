class NotConfigured(Exception):
    """
    Indicates a missing configuration situation
    """
    pass


class ItemNotImportable(Exception):
    """
    Indicates an item could not be imported due to a problem with the data
    """
    pass
