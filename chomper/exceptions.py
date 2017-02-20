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


class ImporterMethodNotFound(Exception):
    """
    Indicates the parent importer did not have a method the was invoked from within an action
    """
    pass


class DropItem(Exception):
    """
    Exception used to drop an item from within an action processor
    """
    pass


class DropField(Exception):
    """
    Exception used to drop the field from within an action processor
    """
    pass
