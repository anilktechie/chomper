import six
import types
import logging
from pprint import pformat
from collections import OrderedDict

from consecution import Node as BaseNode

from chomper.utils import iter_methods, path_get, path_set
from chomper.support.generative import GenerativeBase


TYPE_GROUPS = {
    'empty': [type(None)],
    'string': [type(''), type(u'')],
    'number': [type(1), type(.0)],
    'iterable': [type([]), type(tuple()), types.GeneratorType],
    'dict': [type({})],
    'object': [type(object())],
    'boolean': [type(True)],
    'function': [types.FunctionType, types.LambdaType, types.MethodType, types.BuiltinFunctionType]
}


def processor(accept=None, *_accept):
    """
    Decorator to mark a node method as an item processor. Item processors must declare the
    type (or types) of items they can receive. If you don't provide a explicit type or types
    the processor will receive all items that flow through the pipeline.

    :type accept: list
    :param accept: List of item types the processor should accept

    :rtype: function
    :return:
    """
    accept = (accept if accept is not None else []) + list(_accept)
    processor_types = []

    # Expand any groups in the accepted types
    for typ in accept:
        if typ in TYPE_GROUPS:
            processor_types += TYPE_GROUPS[typ]
        elif not isinstance(typ, type):
            processor_types.append(type(typ))
        else:
            processor_types.append(typ)

    def decorate(func):
        func.processor = True
        func.processor_types = processor_types
        return func

    return decorate


class ProcessorTypeRegistry(type):
    """
    Add a class attribute to the Node with a dict containing all processor
    methods keyed by item type.
    """

    def __init__(cls, name, bases, atts):
        super(ProcessorTypeRegistry, cls).__init__(name, bases, atts)
        registry = {}

        # Loop over the class and find methods that have been decorated with`processor()`.
        # Then add them to the processor registry for all the types the support.
        for name, method in iter_methods(cls):
            if getattr(method, 'processor', False):
                registry[method.__name__] = method.processor_types

        # Order the processors by the number of types they can accept.
        # This allows us to select the most specific processor for the item type.
        # Make sure catch all processors (don't specify the types they accept) are moved to the end
        def order_key(item):
            if not len(item[0]):
                return 9999
            else:
                return len(item[0])
        cls._processor_registry = OrderedDict(sorted(registry.items(), key=order_key))


class NodeProtector(type):
    """
    Protect node methods that should not be overridden. This doesn't disable
    overriding the method, just shows a warning and suggests the correct usage.
    """

    has_base = False
    protected_methods = ['process', '_process']

    def __new__(mcs, name, bases, attrs):
        if mcs.has_base:
            logger = logging.getLogger(__name__)

            for attribute in attrs:
                if attribute in mcs.protected_methods:
                    logger.warning('Overriding the %s method is not recommended. Instead '
                                   'create a new method and use the @processor decorator.' % attribute)

        mcs.has_base = True
        return type.__new__(mcs, name, bases, attrs)


class NodeMeta(ProcessorTypeRegistry, NodeProtector):
    pass


@six.add_metaclass(NodeMeta)
class Node(BaseNode, GenerativeBase):

    def __init__(self, name):
        self._get_path = None
        self._set_path = None
        super(Node, self).__init__(name)

    def __call__(self, item):
        self.process(item)

    @property
    def logger(self):
        return logging.getLogger(self.name)

    def push(self, item):
        """
        Push an item downstream.

        This may be overridden during the pipeline initialization. Defining this method here
        is to make testing nodes easier during development.
        """
        if hasattr(self, 'pipeline'):
            self._push(item)
        else:
            # This not has not been attached to a pipeline, so we can't push the result
            self.logger.info('Could not push item downstream, node not attached to pipeline')
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(pformat(item))

    def open(self):
        """
        Open is called when the parent importer is first instantiated. This can be overridden
        to give the node the opportunity to do any initial setup.

        This method differs from `.begin()` as it will only be called once per importer, whereas
        `.begin()` may be called multiple times if the pipeline is restarted.
        """

    def process(self, item):
        """
        Pass the item to a processors that support the received type.

        If multiple processors can accept the item type it will only be
        passed to the most specific processor.
        """
        item_type = type(item)
        processors = []

        # Find processors in the registry that match the item's type
        for processor_name, processor_types in self._processor_registry.items():
            if len(processor_types) == 0 or item_type in processor_types:
                processors.append(processor_name)

        if len(processors) > 1:
            self.logger.info('Multiple processors found for type "%s". '
                             'Using the most specific processor "%s".' % (item_type, processors[0]))
        elif not len(processors):
            self.logger.warning('"%s" cannot process items of type "%s".' % (self.__class__.__name__, item_type))

        # Use the first matched processor as this is the most specific
        if processors:
            getattr(self, processors[0])(item)

    def close(self):
        """
        Close can be overridden to do any final cleanup before the importer shuts down. This method
        will only be called once, whereas `.close()` may be called multiple times within the
        importers lifetime.
        """


class WrapperNode(Node):
    """
    WrapperNode can be used to convert any regular callable into a Node. Anything returned from
    the callable will be pushed to downstream nodes.
    """

    def __init__(self, name, func):
        super(Node, self).__init__(name)
        self._func = func

    def process(self, item):
        self.push(self._func(item))


class ScopedNode(Node):

    def __init__(self, name, input_path=None, output_path=None, node=None):
        """
        ScopedNode can be used to scope a node/s to only have access to a specific field on an item. The output
        field for the node can also be controlled.

        :type input_path: string
        :param input_path: Path to get the processable value

        :type output_path: string
        :param output_path: Path to set the processed value (optional, defaults to the input path)

        :type node: Node/list
        :param node: Node to receive the value (can be a list of Nodes)
        """
        super(Node, self).__init__(name)

        self._input_path = input_path
        self._output_path = output_path if output_path is not None else input_path

        try:
            self._nodes = list(node)
        except TypeError:
            self._nodes = [node]

        self._initialize_nodes()

    def process(self, item):
        value = path_get(self._input_path, item)
        result = self._nodes[0].process(value)
        path_set(self._output_path, item, result)
        self.push(item)

    def _initialize_nodes(self):
        for idx, node in enumerate(self._nodes):
            try:
                # Scoped nodes can only have a single downstream. Therefore, we can
                # short-circuit the push() logic and directly call the next node's
                # process() method.
                node.push = self._nodes[idx + 1].process
            except IndexError:
                # This is the last node. Return the resulting value so it can
                # be set on the original item.
                node.push = lambda item: item
