# Chomper

[![Build Status](https://travis-ci.org/smilledge/chomper.svg?branch=master)](https://travis-ci.org/smilledge/chomper)

**Note: this is a very early work in progress. Don't use it.**

## Table of contents

  * [Installation](#installation)
  * [Glossary](#glossary)
  * [Importers](#importers)
  * [Items](#items)
    * [Fields](#items)
    * [Expressions](#expressions)
  * [Processors](#processors)
    * [Shortcuts](#shortcuts)
  * [Contrib Modules](#contrib-modules)
    * [Postgres](#postgres)
    * [Redis](#redis)
  * [Custom Processors](#custom-processors)

## Installation

```
pip install git+https://github.com/smilledge/chomper.git
```

## Glossary

**Importer:** Importers define the actions required for ingesting raw data into the system  
**Pipeline:** A piepline is simply a list containing actions  
**Action:** Actions are some form of callable that can create, transform or export items  
**Item:** Items are dict like objects that are passed through a pipeline  
**Field:** Fields are item attributes  
**Feeder:** Feeders are actions that fetch data and add new items to the pipeline  
**Processor:** Processors are used for transforming items and/or their fields  
**Exporter:** Exporters are responsible for storing items in some form of database  
**Reader:** Readers are used by feeders to read data from external sources (Eg. http or s3)  
**Loader:** Loaders are responsible for creating item objects from simple data structures  

## Importers

Importers are classes that define the actions required to load data from an external source, transform the data and then store it in some form of database. 

Here's an example that will take a list of dicts, titlecase their title field and then print the result.

```python
from chomper import Importer, Item
from chomper.feeders import ListFeeder

data = [
    dict(title='hello world'), 
    dict(title='hello world 2')
]

class MyImporter(Importer):

    pipeline = [
        ListFeeder(data),
        Item.title.filter(lambda val: val.title()),
        Item.log()
    ]
```

The pipeline attribute hold a list of actions to be executed for each item that flows through the importer. To run the importer we need to create a new instance and then call the "run" method.

```python
importer = MyImporter()
importer.run()
```

## Feeders

Feeders are actions that fetch data and add new items to the pipeline. Usually they will accept some form a URI that will be fetched using a reader that supports the protocol. After the data is fetched by the reader it will be parsed and loaded into items that will continue through the pipeline.

By default feeders support fetching data using the following protocols; `file://`, `http://`, `https://`, `ftp://` (soon) and `s3://` (soon).

### CSV Feeder

```python
from chomper.feeders import CsvFeeder

# These are all valid URIs
feeder = CsvFeeder('http://example.com/data.csv')
feeder = CsvFeeder('s3://user:pass@bucket/data.csv')
feeder = CsvFeeder('path/to/local/file.csv')

for item in feeder():
    # Calling the feeder instance will yield items
```

### JSON Feeder

```python
from chomper.feeders import JsonFeeder

# Again, all URI formats are supported
feeder = JsonFeeder('http://example.com/data.json')

for item in feeder():
    # Calling the feeder instance will yield items
```

### JSON Lines Feeder

```python
from chomper.feeders import JsonLinesFeeder

# Again, all URI formats are supported
feeder = JsonLinesFeeder('path/to/local/file.jsonlines')

for item in feeder():
    # Calling the feeder instance will yield items
```

## Items

Items are simple dict-like container objects for pieces of data that flow through the pipeline. 

```
item = Item(title='Hello World', author='Jeff Winger')
print item.title # "Hello World"
print item['title'] # "Hello World"
```

### Fields

Accessing a field on the `Item` class will return a `Field` reference object. This field reference can be used to get, set and delete a field on an item instance. It can also be used to create expressions. Field references should also be used instead of key strings as they are more explicit.

```python
item = Item({
    'title': 'Community',
    'cast': [
        {
            'name': 'Jeff Winger',
            'actor': 'Joel McHale'
        }
    ],
    'details': {
        'language': 'English'
    }
})

# All of the following are valid
item[Item.title]
del item[Item.title]
item[Item.details.language]
item[Item.cast[0].actor] = 'Alison Brie'
```

### Expressions

Item fields can be used to create expressions that can be passed to actions and evaluated on item instances.

```python
item = Item(country='Australia', score=15)

item.eval(Item.country == 'Australia') # True
item.eval(Item.country != 'Australia') # False
item.eval(Item.score > 10) # True
```

## Processors 

There are several processers included with Chomper to help with transforming items and their data.

`Defaulter`: Set's default values on fields if they are not set  
`Assigner`: Set a field on an item  
`Dropper`: Drop a field if the provided expression evaluates to true  
`Filter`: Filter the value of an item field  
`Mapper`: Map an item and/or fields keys and/or values  
`Picker`: Drop any item fields that are not in the provided list  
`Omitter`: Drop any item fields in the provided list  

All processers accept a selector (either a Field instance or the Item class) that defines what data should be manipulated.

For example if the `Dropper` processor recieves `Item` as the selector the entire item will be dropped if the expression evaluates to true.

```python
processor = Dropper(Item, Items.country == 'Australia')
# This will raise a `DropItem` exception
processor(Item(country='Australia'))
```

If the `Dropper` processor recieved a `Field` instance as the selector argument then ony that field would be dropped.

```python
processor = Dropper(Item.country, Items.country == 'Australia')
# This will delete the country field on the item
item = processor(Item(country='Australia'))
```

### Shortcuts

Most core processors also add shortcut methods to the `Item` class and `Field` objects. This helps to avoid long import statments and improve the readability of processors.

For example;

```python
Item.country.map({
    'AU': 'Australia',
    'US': 'United States of America'
})
```

Is the same as;

```python
from chomper.processors import Mapper

Mapper(Item.country, {
    'AU': 'Australia',
    'US': 'United States of America'
})
```

## Contrib Modules

TODO

### Postgres

TODO

### Redis

TODO

## Custom Processors

Custom process can be created in several different ways:

1. A standard function, lambda or importer method
2. A callable object
3. A custom processor class

> Note: When naming custom processor classes try to make the class name a noun. For example; "Mapper", "Dropper", "DatabaseInserter", etc. Processor functions can have any descriptive name. For example; "camelcase_item_keys" or "format_item_title".

### Basic functions

The simplest processor is a basic function the accepts an item as the first argument (and optinally an importer instance as the second argument) and returns the item with any transformations applied.

```python
def add_hello_field(item):
    item.hello = 'world'
    return item
```

Items can be dropped by raising a `DropItem` exception from within the processor.

### Callable objects

Callable objects are useful when you need to provide configuration to the processor before it is called by the importer.

```python
from chomper.exceptions import DropItem

class LowScoreDropper(object):

    def __init__(self, min_score=10):
        self.min_score = min_score
    
    def __call__(self, item):
        if item.score < self.min_score:
            raise DropItem()
        else:
            return item
```

### Processor objects

Processor objects are recommended for all non-trivial processors, or when you need a cusomise the implementation of the processor based on the field type.

Custom processor classes need to inherit from the base class `chomper.processors.Processor`. They must also use the `@item_processor()` and `@field_processor()` decorators to identify the methods that should be called when an item passes through the processor.

Methods decorated with `@item_processor()` will be called with the item as the first argument and should return the item. Methods decorated with `@field_processor()` will be called with three arguments; the field key, the field value and the full item object. They should return the processed key and value as a tuple. The `@field_processor()` decorator also accepts a list of types that can be used to limit the types of fields that the processor will be invoked on. For example; `@field_processor(dict, list)` would only be called if the item was a dict or list type, `@field_processor(str, int, float)` would only be called if the field was an string, integer or float and `@field_processor(None)` would be called if the field is empty.

As with most of the core processors a selector will be provided when the processor is instantiated. Processor methods will only be called on items or fields if they match the selector. 

```python
from chomper.processors import Processor, item_processor, field_processor

def Uppercaser(Processor):
    
    @item_processor()
    def uppercase_item_keys(self, item):
        for key, value on item.items():
            item[key.upper()] = value
            del item[key]
        return item
    
    @field_processor(str)
    def uppercase_string_field(self, key, value, item)
        return key.upper(), value
        
    @field_processor(dict)
    def uppercase_dict_keys(self, key, value, item):
        for _key, _value on value.items():
            value[_key.upper()] = _value
            del value[_key]
        return key, value
    
    @field_processor(list)
    def uppercase_list_values(self, key, value, item):
        return key, [v.upper() for v in value]
```
