import logging
from chomper import Importer, Item, config
from chomper.feeders import ListFeeder
from chomper.contrib.sql import Query, PostgresFeeder, PostgresAssigner


logging.basicConfig(level=logging.DEBUG)

config.set_section('postgres', dict(database='test', user='postgres', password='postgres'))


class PostgresImporter(Importer):

    pipeline = [
        ListFeeder([dict(code='AHZ.AX'), dict(code='NEA.AX'), dict(code='CBA.AX')]),
        PostgresFeeder(Query().from_('companies').where('symbol', Item.code).get()),
        PostgresAssigner(Item.sister_company, Query().from_('companies').where('symbol', 'AHZ.AX').first()),
        Item.log()
    ]
