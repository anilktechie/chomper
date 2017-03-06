import logging
from chomper import Importer, Item, set_config
from chomper.feeders import ListFeeder
from chomper.contrib.sql import Query, SqlFeeder


logging.basicConfig(level=logging.DEBUG)

set_config('postgres', dict(database='test', user='postgres', password='postgres'))


class PostgresImporter(Importer):

    pipeline = [
        ListFeeder([dict(code='AHZ.AX'), dict(code='NEA.AX'), dict(code='CBA.AX')]),
        SqlFeeder(Query().from_('companies').where('symbol', Item.code).get()),
        Item.log()
    ]
