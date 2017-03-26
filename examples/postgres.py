import logging
from chomper import config

logging.basicConfig(level=logging.DEBUG)

config.set_section('sql', dict(
    default='postgres'
))

config.set_section('postgres', dict(
    driver='postgres',
    host='127.0.0.1',
    port=5432,
    database='test',
    user='postgres',
    password='postgres'
))

from chomper.contrib.sql.exporters import Upserter

def log(message):
    def func(item):
        logging.getLogger().info(message)
    return func


# .columns(['symbol', 'name'])

upserter = Upserter().table('companies').identifiers('symbol')\
    .on('insert', log('inserted a row!'))\
    .on('update', log('updated a row!'))\
    .on('change', 'name', log('name field was changed on a row!'))

upserter(dict(
    symbol='AHZ.AX',
    name='Admedus 2534534',
    fake_column='123'
))

upserter(dict(
    symbol='NEAR5.AX',
    name='Nearmap 328923489',
    fake_column='123'
))


#
# class PostgresImporter(Importer):
#
#     pipeline = [
#         ListFeeder([dict(code='AHZ.AX'), dict(code='NEA.AX'), dict(code='CBA.AX')]),
#         PostgresFeeder(Query().from_('companies').where('symbol', Item.code).get()),
#         PostgresAssigner(Item.sister_company, Query().from_('companies').where('symbol', 'AHZ.AX').first()),
#         Item.log()
#     ]
