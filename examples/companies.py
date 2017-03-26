import logging
from chomper import Importer, Item, config
from chomper.feeders import CsvFeeder
from chomper.contrib.postgres import PostgresUpserter, PostgresTruncator

logging.basicConfig(level=logging.DEBUG)
config['postgres'] = dict(database='test', user='postgres', password='postgres')


class AsxCompaniesImporter(Importer):

    pipeline = [
        PostgresTruncator('companies'),
        CsvFeeder('http://www.asx.com.au/asx/research/ASXListedCompanies.csv', ['name', 'symbol', 'industry'], skip=3),
        Item.drop(Item.industry.is_in(['Not Applic', 'Class Pend'])),
        Item.industry.map({
            'Pharmaceuticals & Biotechnology': 'Pharmaceuticals, Biotechnology & Life Sciences',
            'Commercial Services & Supplies': 'Commercial & Professional Services'
        }),
        Item.symbol.filter(lambda v: '%s.AX' % v),
        Item.exchange.set('ASX'),
        PostgresUpserter('companies', identifiers=['symbol'])
    ]
