import logging
from chomper import Importer
from chomper.feeds import HttpFeed
from chomper.processors import CsvLoader, ItemLogger, EmptyDropper, ValueDropper, ValueMapper, ValueFilter, FieldRemover, FieldSetter
from chomper.exporters import PostgresInserter, PostgresUpdater, PostgresUpserter

logging.basicConfig(level=logging.DEBUG)


class AsxCompaniesImporter(Importer):

    pipeline = [
        HttpFeed('http://www.asx.com.au/asx/research/ASXListedCompanies.csv', read_lines=True, skip_lines=3),
        # EmptyDropper(),
        CsvLoader(keys=['name', 'symbol', 'industry']),
        ValueDropper('industry', values=['Not Applic', 'Class Pend']),
        ValueMapper('industry', mapping={
            'Pharmaceuticals & Biotechnology': 'Pharmaceuticals, Biotechnology & Life Sciences',
            'Commercial Services & Supplies': 'Commercial & Professional Services'
        }),
        ValueFilter('symbol', lambda v: '%s.AX' % v),
        FieldSetter('exchange', 'get_exchange', cache=True),
        # ValueFilter('industry', lambda: None),
        # PostgresInserter(table='companies', database='test', user='postgres', password='postgres'),
        # PostgresUpdater(identifiers='symbol', table='companies', database='test', user='postgres', password='postgres'),
        PostgresUpserter(identifiers='symbol', on_insert='on_insert', on_name_change='on_name_change', table='companies', database='test', user='postgres', password='postgres'),
        ItemLogger()
    ]

    def get_exchange(self):
        self.logger.info('Called "get_exchange"')
        return 'ASX'

    def on_insert(self):
        self.logger.info('A new company has been added')

    def on_name_change(self):
        self.logger.info('The name of a company has changed')
