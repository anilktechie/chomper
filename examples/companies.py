from chomper import Importer, set_config
from chomper.feeds import HttpFeed
from chomper.loaders import *
from chomper.contrib.postgres import *

set_config('postgres', dict(database='test', user='postgres', password='postgres'))


class AsxCompaniesImporter(Importer):

    pipeline = [
        PostgresTruncator('companies'),
        HttpFeed('http://www.asx.com.au/asx/research/ASXListedCompanies.csv', read_lines=True, skip_lines=3),
        CsvLoader(keys=['name', 'symbol', 'industry']),
        Item.drop(Item.industry.is_in(['Not Applic', 'Class Pend'])),
        Item.industry.map({
            'Pharmaceuticals & Biotechnology': 'Pharmaceuticals, Biotechnology & Life Sciences',
            'Commercial Services & Supplies': 'Commercial & Professional Services'
        }),
        Item.symbol.filter(lambda v: '%s.AX' % v),
        Item.exchange.set('ASX'),
        PostgresUpserter('companies', identifiers=['symbol'])
    ]
