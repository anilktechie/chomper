import logging
from chomper import Importer
from chomper.feeds import HttpFeed
from chomper.processors import CsvLoader, ItemLogger, EmptyDropper, ValueDropper, ValueMapper, ValueFilter


logging.basicConfig(level=logging.DEBUG)


class AsxCompaniesImporter(Importer):

    feed = HttpFeed('http://www.asx.com.au/asx/research/ASXListedCompanies.csv', read_lines=True, skip_lines=3)

    processors = [
        EmptyDropper(),
        CsvLoader(keys=['name', 'symbol', 'industry']),
        ValueDropper('industry', values=['Not Applic', 'Class Pend']),
        ValueMapper('industry', mapping={
            'Pharmaceuticals & Biotechnology': 'Pharmaceuticals, Biotechnology & Life Sciences',
            'Commercial Services & Supplies': 'Commercial & Professional Services'
        }),
        ValueFilter('symbol', lambda v: '%s.AX' % v),
        ItemLogger()
    ]


if __name__ == "__main__":
    importer = AsxCompaniesImporter()
    importer.run()
