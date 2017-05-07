import logging
from chomper import Node, Pipeline, Importer
from chomper.nodes import processor
from chomper.readers import Http
from chomper.loaders import Csv


logging.basicConfig()


class Logger(Node):

    @processor(['empty'])
    def process_empty(self, item):
        print 'empty'
        print item

    @processor(['dict'])
    def process_dict(self, item):
        print 'dict'
        print item

    @processor(['iterable'])
    def process_iterable(self, item):
        print 'iterable'
        print item

    @processor(['string'])
    def process_string(self, item):
        print 'string'
        print item


class TestImporter(Importer):

    name = 'test_importer'

    # TODO: allow user to define the input and output path using ">" operator
    # TODO: wrap simple callables in a node, push whatever they return

    csv_url = 'http://www.asx.com.au/asx/research/ASXListedCompanies.csv'

    pipeline = (
        Http('fetch_companies').get(csv_url),
        Csv('load_csv_data').columns(['name', 'code', 'industry']).skip(3),
        Drop('drop_no_industry').if_(Item.industry.in_(['Not Applic', 'Class Pend'])),
        If('drop_no_industry').field(Item.industry).in_(['Not Applic', 'Class Pend'])).then(Drop()),
        lambda item: None if item.industry in ['Not Applic', 'Class Pend'] else item,
        Map('fix_industry_names').scope(Item.industry).keys({
            'Pharmaceuticals & Biotechnology': 'Pharmaceuticals, Biotechnology & Life Sciences',
            'Commercial Services & Supplies': 'Commercial & Professional Services'
        }),
        (Item.code, str.upper, '{0}.AX'.format, Item.symbol),
        ('ASX', Item.exchange),
        Upsert('upsert_companies').table('companies').identifiers(['symbol']),
        Logger('log_item')
    )


importer = TestImporter()
# importer.plot()e
importer.start()
