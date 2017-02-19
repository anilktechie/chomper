import logging
from chomper import Importer, Item
from chomper.feeders import ListFeeder

logging.basicConfig(level=logging.DEBUG)

data = [
    {
        'name': 'Jeff',
        'age': 32,
        'job': {
            'title': 'Lawyer',
            'company': {
                'name': 'Good Lawyers Inc.',
                'website': 'http://www.good-lawyers-inc.com'
            }
        },
        'friends': [
            {
                'name': 'Annie',
                'age': 24
            },
            {
                'name': 'Britta',
                'age': 28
            }
        ]
    }
]


class FieldsImporter(Importer):

    pipeline = [
        ListFeeder(data),
        Item.name.filter(lambda name: '%s Winger' % name),
        Item.age.filter(lambda age: age * 2),
        Item.job.company.name.filter(lambda name: name.strip('Good ')),
        Item.friends[0].name.filter(lambda name: '%s Edison' % name),
        Item.friends[1]['name'].filter(lambda name: '%s Perry' % name),
        Item.log()
    ]
