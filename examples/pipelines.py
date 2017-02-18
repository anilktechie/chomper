import logging
from chomper import Importer
from chomper.feeders import ListFeeder


logging.basicConfig(level=logging.DEBUG)


def log_item(item, meta, importer):
    importer.logger.info('= %s' % item['title'])
    return item


def log(message):
    def func(item, meta, importer):
        importer.logger.info(message)
        return item
    return func


class PipelinesImporter(Importer):

    pipeline = [
        ListFeeder([dict(title='Item 1'), dict(title='Item 2'), dict(title='Item 3')]),
        log_item,
        log(' |--> Pipeline 1.1'),
        [
            [
                log('   |--> Pipeline 1.2.1'),
                log('   |--> Pipeline 1.2.2'),
                log('   |--> Pipeline 1.2.3')
            ],
            [
                log('   |--> Pipeline 1.3.1'),
                [
                    [
                        log('     |--> Pipeline 1.3.1.1.2'),
                        log('     |--> Pipeline 1.3.1.1.1')
                    ],
                    log('     |--> Pipeline 1.3.1.2')
                ]
            ],
            [
                log('   |--> Pipeline 1.4.1'),
                log('   |--> Pipeline 1.4.2'),
                log('   |--> Pipeline 1.4.3')
            ]
        ],
        log(' |--> Pipeline 1.2'),
        log(' |--> Pipeline 1.3'),
        [
            log('   |--> Pipeline 1.4.1'),
            log('   |--> Pipeline 1.4.2')
        ],
        log(' |--> Pipeline 1.5')
    ]
