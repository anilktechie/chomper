import logging
from orator import DatabaseManager

from chomper import config


logging.basicConfig(level=logging.ERROR)


db_config = {
    'default': 'postgres',
    'postgres': {
        'driver': 'pgsql',
        'host': 'localhost',
        'database': 'chomper_test',
        'user': 'postgres',
        'password': 'postgres',
        'prefix': ''
    }
}

config.set_section('sql', dict(default='postgres'))
config.set_section('postgres', db_config['postgres'])

db = DatabaseManager(db_config)
