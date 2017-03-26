import os
import logging

from orator import DatabaseManager

from chomper import config


logging.basicConfig(level=logging.ERROR)


db_config = {
    'default': 'postgres',
    'postgres': {
        'driver': 'pgsql',
        'host': 'localhost',
        'database': os.getenv('POSTGRES_DATABASE', 'chomper_test'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'prefix': ''
    }
}

config['sql'] = dict(default='postgres')
config['postgres'] = db_config['postgres']

db = DatabaseManager(db_config)
