import unittest

from .conftest import db


class SqlTestCaseBase(unittest.TestCase):

    db = db

    @classmethod
    def _execute_sql_file(cls, path):
        with open(path, 'r') as f:
            cls.db.statement(f.read())
