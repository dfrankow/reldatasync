import unittest
from datetime import datetime, timezone, timedelta

from reldatasync.document import Document
from reldatasync.json import JsonEncoder, JsonDecoder
from reldatasync.schema import Schema
from reldatasync.util import uuid4_string


class TestJson(unittest.TestCase):
    def setUp(self) -> None:
        self.dt_naive = datetime.strptime('2021-01-03', '%Y-%m-%d')
        self.dt_now = datetime.now()
        # Does work:
        # self.dt_aware = datetime.strptime(
        #     '2021-02-04 13:10:03', '%Y-%m-%d %H:%M:%S').astimezone(
        #     timezone(-timedelta(hours=6), name="CST")
        # )
        # Doesn't work:
        # self.dt_aware = datetime.strptime(
        #     '2021-02-04 13:10:03 CST', '%Y-%m-%d %H:%M:%S %Z')
        self.dt_aware = datetime.strptime(
            '2021-02-04 13:10:03 -0600', '%Y-%m-%d %H:%M:%S %z')
        self.assertTrue(self.dt_aware.tzinfo)
        self.assertEqual(timezone(timedelta(days=-1, seconds=64800)),
                         self.dt_aware.tzinfo)
        self.assertEqual('-0600', self.dt_aware.strftime('%z'))
        self.dt_now_aware = datetime.now().astimezone()

        self.id_str = uuid4_string()
        self.doc = Document({'_id': self.id_str,
                             'int': 3,
                             'real': 3.0,
                             'text': 'text',
                             'boolean': False,
                             'date': self.dt_naive.date(),
                             'dt_naive': self.dt_naive,
                             'dt_aware': self.dt_aware,
                             'dt_now': self.dt_now,
                             'dt_now_aware': self.dt_now_aware})

    def test_encode(self):
        doc_str = JsonEncoder().encode(self.doc)
        self.assertEqual(
            f'{{"_id": "{self.id_str}",'
            f' "int": 3,'
            f' "real": 3.0,'
            ' "text": "text",'
            ' "boolean": false,'
            ' "date": "2021-01-03",'
            ' "dt_naive": "2021-01-03T00:00:00",'
            ' "dt_aware": "2021-02-04T13:10:03-06:00",'
            f' "dt_now": "{self.dt_now.isoformat()}",'
            # isoformat includes time offset
            f' "dt_now_aware": "{self.dt_now_aware.isoformat()}"}}',
            doc_str)

    def test_decode(self):
        doc_str = JsonEncoder().encode(self.doc)
        doc = JsonDecoder().decode(doc_str)

        # Without a schema, the date and time values are strings
        # and that causes an error when comparing
        with self.assertRaises(TypeError):
            self.assertNotEqual(self.doc, doc)
        schema = Schema({'_id': 'TEXT',
                         'int': 'INTEGER',
                         'boolean': 'BOOLEAN',
                         'real': 'REAL',
                         'text': 'TEXT',
                         'date': 'DATE',
                         'dt_naive': 'DATETIME',
                         'dt_aware': 'DATETIME',
                         'dt_now': 'DATETIME'})

        # without enough schema, we get an error
        with self.assertRaises(KeyError):
            JsonDecoder(schema=schema).decode(doc_str)

        # with a botched schema, we get an error
        schema.set_field_type('dt_now_aware', 'OOPS')
        with self.assertRaises(ValueError):
            JsonDecoder(schema=schema).decode(doc_str)

        # with full schema, we parse correctly
        schema.set_field_type('dt_now_aware', 'DATETIME')
        doc = JsonDecoder(schema=schema).decode(doc_str)
        self.assertEqual(self.doc, doc)
