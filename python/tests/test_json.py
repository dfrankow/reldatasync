import unittest
from datetime import datetime, timezone, timedelta

from reldatasync.document import Document
from reldatasync.json import JsonEncoder
from reldatasync.util import uuid4_string


class TestUtil(unittest.TestCase):
    def test_encode(self):
        dt_naive = datetime.strptime('2021-01-03', '%Y-%m-%d')
        dt_now = datetime.now()
        # Does work:
        # dt_aware = datetime.strptime(
        #     '2021-02-04 13:10:03', '%Y-%m-%d %H:%M:%S').astimezone(
        #     timezone(-timedelta(hours=6), name="CST")
        # )
        # Doesn't work:
        # dt_aware = datetime.strptime(
        #     '2021-02-04 13:10:03 CST', '%Y-%m-%d %H:%M:%S %Z')
        dt_aware = datetime.strptime(
            '2021-02-04 13:10:03 -0600', '%Y-%m-%d %H:%M:%S %z')
        self.assertTrue(dt_aware.tzinfo)
        self.assertEqual(timezone(timedelta(days=-1, seconds=64800)),
                         dt_aware.tzinfo)
        self.assertEqual('-0600', dt_aware.strftime('%z'))
        dt_now_aware = datetime.now().astimezone()

        id_str = uuid4_string()
        doc = Document({'_id': id_str,
                        'int': 3,
                        'real': 3.0,
                        'text': 'text',
                        'date': dt_naive.date(),
                        'dt_naive': dt_naive,
                        'dt_aware': dt_aware,
                        'dt_now': dt_now,
                        'dt_now_aware': dt_now_aware})
        doc_str = JsonEncoder().encode(doc)
        self.assertEqual(
            f'{{"_id": "{id_str}",'
            f' "int": 3,'
            f' "real": 3.0,'
            ' "text": "text",'
            ' "date": "2021-01-03",'
            ' "dt_naive": "2021-01-03T00:00:00",'
            ' "dt_aware": "2021-02-04T13:10:03-06:00",'
            f' "dt_now": "{dt_now.isoformat()}",'
            # isoformat includes time offset
            f' "dt_now_aware": "{dt_now_aware.isoformat()}"}}',
            doc_str)

    def test_decode(self):
        pass
