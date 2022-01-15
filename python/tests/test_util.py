import unittest

from reldatasync import util


class TestUtil(unittest.TestCase):
    def test_dict_hash(self):
        self.assertEqual(
            '415f653054e3eafb9339cc3c14bcd072',
            util.dict_hash({'A': 1, 'B': 2}))

    def test_uuid4_string(self):
        ustr = util.uuid4_string()
        self.assertEqual(32, len(ustr))
        self.assertNotIn('-', ustr)

        # calling again produces a different id
        ustr2 = util.uuid4_string()
        self.assertNotEqual(ustr, ustr2)
