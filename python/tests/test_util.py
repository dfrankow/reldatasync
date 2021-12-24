import unittest

from reldatasync import util


class TestUtil(unittest.TestCase):
    def test_dict_hash(self):
        self.assertEqual(
            "415f653054e3eafb9339cc3c14bcd072",
            util.dict_hash({'A': 1, 'B': 2}))