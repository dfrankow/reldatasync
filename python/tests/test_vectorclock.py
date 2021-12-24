import unittest

from reldatasync.vectorclock import VectorClock


class TestVectorClock(unittest.TestCase):
    def test_compare(self):
        vca1 = VectorClock({'A': 1})
        self.assertEqual(vca1, vca1)

        vca2 = VectorClock({'A': 2})
        self.assertNotEqual(vca1, vca2)
        self.assertLess(vca1, vca2)
        self.assertLessEqual(vca1, vca2)
        self.assertGreater(vca2, vca1)
        self.assertGreaterEqual(vca2, vca1)

        # different processes with tied max clock
        # these are unordered but, we tie-break deterministically
        vcb = VectorClock({'B': 1})
        self.assertFalse(vca1 == vcb)
        self.assertFalse(vca1 < vcb)
        self.assertFalse(vca1 <= vcb)
        self.assertTrue(vca1 != vcb)
        self.assertTrue(vca1 > vcb)
        self.assertTrue(vca1 >= vcb)

        # different processes with different max clock
        # pick the higher max
        vcb2 = VectorClock({'B': 2})
        self.assertFalse(vca1 == vcb2)
        self.assertTrue(vca1 != vcb2)
        self.assertTrue(vca1 < vcb2)
        self.assertTrue(vca1 <= vcb2)
        self.assertFalse(vca1 > vcb2)
        self.assertFalse(vca1 >= vcb2)
