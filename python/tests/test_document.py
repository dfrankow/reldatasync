import unittest

from reldatasync.document import _ID, Document


class TestDocument(unittest.TestCase):
    def test_compare(self):
        doc = Document(**{_ID: "A", "value": "val1"})
        self.assertEqual(doc, doc)
        doc2 = Document(**{_ID: "A", "value": "val2"})
        self.assertGreater(doc2, doc)
        self.assertLess(doc, doc2)

    def test_compare_ignore_fields(self):
        doc = Document(**{_ID: "A", "value": "val1"})
        # ignore fields
        doc3 = doc.model_copy()
        doc3.other_key = 4
        self.assertNotEqual(doc, doc3)
        self.assertEqual(-1, doc.compare(doc3, ignore_keys={"nonexistent"}))
        self.assertEqual(-1, doc.compare(doc3, ignore_keys={"value"}))
        self.assertEqual(0, doc.compare(doc3, ignore_keys={"other_key"}))

    def test_none(self):
        doc1 = Document(**{_ID: "A", "value": "val1"})
        doc2 = Document(**{_ID: "A", "value": None})
        # equality with None
        self.assertEqual(doc2, doc2)
        # inequality with None
        self.assertGreater(doc1, doc2)
        self.assertLess(doc2, doc1)

        # inequality with None doc
        self.assertLess(None, doc1)
        self.assertLess(None, doc2)
