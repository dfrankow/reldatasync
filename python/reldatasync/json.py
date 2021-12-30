"""
Utilities to encode and decode a Document.

We use JSON, but have to augment it with date and datetime functionality.

The encoding can use python types, but the decoding depends on the datastore.
"""
import json
from datetime import datetime, date

from reldatasync.document import Document


def _json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        # NOTE: timezone naive objects have no suffix, aware ones have an offset
        # https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive  # noqa
        # date objects don't have timezone
        # https://docs.python.org/3/library/datetime.html#aware-and-naive-objects  # noqa
        return obj.isoformat()
    raise TypeError('Type %s not serializable' % type(obj))


class JsonEncoder:
    def encode(self, doc: Document):
        return json.dumps(doc, default=_json_serial)


class JsonDecoder:
    def __init__(self, schema=None):
        self.schema = schema

    def decode(self, json_str):
        return json.loads(json_str)
