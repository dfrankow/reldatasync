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
        """Decode with a schema, which is a map of field to type name."""
        self.schema = schema

    def decode(self, json_str):
        doc = Document(json.loads(json_str))
        if self.schema:
            # Use the schema to decode
            for key, val in doc.items():
                key_type = self.schema[key]

                if key_type == 'DATETIME':
                    doc[key] = datetime.fromisoformat(val)
                elif key_type == 'DATE':
                    doc[key] = datetime.fromisoformat(val).date()
                elif key_type in ('INTEGER', 'REAL', 'TEXT', 'BOOLEAN'):
                    # For now, the only types we parse are DATE and DATETIME
                    pass
                else:
                    raise ValueError(
                        "Unknown schema type for {key}: '{key_type}'")
        return doc