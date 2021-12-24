import hashlib
import json


def dict_hash(a_dict):
    """Return a hash for a dict.

    See https://stackoverflow.com/a/22003440."""
    dict_str = json.dumps(a_dict, sort_keys=True, default=str)
    return hashlib.md5(dict_str.encode('utf8')).hexdigest()
