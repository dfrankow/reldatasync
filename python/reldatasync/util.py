import hashlib
import json
import logging
import uuid


def dict_hash(a_dict):
    """Return a hash for a dict.

    See https://stackoverflow.com/a/22003440."""
    dict_str = json.dumps(a_dict, sort_keys=True, default=str)
    return hashlib.md5(dict_str.encode('utf8')).hexdigest()


def uuid4_string():
    """Return uuid4 string without the dashes."""
    return str(uuid.uuid4()).replace('-', '')


def basic_config(level):
    """basicConfig with a standard logging format"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
