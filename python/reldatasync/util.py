import hashlib
import json
import logging
import os
import uuid


def dict_hash(a_dict):
    """Return a hash for a dict.

    See https://stackoverflow.com/a/22003440."""
    dict_str = json.dumps(a_dict, sort_keys=True, default=str)
    return hashlib.md5(dict_str.encode('utf8')).hexdigest()


def uuid4_string():
    """Return uuid4 string without the dashes."""
    return str(uuid.uuid4()).replace('-', '')


def logging_basic_config(level=None):
    """basicConfig with a standard logging format.

    If level is not given, default to env var LOG_LEVEL, or WARNING."""
    if not level:
        level = os.getenv('LOG_LEVEL', 'WARNING')
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
