from typing import TypeVar, List

# _REV is a vector clock of revisions from every process that changed the doc
_REV = '_rev'
# _SEQ is a sequence number local to a datastore that says when it was inserted
_SEQ = '_seq'
# _ID is a globally unique identifier
_ID = '_id'
# _DELETED is True if the doc has been deleted
_DELETED = '_deleted'

ID_TYPE = TypeVar('ID_TYPE')


class Document(dict):
    def __init__(self, *arg, **kw):
        super(Document, self).__init__(*arg, **kw)
        assert _ID in self

    @staticmethod
    def _compare_vals(one, two) -> int:
        # comparisons have to happen in the right order to respect None
        if one is None and two is None:
            return 0
        elif one is None and two is not None:
            return -1
        elif one is not None and two is None:
            return 1
        elif one < two:
            return -1
        elif one > two:
            return 1
        else:
            return 0

    def compare(self, other: 'Document', ignore_keys: List[str] = None) -> int:
        """Return -1 if self < other, 0 if equal, 1 if self > other or other is None.

        Compare keys and values.

        :param other  Document to which to compare
        :param ignore_keys  Ignore these keys when comparing"""
        keys = set(self.keys())
        if ignore_keys:
            keys = keys.difference(ignore_keys)
        keys = sorted(keys)

        other_keys = set(other.keys()) if other else None
        if other_keys:
            if ignore_keys:
                other_keys = other_keys.difference(ignore_keys)
            other_keys = sorted(other_keys)

        # compare keys
        if other is None or len(keys) > len(other_keys):
            return 1
        elif len(keys) < len(other_keys):
            return -1
        else:
            # same number of keys, now compare them
            for idx in range(len(keys)):
                keycmp = Document._compare_vals(keys[idx], other_keys[idx])
                if keycmp != 0:
                    return keycmp

            # keys were all the same, now compare values
            for idx in range(len(keys)):
                valcmp = Document._compare_vals(
                    self[keys[idx]], other[other_keys[idx]])
                if valcmp != 0:
                    return valcmp

            # everything was equal
            return 0

    def __eq__(self, other):
        return self.compare(other) == 0

    def __ne__(self, other):
        return self.compare(other) != 0

    def __lt__(self, other):
        return self.compare(other) < 0

    def __le__(self, other):
        return self.compare(other) != 1

    def __gt__(self, other):
        return self.compare(other) > 0

    def __ge__(self, other):
        return self.compare(other) != -1

    def copy(self) -> 'Document':
        return Document(super().copy())
