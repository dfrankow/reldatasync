from typing import TypeVar

_REV = '_rev'
_ID = '_id'
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

    def _compare(self, other) -> int:
        """Return -1 if self < other, 0 if equal, 1 if self > other or other is None."""
        # compare keys
        if other is None or len(self) > len(other):
            return 1
        elif len(self) < len(other):
            return -1
        else:
            # same number of keys, now compare them
            keys1 = sorted(self.keys())
            keys2 = sorted(other.keys())
            for idx in range(len(keys1)):
                keycmp = Document._compare_vals(keys1[idx], keys2[idx])
                if keycmp != 0:
                    return keycmp

            # keys were all the same, now compare values
            for idx in range(len(self)):
                valcmp = Document._compare_vals(
                    self[keys1[idx]], other[keys2[idx]])
                if valcmp != 0:
                    return valcmp

            # everything was equal
            return 0

    def __eq__(self, other):
        return self._compare(other) == 0

    def __ne__(self, other):
        return self._compare(other) != 0

    def __lt__(self, other):
        return self._compare(other) < 0

    def __le__(self, other):
        return self._compare(other) != 1

    def __gt__(self, other):
        return self._compare(other) > 0

    def __ge__(self, other):
        return self._compare(other) != -1

    def copy(self):
        return Document(super().copy())
