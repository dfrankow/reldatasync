from typing import Union

from reldatasync import util


class VectorClock:
    """
    A vector clock is a data structure used for determining the partial ordering
    of events in a distributed system and detecting causality violations.

    See https://en.wikipedia.org/wiki/Vector_clock.

    This implements a vector where each process has its own clock.

    A typical description will have the counter start from 1 and increment by
    1 every time there is a change.

    However, this implementation allows the counter increase by whatever the
    client asks for.  Our processes can set the counter to their own clock,
    hence allow us to resolve conflicts (unordered changes) by leaning towards
    later (more recent) object versions.
    """

    def __init__(self, counts):
        """counts is a dict mapping process -> clock (numeric value)."""
        self.counts = counts.copy()

    def set_clock(self, clock, value: int):
        # assert increasing only
        assert clock not in self.counts or self.counts[clock] <= value
        self.counts[clock] = value

    def _compare(self, other) -> Union[int, None]:
        all_keys = self.counts.keys() | other.counts.keys()

        # if there are keys, and every element in the vector is <, then <
        comp = len(all_keys) > 0
        for key in all_keys:
            if not (self.counts.get(key, 0) < other.counts.get(key, 0)):
                comp = False
                break
        if comp:
            return -1

        # if there are keys, and every element in the vector is >, then >
        comp = len(all_keys) > 0
        for key in all_keys:
            if not (self.counts.get(key, 0) > other.counts.get(key, 0)):
                comp = False
                break
        if comp:
            return 1

        # if there are keys, and every element in the vector is ==, then ==
        comp = len(all_keys) > 0
        for key in all_keys:
            if not (self.counts.get(key, 0) == other.counts.get(key, 0)):
                comp = False
                break
        if comp:
            return 0

        # If it's not <, >, or ==, then we tiebreak

        # First, tiebreak by picking the highest clock value (to try to lean
        # towards more recency)
        max_clock1 = max(self.counts.values())
        max_clock2 = max(other.counts.values())
        if max_clock1 != max_clock2:
            # < is -1, > is 1
            return max_clock1 - max_clock2

        # Still tied.  Tiebreak by json dict hash.
        hash1 = util.dict_hash(self.counts)
        hash2 = util.dict_hash(other.counts)

        if hash1 < hash2:
            return -1
        elif hash1 > hash2:
            return 1

        # how unlucky
        # the counts above were not equal, but the hash is the same ?!
        raise ValueError("Unexpectedly, hashes are equal: {hash1}, {hash2}")

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

