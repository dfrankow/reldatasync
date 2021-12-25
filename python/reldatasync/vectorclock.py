import json
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
        """clocks is a dict mapping clock -> value (numeric value)."""
        self.clocks = counts.copy()

    def set_clock(self, clock, value: int) -> None:
        # assert increasing only
        old = self.clocks.get(clock, None)
        if not (old is None or old <= value):
            raise ValueError(f"Can't go backwards from {old} to {value}")
        self.clocks[clock] = value

    def _compare(self, other) -> Union[int, None]:
        all_keys = self.clocks.keys() | other.clocks.keys()

        # if there are keys, and every element in the vector is <, then <
        comp = len(all_keys) > 0
        for key in all_keys:
            if not (self.clocks.get(key, 0) < other.clocks.get(key, 0)):
                comp = False
                break
        if comp:
            return -1

        # if there are keys, and every element in the vector is >, then >
        comp = len(all_keys) > 0
        for key in all_keys:
            if not (self.clocks.get(key, 0) > other.clocks.get(key, 0)):
                comp = False
                break
        if comp:
            return 1

        # if there are keys, and every element in the vector is ==, then ==
        comp = len(all_keys) > 0
        for key in all_keys:
            if not (self.clocks.get(key, 0) == other.clocks.get(key, 0)):
                comp = False
                break
        if comp:
            return 0

        # If it's not <, >, or ==, then we tiebreak

        # First, tiebreak by picking the highest clock value (to try to lean
        # towards more recency)
        max_clock1 = max(self.clocks.values())
        max_clock2 = max(other.clocks.values())
        if max_clock1 != max_clock2:
            # < is -1, > is 1
            return max_clock1 - max_clock2

        # Still tied.  Tiebreak by json dict hash.
        hash1 = util.dict_hash(self.clocks)
        hash2 = util.dict_hash(other.clocks)

        if hash1 < hash2:
            return -1
        elif hash1 > hash2:
            return 1

        # how unlucky
        # the clocks above were not equal, but the hash is the same ?!
        raise ValueError("Unexpectedly, hashes are equal: {hash1}, {hash2}")

    def __eq__(self, other) -> bool:
        return self._compare(other) == 0

    def __ne__(self, other) -> bool:
        return self._compare(other) != 0

    def __lt__(self, other) -> bool:
        return self._compare(other) < 0

    def __le__(self, other) -> bool:
        return self._compare(other) != 1

    def __gt__(self, other) -> bool:
        return self._compare(other) > 0

    def __ge__(self, other) -> bool:
        return self._compare(other) != -1

    def __str__(self) -> str:
        return json.dumps(self.clocks,
                          # Sorting is not needed, but let's be easy on the eyes
                          sort_keys=True,
                          # No whitespace: https://stackoverflow.com/a/16311587
                          separators=(',', ':'))

    @staticmethod
    def from_string(string) -> 'VectorClock':
        # NOTE: check this is a valid string?
        return VectorClock(json.loads(string))
