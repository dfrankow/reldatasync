import json
from json import JSONDecodeError
from typing import Union

from reldatasync import util


class VectorClock:
    """
    A vector clock is a data structure used for determining the partial
    ordering of events in a distributed system and detecting causality
    violations.

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

    def get_clock(self, clock, default=None) -> int:
        return self.clocks.get(clock, default)

    # pylint: disable-next=too-many-return-statements,too-many-branches
    def _compare(self, other) -> Union[int, None]:
        all_keys = self.clocks.keys() | other.clocks.keys()

        # if there are keys, and every element in the vector is ==, then ==
        comp = len(all_keys) > 0
        for key in all_keys:
            if not self.clocks.get(key, 0) == other.clocks.get(key, 0):
                comp = False
                break
        if comp:
            return 0

        # if there are keys, and every element in the vector is <=, and at
        # least one is <, then <
        all_le = True
        some_lt = False
        for key in all_keys:
            val = self.clocks.get(key, 0)
            other_val = other.clocks.get(key, 0)
            if not val <= other_val:
                all_le = False
                break
            if val < other_val:
                some_lt = True
        if all_le and some_lt:
            return -1

        # if there are keys, and every element in the vector is >=, and at
        # least one is >, then >
        all_ge = True
        some_gt = False
        for key in all_keys:
            val = self.clocks.get(key, 0)
            other_val = other.clocks.get(key, 0)
            if not val >= other_val:
                all_ge = False
                break
            if val > other_val:
                some_gt = True
        if all_ge and some_gt:
            return 1

        # If it's not <, >, or ==, then we tiebreak

        # First, tiebreak by picking the highest clock value (to try to lean
        # towards more recency)
        vals1 = self.clocks.values()
        max_clock1 = max(vals1) if vals1 else None
        vals2 = other.clocks.values()
        max_clock2 = max(vals2) if vals2 else None
        if max_clock1 != max_clock2:
            # < is -1, > is 1
            return max_clock1 - max_clock2

        # Still tied.  Tiebreak by json dict hash.
        hash1 = util.dict_hash(self.clocks)
        hash2 = util.dict_hash(other.clocks)

        if hash1 < hash2:
            return -1
        if hash1 > hash2:
            return 1

        return 0

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
        return json.dumps(
            self.clocks,
            # Sorting is not needed, but let's be easy on the eyes
            sort_keys=True,
            # No whitespace: https://stackoverflow.com/a/16311587
            separators=(",", ":"),
        )

    def __repr__(self) -> str:
        return f"VectorClock({self.clocks})"

    @staticmethod
    def from_string(string) -> "VectorClock":
        try:
            return VectorClock(json.loads(string))
        except JSONDecodeError as err:
            raise ValueError from err
