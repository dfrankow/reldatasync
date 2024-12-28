from pydantic import BaseModel, Field, Extra
from typing import Optional, Set

from typing import TypeVar

# _REV is a vector clock of revisions from every process that changed the doc
_REV = "_rev"
# _SEQ is a sequence number local to a datastore that says when it was inserted
_SEQ = "_seq"
# _ID is a globally unique identifier
_ID = "_id"
# _DELETED is True if the doc has been deleted
_DELETED = "_deleted"

ID_TYPE = TypeVar("ID_TYPE")


class Document(BaseModel):
    id: str = Field(..., alias="_id")
    seq: int = Field(alias="_seq", default=None)
    rev: str = Field(alias="_rev", default=None)
    deleted: bool = Field(alias="_deleted", default=False)

    class Config:
        # Allow initialization by either field names or aliases
        populate_by_name = True
        # Allow additional fields to be added dynamically
        extra = Extra.allow

    def to_dict(self) -> dict:
        # Serialize the model into a dictionary, including extra fields
        return self.dict(by_alias=True)

    @staticmethod
    def _compare_vals(one, two) -> int:
        # comparisons have to happen in the right order to respect None
        if one is None and two is None:
            return 0
        if one is None and two is not None:
            return -1
        if one is not None and two is None:
            return 1
        if one < two:
            return -1
        if one > two:
            return 1
        return 0

    def compare(self, other: Optional["Document"], ignore_keys: Set[str] = None) -> int:
        """Compare this Document to another.

        Return -1 if self < other, 0 if equal, 1 if self > other or other is None.

        :param other: Document to compare with.
        :param ignore_keys: Keys to ignore during comparison.
        """
        # Retrieve field names and extra fields for self
        self_dict = self.model_dump(by_alias=True)
        other_dict = other.model_dump(by_alias=True) if other else None

        # Exclude ignored keys
        if ignore_keys:
            self_dict = {k: v for k, v in self_dict.items() if k not in ignore_keys}
            if other_dict:
                other_dict = {
                    k: v for k, v in other_dict.items() if k not in ignore_keys
                }

        # Sort keys
        self_keys = sorted(self_dict.keys())
        other_keys = sorted(other_dict.keys()) if other_dict else None

        # Compare key sets
        if other is None or len(self_keys) > len(other_keys):
            return 1
        if len(self_keys) < len(other_keys):
            return -1

        # Compare keys themselves
        for key, other_key in zip(self_keys, other_keys):
            keycmp = self._compare_vals(key, other_key)
            if keycmp != 0:
                return keycmp

        # Compare values corresponding to keys
        for key, other_key in zip(self_keys, other_keys):
            valcmp = self._compare_vals(self_dict[key], other_dict[other_key])
            if valcmp != 0:
                return valcmp

        # Everything is equal
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
