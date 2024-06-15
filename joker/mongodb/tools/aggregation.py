#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import dataclasses
import random
import string
from functools import cached_property
from typing import Union


def _get_random_key() -> str:
    chars = ["_", *random.choices(string.ascii_lowercase, k=20)]
    return "".join(chars)


_Document = Union[str, dict]
_Expression = Union[bool, int, float, str, list, dict]
_ArrayExpression = Union[str, list]


# https://www.mongodb.com/docs/manual/reference/operator/aggregation/not/
# https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
def not_in(expr: _Expression, array_expr: _ArrayExpression):
    return {"$not": [{"$in": [expr, array_expr]}]}


# https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceRoot/
def replace_root(*docs: _Document):
    if len(docs) == 1:
        return {
            "$replaceRoot": {
                "newRoot": docs[0],
            }
        }
    return {
        "$replaceRoot": {
            "newRoot": {
                "$mergeObjects": list[docs],
            }
        }
    }


@dataclasses.dataclass
class Lookup:
    from_: str
    local_field: str
    foreign_field: str

    @cached_property
    def _key(self) -> str:
        return _get_random_key()

    def _get_lookup_stage(self):
        return {
            "$lookup": {
                "from": self.from_,
                "localField": self.local_field,
                "foreignField": self.foreign_field,
                "as": self._key,
            }
        }

    def build_pipeline(self, fieldmap: dict[str, str] = None):
        if fieldmap is None:
            return [
                self._get_lookup_stage(),
                {"$unwind": f"${self._key}"},
                replace_root(f"${self._key}", "$$ROOT"),
                {"$unset": [self._key]},
            ]
        new_fields = {}
        for new_key, old_key in fieldmap.items():
            new_fields[new_key] = f"${self._key}.{old_key}"
        return [
            self._get_lookup_stage(),
            {"$unwind": f"${self._key}"},
            {"$addFields": new_fields},
            {"$unset": [self._key]},
        ]
