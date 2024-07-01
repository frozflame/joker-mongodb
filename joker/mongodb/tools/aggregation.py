#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import dataclasses
import random
import string
from functools import cached_property
from typing import Union


def _get_random_key(label: str) -> str:
    chars = ["_", label, "_", *random.choices(string.ascii_lowercase, k=20)]
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
                "$mergeObjects": list(docs),
            }
        }
    }


@dataclasses.dataclass
class LookupRecipe:
    """
    Four conceptual stages:
    - lookup {..., _rc: [..., {a: 1, b: 2}]}
    - array flatten {..., _rc: {a: 1, b: 2}}
    - object flatten {..., a: 1, b: 2, _rc: {a: 1, b: 2}}
    - unset {..., a: ..., b: ...}
    """

    from_: str
    local_field: str
    foreign_field: str
    array_idx: int = None  # commonly 0 or -1
    field_map: dict[str, str] = None

    @cached_property
    def _key(self) -> str:
        return _get_random_key(self.from_)

    @property
    def _dollar_key(self) -> str:
        return f"${self._key}"

    # https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
    def get_lookup_stage(self):
        return {
            "$lookup": {
                "from": self.from_,
                "localField": self.local_field,
                "foreignField": self.foreign_field,
                "as": self._key,
            }
        }

    # https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
    def get_array_flatten_stage(self):
        if self.array_idx is None:
            return {
                "$unwind": {
                    "path": self._dollar_key,
                    "preserveNullAndEmptyArrays": True,
                }
            }
        return {
            "$addFields": {
                self._key: {
                    "$ifNull": [
                        {"$arrayElemAt": [self._dollar_key, self.array_idx]},
                        None,
                    ]
                }
            }
        }

    def _infer_object_flatten_stage(self, field_map: dict[str, str] = None):
        if field_map is None:
            return replace_root(self._dollar_key, "$$ROOT")
        mongo_fieldmap = {}
        for new_key, old_key in field_map.items():
            mongo_fieldmap[new_key] = {
                "$ifNull": [f"${self._key}.{old_key}", None],
            }
        return {"$addFields": mongo_fieldmap}

    def get_object_flatten_stage(self):
        return self._infer_object_flatten_stage(self.field_map)

    def get_unset_stage(self):
        return {"$unset": [self._key]}

    # for compatibility; will be removed at ver 0.5.0
    def build_pipeline(self, fieldmap: dict[str, str] = None):
        return [
            self.get_lookup_stage(),
            self.get_array_flatten_stage(),
            self._infer_object_flatten_stage(fieldmap),
            self.get_unset_stage(),
        ]

    def get_stages(self):
        return [
            self.get_lookup_stage(),
            self.get_array_flatten_stage(),
            self.get_object_flatten_stage(),
            self.get_unset_stage(),
        ]


def lookup_and_flatten(
    from_: str,
    local_field: str,
    foreign_field: str,
    array_idx: int = None,
    field_map: dict[str, str] = None,
):
    recipe = LookupRecipe(from_, local_field, foreign_field, array_idx, field_map)
    return recipe.get_stages()


def lookup_unwind_unset(
    from_: str,
    local_field: str,
    foreign_field: str,
    fieldmap: dict[str, str] = None,
    array_idx: int = None,
):
    """for compatibility; will be removed at ver 0.5.0"""
    return lookup_and_flatten(from_, local_field, foreign_field, array_idx, fieldmap)
