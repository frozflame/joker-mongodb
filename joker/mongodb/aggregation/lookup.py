#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import dataclasses
from functools import cached_property

from joker.mongodb.aggregation.ops import _get_random_key, replace_root, if_null


@dataclasses.dataclass
class LookupRecipe:
    """
    Four conceptual stages:
    - lookup {..., _rc: [..., {a: 1, b: 2}]}
    - slice {..., _rc: [{a: 1, b: 2}]}
    - ...
    - unset (remove _rc)
    """

    from_: str
    local_field: str
    foreign_field: str
    array_slice: int | None

    @cached_property
    def _key(self) -> str:
        return _get_random_key(self.from_)

    @property
    def _dollar_key(self) -> str:
        return f"${self._key}"

    def get_lookup_stage(self):
        # https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
        return {
            "$lookup": {
                "from": self.from_,
                "localField": self.local_field,
                "foreignField": self.foreign_field,
                "as": self._key,
            }
        }

    def get_slice_stage(self):
        # https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/
        if self.array_slice is None:
            return
        return {
            "$addFields": {
                self._key: {"$slice": [self._dollar_key, self.array_slice]},
            }
        }

    def get_unset_stage(self):
        return {"$unset": self._key}

    def get_unwind_stage(self):
        return {
            "$unwind": {
                "path": self._dollar_key,
                "preserveNullAndEmptyArrays": True,
            }
        }

    def get_replace_root_stage(self):
        return replace_root(self._dollar_key, "$$ROOT")


@dataclasses.dataclass
class ObjectLookupRecipe(LookupRecipe):
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
    array_slice: int | None
    field_map: dict[str, str] = None

    def get_object_flatten_stage(self):
        if self.field_map is None:
            return self.get_replace_root_stage()
        projection = {}
        for new_key, old_key in self.field_map.items():
            projection[new_key] = if_null(f"${self._key}.{old_key}", None)
        return {"$addFields": projection}

    def get_set_default_stage(self) -> dict | None:
        if self.field_map is None:
            return
        projection = {}
        for key in self.field_map:
            projection[key] = if_null(f"${key}", None)
        return {"$addFields": projection}

    def get_lsuf_stages(self):
        stages = [
            self.get_lookup_stage(),
            self.get_slice_stage(),
            self.get_unwind_stage(),
            self.get_object_flatten_stage(),
            # the order of [unset, set-default] is arbitrary
            self.get_unset_stage(),
            self.get_set_default_stage(),
        ]
        return [st for st in stages if st is not None]

    def get_map_stage(self):
        # https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/
        projection = {}
        for new_key, old_key in self.field_map.items():
            projection[new_key] = f"$$element.{old_key}"
        return {
            "$addFields": {
                self._key: {
                    "$map": {
                        "input": self._dollar_key,
                        "as": "element",
                        "in": projection,
                    }
                }
            }
        }

    def get_lsmur_stages(self):
        stages: list[dict] = [
            self.get_lookup_stage(),
            self.get_slice_stage(),
            self.get_map_stage(),
            self.get_unwind_stage(),
            self.get_replace_root_stage(),
            # the order of [unset, set-default] is arbitrary
            self.get_unset_stage(),
            self.get_set_default_stage(),
        ]
        return [st for st in stages if st is not None]


@dataclasses.dataclass
class ValueLookupRecipe(LookupRecipe):
    """
    Four conceptual stages:
    - lookup {..., _rc: [..., {a: 1, b: 2}, {a: 11, b: 22}]}
    - slice {..., _rc: [{a: 1, b: 2}, {a: 11, b: 22}]}
    - map {..., result: [1, 11], _rc: [...]}
    - unset {..., result: [1, 11]}
    """

    from_: str
    local_field: str
    foreign_field: str
    array_slice: int | None
    target_field: str
    result_field: str

    def get_map_stage(self):
        # https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/
        return {
            "$addFields": {
                self.result_field: {
                    "$map": {
                        "input": self._dollar_key,
                        "as": "element",
                        "in": f"$$element.{self.target_field}",
                    }
                }
            }
        }

    def get_lsm_stages(self):
        stages = [
            self.get_lookup_stage(),
            self.get_slice_stage(),
            self.get_map_stage(),
            self.get_unset_stage(),
        ]
        return [st for st in stages if st is not None]


def _lookup_and_merge(
        from_: str,
        local_field: str,
        foreign_field: str,
        array_slice: int | None,
        field_map: dict[str, str] | None,
):
    recipe = ObjectLookupRecipe(
        from_,
        local_field,
        foreign_field,
        array_slice,
        field_map,
    )
    return recipe.get_lsuf_stages()


def lookup_and_merge_all(
        from_: str,
        local_field: str,
        foreign_field: str,
        field_map: dict[str, str] | None = None,
):
    return _lookup_and_merge(from_, local_field, foreign_field, None, field_map)


def lookup_and_merge_first(
        from_: str,
        local_field: str,
        foreign_field: str,
        field_map: dict[str, str] | None = None,
):
    return _lookup_and_merge(from_, local_field, foreign_field, 1, field_map)


def lookup_and_merge_last(
        from_: str,
        local_field: str,
        foreign_field: str,
        field_map: dict[str, str] | None = None,
):
    return _lookup_and_merge(from_, local_field, foreign_field, -1, field_map)


def lookup_values(
        from_: str,
        local_field: str,
        foreign_field: str,
        target_field: str,
        result_field: str,
):
    recipe = ValueLookupRecipe(
        from_,
        local_field,
        foreign_field,
        None,
        target_field,
        result_field,
    )
    return recipe.get_lsm_stages()
