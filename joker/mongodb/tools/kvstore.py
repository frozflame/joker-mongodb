#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from typing import Union

from pymongo.collection import Collection

_Document = Union[str, int, float, bool, list, dict, None]


def kv_load(coll: Collection, key: str) -> _Document:
    record: Union[dict, None] = coll.find_one(
        {"_id": key},
        projection={"_id": False},
    )
    if record is None:
        return
    try:
        return record["_"]
    except KeyError:
        return record


def kv_save(coll: Collection, key: str, val: _Document):
    filtr = {"_id": key}
    # explode dict if '_' and '_id' are not in it -- be less nested
    if isinstance(val, dict) and "_" not in val and "_id" not in val:
        replacement = val
    else:
        replacement = {"_": val}
    return coll.replace_one(filtr, replacement, upsert=True)


class KVStore:
    def __init__(self, collection: Collection):
        self._collection = collection

    def load(self, key: str) -> _Document:
        record: Union[dict, None] = self._collection.find_one(
            {"_id": key},
            projection={"_id": False, "value": True},
        )
        if record is None:
            return
        return record.get("value")

    def save(self, key: str, value: _Document):
        return self._collection.update_one(
            {"_id": key},
            {"$set": {"value": value}},
            upsert=True,
        )
