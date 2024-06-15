#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from typing import Union

from pymongo.collection import Collection

_Document = Union[str, int, float, bool, list, dict, None]


def kv_load(c: Collection, key: str) -> _Document:
    record: Union[dict, None] = c.find_one(
        {"_id": key},
        projection={"_id": False, "value": True},
    )
    if record is None:
        return
    return record.get("value")


def kv_save(c: Collection, key: str, value: _Document):
    return c.update_one(
        {"_id": key},
        {"$set": {"value": value}},
        upsert=True,
    )


class KVStore:
    def __init__(self, collection: Collection):
        self._collection = collection

    def load(self, key: str) -> _Document:
        return kv_load(self._collection, key)

    def save(self, key: str, value: _Document):
        return kv_save(self._collection, key, value)
