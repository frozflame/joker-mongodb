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


def _kv_load2(coll: Collection, key: str) -> _Document:
    record: Union[dict, None] = coll.find_one(
        {"_id": key},
        projection={"_id": False, "value": True},
    )
    if record is None:
        return
    return record.get("value")


def _kv_save2(coll: Collection, key: str, value: _Document):
    return coll.update_one(
        {"_id": key},
        {"$set": {"value": value}},
        upsert=True,
    )
