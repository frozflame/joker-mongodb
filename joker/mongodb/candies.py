#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import datetime

from bson import ObjectId


def in_(*values):
    return {"$in": list(values)}


def exclude(*keys: str):
    return dict.fromkeys(keys, False)


def _dict_from_keys(keys: tuple[str, ...], value):
    if not keys:
        return value
    return dict.fromkeys(keys, value)


_py_false_vals = [0, "", 0.0, [], {}, False, None]
_js_false_vals = [0, "", 0.0, False, None]


def py_true(*keys: str) -> dict:
    return _dict_from_keys(keys, {"$nin": _py_false_vals})


def py_false(*keys: str) -> dict:
    return _dict_from_keys(keys, {"$in": _py_false_vals})


def js_true(*keys: str) -> dict:
    return _dict_from_keys(keys, {"$nin": _js_false_vals})


def js_false(*keys: str) -> dict:
    return _dict_from_keys(keys, {"$in": _js_false_vals})


def exists(*keys: str) -> dict:
    return _dict_from_keys(keys, {"$exists": True})


def not_exists(*keys: str) -> dict:
    return _dict_from_keys(keys, {"$exists": False})


def recent(days=30, seconds=0):
    delta = datetime.timedelta(days=days, seconds=seconds)
    return datetime.datetime.now() - delta


def oid_range(gte: datetime.datetime = None, lte: datetime.datetime = None):
    filtr = {"$type": "objectId"}
    if gte is not None:
        filtr["$gte"] = ObjectId.from_datetime(gte)
    if lte:
        filtr["$lte"] = ObjectId.from_datetime(lte)
    return filtr
