#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import datetime

from bson import ObjectId


def in_(vals):
    return {"$in": vals}


def exclude(keys):
    return {k: False for k in keys}


def exists(*keys):
    return {k: {"$exists": True} for k in keys}


def oid_filter_by_datetime(
    start: datetime.datetime = None, end: datetime.datetime = None
) -> dict:
    filtr = {}
    if start is not None:
        filtr["$gt"] = ObjectId.from_datetime(start)
    if end:
        filtr["$lt"] = ObjectId.from_datetime(end)
    return filtr


def oid_filter_recent(days=30, seconds=0):
    delta = datetime.timedelta(days=days, seconds=seconds)
    start = datetime.datetime.now() - delta
    return oid_filter_by_datetime(start, None)


def py_true():
    return {"$nin": [0, "", 0.0, [], {}, False, None]}


def py_false():
    return {"$in": [0, "", 0.0, [], {}, False, None]}


def js_true():
    return {"$nin": [0, "", 0.0, False, None]}


def js_false():
    return {"$in": [0, "", 0.0, False, None]}
