#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import datetime

from bson import ObjectId


def oid_filter_by_datetime(
    start: datetime.datetime = None, end: datetime.datetime = None
) -> dict:
    # this function is deprecated
    # this function will be removed at ver 0.5.0
    filtr = {}
    if start is not None:
        filtr["$gt"] = ObjectId.from_datetime(start)
    if end:
        filtr["$lt"] = ObjectId.from_datetime(end)
    return filtr


def oid_filter_recent(days=30, seconds=0):
    # this function is deprecated
    # this function will be removed at ver 0.5.0
    delta = datetime.timedelta(days=days, seconds=seconds)
    start = datetime.datetime.now() - delta
    return oid_filter_by_datetime(start, None)
