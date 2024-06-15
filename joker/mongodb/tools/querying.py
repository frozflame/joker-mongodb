#!/usr/bin/env python3
# coding: utf-8

import logging
from typing import Iterable

from pymongo.collection import Collection
from pymongo.cursor import Cursor

_logger = logging.getLogger(__name__)


def _namemap_to_project(namemap: dict):
    project = {}
    for new_name, old_name in namemap.items():
        if not old_name.startswith("$"):
            old_name = "$" + old_name
        project[new_name] = {"$ifNull": [old_name, None]}
    return project


def _namemap_from_fieldlist(fieldlist: list):
    return {k.split(".")[-1]: k for k in fieldlist}


def find_with_renaming(coll: Collection, filtr: dict, namemap: dict, sort: dict = None):
    pipelines = [
        {"$match": filtr},
        {"$sort": sort or {"_id": -1}},
        {"$project": _namemap_to_project(namemap)},
    ]
    return coll.aggregate(pipelines)


def find_one_with_renaming(
    coll: Collection, filtr: dict, namemap: dict, sort: dict = None
):
    pipelines = [
        {"$match": filtr},
        {"$sort": sort or {"_id": -1}},
        {"$limit": 1},
        {"$project": _namemap_to_project(namemap)},
    ]
    _logger.debug("pipelines: %s", pipelines)
    docs = list(coll.aggregate(pipelines))
    if docs:
        return docs[0]


def find_most_recent(c: Collection, **kwargs) -> Cursor:
    kwargs.setdefault("limit", 10)
    kwargs.setdefault("sort", [("$natural", -1)])
    return c.find(**kwargs)


def find_most_recent_one(c: Collection, **kwargs) -> dict:
    kwargs.setdefault("sort", [("$natural", -1)])
    return c.find_one(**kwargs)


def find_unique(c: Collection, keys: list[str]) -> Iterable[dict]:
    pipeline = [{"$group": {"_id": {k: f"${k}" for k in keys}}}]
    for record in c.aggregate(pipeline):
        yield record["_id"]


def find_unique_tuples(c: Collection, keys: list[str]) -> Iterable[tuple]:
    cursor = find_unique(c, keys)
    for record in cursor:
        yield tuple(record[k] for k in keys)
