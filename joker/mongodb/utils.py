#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import os.path
from os import PathLike
from typing import Union

import bson.json_util
import volkanic.utils
from joker.cast.numeric import human_filesize
from joker.textmanip.tabular import tabular_format
from pymongo import MongoClient
from pymongo.database import Database

from joker.mongodb.logger import MongoCommandLogger

_compat = [
    MongoCommandLogger,
]

Pathlike = Union[str, PathLike]


def is_in_replset(mongo: MongoClient) -> bool:
    db = mongo.get_database("admin")
    return "replication" in db.command("getCmdLineOpts")["parsed"]


def inspect_mongo_storage_sizes(target: Union[MongoClient, Database]):
    if isinstance(target, MongoClient):
        return {r["name"]: r["sizeOnDisk"] for r in target.list_databases()}
    size_of_collections = {}
    for coll_name in target.list_collection_names():
        info = target.command("collStats", coll_name)
        size_of_collections[info["ns"]] = info["storageSize"]
    return size_of_collections


def print_mongo_storage_sizes(target: Union[MongoClient, Database]):
    s_rows = list(inspect_mongo_storage_sizes(target).items())
    s_rows.sort(key=lambda r: r[1], reverse=True)
    rows = []
    for k, v in s_rows:
        num, unit = human_filesize(v)
        rows.append([round(num), unit, k])
    for row in tabular_format(rows):
        print(*row)


def indented_json_dumps(obj, **kwargs):
    kwargs.setdefault("dumps", bson.json_util.dumps)
    return volkanic.utils.indented_json_dumps(obj, **kwargs)


def indented_json_print(obj, **kwargs):
    kwargs.setdefault("dumps", indented_json_dumps)
    return volkanic.utils.indented_json_print(obj, **kwargs)


def infer_coll_triple_from_filename(path: str):
    """
    >>> p = "somedir/local.retail.customers.6789.json"
    >>> infer_coll_triple_from_filename(p)
    ['local', 'retail', 'customers']
    """
    filename = os.path.split(path)[1]
    coll_fullname = filename.rsplit(".", 2)[0]
    return coll_fullname.split(".", 2)


def infer_params(mongo: MongoClient):
    params = dict(zip(["host", "port"], mongo.address))
    params.update(mongo._MongoClient__options._options)  # noqa
    return params
