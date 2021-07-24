#!/usr/bin/env python3
# coding: utf-8
from typing import Union

from joker.cast.numeric import human_filesize
from joker.textmanip.tabular import tabular_format

from pymongo import MongoClient
from pymongo.database import Database


def inspect_mongo_storage_sizes(target: Union[MongoClient, Database]):
    if isinstance(target, MongoClient):
        return {r['name']: r['sizeOnDisk'] for r in target.list_databases()}
    size_of_collections = {}
    for coll_name in target.list_collection_names():
        info = target.command('collStats', coll_name)
        size_of_collections[info['ns']] = info['storageSize']
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