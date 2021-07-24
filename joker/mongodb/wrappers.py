#!/usr/bin/env python3
# coding: utf-8

from collections import defaultdict
from typing import Union

from bson import ObjectId
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database

from joker.mongodb import utils

NoneType = type(None)


class DatabaseWrapper:
    def __init__(self, db: Database):
        self.db = db

    def inspect_storage_sizes(self):
        return utils.inspect_mongo_storage_sizes(self.db)

    def print_storage_sizes(self):
        return utils.print_mongo_storage_sizes(self.db)


class CollectionWrapper:
    def __init__(self, coll: Collection, filtr=None, projection=None):
        self.coll = coll
        self.filtr = filtr or {}
        self.projection = projection

    def exist(self, filtr: Union[ObjectId, dict]):
        return self.coll.find_one(filtr, projection=[])

    def find_recent_by_count(self, count=50) -> Cursor:
        cursor = self.coll.find(self.filtr, projection=self.projection)
        return cursor.sort([('_id', -1)]).limit(count)

    def find_most_recent_one(self) -> dict:
        recs = list(self.find_recent_by_count(1))
        if recs:
            return recs[0]

    def _insert(self, records):
        if records:
            self.coll.insert_many(records, ordered=False)

    @staticmethod
    def _check_for_uniqueness(records, uk):
        vals = [r.get(uk) for r in records]
        uniq_vals = set(vals)
        if len(vals) != len(uniq_vals):
            raise ValueError('records contain duplicating keys')

    def make_fusion_record(self):
        fusion_record = {}
        contiguous_stale_count = -1
        for skip in range(1000):
            record = self.coll.find_one(sort=[('$natural', -1)], skip=skip)
            if not record:
                continue
            contiguous_stale_count += 1
            for key, val in record.items():
                if not record.get(key):
                    fusion_record[key] = val
                    contiguous_stale_count = -1
            if contiguous_stale_count > 10:
                return fusion_record
        return fusion_record

    def query_uniq_values(self, fields: list, limit=1000):
        latest = [('_id', -1)]
        records = self.coll.find(sort=latest, projection=fields, limit=limit)
        uniq = defaultdict(set)
        for key in fields:
            for rec in records:
                val = rec.get(key)
                uniq[key].add(val)
        return uniq
