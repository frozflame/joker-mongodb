#!/usr/bin/env python3
# coding: utf-8

from collections import defaultdict
from typing import Union

from bson import ObjectId
from gridfs import GridFS
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.database import Database
from joker.mongodb.tools import kvstore

from joker.mongodb import utils


class CollectionInterface:
    def __init__(self, coll: Collection, filtr=None, projection=None):
        self.coll = coll
        self.filtr = filtr or {}
        self.projection = projection

    def exist(self, filtr: Union[ObjectId, dict]):
        return self.coll.find_one(filtr, projection=[])

    def kv_load(self, key: str):
        return kvstore.kv_load(self.coll, key)

    def kv_save(self, key: str, val):
        return kvstore.kv_save(self.coll, key, val)

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


class DatabaseInterface:
    def __init__(self, db: Database):
        self.db = db

    def inspect_storage_sizes(self):
        return utils.inspect_mongo_storage_sizes(self.db)

    def print_storage_sizes(self):
        return utils.print_mongo_storage_sizes(self.db)


class MongoClientExtended(MongoClient):
    """An extended client-side representation of a mongodb cluster."""

    def __repr__(self):
        cn = self.__class__.__name__
        return "{}({})".format(cn, self._repr_helper())

    def inspect_storage_sizes(self):
        return utils.inspect_mongo_storage_sizes(self)

    def print_storage_sizes(self):
        return utils.print_mongo_storage_sizes(self)

    get_db = MongoClient.get_database

    def get_dbi(self, db_name: str) -> DatabaseInterface:
        return DatabaseInterface(self.get_database(db_name))

    def get_coll(self, db_name: str, coll_name: str) -> Collection:
        db = self.get_database(db_name)
        return db.get_collection(coll_name)

    def get_colli(self, db_name: str, coll_name: str) -> CollectionInterface:
        coll = self.get_coll(db_name, coll_name)
        return CollectionInterface(coll)

    def get_gridfs(self, db_name: str, coll_name: str = 'fs') \
            -> GridFS:
        # avoid names like "images.files.files"
        if coll_name.endswith('.files') or coll_name.endswith('.chunks'):
            coll_name = coll_name.rsplit('.', 1)[0]
        db = self.get_database(db_name)
        return GridFS(db, collection=coll_name)


class MongoInterface:
    """A interface for multiple mongodb clusters."""

    mongoclient_cls = MongoClient

    def __init__(self, hosts: dict, default: str = None, aliases: dict = None):
        if default is None:
            self.default_host = 'localhost'
            self.default_db_name = 'test1'
        else:
            self.default_host, self.default_db_name = default.split('.')
        self.hosts = hosts
        self.aliases = aliases or {}
        self._clients = {}

    @classmethod
    def from_config(cls, options: dict):
        params = {
            'default': options.pop('_default', None),
            'aliases': options.pop('_aliases', None),
        }
        return cls(options, **params)

    def get_mongo(self, host: str = None) -> MongoClient:
        if host is None:
            host = self.default_host
        try:
            return self._clients[host]
        except KeyError:
            pass
        # host pass through as MongoClient argument
        params = self.hosts.get(host, host)
        if isinstance(params, str):
            params = {'host': params}
        return self._clients.setdefault(host, self.mongoclient_cls(**params))

    @property
    def db(self) -> Database:
        return self.get_db(self.default_host, self.default_db_name)

    def get_db(self, host: str, db_name: str) -> Database:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_db(db_name)

    def get_coll(self, host: str, db_name: str, coll_name: str) \
            -> Collection:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_coll(db_name, coll_name)

    def get_gridfs(self, host: str, db_name: str, coll_name: str = 'fs') \
            -> GridFS:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_gridfs(db_name, coll_name)


class MongoInterfaceExtended(MongoInterface):
    mongoclient_cls = MongoClientExtended

    @property
    def dbi(self) -> DatabaseInterface:
        return self.get_dbi(self.default_host, self.default_db_name)

    def get_dbi(self, host: str, db_name: str) -> DatabaseInterface:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_dbx(db_name)

    def get_colli(self, host: str, db_name: str, coll_name: str) \
            -> CollectionInterface:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_collx(db_name, coll_name)
