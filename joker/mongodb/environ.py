#!/usr/bin/env python3
# coding: utf-8

import abc

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from joker.mongodb.wrappers import DatabaseWrapper, CollectionWrapper


class MultihostMixin(abc.ABC):
    _mongo_clients = {}

    def _get_mongo(self, host: str) -> MongoClient:
        conf = getattr(self, 'conf', {'mongo_hosts': {}})
        hosts = conf['mongo_hosts']
        hosts.setdefault('lh', {})
        try:
            return self._mongo_clients[host]
        except KeyError:
            params = hosts.get(host, {'host': host})
            mongo = MongoClient(**params)
            self._mongo_clients[host] = mongo
            return mongo

    def get_db(self, host: str, db_name: str) -> Database:
        mongo = self._get_mongo(host)
        return mongo.get_database(db_name)

    def get_dbw(self, host: str, db_name: str):
        db = self.get_db(host, db_name)
        return DatabaseWrapper(db)

    def get_coll(self, host: str, db_name: str, coll_name: str) -> Collection:
        db = self.get_db(host, db_name)
        return db.get_collection(coll_name)

    def get_collw(self, host: str, db_name, coll_name) -> CollectionWrapper:
        coll = self.get_coll(host, db_name, coll_name)
        return CollectionWrapper(coll)
