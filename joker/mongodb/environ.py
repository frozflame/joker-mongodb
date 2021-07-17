#!/usr/bin/env python3
# coding: utf-8

from gridfs import GridFS
from joker.mongodb.wrappers import DatabaseWrapper, CollectionWrapper
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from volkanic.compat import cached_property


class MongoInterface:
    def __init__(self, hosts: dict = None, default: list = None):
        self.default = default or ['lh', 'test']
        assert len(default) == 2
        if hosts is None:
            hosts = {}
        hosts.setdefault('lh', {})
        self._clients = {}
        self.hosts = hosts

    def get_mongo(self, host: str = None) -> MongoClient:
        if host is None:
            host = self.default[0]
        try:
            return self._clients[host]
        except KeyError:
            params = self.hosts.get(host, {'host': host})
            mongo = MongoClient(**params)
            self._clients[host] = mongo
            return mongo

    @property
    def db(self):
        return self.get_db(*self.default)

    def get_db(self, host: str, db_name: str) -> Database:
        mongo = self.get_mongo(host)
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

    def get_gridfs(self, host: str, db_name, coll_name='fs') -> GridFS:
        # avoid names like "images.files.files"
        if coll_name.endswith('.files') or coll_name.endswith('.chunks'):
            coll_name = coll_name.rsplit('.', 1)[0]
        db = self.get_db(host, db_name)
        return GridFS(db, collection=coll_name)


class GIMixinMongo:
    @cached_property
    def db(self) -> Database:
        return self.mongoi.db

    @cached_property
    def mongo(self) -> MongoClient:
        return self.mongoi.get_mongo()

    @cached_property
    def mongoi(self) -> MongoInterface:
        conf = getattr(self, 'conf', {})
        return MongoInterface(
            conf.get('mongoi'),
            conf.get('mongoi-default'),
        )
