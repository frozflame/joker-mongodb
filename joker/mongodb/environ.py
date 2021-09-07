#!/usr/bin/env python3
# coding: utf-8

from gridfs import GridFS
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from volkanic.compat import cached_property, abstract_property

from joker.mongodb import utils
from joker.mongodb.wrappers import DatabaseWrapper, CollectionWrapper


class MongoClientExtended(MongoClient):
    """An extended client-side representation of a mongodb cluster."""

    get_db = MongoClient.get_database

    def __repr__(self):
        cn = self.__class__.__name__
        return "{}({})".format(cn, self._repr_helper())

    def inspect_storage_sizes(self):
        return utils.inspect_mongo_storage_sizes(self)

    def print_storage_sizes(self):
        return utils.print_mongo_storage_sizes(self)

    def get_dbw(self, db_name: str) -> DatabaseWrapper:
        return DatabaseWrapper(self.get_database(db_name))

    def get_coll(self, db_name: str, coll_name: str) -> Collection:
        db = self.get_database(db_name)
        return db.get_collection(coll_name)

    def get_collw(self, db_name: str, coll_name: str) -> CollectionWrapper:
        coll = self.get_coll(db_name, coll_name)
        return CollectionWrapper(coll)

    def get_gridfs(self, db_name: str, coll_name: str = 'fs') \
            -> GridFS:
        # avoid names like "images.files.files"
        if coll_name.endswith('.files') or coll_name.endswith('.chunks'):
            coll_name = coll_name.rsplit('.', 1)[0]
        db = self.get_database(db_name)
        return GridFS(db, collection=coll_name)


class MongoInterface:
    """A interface for multiple mongodb clusters."""

    def __init__(self, hosts: dict, default: str = None, aliases: dict = None):
        if default is None:
            self.default_host = 'lh'
            self.default_db_name = 'test'
        elif isinstance(default, str):
            self.default_host, self.default_db_name = default.split('.')
        # TODO: remove this -- default being list / tuple is deprecated
        elif isinstance(default, (list, tuple)):
            self.default_host, self.default_db_name = default
        else:
            raise TypeError
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

    def get_mongo(self, host: str = None) -> MongoClientExtended:
        if host is None:
            host = self.default_host
        try:
            return self._clients[host]
        except KeyError:
            params = self.hosts.get(host, {'host': host})
            mongo = MongoClientExtended(**params)
            self._clients[host] = mongo
            return mongo

    @property
    def db(self) -> Database:
        return self.get_db(self.default_host, self.default_db_name)

    @property
    def dbw(self) -> DatabaseWrapper:
        return self.get_dbw(self.default_host, self.default_db_name)

    def get_db(self, host: str, db_name: str) -> Database:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_db(db_name)

    def get_dbw(self, host: str, db_name: str) -> DatabaseWrapper:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_dbw(db_name)

    def get_coll(self, host: str, db_name: str, coll_name: str) \
            -> Collection:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_coll(db_name, coll_name)

    def get_collw(self, host: str, db_name: str, coll_name: str) \
            -> CollectionWrapper:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_collw(db_name, coll_name)

    def get_gridfs(self, host: str, db_name: str, coll_name: str = 'fs') \
            -> GridFS:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_gridfs(db_name, coll_name)


# deprecated
class GIMixinMongo:
    @abstract_property
    def conf(self) -> dict:
        return NotImplemented

    @cached_property
    def mongo(self) -> MongoClientExtended:
        params = self.conf.get('mongo', {})
        return MongoClientExtended(**params)


# deprecated
class GIMixinMongoi:
    @abstract_property
    def conf(self) -> dict:
        return NotImplemented

    @cached_property
    def db(self) -> Database:
        return self.mongoi.db

    @cached_property
    def dbw(self) -> DatabaseWrapper:
        return self.mongoi.dbw

    @cached_property
    def mongo(self) -> MongoClientExtended:
        return self.mongoi.get_mongo()

    @cached_property
    def mongoi(self) -> MongoInterface:
        return MongoInterface(
            self.conf.get('mongoi'),
            self.conf.get('mongoi-default'),
        )
