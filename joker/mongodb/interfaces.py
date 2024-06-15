#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from gridfs import GridFS
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from joker.mongodb.legacy import CollectionInterface, MongoInterfaceExtended

_compat_names = [
    CollectionInterface,
    MongoInterfaceExtended,
]


class MongoInterface:
    """A interface for multiple mongodb clusters."""

    def __init__(
        self, hosts: dict, default: str = "localhost.default", aliases: dict = None
    ):
        self.default_host, self.default_db_name = default.split(".")
        self.hosts = hosts
        self.aliases = aliases or {}
        self._clients = {}

    @classmethod
    def from_config(cls, options: dict):
        params = {
            "default": options.pop("_default", None),
            "aliases": options.pop("_aliases", None),
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
            params = {"host": params}
        return self._clients.setdefault(host, MongoClient(**params))

    @property
    def db(self) -> Database:
        return self.get_db(self.default_host, self.default_db_name)

    def _check_coll_triple(self, names: tuple) -> tuple:
        n = len(names)
        if n == 1:
            return self.default_host, self.default_db_name, names[0]
        elif n == 3:
            return names
        else:
            c = self.__class__.__name__
            msg = "requires 1 or 3 arguments, got {}".format(c, n)
            raise ValueError(msg)

    def __call__(self, *names) -> Collection:
        names = self._check_coll_triple(names)
        return self.get_coll(*names)

    def get_db(self, host: str, db_name: str) -> Database:
        mongo = self.get_mongo(host)
        db_name = self.aliases.get(db_name, db_name)
        return mongo.get_database(db_name)

    def get_coll(self, host: str, db_name: str, coll_name: str) -> Collection:
        db = self.get_db(host, db_name)
        return db.get_collection(coll_name)

    def get_gridfs(self, host: str, db_name: str, coll_name: str = "fs") -> GridFS:
        assert not coll_name.endswith(".files")
        assert not coll_name.endswith(".chunks")
        db = self.get_db(host, db_name)
        return GridFS(db, collection=coll_name)
