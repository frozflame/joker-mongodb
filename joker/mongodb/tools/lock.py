#!/usr/bin/env python3
# coding: utf-8

import datetime

from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError


class NamedLock(object):
    def __init__(self, name: str, coll: Collection, ttl=12):
        self.name = name
        self.coll = coll
        self.ttl = ttl

    def __call__(self, name, ttl=12) -> 'NamedLock':
        return NamedLock(name, self.coll, ttl)

    def acquire(self):
        now = datetime.datetime.now()
        expire_at = now + datetime.timedelta(seconds=self.ttl)
        self.coll.delete_many({'expire_at': {'$lt': now}})
        record = {'_id': self.name, 'expire_at': expire_at}
        try:
            self.coll.insert_one(record)
        except DuplicateKeyError:
            return False
        return True

    def release(self):
        self.coll.delete_one({'_id': self.name})


class SlotGroup(object):
    pass
