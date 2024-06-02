#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import logging
from typing import Iterable

from pymongo.collection import Collection
from pymongo.errors import OperationFailure

_logger = logging.getLogger(__name__)


def batch_insert(c: Collection, records: Iterable[dict]):
    with c.database.client.start_session() as session:
        with session.start_transaction():
            print("inserting", c, "....")
            # noinspection PyBroadException
            try:
                ir = c.insert_many(records, session=session)
                # commit the transaction: this is implicit if no exception is raised
                session.commit_transaction()
            except OperationFailure:
                _logger.error("transaction failed")
                session.abort_transaction()
                raise
            except Exception:
                session.abort_transaction()
                raise
            return ir


def batch_update(c: Collection, filtr: dict | list, props: dict):
    if isinstance(filtr, list):
        filtr = {"_id": {"$in": filtr}}
    with c.database.client.start_session() as session:
        with session.start_transaction():
            print("deleting", c, filtr, "....")
            # noinspection PyBroadException
            try:
                ur = c.update_many(filtr, {"$set": props})
                session.commit_transaction()
            except OperationFailure:
                _logger.error("transaction failed")
                session.abort_transaction()
                raise
            except Exception:
                session.abort_transaction()
                raise
            return ur


def batch_delete(c: Collection, filtr: dict | list):
    if isinstance(filtr, list):
        filtr = {"_id": {"$in": filtr}}
    with c.database.client.start_session() as session:
        with session.start_transaction():
            print("deleting", c, filtr, "....")
            # noinspection PyBroadException
            try:
                dr = c.delete_many(filtr)
                session.commit_transaction()
            except OperationFailure:
                _logger.error("transaction failed")
                session.abort_transaction()
                raise
            except Exception:
                session.abort_transaction()
                raise
            return dr
