#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from typing import Iterable

from joker.mongodb.tools.pagination import RawResultDict, PaginatedResult

# this module has been renamed to pagination.py
# this module will be removed at ver 0.5.0


def compact_paginated_result(cursor: Iterable[RawResultDict]) -> PaginatedResult:
    # this function is deprecated
    # this function will be removed at ver 0.5.0
    return PaginatedResult.from_raw(cursor)
