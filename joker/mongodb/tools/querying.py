#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

# this module is deprecated

from joker.mongodb.query import (
    find_with_renaming,
    find_one_with_renaming,
    find_most_recent,
    find_most_recent_one,
)

_compat_names = [
    find_with_renaming,
    find_one_with_renaming,
    find_most_recent,
    find_most_recent_one,
]
