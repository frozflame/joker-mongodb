#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from joker.mongodb.aggregation.lookup import (
    LookupRecipe,
    ObjectLookupRecipe,
    ValueLookupRecipe,
    lookup_values,
    lookup_and_merge_first,
    lookup_and_merge_last,
    lookup_and_merge_all,
)
from joker.mongodb.aggregation.ops import (
    replace_root,
    not_in,
    if_null,
)

_names = [
    LookupRecipe,
    ObjectLookupRecipe,
    ValueLookupRecipe,
    lookup_values,
    lookup_and_merge_first,
    lookup_and_merge_last,
    lookup_and_merge_all,
    replace_root,
    not_in,
    if_null,
]
