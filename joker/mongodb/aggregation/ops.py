#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import random
import string
from typing import Union

_Document = Union[str, dict]
_Expression = Union[bool, int, float, str, list, dict, None]
_ArrayExpression = Union[str, list]


def _get_random_key(label: str) -> str:
    chars = ["_", label, "_", *random.choices(string.ascii_lowercase, k=20)]
    return "".join(chars)


def replace_root(*docs: _Document):
    # https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceRoot/
    if len(docs) == 1:
        return {
            "$replaceRoot": {
                "newRoot": docs[0],
            }
        }
    return {
        "$replaceRoot": {
            "newRoot": {
                "$mergeObjects": list(docs),
            }
        }
    }


def not_in(expr: _Expression, array_expr: _ArrayExpression):
    # https://www.mongodb.com/docs/manual/reference/operator/aggregation/not/
    # https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
    return {"$not": [{"$in": [expr, array_expr]}]}


def if_null(*exprs: _Expression):
    return {"$ifNull": list(exprs)}
