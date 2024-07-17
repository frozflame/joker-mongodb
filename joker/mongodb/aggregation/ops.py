#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import random
import string
from typing import Union

_Document = Union[str, dict]
_Expression = Union[bool, int, float, str, list, dict, None]
_ArrayExpression = Union[str, list, dict]


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


def _trim(op: str, input_: str, chars: str = None):
    optional = {}
    if chars is not None:
        optional["chars"] = chars
    return {op: {"input": input_, **optional}}


def trim(input_: str, chars: str = None):
    return _trim("$trim", input_, chars)


def ltrim(input_: str, chars: str = None):
    return _trim("$ltrim", input_, chars)


def rtrim(input_: str, chars: str = None):
    return _trim("$rtrim", input_, chars)


def convert(input_: str, to: str, on_error=None, on_null=None):
    optional = {}
    if on_error is not None:
        optional["onError"] = on_error
    if on_null is not None:
        optional["onNull"] = on_null
    return {"$convert": {"input": input_, "to": to, **optional}}


def xop_slice_to_end(array: _ArrayExpression, n: int):
    """Remove n leading elements from the given array."""
    varname = "9faafcf8"
    ref_varname = f"$${varname}"
    return {
        "$let": {
            "vars": {varname: array},
            "in": {
                "$slice": [
                    ref_varname,
                    n,
                    {"$size": ref_varname},
                ],
            },
        }
    }


def xop_str_join(array: _ArrayExpression, sep: str):
    # https://stackoverflow.com/a/61159696/2925169
    return {
        "$reduce": {
            "input": array,
            "initialValue": "",
            "in": {
                "$cond": [
                    {"$eq": ["$$value", ""]},
                    "$$this",
                    {"$concat": ["$$value", sep, "$$this"]},
                ]
            },
        }
    }


def xop_str_concat(*exprs: _Expression):
    return {"$concat": [if_null(expr, "") for expr in exprs]}
