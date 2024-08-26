#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from typing import Union

_Expression = Union[bool, int, float, str, list, dict, None]


def in_(*values: _Expression):
    return {"$in": list(values)}


def nin(*values: _Expression):
    return {"$nin": list(values)}
