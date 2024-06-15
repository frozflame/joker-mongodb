#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

from joker.mongodb import candies


def test_candies():
    assert candies.py_true() == candies.py_true("a")["a"]


if __name__ == "__main__":
    test_candies()
