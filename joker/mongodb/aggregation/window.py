#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations


def _rank_each_group(partition_by, sort, key: str = "rank", dense: bool = True):
    op = "$denseRank" if dense else "$rank"
    return {
        "$setWindowFields": {
            "partitionBy": partition_by,
            "sortBy": sort,
            "output": {
                key: {
                    op: {},
                    "window": {
                        "documents": ["unbounded", "current"],
                    },
                }
            },
        }
    }
