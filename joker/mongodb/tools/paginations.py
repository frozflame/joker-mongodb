#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations

import dataclasses
from typing import TypedDict, Iterable


class _CountDict(TypedDict):
    total: int


class RawResultDict(TypedDict):
    documents: list[dict]
    counts: list[_CountDict]


@dataclasses.dataclass
class QueryParams:
    skip: int = 0
    limit: int = 10
    sort: str = "_id"
    order: int = -1
    keyword: str | None = None

    def get_pagination_pipeline(self):
        return [
            {"$sort": {self.sort: self.order}},
            {
                "$facet": {
                    "documents": [
                        {"$skip": self.skip},
                        {"$limit": self.limit},
                    ],
                    "counts": [{"$count": "total"}],  # get the total count of documents
                }
            },
        ]


@dataclasses.dataclass
class PaginatedResult:
    items: list[dict]
    total: int

    @classmethod
    def from_raw(
        cls: type[PaginatedResult], raw: Iterable[RawResultDict]
    ) -> PaginatedResult:
        # TODO: use typing.Self -- requires python >= 3.11
        # from typing import Self
        # from_raw(cls: type[Self], raw: Iterable[RawResultDict]) -> Self:
        result: RawResultDict = list(raw)[0]
        counts = result["counts"]
        total = counts[0]["total"] if counts else 0
        return PaginatedResult(items=result["documents"], total=total)

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


def compact_paginated_result(cursor: Iterable[RawResultDict]) -> PaginatedResult:
    # will be deprecated
    return PaginatedResult.from_raw(cursor)
