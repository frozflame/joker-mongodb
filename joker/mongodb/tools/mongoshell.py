#!/usr/bin/env python3
# coding: utf-8

import re

from bson.json_util import loads


def fix_mongoshell_json(s: str):
    s = re.sub(
        r':\s*ObjectId\s*\(\s*\"(\S+)\"\s*\)',
        r': {"$oid": "\1"}', s
    )
    s = re.sub(
        r':\s*ISODate\s*\(\s*(\S+)\s*\)',
        r': {"$date": \1}', s
    )
    return s


def loads_mongoshell_json(s: str):
    return loads(fix_mongoshell_json(s))


def main():
    import sys
    s = fix_mongoshell_json(sys.stdin.read())
    print(fix_mongoshell_json(s))
    print(loads_mongoshell_json(s))


if __name__ == '__main__':
    main()
