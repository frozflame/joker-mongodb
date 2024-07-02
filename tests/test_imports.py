#!/usr/bin/env python3
# coding: utf-8

import importlib

import joker.meta
from volkanic.introspect import find_all_plain_modules

dotpath_prefixes = [
    "joker.mongodb.",
    "tests.",
]


class _GI(joker.meta.JokerInterface):
    package_name = "joker.mongodb"


def _check_prefix(path):
    for prefix in dotpath_prefixes:
        if path.startswith(prefix):
            return True
    return False


def test_module_imports():
    pdir = _GI.under_project_dir()
    for dotpath in find_all_plain_modules(pdir):
        if _check_prefix(dotpath):
            print("importing", dotpath)
            importlib.import_module(dotpath)


def test_api_consistency():
    import importlib

    aggregation = importlib.import_module("joker.mongodb.aggregation")
    print(
        aggregation.LookupRecipe,
        aggregation.replace_root,
        aggregation.not_in,
    )


if __name__ == "__main__":
    test_module_imports()
    test_api_consistency()
