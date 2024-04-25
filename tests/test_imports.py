#!/usr/bin/env python3
# coding: utf-8

import importlib

from volkanic.introspect import find_all_plain_modules

import joker.meta

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


if __name__ == "__main__":
    test_module_imports()
