#!/usr/bin/env python3
# coding: utf-8

import functools
import sys
import traceback

from pymongo.collection import Collection


def _show_call_stack(func):
    @functools.wraps(func)
    def ret_func(*args, **kwargs):
        print(func, file=sys.stderr)
        rv = func(*args, **kwargs)
        traceback.print_stack()
        return rv

    return ret_func



def patch_mongo_for_debug(wrapper_func=None):
    if wrapper_func is None:
        wrapper_func = _show_call_stack
    Collection.find = wrapper_func(Collection.find)
    Collection.find_one = wrapper_func(Collection.find_one)
    Collection.update = wrapper_func(Collection.update)
    Collection.update_one = wrapper_func(Collection.update_one)
    Collection.update_many = wrapper_func(Collection.update_many)