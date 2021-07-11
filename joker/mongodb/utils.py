#!/usr/bin/env python3
# coding: utf-8


def in_(vals):
    return {'$in': vals}


def exclude(keys):
    return {k: False for k in keys}


def exists(*keys):
    return {k: {'$exists': True} for k in keys}
