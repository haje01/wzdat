# -*- coding: utf-8 -*-
"""Sample dapter common"""
import os

from wzdat.value import Value
from wzdat.util import normalize_path_elms


def get_node(nfield, fileo):
    """Return node value form file object."""
    path_elms = fileo.path.split('/')
    region = path_elms[0]
    node = path_elms[1]
    name = '{}_{}'.format(region, node)
    name = normalize_path_elms(name)
    part = os.path.join(region, node)
    obj = Value._instance(None, nfield, name, part, name)

    nfield._values.add(obj)
    return obj
