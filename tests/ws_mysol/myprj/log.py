# -*- coding: utf-8 -*-
"""Sample log adapter"""
import os

from wzdat.value import DateValue
from wzdat import ALL_EXPORT, make_selectors
from wzdat.selector import update as _update, Value, find_files_and_save as\
    _find_files_and_save
from wzdat.util import normalize_path_elms

__all__ = ALL_EXPORT

all_files = fields = ctx = kind = date = node = None
files = kinds = dates = nodes = slot = None


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


def get_kind(sfield, fileo):
    """Return kinds value from file object."""
    filename = fileo.filename
    elms = filename.split('_')
    name = elms[0]
    obj = Value._instance(None, sfield, name, name, name)
    return obj


def get_cols(path):
    return ['datetime', 'type', 'msg']


def get_line_date(line):
    """Return date part of a line which conform python dateutil."""
    return line[:16]


def get_line_kind(line):
    """Return type kind of a line."""
    return line.split()[2][1:-1]


def get_line_msg(line):
    """Return message part of a line."""
    return line.split()[-1]


def get_date(dfield, fileo):
    """Return date value from file object."""
    filename = fileo.filename
    elms = filename.split('_')
    part = elms[1].split('.')[0]
    _date = part.split('-')
    y, m, d = int(_date[0]), int(_date[1]), int(_date[2].split()[0])
    return DateValue._instance(dfield, y, m, d)


def update():
    """Initilize global variables."""
    global all_files, fields, ctx, date, kind, node
    global files, kinds, dates, nodes, slot

    all_files = []
    fields = {}

    ctx, date, kind, node = _update(globals(), 'log', None, ffilter)
    files, kinds, dates, nodes, slot = make_selectors(ctx, all_files)


def find_files_and_save(startdir):
    _find_files_and_save(startdir, 'log')


def ffilter(adir, filenames):
    return [fn for fn in filenames if 'ExLog' not in fn]


update()
