# -*- coding: utf-8 -*-
"""Sample log adapter"""
from wzdat.selector import load_info as _load_info
from wzdat.value import DateValue, Value
from ws_mysol.myprj import get_node as _get_node

get_node = _get_node


def get_kind(sfield, fileo):
    """Return kinds value from file object."""
    filename = fileo.filename
    elms = filename.split('_')
    name = elms[0]
    obj = Value._instance(None, sfield, name, name, name)
    return obj


def get_cols(path):
    return ['datetime', 'type', 'msg']


def get_line_type(line):
    """Return type of a line."""
    return line.split()[2][1:-1]


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


def file_filter(adir, filenames):
    return [fn for fn in filenames if fn.endswith('.log') and 'ExLog' not in
            fn]


def load_info(prog_cb=None):
    _load_info(globals(), 'log', prog_cb)
