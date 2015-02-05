# -*- coding: utf-8 -*-
"""Sample exlog adapter"""
import logging

from wzdat.value import DateValue
from wzdat.selector import load_info as _load_info, Value
from ws_mysol.myprj import get_node as _get_node

get_node = _get_node


def get_kind(sfield, fileo):
    """Return kinds value from file object."""
    part = 'ExLog'
    obj = Value._instance(None, sfield, part, part, part)
    return obj


def get_cols(path):
    return ['datetime', 'node', 'kind', 'level', 'msg']


def get_line_date(line):
    """Return date part of a line which conform python dateutil."""
    return line[:14]


def get_line_type(line):
    """Return type of a line."""
    return line.split()[1]


def get_line_msg(line):
    """Return message part of a line."""
    return line.split()[2]


def get_date(dfield, fileo):
    """Return date value from file object."""
    filename = fileo.filename
    _date = filename.split('-')[1].split('.')[0]
    logging.debug('get_date {}'.format(filename))
    y, m, d = _date[:4], _date[4:6], _date[6:8]
    logging.debug('y {} m {} d {}'.format(y, m, d))
    y, m, d = int(y), int(m), int(d)
    return DateValue._instance(dfield, y, m, d)


def file_filter(adir, filenames):
    if 'exlog' not in adir:
        return []
    return [fn for fn in filenames if fn.endswith('.log')]


def load_info(prog_cb=None):
    """Initilize global variables."""
    _load_info(globals(), 'exlog', prog_cb)
