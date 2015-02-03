# -*- coding: utf-8 -*-
"""Sample exlog adapter"""

from wzdat.value import DateValue
from wzdat import ALL_EXPORT, make_selectors
from wzdat.selector import load_info as _load_info, Value, find_files_and_save as\
    _find_files_and_save
from ws_mysol.myprj import get_node as _get_node

get_node = _get_node

__all__ = ALL_EXPORT

all_files = fields = ctx = kind = date = node = None
files = kinds = dates = nodes = slot = None


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
    y, m, d = _date[:4], _date[4:6], _date[6:8]
    y, m, d = int(y), int(m), int(d)
    return DateValue._instance(dfield, y, m, d)


def ffilter(adir, filenames):
    if 'exlog' not in adir:
        return []
    return [fn for fn in filenames if fn.endswith('.log')]


def load_info(prog_cb=None):
    """Initilize global variables."""
    global all_files, fields, ctx, date, kind, node
    global files, kinds, dates, nodes, slot

    all_files = []
    fields = {}

    ctx, date, kind, node = _load_info(globals(), 'log', None, ffilter,
                                       prog_cb)
    files, kinds, dates, nodes, slot = make_selectors(ctx, all_files)


def find_files_and_save(startdir):
    _find_files_and_save(startdir, 'log', ffilter)
