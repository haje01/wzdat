# -*- coding: utf-8 -*-

"""Sample dump adapter"""

from wzdat import ALL_EXPORT, make_selectors
from wzdat.value import DateValue, Value
from wzdat.selector import update as _update, find_files_and_save as \
    _find_files_and_save
from ws_mysol.myprj import get_node as _get_node

get_node = _get_node

__all__ = ALL_EXPORT

all_files = fields = ctx = kind = date = node = None
files = kinds = dates = nodes = slot = None


def get_kind(sfield, fileo):
    """Return kind value from file object."""
    filename = fileo.filename
    part = filename.split('-')[0]
    return Value._instance(None, sfield, part, part, part)


def get_date(dfield, fileo):
    """Return date value from file object."""
    filename = fileo.filename
    elms = filename.split('-')
    kind = elms[0]
    if len(elms) < 3:
        date = elms[1].split('.')[0]
    else:
        date = elms[1]
        proc = elms[2].split('.')[0]
    y, m, d = int(date[:4]), int(date[4:6]), int(date[6:8])
    return DateValue._instance(dfield, y, m, d)


def update():
    """Initilize global variables."""
    global all_files, fields, ctx, date, kind, node
    global files, kinds, dates, nodes, slot

    all_files = []
    fields = {}
    ctx, date, kind, node = _update(globals(), 'dmp')
    files, kinds, dates, nodes, slot = make_selectors(ctx, all_files)


def find_files_and_save(startdir):
    _find_files_and_save(startdir, 'dmp')


update()
