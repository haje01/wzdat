# -*- coding: utf-8 -*-

"""Sample dump adapter"""

from wzdat import init_export_all
from wzdat.value import DateValue, Value
from wzdat.selector import load_info as _load_info
from ws_mysol.myprj import get_node as _get_node

__all__ = init_export_all(globals())
get_node = _get_node


def get_kind(sfield, fileo):
    """Return kind value from file object."""
    filename = fileo.filename
    part = filename.split('-')[0]
    return Value._instance(None, sfield, part, part, part)


def get_date(dfield, fileo):
    """Return date value from file object."""
    filename = fileo.filename
    elms = filename.split('-')
    if len(elms) < 3:
        date = elms[1].split('.')[0]
    else:
        date = elms[1]
    y, m, d = int(date[:4]), int(date[4:6]), int(date[6:8])
    return DateValue._instance(dfield, y, m, d)


def load_info(target_mod=None, prog_cb=None):
    """Initilize global variables."""
    _load_info(globals(), 'dmp', target_mod, None, prog_cb)
