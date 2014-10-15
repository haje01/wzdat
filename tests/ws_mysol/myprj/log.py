# -*- coding: utf-8 -*-
"""Sample log adapter"""

from wzdat.value import DateValue
from wzdat import ALL_EXPORT, make_selectors
from wzdat.selector import update as _update, find_files_and_save as \
    _find_files_and_save

__all__ = ALL_EXPORT

all_files = fields = ctx = kind = date = node = None
files = kinds = dates = nodes = slot = None


def get_node(nfield, fileo):
    """Return node value form file object."""
    import pdb; pdb.set_trace()
    path_elms = fileo.path.split('/')
    region = path_elms[0]
    node = path_elms[1]
    name = normalize_path_elms(region)[3:]
    if name == 'TW':
        name += '_' + node
    part = os.path.join(region, node)
    obj = Value._instance(None, nfield, name, part, name)

    nfield._values.add(obj)
    return obj


def get_kind(sfield, fileo):
    """Return kinds value from file object."""
    import pdb; pdb.set_trace()
    filename = fileo.filename
    elms = filename.split('_')
    name = elms[0]
    if name[:2] == 'C9':
        name = name[2:-4]
    obj = Value._instance(None, sfield, name, name, name)

    if _is_multi_server(elms):
        subabbr = 'N' + elms[1]
        subpart = '_'.join(elms[0:2]) + '_'
        return Value._instance(obj, sfield, subabbr, subpart, subabbr)
    else:
        return obj

def get_cols(path):
    return ['datetime', 'type', 'msg']


def get_line_date(line):
    """Return date part of a line which conform python dateutil."""
    return line[:20]


def get_line_type(line):
    """Return type part of a line."""
    return line[21:24]


def get_line_msg(line):
    """Return message part of a line."""
    return line[28:].replace('\r\r\n', '')


def get_date(dfield, fileo):
    """Return date value from file object."""
    filename = fileo.filename
    elms = filename.split('_')
    if _is_multi_server(elms):
        rd = elms[2]
    else:
        rd = elms[1]
    part = rd[:-2].strip()
    _date = part.split('-')
    y, m, d = int(_date[0]), int(_date[1]), int(_date[2])
    return DateValue._instance(dfield, y, m, d)


def update():
    """Initilize global variables."""
    global all_files, fields, ctx, date, kind, node
    global files, kinds, dates, nodes, slot

    all_files = []
    fields = {}

    ctx, date, kind, node = _update(globals(), 'log')
    files, kinds, dates, nodes, slot = make_selectors(ctx, all_files)


def find_files_and_save(startdir):
    _find_files_and_save(startdir, 'log')

import os
if "WZDAT_PRJ" in os.environ:
    import pdb; pdb.set_trace()
    update()
