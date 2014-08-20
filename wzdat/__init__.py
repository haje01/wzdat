# -*- coding: utf-8 -*-
""" Webzen data anaylysis toolkit common."""

from wzdat.selector import FileSelector, Selector, update, SlotMap

ALL_EXPORT = ['files', 'kinds', 'dates', 'dates', 'nodes', 'update', 'node',
              'kind', 'date', 'slot']

update = update


def make_selectors(ctx, all_files):
    """Make selectors and return them."""
    _files = FileSelector(ctx, all_files, 'file')
    kinds = Selector(_files, 'kind')
    dates = Selector(_files, 'date')
    nodes = Selector(_files, 'node')
    slot = SlotMap(ctx)
    return _files, kinds, dates, nodes, slot
