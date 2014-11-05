# -*- coding: utf-8 -*-
""" Webzen data anaylysis toolkit common."""
import os

import logging
import logging.config

from wzdat.make_config import make_config
from wzdat.selector import FileSelector, Selector, update as _update, SlotMap

update = _update

ALL_EXPORT = ['files', 'kinds', 'dates', 'dates', 'nodes', 'update', 'node',
              'kind', 'date', 'slot']


if 'WZDAT_CFG' in os.environ:
    cfg = make_config()
    if 'WZDAT_NOLOG' not in os.environ:
        logging.config.dictConfig(cfg['log'])


def make_selectors(ctx, all_files):
    """Make selectors and return them."""
    _files = FileSelector(ctx, all_files, 'file')
    kinds = Selector(_files, 'kind')
    dates = Selector(_files, 'date')
    nodes = Selector(_files, 'node')
    slot = SlotMap(ctx)
    return _files, kinds, dates, nodes, slot
