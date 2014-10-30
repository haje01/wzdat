# -*- coding: utf-8 -*-
""" Webzen data anaylysis toolkit common."""

import logging
import logging.config

import yaml

from wzdat.selector import FileSelector, Selector, update, SlotMap
from wzdat.make_config import make_config

ALL_EXPORT = ['files', 'kinds', 'dates', 'dates', 'nodes', 'update', 'node',
              'kind', 'date', 'slot']

with open('wzdatcfg.yml') as f:
    cfg = yaml.load(f.read())
    if 'log' in cfg:
        logging.config.dictConfig(cfg['log'])


def make_selectors(ctx, all_files):
    """Make selectors and return them."""
    _files = FileSelector(ctx, all_files, 'file')
    kinds = Selector(_files, 'kind')
    dates = Selector(_files, 'date')
    nodes = Selector(_files, 'node')
    slot = SlotMap(ctx)
    return _files, kinds, dates, nodes, slot
