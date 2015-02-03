# -*- coding: utf-8 -*-
""" Webzen data anaylysis toolkit common."""
import os

import logging
import logging.config

from wzdat.make_config import make_config
from wzdat.selector import load_info

ALL_EXPORT = ['files', 'kinds', 'dates', 'nodes', 'load_info', 'node', 'kind',
              'date', 'slot']


cfg = make_config()

if 'WZDAT_NOLOG' not in os.environ and 'log' in cfg:
    logging.config.dictConfig(cfg['log'])


def init_export_all(mod):
    for e in ALL_EXPORT:
        mod[e] = None
    mod['load_info'] = load_info
    return ALL_EXPORT
