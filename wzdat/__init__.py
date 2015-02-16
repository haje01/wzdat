# -*- coding: utf-8 -*-
""" Webzen data anaylysis toolkit common."""
import os

import logging
import logging.config


if 'WZDAT_CFG' in os.environ:
    from wzdat.make_config import make_config
    cfg = make_config()
    if 'WZDAT_NOLOG' not in os.environ and 'log' in cfg:
        logging.config.dictConfig(cfg['log'])
