# -*- coding: utf-8 -*-
"""
    wzdat.make_config
    ~~~~~~~~~~~~~~~~~

    설정값을 읽어온다.

"""

import os

import yaml

from wzdat.const import SOLUTION_DIR


def _expand_var(dic):
    assert type(dic) == dict
    for k, v in dic.iteritems():
        typ = type(v)
        if typ != str and typ != unicode:
            continue
        dic[k] = os.path.expandvars(v)
    return dic


def make_config(prj=None):
    """Make config object for project and return it."""
    assert "WZDAT_SOL_PKG" in os.environ, "No WZDAT_SOL_PKG exists!"
    pkg_name = os.environ["WZDAT_SOL_PKG"]
    pkg_path = os.path.expandvars(os.path.join(SOLUTION_DIR, pkg_name))
    mode = "%s-" % os.environ["WZDAT_MODE"] if "WZDAT_MODE" in os.environ\
        else ''
    cfgname = "%sconfig.yaml" % mode

    cfg_path = os.path.join(pkg_path, cfgname)
    if os.path.isfile(cfg_path):
        loaded = yaml.load(open(cfg_path, 'r'))
        cfg = _expand_var(loaded)
    else:
        cfg = {}

    # check project config
    if prj is not None or "WZDAT_PRJ" in os.environ:
        prj_id = prj if prj is not None else os.environ["WZDAT_PRJ"]
        prj_path = os.path.join(pkg_path, prj_id)
        if os.path.isdir(prj_path):
            pcfg_path = os.path.join(prj_path, cfgname)
            if os.path.isfile(pcfg_path):
                loaded = yaml.load(open(pcfg_path, 'r'))
                pcfg = _expand_var(loaded)
                cfg.update(pcfg)

    return cfg
