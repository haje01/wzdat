# -*- coding: utf-8 -*-
"""
    wzdat.make_config
    ~~~~~~~~~~~~~~~~~

    설정값을 읽어온다.

"""

import os
import sys
import logging

import yaml

_cfg = None
_cfgpath = None


class ChangeDir(object):
    def __init__(self, apath):
        self.cwd = os.getcwd()
        self.path = apath

    def __enter__(self):
        logging.info('change dir to {0}'.format(self.path))
        assert os.path.isdir(self.path)
        os.chdir(self.path)

    def __exit__(self, atype, value, tb):
        os.chdir(self.cwd)


def _expand_var(dic):
    assert type(dic) == dict
    # expand vars
    for k, v in dic.iteritems():
        typ = type(v)
        if typ != str and typ != unicode:
            continue
        dic[k] = os.path.expandvars(v)
    return dic


def invalidate_config():
    global _cfg, _cfgpath
    _cfg = None
    _cfgpath = None


def cfg_path():
    global _cfgpath
    return _cfgpath


def make_config(cfgpath=None, usecache=True):
    """Make config object for project and return it."""
    global _cfg, _cfgpath
    logging.debug('make_config ' + str(cfgpath))

    if _cfg is not None and usecache:
        return _cfg
    else:
        _cfg = {}

    if _cfgpath is None:
        _cfgpath = cfgpath

    if cfgpath is None:
        assert 'WZDAT_CFG' in os.environ
        cfgpath = os.environ['WZDAT_CFG']

    adir = os.path.dirname(cfgpath)
    afile = os.path.basename(cfgpath)

    with ChangeDir(adir):
        loaded = yaml.load(open(afile, 'r'))
        loaded = _expand_var(loaded)
        if 'base_cfg' in loaded:
            bcfgpath = loaded['base_cfg']
            bcfg = make_config(bcfgpath, False)
            del loaded['base_cfg']
            bcfg.update(loaded)
    _cfg.update(loaded)
    #assert "WZDAT_SOL_PKG" in os.environ, "No WZDAT_SOL_PKG exists!"
    #pkg_name = os.environ["WZDAT_SOL_PKG"]
    #sol_dir = os.environ['WZDAT_SOL_DIR']\
        #if 'WZDAT_SOL_DIR' in os.environ else SOLUTION_DIR
    #pkg_path = os.path.expandvars(os.path.join(sol_dir, pkg_name))
    #mode = "%s-" % os.environ["WZDAT_MODE"] if "WZDAT_MODE" in os.environ\
        #else ''
    #cfgname = "%sconfig.yml" % mode

    #cfg_path = os.path.join(pkg_path, cfgname)
    #if os.path.isfile(cfg_path):
        #loaded = yaml.load(open(cfg_path, 'r'))
        #cfg = _expand_var(loaded)
    #else:
        #cfg = {}

    ## check project config
    #if prj is not None or "WZDAT_PRJ" in os.environ:
        #prj_id = prj if prj is not None else os.environ["WZDAT_PRJ"]
        #prj_path = os.path.join(pkg_path, prj_id)
        #if os.path.isdir(prj_path):
            #pcfg_path = os.path.join(prj_path, cfgname)
            #if os.path.isfile(pcfg_path):
                #loaded = yaml.load(open(pcfg_path, 'r'))
                #if loaded:
                    #pcfg = _expand_var(loaded)
                    #cfg.update(pcfg)
    return _cfg


if __name__ == "__main__":
    cfg = make_config()
    values = []
    if len(sys.argv) > 1:
        keys = sys.argv[1:]
        for key in keys:
            val = cfg[key]
            values.append(str(val))
        print ' '.join(values)
