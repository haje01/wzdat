import os
from tempfile import NamedTemporaryFile
from subprocess import check_call

import argh

from wzdat.make_config import make_config

cfg = make_config()


@argh.arg('prj', help="solution package name")
def cache_files(prj):
    print "Caching files for: %s" % prj
    os.environ["WZDAT_PRJ"] = prj
    datadir = '/logdata'
    pkg = os.environ["WZDAT_SOL_PKG"]
    pcfg = make_config(prj)
    for ftype in pcfg["FILE_TYPES"]:
        cmd = ['from %s.%s.%s import find_files_and_save; '
               'find_files_and_save("%s")' % (pkg, prj, ftype, datadir)]
        cmd = ' '.join(cmd)
        exec(cmd)


@argh.arg('hour', help="hour to cache start")
def cron_cache_files(hour):
    pkg = os.environ["WZDAT_SOL_PKG"]
    prj = os.environ["WZDAT_PRJ"]
    cmd = "0 %s * * * cd /solution; WZDAT_SOL_PKG=%s python -m wzdat.jobs "\
        "cache-files %s > /tmp/cache_%s 2>&1\n" % (hour, pkg, prj, prj)
    with NamedTemporaryFile() as f:
        f.write(cmd)
        f.flush()
        check_call(["crontab", f.name])


def register_cron():
    from wzdat.util import get_notebook_dir
    from wzdat.ipynb_runner import find_cron_notebooks, register_cron_notebooks
    nb_dir = get_notebook_dir()
    paths, scheds, _, _ = find_cron_notebooks(nb_dir)
    register_cron_notebooks(paths, scheds)


if __name__ == "__main__":
    argh.dispatch_commands([cache_files, register_cron, cron_cache_files])
