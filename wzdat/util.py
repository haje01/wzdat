"""Utilities."""

import os
import sys
import math
import logging
from datetime import datetime
import datetime as _datetime
import fnmatch
import time
from subprocess import check_call, CalledProcessError
from tempfile import TemporaryFile
import uuid as _uuid
import codecs

import matplotlib.pyplot as plt
import numpy as np
from pandas import HDFStore

from wzdat.make_config import make_config
from wzdat.const import NAMED_TMP_PREFIX, HDF_FILE_PREFIX, HDF_FILE_EXT, \
    SOLUTION_DIR, TMP_DIR, HDF_DIR, CONV_DIR, DATA_DIR, BASE_DIR


def unique_tmp_path(prefix, ext='.txt'):
    """Return temp file path with given extension."""
    uuid = prefix + str(_uuid.uuid4())
    tmp_dir = TMP_DIR
    return os.path.join(tmp_dir, uuid) + ext, uuid


def named_tmp_path(userid, slotname, test=False):
    name = "{prefix}{user}-{slot}".format(prefix=NAMED_TMP_PREFIX, uid=userid,
                                          slot=slotname)
    tmp_dir = TMP_DIR
    return os.path.join(tmp_dir, name)


def normalize_path_elms(path):
    """Replace space & dash into underbar and return it."""
    return path.replace(' ', '__').replace('-', '_')


def sizeof_fmt(num):
    """Return human readable size."""
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def unique_list(l):
    """Return unique list form list."""
    r = []
    for i in l:
        if i not in r:
            r.append(i)
    return r


def remove_old_tmps(_dir, prefix, valid_hour):
    """Remove old tmpe files."""
    tmp_ptrn = '%s*.*' % prefix
    time_limit = _datetime.timedelta(hours=valid_hour)
    for dirpath, _, filenames in os.walk(_dir):
        for filename in fnmatch.filter(filenames, tmp_ptrn):
            curpath = os.path.join(dirpath, filename)
            created = datetime.fromtimestamp(os.path.getctime(curpath))
            if datetime.now() - created > time_limit:
                os.remove(curpath)


def remove_empty_file(_files):
    """Return only non empty files."""
    return [_file for _file in _files if os.path.getsize(_file.abspath) > 0]


class Property(object):

    """Property object. Do not drived from dict to be pickable"""

    def __init__(self):
        self.__dict__['dict'] = {}

    def __setattr__(self, attr, val):
        self.__dict__['dict'][attr] = val

    def __getattr__(self, key):
        try:
            return self.__dict__['dict'][key]
        except KeyError, e:
            raise AttributeError(e)


class Context(Property):

    """Domain context object."""

    @classmethod
    def load(path):
        pass

    def __init__(self, mod, startdir, encoding, fileext):
        super(Context, self).__init__()
        self.mod = mod
        self.files = mod['all_files']
        self.fields = mod['fields']
        self.startdir = startdir
        self.encoding = codecs.lookup(encoding).name
        self.fileext = fileext

    def save(self, path):
        pass

    @property
    def isdblog(self):
        return self.fileext.lower() == 'csv'


def get_kernel_id(cfile):
    return cfile.split('/')[-1][7:-5]


def get_line_count(path):
    return int(os.popen('wc -l "%s"' % path).read().split()[0])


def normalize_idx(idx, cnt):
    if idx < 0:
        idx = cnt + idx
        if idx < 0:
            raise IndexError
    return min(idx, cnt)


def get_slice_idx(slc, cnt):
    idx1 = normalize_idx(slc.start, cnt)
    idx2 = normalize_idx(slc.stop, cnt)
    return idx1, idx2


def nprint(*args):
    for arg in args:
        print arg
    sys.stdout.flush()


def hdf_path(name):
    hdf_dir = HDF_DIR
    path = os.path.join(hdf_dir, HDF_FILE_PREFIX) + name
    if path.split('.')[-1] != HDF_FILE_EXT:
        path += '.%s' % HDF_FILE_EXT
    return path


def hdf_exists(path, key):
    exist = False
    if os.path.isfile(path):
        store = HDFStore(path)
        exist = key in store
        store.close()
    return exist


class HDF(object):
    def __init__(self, username):
        self.username = username
        self.store = None

    def __enter__(self):
        self.store = HDFStore(hdf_path(self.username))
        return self

    def __exit__(self, _type, value, tb):
        if self.store is not None:
            self.store.close()


try:
    from IPython.core.display import clear_output
    have_ipython = True
except ImportError:
    have_ipython = False


class ProgressBar:
    def __init__(self, title, iterations):
        self.title = title
        self.iterations = iterations
        self.prog_bar = '[]'
        self.fill_char = '='
        self.width = 40
        self.prev_pct = -1
        self.prev_time = 0
        self.__update_amount(0)
        if have_ipython:
            self.animate = self.animate_ipython
        else:
            self.animate = self.animate_noipython

    def clear(self):
        try:
            clear_output()
        except Exception:
            # terminal IPython has no clear_output
            pass
        print '\r', self,
        sys.stdout.flush()

    def animate_ipython(self, eiter):
        pct = self.get_pct(eiter)
        if eiter != self.iterations:
            if self.prev_pct == pct:
                return
            curtime = time.time()
            if curtime - self.prev_time < 1 and pct != 100:
                return
            self.prev_time = curtime
        self.clear()
        self.update_iteration(eiter, pct)

    def update_iteration(self, elapsed_iter, pct):
        self.__update_amount(pct)
        self.prog_bar += '  %d of %s complete' % (elapsed_iter,
                                                  self.iterations)

    def get_pct(self, eiter):
        if self.iterations == 0:
            return 100
        return int(round((eiter / float(self.iterations)) * 100.0))

    def __update_amount(self, pct):
        self.prev_pct = pct
        all_full = self.width - 2
        num_hashes = int(round((pct / 100.0) * all_full))
        self.prog_bar = '[' + self.fill_char * num_hashes + ' ' * \
            (all_full - num_hashes) + ']'
        pct_place = (len(self.prog_bar) / 2) - len(str(pct))
        pct_string = '%d%%' % pct
        prog = self.prog_bar[0:pct_place]
        comp = pct_string + self.prog_bar[pct_place + len(pct_string):]
        self.prog_bar = '%s %s %s' % (self.title, prog, comp)

    def __str__(self):
        return str(self.prog_bar)

    def done(self):
        self.animate(self.iterations)
        print '\r', self,
        sys.stdout.flush()


def head_or_tail(path, head, cnt=10):
    rv = None
    with TemporaryFile('rw') as out:
        c = 'head' if head else 'tail'
        cmd = [c, '-n', str(cnt), path]
        check_call(cmd, stdout=out)
        out.flush()
        out.seek(0)
        rv = out.read()
    return rv


def div(elm, _cls=''):
    if len(_cls) > 0:
        _cls = ' ' + _cls
    return '<div class="row%s">%s</div>' % (_cls, elm)


def heat_map(df, ax_fs=13, **kwargs):
    """
     kwargs param explain

     figsize  (tuple): default (11,4)
     rows_len (int)  : default DataFrame Index Length
     cols_len (int)  : default DataFrame Columns Length
     colorbar (bool) : show up colorbar, (default True)
     celltext (bool) : show up dataframe's value on heatmap (default True)
     xlabel   (str)  : setting the graph xlabel name (default 'NA')
     ylabel   (str)  : setting the graph ylabel name (default 'NA')
     colorbar_lable (str) : setting the colorbar label name, (default 'NA')
     xaxis_tick_top (bool): show up x-axis pos, top of the graph(default False)
     label_fs (int)  : all labels(x,ylabel,title,colorbar)fontsize (default 12)
     txt_fmt  (str)  : celltext format. (default u'{:.0f}')
     ax_fs    (int)  : x-axis and y-axis fontsize (default 13)
     skipna   (bool) : heat_map celltext 'nan value' skip, (default True)
     title    (str)  : set heat_map title (default 'NA')
     cmap     (plt.cm): set heat_map colormaps (default plt.cm.jet)
                        do you want another colormaps?
                        go to this site
                    http://wiki.scipy.org/Cookbook/Matplotlib/Show_colormaps
    """
    expected_args = set(['figsize', 'rows_len', 'cols_len',
                         'colorbar', 'cmap', 'celltext', 'title',
                         'xlabel', 'ylabel', 'colorbar_label',
                         'xaxis_tick_top', 'label_fs', 'txt_fmt', 'skipna'])
    got_kwargs = set(kwargs.keys())
    diff = got_kwargs - expected_args
    if diff:
        raise Exception(" {} : Unexpected".format(tuple(diff)))
    else:
        _init_heat_map_kwargs(df, kwargs)
    rows = kwargs['rows_len']
    cols = kwargs['cols_len']
    cmap = kwargs['cmap']
    _, ax = plt.subplots(figsize=kwargs['figsize'])
    plt.imshow(df, interpolation='nearest', cmap=cmap, aspect='auto')
    _heat_map_celltext(df, ax, rows, cols, kwargs)
    if kwargs['xaxis_tick_top'] is True:
        ax.xaxis.tick_top()
    if kwargs['title'] != 'NA':
        ax.set_title(kwargs['title'], fontsize=kwargs['label_fs'])
    _set_ticks_labels(df, ax, ax_fs, rows, cols, kwargs)
    if kwargs['colorbar'] is True:
        color_bar = plt.colorbar()
        if kwargs['colorbar_label'] != 'NA':
            color_bar.set_label(kwargs['colorbar_label'],
                                fontsize=kwargs['label_fs'])


def _init_heat_map_kwargs(df, kwargs):
    kwargs.setdefault('figsize', (11, 4))
    kwargs.setdefault('colorbar', True)
    kwargs.setdefault('celltext', True)
    kwargs.setdefault('xaxis_top', False)
    kwargs.setdefault('label_fs', 12)
    kwargs.setdefault('xaxis_tick_top', False)
    kwargs.setdefault('colorbar_label', 'NA')
    kwargs.setdefault('xlabel', 'NA')
    kwargs.setdefault('ylabel', 'NA')
    kwargs.setdefault('title', 'NA')
    kwargs.setdefault('rows_len', len(df.index))
    kwargs.setdefault('cols_len', len(df.columns))
    kwargs.setdefault('txt_fmt', u'{:.1f}')
    kwargs.setdefault('skipna', False)
    kwargs.setdefault('cmap', plt.cm.jet)


def _heat_map_celltext(df, ax, rows, cols, kwargs):
    celltext = kwargs['celltext']
    skipna = kwargs['skipna']
    txt_fmt = kwargs['txt_fmt']
    if celltext is True:
        _celltext(df, ax, rows, cols, skipna, txt_fmt)


def _celltext(df, ax, rows, cols, skipna, txt_fmt):
    if skipna is True:
        for i in range(rows):
            for j in range(cols):
                if not math.isnan(df.iget_value(i, j)):
                    ax.text(j, i, txt_fmt.format(df.iget_value(i, j)),
                            size='medium', ha='center', va='center')
    else:
        for i in range(rows):
            for j in range(cols):
                ax.text(j, i, txt_fmt.format(df.iget_value(i, j)),
                        size='medium', ha='center', va='center')


def _set_ticks_labels(df, ax, ax_fs, rows, cols, kwargs):
    ax.set_xticks(np.linspace(0, cols - 1, cols))
    ax.set_yticks(np.linspace(0, rows - 1, rows))
    ax.set_xticklabels(df.columns, fontsize=ax_fs)
    ax.set_yticklabels(df.index, fontsize=ax_fs)
    ax.grid('off')
    xlabel_name = kwargs['xlabel']
    ylabel_name = kwargs['ylabel']
    fs = kwargs['label_fs']
    if xlabel_name != 'NA':
        ax.set_xlabel(xlabel_name, fontsize=fs)
    if ylabel_name != 'NA':
        ax.set_ylabel(ylabel_name, fontsize=fs)


def get_notebook_dir():
    cfg = make_config()
    prj = os.environ['WZDAT_PRJ'] if 'WZDAT_PRJ' in os.environ else ''
    return cfg["NOTEBOOK_DIR"] if "NOTEBOOK_DIR" in cfg\
        else os.path.join(SOLUTION_DIR, "__notes__", prj)


def cap_call(cmd, _test=False):
    out = TemporaryFile()
    err = TemporaryFile()
    try:
        logging.info('cap_call: %s', str(cmd))
        check_call(cmd, shell=True, stdout=out, stderr=err)
    except CalledProcessError:
        if not _test:
            raise
    finally:
        out.flush()
        err.flush()
        out.seek(0)
        err.seek(0)
        _out = out.read()
        _err = err.read()
        if len(_out) > 0:
            logging.debug(_out)
            if _test:
                print(_out)
        if len(_err) > 0:
            logging.error(_err)
            if _test:
                print(_err)


def get_data_dir():
    return os.environ['WZDAT_DATA_DIR'] if 'WZDAT_DATA_DIR' in os.environ\
        else DATA_DIR


def get_convfile_path(path):
    relpath = os.path.relpath(path, get_data_dir())
    return os.path.join(CONV_DIR, relpath)


def convert_data_file(srcpath, encoding, dstpath):
    encoding = encoding.replace('-le', '')
    _dir = os.path.dirname(dstpath)
    cmd = ['mkdir', '-p', _dir]
    check_call(cmd)
    cmd = ['iconv', '-f', encoding, '-t', 'utf-8', '-o', dstpath, srcpath]
    check_call(cmd)
    return dstpath


def convert_server_time_to_client(dt):
    import pytz
    cfg = make_config()

    def get_tz(tz):
        return pytz.UTC if tz == 'UTC' else pytz.timezone(tz)

    stz = get_tz(cfg['SERVER_TIMEZONE'])
    ctz = get_tz(cfg['CLIENT_TIMEZONE'])
    sdt = stz.localize(dt)
    return sdt.astimezone(ctz)


def gen_dummydata(td=None, date_cnt=10):
    if td is None:
        td = os.path.join(BASE_DIR, '..', 'tests', 'dummydata')

    if os.path.isdir(td):
        import shutil
        shutil.rmtree(td)
    os.mkdir(td)

    half_date = int(date_cnt * 0.5)
    start = _datetime.datetime(2014, 3, 1, 0, 0, 0)
    dates = [start + _datetime.timedelta(days=x - half_date) for x in range(0,
             date_cnt)]

    _gen_dummy_files(td, dates)


def _gen_dummy_files(td, dates):
    # file path style; kr/node-01/game_01.log'
    locales = ('kr', 'jp', 'us')
    nodes = ('node-1', 'node-2', 'node-3')
    kinds = ('game', 'auth', 'community')
    processes = {'game': 3, 'auth': 1, 'community': 1}
    for locale in locales:
        for node in nodes:
            for kind in kinds:
                procno = processes[kind]
                _dir = os.path.join(td, locale, node)
                _gen_dummy_lines(_dir, locale, node, kind, dates, procno)


def _gen_dummy_lines(_dir, locale, node, kind, dates, procno):

    def gen_level():
        i = 0
        _levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        while True:
            yield _levels[i]
            i += 1
            if i >= len(_levels):
                i = 0

    def gen_msg():
        i = 0
        _msgs = ['Alloc', 'Move', 'Mismatch', 'Async', 'Failed']
        while True:
            yield _msgs[i]
            i += 1
            if i >= len(_msgs):
                i = 0

    def write_lines(fname, date):
        lgen = gen_level()
        mgen = gen_msg()
        with open(fname, 'w') as f:
            dts = [date + _datetime.timedelta(seconds=x * 60 * 60) for x in
                   range(0, 24)]
            for dt in dts:
                sdt = dt.strftime('%Y-%m-%d %H:%M')
                f.write('%s [%s] - %s\n' % (sdt, lgen.next(), mgen.next()))

    if not os.path.isdir(_dir):
        os.makedirs(_dir)
    for date in dates:
        sdate = date.strftime('%Y-%m-%d')
        if procno == 1:
            path = os.path.join(_dir, kind + '_%s.log' % sdate)
            write_lines(path, date)
        else:
            for proc in range(procno):
                path = os.path.join(_dir, kind + "_%s %02d.log" %
                                    (sdate, proc + 1))
                write_lines(path, date)
