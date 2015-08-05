"""Utilities."""

import os
import re
import sys
import math
import logging
from datetime import datetime
import datetime as _datetime
import cPickle
import fnmatch
import time
from subprocess import check_call, CalledProcessError
import tempfile
from tempfile import TemporaryFile
import uuid as _uuid
import codecs
from collections import defaultdict
from tempfile import gettempdir

from crontab import CronTab

from wzdat.make_config import make_config
from wzdat.const import NAMED_TMP_PREFIX, HDF_FILE_PREFIX, HDF_FILE_EXT


LOG_KINDS = ('game', 'auth', 'community')
PROCESSES = {'game': 3}

CRON_CMD = '/usr/bin/crontab'


def unique_tmp_path(prefix, ext='.txt'):
    """Return temp file path with given extension."""
    uuid = prefix + str(_uuid.uuid4())
    tmp_dir = get_tmp_dir()
    return os.path.join(tmp_dir, uuid) + ext, uuid


def named_tmp_path(userid, slotname, test=False):
    name = "{prefix}{user}-{slot}".format(prefix=NAMED_TMP_PREFIX, uid=userid,
                                          slot=slotname)
    tmp_dir = get_tmp_dir()
    return os.path.join(tmp_dir, name)


def normalize_path_elms(path):
    """Replace space & dash into underbar and return it."""
    return path.replace(' ', '__').replace('-', '_')


def sizeof_fmt(num):
    """Return human readable size."""
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            if x == 'bytes':
                return "%d %s" % (num, x)
            else:
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
                try:
                    os.remove(curpath)
                except OSError, e:
                    logging.warning("Fail to remove old tmps: "
                                    "{}".format(str(e)))


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

    def __dir__(self):
        return self.__dict__['dict'].keys()

    def __iter__(self):
        for i in self.__dict__['dict'].keys():
            yield i


class Context(Property):

    """Domain context object."""

    @classmethod
    def load(path):
        pass

    def __init__(self, mod, startdir, encoding, file_type):
        super(Context, self).__init__()
        self.mod = mod
        self.files = mod['all_files'] = []
        self.fields = mod['fields'] = {}
        self.startdir = startdir
        if len(encoding) > 0:
            self.encoding = codecs.lookup(encoding).name
        else:
            self.encoding = ''
        self.file_type = file_type
        assert 'file_filter' in mod,\
            "Adapter must implement 'file_filter' function"
        self.ffilter = mod['file_filter']

    def save(self, path):
        pass

    @property
    def isdblog(self):
        return self.file_type.lower() == 'csv'


def get_kernel_id(cfile):
    return cfile.split('/')[-1][7:-5]


def get_line_count(path):
    return int(os.popen('wc -l "%s"' % path).read().split()[0])


def normalize_idx(idx, cnt):
    if idx < 0:
        idx = 0
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
    hdf_dir = get_hdf_dir()
    path = os.path.join(hdf_dir, HDF_FILE_PREFIX) + name
    if path.split('.')[-1] != HDF_FILE_EXT:
        path += '.%s' % HDF_FILE_EXT
    return path


def hdf_exists(path, key):
    from pandas import HDFStore
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
        from pandas import HDFStore
        self.store = HDFStore(hdf_path(self.username))
        return self

    def __exit__(self, _type, value, tb):
        if self.store is not None:
            self.store.close()


class OfflineNBPath(object):
    def __init__(self, nbpath):
        from tempfile import gettempdir
        self.fpath = os.path.join(gettempdir(), '_offline_nbpath_')
        with open(self.fpath, 'w') as fp:
            fp.write(nbpath.encode('utf8'))

    def __enter__(self):
        return self

    def __exit__(self, _type, value, tb):
        os.unlink(self.fpath.encode('utf8'))


def get_offline_nbpath():
    fpath = os.path.join(gettempdir(), '_offline_nbpath_')
    print(u'get_offline_nbpath {}'.format(fpath))
    with open(fpath, 'r') as f:
        return f.readline().decode('utf8')


try:
    from IPython.core.display import clear_output
    have_ipython = True
except ImportError:
    have_ipython = False


class ProgressBar:
    def __init__(self, title, iterations, prog_cb=None):
        self.title = title
        self.iterations = iterations
        self.prog_bar = '[]'
        self.fill_char = '='
        self.width = 40
        self.prev_pct = -1
        self.prev_time = 0
        self.__update_amount(0)
        self.prog_cb = prog_cb
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
        if self.prog_cb is not None:
            self.prog_cb(pct / 100.0)
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
     cmap     (plt.cm): set heat_map colormaps (default plt.cm.cool)
                        do you want another colormaps?
                        go to this site
                    http://wiki.scipy.org/Cookbook/Matplotlib/Show_colormaps
    """
    import matplotlib.pyplot as plt
    from matplotlib import ticker

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
        txt_fmt = kwargs['txt_fmt']
        if hasattr(txt_fmt, '__call__'):
            def fmt(x, pos):
                return txt_fmt(x)
            color_bar = plt.colorbar(format=ticker.FuncFormatter(fmt))
        else:
            color_bar = plt.colorbar()
        if kwargs['colorbar_label'] != 'NA':
            color_bar.set_label(kwargs['colorbar_label'],
                                fontsize=kwargs['label_fs'])


def make_cmap():
    from matplotlib import colors
    cdict = {'blue': ((0.0, 0, 0.7),
                      (0.5, 0.5, 0.5),
                      (1.0, 0.5, 0.0)),
             'green': ((0.0, 0, 0.5),
                       (0.5, 0.9, 0.9),
                       (0.8, 0.7, 0.7),
                       (1.0, 0.5, 0.0)),
             'red':  ((0.0, 0, 0.5),
                      (0.1, 0.3, 0.3),
                      (0.5, 0.2, 0.2),
                      (0.8, 0.7, 0.7),
                      (1.0, 1.0, 0.0))
             }

    return colors.LinearSegmentedColormap('my_colormap', cdict, 256)


def _init_heat_map_kwargs(df, kwargs):
    import matplotlib.pyplot as plt

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
    kwargs.setdefault('cmap', plt.cm.summer)


def _heat_map_celltext(df, ax, rows, cols, kwargs):
    celltext = kwargs['celltext']
    skipna = kwargs['skipna']
    txt_fmt = kwargs['txt_fmt']
    if celltext is True:
        _celltext(df, ax, rows, cols, skipna, txt_fmt)


def _celltext(df, ax, rows, cols, skipna, txt_fmt):
    def valtxt(i, j):
        val = df.iget_value(i, j)
        if hasattr(txt_fmt, '__call__'):
            return txt_fmt(val)
        else:
            return txt_fmt.format(val)

    if skipna is True:
        for i in range(rows):
            for j in range(cols):
                if not math.isnan(df.iget_value(i, j)):
                    ax.text(j, i, valtxt(i, j), size='medium', ha='center',
                            va='center')
    else:
        for i in range(rows):
            for j in range(cols):
                ax.text(j, i, valtxt(i, j), size='medium', ha='center',
                        va='center')


def _set_ticks_labels(df, ax, ax_fs, rows, cols, kwargs):
    import numpy as np
    ax.set_xticks(np.linspace(0, cols - 1, cols))
    ax.set_yticks(np.linspace(0, rows - 1, rows))
    ax.set_xticklabels(df.columns, fontsize=ax_fs)
    ax.set_yticklabels([i.decode('utf8') if type(i) is str else i for i in
                        df.index], fontsize=ax_fs)
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
    sol_dir = cfg['sol_dir']
    prj = cfg['prj']
    base = cfg['notebook_base_dir'] if 'notebook_base_dir' in cfg else\
        os.path.join(sol_dir, '__notes__')
    return os.path.join(base, prj).decode('utf8')


def _check_makedir(adir, make):
    if not os.path.isdir(adir) and make:
        if os.path.isfile(adir):
            os.remove(adir)
        os.makedirs(adir)


def _get_dir(basedir, subdir, make):
    vardir = os.path.join(basedir, subdir)
    _check_makedir(vardir, make)
    return vardir


def get_data_dir(make=True):
    cfg = make_config()
    data_dir = cfg['data_dir']
    if not os.path.isdir(data_dir) and make:
        if os.path.isfile(data_dir):
            os.remove(data_dir)
        os.makedirs(data_dir)
    return data_dir


def get_var_dir(make=True):
    return _get_dir(get_data_dir(), '_var_', make)


def get_tmp_dir(make=True):
    return _get_dir(get_var_dir(), 'tmp', make)


def get_hdf_dir(make=True):
    return _get_dir(get_var_dir(), 'hdf', make)


def get_conv_dir(make=True):
    return _get_dir(get_var_dir(), 'conv', make)


def get_cache_dir(make=True):
    return _get_dir(get_var_dir(), 'cache', make)


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


def get_convfile_path(path):
    cfg = make_config()
    relpath = os.path.relpath(path, cfg['data_dir'])
    conv_dir = get_conv_dir()
    return os.path.join(conv_dir, relpath)


def convert_data_file(srcpath, encoding, dstpath):
    encoding = encoding.replace('-le', '')
    _dir = os.path.dirname(dstpath)
    if not os.path.exists(_dir):
        os.makedirs(_dir)
    cmd = ['iconv', '-f', encoding, '-t', 'utf-8', '-o', dstpath, srcpath]
    check_call(cmd)
    # if iconv failed
    if not os.path.isfile(dstpath):
        # log it, then touch
        logging.error('convert_data_file failed: from {} to {}'
                      .format(srcpath, dstpath))
        touch(dstpath)
    return dstpath


def touch(path):
    '''Emulate shell touch'''
    with open(path, 'a'):
        os.utime(path, None)


def convert_server_time_to_client(dt, stz=None, ctz=None):
    import pytz
    cfg = make_config()

    def get_tz(tz):
        return pytz.UTC if tz == 'UTC' else pytz.timezone(tz)

    stz = get_tz(cfg['server_timezone']) if stz is None else stz
    ctz = get_tz(cfg['client_timezone']) if ctz is None else ctz
    sdt = stz.localize(dt)
    return sdt.astimezone(ctz)


def gen_dummydata(td=None, date_cnt=10):
    if os.path.isdir(td):
        import shutil
        shutil.rmtree(td)
    if not os.path.exists(td):
        os.makedirs(td)

    half_date = int(date_cnt * 0.5)
    start = _datetime.datetime(2014, 3, 1, 0, 0, 0)
    dates = [start + _datetime.timedelta(days=x - half_date) for x in range(0,
             date_cnt)]

    _gen_dummy_files(td, dates)


def _gen_dummy_files(td, dates):
    # file path style; kr/node-01/game_01.log'
    locales = ('kr', 'jp', 'us')
    nodes = ('node-1', 'node-2', 'node-3')
    for locale in locales:
        for node in nodes:
            bdir = os.path.join(td, locale, node)
            _dir = _get_dir(bdir, 'log', True)  # check dir exist
            for lkind in LOG_KINDS:
                procno = PROCESSES.get(lkind, 1)
                _gen_dummy_log_lines(_dir, locale, node, lkind, dates, procno)

            _dir = _get_dir(bdir, 'dump', True)  # check dir exist
            _gen_dummy_dump(_dir, locale, node, dates)

            _dir = _get_dir(bdir, 'exlog', True)  # check dir exist
            _gen_dummy_exlog_lines(_dir, locale, node, dates)


def _gen_dummy_dump(_dir, locale, node, dates):
    import random
    random.shuffle(dates)
    for i, kind in enumerate(LOG_KINDS):
        d = dates[i]
        procno = PROCESSES.get(kind, 1)
        procno = '' if PROCESSES.get(kind, 1) == 1 else\
            '-{}'.format(random.randint(1, procno))
        fn = "{}-{:02d}{:02d}{:02d}{}.dmp".format(kind, d.year, d.month, d.day,
                                                  procno)
        path = os.path.join(_dir, fn)
        with open(path, 'at') as f:
            f.write('dump data')


def _gen_dummy_level(_levels):
    i = 0
    while True:
        yield _levels[i]
        i += 1
        if i >= len(_levels):
            i = 0


def _gen_dummy_msg(_msgs):
    i = 0
    while True:
        yield _msgs[i]
        i += 1
        if i >= len(_msgs):
            i = 0


def _gen_dummy_exlog_lines(_dir, locale, node, dates):

    def write_lines(fname, date):
        lgen = _gen_dummy_level(['DBG', 'WARN', 'ERR'])
        mgen = _gen_dummy_msg(['Send', 'Receive', 'Exit', 'Enter', 'Failed'])
        with open(fname, 'w') as f:
            dts = [date + _datetime.timedelta(seconds=x * 60 * 60) for x in
                   range(0, 24)]
            for dt in dts:
                sdt = dt.strftime('%Y%m%d-%H:%M')
                f.write('%s %s %s\n' % (sdt, lgen.next(), mgen.next()))

    for date in dates:
        sdate = date.strftime('%Y%m%d')
        path = os.path.join(_dir, 'ExLog-%s.log' % sdate)
        write_lines(path, date)


def _gen_dummy_log_lines(_dir, locale, node, kind, dates, procno):

    def write_lines(fname, date):
        lgen = _gen_dummy_level(['DEBUG', 'INFO', 'WARNING', 'ERROR',
                                 'CRITICAL'])
        mgen = _gen_dummy_msg(['Alloc', 'Move', 'Mismatch', 'Async', 'Failed'])
        with open(fname, 'w') as f:
            dts = [date + _datetime.timedelta(seconds=x * 60 * 60) for x in
                   range(0, 24)]
            for dt in dts:
                sdt = dt.strftime('%Y-%m-%d %H:%M')
                f.write('%s [%s] - %s\n' % (sdt, lgen.next(), mgen.next()))

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


class ChangeDir(object):
    def __init__(self, *dirs):
        self.cwd = os.getcwd()
        self._path = os.path.join(*dirs)

    def __enter__(self):
        logging.info('change dir to %s', self._path)
        assert os.path.isdir(self._path)
        os.chdir(self._path)

    def __exit__(self, atype, value, tb):
        os.chdir(self.cwd)


def remove_ansicolor(text):
    return re.sub(r'\x1b\[([0-9,A-Z]{1,2}(;[0-9]{1,2})?(;[0-9]{3})?)?[m|K]?',
                  '', text)


def get_datetime(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(ts)


def get_sdatetime(ts=None):
    dt = get_datetime(ts)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def get_client_datetime(ts=None):
    if ts is None:
        ts = time.time()
    return convert_server_time_to_client(datetime.fromtimestamp(ts))


def get_client_sdatetime(ts=None):
    dt = get_client_datetime(ts)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def parse_sdatetime(dt):
    from dateutil.parser import parse
    return parse(dt)


def parse_client_sdatetime(dt):
    return convert_server_time_to_client(parse_sdatetime(dt))


def get_cache_path(fmt):
    cache_dir = get_cache_dir()
    return os.path.join(cache_dir, '%s_found_files.pkl' % (fmt))


def _filter_files(adir, filenames, filecnt, file_type, ffilter):
    """
    Filter files by file type(filter function) then returns matching
    files and cumulated count.
    """
    rfiles = ffilter(adir, filenames)
    filecnt += len(rfiles)
    return rfiles, filecnt


def find_files_and_save(startdir, file_type, use_cache, ffilter=None,
                        root_list=None):
    logging.debug('find_files_and_save')
    logging.debug('startdir: ' + str(startdir))
    if root_list is None:
        root_list = []
    nprint('finding files and save info...')
    filecnt = 0
    assert os.path.isdir(startdir)
    for root, dirs, filenames in os.walk(startdir):
        dirs[:] = [d for d in dirs if d not in ('_var_',)]
        _root = [os.path.abspath(root), []]
        root_list.append(_root)
        if len(filenames) > 0:
            rfiles, filecnt = _filter_files(root, filenames, filecnt,
                                            file_type, ffilter)
            rfiles = sorted(rfiles)
            _root[1] = rfiles
    rv = sorted(root_list), filecnt
    if use_cache:
        cpath = get_cache_path(file_type)
        with open(cpath, 'w') as f:
            cPickle.dump(rv, f)
    return rv


def _get_found_time(cpath):
    s = int(time.time() - os.path.getmtime(cpath))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return '%d hour %d minute' % (h, m)
    else:
        return '%d minute %d seconds' % (m, s)


def load_files_precalc(ctx, root_list):
    cfg = make_config()
    use_cache = cfg['use_cache'] if 'use_cache' in cfg else True
    cpath = get_cache_path(ctx.file_type)
    if use_cache and os.path.isfile(cpath):
        tstr = _get_found_time(cpath)
        msg = '\nusing file infos found %s ago.' % tstr
        with open(cpath, 'r') as f:
            root_list, filecnt = cPickle.load(f)
            if filecnt > 0:
                return (root_list, filecnt), msg
    return find_files_and_save(ctx.startdir, ctx.file_type, use_cache,
                               ctx.ffilter, root_list), None


def cache_files():
    import imp
    from rundb import update_cache_info
    logging.debug('cache_files')
    cfg = make_config()
    with ChangeDir(cfg['sol_dir']):
        # prevent using cache
        if 'file_types' not in cfg:
            logging.warning('no file_types in cfg. exit')
            return
        old_use_cache = cfg['use_cache']
        data_dir = cfg['data_dir']
        pkg = cfg['sol_pkg']
        prj = cfg['prj']
        print "Caching files for: %s" % prj
        ftypes = cfg['file_types']
        for ftype in ftypes:
            mpath = '%s/%s/%s.py' % (pkg, prj, ftype)
            mod = imp.load_source('%s' % ftype,  mpath)
            ffilter = mod.file_filter
            find_files_and_save(data_dir, ftype, True, ffilter, [])
        update_cache_info()
        cfg['use_cache'] = old_use_cache


def cache_finder():
    import imp
    from wzdat.rundb import update_finder_info
    # Make cache for file finder.
    logging.debug('cache_finder')
    cfg = make_config()
    with ChangeDir(cfg['sol_dir']):
        if 'file_types' not in cfg:
            logging.warning('no file_types in cfg. exit')
            return
        ret = []
        if ret is None or len(ret) == 0:
            pkg = cfg['sol_pkg']
            prj = cfg['prj']
            ftypes = cfg["file_types"]
            sol_dir = cfg['sol_dir']
            os.chdir(sol_dir)
            ret = []
            for ft in ftypes:
                mpath = '%s/%s/%s.py' % (pkg, prj, ft)
                mod = imp.load_source('%s' % ft,  mpath)
                mod.load_info()
                dates = [str(date) for date in mod.dates[:-15:-1]]
                kinds = sorted([str(kind) for kind in mod.kinds.group()])
                nodes = sorted([str(node) for node in mod.nodes])
                info = ft, dates, kinds, nodes
                ret.append(info)
            update_finder_info(ret)
        return ret


def disable_perfwarn():
    import warnings
    import pandas
    warnings.simplefilter("ignore", pandas.io.common.PerformanceWarning)


def dashboard_alert(atype, htmlmsg):
    return u'<div class="alert alert-{atype}" role="alert">{htmlmsg}</div>'.\
        format(atype=atype, htmlmsg=htmlmsg)


class KeyDefaultDict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


def get_wzdat_host():
    return os.environ['WZDAT_HOST'] if 'WZDAT_B2DHOST' not in os.environ else\
        os.environ['WZDAT_B2DHOST']


def get_dashboard_port():
    cfg = make_config()
    return cfg['host_dashboard_port']


def get_ipython_port():
    cfg = make_config()
    return cfg['host_ipython_port']


def get_notebook_path():
    import json
    import urllib2
    from IPython.lib import kernel
    try:
        connection_file_path = kernel.get_connection_file()
        connection_file = os.path.basename(connection_file_path)
        kernel_id = connection_file.split('-', 1)[1].split('.')[0]
    except (RuntimeError, IndexError):
        return get_offline_nbpath()

    url = "http://{}:{}/api/sessions".format(
        get_wzdat_host(), get_ipython_port())
    sessions = json.load(urllib2.urlopen(url))
    for sess in sessions:
        if sess['kernel']['id'] == kernel_id:
            path = sess['notebook']['path']
            name = sess['notebook']['name']
            return os.path.join(path, name)


class ScbProperty(Property):
    '''
        Property with callback function to be evoked by set operation.
    '''
    def __init__(self, set_cb):
        super(ScbProperty, self).__init__()
        self._set_cb = set_cb

    def __setattr__(self, attr, val):
        self.__dict__['dict'][attr] = val
        self._set_cb(attr, val)


def file_checksum(apath):
    """Return rough checksum by file size & modifiy date"""
    fs = os.stat(apath)
    return hash((apath, fs.st_size, fs.st_mtime))


def dataframe_checksum(df):
    return hash((len(df), df.values.nbytes, df.index.nbytes,
                 df.columns.nbytes))


def get_notebook_manifest_path(nbpath):
    return nbpath.replace(u'.ipynb', u'.manifest.ipynb')


def iter_notebooks(nbdir):
    nbptrn = '*.ipynb'
    nbdir = get_notebook_dir()
    for root, adir, files in os.walk(nbdir.encode('utf8')):
        for nb in fnmatch.filter(files, nbptrn):
            if '-checkpoint' in nb:
                continue
            if '.manifest.' in nb:
                continue
            yield os.path.join(root, nb).decode('utf8')


def iter_notebook_manifest(nbdir, check_depends, skip_nbs=None):
    from wzdat.manifest import Manifest, RecursiveReference
    for npath in iter_notebooks(nbdir):
        if skip_nbs is not None and npath in skip_nbs:
            continue
        mpath = get_notebook_manifest_path(npath)
        if not os.path.isfile(mpath.encode('utf8')):
            continue
        try:
            manifest = Manifest(False, check_depends, npath)
        except RecursiveReference, e:
            logging.error(unicode(e).encode('utf8'))
            continue
        yield npath, manifest


def iter_notebook_manifest_input(nbdir):
    import json
    for npath in iter_notebooks(nbdir):
        mpath = get_notebook_manifest_path(npath)
        if not os.path.isfile(mpath):
            continue
        with open(mpath, 'r') as f:
            data = json.loads(f.read())
            try:
                minp = ''.join(data['worksheets'][0]['cells'][0]['input'])
            except (KeyError, IndexError):
                continue
            minp = eval(minp)
            yield npath, minp


def iter_dashboard_notebook(nbdir):
    """Return dashboard notebook and its manifest input."""
    logging.debug(u"iter_notebook_manifest_input {}".format(nbdir))
    for npath, mip in iter_notebook_manifest_input(nbdir):
        if 'dashboard' in mip:
            yield npath, mip


def find_hdf_notebook_path(_owner, _sname):
    '''return path of hdf source notebook by examining manifest.'''
    nbdir = get_notebook_dir()
    for nbpath, minp in iter_notebook_manifest_input(nbdir):
        if 'output' in minp and 'hdf' in minp['output']:
            owner, sname = minp['output']['hdf']
            if owner == _owner and sname == _sname:
                return nbpath


def iter_scheduled_notebook(nbdir):
    for nbpath, minp in iter_notebook_manifest_input(nbdir):
        if 'schedule' in minp:
            schedule = minp['schedule']
            yield nbpath, schedule


def register_cron_notebooks():
    nb_dir = get_notebook_dir()
    nbpaths = []
    schedules = []
    for path, scd in iter_scheduled_notebook(nb_dir):
        nbpaths.append(path)
        schedules.append(scd)
    _register_crons(nbpaths, schedules)
    return nbpaths, schedules


def _register_crons(paths, scheds):
    logging.debug('register_cron_notebooks')
    # start new cron file with env vars
    filed, tpath = tempfile.mkstemp()
    fileh = os.fdopen(filed, 'wb')
    assert 'WZDAT_CFG' in os.environ
    assert 'WZDAT_HOST' in os.environ
    cfg_path = os.environ['WZDAT_CFG']
    host = os.environ['WZDAT_HOST']
    fileh.write('WZDAT_DIR=/wzdat\n')
    fileh.write('WZDAT_CFG=%s\n' % cfg_path)
    fileh.write('WZDAT_HOST=%s\n' % host)
    fileh.write('LANG=en_US.UTF-8\n')
    fileh.close()
    check_call([CRON_CMD, tpath])
    os.unlink(tpath)

    cron = CronTab()
    # clear registered notebooks
    cron.remove_all('cron-ipynb')

    for i, path in enumerate(paths):
        sched = scheds[i]
        fname = os.path.basename(path)
        cmd = ' '.join(['python', '-m', 'wzdat.jobs run-notebook "%s"' % path,
                        ' > "/tmp/cron-ipynb-%s" 2>&1' % fname])
        job = cron.new(cmd)
        job.setall(sched)
    cron.write()
