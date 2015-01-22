# -*- coding: utf-8 -*-
"""Selectors."""

import os
import types
import fnmatch
import codecs
import zipfile
import time
import shutil
from collections import defaultdict
from subprocess import check_call, CalledProcessError
import pickle
import logging
import traceback

import numpy as np
import pandas as pd
from IPython.display import HTML
from pandas import DataFrame
from pandas import HDFStore

from wzdat.base import Listable, Representable, ISearchable, ILineAttr, \
    IFramable, IPathable, IFilterable, IMergeable
from wzdat.make_config import make_config
from wzdat.const import TMP_PREFIX, PRINT_LMAX, NAMED_TMP_PREFIX, \
    CHUNK_CNT, FORWARDER_LOG_PREFIX
from wzdat.value import ValueList, FailValue, Value, check_date_slice
from wzdat.util import unique_tmp_path, sizeof_fmt, unique_list, \
    remove_empty_file, Property, remove_old_tmps, get_line_count, \
    get_slice_idx, ProgressBar, nprint, Context, convert_data_file,\
    get_convfile_path, get_tmp_dir, get_cache_dir, get_conv_dir
from wzdat.lineinfo import LineInfo, LineInfoImpl_Count, LineInfoImpl_Array

qmode = 'files'
cfg = make_config()


def get_urls():
    assert 'WZDAT_HOST' in os.environ
    host = os.environ['WZDAT_HOST']
    if "host_dashboard_port" in cfg:
        port = ":%s/" % cfg["host_dashboard_port"]
    else:
        port = ":80/"
    mode = cfg['mode'] + '-' if 'mode' in cfg else ''

    data_url = '<a href="http://' + host + port + 'file/%s">%s</a>'
    tmp_url = '<a href="http://' + host + port + mode + 'tmp/%s">%s</a>'
    return data_url, tmp_url


class IllegalOption(Exception):

    """Raise illegal option error."""

    pass


class InvalidProp(Exception):

    """Raise invalid property error."""

    pass


class FileCommon(Representable, ILineAttr, Listable, ISearchable,
                 IFramable):

    """Common file object for one or multiple files."""

    def __init__(self, ctx):
        super(FileCommon, self).__init__()
        self._ctx = ctx


def _get_member(ctx, fname, required=True):
    try:
        return ctx.mod[fname]
    except KeyError:
        if required:
            raise Exception('"%s" is not defined in context.' % fname)
        else:
            return None


def _open_with_codec(ctx, path):
    if open(path).readline().startswith(codecs.BOM_UTF8):
        enc = 'utf-8-sig'
    else:
        enc = ctx.encoding
    return codecs.open(path, 'r', enc, errors='ignore')


class SingleFile(FileCommon, IPathable):

    """File object for one matching file."""

    def __init__(self, ctx, linfos):
        super(SingleFile, self).__init__(ctx)
        self._linfos = linfos
        self._lcount = -1
        self._nodes_cache = None
        self._dates_cache = None
        self._kinds_cache = None
        self._files_cache = None
        self._conv_file = None

    @property
    def abspath(self):
        return self._abspath

    @property
    def filename(self):
        return self._filename

    @property
    def path(self):
        """Return relative path of temp file."""
        return self.abspath.replace(self._ctx.startdir + '/', '')

    def __getitem__(self, idx):
        if isinstance(idx, types.SliceType):
            return self._slice(idx)
        no = 0
        with _open_with_codec(self._ctx, self.abspath) as f:
            while True:
                line = f.readline()
                if no == idx:
                    return line
                no += 1

    def __getslice__(self, idx1, idx2):
        return self._slice(slice(idx1, idx2))

    def _slice(self, slc):
        idx1, idx2 = get_slice_idx(slc, self.count)
        tmp_file, _ = unique_tmp_path(TMP_PREFIX)
        with open(tmp_file, 'w') as out:
            cmd = ['sed', '-n', '%d,%dp' % (idx1 + 1, idx2), self.abspath]
            check_call(cmd, stdout=out)

        # TODO: check with simple test file
        linfos = self._linfos[slice(idx1, idx2)]
        return TempFile(self._ctx, tmp_file, linfos, True)

    def __len__(self):
        return get_line_count(self.abspath)

    @property
    def lcount(self):
        """Return line count of file."""
        return self._calc_lcount()

    def _calc_lcount(self):
        if self._lcount == -1:
            self._lcount = get_line_count(self.abspath)
        return self._lcount

    def head(self, count=10):
        """Return top lines like head command.

        Parameters
        ----------
        count : int
            How many lines to print (the default is 10)

        """
        return _file_head_or_tail(self, True, count)

    def tail(self, count=10):
        """Return bottom lines like tail command.

        Parameters
        ----------
        count : int
            How many lines to print (the default is 10)

        """
        return _file_head_or_tail(self, False, count)

    def to_frame(self, usecols=None, chunk_cnt=CHUNK_CNT, show_prog=True):
        """Build Pandas DataFrame from file and return it."""
        if self.lcount == 0:
            return None
        _to_frame_fn = _get_member(self._ctx, 'to_frame', False)

        if _to_frame_fn is not None:
            return _to_frame_fn(self.abspath, self, usecols)
        else:
            if self._ctx.isdblog:
                assert False, "dblog should have its own 'to_frame'"
            return self._to_frame(usecols, chunk_cnt, show_prog)

    def _to_frame(self, usecols, chunk_cnt, show_prog):
        _c = Property()
        _c.lineno = 0
        _c.linecnt = get_line_count(self.abspath)
        _c.show_prog = show_prog
        _c.chunk_cnt = chunk_cnt
        return pd.concat(self._to_frame_gen(_c, usecols))

    # TODO: refactoring
    def _to_frame_gen(self, _c, usecols):
        f = _open_with_codec(self._ctx, self.abspath)
        if _c.show_prog:
            pg = ProgressBar('to_frame', _c.linecnt)

        tfp = None
        hasna = False
        smap = {}
        _c.lineno = 0
        fdates = [date._sdate for date in self._linfos.dates]
        nodes = [node for node in self._linfos.nodes]
        kinds = [kind for kind in self._linfos.kinds]
        for line in f:
            if tfp is None:
                tfp = self._to_frame_init_tfp()
                slineno = 0
                hasna = False
            if _c.show_prog:
                pg.animate(_c.lineno)

            fdate = fdates[_c.lineno]
            _date, level, msg, hasna = \
                _to_frame_convert_line(tfp, fdate, line, hasna, smap)

            if _date is not None:
                tfp.msgs.append(msg)
                tfp.levels.append(level)
                tfp.dates.append(_date)
                tfp.nodes.append(nodes[_c.lineno])
                tfp.kinds.append(kinds[_c.lineno])

            _c.lineno += 1
            slineno += 1

            if slineno >= _c.chunk_cnt:
                df = self._to_frame_build_data_frame(tfp, hasna)
                tfp = None
                yield df

        df = self._to_frame_build_data_frame(tfp, hasna)
        if _c.show_prog:
            pg.done()
        yield df

    def to_frame_hdf(self, store_path, store_key, df_cb=None, max_msg=None,
                     usecols=None, chunk_cnt=CHUNK_CNT, show_prog=True):
        store = HDFStore(store_path, 'w')
        df = self._to_frame(usecols, chunk_cnt, show_prog)
        df['msg'] = df['msg'].apply(lambda m: m.encode('utf8'))
        if df_cb is not None:
            df_cb(df)
        min_itemsize = {'kind': 20, 'msg': 255}
        if max_msg is not None:
            min_itemsize['msg'] = max_msg
        store.put(store_key, df, format='table', min_itemsize=min_itemsize)
        store.flush()
        store.close()

    def _to_frame_init_tfp(self):
        tfp = Property()

        # get functions
        tfp.get_line_date = _get_member(self._ctx, 'get_line_date', False)
        if tfp.get_line_date is None:
            tfp.get_line_time = _get_member(self._ctx, 'get_line_time')
        tfp.get_line_msg = _get_member(self._ctx, 'get_line_msg')
        tfp.get_line_type = _get_member(self._ctx, 'get_line_type', False)

        tfp.nodes = []
        tfp.kinds = []
        tfp.dates = []
        tfp.levels = []
        tfp.msgs = []
        return tfp

    def _to_frame_build_data_frame(self, tfp, hasna):
        # build data frame
        dfinfo = {'node': tfp.nodes, 'kind': tfp.kinds, 'msg': tfp.msgs}
        dfcols = ['node', 'kind']
        if tfp.get_line_type is not None:
            dfinfo['level'] = tfp.levels
            dfcols.append('level')
        dfcols.append('msg')
        df = DataFrame(dfinfo, index=tfp.dates, columns=dfcols)
        if hasna:
            df = df.dropna()
        df.index.name = 'dtime'
        # pytable not support unicode for now
        df['node'] = df['node'].astype(str)
        df['kind'] = df['kind'].astype(str)
        if 'level' in df.columns:
            df['level'] = df['level'].astype(str)
        return df

    @property
    def nodes(self):
        """Return unique node list for each log line is from."""
        return self._linfos.unique_nodes

    @property
    def kinds(self):
        """Return unique kind list for each log line is from."""
        return self._linfos.unique_kinds

    @property
    def dates(self):
        """Return unique date list for each log line is from."""
        return self._linfos.unique_dates

    @property
    def files(self):
        """Return unique source file list for each log line is from."""
        return self._linfos.unique_files

    @property
    def size(self):
        """Return file size."""
        return os.path.getsize(self.abspath)

    @property
    def hsize(self):
        """Return human readable file size."""
        _size = os.path.getsize(self.abspath)
        return sizeof_fmt(_size)

    @property
    def link(self):
        """Return html link to file for IPython notebook to download."""
        return HTML(self.link_str())

    def link_str(self, label=None):
        """Return URL to file for IPython notebook to download."""
        data_url, tmp_url = get_urls()
        if self._ctx.startdir in self.abspath:
            filename = self.abspath.split(os.path.sep)[-1]
            _label = self.path if label is None else label
            return data_url % (self.path, _label)
        else:
            filename = self.abspath.split(os.path.sep)[-1]
            _label = filename if label is None else label
            _, TMP_URL = get_urls()
            return TMP_URL % (filename, _label)

    @property
    def zlink(self):
        """Return html link to zipped file for IPython notebook to download."""
        tmp_file, _ = unique_tmp_path(TMP_PREFIX, '.zip')
        with zipfile.ZipFile(tmp_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            _add_zip_flat(zf, self.abspath)
        filename = tmp_file.split(os.path.sep)[-1]
        _, TMP_URL = get_urls()
        return HTML(TMP_URL % (filename, filename))


class TempFile(SingleFile):

    """Temp file object."""

    def __init__(self, ctx, path, linfos, showall=False):
        super(TempFile, self).__init__(ctx, linfos)
        self._abspath = path
        self._count = get_line_count(path)
        self.showall = showall

    def __unicode__(self):
        if self._count == 0 or\
           (not self.showall and self.lcount > PRINT_LMAX):
            return '%s\npath: %s\nline count: %d\nsize: %s' % \
                   (type(self), self.abspath, self.lcount, self.hsize)
        else:
            return _open_with_codec(self._ctx, self.abspath).read()

    def __iter__(self):
        with _open_with_codec(self._ctx, self.abspath) as f:
            while True:
                line = f.readline()
                if not line:
                    break
                yield line

    def find(self, word, options=None, include_header=False):
        """Find word in temp file and return result temp file."""
        return _find_in_temp(self._ctx, self, word, options, include_header)

    def save(self, cppath):
        shutil.copy(self.abspath, cppath)
        self._linfos.save(cppath)


def _find_in_temp(ctx, tempo, word, options, include_header):
    """Find word in a temp file and return result temp file."""
    tmp_file, _ = unique_tmp_path(TMP_PREFIX)
    linfos = _find_in_temp_grep(ctx, tmp_file, tempo, word, options,
                                include_header)
    return TempFile(ctx, tmp_file, linfos)


def _to_frame_convert_line(tfp, fdate, line, hasna, smap):
    import dateutil
    try:
        sdate = None
        if tfp.get_line_date is not None:
            sdate = tfp.get_line_date(line)
        elif tfp.get_line_time is not None:
            sdate = "%s %s" % (fdate, tfp.get_line_time(line))
        if sdate in smap:
            _date = smap[sdate]
        else:
            _date = dateutil.parser.parse(sdate)
            smap[sdate] = _date
    except TypeError:
        level = None
        msg = None
        hasna = True
        _date = None
    else:
        level = tfp.get_line_type(line) if tfp.get_line_type is\
            not None else np.nan
        msg = tfp.get_line_msg(line)
    return _date, level, msg, hasna


class Field(object):

    """Field object."""

    def __init__(self, ctx):
        self._values = set()
        ctx.fields[self.__class__] = self
        self._ctx = ctx

    def _file_filter(self):
        for val in self._values:
            if val is not None:
                setattr(self, val._abbr, val)

    def __getattr__(self, key):
        """Return appropriate value instance when asked undefined attribute."""
        vals = []
        for val in self._values:
            if val._abbr.startswith(key):
                vals.append(val)
        if len(vals) > 0:
            return vals
        elif key not in ('__getstate__', '__setstate__'):
            return FailValue()
        else:
            import pdb
            pdb.set_trace()  # NOQA

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        return self.__dict__.update(state)


class NodeField(Field):

    """Node field object.

    At first, two seperate fields(region and node) are tryied, but it turned
    out to be complex in case, so merged into one field.

    """

    _clsname = 'node'

    def __init__(self, ctx):
        super(NodeField, self).__init__(ctx)

    def _match(self, oval, fileo):
        if isinstance(oval._part, types.ListType):
            for part in oval._part:
                if part not in fileo.path:
                    return False
            return True
        else:
            return oval._part in fileo.path

    def _value(self, fileo):
        to_value = self._value_fn()
        return to_value(self, fileo)

    def _value_fn(self):
        try:
            return self._ctx.mod['get_node']
        except Exception:
            raise Exception(
                'get_node is not defined in context.')

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


def _file_head_or_tail(flike, head, count=10):
    tmp_file, _ = unique_tmp_path(TMP_PREFIX)
    with open(tmp_file, 'w') as out:
        c = 'head' if head else 'tail'
        cmd = [c, '-n', str(count), flike.abspath]
        check_call(cmd, stdout=out)

    cnt = get_line_count(tmp_file)
    if head:
        linfos = flike._linfos[0:cnt]
    else:
        linfos = flike._linfos[-cnt:]
    return TempFile(flike._ctx, tmp_file, linfos, True)


def find_in_fileo(ctx, _files, hsize, word, options=None, print_prog=True,
                  include_header=False):
    """Find word in file object and return result temp file."""
    _files = remove_empty_file(_files)
    if len(_files) == 0:
        return None

    def _write_header(_file, result_file):
        with open(_file.abspath, 'r') as ff:
            with open(result_file, 'w') as rf:
                header = ff.readline()
                rf.write(header)

    result_file, _ = unique_tmp_path(TMP_PREFIX)
    tmp_file, _ = unique_tmp_path(TMP_PREFIX)
    fileno = len(_files)
    ctx.pg = ProgressBar('finding in %s' % hsize, fileno)

    # for each target file
    if isinstance(word, types.ListType) or isinstance(word, types.TupleType):
        word = '\|'.join(word)
    linfos = LineInfo()
    for idx, _file in enumerate(_files):
        if include_header:
            _write_header(_file, result_file)
            include_header = False
        linfo = _find_in_fileo_grep(ctx, result_file, tmp_file, word, idx,
                                    _file, fileno, options, print_prog)
        if linfo is not None:
            linfos += linfo
    ctx.pg.done()
    os.unlink(tmp_file)
    tf = TempFile(ctx, result_file, linfos)
    return tf


def _find_in_fileo_grep(ctx, result_file, tmp_file, word, idx, _file, fileno,
                        options, print_prog):
    # grep to tmp
    found = True
    with open(tmp_file, 'w') as out:
        try:
            _find_in_fileo_grep_call(ctx, word, idx, _file, fileno,
                                     options, out, print_prog)
        except CalledProcessError:
            found = False

    # fill info with match count
    cnt = get_line_count(tmp_file)
    with open(result_file, 'a') as out:
        cmd = ['cat', tmp_file]
        check_call(cmd, stdout=out)
    if found:
        return LineInfo(LineInfoImpl_Count(_file.node, _file.kind,
                                           _file.date, _file, cnt))


def _normalize_options(options):
    """Normalize options.

    1. Make them iterable.
    2. Flatten nested list

    Returns
    -------
    list
        normalized options

    """
    try:
        iter(options)
    except TypeError:  # if not iterable
        options = [options]
    opts = []
    for option in options:
        if isinstance(option, types.ListType):
            opts += option
        elif isinstance(option, FileSelector):
            for _file in option:
                opts.append(_file.node)
                opts.append(_file.kind)
                opts.append(_file.date)
        else:
            opts.append(option)
    return opts


def _get_path_elms(fileo):
    _dir = os.path.dirname(fileo.path)
    path_elms = _dir.split('/')
    filename = os.path.basename(fileo.path)
    return path_elms, filename


def _test_a_file(fileo, ofield_vals):
    opasses = dict.fromkeys(ofield_vals.keys(), False)

    for ofield, ovals in ofield_vals.iteritems():
        _test_type_vals(ofield, fileo, ovals, opasses)

    for ofield in opasses.keys():
        if not opasses[ofield]:
            return False

    return True


def _test_type_vals(ofield, fileo, ovals, opasses):
    for oval in ovals:
        if oval._match(fileo):
            opasses[ofield] = True
            return


def _group_options(options, ofield_vals):
    for oval in options:
        ofield_vals[oval._field].append(oval)


class FileSelector(FileCommon, IFilterable, IMergeable):

    """File iterator."""

    def __init__(self, ctx, __files, _qmode, force_all=False):
        super(FileSelector, self).__init__(ctx)
        self.files = __files
        self._count = len(__files)
        self._qmode = _qmode
        self._force_all = force_all
        self._lcount = -1
        self._dates_cache = None
        self._kinds_cache = None
        self._nodes_cache = None

    def find(self, word, options=None, print_prog=True, include_header=False):
        """Find word among files.

        Returns
        -------
        TempFile
            Return result temp file instance.

        """
        return find_in_fileo(self._ctx, self.files, self.hsize, word, options,
                             print_prog, include_header)

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        global qmode
        qmode = self._qmode

        if isinstance(idx, int):
            return self.files[idx]
        else:
            if isinstance(idx, types.SliceType):
                idx = self.__getitem__expand_date_slice(idx)
            return self.__getitem__options(idx)

    def __getitem__expand_date_slice(self, idx):
        _idx = check_date_slice(self.dates, idx)
        if _idx.start != idx.start or idx.stop != idx.stop:
            idx = self.dates[_idx]
        return idx

    def __getitem__options(self, idx):
        options = None
        if isinstance(idx, Value) or isinstance(idx, FileSelector) or\
           isinstance(idx, FileValue):
            options = [idx]
        elif isinstance(idx, types.TupleType) or\
                isinstance(idx, types.ListType) or isinstance(idx, ValueList):
            options = list(idx)

        if options is not None:
            options = _normalize_options(options)
            ofield_vals = defaultdict(list)
            _group_options(options, ofield_vals)

            _files = []
            for fileo in self.files:
                if _test_a_file(fileo, ofield_vals):
                    _files.append(fileo)
            return FileSelector(self._ctx, _files, self._qmode)

    def __getslice__(self, idx1, idx2):
        return self._slice(slice(idx1, idx2))

    def _slice(self, slc):
        idx1, idx2 = get_slice_idx(slc, self.count)
        global qmode
        qmode = self._qmode
        assert len(self.files) > 0
        return FileSelector(self._ctx, self.files[idx1:idx2], self._qmode,
                            True)

    def __iter__(self):
        global qmode
        qmode = self._qmode
        return self

    @property
    def _linfos(self):
        return LineInfo([f._linfos for f in self.files])

    @property
    def _nodes(self):
        rv = []
        for _file in self.files:
            rv.append(_file.node)
        return rv

    @property
    def _kinds(self):
        rv = []
        for _file in self.files:
            rv.append(_file.kind)
        return rv

    @property
    def _dates(self):
        rv = []
        for _file in self.files:
            rv.append(_file.date)
        return rv

    @property
    def nodes(self):
        """Return unique node list for all files."""
        if self._nodes_cache is None:
            rv = ValueList(unique_list(self._nodes))
            self._nodes_cache = rv
        return self._nodes_cache

    @property
    def kinds(self):
        """Return unique kind list for all files."""
        if self._kinds_cache is None:
            rv = ValueList(unique_list(self._kinds))
            self._kinds_cache = rv
        return self._kinds_cache

    @property
    def dates(self):
        """Return unique date list for all files."""
        if self._dates_cache is None:
            rv = sorted(unique_list(self._dates),
                        key=lambda date: date.__repr__())
            rv = ValueList(rv)
            self._dates_cache = rv
        return self._dates_cache

    def _head_or_tail(self, head, count):
        if head:
            _files = self.files[:count]
        else:
            _files = self.files[-count:]
        return FileSelector(self._ctx, _files, self._qmode, True)

    def head(self, count=10):
        """Return top of file list like head command."""
        return self._head_or_tail(True, count)

    def tail(self, count=10):
        """Return bottom of file list like tail command."""
        return self._head_or_tail(False, count)

    def __unicode__(self):
        if (self.count == 0 or self.count > PRINT_LMAX) and not\
                self._force_all:
            return '%s\nfile count: %d\nsize: %s' % \
                (type(self), self.count, self.hsize)
        else:
            return '\n'.join([_file.path for _file in self.files])

    def merge(self):
        """Merge all files into a temp file and return it."""
        linfos = LineInfo()
        tmp_file, _ = unique_tmp_path(TMP_PREFIX)
        filecnt = len(self.files)
        fileno = 0
        pg = ProgressBar('merging', filecnt)

        with open(tmp_file, 'w') as out:
            for _file in self.files:
                pg.animate(fileno)
                cmd = ['cat', _file.abspath]
                check_call(cmd, stdout=out)
                linfos += _file._linfos
                fileno += 1
        pg.done()
        return TempFile(self._ctx, tmp_file, linfos)

    @property
    def noempty(self):
        """Filter non empty files and return them."""
        _files = [_file for _file in self.files if _file.size > 0]
        return FileSelector(self._ctx, _files, self._qmode)

    @property
    def lcount(self):
        """Return total line count of all files."""
        if self._lcount == -1:
            return self._calc_lcount()
        return self._lcount

    def _calc_lcount(self):
        count = 0
        for _file in self.files:
            count += _file.lcount
        return count

    @property
    def size(self):
        """Return total size of all files."""
        _size = 0
        for _file in self.files:
            _size += _file.size
        return _size

    @property
    def hsize(self):
        """Return human readable total size of all files."""
        _size = 0
        for _file in self.files:
            _size += _file.size
        return sizeof_fmt(_size)

    @property
    def link(self):
        """Return html link to all files for IPython notebook to download."""
        return HTML(self.link_str())

    def link_str(self, label=None):
        """Return URL to all files for IPython notebook to download."""
        rv = []
        data_url, _ = get_urls()
        for _file in self.files:
            _label = _file.path if label is None else label
            rv.append(data_url % (_file.path, _label))
        return '<br/>'.join(rv)

    @property
    def zlink(self):
        """Return html link to zipped file for IPython notebook to download."""
        tmp_file, _ = unique_tmp_path(TMP_PREFIX, '.zip')
        with zipfile.ZipFile(tmp_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for _file in self.files:
                _add_zip_flat(zf, _file.abspath)
        filename = tmp_file.split(os.path.sep)[-1]
        _, TMP_URL = get_urls()
        return HTML(TMP_URL % (filename, filename))

    def to_frame(self, usecols=None, chunk_cnt=CHUNK_CNT):
        """Convert to Pandas DataFrame and return it."""
        _c = self._to_frame_prop('to_frame', True)
        df = pd.concat(self._to_frame_gen(_c, usecols, chunk_cnt))
        _c.pg.done()
        return df

    def to_frame_hdf(self, store_path, store_key, df_cb=None, max_msg=None,
                     usecols=None, chunk_cnt=CHUNK_CNT):
        """Convert to Pandas DataFrame and save to HDF then returns
        HDFStore."""
        store = HDFStore(store_path, 'w')
        _c = self._to_frame_prop('to_frame_hdf', False)
        for df in self._to_frame_gen(_c, usecols, chunk_cnt):
            min_itemsize = {'kind': 20, 'msg': 255}
            # pytables not support unicode for now
            df['msg'] = df['msg'].apply(lambda m: m.encode('utf8'))
            if df_cb is not None:
                df_cb(df)
            if max_msg is not None:
                min_itemsize['msg'] = max_msg
            store.append(store_key, df, format='table',
                         min_itemsize=min_itemsize)
        store.flush()
        store.close()
        _c.pg.done()

    def _to_frame_prop(self, title, show_prog=True):
        c = Property()
        c.fileno = 0
        c.filecnt = len(self.files)
        c.files = self.files
        c.show_prog = show_prog
        c.pg = ProgressBar(title, c.filecnt)
        return c

    def _to_frame_gen(self, c, usecols, chunk_cnt):
        for _file in c.files:
            c.pg.animate(c.fileno)
            df = _file.to_frame(usecols, chunk_cnt, False)
            yield df
            c.fileno += 1

    def sample(self, ratio=0.1):
        cnt = self.count
        sampled = sorted(np.random.permutation(cnt)[:cnt * ratio])
        files = [self.files[idx] for idx in sampled]
        return FileSelector(self._ctx, files, self._qmode)


def _add_zip_flat(zf, abspath):
    odir = os.getcwd()
    _dir, filename = os.path.split(abspath)
    os.chdir(_dir)
    zf.write(filename, abspath)
    os.chdir(odir)


class Selector(Representable, Listable):

    """Selector."""

    def __init__(self, _files, fname):
        super(Selector, self).__init__()
        self._files = _files
        self._fname = fname
        self._fnames = fname + 's'

    def __getitem__(self, idx):
        if isinstance(idx, int):
            return getattr(self._files, self._fnames)[idx]
        elif self._fnames == 'dates' and isinstance(idx, types.SliceType):
            idx = check_date_slice(self._files.dates, idx)
            return ValueList(self._files.dates.__getitem__(idx))
        else:
            return getattr(self._files.__getitem__(idx), self._fnames)

    def __getslice__(self, idx1, idx2):
        return self._slice(slice(idx1, idx2))

    def _slice(self, slc):
        idx1, idx2 = get_slice_idx(slc, self.count)
        if isinstance(idx1, int) or isinstance(idx2, int):
            assert isinstance(idx1, int) and isinstance(idx2, int)
            return getattr(self._files, self._fnames)[idx1:idx2]
        else:
            return getattr(self._files.__getslice__(idx1, idx2), self._fname)

    def __unicode__(self):
        if self.count == 0 or self.count > PRINT_LMAX:
            return '%s\ncount: %d' % (type(self), self.count)
        else:
            return str(getattr(self._files, self._fnames))

    @property
    def count(self):
        """Return select files count."""
        return len(getattr(self._files, self._fnames))

    def group(self):
        """Return merged sub-items."""
        return set([i._supobj if i._supobj is not None else i for i in
                    getattr(self._files, self._fnames)])

    def __len__(self):
        return self.count


def _update_files_root_vals(fieldcnt, fields, fileo, field_getter):
    field_errs = []
    vals = set()
    for i in range(fieldcnt):
        fobj = fields[i]
        try:
            val = field_getter[i]()(fobj, fileo)
        except Exception as e:
            field_errs.append((str(fobj) + str(e)))
            logging.error(traceback.format_exc())
            break
        else:
            vals.add(val)
    return vals, field_errs


def _update_files_root(ctx, _root, filecnt, fileno, pg):
    root = _root[0]
    fields = ctx.fields.values()
    fieldcnt = len(fields)
    field_getter = [field._value_fn for field in fields]
    converted = []
    errs = []
    for filename in _root[1]:
        if FORWARDER_LOG_PREFIX in filename:
            continue
        pg.animate(fileno)
        abspath = os.path.join(root, filename)
        if ctx.encoding.startswith('utf-16'):
            convfile = get_convfile_path(abspath)
            if not os.path.isfile(convfile):
                convert_data_file(abspath, ctx.encoding, convfile)
                converted.append(abspath)
            abspath = convfile
        fileo = FileValue(ctx, abspath)

        vals, field_errs = _update_files_root_vals(fieldcnt, fields, fileo,
                                                   field_getter)
        if len(field_errs) > 0:
            errs += field_errs
            continue

        try:
            fileo._set_props(vals)
        except InvalidProp:
            pass
        else:
            ctx.files.append(fileo)
            fileno += 1
    if ctx.encoding.startswith('utf-16'):
        if len(converted) > 0:
            nprint("%d files have been converted." % len(converted))
    return fileno, errs


def _get_found_time(cpath):
    s = int(time.time() - os.path.getmtime(cpath))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return '%d hour %d minute' % (h, m)
    else:
        return '%d minute %d seconds' % (m, s)


def _get_cache_path(fmt):
    cache_dir = get_cache_dir()
    return os.path.join(cache_dir, '%s_found_files.pkl' % (fmt))


def _update_files_precalc(ctx, root_list):
    use_cache = cfg['use_cache'] if 'use_cache' in cfg else True
    cpath = _get_cache_path(ctx.logfmt)
    if use_cache and os.path.isfile(cpath):
        tstr = _get_found_time(cpath)
        msg = '\nusing file infos found %s ago.' % tstr
        with open(cpath, 'r') as f:
            root_list, filecnt = pickle.load(f)
            if filecnt > 0:
                return (root_list, filecnt), msg
    return find_files_and_save(ctx.startdir, ctx.logfmt, ctx.ffilter,
                               root_list), None


def _filter_fmt_files(filenames, filecnt, fmt, ffilter):
    if ffilter is None:
        # if no file filter is exist, use format name as file extension
        rfiles = []
        for filename in fnmatch.filter(filenames, ('*.' + fmt)):
            rfiles.append(filename)
            filecnt += 1
    else:
        rfiles, cnt = ffilter(filenames)
        filecnt += cnt
    return rfiles, filecnt


def find_files_and_save(startdir, fmt, ffilter, root_list=None):
    logging.debug('find_files_and_save')
    logging.debug('startdir: ' + str(startdir))
    if root_list is None:
        root_list = []
    use_cache = cfg['use_cache'] if 'use_cache' in cfg else True
    if use_cache:
        cpath = _get_cache_path(fmt)
    nprint('finding files and save info...')
    filecnt = 0
    assert os.path.isdir(startdir)
    for root, dirs, filenames in os.walk(startdir):
        dirs[:] = [d for d in dirs if d not in ('_var_',)]
        _root = [os.path.abspath(root), None]
        root_list.append(_root)
        rfiles, filecnt = _filter_fmt_files(filenames, filecnt, fmt, ffilter)
        rfiles = sorted(rfiles)
        _root[1] = rfiles
    rv = sorted(root_list), filecnt
    if use_cache:
        with open(cpath, 'w') as f:
            pickle.dump(rv, f)
    return rv


def _update_files(ctx):
    root_list = []
    rv, msg = _update_files_precalc(ctx, root_list)
    root_list, filecnt = rv

    fileno = 0
    pg = ProgressBar('collecting file info', filecnt)
    errors = []
    for _root in root_list:
        fileno, errs = _update_files_root(ctx, _root, filecnt, fileno, pg)
        if len(errs) > 0:
            errors += errs
    pg.done()

    for _, fobj in ctx.fields.iteritems():
        fobj._file_filter()

    if msg is not None:
        nprint(msg)
    if len(errors) > 0:
        nprint(errors[-4:])


class KindField(Field):

    """Kind field object."""

    _clsname = 'kind'

    def __init__(self, ctx):
        super(KindField, self).__init__(ctx)

    def _match(self, oval, fileo):
        return oval._part in fileo.filename

    def _value(self, fileo):
        to_value = self._value_fn()
        return to_value(self, fileo)

    def _value_fn(self):
        return _get_member(self._ctx, 'get_kind')

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


class DateField(Field):

    """Date field object."""

    _clsname = 'date'

    def __init__(self, ctx):
        super(DateField, self).__init__(ctx)

    def _match(self, oval, fileo):
        return oval._part in fileo.filename

    def _value(self, fileo):
        to_datevalue = self._value_fn()
        try:
            return to_datevalue(self, fileo)
        except Exception as e:
            logging.error(str(e))
            logging.error(traceback.format_exc())
            raise

    def _value_fn(self):
        return _get_member(self._ctx, 'get_date')

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


def set_grep_encoding(ctx, word):
    if ctx.encoding and isinstance(word, types.UnicodeType):
        os.environ['LC_ALL'] = ctx.encoding
        return word.encode(ctx.encoding)
    else:
        os.environ['LC_ALL'] = 'C'
    return word


def unset_grep_encoding(ctx, word):
    if ctx.encoding and isinstance(word, types.UnicodeType):
        del os.environ['LC_ALL']


def get_grep(word):
    return 'grep' if '\|' in word else 'fgrep'


def _find_in_temp_grep(ctx, result_file, tempo, word, _options,
                       include_header):
    word = set_grep_encoding(ctx, word)

    tmp_file, _ = unique_tmp_path(TMP_PREFIX)
    # grep to tmp with linenum
    with open(tmp_file, 'w') as out:
        if include_header:
            header = open(tempo.abspath, 'r').readline()
            out.write(header)
        try:
            options = ['-h', '-n']
            if _options is not None:
                options += _options.split()
            cmd = [get_grep(word)] + options + [word, tempo.abspath]
            check_call(cmd, stdout=out)
        except CalledProcessError:
            pass

    unset_grep_encoding(ctx, word)

    # fill infos, and write to result
    return _find_in_temp_grep_write(result_file, tempo, tmp_file)


def _find_in_temp_grep_write(result_file, tempo, tmp_file):
    nodes = []
    kinds = []
    dates = []
    files = []
    linenos = []

    # collect line numbers and write line
    with open(result_file, 'w') as out:
        for line in open(tmp_file):
            cols = line.split(':')
            try:
                lineno = int(cols[0]) - 1
            except ValueError:
                pass
            else:
                linebody = ':'.join(cols[1:])
                linenos.append(lineno)
                out.write(linebody)

    lineno_gen = (lineno for lineno in linenos)
    impl_gen = (impl for impl in tempo._linfos.impls)
    try:
        lineno = lineno_gen.next()
        impl = impl_gen.next()
    except StopIteration:
        pass
    else:
        off = 0
        # find line infos
        while True:
            try:
                if impl.count > lineno:
                    node, kind, date, _file = impl[lineno]
                    nodes.append(node)
                    kinds.append(kind)
                    dates.append(date)
                    files.append(_file)
                    lineno = lineno_gen.next() - off
                else:
                    off += impl.count
                    lineno -= impl.count
                    impl = impl_gen.next()
            except StopIteration:
                break
    return LineInfo(LineInfoImpl_Array(nodes, kinds, dates, files))


def _find_in_fileo_grep_call(ctx, word, idx, _file, fileno, _options, out,
                             print_prog):
    if print_prog:
        ctx.pg.animate(idx)

    word = set_grep_encoding(ctx, word)

    options = ['-h']
    if _options is not None:
        options += _options.split()
    cmd = [get_grep(word)] + options + [word, _file.abspath]
    check_call(cmd, stdout=out)

    unset_grep_encoding(ctx, word)


def _remove_old():
    nprint('deleting old files...')
    cfg = make_config()
    tmp_dir = get_tmp_dir()
    remove_old_tmps(tmp_dir, TMP_PREFIX, cfg["tmp_valid_hour"])
    remove_old_tmps(tmp_dir, NAMED_TMP_PREFIX, cfg["named_tmp_valid_hour"])


def update(mod, fmt, subtype=None, ffilter=None):
    """Update file information.

    Remove temp files when necessary.

    """
    _remove_old()

    cfg = make_config()
    _dir = cfg['data_dir']
    encoding = cfg["data_encoding"]
    if subtype is not None and isinstance(encoding, dict):
        encoding = encoding[subtype]

    if encoding.startswith('utf-16'):
        _dir = get_conv_dir()
    ctx = Context(mod, _dir, encoding, fmt, ffilter)
    date = DateField(ctx)
    kind = KindField(ctx)
    node = NodeField(ctx)

    # collect files
    _update_files(ctx)
    if encoding.startswith('utf-16'):
        ctx.encoding = 'utf-8'
    ctx.updated = time.time()
    return ctx, date, kind, node


class FileField(Field):

    """File field."""

    def __init__(self, ctx):
        super(FileField, self).__init__(ctx)

    def _match(self, oval, fileo):
        node = str(oval.node) == str(fileo.node)
        kind = str(oval.kind) == str(fileo.kind)
        date = oval.date._match(fileo)
        val = node and kind and date
        return val

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


class FileValue(SingleFile):

    """File value."""

    def __init__(self, ctx, abspath):
        """Initialize file value.

        Parameters
        ----------
        ctx : Context
            Search domain context.
        abspath : string
            Absolute path to the file.
        fields : field list
            Domain fields collected in updating.

        """
        super(FileValue, self).__init__(ctx, None)
        self._abspath = abspath
        self._filename = os.path.basename(abspath)
        self._field = file_field
        self._linfos = None

    @property
    def cols(self):
        get_cols = _get_member(self._ctx, 'get_cols', False)
        if get_cols is not None:
            return get_cols(self._abspath)

    def _set_props(self, vals):
        for val in vals:
            if val:
                name = val._field._clsname
                setattr(self, name, val)
            else:
                raise InvalidProp()
        self._linfos = LineInfo(LineInfoImpl_Count(self.node, self.kind,
                                                   self.date, self,
                                                   self._calc_lcount))

    def __unicode__(self):
        if qmode == 'file':
            return self.path
        elif qmode == 'node':
            return str(self.nodes[0])
        elif qmode == 'kind':
            return str(self.kinds[0])
        elif qmode == 'date':
            return str(self.dates[0])
        return unicode(type(self))

    def _match(self, fileo):
        return file_field._match(self, fileo)

    def find(self, word, options=None, print_prog=True, include_header=False):
        """Find word in file.

        Parameters
        ----------
        word : string
            Word to find.
        options : string
            grep options

        Returns
        -------
        TempFile
            Result temp file.

        """
        return find_in_fileo(self._ctx, [self], self.hsize, word, options,
                             print_prog, include_header)


class Slot(object):

    def __init__(self, ctx, path):
        self.ctx = ctx
        self.path = path

    @property
    def exist(self):
        return os.path.isfile(self.path)

    def load_tmp(self):
        """Load from slot path as TempFile."""
        # load line info
        linfos = LineInfo.load(self.path, self.ctx)
        return TempFile(self.ctx, self.path, linfos)

    def remove(self):
        os.remove(self.path)
        self._linfos.remove_saved(self.path)


class SlotMap(object):

    def __init__(self, ctx):
        self.ctx = ctx
        self._dict = dict()

    def __call__(self, userid, slotname):
        key = userid, slotname
        if key not in self._dict:
            name = NAMED_TMP_PREFIX + userid + '-' + slotname
            tmp_dir = get_tmp_dir()
            self._dict[key] = Slot(self.ctx, os.path.join(tmp_dir, name))
        return self._dict[key]


pctx = Property()
pctx.fields = {}
file_field = FileField(pctx)
