import os
import types
import pickle

from wzdat.base import IListable
from wzdat.util import unique_list, get_slice_idx, normalize_idx
from wzdat.const import SAVE_INFO_EXT


class LineInfo(IListable):
    def __init__(self, impl=None):
        self._at = -1
        if impl is not None:
            if isinstance(impl, ILineInfoImpl):
                self.impls = [impl]
            else:
                self.impls = list(impl)
        else:
            self.impls = []

    def __iadd__(self, o):
        self.impls += o.impls
        return self

    def __len__(self):
        lcnt = 0
        for impl in self.impls:
            lcnt += impl.count
        return lcnt

    def __iter__(self):
        self._at = -1
        self._iat = 0
        self._impl = self.impls[self._iat]
        self._impl.__iter__()
        return self

    def next(self):
        """Return next file for iteration."""
        self._at += 1
        if self._impl.count <= self._at:
            self._iat += 1
            if len(self.impls) <= self._iat:
                raise StopIteration()
            self._impl = self.impls[self._iat]
            self._impl.__iter__()
            self._at = -1
        return self._impl.next()

    @property
    def count(self):
        return len(self)

    def add_impl(self, impl):
        self.impls.append(impl)

    @property
    def nodes(self):
        rv = []
        for impl in self.impls:
            rv += impl.nodes
        return rv

    @property
    def servers(self):
        rv = []
        for impl in self.impls:
            rv += impl.servers
        return rv

    @property
    def dates(self):
        rv = []
        for impl in self.impls:
            rv += impl.dates
        return rv

    @property
    def files(self):
        rv = []
        for impl in self.impls:
            rv += impl.files
        return rv

    @property
    def unique_nodes(self):
        return unique_list(self.nodes)

    @property
    def unique_servers(self):
        return unique_list(self.servers)

    @property
    def unique_dates(self):
        return unique_list(self.dates)

    @property
    def unique_files(self):
        return unique_list(self.files)

    def __getslice__(self, idx1, idx2):
        return self._slice(slice(idx1, idx2))

    def __getitem__(self, idx):
        if isinstance(idx, types.SliceType):
            return self._slice(idx)
        idx = normalize_idx(idx, self.count)
        if self.impls:
            at = 0
            impl = self.impls[at]
            while impl.count <= idx:
                at += 1
                idx -= impl.count
                impl = self.impls[at]
            return impl[idx]
        raise IndexError

    def _slice(self, slc):
        idx1, idx2 = get_slice_idx(slc, self.count)
        impls = []
        for impl in self.impls:
            if impl.count <= idx1:
                idx1 -= impl.count
                idx2 -= impl.count
                continue

            if impl.count <= idx2:
                impls.append(impl[idx1:])
                idx1 = 0
                idx2 -= impl.count - idx1
            else:
                impls.append(impl[idx1:idx2])
                break
        return LineInfo(impls)

    def save(self, tmppath):
        sinfopath = tmppath + SAVE_INFO_EXT
        with open(sinfopath, 'w') as f:
            sinfo = LineInfo_SInfo(self)
            pickle.dump(sinfo, f, 2)

    def remove_saved(self, tmppath):
        sinfopath = tmppath + SAVE_INFO_EXT
        if os.path.isfile(sinfopath):
            os.remove(sinfopath)

    @classmethod
    def load(self, tmppath, ctx):
        sinfopath = tmppath + SAVE_INFO_EXT
        with open(sinfopath, 'r') as f:
            sinfo = pickle.load(f)
            impls = []
            for impl_info in sinfo.impl_infos:
                impl = impl_info.make_impl(ctx)
                impls.append(impl)
        return LineInfo(impls)

    def __eq__(self, o):
        if self.count != o.count:
            return False
        cnt = len(self.impls)
        if cnt != len(o.impls):
            return False
        for i in range(cnt):
            if self.impls[i] != o.impls[i]:
                return False
        return True

    def __ne__(self, o):
        return not self.__eq__(o)


class LineInfo_SInfo(object):
    """Serialize Info"""
    def __init__(self, linfo):
        infos = []
        for impl in linfo.impls:
            infos.append(impl.make_sinfo())
        self.impl_infos = infos

    def __setstate__(self, state):
        self.__dict__.update(state)


class ILineInfoImpl(IListable):
    @property
    def __len__(self):
        raise NotImplementedError("Not implemented")

    @property
    def count(self):
        raise NotImplementedError("Not implemented")

    @property
    def nodes(self):
        raise NotImplementedError("Not implemented")

    @property
    def servers(self):
        raise NotImplementedError("Not implemented")

    @property
    def dates(self):
        raise NotImplementedError("Not implemented")

    @property
    def files(self):
        raise NotImplementedError("Not implemented")

    @property
    def unique_nodes(self):
        raise NotImplementedError("Not implemented")

    @property
    def unique_servers(self):
        raise NotImplementedError("Not implemented")

    @property
    def unique_dates(self):
        raise NotImplementedError("Not implemented")

    @property
    def unique_files(self):
        raise NotImplementedError("Not implemented")

    @property
    def save(self):
        raise NotImplementedError("Not implemented")

    def __eq__(self, o):
        return False


class ILineInfoImpl_SInfo(object):
    def __init__(self, impl):
        self.impl_class = impl.__class__

    def make_impl(self, ctx):
        # find unique values TODO: Optimize
        nvals = {f.node for f in ctx.files}
        svals = {f.server for f in ctx.files}
        dvals = {f.date for f in ctx.files}
        return self.impl_class, nvals, svals, dvals


class LineInfoImpl_Count(ILineInfoImpl):
    def __init__(self, node, server, date, _file, count):
        self.node = node
        self.server = server
        self.date = date
        self._file = _file
        self._count = count

    def __len__(self):
        return self.count

    def __iter__(self):
        self._at = -1
        return self

    def next(self):
        """Return next file for iteration."""
        self._at += 1
        if self.count > self._at:
            return self.node, self.server, self.date, self._file
        else:
            self._at = -1
            raise StopIteration()

    def __getitem__(self, idx):
        if isinstance(idx, types.SliceType):
            return self._slice(idx)
        idx = normalize_idx(idx, self.count)
        if self.count > idx:
            return self.node, self.server, self.date, self._file
        raise IndexError

    def __getslice__(self, idx1, idx2):
        return self._slice(slice(idx1, idx2))

    def _slice(self, slc):
        idx1, idx2 = get_slice_idx(slc, self.count)
        cnt = idx2 - idx1
        return LineInfoImpl_Count(self.node, self.server, self.date,
                                  self._file, cnt)

    @property
    def count(self):
        if isinstance(self._count, types.IntType):
            return self._count
        else:
            return self._count()

    @property
    def nodes(self):
        return (self.node,) * self.count

    @property
    def servers(self):
        return (self.server,) * self.count

    @property
    def dates(self):
        return (self.date,) * self.count

    @property
    def files(self):
        return (self._file,) * self.count

    @property
    def unique_nodes(self):
        return (self.node,)

    @property
    def unique_servers(self):
        return (self.server,)

    @property
    def unique_dates(self):
        return (self.dates,)

    @property
    def unique_files(self):
        return (self.files,)

    def make_sinfo(self):
        return LineInfoImpl_Count_SInfo(self)

    def __eq__(self, o):
        return self.node == o.node and self.server == o.server and self.date \
            == o.date and self.count == o.count

    def __ne__(self, o):
        return not self.__eq__(o)


class LineInfoImpl_Count_SInfo(ILineInfoImpl_SInfo):
    def __init__(self, impl):
        super(LineInfoImpl_Count_SInfo, self).__init__(impl)
        self.node = impl.node._repr
        self.server = impl.server._part
        self.date = impl.date._sdate
        self._file = impl._file.path
        self.count = impl.count

    def __setstate__(self, state):
        self.__dict__.update(state)

    def _find_real_value(self, sv, vals, attr):
        for nv in vals:
            if getattr(nv, attr) == sv:
                return nv

    def make_impl(self, ctx):
        impl_cls, nvals, svals, dvals = super(LineInfoImpl_Count_SInfo,
                                              self).make_impl(ctx)
        # find real value by _part
        node = self._find_real_value(self.node, nvals, '_repr')
        server = self._find_real_value(self.server, svals, '_part')
        date = self._find_real_value(self.date, dvals, '_sdate')
        _file = self._find_real_value(self._file, ctx.files, 'path')
        return impl_cls(node, server, date, _file, self.count)


class LineInfoImpl_Array(ILineInfoImpl):
    def __init__(self, nodes, servers, dates, files):
        self._nodes = nodes
        self._servers = servers
        self._dates = dates
        self._files = files
        nodecnt = len(nodes)
        servercnt = len(servers)
        datecnt = len(dates)
        filecnt = len(files)
        assert(nodecnt == servercnt and servercnt == datecnt and datecnt ==
               filecnt)

    @property
    def count(self):
        return len(self._nodes)

    def __iter__(self):
        self._at = -1
        return self

    def __getslice__(self, idx1, idx2):
        return self._slice(slice(idx1, idx2))

    def _slice(self, slc):
        return LineInfoImpl_Array(self._nodes[slc], self._servers[slc],
                                  self._dates[slc], self._files[slc])

    def next(self):
        """Return next file for iteration."""
        self._at += 1
        if self.count > self._at:
            i = self._at
            return self._nodes[i], self._servers[i], self._dates[i], \
                self._files[i]
        else:
            self._at = -1
            raise StopIteration()

    def __getitem__(self, idx):
        if isinstance(idx, types.SliceType):
            raise NotImplementedError("Not implemented")
        idx = normalize_idx(idx, self.count)
        if self.count > idx:
            return self._nodes[idx], self._servers[idx], self._dates[idx], \
                self._files[idx]
        raise IndexError

    @property
    def nodes(self):
        return self._nodes

    @property
    def servers(self):
        return self._servers

    @property
    def dates(self):
        return self._dates

    @property
    def files(self):
        return self._files

    @property
    def unique_nodes(self):
        return unique_list(self.nodes)

    @property
    def unique_servers(self):
        return unique_list(self.servers)

    @property
    def unique_dates(self):
        return unique_list(self.files)

    @property
    def unique_files(self):
        return unique_list(self.files)

    def make_sinfo(self):
        return LineInfoImpl_Array_SInfo(self)

    def __eq__(self, o):
        return self.nodes == o.nodes and self.servers == o.servers and \
            self.dates == o.dates

    def __ne__(self, o):
        return not self.__eq__(o)


class LineInfoImpl_Array_SInfo(ILineInfoImpl_SInfo):
    def __init__(self, impl):
        super(LineInfoImpl_Array_SInfo, self).__init__(impl)
        # save string representation of unique values
        unodes = [node._repr for node in impl.unique_nodes]
        uservers = [server._part for server in impl.unique_servers]
        udates = [date._sdate for date in impl.unique_dates]
        ufiles = [_file.path for _file in impl.unique_files]

        nodeidx = {name: idx for idx, name in enumerate(unodes)}
        serveridx = {name: idx for idx, name in enumerate(uservers)}
        dateidx = {name: idx for idx, name in enumerate(udates)}
        fileidx = {name: idx for idx, name in enumerate(ufiles)}

        self.nodes = [nodeidx[node._repr] for node in impl.nodes]
        self.servers = [serveridx[server._part] for server in impl.servers]
        self.dates = [dateidx[date._sdate] for date in impl.dates]
        self.files = [fileidx[_file.path] for _file in impl.files]

        self.nodemap = {idx: name for idx, name in enumerate(unodes)}
        self.servermap = {idx: name for idx, name in enumerate(uservers)}
        self.datemap = {idx: name for idx, name in enumerate(udates)}
        self.filemap = {idx: name for idx, name in enumerate(ufiles)}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def _find_real_values(self, _map, _indexes, nvals, attr):
        newmap = {}
        for nv in nvals:
            for idx in _map:
                if getattr(nv, attr) == _map[idx]:
                    newmap[idx] = nv
        return [newmap[idx] for idx in _indexes]

    def make_impl(self, ctx):
        impl_cls, nvals, svals, dvals = super(LineInfoImpl_Array.SInfo,
                                              self).make_impl(ctx)
        # find real value by _part
        nodes = self._find_real_values(self.nodemap, self.nodes, nvals,
                                       '_repr')
        servers = self._find_real_values(self.servermap, self.servers, svals,
                                         '_part')
        dates = self._find_real_values(self.datemap, self.dates, dvals,
                                       '_sdate')
        files = self._find_real_values(self.filemap, self.files,
                                       ctx.files, 'path')
        return impl_cls(nodes, servers, dates, files)
