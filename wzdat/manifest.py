# -*- coding: utf-8 -*-
"""Manifest for Dashboard Notebook."""

import os
import ast
import time
import logging
from datetime import datetime
import copy
import json

from nbformat import write, read

from wzdat.util import Property, get_notebook_rpath, get_notebook_dir,\
    dataframe_checksum, HDF, convert_server_time_to_client, sizeof_fmt,\
    get_notebook_manifest_path
from wzdat.notebook_runner import NotebookRunner, NotebookError
from wzdat.const import HDF_CHKSUM_FMT, IPYNB_VER


class RecursiveReference(Exception):
    pass


class ManifestNotExist(Exception):
    pass


class HDFStore(object):
    def __init__(self, manifest, uname, sname):
        self.manifest = manifest
        self.uname = uname
        self.sname = sname

    def select(self, where=None, start=None, stop=None, columns=None,
               iterator=False, chunksize=None, auto_close=False, **kwargs):
        # logging.debug("HDFStore {}/{} select".format(self.uname, self.sname))
        with HDF(self.uname) as hdf:
            # chksum = hdf.store[HDF_CHKSUM_FMT.format(self.sname)][0]
            # self.manifest._dep_hdf_chksum = chksum
            return hdf.store.select(self.sname, where=where, start=start,
                                    stop=stop, columns=columns,
                                    iterator=iterator, chunksize=chunksize,
                                    auto_close=auto_close, **kwargs)

    def put(self, value, aformat=None, columns=None, dropna=None, **kwargs):
        # logging.debug("HDFStore put {}/{}".format(self.uname, self.sname))
        self._append(value, aformat=aformat, append=False, columns=columns,
                     dropna=dropna, **kwargs)

    def append(self, value, aformat=None, columns=None, dropna=None, **kwargs):
        # logging.debug("HDFStore append {}/{}".format(self.uname, self.sname))
        self._append(value, aformat=aformat, append=True, columns=columns,
                     dropna=dropna, **kwargs)

    def _append(self, value, aformat, append, columns, dropna, **kwargs):
        import pandas as pd
        ktuple = tuple(((k, v if isinstance(v, tuple) else tuple(v)) for k,
                        v in kwargs.items()))
        _args = (dataframe_checksum(value), self.uname, self.sname, aformat,
                 append, columns, dropna, ktuple)
        # escape None for hashing
        args = map(lambda x: 0 if x is None else x, _args)
        chksum = hash(tuple(args))
        # logging.debug('  chksum: {}'.format(chksum))
        self.manifest._out_hdf_chksum = chksum
        logging.debug("HDFStore append - _out_hdf_chksum {}".format(chksum))
        with HDF(self.uname) as hdf:
            hdf.store.append(self.sname, value, aformat, append, columns,
                             dropna, **kwargs)
            # write checksum
            hsname = HDF_CHKSUM_FMT.format(self.sname)
            if append:
                pchksum = int(hdf.store[hsname])
                chksum = pchksum + chksum
            hdf.store.append(hsname, pd.Series([str(chksum)]), append=False)

    def checksum(self):
        with HDF(self.uname) as hdf:
            try:
                return int(hdf.store[HDF_CHKSUM_FMT.format(self.sname)])
            except KeyError, e:
                logging.warning("HDFStore - checksum: {}".format(e))
                return 0


class Manifest(Property):
    def __init__(self, check_depends=True, explicit_nbpath=None):
        super(Manifest, self).__init__()

        if explicit_nbpath is None:
            nbdir = get_notebook_dir()
            nbrpath = get_notebook_rpath()
            self._nbapath = os.path.join(nbdir, nbrpath)
            self._path = os.path.join(nbdir,
                                      get_notebook_manifest_path(nbrpath))
        else:
            self._nbapath = explicit_nbpath
            self._path = get_notebook_manifest_path(explicit_nbpath)
        # logging.debug(u"Manifest __init__ for {}".format(self._nbapath))

        if not os.path.isfile(self._path.encode('utf8')):
            raise ManifestNotExist()
        self._init_checksum(check_depends)
        logging.debug("Manifest __init__ done")

    def _init_checksum(self, check_depends):
        logging.debug("_init_checksum")
        self._prev_files_chksum = self._prev_hdf_chksum = \
            self._dep_files_chksum = self._dep_hdf_chksum = \
            self._out_hdf_chksum = None
        try:
            data = self._read_manifest()
        except SyntaxError, e:
            # if user input has error, update manifest to write message
            self._write_manifest_error(str(e))
            raise

        self._data = data
        for k, v in data.iteritems():
            self.__dict__['dict'][k] = v

        if 'output' in data:
            self._init_output(data['output'])

        if 'depends' in data:
            if check_depends:
                self._load_depends(data['depends'])
            for owner, sname in self._iter_depend_hdf():
                self._check_recursive_refer(owner, sname)

        _dict = self.__dict__['dict']
        if 'depends' in _dict and check_depends:
            self._chksum_depends(_dict['depends'])

    def _write_manifest_error(self, err):
        logging.debug(u"_write_manifest_error {}".format(err))
        with open(self._path.encode('utf8'), 'r') as f:
            nbdata = json.loads(f.read())
            cells = nbdata['cells']
            errdt = {
                "metadata": {},
                "output_type": "pyout",
                "prompt_number": 1,
                "text": "Manifest Error: {}".format(err)
            }
            cells[0]['outputs'] = [errdt]
        with open(self._path.encode('utf8'), 'w') as f:
            f.write(json.dumps(nbdata))

    def _iter_depend_hdf(self):
        if 'depends' in self._data and 'hdf' in self._data['depends']:
            hdf = self._data['depends']['hdf']
            if type(hdf[0]) is list:
                for ahdf in hdf:
                    yield ahdf
            else:
                yield hdf

    def _chksum_depends(self, depends):
        if 'files' in depends:
            if type(depends.files) is not list:
                self._dep_files_chksum = depends.files.checksum()
            else:
                self._dep_files_chksum = [files.checksum() for files in
                                          depends.files]
        if 'hdf' in depends:
            if type(depends.hdf) is not list:
                self._dep_hdf_chksum = depends.hdf.checksum()
            else:
                self._dep_hdf_chksum = [hdf.checksum() for hdf in depends.hdf]

    @property
    def _depend_files_changed(self):
        return self._prev_files_chksum != self._dep_files_chksum

    @property
    def _depend_hdf_changed(self):
        chksum = self._dep_hdf_chksum
        pchksum = self._prev_hdf_chksum
        logging.debug("_depend_hdf_changed pchecksum {}, checksum "
                      "{}".format(pchksum, chksum))
        if type(chksum) is not type(pchksum):
            return True

        if isinstance(chksum, list):
            for i, chk in enumerate(chksum):
                if pchksum[i] != chksum[i]:
                    return True
            return False
        else:
            return self._prev_hdf_chksum != self._dep_hdf_chksum

    @property
    def _need_run(self):
        '''Test if notebook should be run.'''
        # test output validity
        if 'output' in self._data and 'hdf' in self._data['output']:
            uname, _sname = self._data['output']['hdf']
            sname = '/' + _sname
            with HDF(uname) as hdf:
                hskeys = hdf.store.keys()
            output_invalid = sname not in hskeys
        else:
            output_invalid = False

        need = self._depend_files_changed or self._depend_hdf_changed or\
            output_invalid
        logging.debug(u"{} _need_run {} - files changed: {}, hdf changed: {}".
                      format(self._path, need, self._depend_files_changed,
                             self._depend_hdf_changed))
        return need

    def _check_output_hdf(self):
        """Check whether output hdf has been writtin"""
        if 'output' in self._data and 'hdf' in self._data['output']:
            if self._out_hdf_chksum is None:
                raise NotebookError("Output HDF has not been written.")

    def _write_result(self, elapsed, max_mem, err):
        logging.debug(u'_write_result {}'.format(self._path))
        nb = read(open(self._path.encode('utf-8')), IPYNB_VER)
        nr = NotebookRunner(nb)
        try:
            nr.run_notebook()
        except NotebookError, e:
            merr = unicode(e)
            logging.error(merr)
        else:
            write(nr.nb, open(self._path.encode('utf-8'), 'w'), IPYNB_VER)

        # clear surplus info
        first_cell = nr.nb['cells'][0]
        first_cell['outputs'] = []
        del nr.nb['cells'][1:]

        last_run = datetime.fromtimestamp(time.time())
        last_run = convert_server_time_to_client(last_run)
        last_run = last_run.strftime("%Y-%m-%d %H:%M:%S")
        body = ["    'last_run': '{}'".format(last_run)]
        body.append("    'elapsed': '{}'".format(elapsed))
        body.append("    'max_memory': '{}'".format(sizeof_fmt(max_mem)))
        body.append("    'error': {}".format('None' if err is None else
                                             json.dumps(err)))
        if self._dep_files_chksum is not None or\
                self._dep_hdf_chksum is not None:
            self._write_result_depends(body)

        if self._out_hdf_chksum is not None:  # could be multiple outputs
            coutput = "        'hdf': {}".format(self._out_hdf_chksum)
            body.append("    'output': {{\n{}\n    }}".format(coutput))

        if len(body) > 0:
            newcell = copy.deepcopy(nr.nb['cells'][0])
            cs_body = "# WARNING: Generated results. Do Not Edit.\n{{\n{}\n}}".\
                format(',\n'.join(body))
            newcell['source'] = cs_body
            nr.nb['cells'].append(newcell)

        write(nr.nb, open(self._path.encode('utf-8'), 'w'), IPYNB_VER)

    def _write_result_depends(self, body):
        cdepends = []
        if self._dep_files_chksum is not None:
            cdepends.append("        'files': {}".
                            format(self._dep_files_chksum))
        if self._dep_hdf_chksum is not None:
            cdepends.append("        'hdf': {}".
                            format(self._dep_hdf_chksum))
        if len(cdepends) > 0:
            body.append("    'depends': {{\n{}\n    }}".
                        format(',\n'.join(cdepends)))

    def _read_manifest(self):
        with open(self._path.encode('utf8'), 'r') as f:
            nbdata = json.loads(f.read())
            cells = nbdata['cells']
            for i, cell in enumerate(cells):
                # user cell
                if i == 0:
                    try:
                        mdata = ast.literal_eval(''.join(cell['source']))
                    except SyntaxError, e:
                        logging.error(u"_read_manifest - {}".format(str(e)))
                        raise
                # generated checksum cell
                elif i == 1:
                    try:
                        chksum = ast.literal_eval(''.join(cell['source']))
                        self._read_manifest_user_chksum_cell(chksum)
                    except SyntaxError:
                        pass
        return mdata

    def _read_manifest_user_cell(self, cell):
        if 'outputs' in cell:
            try:
                return cell['outputs'][0]['text']
            except IndexError:
                pass

    def _read_manifest_user_chksum_cell(self, data):
        if 'last_run' in data:
            self.last_run = datetime.strptime(data['last_run'], '%Y-%m-%d '
                                              '%H:%M:%S')

        if 'depends' in data:
            depends = data['depends']
            if 'files' in depends:
                self._prev_files_chksum = depends['files']
            if 'hdf' in depends:
                self._prev_hdf_chksum = depends['hdf']

    def _parse_depends_files(self, files):
        melm = files[0].split('.')
        frm = '.'.join(melm[:-1])
        mod = melm[-1]
        dates = files[1]
        return frm, mod, dates

    def _load_depends(self, data):
        _dict = self.__dict__['dict']
        prop = Property()
        _dict['depends'] = prop

        if 'files' in data:
            self._load_files_depends(data, _dict)

        if 'hdf' in data:
            self._load_hdf_depends(data, prop)

    def _load_files_depends(self, data, _dict):
        ftype = type(data['files'][0])
        if ftype is not list:
            files = data['files']
            frm, mod, dates = self._parse_depends_files(files)
            cmd = 'from {} import {} as _; _.load_info(); depends.files ='\
                '_.files[_.dates[-{}:]]; _ = None'.format(frm, mod, dates)
            exec(cmd, globals(), _dict)
        else:
            exec('depends.files = []', globals(), _dict)
            for files in data['files']:
                frm, mod, dates = self._parse_depends_files(files)
                cmd = 'from {} import {} as _; _.load_info(); depends.files.'\
                    'append(_.files[_.dates[-{}:]]); _ = None'.\
                    format(frm, mod, dates)
                exec(cmd, globals(), _dict)

    def _load_hdf_depends(self, data, prop):
        self._init_hdf_store(data, prop)

    def _init_hdf_store(self, data, prop):
        ftype = type(data['hdf'][0])
        if ftype is not list:
            uname, sname = data['hdf']
            prop.hdf = HDFStore(self, uname, sname)
        else:
            hdfs = []
            for uname, sname in data['hdf']:
                hdfs.append(HDFStore(self, uname, sname))
            prop.hdf = hdfs

    def _check_recursive_refer(self, owner, sname):
        if 'output' in self._data and 'hdf' in self._data['output']:
            hdf = self._data['output']['hdf']
            if hdf[0] == owner and hdf[1] == sname:
                raise RecursiveReference

    def _init_output(self, output):
        _dict = self.__dict__['dict']
        prop = Property()
        _dict['output'] = prop
        self._init_hdf_store(output, prop)

    def _on_output_set(self, attr, val):
        if self._readonly:
            logging.error("Can't set {} attribute {}".format(attr, val))
            raise AttributeError("Can't set attribute")
