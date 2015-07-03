# -*- coding: utf-8 -*-
"""Manifest for Dashboard Notebook."""

import os
import ast
import time
import logging
from datetime import datetime

from wzdat.util import Property, get_notebook_path, get_notebook_dir,\
    dataframe_checksum, HDF, ScbProperty, convert_server_time_to_client


class NoHDFWritten(Exception):
    pass


class RecursiveReference(Exception):
    pass


class Manifest(Property):
    def __init__(self, write=True, check_depends=True, explicit_nbpath=None):
        super(Manifest, self).__init__()
        self._write = write

        if explicit_nbpath is None:
            nbdir = get_notebook_dir()
            nbrpath = get_notebook_path()
            self._path = os.path.join(nbdir,
                                      nbrpath.replace('.ipynb',
                                                      '.manifest.ipynb'))
        else:
            self._path = explicit_nbpath.replace('.ipynb', '.manifest.ipynb')
        logging.debug(u"Manifest __init__ for {}".format(self._path))

        assert os.path.isfile(self._path), "Manifest file '{}' not "\
            "exist.".format(self._path)

        self._prev_files_chksum = self._prev_hdf_chksum = \
            self._dep_files_chksum = self._dep_hdf_chksum = \
            self._out_hdf_chksum = None
        data = self._read_manifest()

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

    def _iter_depend_hdf(self):
        if 'depends' in self._data and 'hdf' in self._data['depends']:
            hdf = self._data['depends']['hdf']
            if type(hdf[0]) is list:
                for ahdf in hdf:
                    yield ahdf
            else:
                yield hdf

    def __enter__(self):
        return self

    def __exit__(self, atype, value, traceback):
        logging.debug(u"__exit__ for {}".format(self._path))
        if atype is None and self._write:
            self._write_checksums()

    def _chksum_depends(self, depends):
        if 'files' in depends:
            if type(depends.files) is not list:
                self._dep_files_chksum = depends.files.checksum()
            else:
                self._dep_files_chksum = [files.checksum() for files in
                                          depends.files]
        if 'hdf' in depends:
            if type(depends.hdf) is not list:
                self._dep_hdf_chksum = dataframe_checksum(depends.hdf)
            else:
                self._dep_hdf_chksum = [dataframe_checksum(hdf) for hdf in
                                        depends.hdf]

    @property
    def _depend_files_changed(self):
        return self._prev_files_chksum != self._dep_files_chksum

    @property
    def _depend_hdf_changed(self):
        return self._prev_hdf_chksum != self._dep_hdf_chksum

    @property
    def _need_run(self):
        '''Test if notebook should be run.'''
        return self._depend_files_changed or self._depend_hdf_changed

    def _write_checksums(self):
        from IPython.nbformat.current import write, read
        from wzdat.notebook_runner import NotebookRunner, NotebookError

        nb = read(open(self._path.encode('utf-8')), 'json')
        nr = NotebookRunner(nb)
        try:
            nr.run_notebook()
        except NotebookError, e:
            err = unicode(e)
            logging.error(err)
        else:
            write(nr.nb, open(self._path.encode('utf-8'), 'w'), 'json')

        # clear surplus info
        first_cell = nr.nb.worksheets[0].cells[0]
        first_cell['outputs'] = []
        del nr.nb.worksheets[0].cells[1:]

        last_run = datetime.fromtimestamp(time.time())
        last_run = convert_server_time_to_client(last_run)
        last_run = last_run.strftime("%Y-%m-%d %H:%M:%S")
        body = ["    'last_run': '{}'".format(last_run)]
        if self._dep_files_chksum is not None or\
                self._dep_hdf_chksum is not None:
            self._write_checksums_depends(body)

        if self._out_hdf_chksum is not None:  # could be multiple outputs
            coutput = []
            if self._out_hdf_chksum is not None:
                coutput.append("        'hdf': {}".format(
                    self._out_hdf_chksum))
            body.append("    'output': {{\n{}\n    }}".
                        format(',\n'.join(coutput)))
        else:
            if 'output' in self._data and 'hdf' in self._data['output']:
                logging.error(u"NoHDFWritten at {}".format(self._path))
                raise NoHDFWritten(self._path)

        if len(body) > 0:
            import copy
            newcell = copy.deepcopy(nr.nb.worksheets[0].cells[0])
            cs_body = "# WARNING: Generated Checksums. Do Not Edit.\n{{\n{}\n}}".\
                format(',\n'.join(body))
            newcell['input'] = cs_body
            nr.nb.worksheets[0].cells.append(newcell)

        write(nr.nb, open(self._path.encode('utf-8'), 'w'), 'json')

    def _write_checksums_depends(self, body):
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
        import json

        with open(self._path, 'r') as f:
            nbdata = json.loads(f.read())
            cells = nbdata['worksheets'][0]['cells']
            for i, cell in enumerate(cells):
                # user cell
                if i == 0:
                    mdata = ast.literal_eval(''.join(cell['input']))
                # generated checksum cell
                elif i == 1:
                    try:
                        chksum = ast.literal_eval(''.join(cell['input']))
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
        _dict['depends'] = Property()

        if 'files' in data:
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

        if 'hdf' in data:
            self._load_hdf_depends(data, _dict)

    def _load_hdf_depends(self, data, _dict):
        ftype = type(data['hdf'][0])
        if ftype is not list:
            owner, sname = data['hdf']
            cmd = 'from wzdat.util import HDF\nwith HDF("{}") as hdf:\n'\
                '    depends.hdf = hdf.store["{}"]'.format(owner, sname)
            exec(cmd, globals(), _dict)
        else:
            exec('from wzdat.util import HDF', globals(), _dict)
            exec('depends.hdf = []', globals(), _dict)
            for hdf in data['hdf']:
                owner, sname = hdf
                cmd = 'with HDF("{}") as hdf:\n    depends.hdf.append('\
                    'hdf.store["{}"])'.format(owner, sname)
                exec(cmd, globals(), _dict)

    def _check_recursive_refer(self, owner, sname):
        if 'output' in self._data and 'hdf' in self._data['output']:
            hdf = self._data['output']['hdf']
            if hdf[0] == owner and hdf[1] == sname:
                raise RecursiveReference

    def _init_output(self, output):
        _dict = self.__dict__['dict']
        _dict['output'] = ScbProperty(self._on_output_set)

    def _on_output_set(self, attr, val):
        if attr == 'hdf':
            assert 'output' in self._data and 'hdf' in self._data['output'],\
                "Manifest has no output hdf"
            owner, sname = self._data['output']['hdf']
            with HDF(owner) as hdf:
                hdf.store[sname] = val
            self._out_hdf_chksum = dataframe_checksum(val)
