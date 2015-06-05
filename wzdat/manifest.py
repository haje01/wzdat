# -*- coding: utf-8 -*-
"""Manifest for Dashboard Notebook."""

import os
import ast

from wzdat.util import Property, get_notebook_path, get_notebook_dir,\
    dataframe_checksum, HDF, ScbProperty


class RecursiveReference(Exception):
    pass


class Manifest(Property):
    def __init__(self, write=True, test_path=None):
        super(Manifest, self).__init__()

        if test_path is None:
            nbdir = get_notebook_dir()
            nbrpath = get_notebook_path()
            self._path = os.path.join(nbdir,
                                      nbrpath.replace('.ipynb',
                                                      '.manifest.ipynb'))
        else:
            self._path = test_path.replace('.ipynb', '.manifest.ipynb')

        assert os.path.isfile(self._path), "Manifest file '{}' not "\
            "exist.".format(self._path)

        self._output_hdf = self._prev_files_chksum = self._prev_hdf_chksum = \
            self._files_chksum = self._hdf_chksum = None
        self._nr, mtext = self._read_manifest()
        data = ast.literal_eval(mtext)

        if 'output' in data:
            self._init_output(data['output'])

        if 'depends' in data:
            self._exec_depends(data['depends'])

        _dict = self.__dict__['dict']
        if 'depends' in _dict:
            self._init_depends(_dict['depends'])

        if write:
            self._write_checksums()

    def _init_depends(self, depends):
        if 'files' in depends:
            if type(depends.files) not in (list, tuple):
                self._files_chksum = depends.files.checksum()
            else:
                self._files_chksum = [files.checksum() for files in
                                      depends.files]
        if 'hdf' in depends:
            if type(depends.hdf) is not list:
                self._hdf_chksum = dataframe_checksum(depends.hdf)
            else:
                self._hdf_chksum = [dataframe_checksum(hdf) for hdf in
                                    depends.hdf]

    @property
    def _depend_files_changed(self):
        return self._prev_files_chksum != self._files_chksum

    @property
    def _depend_hdf_changed(self):
        return self._prev_hdf_chksum != self._hdf_chksum

    @property
    def _output_exist(self):
        if self._output_hdf is not None:
            owner, sname = self._output_hdf
            with HDF(owner) as hdf:
                return sname in hdf.store
        return True

    @property
    def _need_output(self):
        return self._depend_files_changed or self._depend_hdf_changed or\
            (not self._output_exist)

    def _write_checksums(self):
        from IPython.nbformat.current import write
        # clear surplus info
        first_cell = self._nr.nb.worksheets[0].cells[0]
        first_cell['outputs'] = []
        del self._nr.nb.worksheets[0].cells[1:]

        checksums = []
        if self._files_chksum is not None:  # could be multiple depends
            cdepends = []
            if self._files_chksum is not None:
                cdepends.append("        'files': {}".
                                format(self._files_chksum))
            if len(cdepends) > 0:
                checksums.append("    'depends': {{\n{}\n    }}".
                                 format(',\n'.join(cdepends)))

        if self._hdf_chksum is not None:  # could be multiple output
            coutput = []
            if self._hdf_chksum is not None:
                coutput.append("        'hdf': {}".format(self._hdf_chksum))
            checksums.append("    'output': {{\n{}\n    }}".
                             format(',\n'.join(coutput)))

        if len(checksums) > 0:
            import copy
            newcell = copy.deepcopy(self._nr.nb.worksheets[0].cells[0])
            cs_body = "# WARNING: Generated Checksums. Do Not Edit.\n{{\n{}\n}}".\
                format(',\n'.join(checksums))
            newcell['input'] = cs_body
            self._nr.nb.worksheets[0].cells.append(newcell)

        write(self._nr.nb, open(self._path.encode('utf-8'), 'w'), 'json')

    def _read_manifest(self):
        from IPython.nbformat.current import read
        from wzdat.notebook_runner import NotebookRunner
        nb = read(open(self._path.encode('utf-8')), 'json')
        r = NotebookRunner(nb)
        mtext = ''
        for i, cell in enumerate(r.iter_code_cells()):
            r.run_cell(cell)
            # user cell
            if i == 0:
                mtext = cell['outputs'][0]['text']
            # generated checksum cell
            elif i == 1:
                chksum = cell['outputs'][0]['text']
                data = ast.literal_eval(chksum)
                if 'depends' in data:
                    depends = data['depends']
                    if 'files' in depends:
                        self._prev_files_chksum = depends['files']
                    if 'hdf' in depends:
                        self._prev_hdf_chksum = depends['hdf']
        return r, mtext

    def _parse_depends_files(self, files):
        melm = files[0].split('.')
        frm = '.'.join(melm[:-1])
        mod = melm[-1]
        dates = files[1]
        return frm, mod, dates

    def _exec_depends(self, depends):
        _dict = self.__dict__['dict']
        _dict['depends'] = Property()

        if 'files' in depends:
            ftype = type(depends['files'][0])
            if ftype not in (list, tuple):
                files = depends['files']
                frm, mod, dates = self._parse_depends_files(files)
                cmd = 'from {} import {} as _; _.load_info(); depends.files ='\
                    '_.files[_.dates[-{}:]]; _ = None'.format(frm, mod, dates)
                exec(cmd, globals(), _dict)
            else:
                exec('depends.files = []', globals(), _dict)
                for files in depends['files']:
                    frm, mod, dates = self._parse_depends_files(files)
                    cmd = 'from {} import {} as _; _.load_info(); depends.files.'\
                        'append(_.files[_.dates[-{}:]]); _ = None'.\
                        format(frm, mod, dates)
                    exec(cmd, globals(), _dict)

        if 'hdf' in depends:
            ftype = type(depends['hdf'][0])
            if ftype not in (list, tuple):
                owner, sname = depends['hdf']
                self._check_recursive_refer(owner, sname)
                cmd = 'from wzdat.util import HDF\nwith HDF("{}") as hdf:\n'\
                    '    depends.hdf = hdf.store["{}"]'.format(owner, sname)
                exec(cmd, globals(), _dict)
            else:
                exec('from wzdat.util import HDF', globals(), _dict)
                exec('depends.hdf = []', globals(), _dict)
                for hdf in depends['hdf']:
                    owner, sname = hdf
                    self._check_recursive_refer(owner, sname)
                    cmd = 'with HDF("{}") as hdf:\n    depends.hdf.append('\
                        'hdf.store["{}"])'.format(owner, sname)
                    exec(cmd, globals(), _dict)

    def _check_recursive_refer(self, owner, sname):
        if self._output_hdf[0] == owner and self._output_hdf[1] == sname:
            raise RecursiveReference

    def _init_output(self, output):
        _dict = self.__dict__['dict']
        _dict['output'] = ScbProperty(self._on_output_set)
        if 'hdf' in output:
            self._output_hdf = output['hdf']

    def _on_output_set(self, attr, val):
        if attr == 'hdf':
            assert self._output_hdf is not None, "Manifest has no output hdf"
            owner, sname = self._output_hdf
            with HDF(owner) as hdf:
                hdf.store[sname] = val
            self._hdf_chksum = dataframe_checksum(val)
            self._write_checksums()
