# -*- coding: utf-8 -*-
"""Notebook dependency resolver."""
import os

from wzdat.util import iter_notebook_manifest, OfflineNBPath
from wzdat.ipynb_runner import update_notebook_by_run
from wzdat.manifest import Manifest


class UnresolvedHDFDependency(Exception):
    pass


class CircularDependency(Exception):
    pass


class DependencyTree(object):
    def __init__(self, nbdir, skip_nbs=None):
        self.notebooks = []

        # collect notebooks with manifest
        for nbpath, manifest in iter_notebook_manifest(nbdir, False, skip_nbs):
            nb = Notebook(nbpath, manifest)
            self.notebooks.append(nb)

        # add dependencies
        for nb, hdf in self.iter_notebook_with_dephdf(skip_nbs):
            if type(hdf[0]) is not list:
                self._find_and_add_depend(nb, hdf)
            else:
                for ahdf in hdf:
                    self._find_and_add_depend(nb, ahdf)

    def _find_hdf_out_notebook(self, _hdf):
        for nb, hdf in self.iter_notebook_with_outhdf():
            if _hdf == hdf:
                return nb

    def _find_and_add_depend(self, nb, hdf):
        hnb = self._find_hdf_out_notebook(hdf)
        if hnb is None:
            raise UnresolvedHDFDependency()
        nb.add_depend(hnb)

    def iter_notebook_with_outhdf(self):
        for nb in self.notebooks:
            mdata = nb.manifest._data
            if 'output' in mdata and 'hdf' in mdata['output']:
                yield nb, mdata['output']['hdf']

    def iter_notebook_with_dephdf(self, skip_nbs):
        '''Iterate notebook with depending hdf.'''
        for nb in self.notebooks:
            if skip_nbs is not None and nb.path in skip_nbs:
                continue
            if 'depends' not in nb.manifest or 'hdf' not in\
                    nb.manifest.depends:
                continue
            yield nb, nb.manifest.depends['hdf']

    def get_notebook_by_fname(self, fname):
        for nb in self.notebooks:
            if fname in nb.path:
                return nb

    def iter_noscd_notebook(self):
        '''Iterate non-scheduled notebooks.'''
        for nb in self.notebooks:
            if 'schedule' in nb.manifest:
                continue
            yield nb

    def resolve(self, updaterun=False):
        assert len(self.notebooks) > 0
        resolved = []
        runs = []
        for nb in self.iter_noscd_notebook():
            if nb not in resolved:
                self._resolve(updaterun, nb, resolved, runs, [])
        return resolved, runs

    def _resolve(self, updaterun, notebook, resolved, runs, seen):
        seen.append(notebook)

        # resolve dependencies
        for dnb in notebook.depends:
            if dnb not in resolved:
                if dnb in seen:
                    raise CircularDependency()
                self._resolve(updaterun, dnb, resolved, runs, seen)

        self._run_resolved(updaterun, notebook, resolved, runs)

    def _run_resolved(self, updaterun, notebook, resolved, runs):
        '''Run notebook after all its dependencies resolved.'''
        notebook.reload_manifest()
        need_run = notebook.manifest._need_run
        if need_run and updaterun:
            with OfflineNBPath(notebook.path):
                update_notebook_by_run(notebook.path)
                runs.append(notebook)

        resolved.append(notebook)


class Notebook(object):
    def __init__(self, path, manifest):
        self.path = path
        self.manifest = manifest
        self.depends = []

    def add_depend(self, notebook):
        self.depends.append(notebook)

    def is_depend(self, parent):
        return parent in self.depends

    def reload_manifest(self):
        '''Reload manifest to check to run'''
        self.manifest = Manifest(False, True, self.path)

    @property
    def fname(self):
        return os.path.basename(self.path)
