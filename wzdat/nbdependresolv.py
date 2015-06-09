# -*- coding: utf-8 -*-
"""Notebook dependency resolver."""
import os

from wzdat.util import iter_notebook_manifests
from wzdat.manifest import Manifest


class UnresolvedHDFDependency(Exception):
    pass


class CircularReference(Exception):
    pass


def _find_hdf_out_notebook(tree, owner, sname):
    for nb in tree.notebooks:
        if nb.manifest._output_hdf is not None:
            if nb.manifest._output_hdf == [owner, sname]:
                return nb


def _find_and_add_hdf_edge(tree, nb, owner, sname):
    hnb = _find_hdf_out_notebook(tree, owner, sname)
    if hnb is None:
        raise UnresolvedHDFDependency()
    hnb.add_edge(nb)


class DependencyTree(object):
    def __init__(self, nbdir, skip_nbs=None):
        self.notebooks = []

        # collect notebooks with manifest
        for nbpath, _ in iter_notebook_manifests(nbdir):
            if skip_nbs is not None and nbpath in skip_nbs:
                continue
            manifest = Manifest(False, False, nbpath)
            nb = Notebook(nbpath, manifest)
            self.notebooks.append(nb)

        # add dependencies
        for nb in self.notebooks:
            if skip_nbs is not None and nb.path in skip_nbs:
                continue
            if 'depends' not in nb.manifest or 'hdf' not in\
                    nb.manifest.depends:
                continue

            hdf = nb.manifest.depends.hdf
            if type(hdf[0]) not in (tuple, list):
                owner, sname = hdf
                _find_and_add_hdf_edge(self, nb, owner, sname)
            else:
                for ahdf in hdf:
                    owner, sname = ahdf
                    _find_and_add_hdf_edge(self, nb, owner, sname)

    def get_notebook_by_fname(self, fname):
        for nb in self.notebooks:
            if fname in nb.path:
                return nb

    def iter_top_notebooks(self):
        '''Iterate top notebooks(no dependency of outer hdf).'''
        for nb in self.notebooks:
            if 'depends' not in nb.manifest or 'hdf' not in \
                    nb.manifest.depends:
                yield nb

    def resolve(self):
        assert len(self.notebooks) > 0
        resolved = []
        for top in self.iter_top_notebooks():
            self._resolve(top, resolved, [])
        return resolved

    def _resolve(self, notebook, resolved, seen):
        print notebook.fname
        seen.append(notebook)
        for edge in notebook.edges:
            if edge not in resolved:
                if edge in seen:
                    raise CircularReference()
                self._resolve(edge, resolved, seen)
        resolved.append(notebook)


class Notebook(object):
    def __init__(self, path, manifest):
        self.path = path
        self.manifest = manifest
        self.edges = []

    def add_edge(self, notebook):
        self.edges.append(notebook)

    def is_depend(self, parent):
        return self in parent.edges

    @property
    def fname(self):
        return os.path.basename(self.path)
