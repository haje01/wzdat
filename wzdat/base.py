# -*- coding: utf-8 -*-
"""WzDat Interface classes."""


class IListable(object):
    def __getslice__(self, idx1, idx2):
        raise NotImplementedError("Not implemented")

    def __getitem__(self, idx):
        raise NotImplementedError("Not implemented")

    def __len__(self):
        raise NotImplementedError("Not implemented")

    def head(self, count=10):
        raise NotImplementedError("Not implemented")

    def tail(self, count=10):
        raise NotImplementedError("Not implemented")

    def __iter__(self):
        raise NotImplementedError("Not implemented")

    def next(self):
        raise NotImplementedError("Not implemented")

    @property
    def count(self):
        raise NotImplementedError("Not implemented")


class IMeasurable(object):
    @property
    def size(self):
        raise NotImplementedError("Not implemented")

    @property
    def hsize(self):
        raise NotImplementedError("Not implemented")

    @property
    def lcount(self):
        raise NotImplementedError("Not implemented")


class ISearchable(object):
    def find(self, word, options=None):
        raise NotImplementedError("Not implemented")


class IGroupable(object):
    def group(self):
        raise NotImplementedError("Not implemented")


class ILinkable(object):
    @property
    def link(self):
        raise NotImplementedError("Not implemented")

    @property
    def zlink(self):
        raise NotImplementedError("Not implemented")


class IRepresentable(object):
    def __unicode__(self):
        raise NotImplementedError("Not implemented")

    def __str__(self):
        raise NotImplementedError("Not implemented")

    def __repr__(self):
        raise NotImplementedError("Not implemented")


class IEquatable(object):
    def __hash__(self):
        raise NotImplementedError("Not implemented")

    def __eq__(self, o):
        raise NotImplementedError("Not implemented")


class IPathable(object):
    @property
    def abspath(self):
        raise NotImplementedError("Not implemented")

    @property
    def path(self):
        raise NotImplementedError("Not implemented")

    @property
    def filename(self):
        raise NotImplementedError("Not implemented")


class IFramable(object):
    def to_frame(self):
        raise NotImplementedError("Not implemented")


class IMergeable(object):
    def merge(self):
        raise NotImplementedError("Not implemented")


class ILineAttr(object):
    def nodes(self):
        raise NotImplementedError("Not implemented")

    def kinds(self):
        raise NotImplementedError("Not implemented")

    def dates(self):
        raise NotImplementedError("Not implemented")


class IFilterable(object):
    def noempty(self):
        raise NotImplementedError("Not implemented")


class Representable(IRepresentable):
    def __str__(self):
        return unicode(self).encode('utf-8')

    def __repr__(self):
        return unicode(self).encode('utf-8')


class Listable(IListable):
    def __init__(self):
        super(Listable, self).__init__()
        self._at = -1

    def __iter__(self):
        self._at = -1
        return self

    def next(self):
        """Return next file for iteration."""
        self._at += 1
        if self.count > self._at:
            return self.__getitem__(self._at)
        else:
            self._at = -1
            raise StopIteration()

    @property
    def count(self):
        return len(self)
