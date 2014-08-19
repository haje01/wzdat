# -*- coding: utf-8 -*-
"""Values."""

import types
from datetime import datetime

from wzdat.const import PRINT_LMAX
from wzdat.base import Representable, Listable, IEquatable, IFilterable,\
    IGroupable


class Value(Representable, IEquatable):

    """Value object."""

    _instances = {}

    @classmethod
    def _instance(cls, supobj, field, abbr, part, _repr):
        """Return value instance with combined key."""
        if isinstance(part, types.ListType):
            key = [supobj, field, abbr] + part
        else:
            key = [supobj, field, abbr, part]
        key = tuple(key)
        if key in cls._instances:
            obj = cls._instances[key]
        else:
            obj = cls(supobj, field, abbr, part, _repr)
            cls._instances[key] = obj
            if supobj is not None:
                supobj._values.add(obj)
                setattr(supobj, obj._abbr, obj)
            else:
                field._values.add(obj)
        return obj

    def __init__(self, supobj, field, abbr, part, _repr):
        self._supobj = supobj
        self._field = field
        self._abbr = abbr
        self._part = part
        self._repr = _repr
        self._values = set()
        self._neg = False

    def __hash__(self):
        if not hasattr(self, '_field'):
            return hash('na_val')
        return hash((self._field, self._abbr))

    def __eq__(self, o):
        return self._field == o._field and self._part == o._part

    def __unicode__(self):
        if self._field is None:
            return 'FailValue'
        else:
            if self._supobj is not None:
                return self._supobj._repr + '.' + self._repr
            else:
                return self._repr

    def _match(self, fileo):
        return self._field._match(self, fileo)

    def __neg__(self):
        self._neg = True

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


class FailValue(Value):

    """False value object."""

    def __init__(self):
        super(FailValue, self).__init__(None, None, None, None, None)

    def _match(self, fileo):
        return False

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


class ValueList(Representable, Listable, IEquatable, IFilterable,
                IGroupable):

    """Value list."""

    def __init__(self, values, _force_all=False):
        super(ValueList, self).__init__()
        self._values = values
        self._count = len(values)
        self._force_all = _force_all

    def group(self):
        """Return grouped  sub-items."""
        return set([i._supobj if i._supobj is not None else i for i in
                    self._values])

    def __getitem__(self, idx):
        return self._values[idx]

    def __getslice__(self, idx1, idx2):
        return ValueList(self._values[idx1:idx2], True)

    def __iter__(self):
        super(ValueList, self).__iter__()
        return self

    def __len__(self):
        return len(self._values)

    def __eq__(self, o):
        if isinstance(o, ValueList):
            return self._values == o._values
        elif isinstance(o, types.ListType):
            return self._values == o
        return False

    def __unicode__(self):
        if ((self._count == 0 or self._count > PRINT_LMAX) and not
           self._force_all):
            return '%s\ncount: %d' % (type(self), self._count)
        else:
            return "[%s]" % ',\n'.join([str(svr) for svr in self._values])

    @property
    def _field(self):
        return self._values[0]._field

    def _match(self, fileo):
        for val in self._values:
            if val._match(fileo):
                return True
        return False

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)


class DateValue(Value):

    """Date value object."""

    @classmethod
    def _instance(cls, field, year, month, day):
        """Return value instance with combined key."""
        key = [field, year, month, day]
        key = tuple(key)
        if key in cls._instances:
            obj = cls._instances[key]
        else:
            obj = cls(field, year, month, day)
            cls._instances[key] = obj
            field._values.add(obj)
        return obj

    def __init__(self, field, year, month, day):
        abbr = self._repr = 'D%04d_%02d_%02d' % (year, month, day)
        super(DateValue, self).__init__(None, field, abbr, None, abbr)
        self.year, self.month, self.day = year, month, day
        self._sdate = "%04d-%02d-%02d" % (year, month, day)

    def _match(self, fileo):
        fdate = fileo.date
        return self.year == fdate.year and self.month == fdate.month and\
            self.day == fdate.day

    def __hash__(self):
        if not hasattr(self, 'year'):
            return hash('na_date')
        return datetime(self.year, self.month, self.day).__hash__()

    def __eq__(self, o):
        return self.day == o.day and self.month == o.month \
            and self.year == o.year

    def __getstate__(self):
        s = self.__dict__.copy()
        return s

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def datestr(self):
        return "%d-%d-%d" % (self.year, self.month, self.day)


def check_date_slice(dates, dslice):
    start = dslice.start
    stop = dslice.stop
    step = dslice.step

    def _find_date_value_idx(_dates, date):
        for _idx, _date in enumerate(_dates):
            if _date == date:
                return _idx

    if isinstance(start, DateValue):
        start = _find_date_value_idx(dates, start)

    if isinstance(stop, DateValue):
        stop = _find_date_value_idx(dates, stop)

    return slice(start, stop, step)
