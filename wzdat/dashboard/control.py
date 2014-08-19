from collections import OrderedDict

from BeautifulSoup import BeautifulSoup as bs


def _typed_value(sv):
    if type(sv) == list or type(sv) == tuple:
        return sv
    try:
        return eval(sv)
    except Exception:
        return sv


class Control(object):
    def __init__(self):
        pass


class DummyControl(Control):
    def __init__(self, val):
        self._val = _typed_value(val)

    @property
    def val(self):
        return self._val


class Select(Control):
    def __init__(self, options, values=None, selected=None, label=None,
                 plabel=None):
        self.options = options
        self.values = values
        self.selected = selected
        self.label = label
        self.plabel = plabel

    def dump(self, name):
        rv = []
        for i, opt in enumerate(self.options):
            val = '' if self.values is None else ' value="%s"' %\
                str(self.values[i])
            sel = ' selected="true"' if i == self.selected else ''
            rv.append('  <option%s%s>%s</option>' % (val, sel, opt))
        label = '' if self.label is None else u'\n<label>%s</label>\n' %\
            self.label
        plabel = '' if self.plabel is None else u'\n<label>%s</label>\n' %\
            self.plabel
        return u'<div class="form-group">\n%s'\
               u'<select name="%s">\n%s\n</select>\n%s\n</div>' %\
               (label, name, '\n'.join(rv), plabel)

    @property
    def val(self):
        for i, opt in enumerate(self.options):
            if i == self.selected:
                return self.options[i] if self.values is None else\
                    self.values[i]


class _CheckboxOrRadio(Control):
    def __init__(self, radio, labels, values, checks, group_label=None):
        self.radio = radio
        self.labels = labels
        self.values = values
        self.checks = checks
        self.group_label = group_label

    def dump(self, name):
        rv = []
        for i, label in enumerate(self.labels):
            value = self.labels[i] if self.values is None else self.values[i]
            check = self.checks[i]
            checked = ' checked' if check else ''
            ctl = "radio" if self.radio else "checkbox"
            rv.append('<div class="%s">\n'
                      '  <label>\n'
                      '    <input type="%s" name="%s" value="%s"%s>%s\n'
                      '  </label>\n'
                      '</div>' % (ctl, ctl, name, value, checked, label))
        body = u'\n'.join(rv)
        if len(self.labels) > 0 is not None:
            if self.group_label is not None:
                glabel = '<label>%s</label>\n' % self.group_label
            else:
                glabel = ''
            return u'<div class="form-group">\n%s'\
                   u'  %s\n'\
                   u'</div>' % (glabel, body)
        else:
            return body

    def _val(self, i):
        return self.labels[i] if self.values is None else self.values[i]

    @property
    def val(self):

        if self.radio:
            for i, checked in enumerate(self.checks):
                if checked:
                    return self._val(i)
        else:
            rv = []
            for i, checked in enumerate(self.checks):
                if checked:
                    rv.append(self._val(i))
            if len(rv) == 1:
                return rv[0]
            return rv


class Checkbox(_CheckboxOrRadio):
    def __init__(self, labels, values=None, checks=None, group_label=None):
        super(Checkbox, self).__init__(False, labels, values, checks,
                                       group_label)


class Radio(_CheckboxOrRadio):
    def __init__(self, labels, values=None, checked=None, group_label=None):
        checks = [True if i == checked else False for i, label in
                  enumerate(labels)]
        super(Radio, self).__init__(True, labels, values, checks,
                                    group_label)


class Form(object):
    def __init__(self):
        self.od = OrderedDict()

    def init(self, dic):
        if dic is not None:
            for key in dic.keys():
                self.od[key] = DummyControl(dic[key])

    def register(self, formname):
        rv = []
        for name in self.od.keys():
            ctrl = self.od[name]
            rv.append(ctrl.dump(name))
        html = u'<form class="form-inline ds-ctrl" role="form">\n%s\n'\
            u'<input type="hidden" name="wzd_formname" value="%s"\n'\
            u'<button class="btn btn-primary btn-sm ladda-button view" '\
            u'data-style="expand-right" data-size="s">\n  <span '\
            u'class="ladda-label">View</span>\n</button>\n</form>' %\
            ('\n'.join(rv), formname)
        soup = bs(html)
        return soup.prettify()

    def __setitem__(self, key, value):
        self.od[key] = value

    def __getitem__(self, key):
        return self.od.get(key, None)
