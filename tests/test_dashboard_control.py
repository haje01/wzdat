import re

from BeautifulSoup import BeautifulSoup as bs

from wzdat.dashboard.control import Form, Select, Checkbox, Radio


def test_control():
    show_opts = ['A', 'B', 'ALL']

    form = Form()

    form['show'] = Select(show_opts, None, 2, u"Display Count")
    form['kinds'] = Checkbox([u'Bar', u'Line'], ['bar', 'line'], [False, True],
                             u'Graph Type')
    form['style'] = Radio([u'Solid', u'Dot', u'Circle'], ['-', '--', 'o'], 0,
                          u'Line Style')

    show = form['show'].val
    assert show == 'ALL'
    kind = form['kinds'].val
    assert kind == 'line'
    style = form['style'].val
    assert style == '-'

    html = form.register('form')
    soup = bs(html)
    divs = soup.findAll('div', {'class': 'form-group'})
    assert len(divs) == 3
    assert divs[0].find('label', text=re.compile('Display Count')) is not None
    assert divs[0].find('option', text=re.compile('\sA\s')) is not None
    assert divs[0].find('option', text=re.compile('\sB\s')) is not None
    assert divs[0].find('option', {'selected': 'true'},
                        text=re.compile('\sALL\s')) is not None

    assert divs[1].find('label', text=re.compile('Graph Type')) is not None
    assert divs[1].find('option', text=re.compile('Bar')) is not None
    assert divs[1].find('option', {'checked': 'checked'},
                        text=re.compile('Line')) is not None

    assert divs[2].find('label', text=re.compile('Line Style')) is not None
    assert divs[2].find('input', {'type': 'radio', 'value': '-',
                                  'checked': 'checked'})
    assert divs[2].find('input', {'type': 'radio', 'value': '--'})
    assert divs[2].find('input', {'type': 'radio', 'value': 'o'})
