import os
import sys
import json
import time
import re
from datetime import timedelta, datetime
import urlparse

from flask import Flask, render_template, request, Response, redirect, url_for
from markdown import markdown
from IPython.nbformat.current import reads
import pytz

DEBUG = True

if DEBUG:
    sys.path.insert(0, ".")
from wzdat.util import get_notebook_dir, convert_server_time_to_client
from wzdat.rundb import get_cache_info

app = Flask(__name__)
app.debug = DEBUG

ansi_escape = re.compile(r'\x1b[^m]*m')

IPYTHON_PORT = 8090
DASHBOARD_PORT = 8095


def _page_common_vars():
    proj = os.environ['WZDAT_PRJ'] if 'WZDAT_PRJ' in os.environ else 'Noname'
    projname = proj.upper()
    sdev = ""
    if 'WZDAT_DEV' in os.environ:
        dev = os.environ['WZDAT_DEV']
        if dev.lower() == 'true':
            sdev = '[DEV]'

    ci = get_cache_info()
    if ci is not None:
        ct = datetime.fromtimestamp(ci[0])
        ct = convert_server_time_to_client(ct)
        ctime = ct.strftime('%Y-%m-%d %H:%M')
    else:
        ctime = 'N/A'

    return projname, sdev, ctime


@app.route('/')
def home():
    return redirect(url_for("dashboard"))


@app.route('/dashboard')
def dashboard():
    projname, dev, cache_time = _page_common_vars()

    from wzdat.ipynb_runner import find_cron_notebooks
    assert "WZDAT_HOST" in os.environ
    host = urlparse.urlparse(os.environ["WZDAT_HOST"]).path
    iport = int(os.environ["WZDAT_IPYTHON_PORT"]) if 'WZDAT_IPYTHON_PORT' in\
        os.environ else IPYTHON_PORT
    base_url = 'http://%s:%d/tree' % (host, iport)
    notebook_dir = get_notebook_dir()
    paths, _, _groups, fnames = find_cron_notebooks(notebook_dir, static=True)
    groups = {}
    for i, path in enumerate(paths):
        sdir = os.path.dirname(path).replace(notebook_dir, '')[1:]
        fn = os.path.basename(path)
        url = os.path.join(base_url, sdir, fn)
        gk = _groups[i].decode('utf8')
        fname = os.path.splitext(os.path.basename(fnames[i]))[0]
        if gk not in groups:
            groups[gk] = []
        groups[gk].append((path, url, fname))

    gnbs = []
    for gk in sorted(groups.keys()):
        if gk != '':
            _collect_gnbs(gnbs, gk, groups)
    if '' in groups:
        _collect_gnbs(gnbs, '', groups)

    return render_template("dashboard.html", cur="dashboard",
                           projname=projname, notebooks=gnbs,
                           nb_url=base_url, dev=dev, cache_time=cache_time)


@app.route('/start_view/<path:nbpath>', methods=['POST'])
def start_view(nbpath):
    data = json.loads(request.data)
    kwargs = {}
    notebook_dir = get_notebook_dir()
    nbpath = os.path.join(notebook_dir, nbpath)
    formname = ''
    for kv in data:
        name = kv['name']
        if name == 'wzd_formname':
            formname = kv['value']
        else:
            value = kv['value']
            if name in kwargs:
                if type(kwargs[name]) != list:
                    kwargs[name] = [kwargs[name]]
                kwargs[name].append(value)
            else:
                kwargs[name] = value

    from wzdat.dashboard.tasks import run_view_cell
    task = run_view_cell.delay(nbpath, formname, kwargs)
    return Response(task.task_id)


@app.route('/poll_view/<task_id>', methods=['POST'])
def poll_view(task_id):
    from wzdat.dashboard.tasks import run_view_cell
    from wzdat.util import div

    try:
        task = run_view_cell.AsyncResult(task_id)
        state = task.state
        if state == 'PENDING':
            return 'PROGRESS:0'
        print task.state, task.status
        if task.state == 'PROGRESS':
            return 'PROGRESS:' + str(task.result)
    except Exception:
        err = task.traceback
        err = ansi_escape.sub('', err)
        return Response('<div class="view"><pre class="ds-err">%s</pre></div>'
                        % err)
    outputs = task.get()
    rv = []
    _nb_output_to_html_dashboard(div, rv, outputs, 'view')
    ret = '\n'.join(rv)
    return Response(ret)


def _nb_output_to_html(path):
    rv = []
    with open(path, 'r') as f:
        nb = reads(f.read(), 'json')
        for cell in nb['worksheets'][0]['cells']:
            _cell_output_to_html(rv, cell)
    return '\n'.join(rv)


def _cell_output_to_html(rv, cell):
    from wzdat.util import div

    _type = cell['cell_type']
    _cls = ''
    if _type == 'code' and 'outputs' in cell:
        code = cell['input']
        if '#!dashboard' in code:
            if '#!dashboard_control' in code:
                _cls = 'control'
            elif '#!dashboard_view' in code:
                _cls = 'view'
            _nb_output_to_html_dashboard(div, rv, cell['outputs'], _cls)
    elif _type == 'markdown':
        src = cell['source']
        if '<!--dashboard' in src:
            _cls = ''
            if '<!--dashboard_view-->' in src:
                _cls = 'view'
            rv.append(div(markdown(src), _cls))


def _cell_output_to_html_dashboard(output, image_cell):
    _type = output['output_type']
    if _type == 'stream':
        return output['text']
    elif _type == 'display_data':
        if 'png' in output:
            data = output['png']
            return '<img src="data:image/png;base64,%s"></img>' % data
    elif _type == 'pyout':
        if 'html' in output:
            return '<div class="rendered_html">%s</div>' % output['html']
        elif not image_cell:
            return output['text']


def _nb_output_to_html_dashboard(div, rv, outputs, _cls):
    # check this is image cell
    image_cell = False
    for output in outputs:
        _type = type(output)
        if _type in (unicode, str):
            continue
        _type = output['output_type']
        if _type == 'display_data':
            image_cell = True

    for output in outputs:
        _type = type(output)
        if _type in (unicode, str):
            html = output
        else:
            html = _cell_output_to_html_dashboard(output, image_cell)
        if html is not None:
            rv.append(div(html, _cls))


def _collect_gnbs(gnbs, gk, groups):
    from wzdat import rundb

    nbs = []
    notebook_dir = get_notebook_dir()
    for path, url, fname in groups[gk]:
        out = _nb_output_to_html(path)
        ri = rundb.get_run_info(path)
        if ri is not None:
            executed, elapsed = _get_run_time(ri)
            cur = ri[2]
            total = ri[3]
            ri = (executed, elapsed, cur, total)
        path = path.replace(notebook_dir, '')[1:].decode('utf-8')
        nbs.append((url.decode('utf-8'), fname.decode('utf-8'), out, ri, path))
    gnbs.append((gk, nbs))


def _get_run_time(ri):
    if ri[0] is not None:
        executed = timedelta(seconds=int(time.time() - ri[0]))
    else:
        executed = None
    if ri[1] is not None:
        if ri[1] == -1:
            elapsed = timedelta(seconds=int(ri[1]))
        else:
            elapsed = timedelta(seconds=int(ri[1]))
    else:
        elapsed = None
    return executed, elapsed


@app.route('/finder')
def finder():
    projname, dev, cache_time = _page_common_vars()

    return render_template("finder.html", cur="finder", projname=projname,
                           dev=dev, cache_time=cache_time)


@app.route('/notebooks')
def notebooks():
    projname, dev, cache_time = _page_common_vars()

    assert "WZDAT_HOST" in os.environ
    host = urlparse.urlparse(os.environ["WZDAT_HOST"]).path
    iport = int(os.environ["WZDAT_IPYTHON_PORT"]) if 'WZDAT_IPYTHON_PORT' in\
        os.environ else IPYTHON_PORT
    base_url = 'http://%s:%d/tree' % (host, iport)
    proj = os.environ['WZDAT_PRJ'] if 'WZDAT_PRJ' in os.environ else 'Noname'
    projname = proj.upper()

    return render_template("notebooks.html", cur="notebooks",
                           projname=projname, nb_url=base_url, dev=dev,
                           cache_time=cache_time)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=DASHBOARD_PORT, debug=DEBUG)
