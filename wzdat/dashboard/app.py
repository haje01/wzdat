import os
import json
import time
import re
from datetime import timedelta, datetime
import logging

from flask import Flask, render_template, request, Response, redirect, url_for
from markdown import markdown
from IPython.nbformat.current import reads

from wzdat.util import get_notebook_dir, convert_server_time_to_client
from wzdat.rundb import get_cache_info, get_finder_info
from wzdat.jobs import cache_finder
from wzdat.make_config import make_config

app = Flask(__name__)

ansi_escape = re.compile(r'\x1b[^m]*m')


cfg = make_config()
assert 'WZDAT_HOST' in os.environ
HOST = os.environ['WZDAT_HOST']
app.debug = cfg['debug'] if 'debug' in cfg else False

if not app.debug and 'admins' in cfg:
    admins = cfg['admins']
    from logging.handlers import SMTPHandler
    mail_handler = SMTPHandler('127.0.0.1', 'dashboard@localhost', admins,
                               'Dashboard Errors')
    mail_handler.setLevel(logging.ERROR)
    app.logger.addHandler(mail_handler)


def _page_common_vars():
    prj = cfg['prj']
    projname = prj.upper()
    sdev = ""
    if 'dev' in cfg:
        dev = cfg['dev']
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
    logging.debug("dashboard home")
    projname, dev, cache_time = _page_common_vars()

    from wzdat.ipynb_runner import find_cron_notebooks
    iport = int(cfg["host_ipython_port"])
    base_url = 'http://%s:%d/tree' % (HOST, iport)
    notebook_dir = get_notebook_dir()
    paths, _, _groups, fnames = find_cron_notebooks(notebook_dir, static=True)
    logging.debug("find_cron_notebooks done")
    groups = {}
    for i, path in enumerate(paths):
        sdir = os.path.dirname(path).replace(notebook_dir, '')[1:]
        fn = os.path.basename(path)
        url = os.path.join(base_url, sdir, fn)
        gk = _groups[i]
        fname = os.path.splitext(os.path.basename(fnames[i]))[0]
        if gk not in groups:
            groups[gk] = []
        groups[gk].append((path, url, fname))
    logging.debug("collected notebooks by group")

    gnbs = []
    for gk in sorted(groups.keys()):
        if gk != '':
            _collect_gnbs(gnbs, gk, groups)
    if '' in groups:
        _collect_gnbs(gnbs, '', groups)
    logging.debug("done _collect_gnbs")

    return render_template("dashboard.html", cur="dashboard",
                           projname=projname, notebooks=gnbs,
                           nb_url=base_url, dev=dev, cache_time=cache_time)


@app.route('/start_view/<path:nbpath>', methods=['POST'])
def start_view(nbpath):
    logging.debug('start_view')
    data = json.loads(request.data)
    kwargs = {}
    notebook_dir = get_notebook_dir()
    nbpath = os.path.join(notebook_dir, nbpath)
    formname = ''
    for kv in data:
        name = kv['name']
        logging.debug('name: {}'.format(name))
        if name == 'wzd_formname':
            logging.debug('wzd_formname')
            formname = kv['value']
            logging.debug('formname: {}'.format(formname))
        else:
            value = kv['value']
            logging.debug(u'value: {}'.format(value))
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
    logging.debug('poll_view {}'.format(task_id))
    from wzdat.dashboard.tasks import run_view_cell
    from wzdat.util import div

    try:
        task = run_view_cell.AsyncResult(task_id)
        state = task.state
        if state == 'PENDING':
            return 'PROGRESS:0'
        logging.debug("{} - {}".format(task.state, task.status))
        if task.state == 'PROGRESS':
            return 'PROGRESS:' + str(task.result)
        outputs = task.get()
        # logging.debug('outputs {}'.format(outputs))
    except Exception:
        err = task.traceback
        logging.error(err)
        err = ansi_escape.sub('', err)
        return Response('<div class="view"><pre class="ds-err">%s</pre></div>'
                        % err)
    rv = []
    _nb_output_to_html_dashboard(div, rv, outputs, 'view')
    ret = '\n'.join(rv)
    return Response(ret)


@app.route('/start_rerun/<path:nbpath>', methods=['POST'])
def start_rerun(nbpath):
    logging.debug('start_rerun')
    notebook_dir = get_notebook_dir()
    nbpath = os.path.join(notebook_dir, nbpath)

    from wzdat.dashboard.tasks import rerun_notebook
    task = rerun_notebook.delay(nbpath)
    rv = nbpath + '/' + task.task_id
    logging.debug(u'rv {}'.format(rv))
    return Response(rv)


@app.route('/poll_rerun/<path:task_info>', methods=['POST'])
def poll_rerun(task_info):
    task_id = 0
    logging.debug(u'poll_rerun {}'.format(task_info))
    from wzdat import rundb
    from wzdat.util import div
    from wzdat.dashboard.tasks import rerun_notebook
    task_id = task_info.split('/')[-1]
    nbpath = '/'.join(task_info.split('/')[:-1])

    if nbpath[0] != '/':
        nbpath = '/' + nbpath
    # logging.debug(u'task_id {}, nbpath {}'.format(task_id, nbpath))

    try:
        task = rerun_notebook.AsyncResult(task_id)
        state = task.state
        if state == 'PENDING':
            logging.debug('task pending')
            return 'PROGRESS:0'
        elif task.state == 'PROGRESS':
            ri = rundb.get_run_info(nbpath)
            if ri is not None:
                logging.debug(u"run info exist")
                err = ri[4]
                # logging.debug(u'err: {}'.format(err))
                if err is None:
                    cur = ri[2]
                    total = ri[3] + 1
                    logging.debug(u'cur {} total {}'.format(cur, total))
                    return 'PROGRESS:' + str(cur/float(total))
                else:
                    logging.debug(u"ri error {}".format(err))
                    return Response('<div class="view"><pre '
                                    'class="ds-err">%s</pre></div>' % err)
            else:
                logging.debug(u"run info not exist")
                return 'PROGRESS:0'
        outputs = task.get()
        logging.debug('task done')
        # logging.debug('outputs {}'.format(outputs))
    except Exception, e:
        logging.debug(str(e))
        logging.error(e)
        err = task.traceback
        logging.error(err)
        err = ansi_escape.sub('', err)
        return Response('<div class="view"><pre class="ds-err">%s</pre></div>'
                        % err)

    rv = []
    _nb_output_to_html_dashboard(div, rv, outputs, 'rerun')
    ret = '\n'.join(rv)
    # logging.debug(u'ret {}'.format(ret))
    return Response(ret)


def _nb_output_to_html(path):
    logging.debug('_nb_output_to_html {}'.format(path.encode('utf-8')))
    rv = []
    with open(path, 'r') as f:
        nb = reads(f.read(), 'json')
        ws = nb['worksheets']
        if len(nb) > 0:
            for cell in ws[0]['cells']:
                _cell_output_to_html(rv, cell)
    return '\n'.join(rv)


def _cell_output_to_html(rv, cell):
    from wzdat.util import div

    _type = cell['cell_type']
    _cls = ''
    if _type == 'code' and 'outputs' in cell:
        code = cell['input']
        if '#!dashboard_control' in code or '#!dashboard_view' in code:
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
    logging.debug('_collect_gnbs ' + notebook_dir)
    # logging.debug(str(groups[gk]))
    for path, url, fname in groups[gk]:
        out = _nb_output_to_html(path)
        # logging.debug('get_run_info {}'.format(path.encode('utf-8')))
        ri = rundb.get_run_info(path)
        # logging.debug('get_run_info {}'.format(ri))
        if ri is not None:
            start, elapsed = _get_run_time(ri)
            cur = ri[2]
            total = ri[3]
            err = ri[4]
            if err is not None:
                out = '<div class="fail-result">Check error, fix it, '\
                      'and rerun.</div>'
            # logging.debug(u'err {}'.format(err))
            ri = (start, elapsed, cur, total, err)
        path = path.replace(notebook_dir, '')[1:]
        nbs.append((url, fname, out, ri, path))
    gnbs.append((gk, nbs))
    logging.debug('_collect_gnbs done')


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
    file_types = get_finder_info()
    if file_types is None or len(file_types) == 0:
        file_types = cache_finder()
    return render_template("finder.html", cur="finder", projname=projname,
                           dev=dev, cache_time=cache_time,
                           file_types=file_types)


def _response_task_status(task_fn, task_id):
    try:
        task = task_fn.AsyncResult(task_id)
        state = task.state
        if state == 'PENDING':
            return 'PROGRESS:0'
        if task.state == 'PROGRESS':
            return 'PROGRESS:' + str(task.result)
        outputs = task.get()
        logging.debug('SUCCESS: ' + str(outputs))
    except Exception, e:
        err = task.traceback
        logging.error(err)
        outputs = "Error: {}".format(str(e))
    return Response(outputs)


@app.route('/finder_search/<ftype>', methods=['POST'])
def finder_search(ftype):
    from wzdat.dashboard.tasks import select_files
    logging.debug("finder_search")
    task = select_files.delay(ftype, request.data)
    return Response(task.task_id)


@app.route('/finder_poll_search/<task_id>', methods=['POST'])
def finder_poll_search(task_id):
    logging.debug("finder_poll_search")
    from wzdat.dashboard.tasks import select_files
    return _response_task_status(select_files, task_id)


@app.route('/finder_request_download/<ftype>', methods=['POST'])
def finder_request_download(ftype):
    logging.debug("finder_request_download")
    from wzdat.dashboard.tasks import zip_files
    task = zip_files.delay(ftype, request.data)
    logging.debug(request.data)
    return Response(task.task_id)


@app.route('/finder_poll_request_download/<task_id>', methods=['POST'])
def finder_poll_request_download(task_id):
    logging.debug("finder_poll_request_download")
    from wzdat.dashboard.tasks import zip_files
    return _response_task_status(zip_files, task_id)


@app.route('/notebooks')
def notebooks():
    projname, dev, cache_time = _page_common_vars()

    iport = int(cfg["host_ipython_port"])
    base_url = 'http://%s:%d/tree' % (HOST, iport)
    prj = cfg['prj']
    projname = prj.upper()

    return render_template("notebooks.html", cur="notebooks",
                           projname=projname, nb_url=base_url, dev=dev,
                           cache_time=cache_time)

if __name__ == "__main__":
    app.run(host=HOST, port=cfg['dashboard_port'])
