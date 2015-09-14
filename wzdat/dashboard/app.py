import os
import json
import logging
from collections import defaultdict
from datetime import timedelta

from flask import Flask, render_template, request, Response, redirect, url_for
from nbformat import reads

from wzdat.notebook_runner import NoDataFound
from wzdat.util import get_notebook_dir, parse_client_sdatetime,\
    get_client_datetime, ansi_escape, get_run_info, get_wzdat_host
from wzdat.rundb import get_cache_info, get_finder_info
from wzdat.jobs import cache_finder
from wzdat.make_config import make_config
from wzdat.ipynb_runner import notebook_outputs_to_html,\
    notebook_cell_outputs_to_html
from wzdat.const import IPYNB_VER

app = Flask(__name__)

cfg = make_config()
assert 'WZDAT_HOST' in os.environ
HOST = get_wzdat_host()
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
        ct = parse_client_sdatetime(ci)
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

    from wzdat.util import iter_dashboard_notebook
    iport = int(cfg["host_ipython_port"])
    base_url = 'http://%s:%d/tree' % (HOST, iport)
    nbdir = get_notebook_dir()
    groups = defaultdict(list)
    for nbpath, mip in iter_dashboard_notebook(nbdir):
        logging.debug(u"dashboard notebook {}".format(nbpath))
        sdir = os.path.dirname(nbpath).replace(nbdir, '')[1:]
        fn = os.path.basename(nbpath)
        url = os.path.join(base_url, sdir, fn)
        fname = os.path.splitext(os.path.basename(nbpath))[0]
        dashbrd_info = mip['dashboard']
        if isinstance(dashbrd_info, dict):
            gk = dashbrd_info['group'].decode('utf8')
            groups[gk].append((nbpath, url, fname))
        else:
            groups[''].append((nbpath, url, fname))
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
    nbdir = get_notebook_dir()
    nbpath = os.path.join(nbdir, nbpath)
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

    try:
        task = run_view_cell.AsyncResult(task_id)
        state = task.state
        if state == 'PENDING':
            return 'PROGRESS:0'
        logging.debug("{} - {}".format(task.state, task.status))
        if task.state == 'PROGRESS':
            return 'PROGRESS:' + str(task.result)
        outputs = task.get()
    except Exception, e:
        logging.debug('poll_view - ' + unicode(e))
        err = task.traceback
        logging.error(err)
        err = ansi_escape.sub('', err)
        return Response('<div class="view"><pre class="ds-err">%s</pre></div>'
                        % err)
    rv = []
    notebook_cell_outputs_to_html(rv, outputs, 'view')
    logging.debug(u"poll view  outputs {}".format(outputs))
    ret = '\n'.join(rv)
    return Response(ret)


@app.route('/start_rerun/<path:nbrpath>', methods=['POST'])
def start_rerun(nbrpath):
    logging.debug('start_rerun')
    nbapath = os.path.join(get_notebook_dir(), nbrpath)

    from wzdat.dashboard.tasks import rerun_notebook
    task = rerun_notebook.delay(nbapath)
    rv = nbrpath + '/' + task.task_id
    logging.debug(u'rv {}'.format(rv))
    return Response(rv)


@app.route('/poll_rerun/<path:task_info>', methods=['POST'])
def poll_rerun(task_info):
    task_id = 0
    # logging.debug(u'poll_rerun {}'.format(task_info))
    from wzdat import rundb
    from wzdat.dashboard.tasks import rerun_notebook
    task_id = task_info.split('/')[-1]
    nbrpath = '/'.join(task_info.split('/')[:-1])
    nbapath = os.path.join(get_notebook_dir(), nbrpath)

    try:
        task = rerun_notebook.AsyncResult(task_id)
        state = task.state
        if state == 'PENDING':
            logging.debug('task pending')
            return 'PROGRESS:0'
        elif task.state == 'PROGRESS':
            ri = rundb.get_run_info(nbapath)
            if ri is not None:
                logging.debug(u"run info exist")
                err = ri[4]
                logging.debug(u'err: {}'.format(err))
                if err == 'None':
                    cur = int(ri[2])
                    total = int(ri[3]) + 1
                    logging.debug(u'cur {} total {}'.format(cur, total))
                    return 'PROGRESS:' + str(cur/float(total))
                else:
                    logging.debug(u"ri error {}".format(err))
                    return Response('<div class="view"><pre '
                                    'class="ds-err">%s</pre></div>' % err)
            else:
                logging.debug(u"run info not exist")
                return 'PROGRESS:0'
        nodata = task.get()
        if nodata is not None:
            return Response(nodata)
    except NoDataFound, e:
        logging.debug(unicode(e))
        return Response(u'<div class="view"><pre class="ds-err">{}</pre></div>'
                        .format(unicode(e)))
    except Exception, e:
        logging.debug(str(e))
        logging.error(e)
        err = task.traceback
        logging.error(err)
        err = ansi_escape.sub('', err)
        return Response('<div class="view"><pre class="ds-err">%s</pre></div>'
                        % err)
    ret = _poll_rerun_output(nbapath)
    logging.debug(str(ret))
    return Response(ret)


def _poll_rerun_output(nbapath):
    rv = []
    with open(nbapath, 'r') as f:
        nb = reads(f.read(), IPYNB_VER)
        if len(nb) > 0:
            try:
                for cell in nb['cells']:
                    _type = cell['cell_type']
                    if _type == 'code' and 'outputs' in cell:
                        code = cell['source']
                        logging.debug(cell)
                        if '#!dashboard_view' in code:
                            notebook_cell_outputs_to_html(rv, cell['outputs'],
                                                          'view')
            except IndexError:
                logging.error(u"Incomplete notebook - {}".format(nbapath))
    return '\n'.join(rv)


def _collect_gnbs(gnbs, gk, groups):
    nbs = []
    nbdir = get_notebook_dir()
    logging.debug('_collect_gnbs ' + nbdir)
    # logging.debug(str(groups[gk]))
    for path, url, fname in groups[gk]:
        out = notebook_outputs_to_html(path)
        ri = get_run_info(path)
        logging.debug('get_run_info {}'.format(ri))
        if ri is not None:
            start, elapsed = _get_run_time(ri)
            cur = ri[2]
            total = ri[3]
            err = ri[4].decode('utf8')
            if err != 'None':
                out = '<div class="fail-result">Check error, fix it, '\
                      'and rerun.</div>'
                # logging.debug(u'err {}'.format(err))
            ri = (start, elapsed, cur, total, err)
        path = path.replace(nbdir, '')[1:]
        nbs.append((url, fname, out, ri, path))
    gnbs.append((gk, nbs))
    logging.debug('_collect_gnbs done')


def _get_run_time(ri):
    if ri[0] is not None:
        try:
            executed = get_client_datetime() - parse_client_sdatetime(ri[0])
            executed = timedelta(seconds=int(executed.total_seconds()))
        except ValueError:
            executed = None
    else:
        executed = None
    if ri[1] == 'None':
        elapsed = None
    else:
        elapsed = timedelta(seconds=int(float(ri[1])))
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
    logging.debug("notebooks - {}".format(base_url))

    return render_template("notebooks.html", cur="notebooks",
                           projname=projname, nb_url=base_url, dev=dev,
                           cache_time=cache_time)

if __name__ == "__main__":
    app.run(host=HOST, port=cfg['dashboard_port'])
