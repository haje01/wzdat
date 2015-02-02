import os
from urlparse import parse_qs
import imp
import zipfile

from celery import Celery
from IPython.nbformat.current import read

from wzdat.ipynb_runner import run_notebook_view_cell, get_view_cell_cnt,\
    run_code
from wzdat.make_config import make_config
from wzdat.notebook_runner import NotebookRunner
from wzdat.const import TMP_PREFIX
from wzdat.util import unique_tmp_path
from wzdat.selector import get_urls

app = Celery('wdtask', backend='redis://localhost', broker='redis://localhost')
cfg = make_config()
sol_dir = cfg['sol_dir']
data_dir = cfg['data_dir']


@app.task()
def run_view_cell(nbpath, formname, kwargs):
    nb = read(open(nbpath), 'json')
    r = NotebookRunner(nb, pylab=True)
    total = float(get_view_cell_cnt(r) + 1)

    ipython_init_path = os.path.join(sol_dir, 'ipython_init.py')
    if os.path.isfile(ipython_init_path):
        with open(ipython_init_path) as f:
            init = f.read()
            run_code(r, init)
    init = "from wzdat.dashboard.control import Form; %s = Form();"\
           "form.init(%s)" % (formname, repr(kwargs))
    try:
        run_code(r, init)
    except Exception:
        raise Exception(init)

    run_view_cell.update_state(state='PROGRESS', meta=1 / total)

    rv = []
    cnt = 0
    for i, cell in enumerate(r.iter_cells()):
        wasview = run_notebook_view_cell(rv, r, cell, i)
        if wasview:
            cnt += 1
            run_view_cell.update_state(state='PROGRESS',
                                       meta=(cnt + 1) / total)
    return rv


def _add_zip_flat(zf, abspath):
    odir = os.getcwd()
    _dir, filename = os.path.split(abspath)
    os.chdir(os.path.join(data_dir, _dir))
    zf.write(filename, abspath)
    os.chdir(odir)


def _select_files_dates(m, _start_dt, _end_dt):
    start_dt = end_dt = None
    grab_end = False
    for date in m.dates:
        if grab_end:
            end_dt = date
            break
        if str(date) == _start_dt:
            start_dt = date
        if str(date) == _end_dt:
            grab_end = True
    return start_dt, end_dt


def _select_files_condition(data, ftype):
    os.chdir('/solution')
    pkg = cfg['sol_pkg']
    prj = cfg['prj']
    mpath = '%s/%s/%s.py' % (pkg, prj, ftype)
    m = imp.load_source('%s' % ftype,  mpath)
    m.load_info()

    qs = parse_qs(data)
    _start_dt = qs['start_dt'][0]
    _end_dt = qs['end_dt'][0]
    _nodes = qs['nodes[]']
    _kinds = qs['kinds[]']

    start_dt, end_dt = _select_files_dates(m, _start_dt, _end_dt)
    nodes = []
    for node in m.nodes:
        if str(node) in _nodes:
            nodes.append(node)
    kinds = []
    for kind in m.kinds.group():
        if str(kind) in _kinds:
            kinds.append(kind)
    return m, start_dt, end_dt, nodes, kinds


def _progress(task_fn, rate):
    print '_progress', rate
    task_fn.update_state(state='PROGRESS', meta=rate)


@app.task()
def select_files(ftype, data):
    """Asynchronously select files."""
    print('select_files')
    m, start_dt, end_dt, nodes, kinds = _select_files_condition(data, ftype)
    files = m.files[start_dt:end_dt][nodes][kinds]
    sfiles = str(files)
    if 'size: ' not in sfiles:
        sfiles += '\nsize: ' + files.hsize
    return sfiles


@app.task()
def select_and_zip_files(ftype, data):
    print 'select_and_zip_files'
    print data
    files = data.split('\n')
    total = len(files) + 1
    prog = 1
    tmp_file, _ = unique_tmp_path(TMP_PREFIX, '.zip')
    with zipfile.ZipFile(tmp_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for _file in files:
            prog += 1
            _add_zip_flat(zf, _file)
            print prog / total
            _progress(select_and_zip_files, prog/total)

    filename = tmp_file.split(os.path.sep)[-1]
    _, TMP_URL = get_urls()
    return TMP_URL % (filename, filename)
