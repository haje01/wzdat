import os
from urlparse import parse_qs
import imp
import zipfile

from celery import Celery
from IPython.nbformat.current import read

from wzdat.ipynb_runner import run_notebook_view_cell, get_view_cell_cnt,\
    run_code, update_notebook_by_run
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
def rerun_notebook(nbpath):
    print('rerun_notebook {}'.format(nbpath))
    rerun_notebook.update_state(state='PROGRESS', meta=0)
    update_notebook_by_run(nbpath)
    rerun_notebook.update_state(state='PROGRESS', meta=1)


@app.task()
def run_view_cell(nbpath, formname, kwargs):
    print('run_view_cell {}'.format(formname))
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
    print('init {}'.format(init))
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
        print '=='
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
    m.load_info(lambda rate: _progress(select_files, rate))

    qs = parse_qs(data)
    _start_dt = qs['start_dt'][0] if 'start_dt' in qs else None
    _end_dt = qs['end_dt'][0] if 'end_dt' in qs else None
    print _start_dt, _end_dt
    _nodes = qs['nodes[]'] if 'nodes[]' in qs else None
    _kinds = qs['kinds[]'] if 'kinds[]' in qs else None

    start_dt, end_dt = _select_files_dates(m, _start_dt, _end_dt)
    print '----------'
    print start_dt, end_dt
    nodes = []
    if _nodes is not None:
        for node in m.nodes:
            if str(node) in _nodes:
                nodes.append(node)
    kinds = []
    if _kinds is not None:
        for kind in m.kinds.group():
            if str(kind) in _kinds:
                kinds.append(kind)
    print nodes, kinds
    return m, start_dt, end_dt, nodes, kinds


def _progress(task_fn, rate):
    print '_progress', rate
    task_fn.update_state(state='PROGRESS', meta=rate)


@app.task()
def select_files(ftype, data):
    """Asynchronously select files."""
    print('select_files')
    m, start_dt, end_dt, nodes, kinds = _select_files_condition(data, ftype)
    if start_dt is not None or end_dt is not None:
        print 'date filter'
        files = m.files[start_dt:end_dt]
    else:
        print 'no date filter'
        files = m.files
    print nodes, kinds
    if len(nodes) > 0:
        print 'node filter'
        files = files[nodes]
    if len(kinds) > 0:
        print 'kind filter'
        files = files[kinds]
    sfiles = str(files)
    if 'size: ' not in sfiles:
        sfiles += '\nsize: ' + files.hsize
    return sfiles


@app.task()
def zip_files(ftype, data):
    """Asynchronously zip files."""
    print 'zip_files'
    print data
    files = data.split('\n')
    total = float(len(files))
    prog = 1
    tmp_file, _ = unique_tmp_path(TMP_PREFIX, '.zip')
    with zipfile.ZipFile(tmp_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for _file in files:
            prog += 1
            _add_zip_flat(zf, _file)
            print prog / total
            _progress(zip_files, prog/total)

    filename = tmp_file.split(os.path.sep)[-1]
    _, TMP_URL = get_urls()
    return TMP_URL % (filename, filename)
