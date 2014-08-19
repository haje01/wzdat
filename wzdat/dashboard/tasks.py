import os

from celery import Celery
from IPython.nbformat.current import read

from wzdat.const import SOLUTION_DIR
from wzdat.ipynb_runner import run_notebook_view_cell, get_view_cell_cnt,\
    run_code
from wzdat.make_config import make_config
from wzdat.notebook_runner import NotebookRunner

app = Celery('wdtask', backend='redis://localhost', broker='redis://localhost')
cfg = make_config()


@app.task()
def run_view_cell(nbpath, formname, kwargs):
    nb = read(open(nbpath), 'json')
    r = NotebookRunner(nb, pylab=True)
    total = float(get_view_cell_cnt(r) + 1)

    ipython_init_path = os.path.join(SOLUTION_DIR, 'ipython_init.py')
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
