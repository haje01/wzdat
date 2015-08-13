#!/usr/bin/env python

import os
import re
import logging

from Queue import Empty

from markdown import markdown

from wzdat.util import div, remove_ansicolor, get_notebook_manifest_path
from wzdat import rundb
from wzdat.manifest import Manifest


CRON_PTRN =\
    re.compile(r'\s*\[\s*((?:[^\s@]+\s+){4}[^\s@]+)?\s*(?:@(.+))?\s*\]\s*(.+)')
IGNORE_DIRS = ('.ipynb_checkpoints', '.git')

from IPython.nbformat.current import read, NotebookNode, write
from wzdat.notebook_runner import NotebookRunner, NotebookError


IPYTHON_STARTUP_PATH = "/root/.ipython/profile_default/startup/01-wzdat.py"


class ManifestNotUsed(Exception):
    pass

CHECK_MANIFEST_USED = """
from wzdat.manifest import Manifest
from wzdat.ipynb_runner import ManifestNotUsed
_ = globals().copy()
if len([k for k, v in _.iteritems() if isinstance(v, Manifest)]) == 0:
    raise ManifestNotUsed()
"""


def check_manifest_used(runner):
    run_code(runner, CHECK_MANIFEST_USED, ManifestNotUsed())


def run_code(runner, code, exception=None):
    runner.shell.execute(code)
    reply = runner.shell.get_msg()
    status = reply['content']['status']
    if status == 'error':
        traceback_text = 'Code raised uncaught exception: \n' + \
            '\n'.join(reply['content']['traceback'])
        traceback_text = remove_ansicolor(traceback_text)
        logging.info(traceback_text)
    else:
        logging.info('Code returned')

    outs = list()
    while True:
        try:
            msg = runner.iopub.get_msg(timeout=1)
            if msg['msg_type'] == 'status':
                if msg['content']['execution_state'] == 'idle':
                    break
        except Empty:
            # execution state should return to idle before the queue becomes
            # empty, if it doesn't, something bad has happened
            raise

        content = msg['content']
        msg_type = msg['msg_type']

        # IPython 3.0.0-dev writes pyerr/pyout in the notebook format but uses
        # error/execute_result in the message spec. This does the translation
        # needed for tests to pass with IPython 3.0.0-dev
        notebook3_format_conversions = {
            'error': 'pyerr',
            'execute_result': 'pyout'
        }
        msg_type = notebook3_format_conversions.get(msg_type, msg_type)
        outs = _run_code_type(outs, runner, msg_type, content)

    if status == 'error':
        if exception is None:
            raise Exception('Code error')
        else:
            raise exception


def _run_code_type(outs, runner, msg_type, content):
    out = NotebookNode(output_type=msg_type)
    if msg_type in ('status', 'pyin', 'execute_input'):
        return outs
    elif msg_type == 'stream':
        out.stream = content['name']
        out.text = content['data']
    elif msg_type in ('display_data', 'pyout'):
        for mime, data in content['data'].items():
            try:
                attr = runner.MIME_MAP[mime]
            except KeyError:
                raise NotImplementedError('unhandled mime type: %s' % mime)

            setattr(out, attr, data)
    elif msg_type == 'pyerr':
        out.ename = content['ename']
        out.evalue = content['evalue']
        out.traceback = content['traceback']
    elif msg_type == 'clear_output':
        outs = list()
        return outs
    else:
        raise NotImplementedError('unhandled iopub message: %s' % msg_type)
    outs.append(out)
    return outs


def _run_init(r, path):
    if os.path.isfile(path):
        with open(path.encode('utf-8')) as f:
            init = f.read()
            run_code(r, init)


def update_notebook_by_run(path):
    rundb.reset_run(path)
    logging.debug(u'update_notebook_by_run {}'.format(path))
    mpath = get_notebook_manifest_path(path)
    has_manifest = os.path.isfile(mpath)

    # init runner
    nb = read(open(path.encode('utf-8')), 'json')
    r = NotebookRunner(nb, pylab=True)

    # run config & startup
    _run_init(r, IPYTHON_STARTUP_PATH)

    # run cells
    cellcnt = r.cellcnt
    rundb.start_run(path, cellcnt)
    err = None
    try:
        r.run_notebook(lambda cur: _progress_cell(path, cur))
        if has_manifest:
            check_manifest_used(r)
    except NotebookError, e:
        logging.debug("except NotebookError")
        err = unicode(e)
    except ManifestNotUsed, e:
        # if manifest not used by user, run it implicitly to save checksum
        logging.warn(u"Manifest exists for '{}', but manifest object was not "
                     "instantiated.".format(path))
        manifest = Manifest(True, mpath)
        manifest._write_checksums()
        # this is not an error, so write result
        write(r.nb, open(path.encode('utf-8'), 'w'), 'json')
    else:
        write(r.nb, open(path.encode('utf-8'), 'w'), 'json')
    finally:
        rundb.finish_run(path, err)
        return err


def rerun_notebook_cell(rv, r, cell, cnt):
    _type = cell['cell_type']
    if _type == 'code':
        r.run_cell(cell)
        code = cell['input']
        if '#!dashboard_view' in code:
            outs = r.nb['worksheets'][0]['cells'][cnt]['outputs']
            rv += outs
    elif _type == 'markdown':
        src = cell['source']
        if '<!--dashboard_view-->' in src:
            rv.append(div(markdown(src), 'view'))


def run_notebook_view_cell(rv, r, cell, cnt):
    _type = cell['cell_type']
    if _type == 'code':
        code = cell['input']
        if '#!dashboard_view' in code:
            r.run_cell(cell)
            outs = r.nb['worksheets'][0]['cells'][cnt]['outputs']
            rv += outs
            return True
    elif _type == 'markdown':
        src = cell['source']
        if '<!--dashboard_view-->' in src:
            rv.append(div(markdown(src), 'view'))
            return True


def get_view_cell_cnt(r):
    cnt = 0
    for _, cell in enumerate(r.iter_cells()):
        _type = cell['cell_type']
        if _type == 'code':
            code = cell['input']
            if '#!dashboard_view' in code:
                cnt += 1
        elif _type == 'markdown':
            src = cell['source']
            if '<!--dashboard_view-->' in src:
                cnt += 1
    return cnt


def _progress_cell(path, cur):
    rundb.update_run_info(path, cur)


def _parse_notebook_name(paths, scheds, groups, fnames, path, pjob, static):
    'collect file name if it confront cron rule'
    if path in paths:
        return
    fn = os.path.basename(path)
    m = CRON_PTRN.match(fn)
    if m is not None:
        g = m.groups()
        if len(g) == 0:
            return
        if g[0] is None and not static:
            return
        sched = '' if g[0] is None else g[0].replace('|', '/')
        if pjob.setall(sched):
            # logging.debug(u"Found '{}'.".format(path))
            gname = '' if g[1] is None else g[1]
            fname = g[2]
            paths.append(path)
            scheds.append(sched)
            groups.append(gname)
            fnames.append(fname)
