#!/usr/bin/env python

import os
import re
import logging

from Queue import Empty
import codecs

from markdown import markdown

from wzdat.util import div, remove_ansicolor, ipython_start_script_path,\
    ansi_escape
from wzdat import rundb
from wzdat.const import IPYNB_VER
from wzdat.notebook_runner import NoDataFound


CRON_PTRN =\
    re.compile(r'\s*\[\s*((?:[^\s@]+\s+){4}[^\s@]+)?\s*(?:@(.+))?\s*\]\s*(.+)')
IGNORE_DIRS = ('.ipynb_checkpoints', '.git')

from nbformat import read, NotebookNode, write, reads
from wzdat.notebook_runner import NotebookRunner, NotebookError


def run_code(runner, code):
    logging.debug("run_code")
    runner.kc.execute(code)
    reply = runner.kc.get_shell_msg()
    status = reply['content']['status']
    if status == 'error':
        traceback_text = 'Code raised uncaught exception: \n' + \
            '\n'.join(reply['content']['traceback'])
        traceback_text = remove_ansicolor(traceback_text)
        logging.error(traceback_text)
    else:
        logging.debug('run_code ok')

    outs = list()
    while True:
        try:
            msg = runner.kc.get_iopub_msg(timeout=1)
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
        raise Exception(traceback_text)


def _run_code_type(outs, runner, msg_type, content):
    out = NotebookNode(output_type=msg_type)
    if msg_type in ('status', 'pyin', 'execute_input'):
        return outs
    elif msg_type == 'stream':
        out.stream = content['name']
        if 'text' in content:
            out.text = content['text']
        else:
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


def run_init(r, nbpath):
    initpath = ipython_start_script_path()
    init = u'__nbpath__ = u"{}"\n'.format(nbpath)
    if os.path.isfile(initpath):
        with codecs.open(initpath, 'r') as fp:
            init += fp.read()
    run_code(r, init)


def update_notebook_by_run(path):
    rundb.reset_run(path)
    logging.debug(u'update_notebook_by_run {}'.format(path))

    # init runner
    with codecs.open(path, 'r', 'utf8') as fp:
        nb = read(fp, IPYNB_VER)
    r = NotebookRunner(nb)
    r.clear_outputs()

    # run config & startup
    run_init(r, path)

    # run cells
    cellcnt = r.cellcnt
    rundb.start_run(path, cellcnt)
    err = None
    memory_used = []
    try:
        r.run_notebook(memory_used, lambda cur: _progress_cell(path, cur))
        run_code(r, "if 'manifest_' in globals() and manifest_ is not None: "
                 "manifest_._check_output_hdf()")
    except NotebookError, e:
        logging.debug("except NotebookError")
        err = unicode(e)
    except NoDataFound, e:
        logging.debug(unicode(e))
        with codecs.open(path, 'w', 'utf8') as fp:
            write(r.nb, fp, IPYNB_VER)
    else:
        with codecs.open(path, 'w', 'utf8') as fp:
            write(r.nb, fp, IPYNB_VER)
    finally:
        logging.debug("update_notebook_by_run finally")
        max_mem = max(memory_used)
        elapsed = rundb.finish_run(path, err)
        run_code(r, u"if 'manifest_' in globals() and manifest_ is not None: "
                 u"manifest_._write_result({}, {}, '''{}''')".
                 format(elapsed, max_mem, err))
        return err


def run_notebook_view_cell(rv, r, cell, idx):
    logging.debug('run_notebook_view_cell')
    _type = cell['cell_type']
    if _type == 'code':
        code = cell['source']
        if '#!dashboard_view' in code:
            try:
                r.run_cell(cell, idx)
            except NoDataFound:
                logging.debug("run_cell - NoDataFound")
                raise
            else:
                outs = r.nb['cells'][idx]['outputs']
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
            code = cell['source']
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


def notebook_outputs_to_html(path):
    logging.debug('notebook_outputs_to_html {}'.format(path.encode('utf-8')))
    rv = []
    with codecs.open(path, 'r', 'utf8') as f:
        nb = reads(f.read(), IPYNB_VER)
        if len(nb) > 0:
            try:
                for cell in nb['cells']:
                    cont = _cell_output_to_html(rv, cell)
                    if not cont:
                        break
            except IndexError:
                logging.error(u"Incomplete notebook - {}".format(path))
    return '\n'.join(rv)


def _cell_output_to_html_check_nodata(rv, code, outputs):
    if len(outputs) > 0:
        # logging.debug(outputs)
        for output in outputs:
            if 'ename' in output and output['ename'] == u'NoDataFound':
                _cls = _get_output_class(code)
                logging.debug("NoDataFound in a cell {}".format(_cls))
                notebook_cell_outputs_to_html(rv, outputs, _cls)
                return False
                # return _nodata_msg_to_html(rv, output)
    return True


def _get_output_class(code):
    if '#!dashboard_control' in code:
        return 'control'
    elif '#!dashboard_view' in code:
        return 'view'
    return ''


def _cell_output_to_html_vieworctrl(rv, code, outputs):
    _cls = ''
    if '#!dashboard_control' in code or '#!dashboard_view' in code:
        _cls = _get_output_class(code)
        notebook_cell_outputs_to_html(rv, outputs, _cls)


def _cell_output_to_html(rv, cell):
    '''Append html output for cell and return whether continue or not'''
    _type = cell['cell_type']
    # logging.debug("_cell_output_to_html {}".format(_type))
    _cls = ''
    if _type == 'code' and 'outputs' in cell:
        code = cell['source']
        outputs = cell['outputs']
        if not _cell_output_to_html_check_nodata(rv, code, outputs):
            return False

        _cell_output_to_html_vieworctrl(rv, code, outputs)
    elif _type == 'markdown':
        src = cell['source']
        if '<!--dashboard' in src:
            _cls = ''
            if '<!--dashboard_view-->' in src:
                _cls = 'view'
            rv.append(div(markdown(src), _cls))
    return True


def notebook_cell_outputs_to_html(rv, outputs, _cls):
    # check this is image cell
    logging.debug("notebook_cell_outputs_to_html {}".format(_cls))
    html = None
    img_cell = False
    for output in outputs:
        if 'data' in output.keys():
            if 'image/png' in output['data']:
                img_cell = True

    for output in outputs:
        opkeys = output.keys()
        if 'name' in opkeys:
            if 'text' in output:
                html = ''.join(output['text'])
        elif 'data' in opkeys:
            if 'image/png' in output['data']:
                imgdata = output['data']['image/png']
                html = '<img src="data:image/png;base64,%s"></img>' % imgdata
            elif 'text/html' in output['data']:
                html = ''.join(output['data']['text/html'])
            elif 'text/plain' in output['data'] and not img_cell:
                html = output['data']['text/plain']
        elif 'ename' in opkeys:
            html = ansi_escape.sub('', output['traceback'][-1])
        if html is not None:
            rv.append(div(html, _cls))
