"""

    From runipy

"""

from __future__ import print_function

try:
    # python 2
    from Queue import Empty
except:
    # python 3
    from queue import Empty

import platform
from time import sleep
import logging
import os

from nbformat import NotebookNode
from nbformat.v4.convert import upgrade_outputs
from jupyter_client.manager import KernelManager

from wzdat.util import remove_ansicolor, system_memory_used, sizeof_fmt


class NoDataFound(Exception):
    def __init__(self, msg):
        super(NoDataFound, self).__init__()
        self.msg = msg

    def __unicode__(self):
        return self.msg

    def __str__(self):
        return self.msg.encode('utf8')


class NotebookError(Exception):
    pass


class NotebookRunner(object):
    # The kernel communicates with mime-types while the notebook
    # uses short labels for different cell types. We'll use this to
    # map from kernel types to notebook format types.

    MIME_MAP = {
        'image/jpeg': 'jpeg',
        'image/png': 'png',
        'text/plain': 'text',
        'text/html': 'html',
        'text/latex': 'latex',
        'application/javascript': 'html',
        'image/svg+xml': 'svg',
    }

    def __init__(self, nb, pylab=False, mpl_inline=False, working_dir=None):
        self.km = KernelManager()

        args = []

        if pylab:
            args.append('--pylab=inline')
            logging.warn('--pylab is deprecated and will be removed in a '
                         'future version')
        elif mpl_inline:
            args.append('--matplotlib=inline')
            logging.warn('--matplotlib is deprecated and will be removed in a '
                         'future version')

        cwd = os.getcwd()

        if working_dir:
            os.chdir(working_dir)

        self.km.start_kernel(extra_arguments=args)

        os.chdir(cwd)

        if platform.system() == 'Darwin':
            # There is sometimes a race condition where the first
            # execute command hits the kernel before it's ready.
            # It appears to happen only on Darwin (Mac OS) and an
            # easy (but clumsy) way to mitigate it is to sleep
            # for a second.
            sleep(1)

        self.kc = self.km.client()
        self.kc.start_channels()
        try:
            self.kc.wait_for_ready()
        except AttributeError:
            # IPython < 3
            self._wait_for_ready_backport()

        self.nb = nb

    def __del__(self):
        self.kc.stop_channels()
        self.km.shutdown_kernel(now=True)

    def run_cell(self, cell, cidx):
        '''
        Run a notebook cell and update the output of that cell in-place.
        '''
        logging.debug('running cell {}'.format(cidx))
        # logging.debug(u'cell.input {}'.format(cell.input))
        self.kc.execute(cell.source)
        reply = self.kc.get_shell_msg()
        status = reply['content']['status']
        max_mem = system_memory_used()
        logging.info('  memory used: {}'.format(sizeof_fmt(max_mem)))
        if status == 'error':
            traceback_text = 'Cell raised uncaught exception: \n' + \
                '\n'.join(reply['content']['traceback'])
            traceback_text = remove_ansicolor(traceback_text)
            if 'NoDataFound' not in traceback_text:
                logging.error(traceback_text)
        else:
            logging.debug('run_cell ok')

        outs = list()
        while True:
            try:
                msg = self.kc.get_iopub_msg(timeout=1)
                if msg['msg_type'] == 'status':
                    if msg['content']['execution_state'] == 'idle':
                        break
            except Empty:
                # execution state should return to idle before the queue
                # becomes empty,
                # if it doesn't, something bad has happened
                logging.error("empty exception")
                raise

            content = msg['content']
            msg_type = msg['msg_type']

            # IPython 3.0.0-dev writes pyerr/pyout in the notebook format but
            # uses error/execute_result in the message spec. This does the
            # translation needed for tests to pass with IPython 3.0.0-dev
            notebook3_format_conversions = {
                'error': 'pyerr',
                'execute_result': 'pyout'
            }
            msg_type = notebook3_format_conversions.get(msg_type, msg_type)

            out = NotebookNode(output_type=msg_type)

            #if 'execution_count' in content:
                #cell['prompt_number'] = content['execution_count']
                #out.prompt_number = content['execution_count']

            if msg_type in ('status', 'pyin', 'execute_input'):
                continue
            elif msg_type == 'stream':
                out.stream = content['name']
                if 'text' in content:
                    out.text = content['text']
                else:
                    out.text = content['data']
                # print(out.text, end='')
            elif msg_type in ('display_data', 'pyout'):
                for mime, data in content['data'].items():
                    try:
                        attr = self.MIME_MAP[mime]
                    except KeyError:
                        logging.error("unhandled mime")
                        raise NotImplementedError('unhandled mime type: %s' %
                                                  mime)

                    setattr(out, attr, data)
            elif msg_type == 'pyerr':
                out.ename = content['ename']
                out.evalue = content['evalue']
                out.traceback = content['traceback']
            elif msg_type == 'clear_output':
                outs = list()
                continue
            else:
                logging.error("unhandled iopub")
                raise NotImplementedError('unhandled iopub message: %s' %
                                          msg_type)
            outs.append(out)
        # NOTE: Ver 4 format still have 'pyout', Why?
        cell['outputs'] = upgrade_outputs(outs)

        logging.debug("status: {}".format(status))
        if status == 'error':
            if 'NoDataFound' in traceback_text:
                raise NoDataFound(traceback_text.split('\n')[-1])
            else:
                logging.debug(u"NotebookError raised")
                raise NotebookError(traceback_text)

    def iter_code_cells(self):
        '''
        Iterate over the notebook cells containing code.
        '''
        for cell in self.nb['cells']:
            if cell.cell_type == 'code':
                yield cell

    def iter_cells(self):
        '''
        Iterate over the notebook cells.
        '''
        for cell in self.nb['cells']:
            yield cell

    def clear_outputs(self):
        for cell in self.iter_cells():
            if 'outputs' in cell:
                cell['outputs'] = []

    @property
    def cellcnt(self):
        return len(self.nb['cells'])

    def run_notebook(self, memory_used=None, progress_cb=None,
                     skip_exceptions=False):
        '''
        Run all the cells of a notebook in order and update
        the outputs in-place.

        If ``skip_exceptions`` is set, then if exceptions occur in a cell, the
        subsequent cells are run (by default, the notebook execution stops).
        '''
        cur = 0
        if memory_used is not None:
            memory_used.append(system_memory_used())
        for cell in self.iter_code_cells():
            cur += 1
            try:
                if progress_cb is not None:
                    progress_cb(cur)
                self.run_cell(cell, cur)
                if memory_used is not None:
                    memory_used.append(system_memory_used())
            except NotebookError:
                if not skip_exceptions:
                    raise
