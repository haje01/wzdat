import os

from wzdat.make_config import make_config

c = get_config()

# This starts plotting support always with matplotlib
c.IPKernelApp.pylab = 'inline'

c.NotebookApp.ip = '*'
c.NotebookApp.port = 8090
c.NotebookApp.open_browser = False

host = os.environ['WZDAT_HOST']
cfg = make_config()
if 'notebook_base_dir' in cfg:
    notedir = os.path.join(cfg['notebook_base_dir'], cfg['prj'])
else:
    soldir = os.environ['WZDAT_SOL_DIR']
    notedir = os.path.join(soldir, '__notes__', cfg['prj'])
c.NotebookManager.notebook_dir = notedir
dport = cfg['host_dashboard_port']
c.NotebookApp.webapp_settings = {'headers': {'X-Frame-Options': 'ALLOW-FROM '
                                             'http://%s:%s' % (host, dport)}}
