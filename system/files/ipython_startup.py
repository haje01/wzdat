import os
import sys
sys.path.append('/solution')
import matplotlib
import matplotlib as mlp
mlp.rcParams['font.family'] = u'NanumGothic'
mlp.rcParams['font.size'] = 10

import pandas as pd
pd.set_option('io.hdf.default_format', 'table')  # default hdf format 'table'

from pandas import Series, DataFrame
import numpy as np
import matplotlib.pyplot as plt
from wzdat.util import hdf_path, hdf_exists, get_notebook_rpath, get_notebook_dir
from wzdat.notebook_runner import NoDataFound

from wzdat.manifest import Manifest, ManifestNotExist
try:
    nbrpath = __nbpath__ if '__nbpath__' in globals() else\
        get_notebook_rpath(False)
    nbapath = os.path.join(get_notebook_dir(), nbrpath)
    manifest_ = Manifest(True, nbapath)
except ManifestNotExist:
    manifest_ = None
import os
