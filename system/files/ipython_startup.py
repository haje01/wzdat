import sys
sys.path.append('/solution')
import matplotlib as mlp
mlp.rcParams['font.family'] = u'NanumGothic'
mlp.rcParams['font.size'] = 10

import pandas as pd
pd.set_option('io.hdf.default_format', 'table')  # default hdf format 'table'

from pandas import Series, DataFrame
import numpy as np
import matplotlib.pyplot as plt
from wzdat.util import hdf_path, hdf_exists
from wzdat.ipynb_runner import NoDataFound

from wzdat.manifest import Manifest, ManifestNotExist
try:
    manifest_ = Manifest()
except ManifestNotExist:
    manifest_ = None

import os
