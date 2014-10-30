import os

PRINT_LMAX = 50
TMP_PREFIX = 'wzdat-'
NAMED_TMP_PREFIX = 'wzdatnm-'
HDF_FILE_PREFIX = 'wzdathdf-'
HDF_FILE_EXT = 'h5'
SAVE_INFO_EXT = '.sinfo'
CHUNK_CNT = 500000
FORWARDER_LOG_PREFIX = '_wdfwd'

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SOLUTION_DIR = '/solution'
RUNNER_DB_PATH = "/var/wzdat/runner.db"
TMP_DIR = '/var/tmp/wzdat'
HDF_DIR = '/var/wzdat/hdf'
CONV_DIR = '/var/wzdat/conv'
DATA_DIR = '/logdata'
WZDAT_DIR = '/wzdat'
