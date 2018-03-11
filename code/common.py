import os
import logging
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import pandas as pd
import keras as K
import parquet as pq
import tensorflow as tf
import numpy as np
from IPython import embed as breakpoint
import os
from scipy.sparse import csr_matrix
import itertools
from df_processes import *

import parquet as pq
import df_processes as dfp

def interactive_debugger():
    from IPython.core import ultratb
    sys.excepthook = ultratb.FormattedTB(mode='Verbose',
    color_scheme='Linux', call_pdb=1)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.max_colwidth', 100)
FORMAT = '%(asctime)s %(levelname)-5s: %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)
