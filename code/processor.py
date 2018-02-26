
from common import *

import os

from scipy.sparse import csr_matrix
import itertools


def _as_list(x):
    return x if type(x) == list else [x]

def _strip(df, columns):
    for c in _as_list(columns):
        df.loc[:,c] = df[c].str.strip()
    return df

def _lower(df, columns):
    for c in _as_list(columns):
        df.loc[:,c] = df[c].str.lower()
    return df

def list_unroller(df,col):
    X = pd.concat([pd.DataFrame(v, index=np.repeat(k,len(v))) for k,v in df[col].to_dict().items()]).rename(columns = {0 : col})
    return X

def _factorize_helper(df, index_col, extra_columns = [], count_name = "COUNT",
        strip = True):

    index_col = _as_list(index_col)

    if strip: _strip(df, index_col)

    code = df.groupby(index_col).first().sort_index()
    code.loc[:,count_name] = df.groupby(index_col).size()
    code = code.reset_index().loc[:,index_col + extra_columns + ["COUNT"]]

    code = code.sort_values(index_col).reset_index(drop=True)
    index_array = [df[i].as_matrix() for i in index_col]
    factored = pd.factorize( pd.lib.fast_zip( index_array ), sort=True)[0]
    s = pd.Series(factored, index=df.index)

    return s, code

def get_val(index, df,col, mode = 'NAN'):
    try:
	return df.get_value(index, col)
    except:
        if mode == 'NAN':
            return np.nan
        elif mode == 'ONE':
            return 1

class RawProcessor(object):
    name = "Name"
    keys = []
    extra = []
    destination = []
    fname = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __call__(self, file, base=""):
        data = self.read(base)
        log.info("processing %s ..." % file)
        data = self.process(data)
        self.write(file, data)

    def process(self, data):
        data.sort_index(inplace=True)
        return data

    def read(self, base=""):
        f = os.path.join(base, self.fname)
        return pq.read_parquet(f, columns = _as_list(self.keys) + self.extra)

    def write(self, file, processed):
        pq.write_parquet(processed, file)

class SparseRawProcessor(RawProcessor):
    name = "Name"
    keys = []
    extra = []
    destination = []
    fname = None

    def process(self, data):
        factored, code = _factorize_helper(data, self.keys, self.extra)
        data = factored.groupby(level=0).apply(set).apply(sorted).rename(self.name).to_frame()
        data.sort_index(inplace=True)
        return data, code

    def __call__(self, data_file, code_file, base=""):
        data = self.read(base)
        log.info("processing %s ..." % data_file)
        data = self.process(data)
        print data[0]
        print data[1]
        self.write(data_file, code_file, data)

    def write(self, data_file, code_file, processed):
        pq.write_parquet(processed[0], data_file)
        pq.write_parquet(processed[1], code_file)

