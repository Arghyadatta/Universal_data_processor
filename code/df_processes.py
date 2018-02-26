from common import *

import os

from scipy.sparse import csr_matrix
import itertools


def to_list(A):
    return [A] if type(A) != list else A

def strip_col(df, cols):
    df.loc[:,col] = df[col].str.strip() for c in to_list(cols)
    return df

def to_lower(df, cols):
    df.loc[:,c] = df[c].str.lower() for c in to_list(cols)
    return df

def list_unroller(df,col):
    X = pd.concat([pd.DataFrame(v, index=np.repeat(k,len(v))) for k,v in df[col].to_dict().items()]).rename(columns = {0 : col})
    return X

def mapper(df, index_col, extra_columns = [], count_name = "COUNT", strip = True):

    index_col = to_list(index_col)

    if strip: strip_col(df, index_col)

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

