from common import *

import os

from scipy.sparse import csr_matrix
import itertools


def as_list(A):
    if type(A) is list: return A
    elif type(A) is tuple:
        temp = []
        for i in A:
            temp.append(i)
        return temp
    elif type(A) is str:
        return [A] 

def strip_col(df, cols):
    for col in as_list(cols):
        df.loc[:,col] = df[col].str.strip()
    return df

def as_lower(df, cols):
    for col in to_list(cols):
        df.loc[:,col] = df[col].str.lower()
    return df

def list_unroller(df,col):
    X = pd.concat([pd.DataFrame(v, index=np.repeat(k,len(v))) for k,v in df[col].to_dict().items()]).rename(columns = {0 : col})
    return X

def mapper(df, index_col, extra_columns = [], count_name = "COUNT", strip = True):

    index_col = as_list(index_col)

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
        elif mode == 'ZERO':
            return 0
