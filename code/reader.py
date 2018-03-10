from common import *


import itertools
from scipy.sparse import csr_matrix

def nested_to_sparse(df, num_columns, index=None):
    if not (index is None): df = df.reindex(index)
    assert len(df.columns) == 1

    series = df[df.columns[0]]

    N = num_columns

    series.loc[series.isnull()] = series[series.isnull()].apply(lambda d: [])

    idx_ptr = np.array([0] + list(series.apply(len).cumsum()))
    idx = np.array(list(itertools.chain.from_iterable(series)))
    data = np.ones(idx.shape)

    return csr_matrix((data, idx, idx_ptr), shape=(len(df), N)).tocoo()

class PqReader(object):
    def __init__(self, file, **kwargs):
        self.file = file
        self.__dict__.update(**kwargs)

    def load(self):
        self.data = pq.read_parquet(self.file)
        self.data = self.process(self.data)

    def process(self, data):
        return data

    def len_code(self):
        return len(self.data.columns)

    def as_matrix(self, index=None):
        df = self.data
        if not (index is None): df = df.reindex(index)
        if isinstance(df, pd.Series): df = df.to_frame()
        return df.as_matrix()

    def keras_input(self, name):
        import keras as K
        return K.Input(self.data.shape, name=name)


class SparsePqReader(PqReader):

    def load(self):
        self.data = pq.read_parquet(self.file)
        self.code = pq.read_parquet(self.file + ".code")
        self.data, self.code = self.process(self.data, self.code)

    def len_code(self):
        return len(self.code)

    def as_matrix(self, index=None):
        N = self.len_code()
        return nested_to_sparse(self.data, N, index)

    def keras_input(self, name):
        import keras as K
        N = self.len_code()
        return K.Input((N,), sparse=True, name=name )

    def process(self, data, code):
        return data, code


class PqCutoffReader(PqReader):

    def __init__(self, file, cutoffs, **kwargs):
        self.file = file
        self.cutoffs = cutoffs
        self.__dict__.update(**kwargs)

    def process(self, data):
        df = data
        COLS = []
        for name in df.columns:
            for c in self.cutoffs:
                S = ( df[name] > c ).astype(float)
                S.loc[ df[name].isnull() ] = np.nan
                S = S.rename( (name, c) )
                COLS.append( S )

        return pd.concat(COLS, axis=1).astype(float)



def _missing_to_indicator(series, replace_null=0.0, conditionally=True):
    mask = series.isnull()

    if conditionally and not mask.any():
        return series.to_frame()

    series = series.fillna(replace_null)
    mask = mask.astype("float")
    return pd.concat([series, mask], axis=1)

def _normalize(series):
    m, s = series.mean(), series.std()
    return (series - m) / s, m, s

def _window(series):
    M, m = series.max(), series.min()
    return ( series - m ) / (M - m), M, m


class ExprReader(PqReader):
    expression = NotImplemented

    def get_vars(self, data):
        V = {}
        for f in data.columns:
            V[f] = data[f]
        return V

    def process(self,data):
        OUT = eval(self.expression, self.get_vars(data),{})
        OUT, self.max, self.min = _window(OUT)
        return _missing_to_indicator(OUT, conditionally=True)

class AgeReader(PqReader):
    def process(self, data):
        Y = (data.ADMIT_DATE - data.MASKED_DOB).dt.days / 365.25
        Y, self.max, self.min = _window(Y)
        return _missing_to_indicator(Y, conditionally=True)


class FilterSparsePqReader(SparsePqReader):

    def process(self, data, code):
        filter = self.filter(data, code)
        new_code = code.reindex( code.index[~filter] )
        MAP = { i:n for n,i in enumerate(new_code.index) }
        new_code = new_code.reset_index()
        new_data = data.apply(self.remap, args=(MAP,))

        return new_data, new_code

    def filter(self, data, code): return []

    def remap(self, seqs, map):
        out = []
        for seq in seqs:
            if seq is np.nan:
                out.append(seq)
                continue
            new_map = np.array([map[s] for s in seq if s in map])

            if not len(new_map): new_map = np.nan
            out.append(new_map)
        return out


class MultiFilterSparsePqReader(SparsePqReader):

    def process(self, data, code):
        for name, filter, negate in self.filter(data, code):
            if negate: filter = ~filter
            new_code = code.reindex( code.index[filter] )

            MAP = { i:n for n,i in enumerate(new_code.index) }
            new_code = new_code.reset_index()

            new_data = data.apply(self.remap, args=(MAP,))

            self.add(name, new_data, new_code)

        return None, code

    def add(self, name, data, code):
        if hasattr(type(self), name):
            raise RuntimeError("Filter name '%s' clobbers attribute of class '%s'" % (name, str(type(self))))

        R = SparsePqReader(self, data = data, code = code, name = name)
        self.__dict__[name] = R


    def filter(self, data, code): yield []

    def remap(self, seqs, map):
        out = []
        for seq in seqs:
            if seq is np.nan:
                out.append(seq)
                continue
            new_map = np.array([map[s] for s in seq if s in map])

            if not len(new_map): new_map = np.nan
            out.append(new_map)
        return out



