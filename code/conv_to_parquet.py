from common import *



def to_format_for_nans(x, dtype):
    try:
        if dtype == 'date':
            return pd.to_datetime(x)
        elif dtype == 'str':
	    return str(x)
        elif dtype == 'int':
            return int(x)
        elif dtype == 'float':
            return float(x)
    except:
        return np.nan


class packer(object):

    fname = None
    index_col = None
    cols = []
    col_types = []
    out_fname = []
    nan_list = []
    nan_list_types = []    
    drop_nan = False

    def pack_to_parquet(self.fname, self.index_col, self.cols, self.col_types, self.out_fname, self. drop_nan = False):
	if len(self.cols) != len(self.col_types) or len(self.cols) == 0 or len(self.col_types) == 0:
            raise ValueError
        if len(self.nan_list) != len(self.nan_list_types):
            raise ValueError
        df = pd.read_csv("fname")
        for col in self.cols:
            temp = df[df[col].isnull()]
            if len(temp) != 0:
                if col not in self.nan_list;
                    raise ValueError
        if self.drop_nan == True:
            if self.index_col not in self.nan_list:
                df = df.dropna(subset = self.nan_list, axis = 1)
            else:
                raise ValueError
        if self.drop_nan == False and len(self.nan_list) != 0:
            for i in range(0, len(self.nan_list)):
                df[self.nan_list[i]] = df[self.nan_list[i]].map(lambda x: to_format_for_nans(x, self.nan_list_types[i]))
        for col in self.cols:
            df.loc[:,col] = df[col].str.strip()  # to remove trailing spaces 
        for index in range(0,len(self.cols)):
            df[cols[index]] = df[cols[index]].astype(self.col_types[index])
        df.set_index(index_col, inplace = True)
        pq.write_parquet(df, out_fname)
