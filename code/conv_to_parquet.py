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
    out_fname = None
    nan_list = []
    nan_list_types = []    
    drop_nan = False

    def pack_to_parquet(self):
 
        drop_list = []

	if len(self.cols) != len(self.col_types) or len(self.cols) == 0 or len(self.col_types) == 0:
            raise Exception("Length of col_list and col_types are either 0 or they do not match")

        if len(self.nan_list) != len(self.nan_list_types):
            raise Exception("Length of nan_list and nan_list_types do not match")

        df = pd.read_csv(self.fname)

        for col in df.columns:
            if col not in self.cols and col != self.index_col:
                drop_list.append(col)

        df = df.drop(labels = drop_list, axis = 1)

        for col in self.cols:
            temp = df[df[col].isnull()]
            if len(temp) != 0:
                if col not in self.nan_list:
                    raise Exception(col, ' Has NULL ENTRIES. ADD to nan_list')

        if self.drop_nan == True:
            if self.index_col not in self.nan_list:
                df = df.dropna(subset = self.nan_list, axis = 1)
            else:
                raise Exception('Index Column contains NAN values')

        if self.drop_nan == False and len(self.nan_list) != 0:
            for i in range(0, len(self.nan_list)):
                df[self.nan_list[i]] = df[self.nan_list[i]].map(lambda x: to_format_for_nans(x, self.nan_list_types[i]))

        for i in range(0,len(self.cols)):
            if self.col_types[i] == 'str':
                df.loc[:,self.cols[i]] = df[self.cols[i]].str.strip()  # to remove trailing spaces 

        for index in range(0,len(self.cols)):
            df[self.cols[index]] = df[self.cols[index]].astype(self.col_types[index])

        df.set_index(self.index_col, inplace = True)
        pq.write_parquet(df, self.out_fname)


#D = packer()
#D.fname = 'toy_dx.txt'
#D.index_col = 'MASKED_REG_NO'
#D.cols = ['FACILITY_CONCEPT_ID', 'MASKED_REFERENCE_NO', 'SEQUENCE_NO', 'ICDX_DIAGNOSIS_CODE', 'ICDX_VERSION_NO']
#D.col_types = ['int64','int64','int64','str', 'str']
#D.out_fname = 'op_pq_file.txt'
#D.pack_to_parquet()
