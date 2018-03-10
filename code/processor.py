from common import *

class BaseProcessor(object):
    name = "name"
    keys = None
    extra = None
    destination = None
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

class SparseBaseProcessor(RawProcessor):
    name = "name"
    keys = None
    extra = None
    destination = None
    fname = None

    def process(self, data):
        factored, code = mapper(data, self.keys, self.extra)
        data = factored.groupby(level=0).apply(set).apply(sorted).rename(self.name).to_frame()
        data.sort_index(inplace=True)
        return data, code

    def __call__(self, data_file, code_file, base=""):
        data = self.read(base)
        log.info("processing %s ..." % data_file)
        data = self.process(data)
        self.write(data_file, code_file, data)

    def write(self, data_file, code_file, processed):
        pq.write_parquet(processed[0], data_file)
        pq.write_parquet(processed[1], code_file)

class MapperProcessor(SparseRawProcessor):

    new_key = None
    src_map_file = None    
    src_map_key = None
    src_check_col = None
    src_check_val = None

    def process(self,data):
        if len(self.src_check_col) != len(self.src_check_val): raise ValueError
        src_map = pq.read_parquet(self.src_map_file)
        src_check_col = dfp.as_list(self.src_check_col)
        for i in range(0,len(src_check_col)):
            src_map = src_map[src_map[src_check_col[i]].isin(self.src_check_val[i])]
        data[self.src_map_key] = data[self.keys].map(src_map[src_map_key]).groupby(level=0).first()
        data = data.dropna(subset=[self.src_map_key], axis = 1)
        data.loc[:,self.src_map_key] = data[self.src_map_key].str.lower()
        data, code = mapper(data, self.src_map_key, extra_columns=self.extra)
        data = data.groupby(level=0).apply(set).apply(sorted).rename(self.name).to_frame()
        data.sort_index(inplace = True)
        return data, code

class MapperProcessorByDate(MapperProcessor):
    
    src_date_file = None
    src_date_col = None
    data_date_col =None

    def process(self,data):
        dates = pq.read_parquet(self.src_data_file)
        data[self.src_date_col] = data.index.map(lambda x: get_value(x, dates, self.src.data_col))
        data['DAYS'] = (data[self.src_date_col] - data[self.data_date_col]).dt.days
        data = data[data.DAYS > -1]
        data.DAYS = data.DAYS + 1
        super(MapperProcessorByDate, self).process(data)


