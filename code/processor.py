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
