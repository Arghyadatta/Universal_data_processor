import pyarrow.parquet as paq
import pyarrow as pa
import pandas as pd
import StringIO
import logging

from common import *

log = logging.getLogger(__name__)

def info(df):
    s = StringIO.StringIO()
    df.info(buf=s)
    return s.getvalue()


def write_parquet(df, destination):
    log.info( "writing %s ... " % (destination) )
    paq.write_table( pa.Table.from_pandas(df), destination )
    scheme = paq.read_schema( destination ).remove_metadata()

    log.info( "finished writing %s ... \n\n%s\n" % (destination, scheme) )

def read_parquet(source, columns=None, nthreads=4, use_pandas_metadata=True):
    scheme = paq.read_schema( source ).remove_metadata()
    log.info( "reading %s ... \n\n%s\n" % (source, scheme) )
    return paq.read_table(source, columns=columns, nthreads=nthreads,
        use_pandas_metadata=use_pandas_metadata).to_pandas()

