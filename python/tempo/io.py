import os
import logging
from collections import deque

import pyspark.sql.functions as f

logger = logging.getLogger(__name__)


def write(tsdf, spark, table_name, optimization_cols = None, mode = "append", options = {}, table_format = "delta"):
  """
  param: tsdf input TSDF object to write
  param: table_name Delta output table name
  param: mode Delta output mode. default append
  param: optimization_cols list of columns to optimize on (time)
  param: Additional options for the writes
  param: table_format delta by default 
  """
  # hilbert curves more evenly distribute performance for querying multiple columns for Delta tables
  spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

  df = tsdf.df
  ts_col = tsdf.ts_col
  partitionCols = tsdf.partitionCols
  if optimization_cols:
     optimization_cols = optimization_cols + ['event_time']
  else:
     optimization_cols = ['event_time']

  useDeltaOpt = (os.getenv('DATABRICKS_RUNTIME_VERSION') != None)
  
  view_df = df.withColumn("event_dt", f.to_date(f.col(ts_col))) \
      .withColumn("event_time", f.translate(f.split(f.col(ts_col).cast("string"), ' ')[1], ':', '').cast("double"))
  view_cols = deque(view_df.columns)
  view_cols.rotate(1)
  view_df = view_df.select(*list(view_cols))
  
  if not view_df.isStreaming:
    view_df.write.mode(mode).partitionBy("event_dt").options(**options).format(table_format).saveAsTable(table_name)
    if useDeltaOpt:
      if table_format.lower() == "delta":
          try:
             spark.sql("optimize {} zorder by {}".format(table_name, "(" + ",".join(partitionCols + optimization_cols) + ")"))
          except Exception as e: 
             logger.error("Delta optimizations attempted, but was not successful.\nError: {}".format(e))
      else:
        logger.warning("Delta optimizations attempted on a non-delta format detected. Switch to Delta format to use Databricks Runtime to get optimization advantages.")
    else:
        logger.warning("Delta optimizations attempted on a non-Databricks platform. Switch to use Databricks Runtime to get optimization advantages.")
  else:
    view_df.writeStream.partitionBy("event_dt").format(table_format).options(**options).outputMode(mode).toTable(table_name)

    #joined_df.writeStream.options(**options).queryName("Interim_Results").toTable(interim_table)