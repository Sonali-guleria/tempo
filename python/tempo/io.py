from __future__ import annotations

import logging
import os
from collections import deque
from typing import Optional

import pyspark.sql.functions as sfn
from pyspark.sql import SparkSession
from pyspark.sql.utils import ParseException

import tempo.tsdf as t_tsdf

logger = logging.getLogger(__name__)


def write(
    tsdf: t_tsdf.TSDF,
    spark: SparkSession,
    tabName: str,
    optimizationCols: Optional[list[str]] = None,
    mode: str = "append",
    options: dict = {},
    triggerOptions: dict = {},
) -> None:
    """
    param: tsdf: input TSDF object to write
    param: tabName: Delta output table name
    param: optimizationCols: list of columns to optimize on (time)
    param: mode: write mode for delta. default: "Append"
    param: options: Additional options for delta writes
    param: triggerOptions: The trigger settings of a streaming query define the timing of streaming data processing
    """
    # hilbert curves more evenly distribute performance for querying multiple columns for Delta tables
    spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

    ts_col = tsdf.ts_col
    partitionCols = tsdf.partitionCols
    view_df = tsdf.df.withColumn("event_dt", sfn.to_date(sfn.col(ts_col))).withColumn(
        "event_time",
        sfn.translate(sfn.split(sfn.col(ts_col).cast("string"), " ")[1], ":", "").cast(
            "double"
        ),
    )
    view_cols = deque(view_df.columns)
    view_cols.rotate(2)
    view_df = view_df.select(*list(view_cols))

    useDeltaOpt = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None


    if not view_df.isStreaming:
        if optimizationCols:
            optimizationCols = optimizationCols + ["event_time"]
        else:
            optimizationCols = ["event_time"]
        view_df.write.mode(mode).partitionBy("event_dt").options(**options).format(
            "delta"
        ).saveAsTable(table_name)

        if useDeltaOpt:
          try:
            spark.sql(
              f"optimize {table_name} zorder by {'(' + ','.join(partitionCols + optimization_cols) + ')'}"
            )
          except ParseException as e:
              logger.error(
                  f"Delta optimizations attempted, but was not successful.\nError: {e}"
              )
        else:
        logger.warning(
            "Delta optimizations attempted on a non-Databricks platform. "
            "Switch to use Databricks Runtime to get optimization advantages."
        )
        
    else:
        if "checkpointLocation" not in options:
            logger.warning(
                f"No Checkpoint location provided in options; using default /tmp/tempo/streaming_checkpoints/{table_name}"
            )
            options["checkpointLocation"] = (
                "/tmp/tempo/streaming_checkpoints/" + table_name
            )
        if triggerOptions:
            view_df.writeStream.trigger(**triggerOptions).partitionBy(
                "event_dt"
            ).options(**options).outputMode(mode).toTable(tabName)
        else:
            view_df.writeStream.partitionBy("event_dt").options(**options).outputMode(
                mode
            ).toTable(tabName)