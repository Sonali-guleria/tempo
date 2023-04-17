from __future__ import annotations

import logging
from collections import deque

import pyspark.sql.functions as f
import tempo
from pyspark.sql import SparkSession
from pyspark.sql.utils import ParseException

logger = logging.getLogger(__name__)


def write(
    tsdf: tempo.TSDF,
    spark: SparkSession,
    table_name: str,
    optimization_cols: list[str] = None,
    mode: str = "append",
    options: dict = {},
    trigger_options: dict = {},
):
    """
    param: tsdf: input TSDF object to write
    param: table_name: Delta output table name
    param: optimization_cols: list of columns to optimize on (time)
    param: mode: write mode for delta. default: "Append"
    param: options: Additional options for delta writes
    param: trigger_options: The trigger settings of a streaming query define the timing of streaming data processing
    """
    # hilbert curves more evenly distribute performance for querying multiple columns for Delta tables
    spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

    ts_col = tsdf.ts_col
    partitionCols = tsdf.partitionCols
    view_df = tsdf.df.withColumn("event_dt", f.to_date(f.col(ts_col))).withColumn(
        "event_time",
        f.translate(f.split(f.col(ts_col).cast("string"), " ")[1], ":", "").cast(
            "double"
        ),
    )
    view_cols = deque(view_df.columns)
    view_cols.rotate(2)
    view_df = view_df.select(*list(view_cols))

    if not view_df.isStreaming:
        if optimization_cols:
            optimization_cols = optimization_cols + ["event_time"]
        else:
            optimization_cols = ["event_time"]
        view_df.write.mode(mode).partitionBy("event_dt").options(**options).format(
            "delta"
        ).saveAsTable(table_name)

        try:
            spark.sql(
                f"optimize {table_name} zorder by {'(' + ','.join(partitionCols + optimization_cols) + ')'}"
            )
        except ParseException as e:
            logger.error(
                f"Delta optimizations attempted, but was not successful.\nError: {e}"
            )
    else:
        if "checkpointLocation" not in options:
            logger.warning(
                f"No Checkpoint location provided in options; using default /tmp/tempo/streaming_checkpoints/{table_name}"
            )
            options["checkpointLocation"] = (
                "/tmp/tempo/streaming_checkpoints/" + table_name
            )
        if trigger_options:
            view_df.writeStream.trigger(**trigger_options).partitionBy(
                "event_dt"
            ).options(**options).outputMode(mode).toTable(table_name)
        else:
            view_df.writeStream.partitionBy("event_dt").options(**options).outputMode(
                mode
            ).toTable(table_name)
