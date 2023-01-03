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
    mode="append",
    options={},
    trigger_options={},
):
    """
    param: tsdf: input TSDF object to write
    param: table_name Delta output table name
    param: optimization_cols list of columns to optimize on (time)
    """
    # hilbert curves more evenly distribute performance for querying multiple columns for Delta tables
    spark.conf.set("spark.databricks.io.skipping.mdc.curve", "hilbert")

    df = tsdf.df
    ts_col = tsdf.ts_col
    partitionCols = tsdf.partitionCols
    view_df = tsdf.df.withColumn("event_dt", f.to_date(f.col(ts_col))).withColumn(
        "event_time",
        f.translate(f.split(f.col(ts_col).cast("string"), " ")[1], ":", "").cast(
            "double"
        ),
    )

    if not view_df.isStreaming:
        if optimization_cols:
            optimization_cols = optimization_cols + ["event_time"]
        else:
            optimization_cols = ["event_time"]

        view_cols = deque(view_df.columns)
        view_cols.rotate(1)
        view_df = view_df.select(*list(view_cols))

        view_df = df.withColumn(
            "event_time",
            f.translate(f.split(f.col(ts_col).cast("string"), " ")[1], ":", "").cast(
                "double"
            ),
        )
        view_cols = deque(view_df.columns)
        view_cols.rotate(1)
        view_df = view_df.select(*list(view_cols))

        view_df.write.mode(mode).partitionBy("event_dt").options(**options).format(
            "delta"
        ).saveAsTable(table_name)

        try:
            spark.sql(
                "optimize {} zorder by {}".format(
                    table_name, "(" + ",".join(partitionCols + optimization_cols) + ")"
                )
            )
        except ParseException as e:
            logger.error(
                "Delta optimizations attempted, but was not successful.\nError: {}".format(
                    e
                )
            )
    else:
        if "checkpointLocation" not in options:
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
