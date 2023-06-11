import unittest
from unittest.mock import patch

import pyspark.sql.functions as f
from tempo import TSDF
from tests.base import SparkTest


class AsOfJoinTest(SparkTest):
    def test_asof_join(self):
        """AS-OF Join with out a time-partition test"""

        # Construct dataframes
        tsdf_left = self.get_data_as_tsdf("left")
        tsdf_right = self.get_data_as_tsdf("right")
        dfExpected = self.get_data_as_sdf("expected")
        noRightPrefixdfExpected = self.get_data_as_sdf("expected_no_right_prefix")

        # perform the join
        joined_df = tsdf_left.asofJoin(
            tsdf_right, left_prefix="left", right_prefix="right"
        ).df
        non_prefix_joined_df = tsdf_left.asofJoin(
            tsdf_right, left_prefix="left", right_prefix=""
        ).df

        # joined dataframe should equal the expected dataframe
        self.assertDataFrameEquality(joined_df, dfExpected)
        self.assertDataFrameEquality(non_prefix_joined_df, noRightPrefixdfExpected)

        spark_sql_joined_df = tsdf_left.asofJoin(
            tsdf_right, left_prefix="left", right_prefix="right"
        ).df
        self.assertDataFrameEquality(spark_sql_joined_df, dfExpected)

    def test_asof_join_skip_nulls_disabled(self):
        """AS-OF Join with skip nulls disabled"""

        # fetch test data
        tsdf_left = self.get_data_as_tsdf("left")
        tsdf_right = self.get_data_as_tsdf("right")
        dfExpectedSkipNulls = self.get_data_as_sdf("expected_skip_nulls")
        dfExpectedSkipNullsDisabled = self.get_data_as_sdf(
            "expected_skip_nulls_disabled"
        )

        # perform the join with skip nulls enabled (default)
        joined_df = tsdf_left.asofJoin(
            tsdf_right, left_prefix="left", right_prefix="right"
        ).df

        # joined dataframe should equal the expected dataframe with nulls skipped
        self.assertDataFrameEquality(joined_df, dfExpectedSkipNulls)

        # perform the join with skip nulls disabled
        joined_df = tsdf_left.asofJoin(
            tsdf_right, left_prefix="left", right_prefix="right", skipNulls=False
        ).df

        # joined dataframe should equal the expected dataframe without nulls skipped
        self.assertDataFrameEquality(joined_df, dfExpectedSkipNullsDisabled)

    def test_sequence_number_sort(self):
        """Skew AS-OF Join with Partition Window Test"""

        # fetch test data
        tsdf_left = self.get_data_as_tsdf("left")
        tsdf_right = self.get_data_as_tsdf("right")
        dfExpected = self.get_data_as_sdf("expected")

        # perform the join
        joined_df = tsdf_left.asofJoin(tsdf_right, right_prefix="right").df

        # joined dataframe should equal the expected dataframe
        self.assertDataFrameEquality(joined_df, dfExpected)

    def test_partitioned_asof_join(self):
        """AS-OF Join with a time-partition"""
        with self.assertLogs(level="WARNING") as warning_captured:
            # fetch test data
            tsdf_left = self.get_data_as_tsdf("left")
            tsdf_right = self.get_data_as_tsdf("right")
            dfExpected = self.get_data_as_sdf("expected")

            joined_df = tsdf_left.asofJoin(
                tsdf_right,
                left_prefix="left",
                right_prefix="right",
                tsPartitionVal=10,
                fraction=0.1,
            ).df

            self.assertDataFrameEquality(joined_df, dfExpected)
            self.assertEqual(
                warning_captured.output,
                [
                    "WARNING:tempo.tsdf:You are using the skew version of the AS OF join. This "
                    "may result in null values if there are any values outside of the maximum "
                    "lookback. For maximum efficiency, choose smaller values of maximum lookback, "
                    "trading off performance and potential blank AS OF values for sparse keys"
                ],
            )

    def test_asof_join_nanos(self):
        """As of join with nanosecond timestamps"""

        # fetch test data
        tsdf_left = self.get_data_as_tsdf("left")
        tsdf_right = self.get_data_as_tsdf("right")
        dfExpected = self.get_data_as_sdf("expected")

        # perform join
        joined_df = tsdf_left.asofJoin(
            tsdf_right, left_prefix="left", right_prefix="right"
        ).df

        # compare
        self.assertDataFrameEquality(joined_df, dfExpected)

    def test_asof_join_tolerance(self):
        """As of join with tolerance band"""

        # fetch test data
        tsdf_left = self.get_data_as_tsdf("left")
        tsdf_right = self.get_data_as_tsdf("right")

        tolerance_test_values = [None, 0, 5.5, 7, 10]
        for tolerance in tolerance_test_values:
            # perform join
            joined_df = tsdf_left.asofJoin(
                tsdf_right,
                left_prefix="left",
                right_prefix="right",
                tolerance=tolerance,
            ).df

            # compare
            expected_tolerance = self.get_data_as_sdf(f"expected_tolerance_{tolerance}")
            self.assertDataFrameEquality(joined_df, expected_tolerance)

    def test_asof_join_sql_join_opt_and_bytes_threshold(self):
        """AS-OF Join with out a time-partition test"""
        with patch("tempo.tsdf.TSDF._TSDF__getBytesFromPlan", return_value=1000):
            # Construct dataframes
            tsdf_left = self.get_data_as_tsdf("left")
            tsdf_right = self.get_data_as_tsdf("right")
            dfExpected = self.get_data_as_sdf("expected")
            noRightPrefixdfExpected = self.get_data_as_sdf("expected_no_right_prefix")

            # perform the join
            joined_df = tsdf_left.asofJoin(
                tsdf_right, left_prefix="left", right_prefix="right", sql_join_opt=True
            ).df
            non_prefix_joined_df = tsdf_left.asofJoin(
                tsdf_right, left_prefix="left", right_prefix="", sql_join_opt=True
            ).df

            # joined dataframe should equal the expected dataframe
            self.assertDataFrameEquality(joined_df, dfExpected)
            self.assertDataFrameEquality(non_prefix_joined_df, noRightPrefixdfExpected)

            spark_sql_joined_df = tsdf_left.asofJoin(
                tsdf_right, left_prefix="left", right_prefix="right"
            ).df
            self.assertDataFrameEquality(spark_sql_joined_df, dfExpected)
            
    def test_streaming_asofJoin(self):
        tsdf_left = self.get_data_as_tsdf("left")
        tsdf_right = self.get_data_as_tsdf("right")

        tsdf_left.write(self.spark, "left_ts_table", mode="overwrite")
        tsdf_right.write(self.spark, "right_ts_table", mode="overwrite")

        df_trades = (
            self.spark.readStream.format("delta")
            .table("left_ts_table")
            .withColumn("event_ts", f.col("event_ts").cast("timestamp"))
            .withWatermark("event_ts", "30 minutes")
        )

        df_quotes = (
            self.spark.readStream.format("delta")
            .table("right_ts_table")
            .withColumn("event_ts", f.col("event_ts").cast("timestamp"))
            .withWatermark("event_ts", "30 minutes")
        )
        left = TSDF(df_trades, partition_cols=["symbol"], ts_col="event_ts")
        right = TSDF(df_quotes, partition_cols=["symbol"], ts_col="event_ts")
        joined_df = left.asofJoin(
            right,
            interim_table="rp_interim_results3",
            options={"checkpointLocation": "/tmp/tempo/streaming_checkpoints/ii"},
        )

        joined_df.df.writeStream.queryName("test").format("memory").outputMode(
            "append"
        ).start()
        for x in range(5):
            import time

            self.spark.sql("select * from test").show()
            time.sleep(10)


# MAIN
if __name__ == "__main__":
    unittest.main()