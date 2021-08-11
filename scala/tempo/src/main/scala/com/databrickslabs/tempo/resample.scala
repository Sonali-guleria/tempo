package com.databrickslabs.tempo

import org.apache.spark.sql.functions._

/**
  * The following object contains methods for resampling (up to millions of time series in parallel).
  */
object resample {

// define global frequency options
val SEC = "sec"
val MIN = "min"
val HR = "hr"
val DAY = "day"

// define global aggregate function options for downsampling
// these are legacy settings which are still supported which are replaced by floor, min, max, ceil
val CLOSEST_LEAD = "closest_lead"
val MIN_LEAD = "min_lead"
val MAX_LEAD = "max_lead"
val MEAN_LEAD = "mean_lead"

val FLOOR = "floor"
val MINIMUM = "min"
val MAXIMUM = "max"
val AVERAGE = "mean"
val CEILING = "ceil"

val freq_dict = Map("sec" -> "seconds", "min" -> "minutes", "hr" -> "hours", "day" -> "days")

val allowableFreqs = List(SEC, MIN, HR, DAY)
val allowableFuncs = List(FLOOR, MINIMUM, MAXIMUM, AVERAGE, CEILING)


  /**
    *
    * @param tsdf - TSDF object as input
    * @param freq - frequency at which to upsample
    * @return - return a TSDF with a new aggregate key (called agg_key)
    */
  private[tempo] def __appendAggKey(tsdf : TSDF, freq : String) : TSDF = {
    var df = tsdf.df

    val parsed_freq = checkAllowableFreq(tsdf, freq)
    val period = parsed_freq._1
    val units = freq_dict.get(parsed_freq._2) match {
      case Some(unit) => unit
      case None => throw new IllegalArgumentException("Invalid unit provided - please provide a unit of the form min/minute(s), sec/second(s), hour(s), or day(s)")
    }
    val agg_window = window(col(tsdf.tsColumn.name), s"$period $units")

    df = df.withColumn("agg_key", agg_window)
    return TSDF(df, tsColumnName= tsdf.tsColumn.name, partitionColumnNames = tsdf.partitionCols.map(x => x.name))
  }

  /**
    *
    * @param tsdf - input TSDF object
    * @param freq - aggregate function
    * @param func - aggregate function
    * @param metricCols - columns used for aggregates
    * return - TSDF object with newly aggregated timestamp as ts_col with aggregated values
    */
  def rs_agg(tsdf : TSDF, freq : String, func : String, metricCols : Option[List[String]] = None) : TSDF =  {

       validateFuncExists(func)

       var tsdf_w_aggkey = __appendAggKey(tsdf, freq)
       val df = tsdf_w_aggkey.df
       var adjustedMetricCols = List("")

       var groupingCols = tsdf.partitionCols.map(x => x.name) :+ "agg_key"

       adjustedMetricCols = metricCols match {
         case Some(s) => metricCols.get
         case None => (df.columns.toSeq diff (groupingCols :+ tsdf.tsColumn.name)).toList
       }


       var metricCol = col("")
       var groupedRes = df.withColumn("struct_cols", struct( (Seq(tsdf.tsColumn.name) ++ adjustedMetricCols.toSeq).map(col): _*)).groupBy(groupingCols map col: _*)

       var res = groupedRes.agg(min("struct_cols").alias("closest_data")).select("*", "closest_data.*").drop("closest_data")

       if ((func == FLOOR) || (func == CLOSEST_LEAD))  {
         groupedRes = df.withColumn("struct_cols", struct( (Seq(tsdf.tsColumn.name) ++ adjustedMetricCols.toSeq).map(col): _*)).groupBy(groupingCols map col: _*)
         res = groupedRes.agg(min("struct_cols").alias("closest_data")).select("*", "closest_data.*").drop("closest_data")
       } else if ((func == AVERAGE) || (func == MEAN_LEAD)) {
         val exprs = adjustedMetricCols.map(k => avg(df(s"$k")).alias("avg_" + s"$k"))
         res = df.groupBy(groupingCols map col: _*).agg(exprs.head, exprs.tail:_*)
       } else if ((func == MINIMUM) || (func == MIN_LEAD))  {
         val exprs = adjustedMetricCols.map(k => min(df(s"$k")).alias("avg_" + s"$k"))
         res = df.groupBy(groupingCols map col: _*).agg(exprs.head, exprs.tail:_*)
       } else if ((func == MAXIMUM) || (func == MAX_LEAD)) {
         val exprs = adjustedMetricCols.map(k => max(df(s"$k")).alias("avg_" + s"$k"))
         res = df.groupBy(groupingCols map col: _*).agg(exprs.head, exprs.tail:_*)
       }

       res = res.drop(tsdf.tsColumn.name).withColumnRenamed("agg_key", tsdf.tsColumn.name).withColumn(tsdf.tsColumn.name, col(tsdf.tsColumn.name + ".start"))
       return(TSDF(res, tsColumnName = tsdf.tsColumn.name, partitionColumnNames = tsdf.partitionCols.map(x => x.name)))
  }


       def checkAllowableFreq(tsdf : TSDF, freq : String) : (String, String) = {


         if (!allowableFreqs.contains(freq) ) {

           var units = ""
           var periods = ""

           try {
             periods = freq.toLowerCase().split(" ")(0).trim()
             units = freq.toLowerCase().split(" ")(1).trim()
           } catch {
             case e : IllegalArgumentException => println("Allowable grouping frequencies are sec (second), min (minute), hr (hour), day. Reformat your frequency as <integer> <day/hour/minute/second>")
           }

           if (units.startsWith(SEC)) {
             return (periods, SEC)
           } else if (units.startsWith(MIN)) {
             return (periods, MIN)
           } else if (units.startsWith("hour")) {
             return (periods, "hour")
           } else if (units.startsWith(DAY)) {
             return (periods, DAY)
           } else {
             return ("1", "minute")
           }
         }
         else {
             return ("1", freq)
           }
         }

       def validateFuncExists(func : String): Unit = {
         if (func == None) {
           throw new IllegalArgumentException("Aggregate function missing. Provide one of the allowable functions: " + List(CLOSEST_LEAD, MIN_LEAD, MAX_LEAD, MEAN_LEAD, FLOOR, MINIMUM, MAXIMUM, CEILING, AVERAGE).mkString(","))
         } else if (!List(CLOSEST_LEAD, MIN_LEAD, MAX_LEAD, MEAN_LEAD, FLOOR, MINIMUM, MAXIMUM, CEILING, AVERAGE).contains(func)) {
           throw new IllegalArgumentException("Aggregate function is not in the valid list. Provide one of the allowable functions: " + List(CLOSEST_LEAD, MIN_LEAD, MAX_LEAD, MEAN_LEAD, FLOOR, MINIMUM, MAXIMUM, CEILING, AVERAGE).mkString(","))
         }
       }
  }