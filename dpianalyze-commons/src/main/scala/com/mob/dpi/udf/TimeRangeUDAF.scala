package com.mob.dpi.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 计算时间范围UDAF, 以每时间戳-300s作为时间窗口, 并合并所有交集的窗口记录
 *
 * 例:
 * 时间戳为 ->
 * 1000, 1100, 2000, 3000
 * 获取时间窗口为 ->
 * [700-1000], [800-1100], [1700-2000], [2700-3000]
 * 合并输出为 ->
 * [700-1100], [1700-2000], [2700-3000]
 * 输出格式为 ->
 *  imei | time_range
 *   i1  | 700,1100|1700,2000|2700,3000
 */
object TimeRangeUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("input", LongType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("range",
      ArrayType(StructType(StructField("min", LongType) :: StructField("max", LongType) :: Nil))) :: Nil)

  override def dataType: DataType = org.apache.spark.sql.types.StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Row]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0)) return
    val inputRow: Row = Row(input.getLong(0) - 300, input.getLong(0))
    val rangeArr: Seq[Row] = buffer.getSeq[Row](0)

    val tempArr: mutable.Seq[Row] = if (rangeArr.isEmpty) {
      ArrayBuffer(inputRow)
    } else {
      mergeTimeRange(inputRow +: rangeArr: _*)
    }

    buffer(0) = tempArr.toArray
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.getSeq(0).nonEmpty && buffer2.getSeq(0).nonEmpty) {
      buffer1(0) = mergeTimeRange(buffer1.getSeq[Row](0) ++ buffer2.getSeq[Row](0)
        : _*).toArray
    } else if (buffer1.getSeq(0).isEmpty) {
      buffer1(0) = buffer2.getSeq[Row](0)
    }

  }

  override def evaluate(buffer: Row): Any = {
    buffer.getSeq[Row](0).map(r => s"${r.getLong(0)},${r.getLong(1)}").mkString("|")
  }

  def mergeTimeRange(row: Row*): ArrayBuffer[Row] = {
    if (row.isEmpty) {
      return ArrayBuffer.empty[Row]
    }
    var _previous: Row = null
    val _tmp = row.sortBy(
      _.getLong(0)
    ).foldLeft(
      ArrayBuffer[Row]()) { (buffer: ArrayBuffer[Row], row: Row) =>
      if (_previous == null) {
        _previous = row
      }
      else if (_previous.getLong(1) < row.getLong(0)) {
        buffer += _previous
        _previous = row
      }
      else {
        _previous = Row(_previous.getLong(0), row.getLong(1))
      }
      buffer
    }

    _tmp += _previous
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    import spark.implicits._

    spark.udf.register("seconds_limit", TimeRangeUDAF)

    val sourceDF = Seq(
      "i1" -> 10100,
      "i1" -> 10000,
      "i1" -> 30000,
      "i1" -> 31000,
      "i1" -> 30200,
      "i1" -> 30300,
      "i1" -> 30500,
      "i1" -> 30700,
      "i1" -> 31500
    ).toDF(
      "id", "start_time")

    sourceDF.show(false)

    sourceDF.groupBy("id")
      .agg(callUDF("seconds_limit", $"start_time")).as("time_range").show(false)

    spark.stop()

  }
}
