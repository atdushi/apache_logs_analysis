package org.example

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object App {
  private val count_bots = true

  def main(args: Array[String]): Unit = {
    val spark = init()

    //    print(new DateTime(spark.sparkContext.startTime))

    var df = extract(spark)

    //    df.printSchema()

    //    df.show()

    df = cleansing(df)

    df = enrich(df)

    df = load(df, "./result");

    df.printSchema()

    df.show(truncate = false)
  }

  private def init(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Task3")
      //      .withExtensions(new CassandraSparkExtensions)
      //      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      //      .config("spark.cassandra.connection.host", "localhost")
      //      .config("spark.cassandra.connection.port", 9042)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  private def extract(spark: SparkSession, table: String = "apache_logs"): DataFrame = {
    //    val df = spark
    //      .read
    //      .format("org.apache.spark.sql.cassandra")
    //      .options(Map("keyspace" -> "my_keyspace", "table" -> table))
    //      .load()

    val df = spark.read.cassandraFormat("apache_logs", "my_keyspace").load()

    df
  }

  private def cleansing(df: DataFrame): DataFrame = {
    if (!count_bots)
      df.filter(col("is_bot") === false)
    else
      df
  }

  /*
    Assumption: user == remote_host
   */
  private def enrich(in: DataFrame): DataFrame = {
    var df = in.withColumn("total_distinct_hosts", size(collect_set("remote_host").over()))
    df = df.withColumn("distinct_hosts_by_device", size(collect_set("remote_host").over(Window.partitionBy("device_model"))))
    df = df.withColumn("total_request_count", size(collect_set("request_line").over()))
    df = df.withColumn("request_count_by_device", count(col("request_line")).over(Window.partitionBy("device_model")))
    df = df.withColumn("bad_response_count_by_device", count(when(col("final_status") =!= 200, true)).over(Window.partitionBy("device_model")))
    df = df.withColumn("request_count_by_device_browser",
      count("browser_family").over(Window.partitionBy("device_model", "browser_family")))

    //    df.select("device_model", "browser_family", "request_by_device-browser_count").show

    df
  }

  private def load(in: DataFrame, path: String): DataFrame = {
    val df = in.groupBy(col("device_model"))
      .agg(
        count(lit("*")).as("device count"),
        (first("distinct_hosts_by_device") / first("total_distinct_hosts") * 100).cast(IntegerType).as("device share %"),
        first("request_count_by_device").as("request count"),
        (first("request_count_by_device") / first("total_request_count") * 100).cast(IntegerType).as("request share %"),
        first("bad_response_count_by_device").as("bad response count"),
        transform(
          slice(
            sort_array(
              collect_set(struct("request_count_by_device_browser", "request_count_by_device", "browser_family")),
              asc = false
            ), 1, 5
          ),
          x => concat((x.getItem("request_count_by_device_browser") / x.getItem("request_count_by_device") * 100).cast(IntegerType),
            lit("% - "),
            x.getItem("browser_family")
          )
        ).as("popular_browsers_aux")
      )
      .withColumn("popular browsers", concat_ws(", ", col("popular_browsers_aux")))
      .select("device_model", "device count", "device share %", "request count", "request share %",
        "bad response count", "popular browsers")


    //    df.write.option("header", true).mode(SaveMode.Overwrite).csv(path)

    df

    //    val df_aux = df.groupBy("date", "passenger_count").agg(
    //      count("passenger_count").as("count"),
    //      first("trip_count").as("trip_count"),
    //      max("total_amount").as("max_total_amount"),
    //      min("total_amount").as("min_total_amount")
    //    )
    //
    //    val df_0p = df_aux.filter(col("passenger_count") === 0)
    //
    //    val df_1p = df_aux.filter(col("passenger_count") === 1)
    //
    //    val df_2p = df_aux.filter(col("passenger_count") === 2)
    //
    //    val df_3p = df_aux.filter(col("passenger_count") === 3)
    //
    //    val df_4p_plus = df_aux
    //      .filter(col("passenger_count") >= 4)
    //      .groupBy(col("date"))
    //      .agg(
    //        sum("count").as("count"),
    //        first("trip_count").as("trip_count"),
    //        max("max_total_amount").as("max_total_amount"),
    //        min("min_total_amount").as("min_total_amount"),
    //      )
    //
    //    val df_all = df_0p.alias("df0")
    //      .join(df_1p.alias("df1"),
    //        col("df0.date") === col("df1.date"), "outer")
    //      .join(df_2p.alias("df2"),
    //        coalesce(col("df0.date"), col("df1.date")) === col("df2.date"), "outer")
    //      .join(df_3p.alias("df3"),
    //        coalesce(col("df0.date"), col("df1.date"), col("df2.date")) === col("df3.date"), "outer")
    //      .join(df_4p_plus.alias("df4"),
    //        coalesce(col("df0.date"), col("df1.date"), col("df2.date"), col("df3.date")) === col("df4.date"), "outer")
    //      .select(
    //        coalesce(
    //          col("df0.date"),
    //          col("df1.date"),
    //          col("df2.date"),
    //          col("df3.date"),
    //          col("df4.date"))
    //          .alias("date"),
    //        coalesce(round(col("df0.count") / col("df0.trip_count") * 100), lit(0)).alias("percentage_zero").cast(IntegerType),
    //        coalesce(round(col("df1.count") / col("df1.trip_count") * 100), lit(0)).alias("percentage_1p").cast(IntegerType),
    //        coalesce(round(col("df2.count") / col("df2.trip_count") * 100), lit(0)).alias("percentage_2p").cast(IntegerType),
    //        coalesce(round(col("df3.count") / col("df3.trip_count") * 100), lit(0)).alias("percentage_3p").cast(IntegerType),
    //        coalesce(round(col("df4.count") / col("df4.trip_count") * 100), lit(0)).alias("percentage_4p_plus").cast(IntegerType),
    //        coalesce(col("df0.max_total_amount"), lit("0.0")).alias("zero_max_total_amount"),
    //        coalesce(col("df0.min_total_amount"), lit("0.0")).alias("zero_min_total_amount"),
    //        coalesce(col("df1.max_total_amount"), lit("0.0")).alias("1p_max_total_amount"),
    //        coalesce(col("df1.min_total_amount"), lit("0.0")).alias("1p_min_total_amount"),
    //        coalesce(col("df2.max_total_amount"), lit("0.0")).alias("2p_max_total_amount"),
    //        coalesce(col("df2.min_total_amount"), lit("0.0")).alias("2p_min_total_amount"),
    //        coalesce(col("df3.max_total_amount"), lit("0.0")).alias("3p_max_total_amount"),
    //        coalesce(col("df3.min_total_amount"), lit("0.0")).alias("3p_min_total_amount"),
    //        coalesce(col("df4.max_total_amount"), lit("0.0")).alias("4p_plus_max_total_amount"),
    //        coalesce(col("df4.min_total_amount"), lit("0.0")).alias("4p_plus_min_total_amount")
    //      )
    //
    //    df_all.write.mode(SaveMode.Overwrite).parquet(path)
    //
    //    df_all
  }
}