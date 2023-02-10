package org.example

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

object App {
  private val count_bots = true

  def main(args: Array[String]): Unit = {
    val spark = init()

    print(new DateTime(spark.sparkContext.startTime))

    var df = extract(spark)

    df.printSchema()

    df.show()

    df = cleansing(df)

    df = enrich(df)

    df = load(df);

    df.printSchema()

    df.show(truncate = false)

    df.write.option("header", true).mode(SaveMode.Overwrite).csv("/tmp/apache_logs_analysis/result")
  }

  private def init(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Task3")
      .withExtensions(new CassandraSparkExtensions)
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.cassandra.connection.host", "host.docker.internal") // for "minikube start --driver=docker"
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

  def cleansing(df: DataFrame): DataFrame = {
    if (!count_bots)
      df.filter(col("is_bot") === false)
    else
      df
  }

  /*
    Assumption: user == remote_host
   */
  def enrich(in: DataFrame): DataFrame = {
    var df = in.withColumn("total_distinct_hosts", size(collect_set("remote_host").over()))
    df = df.withColumn("distinct_hosts_by_device", size(collect_set("remote_host").over(Window.partitionBy("device_model"))))
    df = df.withColumn("total_request_count", size(collect_set("request_line").over()))
    df = df.withColumn("request_count_by_device", count(col("request_line")).over(Window.partitionBy("device_model")))
    df = df.withColumn("bad_response_count_by_device", count(when(col("final_status") =!= 200, true)).over(Window.partitionBy("device_model")))
    df = df.withColumn("request_count_by_device_browser",
      count("browser_family").over(Window.partitionBy("device_model", "browser_family")))
    df
  }

  def load(in: DataFrame): DataFrame = {
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

    val responses = in.groupBy("device_model").pivot("final_status").count()

    //    in.groupBy("device_model").pivot("final_status").count().show()

    val columns = ArrayBuffer[String]()
    columns ++= responses.columns
    columns -= "device_model"
    columns += "device count"
    columns += "df.device share %"
    columns += "df.request count"
    columns += "df.request share %"
    columns += "df.bad response count"
    columns += "df.popular browsers"

    val result = df.alias("df")
      .join(responses.alias("resp"),
        col("df.device_model") === col("resp.device_model"), "inner")
      .select(
        "df.device_model",
        columns: _*
      )

    result
  }
}