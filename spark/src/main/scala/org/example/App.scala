package org.example

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object App {
  def main(args: Array[String]): Unit = {
    val spark = init()

    print(spark.sparkContext.startTime)

    var df = extract(spark)

    df.printSchema()

    df.show()

    df = cleansing(df)

    df = enrich(df)

    df = load(df, "./result");

    df.printSchema()

    df.show()
  }

  private def init(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Task5")
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
    df.filter(col("remote_host").isNotNull)
  }

  private def enrich(df: DataFrame): DataFrame = {
    df.withColumn("host_count", count(col("remote_host")).over(Window.partitionBy("remote_host")))
  }

  private def load(df: DataFrame, path: String): DataFrame = {

    df.write.option("header", true).mode(SaveMode.Overwrite).csv(path)

    df

  }
}