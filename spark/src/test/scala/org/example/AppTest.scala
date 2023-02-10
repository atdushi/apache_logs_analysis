package org.example

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalamock.function.MockFunctions
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}

class AppTest extends FunSuite with BeforeAndAfterAll
  with BeforeAndAfterEach with DataFrameSuiteBase with DatasetComparer
  with SharedSparkContext with Matchers with MockFactory with MockFunctions {

  //Double and Timestamp data types comparison accuracy
  val accuracy: Double = 0.11

  var session: SparkSession = _

  override def beforeAll: Unit = {
    session = SparkSession.builder()
      .appName("testing sparkbasics")
      .master("local[*]")
      .config("spark.testing.memory", "536870912") // 512Mb
      .config("spark.sql.session.timeZone", "GMT+3")
      .getOrCreate()
  }

  override def beforeEach() {
    session.catalog.clearCache()
  }

  test("testing transform") {
    var df = session.read
      .option("header", true)
      .csv("src/test/resources/apache_logs.csv")

    df = App.cleansing(df)
    df = App.enrich(df)
    df = App.load(df)

    val expected = session.read
      .option("header", true)
      .csv("src/test/resources/result.csv")

    //      assertDataFrameApproximateEquals(df, expected, accuracy)
    assertDataFrameDataEquals(df, expected)
  }

  test("testing enrich") {
    var df = session.read
      .option("header", true)
      .csv("src/test/resources/apache_logs.csv")

    val initial_columns = df.columns.length
    df = App.enrich(df)

    assert(df.columns.length > initial_columns)
  }
}
