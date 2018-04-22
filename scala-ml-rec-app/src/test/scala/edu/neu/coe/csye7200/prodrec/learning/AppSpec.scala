package edu.neu.coe.csye7200.prodrec.learning

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Spark Data Model App Test")
      .getOrCreate()
  }

}

class AppSpec extends FlatSpec with SparkSessionTestWrapper with Matchers {

  behavior of "load data method"

  val trainDF = DataModelApp.loadCleanedData(spark, "./dataset/sample.csv")

  it should "have total 21 columns" in {
    assert(20 == trainDF.columns.length)
  }

  it should "have total 100 row" in {
    assertResult(100) {
      trainDF.count
    }
  }

  it should "not have null values in product column" in {
    assertResult(0) {
      trainDF.filter(trainDF("product").isNull || trainDF("product") === "" || trainDF("product").isNaN).count()
    }
  }

  behavior of "filter data method"

  val filteredData = DataModelApp.filterData(trainDF)

  it should "have total columns 12 after filtering the features" in {
    assertResult(12) {
      filteredData.columns.length
    }
  }
}