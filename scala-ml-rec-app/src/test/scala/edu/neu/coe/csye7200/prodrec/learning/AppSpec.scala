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

  behavior of "app"

  val trainDF = DataModelApp.loadCleanedData(spark, "./dataset/trim_trait.csv")

  it should "have total 24 columns" in {
    assert(21 == trainDF.columns.length)
  }

  it should "have total 100 row" in {
    assertResult(834) {
      trainDF.count
    }
  }

  /*it should "not have null values in products columns" in {
    assertResult(false) {
      trainDF("product").isNull
    }
  }*/
}