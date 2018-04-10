package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.io.DataParser
import org.apache.spark.sql.{DataFrame, SparkSession}

object Pipeline {

  def run(input: String, ss: SparkSession): DataFrame = {

    import ss.implicits._

    //Step 1: Get Dataset[String]
    val stringDS = DataParser.getStringDS(input, ss)

    //Step 2: Parse Dataset[String] to Dataset[SantanderRecord]
    val classDS = DataParser.stringDStoClassDS(stringDS, ss)

    //Step 3: Filter for customer who failed parsing Customer.code
    val classDS1 = classDS.filter(d => d.customerInfo.code != None)

    //Step : Convert Dataset[SantanderRecord] to DataFrame with uncurry
    val df = DataParser.classDStoDF(classDS1)
    df
  }
}