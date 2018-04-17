package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.io.DataParser
import org.apache.spark.sql.{DataFrame, SparkSession}

object Pipeline {

  def run(input: String, ss: SparkSession): DataFrame = {

    import ss.implicits._

    /* Parsing */
    //Get Dataset[String]
    val stringDS = DataParser.getStringDS(input, ss)
    //Parse Dataset[String] to Dataset[SantanderRecord]
    val classDS = DataParser.stringDStoClassDS(stringDS, ss)

    /* Filtering */
    //Filter for customer who failed parsing Customer.code
    val classDS1 = classDS.filter(d => d.customerInfo.code != None)


    /* Transformation */
    //Convert Dataset[SantanderRecord] to DataFrame with uncurry
    val df = TransfomationLogic.classDStoDF(classDS1)
    // Replace column with empty string to "Unknown"
    val df1 = TransfomationLogic.replaceEmptyToUnknown(df)
    //Fill in Gender
    val df2 = TransfomationLogic.replaceEmptyToRation(df1)

    /* Format */
    val df3 = TransfomationLogic.formatColumnDF(df2)

    df3
  }
}