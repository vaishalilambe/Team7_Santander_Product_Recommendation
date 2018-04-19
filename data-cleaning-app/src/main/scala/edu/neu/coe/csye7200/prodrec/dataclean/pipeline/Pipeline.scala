package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.io.DataParser
import org.apache.spark.sql.{DataFrame, SparkSession}

object Pipeline {

  def run(input: String, ss: SparkSession): DataFrame = {

    /* Parsing */
    val stringDS = DataParser.getStringDS(input, ss)
    val classDS = DataParser.stringDStoClassDS(stringDS, ss)

    /* Filtering */
    val classDS1 = classDS.filter(d => d.customerInfo.code != None)
      .filter(d => d.productInfo.product != "[]")

    /* Transformation */
    var df = TransfomationLogic.classDStoDF(classDS1)
    df = TransfomationLogic.replaceEmptyToUnknown(df)
    df = TransfomationLogic.replaceEmptyToRation(df)
    df = TransfomationLogic.replaceNullWithAvg(df)

    /* Format */
    df = TransfomationLogic.formatColumnDF(df)

    df
  }
}