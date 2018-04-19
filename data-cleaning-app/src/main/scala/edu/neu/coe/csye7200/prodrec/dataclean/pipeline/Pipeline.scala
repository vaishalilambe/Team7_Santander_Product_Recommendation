package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.io.DataParser
import edu.neu.coe.csye7200.prodrec.dataclean.model.{Customer, SantanderRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Pipeline {

  def run(input: String, ss: SparkSession): DataFrame = {
    import ss.implicits._
    
    /* Parsing */
    val stringDS = DataParser.getStringDS(input, ss)
    val classDS = DataParser.stringDStoClassDS(stringDS, ss)

    /* Filtering */
    var classDS1 = classDS.filter(d => d.customerInfo.code != None)
      .filter(d => d.productInfo.product != "[]")
      .filter(d => d.accountInfo.seniority.getOrElse(0) >= 0)
      .map(d => if (d.customerInfo.age.getOrElse(0) > 100) {
        new SantanderRecord(
          Customer(d.customerInfo.code,
            d.customerInfo.employmentStatus,
            d.customerInfo.countryOfResidence,
            d.customerInfo.gender,
            None,
            d.customerInfo.income),
          d.accountInfo,
          d.productInfo
        )
      } else d )

    /* Transformation */
    var df = TransfomationLogic.classDStoDF(classDS1)
    df = TransfomationLogic.replaceEmptyToUnknown(df)
    df = TransfomationLogic.replaceEmptyToRation(df)
    df = TransfomationLogic.replaceNullWithAvg(df)
    df = TransfomationLogic.fixAge(df)

    /* Format */
    df = TransfomationLogic.formatColumnDF(df)

    df
  }
}