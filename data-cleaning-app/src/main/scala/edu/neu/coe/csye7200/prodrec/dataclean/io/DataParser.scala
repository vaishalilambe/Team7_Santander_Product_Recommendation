package edu.neu.coe.csye7200.prodrec.dataclean.io

import edu.neu.coe.csye7200.prodrec.dataclean.model.{Account, Customer, Product, SantanderRecord}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object DataParser extends Serializable {


  def getStringDS(inputPath: String, ss: SparkSession): Dataset[String] = {
    import ss.implicits._

    val result = ss.read.format("CSV").option("header", "true").textFile(inputPath)
    result.show()
    result
  }

  def stringDStoClassDS(stringDS: Dataset[String], ss: SparkSession): Dataset[SantanderRecord] = {

    import ss.implicits._

    val classDS: Dataset[SantanderRecord] = stringDS map {
      input =>

        val splitRow = input.split(""",(?=([^\"]*\"[^\"]*\")*[^\"]*$)""")

        val customer = Customer(splitRow(1), splitRow(2), splitRow(3), splitRow(4), splitRow(5), splitRow(22))

        val account = Account(splitRow(23), splitRow(6), splitRow(7), splitRow(8), splitRow(9), splitRow(11),
          splitRow(12), splitRow(13), splitRow(14), splitRow(16), splitRow(17), splitRow(20), splitRow(21))

        val product = Product(splitRow(24), splitRow(25), splitRow(26), splitRow(27), splitRow(28), splitRow(29),
          splitRow(30), splitRow(31), splitRow(32), splitRow(33), splitRow(34), splitRow(35), splitRow(36),
          splitRow(37), splitRow(38), splitRow(39), splitRow(40), splitRow(41), splitRow(42), splitRow(43),
          splitRow(44), splitRow(45), splitRow(46), splitRow(47)
        )

        SantanderRecord(customer, account, product)
    }
    classDS
  }


  def classDStoDF(classDS: Dataset[SantanderRecord]): DataFrame = {
    var df = classDS.toDF()
    val custCol = Seq("code", "employmentStatus", "countryOfResidence", "gender", "age", "income")
    for (x <- custCol) {
      df = df.withColumn(x, col("customerInfo")(x))
    }
    df = df.drop(col("customerInfo"))
    val accCol = Seq("customerType", "joinDate", "isCustomerAtMost6MonthOld", "seniority", "isPrimaryCustomer", "customerTypeFirstMonth",
      "customerRelationTypeFirstMonth", "customerResidenceIndex", "customerForeignIndex", "channelOfJoin", "deceasedIndex", "customerAddrProvinceName", "isCustomerActive")
    for (x <- accCol) {
      df = df.withColumn(x, col("accountInfo")(x))
    }
    df = df.drop(col("accountInfo"))
    val prodCol = Seq("savingAcc", "guarantees", "currentAcc", "derivedAcc", "payrollAcc", "juniorAcc", "moreParticularAcc", "particularAcc", "particularPlusAcc",
      "shortTermDeposit", "midTermDeposit", "longTermDeposit", "eAccount", "funds", "mortgage", "pensionPlan", "loan", "taxes", "creditCard",
      "securities", "homeAcc", "payrollNom", "pensionNom", "directDebit")
    for (x <- prodCol) {
      df = df.withColumn(x, col("productInfo")(x))
    }
    df = df.drop(col("productInfo"))
    df
  }

}