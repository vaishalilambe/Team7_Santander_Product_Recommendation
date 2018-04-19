package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.model.SantanderRecord
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, mean, udf}

object TransfomationLogic {

  def replaceNullWithAvg(df: DataFrame): DataFrame = {
    var df1 = df

    val avgIncome: Double = df.select(mean(df("income"))).collect()(0).get(0).toString.toDouble

    df1 = df1.na.fill(avgIncome, Seq("income"))
    df1
  }

  def replaceEmptyToRation(df: DataFrame): DataFrame = {
    var df1 = df

    def emptyToGenderRation: (String => String) = { x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.54) "V" else "H"
        case a => a
      }
    }
    }

    df1 = df1.withColumn("gender", udf(emptyToGenderRation).apply(col("gender")))

    def emptyToSNRatio: (String => String) = { x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.54) "S" else "N"
        case a => a
      }
    }
    }

    df1 = df1.withColumn("deceasedIndex", udf(emptyToSNRatio).apply(col("deceasedIndex")))
    df1 = df1.withColumn("customerResidenceIndex", udf(emptyToSNRatio).apply(col("customerResidenceIndex")))
    df1 = df1.withColumn("employmentStatus", udf(emptyToSNRatio).apply(col("employmentStatus")))

    def emptyToIARatio: (String => String) = { x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.54) "A" else "I"
        case a => a
      }
    }
    }

    df1 = df1.withColumn("customerRelationTypeFirstMonth", udf(emptyToIARatio).apply(col("customerRelationTypeFirstMonth")))

    def emptyToSNRatioRev: (String => String) = { x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.46) "S" else "N"
        case a => a
      }
    }
    }

    df1 = df1.withColumn("customerForeignIndex", udf(emptyToIARatio).apply(col("customerForeignIndex")))

    df1
  }

  def replaceEmptyToUnknown(df: DataFrame): DataFrame = {

    var df1 = df

    def emptyToUnknown: (String => String) = { x => if (x == "") "Unknown" else x }

    val columns = Seq("customerAddrProvinceName", "customerType", "customerTypeFirstMonth", "channelOfJoin")

    for (x <- columns) {
      df1 = df1.withColumn(x, udf(emptyToUnknown).apply(col(x)))
    }
    df1
  }

  def formatColumnDF(df: DataFrame): DataFrame = {

    def removeQuotes: (String => String) = {
      x =>
        if (x.charAt(0) == '"' && x.charAt(x.length - 1) == '"') {
          x.substring(1, x.length - 1)
        }
        else {
          x
        }
    }

    val df1 = df.withColumn("customerAddrProvinceName", udf(removeQuotes).apply(col("customerAddrProvinceName")))

    df1
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

    df = df.withColumn("product", col("productInfo")("product"))
    df = df.drop(col("productInfo"))
    df
  }
}
