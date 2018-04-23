package edu.neu.coe.csye7200.prodrec.dataclean.pipeline

import edu.neu.coe.csye7200.prodrec.dataclean.model.SantanderRecord
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, mean, udf}

object TransfomationLogic {

  def emptyToGenderRation: (String => String) = {
    x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.54) "V" else "H"
        case a => a
      }
    }
  }

  def emptyToSNRatio: (String => String) = {
    x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.54) "S" else "N"
        case a => a
      }
    }
  }

  def emptyToIARatio: (String => String) = {
    x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.54) "A" else "I"
        case a => a
      }
    }
  }

  def emptyToSNRatioRev: (String => String) = {
    x => {
      val r = new scala.util.Random
      x match {
        case "" => if (r.nextFloat > 0.46) "S" else "N"
        case a => a
      }
    }
  }

  def emptyToUnknown: (String => String) = {
    x =>
      if (x == "")
        "Unknown"
      else
        x
  }

  def removeQuotes: (String => String) = {
    x =>
      if (x.charAt(0) == '"' && x.charAt(x.length - 1) == '"') {
        x.substring(1, x.length - 1)
      }
      else {
        x
      }
  }

  def fixAge(df: DataFrame): DataFrame = {
    val avgAge: Int = df.select(mean(df("age"))).collect()(0).get(0).toString.toDouble.toInt
    val df1 = df.na.fill(avgAge, Seq("age"))
    df1
  }

  def replaceNullWithAvg(df: DataFrame): DataFrame = {
    val avgIncome: Double = df.select(mean(df("income"))).collect()(0).get(0).toString.toDouble
    val df1 = df.na.fill(avgIncome, Seq("income"))
    df1
  }

  def replaceEmptyToRation(df: DataFrame): DataFrame = {
    var df1 = df

    df1 = df1.withColumn("gender", udf(emptyToGenderRation).apply(col("gender")))
    df1 = df1.withColumn("deceasedIndex", udf(emptyToSNRatio).apply(col("deceasedIndex")))
    df1 = df1.withColumn("customerResidenceIndex", udf(emptyToSNRatio).apply(col("customerResidenceIndex")))
    df1 = df1.withColumn("employmentStatus", udf(emptyToSNRatio).apply(col("employmentStatus")))
    df1 = df1.withColumn("customerRelationTypeFirstMonth", udf(emptyToIARatio).apply(col("customerRelationTypeFirstMonth")))
    df1 = df1.withColumn("customerForeignIndex", udf(emptyToIARatio).apply(col("customerForeignIndex")))

    df1
  }

  def replaceEmptyToUnknown(df: DataFrame): DataFrame = {
    var df1 = df
    val columns = Seq("customerAddrProvinceName", "customerType", "customerTypeFirstMonth", "channelOfJoin")
    for (x <- columns) {
      df1 = df1.withColumn(x, udf(emptyToUnknown).apply(col(x)))
    }
    df1
  }

  def formatColumnDF(df: DataFrame): DataFrame = {
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
