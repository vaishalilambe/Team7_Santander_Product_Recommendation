package edu.neu.coe.csye7200.prodrec.dataclean.main

import org.apache.spark.sql.SparkSession
import edu.neu.coe.csye7200.prodrec.dataclean.util.DFUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType}

object AppRunner extends App with Serializable {

  val logger = LogManager.getLogger(getClass.getName)

  val filePath = "data-cleaning-app/src/main/resources/train_ver2.csv"

  val spark = SparkSession
    .builder
    .master("local")
    .appName("DataCleaning")
    .getOrCreate()

  var df = spark.read
    .option("header", "true")
    //    .schema(customSchema)
    .csv(filePath)

//  df.printSchema()

  df = DFUtils.removeWhitespaceAllColumns(df)

  df = df.filter(not(col("age") === ""))

  df = DFUtils.castColumnTo( df, "age", IntegerType )

  df = df.filter(col("age") > 27)

  df.write.format("csv").save("destination")

  df.show()

  spark.close()
}
