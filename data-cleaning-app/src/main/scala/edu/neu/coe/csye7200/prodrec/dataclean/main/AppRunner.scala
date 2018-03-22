package edu.neu.coe.csye7200.prodrec.dataclean.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, DateType}

object AppRunner extends App with Serializable {

  val logger = LogManager.getLogger(getClass.getName)

  val filePath = "data-cleaning-app/src/main/resources/train_ver2.csv"

  val spark = SparkSession
    .builder
    .master("local")
    .appName("DataCleaning")
    .getOrCreate()
  //
  //  val customSchema = StructType(Array(
  //    StructField("fecha_dato", StringType, nullable = true),
  //    StructField("ncodpers", StringType, nullable = true),
  //    StructField("ind_empleado", StringType, nullable = true),
  //    StructField("pais_residencia", StringType, nullable = true),
  //
  //    StructField("sexo", StringType, nullable = true),
  //    StructField("age", IntegerType, nullable = false),
  //    StructField("fecha_alta", StringType, nullable = true),
  //    StructField("ind_nuevo", StringType, nullable = true),
  //
  //    StructField("antiguedad", StringType, nullable = true),
  //    StructField("indrel", StringType, nullable = true),
  //    StructField("ult_fec_cli_1t", StringType, nullable = true),
  //    StructField("indrel_1mes", StringType, nullable = true),
  //
  //    StructField("tiprel_1mes", StringType, nullable = true),
  //    StructField("indresi", StringType, nullable = true),
  //    StructField("indext", StringType, nullable = true),
  //    StructField("conyuemp", StringType, nullable = true),
  //
  //    StructField("canal_entrada", StringType, nullable = true),
  //    StructField("indfall", StringType, nullable = true),
  //    StructField("tipodom", StringType, nullable = true),
  //    StructField("cod_prov", StringType, nullable = true),
  //
  //    StructField("nomprov", StringType, nullable = true),
  //    StructField("ind_actividad_cliente", StringType, nullable = true),
  //    StructField("renta", StringType, nullable = true),
  //    StructField("segmento", StringType, nullable = true)
  //  ))

  var df = spark.read
    .option("header", "true")
    //    .schema(customSchema)
    .csv(filePath)

  df.printSchema()


  val count = df.count()
  logger.info(f"$count%d")

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }

//  def replaceWhiteWithNull(col: Column): Column = {
//    regexp_replace(col, "", null)
//  }
//
//
  //
  //  df.columns.foldLeft(df) { (memoDF, colName) =>
  //    memoDF.withColumn(
  //      colName,
  //      replaceWhiteWithNull(col(colName))
  //    )
  //  }

  //  df = df.withColumn("age", regexp_replace(col("age"), "", null))
  /*Drop rows for whom provided columns are null*/
  //  val nullColumn: Seq[String] = Seq("age")
  //  df.na.drop(nullColumn)
  //  val dff = df.filter("age != ''")
  //
  //  df.na.replace(df.columns,Map("" -> null))
  //  df.filter($"colName" =!= "")

//  df.write.format("csv").save("myfile")

  df = df.columns.foldLeft(df) { (memoDF, colName) =>
    memoDF.withColumn(
      colName,
      removeAllWhitespace(col(colName))
    )
  }

//  df.write.format("csv").save("myfile1")


  df = df.filter(not(col("age") === ""))


  df.write.format("csv").save("destination")

  df.show()


  val count1 = df.count()
  logger.info(f"$count1%d")

  spark.close()
}