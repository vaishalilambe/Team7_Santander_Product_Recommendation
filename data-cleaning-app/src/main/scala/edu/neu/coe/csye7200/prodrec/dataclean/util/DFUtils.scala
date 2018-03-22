package edu.neu.coe.csye7200.prodrec.dataclean.util

import edu.neu.coe.csye7200.prodrec.dataclean.main.AppRunner.df
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.DataType

object DFUtils {
  def castColumnTo(df: DataFrame, cn: String, tpe: DataType): DataFrame = {
    df.withColumn(cn, df(cn).cast(tpe))

  }

  def removeWhitespaceAllColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumn(
        colName,
        removeAllWhitespace(col(colName))
      )
    }
  }

  def removeAllWhitespace(col: Column): Column = {
    regexp_replace(col, "\\s+", "")
  }
}
