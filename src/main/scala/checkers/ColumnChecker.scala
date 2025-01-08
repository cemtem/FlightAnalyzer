package com.example
package checkers

import constants.ReqColumns

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

case class MissingColumnsException(msg: String) extends Exception(msg)

private class ColumnChecker(df: DataFrame) {
  def validateColumnPresence(): DataFrame = {
    val correctColumnNames = ReqColumns.COLUMNS

    val currentColumnNames = df.columns
    val columnMapping = correctColumnNames.zip(currentColumnNames)

    val alignedDf = columnMapping.foldLeft(df) { (df, mapping) =>
      val (desiredName, currentName) = mapping
      if (currentColumnNames.contains(currentName)) {
        df.withColumnRenamed(currentName, desiredName)
      } else {
        df.withColumn(desiredName, lit(null))
      }
    }

    val finalDf = alignedDf.select(correctColumnNames.map(col): _*)
    finalDf
  }
}


trait DfValidator {

  def validateColumnPresence()(df: DataFrame): DataFrame = {
    val checker = new ColumnChecker(df)
    checker.validateColumnPresence()
  }

}
