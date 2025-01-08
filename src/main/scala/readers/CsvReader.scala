package com.example
package readers

import checkers.FileExistChecker
import configs.Reader

import org.apache.spark.sql.{DataFrame, SparkSession}


class CsvReader(spark: SparkSession, config: Reader.ReaderConfig) extends DataFrameReader {
  val checker = new FileExistChecker()

  override def read(filePath: String): DataFrame = {
    val fullPath = checker.validateFileExists(filePath)
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .csv(fullPath)
  }
}