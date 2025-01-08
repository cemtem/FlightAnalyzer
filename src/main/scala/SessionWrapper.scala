package com.example

import org.apache.spark.sql.SparkSession

trait SessionWrapper {

  lazy val spark: SparkSession = createSession()

  def createSession(): SparkSession =
    SparkSession.builder()
      .appName("FlightAnalyzer")
      .master("local[*]")
      .getOrCreate()

}
