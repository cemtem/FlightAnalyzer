package com.example
package jobs

import checkers.DfValidator
import configs.JobConfig
import constants.FilePaths
import readers.CsvReader

import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

import java.sql.Date
import java.time.LocalDate

class Job(spark: SparkSession, config: JobConfig) extends DfValidator {
  private val csvReader = new CsvReader(spark, config.readerConfig)
  private val log = LogManager.getRootLogger


  def run(): Unit = {
    val (airportsDf, airlinesDf) = readDfs()

    val flightsDf = validateColumnPresence()(readCurrentDf().withColumnRenamed("YEAR", "YEARS"))

    val orderColumn = config.order match {
      case "desc" => col("count_FLIGHT_NUMBER").desc
      case "asc" => col("count_FLIGHT_NUMBER").asc
      case _ =>
        log.error(s"Wrong value ${config.order} for order param. Available options: desc, asc")
        throw new IllegalArgumentException()
    }

    val flightsDatesDf = flightsDf
      .transform(extractDatesFormatted)

    val periodFrom = extractDateFromFlights(flightsDatesDf, "min")
    val periodTo = extractDateFromFlights(flightsDatesDf, "max")

    val airportFlightsCondition = (
      col("IATA_CODE") === col("ORIGIN_AIRPORT")
        || col("IATA_CODE") === col("DESTINATION_AIRPORT")
      )

    val topTenAirportsDf = broadcast(airportsDf)
      .transform(joinDfsOn(_, flightsDf, airportFlightsCondition))
      .transform(extractTotalFlightsPerColumn(_, col("IATA_CODE")))
      .transform(extractTopTenByNumber(_, orderColumn))
      .transform(renameColumn(_, "IATA_CODE", "most_popular_airports"))

    val airlineFlightsCondition = broadcast(airlinesDf).col("IATA_CODE") === flightsDf.col("AIRLINE")

    val notDelayedFlights = flightsDf
      .transform(filterDelayedFlights)
      .persist()

    val notDelayedFlightsAirlinesDf = notDelayedFlights
      .transform(joinDfsOn(_, airlinesDf, airlineFlightsCondition))
      .persist()

    val topTenAirlinesDf = notDelayedFlightsAirlinesDf
      .transform(extractTotalFlightsPerColumn(_, col("IATA_CODE")))
      .transform(extractTopTenByNumber(_, orderColumn))
      .transform(renameColumn(_, "IATA_CODE", "most_popular_airlines"))


    val topTenDestinationsPerAirportDf = notDelayedFlights
      .transform(extractTotalFlightsPerColumn(_, col("ORIGIN_AIRPORT"), col("DESTINATION_AIRPORT")))
      .transform(filterTopTen(_, orderColumn))

    val topTenAirlinesPerAirportDf = notDelayedFlightsAirlinesDf
      .transform(extractTotalFlightsPerColumn(_, notDelayedFlights.col("ORIGIN_AIRPORT"), airlinesDf.col("AIRLINE")))
      .transform(filterTopTen(_, orderColumn))

    val dayOfWeekSortedByDelayRateDf = flightsDf
      .transform(withColumnDelayed)
      .transform(orderByDelayPerWeekDay)

    val flightsDelayedByReasonsDf = flightsDf
      .transform(extractWithSpecialDelay)
      .persist()

    val flightsDelayedByReasonsCountDf = flightsDelayedByReasonsDf
      .transform(extractNumberOfFlightsWithReason(_, periodFrom, periodTo))

    val delayTypePercentsDf = flightsDelayedByReasonsDf
      .transform(extractDelayTypePercent)

    writeData(topTenAirportsDf, "topTenAirports")
    writeData(topTenAirlinesDf, "topTenAirlines")
    writeData(topTenDestinationsPerAirportDf, "topTenDestinationsPerAirport")
    writeData(topTenAirlinesPerAirportDf, "topTenAirlinesPerAirport")
    writeData(dayOfWeekSortedByDelayRateDf, "dayOfWeekSortedByDelayRate")
    writeData(flightsDelayedByReasonsCountDf, "flightsDelayedByReasonsCount")
    writeData(delayTypePercentsDf, "delayTypePercents")

    val metadataDf = createMetadataDf(periodFrom, periodTo)

    writeMetadata(metadataDf)
  }


  private def writeData(df: DataFrame, folder: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(s"result_data/data_marts/$folder")
  }


  private def extractDateFromFlights(df: DataFrame, agg: String) = {
    agg match {
      case "min" => df.agg(min("FULL_DATE")).head().getDate(0)
      case "max" => df.agg(max("FULL_DATE")).head().getDate(0)
      case _ => throw new IllegalArgumentException()
    }
  }


  private def extractDatesFormatted(df: DataFrame) =
    df
      .withColumn(
        "FULL_DATE",
        expr("CAST(CONCAT(YEAR, '-', LPAD(MONTH, 2, '0'), '-', LPAD(DAY, 2, '0')) AS DATE)")
      )


  private def writeMetadata(df: DataFrame): Unit = {
    df.write
      .mode(SaveMode.Append)
      .format(config.writerConfig.format)
      .save(config.writerConfig.outputFile)
  }


  private def renameColumn(df: DataFrame, oldName: String, newName: String) =
    df.withColumnRenamed(oldName, newName)


  private def createMetadataDf(periodFrom: Date, periodTo: Date): DataFrame = {
    import spark.implicits._

    val currentDate = LocalDate.now()
    val metadata = Seq((periodFrom, periodTo, currentDate))
    metadata.toDF("periodFrom", "periodTo", "processed")
  }


  private def extractDelayTypePercent(df: DataFrame) =
    df.agg(sum("AIR_SYSTEM_DELAY").alias("AIR_SYSTEM_DELAY"),
        sum("SECURITY_DELAY").alias("SECURITY_DELAY"),
        sum("AIRLINE_DELAY").alias("AIRLINE_DELAY"),
        sum("LATE_AIRCRAFT_DELAY").alias("LATE_AIRCRAFT_DELAY"),
        sum("WEATHER_DELAY").alias("WEATHER_DELAY")
      )
      .withColumn(
        "TOTAL_DELAY",
        col("AIR_SYSTEM_DELAY") +
          col("SECURITY_DELAY") +
          col("AIRLINE_DELAY") +
          col("LATE_AIRCRAFT_DELAY") +
          col("WEATHER_DELAY")
      )
      .selectExpr(
        s"stack(5, " +
          s"'AIR_SYSTEM_DELAY', (AIR_SYSTEM_DELAY / TOTAL_DELAY) * 100, " +
          s"'SECURITY_DELAY', (SECURITY_DELAY / TOTAL_DELAY) * 100, " +
          s"'AIRLINE_DELAY', (AIRLINE_DELAY / TOTAL_DELAY) * 100, " +
          s"'LATE_AIRCRAFT_DELAY', (LATE_AIRCRAFT_DELAY / TOTAL_DELAY) * 100, " +
          s"'WEATHER_DELAY', (WEATHER_DELAY / TOTAL_DELAY) * 100" +
          s") as (DELAY_TYPE, PERCENT_DELAY)"
      )


  private def extractNumberOfFlightsWithReason(df: DataFrame, periodFrom: Date, periodTo: Date) =
    df.agg(count("*").alias("number_of_flights_with_delay_reason"))
      .withColumn("start_date", lit(periodFrom))
      .withColumn("end_date", lit(periodTo))

  private def extractWithSpecialDelay(df: DataFrame) =
    df.where(col("AIR_SYSTEM_DELAY").isNotNull ||
      col("SECURITY_DELAY").isNotNull ||
      col("AIRLINE_DELAY").isNotNull ||
      col("LATE_AIRCRAFT_DELAY").isNotNull ||
      col("WEATHER_DELAY").isNotNull
    )


  private def orderByDelayPerWeekDay(df: DataFrame) =
    df.groupBy("DAY_OF_WEEK")
      .agg(avg("delayed").alias("avg_delay_rate"))
      .orderBy("avg_delay_rate")


  private def withColumnDelayed(df: DataFrame) =
    df.withColumn(
      "delayed",
      when(col("ARRIVAL_DELAY").gt(0), -1).otherwise(0)
    )


  private def filterTopTen(df: DataFrame, countColumn: Column) =
    df.withColumn("rank",
        dense_rank().over(Window.orderBy(countColumn).partitionBy("ORIGIN_AIRPORT"))
      )
      .where(col("rank") <= 10)


  private def filterDelayedFlights(df: DataFrame) =
    df.where(col("DEPARTURE_DELAY") <= 0)
      .where(col("ARRIVAL_DELAY") <= 0)


  private def extractTopTenByNumber(df: DataFrame, column: Column) =
    df.sort(column)
      .limit(10)


  private def extractTotalFlightsPerColumn(df: DataFrame, cols: Column*): DataFrame =
    df.groupBy(cols: _*)
      .agg(count("FLIGHT_NUMBER").alias("count_FLIGHT_NUMBER"))


  private def joinDfsOn(dfLeft: DataFrame, dfRight: DataFrame, condition: Column): DataFrame =
    dfLeft.join(
      dfRight,
      condition
    )


  private def readDfs(): (DataFrame, DataFrame) =
    (
      csvReader.read(FilePaths.AIRPORTS),
      csvReader.read(FilePaths.AIRLINES),
    )


  private def readCurrentDf(): DataFrame =
    csvReader.read(config.readerConfig.filePath)
}
