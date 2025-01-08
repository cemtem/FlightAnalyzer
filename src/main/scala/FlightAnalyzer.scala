package com.example


import configs.JobConfig
import configs.Reader.ReaderConfig
import configs.Writer.WriterConfig
import jobs.Job

object FlightAnalyzer extends App with SessionWrapper {

  if (args.length < 2) throw new IllegalArgumentException()
  val ordering = if (args.length == 3) args(2) else "desc"

  val job = new Job(
    spark,
    JobConfig(
      ReaderConfig(
        filePath = args(0),
        hasHeader = true,
        separator = ','),
      WriterConfig(
        outputFile = args(1),
        format = "csv"),
      order = ordering
    )
  )

  job.run()

}
