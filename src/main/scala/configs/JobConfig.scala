package com.example
package configs


object Reader {
  case class ReaderConfig(
                           filePath: String,
                           hasHeader: Boolean = true,
                           separator: Char = ',')
}

object Writer {
  case class WriterConfig(
                           outputFile: String,
                           format: String)
}


case class JobConfig(
                      readerConfig: Reader.ReaderConfig,
                      writerConfig: Writer.WriterConfig,
                      order: String = "desc"
                    )