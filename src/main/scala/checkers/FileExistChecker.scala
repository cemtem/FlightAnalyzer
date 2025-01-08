package com.example
package checkers

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}
import scala.reflect.io.File


class FileExistChecker {

  def validateFileExists(filePath: String): String = {
    if (File(filePath).exists) filePath
    else throw new FileNotFoundException()
  }

  def checkFileOrFolderExists(path: String): Boolean =
    Files.exists(Paths.get(path))

}
