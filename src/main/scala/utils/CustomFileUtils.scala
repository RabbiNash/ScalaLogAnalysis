package utils

import org.apache.commons.io.FileUtils

import java.nio.file.{ DirectoryNotEmptyException, Files, NoSuchFileException, Path, Paths }
import java.io.{ FileInputStream, IOException }
import java.util.zip.GZIPInputStream

object CustomFileUtils {

  @throws[IOException]
  def decompressGzipNio(source: Path, target: Path): Unit = {
    try {
      val gis = new GZIPInputStream(new FileInputStream(source.toFile))
      try Files.copy(gis, target)
      finally if (gis != null) gis.close()
    } catch {
      case x: IOException =>
        System.err.println(x)
    }
  }

  def deleteFile(filePath: Path): Unit = {
    try Files.delete(filePath)
    catch {
      case x: NoSuchFileException =>
        System.err.format("%s: no such" + " file or directory%n", filePath)
      case x: DirectoryNotEmptyException =>
        System.err.format("%s not empty%n", filePath)
      case x: IOException =>
        // File permission problems are caught here.
        System.err.println(x)
    }
  }

  def deleteIfExists(outputPath: Path): Unit = {
    if (checkIfFileExists(outputPath))
      deleteDirectory(outputPath)
  }

  def deleteDirectory(filePath: Path): Unit = {
    FileUtils.deleteDirectory(filePath.toFile)
  }

  def checkIfFileExists(filePath: Path): Boolean = Files.exists(filePath)
}
