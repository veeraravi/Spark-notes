test("findMostRecentDatePartition - valid most recent directory") {
    val tmpDir = Files.createTempDirectory("testBase").toFile
    val d1 = new File(tmpDir, "2023-01"); d1.mkdir()
    val d2 = new File(tmpDir, "2023-12"); d2.mkdir()

    val result = UtilityFunctions.findMostRecentDatePartition(
      tmpDir.getAbsolutePath,
      raw"\\d{4}-\\d{2}".r,
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM")
    )
    assert(result.endsWith("2023-12"))
  }

  test("findMostRecentDatePartition - no valid directories throws") {
    val tmpDir = Files.createTempDirectory("emptyTestBase").toFile
    intercept[Exception] {
      UtilityFunctions.findMostRecentDatePartition(
        tmpDir.getAbsolutePath,
        raw"\\d{4}-\\d{2}".r,
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM")
      )
    }
  }

  test("dateToString - formats date") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse("2023-06-15")
    val str = UtilityFunctions.dateToString(date)
    assert(str == "20230615")
  }

  test("today - returns date string") {
    val today = UtilityFunctions.today()
    assert(today.matches("\\d{8}"))
  }

  test("combinePartFiles - combines multiple parts correctly") {
    val tempDir = Files.createTempDirectory("parts").toFile
    val p1 = new File(tempDir, "part-0000"); Files.write(p1.toPath, "header\\ndata1".getBytes)
    val p2 = new File(tempDir, "part-0001"); Files.write(p2.toPath, "header\\ndata2".getBytes)

    val finalFile = Files.createTempFile("final", ".csv").toFile.getAbsolutePath
    UtilityFunctions.combinePartFiles(tempDir.getAbsolutePath, finalFile)
    val lines = Source.fromFile(finalFile).getLines().toList

    assert(lines.head == "header")
    assert(lines.contains("data1"))
    assert(lines.contains("data2"))
  }

  test("combinePartFiles - no parts no error") {
    val emptyDir = Files.createTempDirectory("emptyParts").toFile
    val finalFile = Files.createTempFile("finalEmpty", ".csv").toFile.getAbsolutePath
    UtilityFunctions.combinePartFiles(emptyDir.getAbsolutePath, finalFile)
    val lines = Source.fromFile(finalFile).getLines().toList
    assert(lines.isEmpty)
  }

  test("unzipFilesInDirectory - empty dir yields empty list") {
    val zipDir = Files.createTempDirectory("zipDir").toFile
    val outDir = Files.createTempDirectory("outDir").toString
    val res = UtilityFunctions.unzipFilesInDirectory(zipDir.getAbsolutePath, outDir, Some("2023-06"))
    assert(res.isEmpty)
  }

  test("readSchemaConfig - invalid path throws") {
    val config = ConfigFactory.parseString("schemaConfigPath=\"/invalid/path.json\"")
    intercept[Exception] {
      UtilityFunctions.readSchemaConfig(config)
    }
  }

  test("combinePartFiles - handles single part") {
    val tempDir = Files.createTempDirectory("singlePart").toFile
    val part = new File(tempDir, "part-0000"); Files.write(part.toPath, "header\\ndataX".getBytes)
    val finalFile = Files.createTempFile("singleFinal", ".csv").toFile.getAbsolutePath

    UtilityFunctions.combinePartFiles(tempDir.getAbsolutePath, finalFile)
    val lines = Source.fromFile(finalFile).getLines().toList
    assert(lines.head == "header")
    assert(lines.contains("dataX"))
  }
}



import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import java.nio.file.Files
import java.io.File
import java.util.Date
import java.text.SimpleDateFormat
import com.typesafe.config.ConfigFactory
import com.visa.gmr.cs_opcode.util.UtilityFunctions

class UtilityFunctionsTest extends AnyFunSuite {

  test("findMostRecentDatePartition should return most recent directory") {
    val tmpDir = Files.createTempDirectory("testBase").toFile
    val d1 = new File(tmpDir, "2023-01"); d1.mkdir()
    val d2 = new File(tmpDir, "2023-06"); d2.mkdir()
    val d3 = new File(tmpDir, "not-a-date"); d3.mkdir()

    val result = UtilityFunctions.findMostRecentDatePartition(
      tmpDir.getAbsolutePath,
      raw"\\d{4}-\\d{2}".r,
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM")
    )
    assert(result.endsWith("2023-06"))
  }

  test("findMostRecentDatePartition should throw exception when no date dirs") {
    val tmpDir = Files.createTempDirectory("testEmptyBase").toFile
    intercept[Exception] {
      UtilityFunctions.findMostRecentDatePartition(
        tmpDir.getAbsolutePath,
        raw"\\d{4}-\\d{2}".r,
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM")
      )
    }
  }

  test("dateToString should format date correctly") {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse("2023-06-25")
    val result = UtilityFunctions.dateToString(date)
    assert(result == "20230625")
  }

  test("today should return date in yyyyMMdd format") {
    val result = UtilityFunctions.today()
    assert(result.matches("\\d{8}"))
  }

  test("combinePartFiles should combine files into single file") {
    val tempDir = Files.createTempDirectory("parts").toFile
    val f1 = new File(tempDir, "part-0001"); Files.write(f1.toPath, "header\\ndata1".getBytes)
    val f2 = new File(tempDir, "part-0002"); Files.write(f2.toPath, "header\\ndata2".getBytes)
    val finalOut = Files.createTempFile("final", ".csv").toFile.getAbsolutePath

    UtilityFunctions.combinePartFiles(tempDir.getAbsolutePath, finalOut)
    val lines = scala.io.Source.fromFile(finalOut).getLines().toList
    assert(lines.head == "header")
    assert(lines.contains("data1"))
    assert(lines.contains("data2"))
  }

  test("unzipFilesInDirectory should handle empty directory") {
    val tmpDir = Files.createTempDirectory("zipDir").toFile
    val tempExtract = Files.createTempDirectory("extractDir").toString

    val result = UtilityFunctions.unzipFilesInDirectory(
      tmpDir.getAbsolutePath,
      tempExtract,
      Some("2023-06")
    )
    assert(result.isEmpty)
  }

  test("readSchemaConfig should load dummy config") {
    val configStr = "schemaConfigPath = \"/dummy/path\""
    val config = ConfigFactory.parseString(configStr)
    intercept[Exception] {
      UtilityFunctions.readSchemaConfig(config)
    }
  }

}
