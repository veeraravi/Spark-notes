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
