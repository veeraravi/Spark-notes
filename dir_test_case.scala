import org.scalatest.funsuite.AnyFunSuite
import com.visa.gmr.cs_opcode.util.UtilityFunctions._
import java.nio.file.Files
import org.apache.spark.sql.SparkSession

class ConverterTestSuite extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder
    .appName("Test Converter")
    .master("local[*]")
    .getOrCreate()

  test("findMostRecentDatePartition - valid scenario") {
    val baseDir = Files.createTempDirectory("testBaseDir").toFile
    val d1 = new File(baseDir, "2023-01")
    val d2 = new File(baseDir, "2023-06")
    d1.mkdir()
    d2.mkdir()

    val result = findMostRecentDatePartition(
      baseDir.getAbsolutePath,
      raw"\d{4}-\d{2}".r,
      DateTimeFormatter.ofPattern("yyyy-MM")
    )

    assert(result.endsWith("2023-06"))
  }

  test("findMostRecentDatePartition - exception scenario") {
    val baseDir = Files.createTempDirectory("testBaseDirEmpty").toFile

    val thrown = intercept[Exception] {
      findMostRecentDatePartition(
        baseDir.getAbsolutePath,
        raw"\d{4}-\d{2}".r,
        DateTimeFormatter.ofPattern("yyyy-MM")
      )
    }

    assert(thrown.getMessage.contains("No valid date partitions found"))
  }

  test("unzipFilesInDirectory - validate unzip logic") {
    val zipDir = Files.createTempDirectory("zipDir").toFile
    val tempDir = Files.createTempDirectory("tempDir").toString
    val extractedDate = Some("2023-06")

    // Assuming zip file setup is already done here

    val results = unzipFilesInDirectory(
      zipDir.getAbsolutePath,
      tempDir,
      extractedDate
    )

    assert(results.nonEmpty, "Unzipped files should not be empty")
  }

  test("readSchemaConfig - load valid schema") {
    val jobConfig = ConfigFactory.parseString(
      """schemaConfigPath = "schemas/test-schema.json""""
    )

    val schema = readSchemaConfig(jobConfig)

    assert(schema.nonEmpty, "Schema config should be loaded")
    assert(schema.contains("test-schema"))
  }

  test("combinePartFiles - files combined correctly") {
    val tempOutputDir = Files.createTempDirectory("partFilesDir").toFile
    val finalOutputFile = Files.createTempFile("final", ".csv").toString

    val part1 = new File(tempOutputDir, "part-0001")
    Files.write(part1.toPath, "header\ndata1".getBytes)
    val part2 = new File(tempOutputDir, "part-0002")
    Files.write(part2.toPath, "header\ndata2".getBytes)

    combinePartFiles(tempOutputDir.getAbsolutePath, finalOutputFile)

    val combinedContent = Files.readAllLines(new File(finalOutputFile).toPath)

    assert(combinedContent.size == 3, "File should have header and two data lines")
    assert(combinedContent.get(0) == "header")
  }

  test("dateToString - correct format") {
    val date = new SimpleDateFormat("yyyy-MM-dd").parse("2023-06-25")
    val formatted = dateToString(date)

    assert(formatted == "20230625")
  }

  test("today - correct format") {
    val todayDate = today()
    val pattern = raw"\d{8}".r

    assert(pattern.matches(todayDate), s"Today format should be yyyyMMdd, got $todayDate")
  }

  spark.stop()
}
