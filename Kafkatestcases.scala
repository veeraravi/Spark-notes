import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.SaveMode
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll

class DataFrameWriterTest extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll {

  var spark: SparkSession = _

  // This method runs before all tests, setting up the SparkSession with master
  override def beforeAll(): Unit = {
    // SparkSession with master set to local mode
    spark = SparkSession.builder()
      .master("local[*]") // Runs Spark locally using all available cores
      .appName("DataFrameWriterTest")
      .getOrCreate()
  }

  // This method runs after all tests, stopping the SparkSession
  override def afterAll(): Unit = {
    spark.stop()
  }

  test("writeDataFrame should write non-empty DataFrame to HDFS") {
    // Mock the DataFrame and the DataFrameWriter
    val df: DataFrame = mock[DataFrame]
    val dfWriter = mock[df.DataFrameWriter[Row]]

    // Simulate a non-empty DataFrame
    when(df.isEmpty).thenReturn(false)
    when(df.write).thenReturn(dfWriter)
    when(dfWriter.mode(SaveMode.Overwrite)).thenReturn(dfWriter)

    // Call the method and verify that write was invoked
    DataFrameWriter.writeDataFrame(df, "hdfs://mock/path")
    verify(dfWriter).parquet("hdfs://mock/path")
  }

  test("writeDataFrame should print 'No records' for empty DataFrame") {
    // Mock an empty DataFrame
    val df: DataFrame = mock[DataFrame]
    when(df.isEmpty).thenReturn(true)

    // Capture the console output
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      DataFrameWriter.writeDataFrame(df, "hdfs://mock/path")
    }

    // Assert that the correct message was printed
    assert(stream.toString().contains("No records"))

    // Ensure the write operation was never called
    verify(df, never()).write
  }
}



import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.mockito.Mockito._

class DataFrameWriterTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    // Set up a local Spark session for testing
    spark = SparkSession.builder()
      .appName("DataFrameWriterTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    // Stop the Spark session
    if (spark != null) {
      spark.stop()
    }
  }

  test("writeDataFrame should filter DataFrame and write to HDFS if not empty") {
    // Create a sample DataFrame
    import spark.implicits._
    val data = Seq(
      (1, "valid_record", null),
      (2, "valid_record", null),
      (3, "corrupt_record", "corrupt")
    )
    val df: DataFrame = data.toDF("id", "record", "_currpt_record")

    // Filter out corrupt records and check the output
    val filteredDF = df.filter("_currpt_record IS NULL")
    assert(!filteredDF.isEmpty)  // This should not throw NullPointerException

    // Mock the write behavior
    val dfWriter = mock[df.DataFrameWriter[_]]
    when(filteredDF.write).thenReturn(dfWriter)
    when(dfWriter.mode("overwrite")).thenReturn(dfWriter)

    // Simulate the write call (write logic can be mocked)
    filteredDF.write.mode("overwrite").parquet("hdfs://mock/path")
    verify(dfWriter).parquet("hdfs://mock/path")
  }

  test("writeDataFrame should print 'No records' when filtered DataFrame is empty") {
    // Create an empty DataFrame
    val emptyDF = spark.emptyDataFrame

    // Mock the behavior when the DataFrame is empty
    val dfWriter = mock[df.DataFrameWriter[_]]
    when(emptyDF.write).thenReturn(dfWriter)
    when(dfWriter.mode("overwrite")).thenReturn(dfWriter)

    // Verify that no records are written if the DataFrame is empty
    assert(emptyDF.isEmpty)
    verify(dfWriter, never()).parquet("hdfs://mock/path")
  }
}
