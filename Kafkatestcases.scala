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
