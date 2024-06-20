import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

// Initialize Spark session
val spark = SparkSession.builder()
  .appName("Extract Schema")
  .enableHiveSupport()
  .getOrCreate()

// Sample DataFrame (replace this with your actual DataFrame)
val data = Seq(
  (1, "Alice", (10, 20, "hello", 1.1)),
  (2, "Bob", (30, 40, "world", 2.2))
)

val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("msg", StructType(Seq(
    StructField("c1", IntegerType, nullable = false),
    StructField("c2", IntegerType, nullable = false),
    StructField("c3", StringType, nullable = false),
    StructField("c4", DoubleType, nullable = false)
  )), nullable = false)
))

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  schema
)

// Function to convert schema to Hive DDL string
def schemaToDDL(schema: StructType): String = {
  schema.fields.map { field =>
    field.dataType match {
      case structType: StructType => 
        structType.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(", ")
      case _ => s"${field.name} ${field.dataType.sql}"
    }
  }.mkString(", ")
}

val ddlSchema = schemaToDDL(df.schema)

val hiveDDL = s"CREATE EXTERNAL TABLE MYTABLE ($ddlSchema) LOCATION '/path/to/table'"

println(hiveDDL)

// Execute the Hive DDL statement
spark.sql(hiveDDL)
