import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate

import spark.implicits._
import org.apache.spark.sql.SparkSession
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@471e24c0
import spark.implicits._
val df = Seq(
    ("one", 2.0),
    ("two", 1.5),
    ("three", 8.0)
  ).toDF("id", "val")
df: org.apache.spark.sql.DataFrame = [id: string, val: double]
df.show()
+-----+---+
|   id|val|
+-----+---+
|  one|2.0|
|  two|1.5|
|three|8.0|
+-----+---+

// Simple scala way of mapping
df.select("id").collect().map(_(0)).toList
res15: List[Any] = List(one, two, three)
//RDD way of mapping
df.select("id").rdd.map(_(0)).collect.toList 
res16: List[Any] = List(one, two, three)
// Pure Dataframe way of map
df.select("id").map(_.getString(0)).collect.toList 
res17: List[String] = List(one, two, three)
case class Rec(id: String, value: Double)

val df = Seq(
    Rec("first", 2.0),
    Rec("test", 1.5),
    Rec("choose", 8.0)
  ).toDF()

df.select("id").map(_.getString(0)).collect()
defined class Rec
df: org.apache.spark.sql.DataFrame = [id: string, value: double]
res18: Array[String] = Array(first, test, choose)
df.select("id").map(_(0)).collect()
<console>:54: error: Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
       df.select("id").map(_(0)).collect()
                          ^
