import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

val df = Seq(
  ("str", 1, 0.2)
).toDF("a", "b", "c").
  withColumn("struct", struct($"a", $"b", $"c"))

// UDF for struct
val func = udf((x: Any) => {
  x match {
    case Row(a: String, b: Int, c: Double) => 
      s"$a-$b-$c"
    case other => 
      sys.error(s"something else: $other")
  }
}, StringType)

df.withColumn("d", func($"struct")).show
/*
+---+---+---+-----------+---------+
|  a|  b|  c|     struct|        d|
+---+---+---+-----------+---------+
|str|  1|0.2|[str,1,0.2]|str-1-0.2|
+---+---+---+-----------+---------+
*/
