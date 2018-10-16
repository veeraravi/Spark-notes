val vec1 = Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6).toDF("a")
val vec2 = Seq(1.1, 1.2, 1.3, 1.4, 1.5, 1.6).toDF("a")

// Approach 1
// index both vectors and join on that index
def join1(vec1: org.apache.spark.sql.DataFrame, vec2: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
  val v1 = vec1.withColumn("id", monotonically_increasing_id()).
    withColumn("id", dense_rank().over(org.apache.spark.sql.expressions.Window.orderBy("id")))
  val v2 = vec2.withColumn("id", monotonically_increasing_id()).
    withColumn("id", dense_rank().over(org.apache.spark.sql.expressions.Window.orderBy("id")))
  v1.join(v2, "id").select(v1("a").as("a"), v2("a").as("b")).drop("id")
}

// Approach 2
// based on union of two vectors and fetching values based on offset
def join2(vec1: org.apache.spark.sql.DataFrame, vec2: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
  val len1 = vec1.count.toInt
  vec1.union(vec2).
    withColumn("b", monotonically_increasing_id()).
    withColumn("b", lead($"a", len1).over(org.apache.spark.sql.expressions.Window.orderBy("b"))).limit(len1)
}

// Usage:
join1(vec1, vec2).show()
join2(vec1, vec2).show()
