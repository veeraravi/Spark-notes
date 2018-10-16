val df = Seq(
  (System.currentTimeMillis, "user1", 0.3, Seq(0.1, 0.2)),
  (System.currentTimeMillis + 1000000L, "user1", 0.5, Seq(0.1, 0.2)),
  (System.currentTimeMillis + 2000000L, "user1", 0.2, Seq(0.1, 0.2)),
  (System.currentTimeMillis + 3000000L, "user1", 0.1, Seq(0.1, 0.2)),
  (System.currentTimeMillis + 4000000L, "user1", 1.3, Seq(0.1, 0.2)),
  (System.currentTimeMillis + 5000000L, "user1", 2.3, Seq(0.1, 0.2)),
  (System.currentTimeMillis + 6000000L, "user2", 2.3, Seq(0.1, 0.2))
).toDF("t", "u", "s", "l")

val get_time = udf((x: Long) => {
  new java.sql.Timestamp(x).toString
})
val below = df.
  withColumn("t", get_time($"t")).
  withColumn("struct", struct($"t", $"s", $"l")).
  select("u", "struct").
  groupBy("u").agg(collect_list("struct").as("struct"))

val res = df.
  withColumn("min", min("t").over(org.apache.spark.sql.expressions.Window.partitionBy("u"))).
  withColumn("max", max("t").over(org.apache.spark.sql.expressions.Window.partitionBy("u"))).
  filter("s > 1.0").join(below, Seq("u"))

/*
+-----+-------------+---+----------+-------------+-------------+--------------------+
|    u|            t|  s|         l|          min|          max|              struct|
+-----+-------------+---+----------+-------------+-------------+--------------------+
|user1|1501200459653|2.3|[0.1, 0.2]|1501195459653|1501200459653|[[2017-07-28 11:0...|
|user1|1501199459653|1.3|[0.1, 0.2]|1501195459653|1501200459653|[[2017-07-28 11:0...|
+-----+-------------+---+----------+-------------+-------------+--------------------+
*/

val below = df.
  withColumn("t", get_time($"t")).
  withColumn("struct", struct($"t", $"s", $"l")).
  select("u", "struct").
  groupBy("u").agg(collect_list("struct").as("struct"))

val res = below.
  select($"u", explode($"struct").as("x"), $"struct").
  select($"u", $"x.l".as("l"), $"x.t".as("t"), $"x.s".as("s"), $"struct").
  filter($"s" > 1.0)
res.show

/*
+-----+----------+--------------------+---+--------------------+
|    u|         l|                   t|  s|              struct|
+-----+----------+--------------------+---+--------------------+
|user1|[0.1, 0.2]|2017-07-28 12:12:...|2.3|[[2017-07-28 11:0...|
|user1|[0.1, 0.2]|2017-07-28 11:55:...|1.3|[[2017-07-28 11:0...|
|user2|[0.1, 0.2]|2017-07-28 12:29:...|2.3|[[2017-07-28 12:2...|
+-----+----------+--------------------+---+--------------------+
*/
