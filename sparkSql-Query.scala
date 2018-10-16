// == Traffic case ==
val df = Seq(
  ("1", "2", 3),
  ("3", "2", 2),
  ("1", "3", 3),
  ("2", "1", 2),
  ("2", "3", 5)
).toDF("a", "b", "cnt")
df.createOrReplaceTempView("test")

sql("""
  select 
    case when (a > b) then b else a end as a, 
    case when (a > b) then a else b end as b,
    sum(cnt)
  from test
  group by 1, 2
""").show

// == Proxy case ==
val df = Seq(
  ("2", "A", "1", "B", "3", "C", 2),
  ("2", "A", "1", "B", "3", "C", 4),
  ("1", "A", "2", "B", "3", "C", 3),
  ("1", "A", "3", "B", "5", "C", 1)
).toDF("a", "a_type", "b", "b_type", "c", "c_type", "time")
df.createOrReplaceTempView("test")

sql("""
  with t as (
    select a, a_type, b, b_type, c, c_type, min(time) as min, max(time) as max
    from test
    group by a, a_type, b, b_type, c, c_type
  ), 
  p as (
    select a_type as t1, a as v1, b_type as t2, b as v2, min, max from t
    union all
    select b_type as t1, b as v1, c_type as t2, c as v2, min, max from t
    union all 
    select a_type as t1, a as v1, c_type as t2, c as v2, min, max from t    
  )
  select 
    case when (v1 > v2) then v2 else v1 end as col1, 
    case when (v1 > v2) then t2 else t1 end as col1_type, 
    case when (v1 > v2) then v1 else v2 end as col2,
    case when (v1 > v2) then t1 else t2 end as col2_type,
    min(min) as min,
    max(max) as max
  from p
  group by 1, 2, 3, 4
""").show

// == with if ==
sql("""
  with t as (
    select a, a_type, b, b_type, c, c_type, min(time) as min, max(time) as max
    from test
    group by a, a_type, b, b_type, c, c_type
  ), 
  p as (
    select a_type as t1, a as v1, b_type as t2, b as v2, min, max from t
    union all
    select b_type as t1, b as v1, c_type as t2, c as v2, min, max from t
    union all 
    select a_type as t1, a as v1, c_type as t2, c as v2, min, max from t    
  )
  select 
    if (v1 > v2, v2, v1) as col1,
    if (v1 > v2, t2, t1) as col1_type,
    if (v1 > v2, v1, v2) as col2,
    if (v1 > v2, t1, t2) as col2_type,
    min(min) as min,
    max(max) as max
  from p
  group by 1, 2, 3, 4
""").show

+----+---------+----+---------+---+---+
|col1|col1_type|col2|col2_type|min|max|
+----+---------+----+---------+---+---+
|   2|        A|   2|        B|  4|  4|
|   2|        B|   3|        C|  3|  4|
|   1|        A|   3|        C|  3|  3|
|   1|        A|   3|        B|  1|  1|
|   1|        B|   3|        C|  2|  2|
|   3|        B|   5|        C|  1|  1|
|   1|        A|   2|        B|  3|  3|
|   1|        B|   2|        A|  2|  2|
|   1|        A|   5|        C|  1|  1|
|   2|        A|   3|        C|  2|  4|
+----+---------+----+---------+---+---+
