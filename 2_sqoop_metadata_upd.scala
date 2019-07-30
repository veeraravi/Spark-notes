package main.scala.com.sapient.rfi.invesco
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import spark.implicits._
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext

object sqoop_metadata_upd {
  def main(args: Array[String]){
  if (args.length < 1) {
      System.err.println("Usage: SchemaValidation <dd-mm-yyyy>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("Sqoop_Metadata_update")
      .getOrCreate()
      val sqlContext = new HiveContext(sc)
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    val date = format.format(new java.util.Date())
    val batch_num = sqlContext.sql("select batch_id from batch_table where batch_end_time = 'null'").first.getInt(0)
    println("************** Write to metadata table for staging zone**********************")
    val stag_flag = FileSystem.get(new URI("s3://inv-rfi-staging"), sc.hadoopConfiguration).exists(new Path("s3://inv-rfi-staging/security-master/" + date + "/_SUCCESS"))
    val staging_rec_cnt = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("s3://inv-rfi-staging/security-master/"+date+"/*").count
    if(stag_flag)
    {
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",1,'success',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",'2','s3://inv-rfi-staging/security-master/"+date+"',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",3,"+staging_rec_cnt+",FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    }
    prinln("Success!!!")
    spark.stop()
  
}
}