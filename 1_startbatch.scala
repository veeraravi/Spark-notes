package main.scala.com.sapient.rfi.invesco

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import spark.implicits._
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext

object startbatch {
  def main(args: Array[String]){
  if (args.length < 1) {
      System.err.println("Usage: SchemaValidation <dd-mm-yyyy>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("start_batch")
      .getOrCreate()
    
  val sqlContext = new HiveContext(sc)
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    val date = format.format(new java.util.Date())
    
    println("************** Write to metadata table for start of batch zone**********************")
    val start_batch_file = FileSystem.get(new URI("s3://inv-rfi-landing"), sc.hadoopConfiguration).exists(new Path("s3://inv-rfi-landing/security-master/" + date + "/Security*"))
    
    if(start_batch_file)
    {
    
    val batch_num = sqlContext.sql("select nvl(max(batch_id),0)+1 as batch_id from batch_table").first.getInt(0)
    val landing_rec_cnt = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("s3://inv-rfi-landing/security-master/"+date+"/*").count
    
    sqlContext.sql("insert into table batch_table select t.* from (select "+batch_num+",'s3://inv-rfi-landing/security-master/" + date + "/Security',"+landing_rec_cnt+",FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm'),'null') t")
    }
    prinln("Success!!!")
    spark.stop()
}
}