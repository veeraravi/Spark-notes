package main.scala.com.sapient.rfi.invesco

import java.util.Calendar
import org.apache.spark.sql.SparkSession
import spark.implicits._
import java.net.URI
import org.apache.spark.sql.hive.HiveContext
object end_batch {
  def main(args: Array[String]){
  if (args.length < 1) {
      System.err.println("Usage: SchemaValidation <dd-mm-yyyy>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("end_batch")
      .getOrCreate()
      val sqlContext = new HiveContext(sc)
      println("************** Write to metadata table for end of batch zone**********************")
      sqlContext.sql("insert into table batch_table (select batch_id,file_name,no_of_records,batch_start_time,FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm') from batch_table where batch_end_time = 'null')")
      sqlContext.sql("insert overwrite table batch_table (select * from batch_table where batch_end_time != 'null')")
      prinln("Success!!!")
      spark.stop()
  
}
}