package com.sapient.rfi.invesco

import org.apache.spark.sql.SparkSession
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import spark.implicits._
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by roshni on 12/26/2016.
  */
object DataValidation {
  def main(args: Array[String]) {
    /*if (args.length < 1) {
      System.err.println("Usage: DataValidation <dd-mm-yyyy>")
      System.exit(1)
    }*/
    val spark = SparkSession
      .builder
      .appName("DataValidation")
      .getOrCreate()

    val sqlContext = new HiveContext(sc)
    val batch_num = sqlContext.sql("select batch_id from batch_table where batch_end_time = 'null'").first.getInt(0)
    val batch_start_time = sqlContext.sql("select batch_start_time from batch_table where batch_end_time = 'null'").first.getString(0)
    println("*****************Get the system Date**************************")
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    val date = format.format(new java.util.Date())
    val security_data = spark.read.parquet("s3://inv-rfi-rawzone/security-master/success_files/" + date + "/*")
    val raw_zone_flag_suc = FileSystem.get(new URI("s3://inv-rfi-rawzone"), sc.hadoopConfiguration).exists(new Path("s3://inv-rfi-rawzone/security-master/success_files/" + date + "/_SUCCESS"))
    
    val raw_zone_rec_cnt  = security_data.count
    
    
    val raw_zone_flag_rej = FileSystem.get(new URI("s3://inv-rfi-rawzone"), sc.hadoopConfiguration).exists(new Path("s3://inv-rfi-rawzone/security-master/reject-files/" + date + "/_SUCCESS"))
    
    val raw_zone_rej_count = spark.read.parquet("s3://inv-rfi-rawzone/security-master/reject-files/" + date + "/*").count
    
    
    println("************** Write to metadata table for raw zone**********************")
    
    if(raw_zone_flag_suc)
    {
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",4,'success',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",5,'success',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",6,"+raw_zone_rec_cnt+",FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",'8','s3://inv-rfi-rawzone/security-master/success_files/"+date+"',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    }
    
    
    
    if(raw_zone_flag_rej)
    {
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",7,"+raw_zone_rej_count+",FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",9,'s3://inv-rfi-rawzone/security-master/reject-files/"+date+"',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    }
    
    println("Event Table data insertion successfull")
    
    println("*****************Starts Data loading**************************")
    security_data.createOrReplaceTempView("security_master")
    val country = spark.read.parquet("s3://inv-rfi-rawzone/reference-files/Country/*")
    country.createOrReplaceTempView("country")
    val currency = spark.read.parquet("s3://inv-rfi-rawzone/reference-files/Currency/*")
    currency.createOrReplaceTempView("currency")
    val issue_master = spark.read.parquet("s3://inv-rfi-rawzone/reference-files/Issue-Master/*")
    issue_master.createOrReplaceTempView("issue_master")
    println("*****************Starts Data Validation**************************")
    val gooddata = spark.sql("SELECT s.* from security_master s JOIN country c JOIN currency cur JOIN issue_master im  where s.Country_Issue = c.COUNTRY_ISO_2 and s.CURRENCY_TRADED = cur.ISO_CCY_CODE and s.CADIS_ID_ISSUER = im.CADIS_ID_ISSUER")
    gooddata.createOrReplaceTempView("data_filtered_good_table")
    
    println("**************Bad reasons capture for bad records******************8")
    
    val good_country_rec = spark.sql("SELECT s.* from security_master s JOIN country c where s.Country_Issue = c.COUNTRY_ISO_2" )
    val bad_country = security_data.except(good_country_rec)
    bad_country.createOrReplaceTempView("bad_country")
    val step1_country = spark.sql("select b.* ,'Invalid Country' as reason_for_failure from bad_country b")
    
    
    val good_currency_rec = spark.sql("SELECT s.* from security_master s JOIN currency cur where s.CURRENCY_TRADED = cur.ISO_CCY_CODE")
    val bad_currency = security_data.except(good_currency_rec)
    bad_currency.createOrReplaceTempView("bad_currency")
    val step2_currency = spark.sql("select b.* ,'Invalid currency' as reason_for_failure from bad_currency b")
    
    
    val good_im_rec = spark.sql("SELECT s.* from security_master s JOIN issue_master im where s.CADIS_ID_ISSUER = im.CADIS_ID_ISSUER")
    val bad_im = security_data.except(good_im_rec)
    bad_im.createOrReplaceTempView("bad_im")
    val step3_im = spark.sql("select b.* ,'Invalid Issue Master' as reason_for_failure from bad_im b")
    
    val final_baddata = step1_country.union(step2_currency).union(step3_im)
    
    final_baddata.createOrReplaceTempView("final_bad_table")
    
    println("*****************Starts Data Enrichment**************************")
    val final_gooddata = spark.sql("SELECT sm.CADIS_ID, sm.ASSET_CLASS_CODE, sm.BB_EXCH_CODE, sm.CADIS_ID_ISSUER, sm.COUNTRY_INCORPORATION, sm.COUNTRY_ISSUE, cou.COUNTRY_NAME as COUNTRY_ISSUE_DESC, sm.CURRENCY_TRADED, cur.ISO_DESCRIPTION as CURRENCY_TRADED_DESC, sm.CUSIP, concat(sm.CUSIP,'/',sm.ISIN) as CUSIP_ISIN,sm.DATA_QUALITY_IND, sm.INDUSTRY_CODE, sm.ISIN, sm.ISSUE_DATE, iss.LONG_NAME as ISSUER_LONG_NAME, iss.SHORT_NAME as ISSUER_SHORT_NAME, sm.PRIMARY_EXCHANGE, sm.PRIMARY_SECURITY_TICKER, sm.SEC_TYP_BB, sm.SEC_TYP2_BB, sm.SEDOL, sm.TICKER, sm.TICKER_BB, sm.CURRENCY_SETTLEMENT from data_filtered_good_table sm JOIN country cou JOIN currency cur JOIN issue_master iss  WHERE iss.CADIS_ID_ISSUER = sm.CADIS_ID_ISSUER and cou.COUNTRY_ISO_2  = sm.COUNTRY_ISSUE and sm.CURRENCY_TRADED = cur.ISO_CCY_CODE")
    println("*****************Load good and bad Data in Curated Zone**************************")
    final_gooddata.write.format("com.databricks.spark.csv").option("header","true").save("s3://inv-rfi-curatedzone/security-master/good-records/"+date)
    final_baddata.write.format("com.databricks.spark.csv").option("header","true").save("s3://inv-rfi-curatedzone/security-master/bad-records/"+date)
    println("Program ends for data validation")
    println("Start writting to event table if file is success")
    
    val good_cnt = final_gooddata.count
    
    val bad_cnt = final_baddata.count
    
    println("************** Write to metadata table for curated zone**********************")
    
    val curated_zone_flag = FileSystem.get(new URI("s3://inv-rfi-curatedzone"), sc.hadoopConfiguration).exists(new Path("s3://inv-rfi-curatedzone/security-master/good-records/"+date+"/_SUCCESS"))
    
    if(curated_zone_flag)
    {
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",10,'success',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",'13','s3://inv-rfi-curatedzone/security-master/good-records/"+date+"',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+ ",14,'s3://inv-rfi-curatedzone/security-master/bad-records/"+date+"',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",11,"+good_cnt+",FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into table event_metadata select t.* from (select "+batch_num+",12,"+bad_cnt+",FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    
    sqlContext.sql("insert into batch_table select t.* from (select "+batch_num+",'s3://inv-rfi-rawzone/security-master/"+date+"',"+raw_zone_rec_cnt+",'"+batch_start_time+"',FROM_UNIXTIME( UNIX_TIMESTAMP(), 'dd/MM/YYYY HH:mm')) t")
    }
    prinln("Success!!!")


    spark.stop()
  }
}