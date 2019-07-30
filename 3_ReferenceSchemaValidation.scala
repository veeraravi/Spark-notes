/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.sapient.rfi.invesco

import org.apache.spark.sql.SparkSession

object ReferenceSchemaValidation {

  /** Usage: SchemaValidation [file] */
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("ReferenceSchemaValidation <srcdir> <tgtsuccessdir> <tgtrejectdir> <expectedschema>")
      .getOrCreate()

//    val country_sourcedir = "s3://inv-rfi-staging/reference-files/Country"
//    val country_targetdir = "s3://inv-rfi-rawzone/reference-files/Country/success_files"
//    val country_rejectdir = "s3://inv-rfi-rawzone/reference-files/Country/reject-files"
    val sourcedir = args(0)
    val targetdir = args(1)
    val rejectdir = args(2)
//    val expectedcountryschema="struct<COUNTRY_ISO_2:string,COUNTRY_ISO_3:string,COUNTRY_NAME:string>"
//    val expectecurrencydschema="struct<ISO_CCY_CODE:string,ISO_DESCRIPTION:string,COUNTRY_ISO_2:string>"
//    val expectedissuerschema="struct<CADIS_ID_ISSUER:string,LONG_NAME:string,SHORT_NAME:string,COUNTRY_OF_DOMICILE:string,COUNTRY_OF_INCORPORATION:string,COUNTRY_OF_RISK_IVZ:string,BLK_ISSUER_ID:string,REPORT_ISSUER_NAME:string,COUNTRY_RISK_ISO_CODE:string,ISSUER_ISIN:string,ID_BB_GLOBAL_COMPANY:string>"
    val df =  spark.read.option("header", true).csv(sourcedir)
    val expectedschema=args(3)
    if (df.schema.simpleString == expectedschema)
      df.write.parquet(targetdir)
    else
      df.write.parquet(rejectdir)
    spark.stop()
  }
}


// scalastyle:on println
