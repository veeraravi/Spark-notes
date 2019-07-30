from contextlib import contextmanager
import sys
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_replace, trim, col, lower
import string
import re
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
spark=SparkSession.builder.appName("PythonSQL").getOrCreate()
dateparquet=datetime.datetime.today().strftime('%d-%m-%Y')
sourcedir = "s3://rfi-landing/security-master/" + dateparquet + "/security_master_data"
targetdir = "s3://rfi-rawzone/security-master/" + dateparquet
rejectdir = "s3://rfi-rawzone/reject-files/" + dateparquet
df =  spark.read.csv(sourcedir, sep='|', header='false')
sqlContext.registerDataFrameAsTable(df,"df_table")
df_header=sqlContext.sql("select * from df_table where _c0 like '%CADIS_ID%' and _c0 not like '%CADIS_ID_ISSUER%' and _c0 like '%ASSET_CLASS_CODE%' and _c0 like '%COUNTRY_INCORPORATION%' and _c0 like '%COUNTRY_ISSUE%' and _c0 like '%CURRENCY_TRADED%' and _c0 like '%TICKER%' and _c0 like '%CURRENCY_SETTLEMENT%'")
var1=df_header.count()
df_org =  spark.read.csv(sourcedir, sep=',', header='true')
if var1 == 1:
	df_org.write.parquet(targetdir)
else:
	df_org.write.parquet(rejectdir)
	
