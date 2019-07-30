from pyspark.sql.functions import regexp_replace, trim, col, lower
import string
import re
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import datetime
spark=SparkSession.builder.appName("PythonSQL").getOrCreate()
dateparquet=datetime.datetime.today().strftime('%d-%m-%Y')
sourcedir_country = "s3://rfi-landing/country/Country.csv"
sourcedir_currency = "s3://rfi-landing/currency/Currency.csv"
sourcedir_issuer = "s3://rfi-landing/issue-master/issue-master"
targetdir_country = "s3://rfi-staging/reference-files/Country"
targetdir_currency = "s3://rfi-staging/reference-files/Currency"
targetdir_issuer = "s3://rfi-staging/reference-files/Issue_Master"
df_country = spark.read.csv(sourcedir_country, sep=',', header='true')
df_currency = spark.read.csv(sourcedir_currency, sep=',', header='true')
df_issuer = spark.read.csv(sourcedir_issuer, sep=',', header='true')
df_country.write.parquet(targetdir_country)
df_currency.write.parquet(targetdir_currency)
df_issuer.write.parquet(targetdir_issuer)
