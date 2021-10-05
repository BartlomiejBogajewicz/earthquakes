# Databricks notebook source
import requests 
import json
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,explode,expr,from_unixtime,substring,regexp_extract,broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType
from pyspark.sql import Row

# COMMAND ----------

#get list with all countries and their country codes
def download_country_mapper():
    response = requests.get("https://raw.githubusercontent.com/umpirsky/country-list/master/data/ak/country.txt")
    data = list()
    
    for item in response.text.split("\n")[0:-1]:
        a,b = item[0:-5],item[-4:]
        data.append([a,b])
        
    return data

# COMMAND ----------

#download the file containg over 12 milion coordinates 
with open("/dbfs/allCountries.zip","wb") as file:
  reponse = requests.get("http://download.geonames.org/export/dump/allCountries.zip")
  file.write(reponse.content)
  
with ZipFile("/dbfs/allCountries.zip","r") as zip:
  zip.extract("allCountries.txt","/dbfs")  

# COMMAND ----------

countries_mapper_df = spark.createDataFrame(download_country_mapper(),\
                                         StructType([StructField("country_name",StringType()),\
                                                     StructField("country_code",StringType())]))

countries_mapper_df = countries_mapper_df.select(col("country_name"),\
                                           regexp_extract(col("country_code"),"([A-Z]+)",1).alias("country_code"))


# COMMAND ----------

#joining list of countries with codes with list of location
locations_df = spark.read.option("delimiter","\t").csv("/allCountries.txt")

locations_df = locations_df.select(col("_c1").alias("location"),col("_c4").cast(FloatType()).alias("latitude"),\
                                   col("_c5").cast(FloatType()).alias("longitude"),col("_c8").alias("country_code"))

locations_df = locations_df.join(broadcast(countries_mapper_df),"country_code").drop("country_code")

locations_df.dropDuplicates(["latitude","longitude"]).write.mode("overwrite").saveAsTable("locations_vw")
