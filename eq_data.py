# Databricks notebook source
import requests 
import json
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,explode,expr,from_unixtime,substring,length,regexp_extract,broadcast,lit,count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType
from pyspark.sql import Row

# COMMAND ----------

def make_request_usgs(startdate,enddate):
    
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?"

    parameters = {
        'format':'geojson',
        'starttime':startdate,
        'endtime':enddate
    }
    try:
        response = requests.get(url,params = parameters)
        return json.loads(response.text)
    except:
        return None

# COMMAND ----------

with open("/dbfs/allCountries.zip","wb") as file:
  reponse = requests.get("http://download.geonames.org/export/dump/allCountries.zip")
  file.write(reponse.content)

# COMMAND ----------

with ZipFile("/dbfs/allCountries.zip","r") as zip:
  zip.extract("allCountries.txt","/dbfs")

# COMMAND ----------

schema = StructType([
  StructField("features", ArrayType(
    StructType([
      StructField("properties", 
          StructType([StructField("mag",StringType()),
                      StructField("place",StringType()),
                      StructField("time",StringType()),
                      StructField("alert",StringType()),
                      StructField("tsunami",StringType()),
                     ])
                 ),
      StructField("geometry",StructType([
         StructField("coordinates",ArrayType(StringType()))]))
            ])
  ))])

# COMMAND ----------

udf_execute_api = udf(make_request_usgs, schema)

# COMMAND ----------

api_row = Row("startdate", "enddate")
request_df = spark.createDataFrame([
            api_row('2021-09-28','2021-09-29')
          ])

# COMMAND ----------

result_df = request_df.withColumn("result", udf_execute_api(col("startdate"), col("enddate"))) 

# COMMAND ----------

result_df = result_df.select(explode(col("result.features")).alias("data"))

# COMMAND ----------

result_df = result_df.select(col("data.properties.place").alias("place"),\
                 expr("from_unixtime(substring(data.properties.time,0,length(data.properties.time)-3),'dd-MM-yyyy HH:mm:ss')").alias("timestamp_UTC"),\
                 col("data.properties.mag").cast(FloatType()).alias("mag"),\
                 col("data.geometry.coordinates")[1].cast(FloatType()).alias("latitude"),\
                 col("data.geometry.coordinates")[0].cast(FloatType()).alias("longitude"),\
                 col("data.properties.alert").alias("alert"),col("data.properties.tsunami").alias("tsunami")).\
                 orderBy("timestamp_UTC",ascending=False)

# COMMAND ----------

def download_country_mapper():
    response = requests.get("https://raw.githubusercontent.com/umpirsky/country-list/master/data/ak/country.txt")
    data = list()
    
    for item in response.text.split("\n")[0:-1]:
        a,b = item[0:-5],item[-4:]
        data.append([a,b])
        
    return data


# COMMAND ----------

countries_mapper_df = spark.createDataFrame(download_country_mapper(),\
                                         StructType([StructField("country_name",StringType()),\
                                                     StructField("country_code",StringType())]))

countries_mapper_df = countries_mapper_df.select(col("country_name"),\
                                           regexp_extract(col("country_code"),"([A-Z]+)",1).alias("country_code"))


# COMMAND ----------

locations_df = spark.read.option("delimiter","\t").csv("/allCountries.txt")

locations_df = locations_df.select(col("_c1").alias("location"),col("_c4").cast(FloatType()).alias("latitude"),\
                                   col("_c5").cast(FloatType()).alias("longitude"),col("_c8").alias("country_code"))

locations_df = locations_df.join(broadcast(countries_mapper_df),"country_code").drop("country_code")

locations_df = locations_df.dropDuplicates(["latitude","longitude"])

# COMMAND ----------

result_df.createOrReplaceTempView("result_vw")
locations_df.createOrReplaceTempView("locations_vw")

result_with_countries_df = spark.sql("""select  r.place
                                              , r.timestamp_UTC
                                              , r.mag
                                              , r.latitude
                                              , r.longitude
                                              , NVL(l.country_name,"Ocean") as country_name
                                      from result_vw r
                                      left join locations_vw l ON (l.latitude between r.latitude -0.2 and r.latitude + 0.2)
                                                              and (l.longitude between r.longitude -0.2 and r.longitude + 0.2) """).dropDuplicates(["timestamp_UTC","mag","latitude","longitude"]).cache()

# COMMAND ----------

result_agg_df = result_with_countries_df.groupBy("country_name").agg(count(lit(1)).alias("earthquakes_no"))

result_agg_df.write.mode("overwrite").saveAsTable("earthquakes_agg")
result_with_countries_df.write.mode("overwrite").saveAsTable("earthquakes")
