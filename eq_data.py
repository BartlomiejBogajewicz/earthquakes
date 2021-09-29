# Databricks notebook source
import requests 
import json
from geopy.geocoders import Nominatim

from pyspark.sql.functions import udf, col,explode,from_unixtime,substring,length,expr
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

geolocator = Nominatim(user_agent="get_eq_position")
def position(lad,long):
      coord = str(lad) + ", " + str(long)
      try:
        location = geolocator.reverse(coord)
        return location.raw['address'].get("country","")
      except:
        return None

# COMMAND ----------

position(10,139)

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

spark.udf.register("udf_position", position, StringType())

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
                 col("data.geometry.coordinates")[1].alias("latitude"),col("data.geometry.coordinates")[0].alias("longitude"),\
                 col("data.properties.alert").alias("alert"),col("data.properties.tsunami").alias("tsunami")).orderBy("timestamp_UTC",ascending=False).write.mode("overwrite").saveAsTable("earthquakes")

# COMMAND ----------

#result_df = result_df.select(result_df.place,result_df.timestamp_UTC,result_df.mag,result_df.latitude,result_df.longitude,\
#                             expr("udf_position(latitude,longitude)").alias("position")).first()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from earthquakes
