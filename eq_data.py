# Databricks notebook source
import requests 
import json
from zipfile import ZipFile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,explode,expr,from_unixtime,substring,length,regexp_extract,broadcast,lit,count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType
from pyspark.sql import Row

# COMMAND ----------

#get data from earthquake.usgs.gov api 
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

#create schema for earthquakes dataset
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

#api request is executed by spark and saved in dataframe
udf_execute_api = udf(make_request_usgs, schema)

api_row = Row("startdate", "enddate")
request_df = spark.createDataFrame([
            api_row('2021-10-03','2021-10-04')
          ])

result_df = request_df.withColumn("result", udf_execute_api(col("startdate"), col("enddate"))) 

# COMMAND ----------

#data tranformation
result_df = result_df.select(explode(col("result.features")).alias("data"))

result_df = result_df.select(col("data.properties.place").alias("place"),\
                 expr("from_unixtime(substring(data.properties.time,0,length(data.properties.time)-3),'dd-MM-yyyy HH:mm:ss')").alias("timestamp_UTC"),\
                 col("data.properties.mag").cast(FloatType()).alias("mag"),\
                 col("data.geometry.coordinates")[1].cast(FloatType()).alias("latitude"),\
                 col("data.geometry.coordinates")[0].cast(FloatType()).alias("longitude"),\
                 col("data.properties.alert").alias("alert"),col("data.properties.tsunami").alias("tsunami")).\
                 orderBy("timestamp_UTC",ascending=False)

# COMMAND ----------

if 'locations_vw' not in sqlContext.tableNames():
  dbutils.notebook.run("get_locations",600)

# COMMAND ----------

#mapping coordinates from earthquakes api to nearest coordinates in location file to know in which country the earthquake had occured
result_df.createOrReplaceTempView("result_vw")

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

result_with_countries_df.show()

# COMMAND ----------

#aggreagate the data and save results
result_agg_df = result_with_countries_df.groupBy("country_name").agg(count(lit(1)).alias("earthquakes_no"))

result_agg_df.write.mode("overwrite").saveAsTable("earthquakes_agg")
result_with_countries_df.write.mode("overwrite").saveAsTable("earthquakes")

# COMMAND ----------


