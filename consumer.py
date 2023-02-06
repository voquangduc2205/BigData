import pyspark
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

bootstrap_servers = "localhost:9092"
topic_name = "weather_data"


schema = StructType([StructField("observations", ArrayType(StructType([
                    StructField("stationID", StringType(), True),
                    StructField("obsTimeUtc", DateType(), True),
                    StructField("obsTimeLocal", StringType(), True),
                    StructField("neighborhood", StringType(), True),
                    StructField("softwareType", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("solarRadiation", StringType(), True),
                    StructField("lon", FloatType(), True),
                    StructField("realtimeFrequency", StringType(), True),
                    StructField("epoch", LongType(), True),
                    StructField("lat", FloatType(), True),
                    StructField("uv", StringType(), True),
                    StructField("windir", FloatType(), True),
                    StructField("qcStatus", IntegerType(), True),
                    StructField("imperial", 
                                StructType([StructField("temp", FloatType(), True),
                                            StructField("heatIndex", FloatType(), True),
                                            StructField("dewpt", FloatType(), True),
                                            StructField("windChill", FloatType(), True),
                                            StructField("windSpeed", FloatType(), True),
                                            StructField("windGust", FloatType(), True),
                                            StructField("pressure", FloatType(), True),
                                            StructField("precipRate", FloatType(), True),
                                            StructField("precipTotal", FloatType(), True),
                                            StructField("elev", FloatType(), True)]), True)
]), True), True)])

data = spark.readStream.format("kafka") \
       .option("kafka.bootstrap.servers", bootstrap_servers) \
       .option("subscribe", topic_name) \
       .option("includeHeaders", "true") \
       .load()
       
data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
data.printSchema()

print(type(data))
# table = data.select(from_json(data.value.cast("string"), schema=schema)).alias("observations")

# print('=====================')
# query = table.select("observations.*")
# print(query.printSchema())
# print('Done')