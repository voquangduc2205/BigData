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

sampleDataFrame = spark.readStream.format("kafka") \
       .option("kafka.bootstrap.servers", bootstrap_servers) \
       .option("subscribe", topic_name) \
       .option("startingOffsets", "latest") \
       .load()
       

data_df = sampleDataFrame.selectExpr("CAST(value as string)", "timestamp")

weather_df = data_df.select(from_json(col("value"), schema=schema).alias("sample"), "timestamp")

weather_df = weather_df.withColumn("stationID", weather_df["sample"]["observations"].getItem(0)["stationID"])
weather_df = weather_df.withColumn("obsTimeUtc", weather_df["sample"]["observations"].getItem(0)["obsTimeUtc"])
weather_df = weather_df.withColumn("obsTimeLocal", weather_df["sample"]["observations"].getItem(0)["obsTimeLocal"])
weather_df = weather_df.withColumn("neighborhood", weather_df["sample"]["observations"].getItem(0)["neighborhood"])
weather_df = weather_df.withColumn("softwareType", weather_df["sample"]["observations"].getItem(0)["softwareType"])
weather_df = weather_df.withColumn("solarRadiation", weather_df["sample"]["observations"].getItem(0)["solarRadiation"])
weather_df = weather_df.withColumn("longitude", weather_df["sample"]["observations"].getItem(0)["lon"])
weather_df = weather_df.withColumn("realtimeFrequency", weather_df["sample"]["observations"].getItem(0)["realtimeFrequency"])
weather_df = weather_df.withColumn("epoch", weather_df["sample"]["observations"].getItem(0)["epoch"])
weather_df = weather_df.withColumn("latitude", weather_df["sample"]["observations"].getItem(0)["lat"])
weather_df = weather_df.withColumn("uv", weather_df["sample"]["observations"].getItem(0)["uv"])
weather_df = weather_df.withColumn("windir", weather_df["sample"]["observations"].getItem(0)["windir"])
weather_df = weather_df.withColumn("qcStatus", weather_df["sample"]["observations"].getItem(0)["qcStatus"])
weather_df = weather_df.withColumn("temp", weather_df["sample"]["observations"].getItem(0)["imperial"]["temp"])
weather_df = weather_df.withColumn("heatIndex", weather_df["sample"]["observations"].getItem(0)["imperial"]["temp"])
weather_df = weather_df.withColumn("dewpt", weather_df["sample"]["observations"].getItem(0)["imperial"]["temp"])
weather_df = weather_df.withColumn("windChill", weather_df["sample"]["observations"].getItem(0)["imperial"]["windChill"])
weather_df = weather_df.withColumn("windSpeed", weather_df["sample"]["observations"].getItem(0)["imperial"]["windSpeed"])
weather_df = weather_df.withColumn("windGust", weather_df["sample"]["observations"].getItem(0)["imperial"]["windGust"])
weather_df = weather_df.withColumn("pressure", weather_df["sample"]["observations"].getItem(0)["imperial"]["pressure"])
weather_df = weather_df.withColumn("precipRate", weather_df["sample"]["observations"].getItem(0)["imperial"]["precipRate"])
weather_df = weather_df.withColumn("precipTotal", weather_df["sample"]["observations"].getItem(0)["imperial"]["precipTotal"])
weather_df = weather_df.withColumn("elev", weather_df["sample"]["observations"].getItem(0)["imperial"]["elev"])



weather_df.drop("sample").printSchema()

# table = data.select(from_json(data.value.cast("string"), schema=schema)).alias("observations")

# print('=====================')
# query = table.select("observations.*")
# print(query.printSchema())
# print('Done')