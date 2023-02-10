import pyspark
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.streaming import StreamingQueryManager
import findspark


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'
findspark.init()

spark = SparkSession.builder.master("spark://172.17.0.1:7087").appName('HumidityProcess.com') \
              .config("spark.jars", "./postgresql-42.5.2.jar") \
              .getOrCreate()
              
spark.sparkContext.setLogLevel("ERROR")

bootstrap_servers = "localhost:9092"
topic_name = "humidity"

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
                    StructField("humidity", FloatType(), True),
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
]), True), True), StructField("city", StringType(), True)])

sampleDataFrame = spark.readStream.format("kafka") \
       .option("kafka.bootstrap.servers", bootstrap_servers) \
       .option("subscribe", topic_name) \
       .option("startingOffsets", "latest") \
       .load()

# sampleDataFrame.printSchema()       

data_df = sampleDataFrame.selectExpr("CAST(value as string)", "timestamp")

weather_df = data_df.select(from_json(col("value"), schema=schema).alias("sample"), "timestamp")

weather_df = weather_df.withColumn("city", weather_df["sample"]["city"])
weather_df = weather_df.withColumn("station_id", weather_df["sample"]["observations"].getItem(0)["stationID"])
weather_df = weather_df.withColumn("obs_time_utc", weather_df["sample"]["observations"].getItem(0)["obsTimeUtc"])
weather_df = weather_df.withColumn("obs_time_local", weather_df["sample"]["observations"].getItem(0)["obsTimeLocal"])
weather_df = weather_df.withColumn("neighborhood", weather_df["sample"]["observations"].getItem(0)["neighborhood"])
weather_df = weather_df.withColumn("software_type", weather_df["sample"]["observations"].getItem(0)["softwareType"])
weather_df = weather_df.withColumn("solar_radiation", weather_df["sample"]["observations"].getItem(0)["solarRadiation"])
weather_df = weather_df.withColumn("lon", weather_df["sample"]["observations"].getItem(0)["lon"])
weather_df = weather_df.withColumn("realtime_frequency", weather_df["sample"]["observations"].getItem(0)["realtimeFrequency"])
weather_df = weather_df.withColumn("epoch", weather_df["sample"]["observations"].getItem(0)["epoch"])
weather_df = weather_df.withColumn("country", weather_df["sample"]["observations"].getItem(0)["country"])
weather_df = weather_df.withColumn("lat", weather_df["sample"]["observations"].getItem(0)["lat"])
weather_df = weather_df.withColumn("uv", weather_df["sample"]["observations"].getItem(0)["uv"])
weather_df = weather_df.withColumn("windir", weather_df["sample"]["observations"].getItem(0)["windir"])
weather_df = weather_df.withColumn("humidity", weather_df["sample"]["observations"].getItem(0)["humidity"])
weather_df = weather_df.withColumn("qc_status", weather_df["sample"]["observations"].getItem(0)["qcStatus"])
weather_df = weather_df.withColumn("temp", weather_df["sample"]["observations"].getItem(0)["imperial"]["temp"])
weather_df = weather_df.withColumn("heat_index", weather_df["sample"]["observations"].getItem(0)["imperial"]["temp"])
weather_df = weather_df.withColumn("dewpt", weather_df["sample"]["observations"].getItem(0)["imperial"]["temp"])
weather_df = weather_df.withColumn("wind_chill", weather_df["sample"]["observations"].getItem(0)["imperial"]["windChill"])
weather_df = weather_df.withColumn("wind_speed", weather_df["sample"]["observations"].getItem(0)["imperial"]["windSpeed"])
weather_df = weather_df.withColumn("wind_gust", weather_df["sample"]["observations"].getItem(0)["imperial"]["windGust"])
weather_df = weather_df.withColumn("pressure", weather_df["sample"]["observations"].getItem(0)["imperial"]["pressure"])
weather_df = weather_df.withColumn("precip_rate", weather_df["sample"]["observations"].getItem(0)["imperial"]["precipRate"])
weather_df = weather_df.withColumn("precip_total", weather_df["sample"]["observations"].getItem(0)["imperial"]["precipTotal"])
weather_df = weather_df.withColumn("elev", weather_df["sample"]["observations"].getItem(0)["imperial"]["elev"])

weather_df.drop("sample").printSchema()

output_data = weather_df.select("station_id", "obs_time_utc", "obs_time_local", "neighborhood", "software_type", "solar_radiation", "lon", 
                                "realtime_frequency", "epoch", "lat", "uv", "windir", "humidity", "qc_status", "temp", "heat_index", "city",
                                "dewpt", "wind_chill", "wind_speed", "wind_gust", "pressure", "precip_rate", "precip_total", "elev", "country")

humidity_df = weather_df.select("station_id", "city", "obs_time_utc", "obs_time_local", "humidity", "dewpt", 
                                "pressure", "precip_rate", "precip_total", "lon", "lat", "country")
def foreach_batch_function1(df, epoch_id):
    df.write.format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/test") \
      .option("driver", "org.postgresql.Driver") \
      .option("dbtable","humidity").option("user","duc") \
      .option("password", "root") \
      .mode("append").save()

humidity_df.writeStream.foreachBatch(foreach_batch_function1).start().awaitTermination()
