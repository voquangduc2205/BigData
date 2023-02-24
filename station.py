import os

import findspark
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQuery, StreamingQueryManager
from pyspark.sql.types import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'
findspark.init()

spark = SparkSession.builder.master("local[1]").appName('Temperature.com') \
              .config("spark.jars.packages", "postgresql-42.5.2.jar") \
              .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv('city.csv')

print(df.show())

def foreach_batch_function(df, epoch_id):
    print(df.show())
    df.write.format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/test") \
      .option("driver", "org.postgresql.Driver") \
      .option("dbtable","temperature_sample").option("user","postgres") \
      .option("password", "root") \
      .mode("append").save()

df.write.format("jdbc") \
      .option("url", "jdbc:postgresql://localhost:5432/test") \
      .option("driver", "org.postgresql.Driver") \
      .option("dbtable","station_sample").option("user","postgres") \
      .option("password", "root") \
      .mode("append").save()
