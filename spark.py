import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./dict2016.csv")

df.show()