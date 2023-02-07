import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com') \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
        .config("spark.mongodb.input.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.Artists_test")\
        .config("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.Artists_test")\
        .getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./dict2016.csv")

df.show()