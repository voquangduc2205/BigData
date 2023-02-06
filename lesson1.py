import pyspark
import sys

from pyspark.sql.functions import count
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType, StructField, StructType

print(SparkSession)

student_data = [("Chris",1523,0.72,"CA"),
                ("Jake", 1555,0.83,"NY"),
                ("Cody", 1439,0.92,"CA"),
                ("Lisa",1442,0.81,"FL"),
                ("Daniel",1600,0.88,"TX"),
                ("Kelvin",1382,0.99,"FL"),
                ("Nancy",1442,0.74,"TX"),
                ("Pavel",1599,0.82,"NY"),
                ("Josh",1482,0.78,"CA"),
                ("Cynthia",1582,0.94,"CA")]

schema = StructType([
    
])

