import pyspark
import os
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import findspark

spark = SparkSession.builder.config("spark.jars", "./postgresql-42.5.2.jar") \
                            .master("local").appName("Test").getOrCreate()

studentDf = spark.createDataFrame([
	Row(id=1,name='vijay',marks=67),
	Row(id=2,name='Ajay',marks=88),
	Row(id=3,name='jay',marks=79),
	Row(id=4,name='vinay',marks=67),
])

studentDf.show()

# studentDf.select("id", "name", "marks").write.format("jdbc") \
#          .option("url", "jdbc:postgresql://localhost:5432/test") \
#          .option("driver", "org.postgresql.Driver") \
#          .option("dbtable", "students") \
#          .option("user", "duc").option("password", "root").save()
         
def foreach_batch_function(df, epoch_id):
    df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/test") \
      .option("driver", "org.postgresql.Driver") \
      .option("dbtable","test").option("user","postgres") \
      .option("password", "root").mode("append")
      
studentDf.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

