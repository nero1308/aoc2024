from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, abs, col, sum
from pyspark.sql.types import StructType, StructField, IntegerType
spark = SparkSession.builder.appName('Day1').getOrCreate()


schema = StructType([
    StructField("loc1", IntegerType(), False),
    StructField("loc2", IntegerType(), False)])

input = spark.read.csv("input.txt", sep="   ", schema=schema)

l1 = input.select("loc1") \
    .sort("loc1") \
    .withColumn("id1", monotonically_increasing_id())

l2 = input.select("loc2") \
    .sort("loc2") \
    .withColumn("id2", monotonically_increasing_id())

df = l1.join(l2, l1.id1 == l2.id2, "inner")
df = df.withColumn("distance", abs(df.loc1 - df.loc2))

res = df.select(sum(col("distance")))

res.show()

df2 = l1.join(l2, l1.loc1 == l2.loc2, "inner")

res2 = df2.select(sum(col("loc1")))

res2.show()