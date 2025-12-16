from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('./data/*.csv', header=True, inferSchema=True)

df.printSchema()

df.show(10)

print(f"Total rows: {df.count()}")