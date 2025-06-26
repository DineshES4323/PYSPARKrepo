
import pandas as pd
pd.read_parquet('/content/house-price.parquet')

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HousePriceAnalysis").getOrCreate()
df = spark.read.parquet("/content/house-price.parquet")


df.show(5)

df.printSchema()

df.describe().show()

df.columns


df.count()


df.groupBy("furnishingstatus").count().show()

df.filter(df["price"] > 5000000).show()


df.orderBy(df["price"].desc()).show(10)


from pyspark.sql.functions import col, isnan, when, count

df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show()


df = df.dropna()


df = df.fillna({
    "price": 0,
    "area": 0,
})

df = df.withColumn("price", col("price").cast("int"))


df = df.withColumnRenamed("sqft", "area")


df = df.withColumn("price_per_sqft", (col("price") / col("area")).cast("int"))


df.write.mode("overwrite").parquet("/mnt/data/cleaned_house_price.parquet")


df.describe().show()


from pyspark.sql.functions import avg

df.groupBy("furnishingstatus").agg(avg("price").alias("avg_price")).orderBy("avg_price", ascending=False).show()

from pyspark.sql.functions import round

df = df.withColumn("area_sq_m", round(col("area") * 0.092903, 2))


df.orderBy(df["price"].desc()).show(10)


df.write.mode("overwrite").parquet("/mnt/data/processed_house_price.parquet")


df = df.withColumn("price_per_sqft", (col("price") / col("area")).cast("int"))
df.select("area", "price", "price_per_sqft").show()

from pyspark.sql.functions import avg

df.groupBy("furnishingstatus").agg(avg("price").alias("avg_price")).show()