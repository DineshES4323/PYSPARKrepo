from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, isnan

spark = SparkSession.builder.appName("MissingValuesHandler").getOrCreate()

data = [
    (1000, 5000000, "Chennai"),
    (None, 4500000, "Bangalore"),
    (1200, None, "Delhi"),
    (None, None, None),
    (900, 4000000, None)
]

columns = ["area", "price", "location"]

df = spark.createDataFrame(data, columns)

print("Original Data:")
df.show()

print("Missing Values Count:")
df.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c)
    for c in df.columns
]).show()

df_cleaned = df.dropna()

print("After Dropping Nulls:")
df_cleaned.show()

df_filled = df.fillna({
    "area": 0,
    "price": 0,
    "location": "Unknown"
})

print("After Filling Nulls:")
df_filled.show()
