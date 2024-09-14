from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, regexp_replace, lower, lit, unix_timestamp
from pyspark.sql.types import DateType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("ETL NYC Airbnb Data").getOrCreate()


# Step 3: Load - Write the transformed data to PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"  # Update with your correct PostgreSQL IP address
table_name = "airbnb"  # Target table name
properties = {
    "user": "postgres",  # PostgreSQL username
    "password": "admin",  # PostgreSQL password
    "driver": "org.postgresql.Driver"
}


# 1. EXTRACT - Read data from PostgreSQL (local or another source)
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# 2. TRANSFORM - Clean the dataset
# Handle missing values
df = df.fillna({'name': 'Unknown', 'host_name': 'Unknown'})

avg_reviews = df.select(mean(col('reviews_per_month'))).collect()[0][0]
df = df.fillna({'reviews_per_month': avg_reviews})

df = df.withColumn("last_review", when(col("last_review").isNull(), "1970-01-01").otherwise(col("last_review")))

# Convert 'last_review' to DateType
df = df.withColumn("last_review", col("last_review").cast(DateType()))

# Handle outliers in price and availability
df = df.filter((df['price'] > 0) & (df['price'] <= 10000)) # Remove negative prices and extremely high prices
df = df.filter(df['minimum_nights'] <= 365)

# Remove duplicates based on 'id'
df = df.dropDuplicates(['id'])

# Clean categorical data (standardize case)
df = df.withColumn("room_type", lower(col("room_type")))
df = df.withColumn("neighbourhood_group", lower(col("neighbourhood_group")))

# Validate latitude and longitude (for NYC bounds)
df = df.filter((col("latitude") >= 40.477399) & (col("latitude") <= 40.917577) &
               (col("longitude") >= -74.259090) & (col("longitude") <= -73.700272))

# Validate 'availability_365'
df = df.filter((df['availability_365'] >= 0) & (df['availability_365'] <= 365))

# Check for future dates in 'last_review'
current_date = F.current_date()
df = df.filter(unix_timestamp(col('last_review')) <= unix_timestamp(lit(current_date)))

# Handle special characters in 'name'
df = df.withColumn("name", regexp_replace(col("name"), "[^a-zA-Z0-9 ]", ""))

# Remove irrelevant columns
df = df.drop("id")

# 3. LOAD - Write cleaned data into Railway PostgreSQL cloud
railway_jdbc_url = "jdbc:postgresql://<host>:<port>/<db_name>"
railway_properties = {
    "user": "<user>",
    "password": "<password>",
    "driver": "org.postgresql.Driver"
}


# Write to Railway PostgreSQL with overwrite mode
#df.write.jdbc(url=railway_jdbc_url, table="cleaned_nyc_airbnb", mode="overwrite", properties=railway_properties)

df.write \
    .format("jdbc") \
    .option("url", railway_jdbc_url) \
    .option("dbtable", "cleaned_nyc_airbnb") \
    .option("user", railway_properties["user"]) \
    .option("password", railway_properties["password"]) \
    .option("driver", railway_properties["driver"]) \
    .mode("overwrite") \
    .save()

# Stop the Spark session
spark.stop()

