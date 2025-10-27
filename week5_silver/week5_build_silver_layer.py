import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_handle = os.environ.get("AWS_HANDLE")
load_location = f"s3a://hwe-fall-2025/{aws_handle}/bronze/reviews"
save_location = f"s3a://hwe-fall-2025/{aws_handle}/silver/reviews"


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

bronze_schema = StructType([
    StructField('marketplace', StringType(), True,),
    StructField('customer_id', StringType(), True,),
    StructField('review_id', StringType(), True,),
    StructField('product_id', StringType(), True,),
    StructField('product_parent', StringType(), True,),
    StructField('product_title', StringType(), True,),
    StructField('product_category', StringType(), True,),
    StructField('star_rating', StringType(), True,),
    StructField('helpful_votes', StringType(), True,),
    StructField('total_votes', StringType(), True,),
    StructField('vine', StringType(), True,),
    StructField('verified_purchase', StringType(), True,),
    StructField('review_headline', StringType(), True,),
    StructField('review_body', StringType(), True,),
    StructField('purchase_date', StringType(), True,),
    StructField('review_timestamp', TimestampType(), False,)
])

bronze_reviews = spark \
                .readStream \
                .format("parquet") \
                .schema(bronze_schema) \
                .option("maxFilesPerTrigger", "1") \
                .load(load_location) 

bronze_reviews.createOrReplaceTempView("reviews")
reviews_df = spark.sql("select * from reviews")

bronze_customers = spark.read.parquet(f"s3a://hwe-fall-2025/{aws_handle}/bronze/customers")
bronze_customers.createOrReplaceTempView("customers")
customers_df = spark.sql("select * from customers")


silver_data = spark.sql("select r.*, c.customer_name, c.gender, c.date_of_birth, c.city, c.state from reviews as r left outer join customers as c on r.customer_id = c.customer_id")
silver_data.filter(col("customer_id").isNotNull())
# silver_data.printSchema()

streaming_query =  silver_data \
  .writeStream \
  .format("parquet") \
  .outputMode("append") \
  .option("path", save_location) \
  .option("checkpointLocation", "/tmp/silver_checkpoint") 

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
