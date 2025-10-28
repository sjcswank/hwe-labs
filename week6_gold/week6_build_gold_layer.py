import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_handle = os.environ.get("AWS_HANDLE")
load_location = f"s3a://hwe-fall-2025/{aws_handle}/silver/reviews"


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week6Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,io.delta:delta-core_2.12:1.0.1') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

#Define a Schema which describes the Parquet files under the silver reviews directory on S3
silver_schema = StructType([
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
    StructField('review_timestamp', TimestampType(), False,),
    StructField('customer_name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('date_of_birth', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True)
])

#Define a streaming dataframe using readStream on top of the silver reviews directory on S3
silver_data = spark \
                .readStream \
                .format("parquet") \
                .schema(silver_schema) \
                .load(load_location)

int_cols = ['star_rating', 'helpful_votes', 'total_votes']
for column_name in int_cols:
    silver_data = silver_data.withColumn(column_name, col(column_name).cast(IntegerType()))
# silver_data.printSchema()

#Define a watermarked_data dataframe by defining a watermark on the `review_timestamp` column with an interval of 10 seconds
watermarked_data = silver_data.withWatermark("review_timestamp", "10 seconds")

#Define an aggregated dataframe using `groupBy` functionality to summarize that data over any dimensions you may find interesting
# aggregated_data = watermarked_data.groupBy("product_title", "review_timestamp").agg(
#     avg("star_rating").alias("avg_star_rating")
# )

aggregated_data = watermarked_data.groupBy("product_id", "review_timestamp").agg(
    count("*").alias("total_by_product")
)
aggregated_data.printSchema()


#Write that aggregate data to S3 under s3a://hwe-$CLASS/$HANDLE/gold/fact_review using append mode and a checkpoint location of `/tmp/gold-checkpoint`
save_location = f"s3a://hwe-fall-2025/{aws_handle}/gold/total_review"

write_gold_query = aggregated_data \
                    .writeStream \
                    .format("delta") \
                    .outputMode("append") \
                    .option("path", save_location) \
                    .option("checkpointLocation", "/tmp/gold-checkpoint")

write_gold_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()