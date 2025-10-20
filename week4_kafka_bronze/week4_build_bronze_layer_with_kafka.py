import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_handle = os.environ.get("AWS_HANDLE")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load() \
    .select(col('value').cast('string').alias('review')) \
    .select(split('review', '\t').alias("review")) \
    .selectExpr(
      "review[0] as marketplace",
      "review[1] as customer_id",
      "review[2] as review_id",
      "review[3] as product_id",
      "review[4] as product_parent",
      "review[5] as product_title",
      "review[6] as product_category",
      "review[7] as star_rating",
      "review[8] as helpful_votes",
      "review[9] as total_votes",
      "review[10] as vine",
      "review[11] as verified_purchase",
      "review[12] as review_headline",
      "review[13] as review_body",
      "review[14] as purchase_date"
    ) \
    .withColumn("review_timestamp", current_timestamp())

# columns = ["marketplace", "customer_id"]
# df = df.select([col('review').getItem(i).alias(columns[i]) for i in range(0, 14)])

# Process the received data
df.printSchema()

save_location = f"s3a://hwe-fall-2025/{aws_handle}/bronze/reviews"

query = df \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", save_location) \
  .option("checkpointLocation", "/tmp/kafka-checkpoint") \
  .start()

# Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession
spark.stop()