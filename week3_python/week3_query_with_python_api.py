import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import current_timestamp

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_handle = os.environ.get("AWS_HANDLE")

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week3Lab") \
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

### Questions

# Remember, this week we are using the Spark DataFrame API (and last week was the Spark SQL API).

print('Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe.')
#You will use the "reviews" dataframe defined here to answer all the questions below...
reviews = spark.read.csv('resources/reviews.tsv.gz', sep='\t', header=True)

print('\nQuestion 2: Display the schema of the dataframe.')
reviews.printSchema()

print('\nQuestion 3: How many records are in the dataframe?')
#Store this number in a variable named "reviews_count".
reviews_count = reviews.count()
print(reviews_count)

print('\nQuestion 4: Print the first 5 rows of the dataframe.')
#Some of the columns are long - print the entire record, regardless of length.
reviews.show(n=5, truncate=False)

print('\nQuestion 5: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.')
#Look at the first 50 rows of that dataframe. 
#Which value appears to be the most common?
product_categories = reviews.select(reviews['product_category'])
product_categories.show(n=50, truncate=False)
# +-------------------+
# |product_category   |
# +-------------------+
# |Digital_Video_Games|
# +-------------------+
print('---OR---')
product_categories = reviews.select(reviews['product_category']).limit(50).groupBy(reviews['product_category']).count()
product_categories.show()
# +-------------------+-----+
# |   product_category|count|
# +-------------------+-----+
# |Digital_Video_Games|   50|
# +-------------------+-----+


print('\nQuestion 6: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.')
#What is the product title for that review? How many helpful votes did it have?
most_helpful = reviews.select(reviews['product_title'], col('helpful_votes').cast('int')).orderBy(col('helpful_votes').desc()).limit(1)
most_helpful.printSchema()
most_helpful.show(truncate=False)
# root
#  |-- product_title: string (nullable = true)
#  |-- helpful_votes: integer (nullable = true)

# +-------------------------+-------------+
# |product_title            |helpful_votes|
# +-------------------------+-------------+
# |SimCity - Limited Edition|5068         |
# +-------------------------+-------------+

print('\nQuestion 7: How many reviews have a 5 star rating?')
print(reviews.filter(reviews['star_rating'] == '5').count())
# 80677

print('''\nQuestion 8: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
Create a new dataframe with just those 3 columns, except cast them as "int"s.''')
#Look at 10 rows from this dataframe.
reviews_numbers = reviews.select(
    reviews['star_rating'].cast('int'), 
    reviews['helpful_votes'].cast('int'), 
    reviews['total_votes'].cast('int'))
reviews_numbers.printSchema()
reviews_numbers.show(10)
# root
#  |-- star_rating: integer (nullable = true)
#  |-- helpful_votes: integer (nullable = true)
#  |-- total_votes: integer (nullable = true)

# +-----------+-------------+-----------+
# |star_rating|helpful_votes|total_votes|
# +-----------+-------------+-----------+
# |          2|            2|          3|
# |          5|            0|          0|
# |          5|            0|          0|
# |          5|            0|          0|
# |          5|            0|          0|
# |          5|            0|          0|
# |          4|            0|          0|
# |          5|            0|          0|
# |          5|            0|          0|
# |          5|            0|          0|
# +-----------+-------------+-----------+
# only showing top 10 rows

print('\nQuestion 8: Find the date with the most purchases.')
#Print the date and total count of the date with the most purchases
most_orders_by_date = reviews.groupBy(reviews['purchase_date']).count().orderBy(col('count').desc()).limit(1)
most_orders_by_date.show()
# +-------------+-----+
# |purchase_date|count|
# +-------------+-----+
# |   2013-03-07|  760|
# +-------------+-----+

print('\nQuestion 9: Add a column to the dataframe named "review_timestamp", representing the current time on your computer.') 
#Hint: Check the documentation for a function that can help: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
#Print the schema and inspect a few rows of data to make sure the data is correctly populated.
reveiws_with_timestamp = reviews.withColumn('review_timestamp', current_timestamp())
reveiws_with_timestamp.printSchema()
reveiws_with_timestamp.show(3)
# root
#  |-- marketplace: string (nullable = true)
#  |-- customer_id: string (nullable = true)
#  |-- review_id: string (nullable = true)
#  |-- product_id: string (nullable = true)
#  |-- product_parent: string (nullable = true)
#  |-- product_title: string (nullable = true)
#  |-- product_category: string (nullable = true)
#  |-- star_rating: string (nullable = true)
#  |-- helpful_votes: string (nullable = true)
#  |-- total_votes: string (nullable = true)
#  |-- vine: string (nullable = true)
#  |-- verified_purchase: string (nullable = true)
#  |-- review_headline: string (nullable = true)
#  |-- review_body: string (nullable = true)
#  |-- purchase_date: string (nullable = true)
#  |-- review_timestamp: timestamp (nullable = false)

# +-----------+-----------+--------------+----------+--------------+--------------------+-------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-------------+--------------------+
# |marketplace|customer_id|     review_id|product_id|product_parent|       product_title|   product_category|star_rating|helpful_votes|total_votes|vine|verified_purchase|     review_headline|         review_body|purchase_date|    review_timestamp|
# +-----------+-----------+--------------+----------+--------------+--------------------+-------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-------------+--------------------+
# |         US|   21269168| RSH1OZ87OYK92|B013PURRZW|     603406193|Madden NFL 16 - X...|Digital_Video_Games|          2|            2|          3|   N|                N|A slight improvem...|I keep buying mad...|   2015-08-31|2025-10-03 22:53:...|
# |         US|     133437|R1WFOQ3N9BO65I|B00F4CEHNK|     341969535| Xbox Live Gift Card|Digital_Video_Games|          5|            0|          0|   N|                Y|          Five Stars|             Awesome|   2015-08-31|2025-10-03 22:53:...|
# |         US|   45765011| R3YOOS71KM5M9|B00DNHLFQA|     951665344|Command & Conquer...|Digital_Video_Games|          5|            0|          0|   N|                Y|Hail to the great...|If you are preppi...|   2015-08-31|2025-10-03 22:53:...|
# +-----------+-----------+--------------+----------+--------------+--------------------+-------------------+-----------+-------------+-----------+----+-----------------+--------------------+--------------------+-------------+--------------------+

print('\nQuestion 10: Write the dataframe with load timestamp to s3a://hwe-$CLASS/$HANDLE/bronze/reviews_static in Parquet format.')
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
save_location = f"s3a://hwe-fall-2025/{aws_handle}/bronze/reviews_static"
reveiws_with_timestamp.write.mode('overwrite').parquet(save_location)

print('''\nQuestion 11: Read the tab separated file named "resources/customers.tsv.gz" into a dataframe
Write to S3 under s3a://hwe-$CLASS/$HANDLE/bronze/customers''')
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
#There are no questions to answer about this data set right now, but you will use it in a later lab...
customers = spark.read.csv('resources/customers.tsv.gz', sep='\t', header=True)
save_location = f"s3a://hwe-fall-2025/{aws_handle}/bronze/customers"
customers.write.mode('overwrite').parquet(save_location)

print('\nQuestion 12: Display the schema of the customers dataframe.')
#This will be useful when you use it in a later lab...
customers.printSchema()

# Stop the SparkSession
spark.stop()
