from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

### Setup: Create a SparkSession
spark = SparkSession.builder \
    .appName("Week2 Lab") \
    .master("local[1]") \
    .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv('resources/reviews.tsv.gz', sep='\t', header=True)
# reviews.printSchema()
# reviews.show(n=2)

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView('reviews')
# test = spark.sql('SELECT marketplace, customer_id FROM reviews')
# print(test)

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer.
reviews = reviews.withColumn('review_timestamp', current_timestamp())
# reviews.printSchema()
# reviews.show(3)

# Question 4: How many records are in the reviews dataframe? 
total_reviews = reviews.count()
# print(total_reviews)
# 145431

# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
# reviews.show(n=5, truncate=False)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
product_categories = spark.sql('SELECT product_category FROM reviews')
# product_categories.show(50)
# +-------------------+
# |   product_category|
# +-------------------+
# |Digital_Video_Games|
product_categories = spark.sql('SELECT product_category, COUNT(*) FROM (SELECT * FROM reviews LIMIT 50) GROUP BY product_category')
# Digital_Video_Games
# +-------------------+--------+
# |   product_category|count(1)|
# +-------------------+--------+
# |Digital_Video_Games|      50|
# +-------------------+--------+

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
helpful_reviews = spark.sql('SELECT cast(helpful_votes as int), product_title FROM reviews ORDER BY helpful_votes DESC LIMIT 1')
# helpful_reviews.show(n=1, truncate=False)
# +-------------+-------------------------+
# |helpful_votes|product_title            |
# +-------------+-------------------------+
# |5068         |SimCity - Limited Edition|
# +-------------+-------------------------+

# Question 8: How many reviews exist in the dataframe with a 5 star rating?
five_star_reviews_count = spark.sql('SELECT COUNT(*) FROM reviews WHERE star_rating = 5')
# five_star_reviews_count.show(n=1)
# +--------+
# |count(1)|
# +--------+
# |   80677|
# +--------+
five_star_reviews = spark.sql('SELECT * FROM reviews WHERE star_rating = 5')
# print(five_star_reviews.count())
# 80677

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
# star_rating|helpful_votes|total_votes
review_numbers = spark.sql('SELECT cast(star_rating as int), cast(helpful_votes as int), cast(total_votes as int) FROM reviews')
# review_numbers.show(n=10)
columns_to_cast = [ "star_rating", "helpful_votes", "total_votes" ]
reviews_numbers = reviews.select(*[col(c).cast('int').alias(c) for c in columns_to_cast])
# reviews_numbers.show(n=10)


# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
most_purchases = spark.sql('SELECT purchase_date, COUNT (*) AS count FROM reviews GROUP BY purchase_date ORDER BY count DESC LIMIT 1')
# most_purchases.show(n=2)
# +-------------+-----+
# |purchase_date|count|
# +-------------+-----+
# |   2013-03-07|  760|
# +-------------+-----+

##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
# reviews.write.mode("overwrite").json('week2_sql/reveiws_with_timestamp_json')


### Teardown
# Stop the SparkSession
spark.stop()