""" spark_stream.py
Reads Reddit JSON files and computes analytics:
- Top subreddits
- Top authors
- Frequent words
- Most commented posts
- Active posting hours
Writes results to ./analytics/
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, count, avg, hour
from pyspark.sql.types import StructType, StringType, FloatType, LongType
import pyspark.sql.functions as F

INPUT_DIR = "./reddit_data"
OUTPUT_DIR = "./analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("RedditStreamAnalytics") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("subreddit", StringType()) \
    .add("title", StringType()) \
    .add("author", StringType()) \
    .add("score", FloatType()) \
    .add("created_utc", FloatType()) \
    .add("url", StringType()) \
    .add("num_comments", LongType()) \
    .add("upvote_ratio", FloatType()) \
    .add("domain", StringType()) \
    .add("link_flair_text", StringType())

raw = spark.readStream.format("json").schema(schema).load(INPUT_DIR)
parsed = raw.withColumn("timestamp", F.from_unixtime("created_utc").cast("timestamp"))

# Top subreddits by post count & avg score
top_subreddits = parsed.groupBy("subreddit").agg(
    count("*").alias("post_count"),
    F.round(avg("score"), 2).alias("avg_score")
)

#  Top authors
top_authors = parsed.groupBy("author").agg(count("*").alias("author_count"))

# Word frequency
clean = parsed.withColumn("clean_title", regexp_replace(F.lower(col("title")), r"[^a-z\s]", ""))
words = clean.withColumn("word", explode(split(col("clean_title"), r"\s+"))).filter(col("word") != "")
word_counts = words.groupBy("word").agg(count("*").alias("word_count"))


# Most commented posts (no sorting in stream)
most_commented = parsed.select("title", "subreddit", "num_comments", "url")

# ...
most_commented.writeStream.outputMode("append").foreachBatch(
    lambda df, eid: write_batch(df, eid, "most_commented.json", 20, "num_comments")
).start()


# Active posting hours
active_hours = parsed.withColumn("hour", hour(col("timestamp"))).groupBy("hour").agg(count("*").alias("posts"))

def write_batch(df, epoch_id, filename, topn=20, sort_col=None):
    if df.rdd.isEmpty():
        return
    pdf = df.toPandas()
    if sort_col:
        pdf = pdf.sort_values(sort_col, ascending=False).head(topn)
    pdf.to_json(os.path.join(OUTPUT_DIR, filename), orient="records", force_ascii=False)

queries = [
    top_subreddits.writeStream.outputMode("complete").foreachBatch(lambda df, eid: write_batch(df, eid, "top_subreddits.json", 20, "post_count")).start(),
    top_authors.writeStream.outputMode("complete").foreachBatch(lambda df, eid: write_batch(df, eid, "top_authors.json", 20, "author_count")).start(),
    word_counts.writeStream.outputMode("complete").foreachBatch(lambda df, eid: write_batch(df, eid, "top_words.json", 30, "word_count")).start(),
    most_commented.writeStream.outputMode("append").foreachBatch(lambda df, eid: write_batch(df, eid, "most_commented.json", 20, "num_comments")).start(),
    active_hours.writeStream.outputMode("complete").foreachBatch(lambda df, eid: write_batch(df, eid, "active_hours.json", 24, "posts")).start()
]

print("[spark_stream] Streaming started — writing to ./analytics/")
spark.streams.awaitAnyTermination()
