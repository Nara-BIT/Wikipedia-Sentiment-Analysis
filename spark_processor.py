from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("WikiSentimentAnalysis") \
    .getOrCreate()

# Hide excessive logs
spark.sparkContext.setLogLevel("WARN")

# 2. Define the JSON schema we expect from Kafka
schema = StructType([
    StructField("user", StringType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("wiki", StringType(), True)
])

# 3. Define the NLP Sentiment Function
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    if not text:
        return 0.0
    # VADER returns a 'compound' score between -1 (extreme negative) and 1 (extreme positive)
    return analyzer.polarity_scores(text)['compound']

def get_sentiment_label(score):
    if score >= 0.05:
        return "Positive"
    elif score <= -0.05:
        return "Negative"
    else:
        return "Neutral"

# Register functions as Spark UDFs
sentiment_score_udf = udf(get_sentiment_score, FloatType())
sentiment_label_udf = udf(get_sentiment_label, StringType())

# 4. Read the live stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "wiki_edits") \
    .option("startingOffsets", "latest") \
    .load()

# 5. Parse JSON and apply NLP Sentiment Analysis
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = parsed_df \
    .withColumn("sentiment_score", sentiment_score_udf(col("comment"))) \
    .withColumn("sentiment_label", sentiment_label_udf(col("sentiment_score")))

# 6. Write the stream directly to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db:5432/voting_db") \
        .option("dbtable", "wiki_sentiment") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"📊 Processed Batch {batch_id}: Analyzed {batch_df.count()} Wikipedia edits.")

query = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

print("🧠 Spark NLP Processor is running... analyzing sentiments!")
query.awaitTermination()