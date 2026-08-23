from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, unix_timestamp, avg, count,window
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
import time
import threading

# Schema definition
schema = StructType([
    StructField("Time", DoubleType(), True),
    *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
    StructField("Amount", DoubleType(), True),
    StructField("Class", DoubleType(), True)
])

input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]


def process_stream(spark, topic):
    import os
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    model_path = os.path.join(base_dir, "ml_model", "trained_model")
    model = RandomForestClassificationModel.load(model_path)
    print(f"Model loaded for topic {topic}")

    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .load()

    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_stream = assembler.transform(parsed_stream)

    predictions = model.transform(processed_stream)
    predictions = predictions.withColumn("ProcessingTime", current_timestamp())
    predictions = predictions.withColumn(
        "ResponseTime", unix_timestamp(col("ProcessingTime")) - col("Time")
    )

    fraud_alerts = predictions.filter(col("prediction") == 1)
    fraud_alerts.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    print(f"Fraud detection for topic {topic} started.")
    predictions.writeStream \
        .outputMode("append") \
        .format("console") \
        .start() \
        .awaitTermination()
    

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    topics = ["task-topic-1", "task-topic-1"]
    threads = [threading.Thread(target=process_stream, args=(spark, topic)) for topic in topics]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # # Load the trained model
    # model_path = "../ml_model/trained_model"  # Replace with your actual path
    # model = RandomForestClassificationModel.load(model_path)
    # print(f"Model loaded from: {model_path}")

    # # Read from Kafka
    # kafka_stream = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", "task-topic") \
    #     .load()

    # # Parse the Kafka stream
    # parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    #     .select(from_json(col("value"), schema).alias("data")) \
    #     .select("data.*")

    # # Preprocess data
    # assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    # processed_stream = assembler.transform(parsed_stream)

    # # Apply the trained model
    # predictions = model.transform(processed_stream)
    # predictions = predictions.withColumn("ProcessingTime", current_timestamp())
    # predictions = predictions.withColumn(
    #     "ResponseTime", unix_timestamp(col("ProcessingTime")) - col("Time")
    # )

    # # Calculate transactions per second
    #  # Calculate throughput
    # transactions_per_second = predictions.groupBy(
    #     window(col("ProcessingTime"), "1 second")
    # ).agg(
    #     count("*").alias("transactions_per_second")
    # )

    # # Calculate response time metrics
    # metrics = predictions.groupBy().agg(
    #     count("*").alias("total_transactions"),
    #     avg("ResponseTime").alias("avg_response_time")
    # )

    # # Write throughput to console
    # query_throughput = transactions_per_second.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()

    # # Write response time metrics to console
    # query_metrics = metrics.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .start()

    
    
    # # query_metrics.awaitTermination()


    # # Select flagged fraud alerts
    # fraud_alerts = predictions.filter(col("prediction") == 1)
    # query_alerts = fraud_alerts.selectExpr("to_json(struct(*)) AS value") \
    #     .writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start()

    # print("Fraud detection streaming started...")
    # print("Streaming metrics monitoring started...")
    
    # query_alerts.awaitTermination()
    # query_throughput.awaitTermination()
    # query_metrics.awaitTermination()

if __name__ == "__main__":
    main()
