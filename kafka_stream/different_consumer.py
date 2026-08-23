from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, unix_timestamp, avg, count, window
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
import time

# Schema definition
schema = StructType([
    StructField("Time", DoubleType(), True),
    *[StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)],
    StructField("Amount", DoubleType(), True),
    StructField("Class", DoubleType(), True)
])

input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Load the trained model
    model_path = "../ml_model/trained_model"  # Replace with your actual path
    model = RandomForestClassificationModel.load(model_path)
    print(f"Model loaded from: {model_path}")

    # Consumer for task-topic-1
    print("Starting consumer for task-topic-1")
    kafka_stream_1 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "task-topic-1") \
        .load()

    # Parse the Kafka stream
    parsed_stream_1 = kafka_stream_1.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Preprocess data
    assembler_1 = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_stream_1 = assembler_1.transform(parsed_stream_1)

    # Apply the trained model
    predictions_1 = model.transform(processed_stream_1)
    predictions_1 = predictions_1.withColumn("ProcessingTime", current_timestamp())
    predictions_1 = predictions_1.withColumn(
        "ResponseTime", unix_timestamp(col("ProcessingTime")) - col("Time")
    )

    # Calculate transactions per second for topic-1
    transactions_per_second_1 = predictions_1.groupBy(
        window(col("ProcessingTime"), "1 second")
    ).agg(
        count("*").alias("transactions_per_second")
    )

    # Write throughput to console for topic-1
    query_throughput_1 = transactions_per_second_1.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Select flagged fraud alerts from topic-1
    fraud_alerts_1 = predictions_1.filter(col("prediction") == 1)
    query_alerts_1 = fraud_alerts_1.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()

    # Consumer for task-topic-2
    print("Starting consumer for task-topic-2")
    kafka_stream_2 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "task-topic-2") \
        .load()

    # Parse the Kafka stream
    parsed_stream_2 = kafka_stream_2.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Preprocess data
    assembler_2 = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_stream_2 = assembler_2.transform(parsed_stream_2)

    # Apply the trained model
    predictions_2 = model.transform(processed_stream_2)
    predictions_2 = predictions_2.withColumn("ProcessingTime", current_timestamp())
    predictions_2 = predictions_2.withColumn(
        "ResponseTime", unix_timestamp(col("ProcessingTime")) - col("Time")
    )

    # Calculate transactions per second for topic-2
    transactions_per_second_2 = predictions_2.groupBy(
        window(col("ProcessingTime"), "1 second")
    ).agg(
        count("*").alias("transactions_per_second")
    )

    # Write throughput to console for topic-2
    query_throughput_2 = transactions_per_second_2.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # Select flagged fraud alerts from topic-2
    fraud_alerts_2 = predictions_2.filter(col("prediction") == 1)
    query_alerts_2 = fraud_alerts_2.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    print("Fraud detection streaming started...")
    print("Streaming metrics monitoring started...")

    query_alerts_1.awaitTermination()
    query_throughput_1.awaitTermination()
    query_alerts_2.awaitTermination()
    query_throughput_2.awaitTermination()

if __name__ == "__main__":
    main()
