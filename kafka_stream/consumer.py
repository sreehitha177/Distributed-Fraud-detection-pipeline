# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, unix_timestamp, avg, count,current_timestamp,min,from_unixtime
# from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType
# from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.classification import RandomForestClassificationModel
# import csv
# from pathlib import Path
#
# BASE_DIR = Path(__file__).resolve().parent
# PROJECT_ROOT = BASE_DIR.parent
#
# MODEL_PATH = PROJECT_ROOT / "ml_model" / "trained_model_v3"
#
# metrics_file ='streaming_metrics.csv'
#
# def main():
#     spark = SparkSession.builder \
#         .appName("FraudDetectionStreaming") \
#         .getOrCreate()
#
#     spark.sparkContext.setLogLevel("WARN")
#
#     # Define the schema for incoming Kafka data
#     schema = StructType([
#         StructField("Time", DoubleType(), False),
#         *[
#             StructField(f"V{i}", DoubleType(), False)
#             for i in range(1, 29)
#         ],
#         StructField("Amount", DoubleType(), False),
#         StructField("Class", DoubleType(), True),
#         StructField("EventTimestamp", DoubleType(), False),
#         StructField("IngestionRate", DoubleType(), True)
#     ])
#
#
#     model = RandomForestClassificationModel.load(
#         str(MODEL_PATH)
#     )
#     print(f"Model loaded from: {MODEL_PATH}")
#
#     kafka_stream = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "task-topic") \
#         .load()
#
#     parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col("value"), schema).alias("data")) \
#         .select("data.*")
#
#     FEATURE_COLUMNS = (
#             [f"V{i}" for i in range(1, 29)]
#             + ["Time", "Amount"]
#     )
#     assembler = VectorAssembler(
#         inputCols=FEATURE_COLUMNS,
#         outputCol="features"
#     )
#     feature_data = assembler.transform(parsed_stream)
#
#     predictions = model.transform(feature_data)
#     predictions = predictions.withColumn("Time", from_unixtime(col("Time")).cast(TimestampType()))
#     predictions = predictions.withColumn("ProcessingTime", current_timestamp())
#     predictions = predictions.withColumn(
#         "ResponseTime", unix_timestamp("ProcessingTime") - unix_timestamp("Time")
#     )
#
#     # Group by IngestionRate and calculate metrics
#     #
#     metrics = predictions.groupBy("IngestionRate").agg(
#         min(unix_timestamp("Time")).alias("first_transaction_time"),  # Corrected
#         avg("ResponseTime").alias("avg_response_time"),  # Corrected
#         count(col("*")).alias("total_transactions")  # Corrected
#     )
#
#
#     # Calculate throughput as transactions per second
#     throughput = metrics.select(
#         col("IngestionRate"),
#         col("avg_response_time"),
#         (col("total_transactions") / (unix_timestamp(current_timestamp()) - col("first_transaction_time"))).alias("throughput")
#     )
#
#     query = throughput.writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start()
#
#     query.awaitTermination()
#
# if __name__ == "__main__":
#     main()


from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType
)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.functions import vector_to_array
import time
from pyspark.sql import functions as F
import argparse

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

MODEL_PATH = (
        PROJECT_ROOT
        / "ml_model"
        / "trained_model_v3"
)

# KAFKA_TOPIC = "task-topic"
parser = argparse.ArgumentParser()
parser.add_argument("--topic", default="task-topic")
args = parser.parse_args()

KAFKA_TOPIC = args.topic

FEATURE_COLUMNS = (
        [f"V{i}" for i in range(1, 29)]
        + ["Time", "Amount"]
)

THRESHOLD = 0.30


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    start = time.perf_counter()

    metrics = (
        batch_df
        .agg(
            F.count("*").alias("transaction_count"),

            F.first("IngestionRate").alias("requested_rate"),

            F.avg("LatencySeconds").alias("avg_latency"),

            F.min("LatencySeconds").alias("min_latency"),

            F.max("LatencySeconds").alias("max_latency"),

            F.expr(
                """
                percentile_approx(
                    LatencySeconds,
                    array(0.5, 0.95, 0.99),
                    10000
                )
                """
            ).alias("latency_percentiles"),

            F.sum("final_prediction").alias("fraud_alerts"),

            F.min("EventTimestamp").alias("first_event"),

            F.max("EventTimestamp").alias("last_event")
        )
        .collect()[0]
    )

    processing_time = time.perf_counter() - start

    count = metrics["transaction_count"]

    consumer_throughput = (
        count / processing_time
        if processing_time > 0
        else 0
    )

    event_span = (
            metrics["last_event"]
            - metrics["first_event"]
    )

    # For N events there are N-1 intervals.
    observed_arrival_rate = (
        (count - 1) / event_span
        if count > 1 and event_span > 0
        else 0
    )

    p50, p95, p99 = metrics["latency_percentiles"]

    print("\n" + "=" * 55)
    print(f"BATCH {batch_id}")
    print("=" * 55)

    print(f"Transactions:              {count}")
    print(
        f"Requested producer rate:   "
        f"{metrics['requested_rate']:.2f} tx/s"
    )
    print(
        f"Observed arrival rate:     "
        f"{observed_arrival_rate:.2f} tx/s"
    )

    print(
        f"Consumer processing time:  "
        f"{processing_time:.3f} s"
    )
    print(
        f"Consumer throughput:       "
        f"{consumer_throughput:.2f} tx/s"
    )

    print("\nLatency")
    print(f"Average:                    {metrics['avg_latency']:.3f} s")
    print(f"Minimum:                    {metrics['min_latency']:.3f} s")
    print(f"Maximum:                    {metrics['max_latency']:.3f} s")
    print(f"P50:                        {p50:.3f} s")
    print(f"P95:                        {p95:.3f} s")
    print(f"P99:                        {p99:.3f} s")

    print(
        f"\nFraud alerts:               "
        f"{int(metrics['fraud_alerts'] or 0)}"
    )

def main():

    spark = (
        SparkSession.builder
        .appName("FraudDetectionStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Spark master:", spark.sparkContext.master)
    print("Spark default parallelism:", spark.sparkContext.defaultParallelism)
    print("Kafka topic:", KAFKA_TOPIC)

    schema = StructType([
        StructField("Time", DoubleType(), False),

        *[StructField(
                f"V{i}",
                DoubleType(),
                False
            )
            for i in range(1, 29)],

        StructField("Amount", DoubleType(), False ),
        StructField("Class", DoubleType(), True),
        StructField("EventTimestamp", DoubleType(), False ),
        StructField("IngestionRate", DoubleType(), True ),
    ])

    model = RandomForestClassificationModel.load(
        str(MODEL_PATH)
    )

    print(
        f"Model loaded from: {MODEL_PATH}"
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .selectExpr(
            "CAST(value AS STRING) AS value"
        )
        .select(
            from_json(
                col("value"),
                schema
            ).alias("data")
        )
        .select("data.*")
    )

    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features"
    )

    feature_data = assembler.transform(
        parsed_stream
    )

    predictions = model.transform(
        feature_data
    )

    predictions = predictions.withColumn(
        "fraud_probability",
        vector_to_array("probability")[1]
    )

    predictions = predictions.withColumn(
        "final_prediction",
        when(
            col("fraud_probability")
            >= THRESHOLD,
            1.0
        ).otherwise(0.0)
    )

    predictions = predictions.withColumn(
        "ProcessingTimestamp",
        current_timestamp()
    )

    predictions = predictions.withColumn(
        "LatencySeconds",
        col("ProcessingTimestamp").cast("double")
        - col("EventTimestamp")
    )

    output = predictions.select(
        "Time",
        "Amount",
        "Class",
        "fraud_probability",
        "final_prediction",
        "EventTimestamp",
        "ProcessingTimestamp",
        "LatencySeconds"
    )

    # query = (
    #     output.writeStream
    #     .format("console")
    #     .outputMode("append")
    #     .option(
    #         "truncate",
    #         False
    #     )
    #     .start()
    # )
    query = (
        predictions.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .start()
    )

    print(
        "Fraud detection consumer started..."
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()