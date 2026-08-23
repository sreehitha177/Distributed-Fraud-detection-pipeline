import argparse
import json
import random
import time
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import SparkSession


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

TEST_DATA_PATH = (
        PROJECT_ROOT
        / "ml_model"
        / "testdata_v3.parquet"
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

FEATURE_COLUMNS = (
        [f"V{i}" for i in range(1, 29)]
        + ["Time", "Amount"]
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rate",
        type=float,
        default=20,
        help="Requested transactions per second"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of transactions to send"
    )

    parser.add_argument(
        "--topic",
        type=str,
        default="task-topic"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42
    )

    return parser.parse_args()


def load_transactions(spark):
    """
    Load test transactions once into Python memory.

    No Spark operations occur inside the Kafka send loop.
    """

    print(f"Loading test data from: {TEST_DATA_PATH}")

    test_data = (
        spark.read
        .parquet(str(TEST_DATA_PATH))
        .select(
            *FEATURE_COLUMNS,
            "Class"
        )
    )

    transactions = [
        row.asDict(recursive=True)
        for row in test_data.collect()
    ]

    if not transactions:
        raise ValueError("Test dataset is empty.")

    print(
        f"Loaded {len(transactions)} transactions into memory."
    )

    return transactions


def create_transaction(row, ingestion_rate):
    transaction = dict(row)

    # Keep the original ML Time feature unchanged.

    # Real wall-clock timestamp for latency measurement.
    transaction["EventTimestamp"] = time.time()

    transaction["IngestionRate"] = float(
        ingestion_rate
    )

    return transaction


def main():
    args = parse_args()

    if args.rate <= 0:
        raise ValueError("--rate must be greater than 0")

    if args.count <= 0:
        raise ValueError("--count must be greater than 0")

    spark = (
        SparkSession.builder
        .appName("FraudKafkaProducer")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    producer = None

    try:
        transactions = load_transactions(spark)

        spark.stop()
        spark = None

        rng = random.Random(args.seed)

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),
            acks="all"
        )

        print(
            f"\nSending {args.count} transactions "
            f"at {args.rate:.2f} tx/s"
        )

        print(f"Kafka topic: {args.topic}")

        start_time = time.perf_counter()

        for i in range(args.count):

            # Pick an existing transaction in Python.
            row = rng.choice(transactions)

            # Try to maintain the requested rate without
            # accumulating sleep error.
            target_time = (
                    start_time
                    + i / args.rate
            )

            sleep_time = (
                    target_time
                    - time.perf_counter()
            )

            if sleep_time > 0:
                time.sleep(sleep_time)

            transaction = create_transaction(
                row,
                args.rate
            )

            producer.send(
                args.topic,
                value=transaction
            )

        # Wait until all queued messages are acknowledged.
        producer.flush()

        end_time = time.perf_counter()

        elapsed = end_time - start_time

        actual_rate = (
            args.count / elapsed
            if elapsed > 0
            else 0
        )

        print("\nProducer run complete")
        print("---------------------")
        print(f"Requested rate: {args.rate:.2f} tx/s")
        print(f"Transactions:   {args.count}")
        print(f"Elapsed time:   {elapsed:.3f} s")
        print(f"Actual rate:    {actual_rate:.2f} tx/s")

    finally:
        if producer is not None:
            producer.close()

        if spark is not None:
            spark.stop()



if __name__ == "__main__":
    main()