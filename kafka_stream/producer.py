"""Replay a completed training run's test transactions into Kafka."""

import argparse
from collections import deque
import json
import math
from pathlib import Path
import random
import sys
import time
import uuid

from kafka import KafkaProducer
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if not __package__:
    sys.path.insert(0, str(PROJECT_ROOT))

from ml_model.model_utils import load_run_manifest
from ml_model.train import validate_data


ACK_TIMEOUT_SECONDS = 30
MAX_PENDING_SENDS = 1000


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True,
                        help="Completed training run containing manifest.json and test.parquet")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Comma-separated Kafka broker addresses")
    parser.add_argument("--topic", default="fraud-transactions")
    parser.add_argument("--master", default="local[2]", help="Spark master used only to load the test data")
    parser.add_argument("--rate", type=float, default=20.0, help="Requested transactions per second")
    parser.add_argument("--count", type=int, default=50, help="Number of transactions to send")
    parser.add_argument("--seed", type=int, default=42, help="Seed for random sampling with replacement")
    parser.add_argument("--sequential", action="store_true",
                        help="Send the first count rows in the loaded order, without replacement")
    args = parser.parse_args(argv)
    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be finite and greater than 0")
    if args.count <= 0:
        parser.error("--count must be greater than 0")
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    args.bootstrap_servers = [address.strip() for address in args.bootstrap_servers.split(",")]
    if not all(args.bootstrap_servers):
        parser.error("--bootstrap-servers must contain nonempty broker addresses")
    return args


def load_transactions(spark, data_path, feature_columns):
    """Validate and collect the saved test split once, before connecting to Kafka."""
    if (not isinstance(feature_columns, list) or not feature_columns
            or any(not isinstance(name, str) or not name for name in feature_columns)
            or len(feature_columns) != len(set(feature_columns))
            or "Class" in feature_columns):
        raise ValueError("The run manifest must contain distinct feature column names excluding Class")
    print(f"Loading test data from: {data_path}", flush=True)
    test_data = spark.read.parquet(str(data_path)).select(*feature_columns, "Class")
    validated = validate_data(test_data, dataset_name="Producer test data")
    # Emit JSON floating-point numbers for the consumer's DoubleType fields,
    # including Time/Class, which can be integer columns in saved Parquet.
    transactions = [
        {name: float(row[name]) for name in feature_columns + ["Class"]}
        for row in validated.collect()
    ]
    if not transactions:
        raise ValueError("Test dataset is empty")
    print(f"Loaded {len(transactions)} transactions into memory.", flush=True)
    return transactions


def create_transaction(row, ingestion_rate, producer_run_id, event_index):
    """Create one event; its identity remains unchanged during Kafka retries."""
    transaction = {name: float(value) for name, value in row.items()}
    transaction["EventId"] = f"{producer_run_id}:{event_index}"
    transaction["ProducerRunId"] = producer_run_id
    # Keep the original ML Time feature and use a separate wall-clock timestamp.
    transaction["EventTimestamp"] = time.time()
    transaction["IngestionRate"] = float(ingestion_rate)
    return transaction


def validate_send_request(transactions, rate, count, sequential=False):
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("Rate must be finite and greater than 0")
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        raise ValueError("Count must be a positive integer")
    if not transactions:
        raise ValueError("No transactions are available to send")
    if sequential and count > len(transactions):
        raise ValueError("--count cannot exceed the dataset row count with --sequential")


def send_transactions(producer, transactions, *, topic, rate, count, seed=42,
                      sequential=False, producer_run_id=None):
    """Rate-limit sends and verify every acknowledgement with bounded pending state."""
    validate_send_request(transactions, rate, count, sequential)
    producer_run_id = producer_run_id or uuid.uuid4().hex
    rng = random.Random(seed)
    pending = deque()
    acknowledged = 0
    started = time.perf_counter()
    print(f"Producer run ID: {producer_run_id}", flush=True)
    print(f"Sending {count} transactions to {topic} at {rate:.2f} tx/s", flush=True)

    for index in range(count):
        row = transactions[index] if sequential else rng.choice(transactions)
        sleep_seconds = started + index / rate - time.perf_counter()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        transaction = create_transaction(row, rate, producer_run_id, index)
        pending.append(producer.send(
            topic, key=transaction["EventId"].encode("utf-8"), value=transaction
        ))
        # A failed delivery raises here instead of being hidden by flush().
        if len(pending) >= MAX_PENDING_SENDS:
            pending.popleft().get(timeout=ACK_TIMEOUT_SECONDS)
            acknowledged += 1

    while pending:
        pending.popleft().get(timeout=ACK_TIMEOUT_SECONDS)
        acknowledged += 1
    producer.flush(timeout=ACK_TIMEOUT_SECONDS)
    elapsed = time.perf_counter() - started
    result = {
        "producer_run_id": producer_run_id,
        "acknowledged": acknowledged,
        "elapsed_seconds": elapsed,
        "actual_rate": acknowledged / elapsed if elapsed > 0 else 0.0,
    }
    print("\nProducer run complete", flush=True)
    print(f"Acknowledged: {acknowledged}/{count}; elapsed: {elapsed:.3f}s; "
          f"actual rate: {result['actual_rate']:.2f} tx/s", flush=True)
    return result


def main():
    args = parse_args()
    manifest = load_run_manifest(args.run_dir)
    feature_columns = manifest.get("feature_columns")
    data_path = args.run_dir / "test.parquet"
    spark = (SparkSession.builder.appName("FraudKafkaProducer")
             .master(args.master).getOrCreate())
    try:
        spark.sparkContext.setLogLevel("WARN")
        transactions = load_transactions(spark, data_path, feature_columns)
    finally:
        spark.stop()

    validate_send_request(transactions, args.rate, args.count, args.sequential)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, allow_nan=False).encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )
    try:
        send_transactions(
            producer, transactions, topic=args.topic, rate=args.rate, count=args.count,
            seed=args.seed, sequential=args.sequential,
        )
    finally:
        producer.close(timeout=ACK_TIMEOUT_SECONDS)


if __name__ == "__main__":
    main()
