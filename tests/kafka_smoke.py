"""Check real Kafka scoring and checkpoint recovery with 15 small fixture events.

Run with spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4
and an existing empty test topic. This checks transport correctness, not model F1.
"""

import argparse
import json
import sys
import uuid
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka_stream.consumer import parse_events, prepare_output, score_events
from kafka_stream.producer import ACK_TIMEOUT_SECONDS, create_transaction, send_transactions
from ml_model.model_utils import get_classifier, load_fraud_model, load_run_manifest, transform_model, validate_run_model
from ml_model.train import FEATURE_COLUMNS, write_json
from ml_model.tune_threshold import load_selected_threshold


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", required=True, help="An existing empty topic reserved for this test")
    parser.add_argument("--output-dir", type=Path, required=True, help="A new or empty output directory")
    args = parser.parse_args()
    args.run_dir, args.output_dir = args.run_dir.resolve(), args.output_dir.resolve()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        parser.error("--output-dir must be empty; use a new directory for each smoke test")
    return args


def require_empty_topic(args):
    reader = KafkaConsumer(bootstrap_servers=args.bootstrap_servers, enable_auto_commit=False,
                           request_timeout_ms=10000, allow_auto_create_topics=False)
    try:
        partitions = reader.partitions_for_topic(args.topic)
        if not partitions:
            raise ValueError(f"Create the test topic first: {args.topic}")
        offsets = reader.end_offsets([TopicPartition(args.topic, number) for number in partitions])
        if any(offsets.values()):
            raise ValueError("The test topic must be empty and have no previous offsets")
    finally:
        reader.close()


def main():
    args = parse_args()
    manifest = load_run_manifest(args.run_dir)
    require_empty_topic(args)
    spark = (SparkSession.builder.master("local[2]").appName("FraudKafkaSmokeTest")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    sender = None
    try:
        model = load_fraud_model(args.run_dir / "model")
        features = validate_run_model(manifest, model, FEATURE_COLUMNS)
        classifier = get_classifier(model)
        threshold = load_selected_threshold(args.run_dir / "threshold.json", classifier.uid, features)
        test = spark.read.parquet(str(args.run_dir / "test.parquet"))
        selected = test.where(F.col("Class") == 1).limit(6).unionByName(test.where(F.col("Class") == 0).limit(6))
        transactions = [{name: float(row[name]) for name in features + ["Class"]} for row in selected.collect()]
        assert len(transactions) == 12 and sum(row["Class"] for row in transactions) == 6, "Need six rows of each class"
        run_id = "smoke-" + uuid.uuid4().hex
        offline_input = spark.createDataFrame([
            dict(row, EventId=f"{run_id}:{index}") for index, row in enumerate(transactions)
        ])
        offline = transform_model(model, offline_input, features).select(
            "EventId", vector_to_array(classifier.getProbabilityCol())[1].alias("score"))
        expected = {row["EventId"]: row["score"] for row in offline.collect()}
        configuration = dict(schema_version=1, bootstrap_servers=args.bootstrap_servers, topic=args.topic,
                             run_dir=str(args.run_dir), model_uid=classifier.uid, feature_columns=features,
                             threshold=threshold, output_dir=str(args.output_dir))
        prepare_output(args.output_dir, configuration)
        sender = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers, acks="all", retries=3,
            max_in_flight_requests_per_connection=1,
            value_serializer=lambda value: value if isinstance(value, bytes) else json.dumps(value, allow_nan=False).encode("utf-8"),
        )
        result = send_transactions(sender, transactions, topic=args.topic, rate=20, count=12,
                                   sequential=True, producer_run_id=run_id)
        assert result["acknowledged"] == 12
        sender.send(args.topic, key=f"{run_id}:malformed".encode(), value=b"{malformed json").get(timeout=ACK_TIMEOUT_SECONDS)
        unlabeled = {name: value for name, value in transactions[0].items() if name != "Class"}

        def send_extra(row, index, original_index):
            event = create_transaction(row, 20, run_id, index)
            sender.send(args.topic, key=event["EventId"].encode(), value=event).get(timeout=ACK_TIMEOUT_SECONDS)
            expected[event["EventId"]] = expected[f"{run_id}:{original_index}"]

        send_extra(unlabeled, 12, 0)
        source = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", args.bootstrap_servers)
                  .option("subscribe", args.topic).option("startingOffsets", "earliest")
                  .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 10000).load())
        output = score_events(parse_events(source, features), model, features, threshold)

        def consume_available():
            prepare_output(args.output_dir, configuration)
            query = (output.writeStream.format("json").outputMode("append")
                     .option("path", str(args.output_dir / "predictions"))
                     .option("checkpointLocation", str(args.output_dir / "checkpoint"))
                     .option("ignoreNullFields", "false").trigger(availableNow=True).start())
            try:
                if not query.awaitTermination(60):
                    raise TimeoutError("Smoke-test streaming query exceeded 60 seconds")
            finally:
                if query.isActive:
                    query.stop()

        def verify(expected_count):
            # Read the directory, not a glob: Spark honors the file sink's commit metadata.
            rows = spark.read.schema(output.schema).json(str(args.output_dir / "predictions")).collect()
            assert len(rows) == expected_count, f"Expected {expected_count} outputs, got {len(rows)}"
            offsets = {(row["kafka_topic"], row["kafka_partition"], row["kafka_offset"]) for row in rows}
            assert len(offsets) == len(rows), "Duplicate source offsets"
            rejected = [row for row in rows if row["status"] == "rejected"]
            assert len(rejected) == 1 and rejected[0]["raw_json"] == "{malformed json"
            assert rejected[0]["fraud_probability"] is None and rejected[0]["final_prediction"] is None
            scored = [row for row in rows if row["status"] == "scored"]
            assert len(scored) == expected_count - 1
            assert {row["EventId"] for row in scored} == set(expected), "Missing or unexpected event IDs"
            for row in rows:
                assert row["ModelUID"] == classifier.uid and row["Threshold"] == threshold
            for row in scored:
                probability = expected[row["EventId"]]
                assert row["fraud_probability"] == probability, "Offline/streaming probability mismatch"
                assert row["final_prediction"] == int(probability >= threshold), "Threshold mismatch"
            assert next(row for row in scored if row["EventId"] == f"{run_id}:12")["Class"] is None
            return offsets

        consume_available()
        first_offsets = verify(14)
        print("Initial 14 events: 13 scored, one rejected; exact offline score parity.", flush=True)
        consume_available()
        assert verify(14) == first_offsets, "Restart changed committed output"
        send_extra(transactions[1], 13, 1)
        consume_available()
        assert first_offsets < verify(15), "Restart did not append exactly one new source offset"
        summary = dict(status="passed", topic=args.topic, producer_run_id=run_id, model_uid=classifier.uid,
                       threshold=threshold, initial_rows=14, rows_after_empty_restart=14, final_rows=15,
                       final_scored=14, final_rejected=1, exact_probability_parity=True,
                       checkpoint_resume_verified=True, duplicate_source_offsets=0)
        write_json(args.output_dir / "smoke_result.json", summary)
        print(json.dumps(summary, indent=2), flush=True)
    finally:
        try:
            if sender is not None:
                sender.close(timeout=ACK_TIMEOUT_SECONDS)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
