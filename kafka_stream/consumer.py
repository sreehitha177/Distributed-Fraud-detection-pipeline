"""Score Kafka transactions with a completed training run and persist the results."""

import argparse
import json
import math
import re
import sys
from pathlib import Path

from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F, types as T

# Also support spark-submit kafka_stream/consumer.py from the repository root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ml_model.model_utils import (
    get_classifier, load_fraud_model, load_run_manifest, transform_model,
    validate_run_model,
)
from ml_model.train import FEATURE_COLUMNS, write_json
from ml_model.tune_threshold import load_selected_threshold


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="fraud-transactions")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--output-dir", type=Path,
                        help="Local results directory; reuse it to resume the same stream")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest",
                        help="Applies only on first start; restarts use the saved checkpoint")
    parser.add_argument("--trigger-seconds", type=float, default=2.0)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    parser.add_argument("--available-now", action="store_true",
                        help="Process currently available Kafka records, then exit")
    args = parser.parse_args(argv)
    if not math.isfinite(args.trigger_seconds) or args.trigger_seconds <= 0:
        parser.error("--trigger-seconds must be finite and greater than zero")
    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger must be greater than zero")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,249}", args.topic) or args.topic in (".", ".."):
        parser.error("--topic must be a single valid Kafka topic name")
    if not args.bootstrap_servers.strip():
        parser.error("--bootstrap-servers cannot be empty")
    args.run_dir = args.run_dir.resolve()
    args.output_dir = (args.output_dir or PROJECT_ROOT / "kafka_stream" / "runtime" / args.topic).resolve()
    return args


def finite(column):
    value = F.col(column)
    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf"))


def parse_events(kafka_data, feature_columns):
    """Keep source offsets and flag malformed events without dropping them."""
    schema = T.StructType(
        [T.StructField(name, T.DoubleType(), True) for name in feature_columns]
        + [T.StructField("Class", T.DoubleType(), True),
           T.StructField("EventId", T.StringType(), True),
           T.StructField("ProducerRunId", T.StringType(), True),
           T.StructField("EventTimestamp", T.DoubleType(), True),
           T.StructField("IngestionRate", T.DoubleType(), True),
           T.StructField("_corrupt_record", T.StringType(), True)]
    )
    parsed = kafka_data.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_json"),
    ).withColumn("event", F.from_json("raw_json", schema, {
        "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record",
    })).select("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
               "raw_json", "event.*")
    valid = (F.col("_corrupt_record").isNull()
             & (F.length(F.trim(F.col("EventId"))) > 0)
             & finite("EventTimestamp") & (F.col("EventTimestamp") > 0))
    for name in feature_columns:
        valid = valid & finite(name)
    return parsed.withColumn("valid_event", F.coalesce(valid, F.lit(False)))


def score_events(parsed, model, feature_columns, threshold):
    """Use the saved assembler and threshold; invalid inputs receive no prediction."""
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("Threshold must be finite and between zero and one")
    # Sanitize only the temporary model inputs for rejected events. This allows
    # one source pass, preserves every offset, and avoids assembler failures.
    prepared = parsed.withColumn("original_time", F.col("Time")).withColumn(
        "original_amount", F.col("Amount")
    )
    prepared = prepared.select(*[
        F.when(F.col("valid_event"), F.col(name)).otherwise(F.lit(0.0)).alias(name)
        if name in feature_columns else F.col(name)
        for name in prepared.columns
    ])
    classifier = get_classifier(model)
    predictions = transform_model(model, prepared, feature_columns)
    predictions = predictions.withColumn(
        "fraud_probability",
        F.when(F.col("valid_event"), vector_to_array(classifier.getProbabilityCol())[1]),
    ).withColumn(
        "final_prediction",
        F.when(F.col("valid_event"), (F.col("fraud_probability") >= threshold).cast("int")),
    )
    return predictions.select(
        "EventId", "ProducerRunId", "EventTimestamp", "IngestionRate", "Class",
        F.col("original_time").alias("Time"), F.col("original_amount").alias("Amount"),
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "fraud_probability", "final_prediction",
        F.when(F.col("valid_event"), F.lit("scored")).otherwise("rejected").alias("status"),
        F.when(~F.col("valid_event"), F.lit(
            "Invalid JSON, missing/nonfinite features, empty EventId, or invalid EventTimestamp"
        )).alias("error"),
        F.when(~F.col("valid_event"), F.col("raw_json")).alias("raw_json"),
        F.lit(classifier.uid).alias("ModelUID"), F.lit(threshold).alias("Threshold"),
        # Spark evaluates current_timestamp at the start of a micro-batch.
        # It is deliberately not presented as prediction completion/latency.
        F.current_timestamp().alias("BatchTimestamp"),
    )


def prepare_output(output_dir, configuration):
    """Bind a local checkpoint/output pair to one source, model, and threshold."""
    output_dir = Path(output_dir)
    config_path = output_dir / "stream.json"
    if config_path.exists():
        if json.loads(config_path.read_text()) != configuration:
            raise ValueError("Stream configuration changed. Use a new --output-dir for this experiment.")
        predictions_exist = (output_dir / "predictions").exists()
        checkpoint_exists = (output_dir / "checkpoint").exists()
        if predictions_exist and not checkpoint_exists:
            raise ValueError("Predictions exist without their checkpoint. Use a new --output-dir.")
        if checkpoint_exists and not predictions_exist:
            raise ValueError("Checkpoint exists without its predictions. Use a new --output-dir.")
    else:
        if output_dir.exists() and any(output_dir.iterdir()):
            raise ValueError("Output directory is not empty and has no stream.json; use a new --output-dir.")
        output_dir.mkdir(parents=True, exist_ok=True)
        write_json(config_path, configuration)


def main():
    args = parse_args()
    manifest = load_run_manifest(args.run_dir)
    threshold_path = args.run_dir / "threshold.json"
    if not threshold_path.is_file():
        raise ValueError(f"Selected threshold file is missing: {threshold_path}")
    spark = (SparkSession.builder.appName("FraudDetectionStreaming").master(args.master)
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        model = load_fraud_model(args.run_dir / "model")
        feature_columns = validate_run_model(manifest, model, FEATURE_COLUMNS)
        classifier = get_classifier(model)
        threshold = load_selected_threshold(threshold_path, classifier.uid, feature_columns)
        prepare_output(args.output_dir, {
            "schema_version": 1, "bootstrap_servers": args.bootstrap_servers,
            "topic": args.topic, "run_dir": str(args.run_dir), "model_uid": classifier.uid,
            "feature_columns": feature_columns, "threshold": threshold,
            "output_dir": str(args.output_dir),
        })
        kafka_data = (spark.readStream.format("kafka")
                      .option("kafka.bootstrap.servers", args.bootstrap_servers)
                      .option("subscribe", args.topic)
                      .option("startingOffsets", args.starting_offsets)
                      .option("failOnDataLoss", "true")
                      .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger).load())
        output = score_events(parse_events(kafka_data, feature_columns), model, feature_columns, threshold)
        writer = (output.writeStream.format("json").outputMode("append")
                  .option("path", str(args.output_dir / "predictions"))
                  .option("checkpointLocation", str(args.output_dir / "checkpoint"))
                  .option("ignoreNullFields", "false"))
        writer = (writer.trigger(availableNow=True) if args.available_now
                  else writer.trigger(processingTime=f"{args.trigger_seconds} seconds"))
        query = writer.start()
        print(f"Model: {args.run_dir / 'model'}", flush=True)
        print(f"Threshold: {threshold:.12g}; master: {spark.sparkContext.master}", flush=True)
        print(f"Kafka: {args.bootstrap_servers}; topic: {args.topic}", flush=True)
        print(f"Results: {args.output_dir / 'predictions'}", flush=True)
        print("Consumer started. Ctrl+C stops it; the same output directory resumes its checkpoint.", flush=True)
        seen_batch = -1
        while True:
            finished = query.awaitTermination(1)
            for progress in query.recentProgress:
                # Idle progress can reuse the next data batch's ID. Do not mark
                # it as reported before that batch actually processes records.
                if progress["numInputRows"] == 0:
                    continue
                batch_id = progress["batchId"]
                if batch_id <= seen_batch:
                    continue
                seen_batch = batch_id
                print(f"Batch {batch_id}: {progress['numInputRows']} input events; "
                      f"{progress['processedRowsPerSecond']:.2f} processed rows/s; "
                      f"{progress.get('durationMs', {}).get('triggerExecution', 0)} ms trigger", flush=True)
            if finished:
                break
    except KeyboardInterrupt:
        print("Stopping consumer...", flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
