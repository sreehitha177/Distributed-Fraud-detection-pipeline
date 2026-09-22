"""Streaming CLI checks and opt-in synthetic Kafka-record scoring tests."""

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from kafka_stream import consumer, producer
from ml_model.model_utils import get_classifier, transform_model
from ml_model.train import FEATURE_COLUMNS, train_random_forest


class ConsumerCLITests(unittest.TestCase):
    def test_run_directory_is_required(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            consumer.parse_args([])

    def test_default_local_consumer_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            args = consumer.parse_args(["--run-dir", directory])
        self.assertEqual(args.run_dir.resolve(), Path(directory).resolve())
        self.assertEqual(args.topic, "fraud-transactions")
        self.assertEqual(args.bootstrap_servers, "localhost:9092")
        self.assertEqual(args.master, "local[2]")
        self.assertEqual(args.starting_offsets, "earliest")
        self.assertEqual(args.trigger_seconds, 2.0)
        self.assertEqual(args.max_offsets_per_trigger, 10000)
        self.assertFalse(args.available_now)
        self.assertEqual(args.output_dir.name, args.topic)
        self.assertEqual(args.output_dir.parent.name, "runtime")

    def test_explicit_execution_and_load_options(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            args = consumer.parse_args([
                "--run-dir", directory, "--topic", "experiment-2",
                "--bootstrap-servers", "broker:9092", "--master", "local[4]",
                "--output-dir", str(output), "--starting-offsets", "latest",
                "--trigger-seconds", "0.25", "--max-offsets-per-trigger", "25",
                "--available-now",
            ])
        self.assertEqual(args.output_dir.resolve(), output.resolve())
        self.assertEqual(args.topic, "experiment-2")
        self.assertEqual(args.master, "local[4]")
        self.assertEqual(args.bootstrap_servers, "broker:9092")
        self.assertEqual(args.starting_offsets, "latest")
        self.assertEqual(args.trigger_seconds, 0.25)
        self.assertEqual(args.max_offsets_per_trigger, 25)
        self.assertTrue(args.available_now)

    def test_invalid_trigger_limits_and_offsets_are_rejected(self):
        invalid_options = [
            ("--trigger-seconds", value) for value in ("0", "-1", "nan", "inf")
        ] + [
            ("--max-offsets-per-trigger", value) for value in ("0", "-1", "1.5")
        ] + [("--starting-offsets", "middle"), ("--topic", "../outside"),
             ("--topic", ".."), ("--bootstrap-servers", " ")]
        with tempfile.TemporaryDirectory() as directory:
            for option, value in invalid_options:
                with self.subTest(option=option, value=value):
                    with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                        consumer.parse_args(["--run-dir", directory, option, value])

    def test_invalid_threshold_is_rejected_before_spark_operations(self):
        for threshold in (float("nan"), float("inf"), -0.1, 1.1):
            with self.subTest(threshold=threshold), self.assertRaisesRegex(ValueError, "Threshold"):
                consumer.score_events(None, None, FEATURE_COLUMNS, threshold)


class ConsumerCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name) / "stream"
        self.configuration = {
            "schema_version": 1, "bootstrap_servers": "localhost:9092",
            "topic": "fraud-transactions", "run_dir": "/models/run-one",
            "model_uid": "RandomForestClassifier_test", "threshold": 0.35,
            "feature_columns": FEATURE_COLUMNS, "output_dir": str(self.output),
        }

    def test_identical_configuration_resumes_without_replacing_existing_files(self):
        consumer.prepare_output(self.output, self.configuration)
        saved = (self.output / "stream.json").read_text()
        checkpoint = self.output / "checkpoint"
        predictions = self.output / "predictions"
        checkpoint.mkdir()
        predictions.mkdir()
        (checkpoint / "offset-marker").write_text("offset 123")
        (predictions / "result-marker").write_text("previous prediction")

        consumer.prepare_output(self.output, dict(self.configuration))

        self.assertEqual((self.output / "stream.json").read_text(), saved)
        self.assertEqual(json.loads(saved), self.configuration)
        self.assertEqual((checkpoint / "offset-marker").read_text(), "offset 123")
        self.assertEqual((predictions / "result-marker").read_text(), "previous prediction")

    def test_changed_model_threshold_or_kafka_source_cannot_reuse_checkpoint(self):
        consumer.prepare_output(self.output, self.configuration)
        for field, replacement in (("model_uid", "another-classifier"), ("threshold", 0.5),
                                   ("topic", "another-topic"), ("bootstrap_servers", "other:9092"),
                                   ("feature_columns", list(reversed(FEATURE_COLUMNS)))):
            changed = {**self.configuration, field: replacement}
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "configuration changed"):
                consumer.prepare_output(self.output, changed)
            self.assertEqual(json.loads((self.output / "stream.json").read_text()), self.configuration)

    def test_existing_predictions_without_checkpoint_cannot_restart_from_beginning(self):
        consumer.prepare_output(self.output, self.configuration)
        (self.output / "predictions").mkdir()
        with self.assertRaisesRegex(ValueError, "without their checkpoint"):
            consumer.prepare_output(self.output, self.configuration)

    def test_checkpoint_without_predictions_cannot_skip_missing_results(self):
        consumer.prepare_output(self.output, self.configuration)
        (self.output / "checkpoint").mkdir()
        with self.assertRaisesRegex(ValueError, "without its predictions"):
            consumer.prepare_output(self.output, self.configuration)

    def test_unrecognized_nonempty_output_is_preserved(self):
        self.output.mkdir()
        existing = self.output / "existing-results.txt"
        existing.write_text("keep these results")
        with self.assertRaisesRegex(ValueError, "not empty"):
            consumer.prepare_output(self.output, self.configuration)
        self.assertEqual(existing.read_text(), "keep these results")
        self.assertFalse((self.output / "stream.json").exists())


class ProducerDeliveryTests(unittest.TestCase):
    def test_event_timestamp_does_not_replace_ml_time_or_mutate_source_row(self):
        row = {"Time": 125, "V1": 0.25, "Amount": 19, "Class": 0}
        original = dict(row)
        with patch.object(producer.time, "time", return_value=1700000000.125):
            event = producer.create_transaction(row, 20, "producer-run", 7)
        self.assertEqual(row, original)
        self.assertEqual(event["Time"], 125.0)
        self.assertEqual(event["EventTimestamp"], 1700000000.125)
        self.assertEqual(event["EventId"], "producer-run:7")
        self.assertEqual(event["ProducerRunId"], "producer-run")
        self.assertEqual(event["IngestionRate"], 20.0)
        for name, value in original.items():
            self.assertEqual(event[name], value)
            self.assertIsInstance(event[name], float)

    def test_every_delivery_is_acknowledged_with_unique_event_keys(self):
        futures = [Mock() for _ in range(3)]
        kafka_producer = Mock()
        kafka_producer.send.side_effect = futures
        transactions = [{"Time": index, "Amount": 10} for index in range(3)]
        with patch.object(producer, "MAX_PENDING_SENDS", 2), \
                patch.object(producer.time, "perf_counter", return_value=1000.0), \
                patch.object(producer.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
            result = producer.send_transactions(
                kafka_producer, transactions, topic="fraud-transactions", rate=20, count=3,
                sequential=True, producer_run_id="delivery-test")
        self.assertEqual(result["acknowledged"], 3)
        self.assertEqual(kafka_producer.send.call_count, 3)
        for index, (future, sent) in enumerate(zip(futures, kafka_producer.send.call_args_list)):
            future.get.assert_called_once_with(timeout=producer.ACK_TIMEOUT_SECONDS)
            self.assertEqual(sent.args, ("fraud-transactions",))
            self.assertEqual(sent.kwargs["key"], f"delivery-test:{index}".encode("utf-8"))
            self.assertEqual(sent.kwargs["value"]["Time"], index)
        kafka_producer.flush.assert_called_once_with(timeout=producer.ACK_TIMEOUT_SECONDS)

    def test_failed_acknowledgement_propagates_during_both_pending_queue_drains(self):
        # Exercise an acknowledgement checked during sending and one checked
        # after all sends: neither failure may be hidden by a successful flush.
        for pending_limit in (2, 1000):
            with self.subTest(pending_limit=pending_limit):
                futures = [Mock() for _ in range(3)]
                futures[1].get.side_effect = RuntimeError("delivery failed")
                kafka_producer = Mock()
                kafka_producer.send.side_effect = futures
                with patch.object(producer, "MAX_PENDING_SENDS", pending_limit), \
                        patch.object(producer.time, "perf_counter", return_value=1000.0), \
                        patch.object(producer.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError, "delivery failed"):
                        producer.send_transactions(
                            kafka_producer, [{"Time": 1}], topic="fraud-transactions",
                            rate=20, count=3, producer_run_id="failed-delivery")
                futures[0].get.assert_called_once_with(timeout=producer.ACK_TIMEOUT_SECONDS)
                futures[1].get.assert_called_once_with(timeout=producer.ACK_TIMEOUT_SECONDS)
                kafka_producer.flush.assert_not_called()


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1",
                     "Set RUN_SPARK_TESTS=1 to run local Spark streaming tests")
class ConsumerSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("FraudStreamingIntegrationTests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.default.parallelism", "2")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        rows = []
        for index in range(128):
            label = float(index % 5 == 0)
            values = [float(index), label]
            values.extend(((index * (column + 3)) % 101) / 101.0 for column in range(2, 30))
            rows.append(values + [label])
        schema = StructType([StructField(name, DoubleType()) for name in FEATURE_COLUMNS + ["Class"]])
        training = cls.spark.createDataFrame(rows, schema)
        cls.model = train_random_forest(training, FEATURE_COLUMNS, seed=42,
                                        num_trees=3, max_depth=3)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def event(index, integer_tokens=False, with_label=True):
        event = {name: position + index if integer_tokens else (position + index) / 10.0
                 for position, name in enumerate(FEATURE_COLUMNS)}
        event.update(EventId=f"event-{index}", ProducerRunId="synthetic-producer",
                     EventTimestamp=1700000000 if integer_tokens else 1700000000.125,
                     IngestionRate=5 if integer_tokens else 5.0)
        if with_label:
            event["Class"] = index % 2 if integer_tokens else float(index % 2)
        return event

    def kafka_records(self, events):
        schema = StructType([
            StructField("value", BinaryType()), StructField("topic", StringType()),
            StructField("partition", IntegerType()), StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
        ])
        rows = []
        for offset, event in enumerate(events):
            payload = event if isinstance(event, str) else json.dumps(event)
            rows.append((payload.encode("utf-8"), "test-transactions", offset % 2,
                         offset, datetime(2026, 9, 17, 12, 0, 0)))
        return self.spark.createDataFrame(rows, schema)

    def test_parsing_preserves_numeric_tokens_and_kafka_metadata(self):
        events = [self.event(0, integer_tokens=True), self.event(1)]
        parsed = consumer.parse_events(self.kafka_records(events), FEATURE_COLUMNS)
        rows = parsed.orderBy("kafka_offset").collect()
        self.assertEqual(len(rows), 2)
        for offset, row in enumerate(rows):
            self.assertTrue(row["valid_event"])
            self.assertEqual(row["kafka_topic"], "test-transactions")
            self.assertEqual(row["kafka_partition"], offset % 2)
            self.assertEqual(row["kafka_offset"], offset)
            self.assertEqual(row["kafka_timestamp"], datetime(2026, 9, 17, 12, 0, 0))
            self.assertEqual(json.loads(row["raw_json"]), events[offset])
            for feature in FEATURE_COLUMNS:
                self.assertEqual(row[feature], float(events[offset][feature]))
            self.assertEqual(row["EventTimestamp"], events[offset]["EventTimestamp"])

    def test_unlabeled_events_are_scored_and_label_does_not_change_prediction(self):
        events = [self.event(0, with_label=False) for _ in range(3)]
        events[1].update(EventId="label-zero", Class=0.0)
        events[2].update(EventId="label-one", Class=1.0)
        parsed = consumer.parse_events(self.kafka_records(events), FEATURE_COLUMNS)
        scored = consumer.score_events(parsed, self.model, FEATURE_COLUMNS, 0.4)
        rows = scored.orderBy("kafka_offset").collect()
        self.assertEqual(len(rows), 3)
        self.assertIsNone(rows[0]["Class"])
        self.assertEqual({row["status"] for row in rows}, {"scored"})
        self.assertEqual(len({row["fraud_probability"] for row in rows}), 1)
        self.assertEqual(len({row["final_prediction"] for row in rows}), 1)
        for row in rows:
            self.assertEqual(row["ModelUID"], get_classifier(self.model).uid)
            self.assertEqual(row["Threshold"], 0.4)
            self.assertIsNotNone(row["BatchTimestamp"])

    def test_invalid_events_are_retained_as_rejections_in_a_mixed_batch(self):
        events = [self.event(0, with_label=False)]
        for name, value in (("V1", None), ("V2", "not-a-number"),
                            ("V3", float("inf")), ("Amount", float("nan")),
                            ("EventId", ""), ("EventTimestamp", 0.0),
                            ("EventTimestamp", float("inf"))):
            event = self.event(len(events))
            event[name] = value
            events.append(event)
        missing = self.event(len(events))
        del missing["V28"]
        events.extend([missing, "{malformed json", "null"])

        parsed = consumer.parse_events(self.kafka_records(events), FEATURE_COLUMNS)
        rows = consumer.score_events(parsed, self.model, FEATURE_COLUMNS, 0.5).orderBy("kafka_offset").collect()
        self.assertEqual(len(rows), len(events))
        self.assertEqual(rows[0]["status"], "scored")
        for offset, row in enumerate(rows[1:], start=1):
            with self.subTest(offset=offset):
                self.assertEqual(row["kafka_offset"], offset)
                self.assertEqual(row["status"], "rejected")
                self.assertIsNone(row["fraud_probability"])
                self.assertIsNone(row["final_prediction"])
                self.assertIsNotNone(row["raw_json"])

    def test_streaming_scores_match_offline_pipeline_and_include_threshold_equality(self):
        events = [self.event(index, with_label=False) for index in range(6)]
        parsed = consumer.parse_events(self.kafka_records(events), FEATURE_COLUMNS)
        raw = parsed.select(*FEATURE_COLUMNS, "EventId")
        offline = transform_model(self.model, raw, FEATURE_COLUMNS).select(
            "EventId", vector_to_array("probability")[1].alias("score")
        ).collect()
        expected = {row["EventId"]: row["score"] for row in offline}
        threshold = expected["event-0"]
        rows = consumer.score_events(parsed, self.model, FEATURE_COLUMNS, threshold).collect()
        self.assertEqual(len(rows), len(events))
        for row in rows:
            score = expected[row["EventId"]]
            self.assertEqual(row["fraud_probability"], score)
            self.assertEqual(row["final_prediction"], float(score >= threshold))
        at_threshold = next(row for row in rows if row["EventId"] == "event-0")
        self.assertEqual(at_threshold["final_prediction"], 1.0)


if __name__ == "__main__":
    unittest.main()
