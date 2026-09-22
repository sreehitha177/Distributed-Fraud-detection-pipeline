"""Small synthetic integration checks; opt in with RUN_SPARK_TESTS=1.

These tests start a local[2] Spark session and use temporary artifacts only.
They never read the project's credit-card CSV or its existing model/splits.
"""

import csv
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from ml_model import train, tune_threshold
from ml_model.model_utils import (
    get_classifier,
    get_feature_columns,
    load_fraud_model,
    load_run_manifest,
    transform_model,
    validate_run_model,
)


@unittest.skipUnless(os.environ.get("RUN_SPARK_TESTS") == "1",
                     "Set RUN_SPARK_TESTS=1 to run local Spark integration tests")
class TrainingSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("FraudTrainingIntegrationTests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.default.parallelism", "2")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def synthetic_rows(count=256):
        rows = []
        for index in range(count):
            label = float(index % 5 == 0)
            features = [float(index), label]
            features.extend(((index * (column + 3)) % 101) / 101.0 for column in range(2, 30))
            rows.append(features + [label])
        return rows

    def frame(self, rows=None, string_column=None):
        schema = StructType([
            StructField(name, StringType() if name == string_column else DoubleType(), True)
            for name in train.REQUIRED_COLUMNS
        ])
        rows = rows if rows is not None else self.synthetic_rows()
        if string_column is not None:
            position = train.REQUIRED_COLUMNS.index(string_column)
            rows = [list(row) for row in rows]
            for row in rows:
                row[position] = str(row[position])
        return self.spark.createDataFrame(rows, schema)

    @staticmethod
    def scores(model, frame):
        predicted = transform_model(model, frame, train.FEATURE_COLUMNS)
        return [(row["Time"], row["score"]) for row in predicted.select(
            "Time", vector_to_array("probability")[1].alias("score")
        ).orderBy("Time").collect()]

    def test_validation_rejects_bad_types_values_and_class_coverage(self):
        good_rows = self.synthetic_rows(2)
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            train.validate_data(self.frame(good_rows).drop("Amount"))
        for column in ("V1", "Class"):
            with self.subTest(string_column=column):
                with self.assertRaisesRegex(ValueError, "nonnumeric"):
                    train.validate_data(self.frame(good_rows, string_column=column))
        for column, value in (("V1", None), ("V1", float("nan")),
                              ("Amount", float("inf")), ("Time", -float("inf")),
                              ("Class", 2.0), ("Class", 0.5), ("Class", None)):
            with self.subTest(column=column, value=value):
                rows = [list(row) for row in good_rows]
                rows[0][train.REQUIRED_COLUMNS.index(column)] = value
                with self.assertRaisesRegex(ValueError, "missing/non-finite|labels"):
                    train.validate_data(self.frame(rows))
        for rows in ([], good_rows[:1], good_rows[1:]):
            with self.subTest(rows=rows):
                with self.assertRaisesRegex(ValueError, "both fraud"):
                    train.validate_data(self.frame(rows))

    def test_cleaning_keeps_existing_missing_row_policy_and_rejects_other_bad_data(self):
        rows = self.synthetic_rows(4)
        rows[2][1] = None
        rows[3][2] = float("nan")
        cleaned = train.clean_data(self.frame(rows))
        self.assertEqual({row["Time"] for row in cleaned.select("Time").collect()}, {0.0, 1.0})
        with self.assertRaisesRegex(ValueError, "nonnumeric"):
            train.clean_data(self.frame(string_column="V1"))
        for column, value in (("V2", float("inf")), ("Class", 2.0)):
            rows = self.synthetic_rows(3)
            rows[2][train.REQUIRED_COLUMNS.index(column)] = value
            with self.subTest(column=column):
                with self.assertRaisesRegex(ValueError, "missing/non-finite|labels"):
                    train.clean_data(self.frame(rows))

    def test_fixed_seed_splits_repeat_membership_and_approximate_64_16_20(self):
        frame = self.frame(self.synthetic_rows(3000)).cache()
        self.addCleanup(frame.unpersist)
        memberships = []
        for _ in range(2):
            memberships.append([
                {row["Time"] for row in split.select("Time").collect()}
                for split in train.split_data(frame)
            ])
        self.assertEqual(memberships[0], memberships[1])
        training, validation, test = memberships[0]
        self.assertFalse(training & validation or training & test or validation & test)
        self.assertEqual(training | validation | test, set(range(3000)))
        for members, expected in zip(memberships[0], (0.64, 0.16, 0.20)):
            self.assertAlmostEqual(len(members) / 3000, expected, delta=0.035)

    def test_same_forest_seed_reproduces_scores(self):
        frame = self.frame().cache()
        self.addCleanup(frame.unpersist)
        first = train.train_random_forest(frame, train.FEATURE_COLUMNS,
                                          seed=17, num_trees=3, max_depth=3)
        second = train.train_random_forest(frame, train.FEATURE_COLUMNS,
                                           seed=17, num_trees=3, max_depth=3)
        self.assertEqual(self.scores(first, frame), self.scores(second, frame))

    def test_weighted_pipeline_and_legacy_forest_round_trip_raw_features(self):
        frame = self.frame().cache()
        self.addCleanup(frame.unpersist)
        model = train.train_random_forest(frame, train.FEATURE_COLUMNS, weighting="balanced",
                                          seed=42, num_trees=3, max_depth=3)
        self.assertIsInstance(model, PipelineModel)
        self.assertIsInstance(model.stages[0], VectorAssembler)
        self.assertEqual(model.stages[0].getInputCols(), train.FEATURE_COLUMNS)
        self.assertEqual(get_classifier(model).getWeightCol(), "class_weight")
        expected = self.scores(model, frame)
        self.assertEqual(len(expected), 256)
        with tempfile.TemporaryDirectory() as directory:
            pipeline_path = Path(directory) / "pipeline"
            train.save_model(model, pipeline_path)
            loaded = load_fraud_model(pipeline_path)
            self.assertIsInstance(loaded, PipelineModel)
            self.assertEqual(get_classifier(loaded).uid, get_classifier(model).uid)
            self.assertEqual(get_feature_columns(loaded, []), train.FEATURE_COLUMNS)
            self.assertEqual(self.scores(loaded, frame), expected)

            legacy_path = Path(directory) / "legacy-forest"
            get_classifier(model).write().save(str(legacy_path))
            legacy = load_fraud_model(legacy_path)
            self.assertIsInstance(legacy, RandomForestClassificationModel)
            self.assertEqual(self.scores(legacy, frame), expected)

    def test_training_search_saves_complete_run_and_scores_validation_only(self):
        small_candidates = [
            dict(name="tiny", num_trees=2, max_depth=2, min_instances_per_node=1, weighting="none"),
            dict(name="tiny_weighted", num_trees=2, max_depth=2, min_instances_per_node=1, weighting="balanced"),
        ]
        scored_memberships = []
        original_score = tune_threshold.score_validation_data

        def record_scoring(model, frame):
            scored_memberships.append({row["Time"] for row in frame.select("Time").collect()})
            return original_score(model, frame)

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "synthetic.csv"
            with source.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(train.REQUIRED_COLUMNS)
                writer.writerows(self.synthetic_rows())
            run_dir = Path(directory) / "run"
            with patch.object(train, "CANDIDATES", small_candidates), \
                    patch.object(tune_threshold, "score_validation_data", side_effect=record_scoring):
                returned = train.run_training(self.spark, source, run_dir, search=True)

            manifest = load_run_manifest(run_dir)
            self.assertEqual(returned["status"], "completed")
            self.assertEqual(manifest["status"], "completed")
            for relative_path in manifest["artifacts"].values():
                self.assertTrue((run_dir / relative_path).exists(), relative_path)
            self.assertEqual(set(manifest["splits"]["test"]), {"rows", "class_counts"})

            validation = self.spark.read.parquet(str(run_dir / "validation.parquet"))
            validation_ids = {row["Time"] for row in validation.select("Time").collect()}
            test = self.spark.read.parquet(str(run_dir / "test.parquet"))
            test_ids = {row["Time"] for row in test.select("Time").collect()}
            self.assertEqual(len(test_ids), manifest["splits"]["test"]["rows"])
            self.assertFalse(validation_ids & test_ids)
            self.assertEqual(scored_memberships, [validation_ids, validation_ids])

            model = load_fraud_model(run_dir / "model")
            self.assertEqual(validate_run_model(manifest, model, train.FEATURE_COLUMNS),
                             train.FEATURE_COLUMNS)
            artifact = json.loads((run_dir / "threshold.json").read_text())
            self.assertEqual(artifact["model_uid"], get_classifier(model).uid)
            self.assertEqual(artifact["model_uid"], manifest["model_uid"])
            selected_threshold = tune_threshold.load_selected_threshold(
                run_dir / "threshold.json", get_classifier(model).uid, train.FEATURE_COLUMNS)
            self.assertEqual(selected_threshold, manifest["selected"]["threshold"])
            candidates = json.loads((run_dir / "candidates.json").read_text())
            self.assertEqual(manifest["selected_candidate"],
                             max(candidates, key=train.candidate_rank)["name"])


if __name__ == "__main__":
    unittest.main()
