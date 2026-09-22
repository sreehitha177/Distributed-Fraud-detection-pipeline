"""Model compatibility and run routing without starting Spark."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch, sentinel

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

from ml_model import evaluate, model_utils, tune_threshold
from ml_model.train import FEATURE_COLUMNS


def mock_classifier():
    model = Mock(spec=RandomForestClassificationModel)
    model.uid = "RandomForestClassifier_test"
    model.numFeatures = len(FEATURE_COLUMNS)
    model.numClasses = 2
    model.getFeaturesCol.return_value = "features"
    return model


def mock_pipeline():
    classifier = mock_classifier()
    assembler = Mock(spec=VectorAssembler)
    assembler.getInputCols.return_value = FEATURE_COLUMNS
    assembler.getOutputCol.return_value = "features"
    model = Mock(spec=PipelineModel)
    model.stages = [assembler, classifier]
    return model


class ModelLoadingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def write_metadata(self, metadata):
        directory = self.directory / "model" / "metadata"
        directory.mkdir(parents=True)
        (directory / "part-00000").write_text(json.dumps(metadata))
        return directory.parent

    def test_legacy_metadata_chooses_forest_loader(self):
        model_path = self.write_metadata({
            "class": "org.apache.spark.ml.classification.RandomForestClassificationModel"
        })
        classifier = mock_classifier()
        with patch.object(RandomForestClassificationModel, "load", return_value=classifier) as load, \
                patch.object(PipelineModel, "load") as load_pipeline:
            self.assertIs(model_utils.load_fraud_model(model_path), classifier)
        load.assert_called_once_with(str(model_path))
        load_pipeline.assert_not_called()

    def test_pipeline_metadata_chooses_pipeline_loader_and_saved_order(self):
        model_path = self.write_metadata({"class": "org.apache.spark.ml.PipelineModel"})
        pipeline = mock_pipeline()
        with patch.object(PipelineModel, "load", return_value=pipeline) as load, \
                patch.object(RandomForestClassificationModel, "load") as load_classifier:
            self.assertIs(model_utils.load_fraud_model(model_path), pipeline)
        load.assert_called_once_with(str(model_path))
        load_classifier.assert_not_called()
        self.assertIs(model_utils.get_classifier(pipeline), pipeline.stages[-1])
        self.assertEqual(model_utils.get_feature_columns(pipeline, ["wrong", "order"]), FEATURE_COLUMNS)

    def test_rejects_unsupported_metadata_before_any_spark_loader(self):
        model_path = self.write_metadata({"class": "unrecognized.Model"})
        with patch.object(PipelineModel, "load") as pipeline_load, \
                patch.object(RandomForestClassificationModel, "load") as forest_load:
            with self.assertRaisesRegex(ValueError, "Unsupported"):
                model_utils.load_fraud_model(model_path)
        pipeline_load.assert_not_called()
        forest_load.assert_not_called()

    def test_rejects_malformed_metadata_before_any_spark_loader(self):
        model_path = self.write_metadata({})
        (model_path / "metadata" / "part-00000").write_text("{bad-json")
        with patch.object(PipelineModel, "load") as pipeline_load, \
                patch.object(RandomForestClassificationModel, "load") as forest_load:
            with self.assertRaises(ValueError):
                model_utils.load_fraud_model(model_path)
        pipeline_load.assert_not_called()
        forest_load.assert_not_called()

    def test_pipeline_transform_uses_saved_assembler(self):
        pipeline = mock_pipeline()
        result = model_utils.transform_model(pipeline, sentinel.data, FEATURE_COLUMNS)
        pipeline.transform.assert_called_once_with(sentinel.data)
        self.assertIs(result, pipeline.transform.return_value)
        pipeline.stages[0].transform.assert_not_called()

    def test_pipeline_transform_rejects_wrong_feature_order(self):
        pipeline = mock_pipeline()
        with self.assertRaisesRegex(ValueError, "feature order"):
            model_utils.transform_model(pipeline, sentinel.data, list(reversed(FEATURE_COLUMNS)))
        pipeline.transform.assert_not_called()

    def test_incomplete_or_missing_manifest_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "no manifest"):
            model_utils.load_run_manifest(self.directory)
        for status in (None, "running", "failed"):
            with self.subTest(status=status):
                (self.directory / "manifest.json").write_text(json.dumps({"status": status}))
                with self.assertRaisesRegex(ValueError, "not completed"):
                    model_utils.load_run_manifest(self.directory)

    def test_manifest_rejects_mismatched_classifier_or_feature_order(self):
        pipeline = mock_pipeline()
        manifest = {"model_uid": pipeline.stages[-1].uid, "feature_columns": FEATURE_COLUMNS}
        self.assertEqual(model_utils.validate_run_model(manifest, pipeline, []), FEATURE_COLUMNS)
        with self.assertRaisesRegex(ValueError, "different classifier"):
            model_utils.validate_run_model(dict(manifest, model_uid="other"), pipeline, [])
        with self.assertRaisesRegex(ValueError, "feature order"):
            model_utils.validate_run_model(
                dict(manifest, feature_columns=list(reversed(FEATURE_COLUMNS))), pipeline, []
            )


class RunRoutingTests(unittest.TestCase):
    def test_tuning_legacy_defaults_and_explicit_paths(self):
        args = tune_threshold.parse_args([])
        self.assertIsNone(args.run_dir)
        self.assertEqual(args.model_path, tune_threshold.MODEL_PATH)
        self.assertEqual(args.validation_data_path, tune_threshold.VALIDATION_DATA_PATH)
        self.assertEqual(args.output, tune_threshold.THRESHOLD_PATH)
        self.assertEqual(args.metrics_output, tune_threshold.METRICS_PATH)
        args = tune_threshold.parse_args(["--model-path", "custom/model", "--output", "custom/threshold.json"])
        self.assertEqual(args.model_path, Path("custom/model"))
        self.assertEqual(args.output, Path("custom/threshold.json"))

    def test_tuning_run_dir_routes_all_artifacts_together(self):
        args = tune_threshold.parse_args(["--run-dir", "runs/one", "--master", "local[2]"])
        self.assertEqual(args.model_path, Path("runs/one/model"))
        self.assertEqual(args.validation_data_path, Path("runs/one/validation.parquet"))
        self.assertEqual(args.output, Path("runs/one/threshold.json"))
        self.assertEqual(args.metrics_output, Path("runs/one/threshold_metrics.csv"))
        self.assertEqual(args.master, "local[2]")
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            tune_threshold.parse_args(["--run-dir", "runs/one", "--model-path", "other"])

    def test_evaluation_legacy_defaults_and_run_dir(self):
        args = evaluate.parse_args([])
        self.assertEqual(args.model_path, evaluate.MODEL_PATH)
        self.assertEqual(args.test_data_path, evaluate.TEST_DATA_PATH)
        self.assertEqual(args.threshold_path, evaluate.THRESHOLD_PATH)
        args = evaluate.parse_args(["--run-dir", "runs/one"])
        self.assertEqual(args.model_path, Path("runs/one/model"))
        self.assertEqual(args.test_data_path, Path("runs/one/test.parquet"))
        self.assertEqual(args.threshold_path, Path("runs/one/threshold.json"))

    def test_run_evaluation_requires_saved_threshold_before_starting_spark(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "manifest.json").write_text(json.dumps({"status": "completed"}))
            args = evaluate.parse_args(["--run-dir", directory])
            with patch.object(evaluate, "parse_args", return_value=args), \
                    patch.object(evaluate, "create_spark_session") as create_spark:
                with self.assertRaisesRegex(ValueError, "threshold file is missing"):
                    evaluate.main()
            create_spark.assert_not_called()

    def test_zero_threshold_override_is_honored_without_saved_threshold(self):
        classifier = mock_classifier()
        with tempfile.TemporaryDirectory() as directory:
            manifest = {"status": "completed", "model_uid": classifier.uid, "feature_columns": FEATURE_COLUMNS}
            (Path(directory) / "manifest.json").write_text(json.dumps(manifest))
            args = evaluate.parse_args(["--run-dir", directory, "--threshold", "0"])
            with patch.object(evaluate, "parse_args", return_value=args), \
                    patch.object(evaluate, "create_spark_session") as create_spark, \
                    patch.object(evaluate, "load_model", return_value=classifier), \
                    patch.object(evaluate, "load_test_data", return_value=sentinel.test_data), \
                    patch.object(evaluate, "evaluate_model") as evaluate_model, \
                    patch.object(evaluate, "load_selected_threshold") as load_threshold, \
                    contextlib.redirect_stdout(io.StringIO()):
                evaluate.main()
            self.assertEqual(evaluate_model.call_args.kwargs["threshold"], 0.0)
            load_threshold.assert_not_called()
            create_spark.return_value.stop.assert_called_once_with()

    def test_retuning_updates_threshold_files_and_manifest_together(self):
        pipeline = mock_pipeline()
        metrics = tune_threshold.compute_threshold_metrics([(0.35, 2, 0), (0.1, 0, 4)])
        selected = tune_threshold.select_threshold(metrics)
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            manifest_path = run_dir / "manifest.json"
            manifest = {
                "status": "completed", "model_uid": pipeline.stages[-1].uid,
                "feature_columns": FEATURE_COLUMNS, "selected_candidate": "baseline",
                "selected": {"threshold": 0.5},
            }
            manifest_path.write_text(json.dumps(manifest))
            args = tune_threshold.parse_args(["--run-dir", directory])
            with patch.object(tune_threshold, "parse_args", return_value=args), \
                    patch.object(tune_threshold, "create_spark_session") as create_spark, \
                    patch.object(tune_threshold, "load_fraud_model", return_value=pipeline), \
                    patch.object(tune_threshold, "tune_threshold", return_value=(selected, metrics)), \
                    contextlib.redirect_stdout(io.StringIO()):
                tune_threshold.main()
            saved_threshold = json.loads((run_dir / "threshold.json").read_text())
            saved_manifest = json.loads(manifest_path.read_text())
            self.assertEqual(saved_threshold["model_uid"], pipeline.stages[-1].uid)
            self.assertEqual(saved_threshold["feature_columns"], FEATURE_COLUMNS)
            self.assertEqual(saved_threshold["selected"], selected)
            self.assertEqual(saved_manifest["selected"], saved_threshold["selected"])
            self.assertEqual(saved_manifest["threshold_updated_at"], saved_threshold["created_at"])
            self.assertEqual(saved_manifest["selected_candidate"], "baseline")
            self.assertEqual(len((run_dir / "threshold_metrics.csv").read_text().splitlines()), len(metrics) + 1)
            create_spark.return_value.stop.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
