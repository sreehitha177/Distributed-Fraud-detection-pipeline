"""Training configuration and lifecycle checks that do not start Spark."""

import math
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ml_model import train


class TrainingClassWeightsTests(unittest.TestCase):
    def test_unweighted_training_gives_both_classes_unit_weight(self):
        self.assertEqual(train.training_class_weights({0: 900, 1: 100}, "none"),
                         {0: 1.0, 1: 1.0})

    def test_balanced_weights_equalize_total_weight_per_class(self):
        counts = {0: 900, 1: 100}
        weights = train.training_class_weights(counts, "balanced")
        self.assertAlmostEqual(weights[0], 5 / 9)
        self.assertAlmostEqual(weights[1], 5.0)
        self.assertAlmostEqual(counts[0] * weights[0], counts[1] * weights[1])
        self.assertAlmostEqual(sum(counts[label] * weights[label] for label in counts),
                               sum(counts.values()))

    def test_square_root_weights_reduce_balancing_strength(self):
        weights = train.training_class_weights({0: 900, 1: 100}, "sqrt_balanced")
        self.assertAlmostEqual(weights[0], math.sqrt(5 / 9))
        self.assertAlmostEqual(weights[1], math.sqrt(5))
        self.assertAlmostEqual(weights[1] / weights[0], 3.0)

    def test_rejects_missing_invalid_or_empty_classes(self):
        for counts in ({}, {0: 100}, {1: 10}, {0: 100, 1: 0},
                       {0: -1, 1: 10}, {0: 100, 1: 10, 2: 1}):
            with self.subTest(counts=counts):
                with self.assertRaises(ValueError):
                    train.training_class_weights(counts, "balanced")

    def test_rejects_unknown_weighting_mode(self):
        with self.assertRaisesRegex(ValueError, "Unknown weighting"):
            train.training_class_weights({0: 100, 1: 10}, "unsupported")

    def test_rejects_fractional_nonfinite_and_boolean_counts(self):
        for invalid_count in (0.5, float("nan"), float("inf"), True):
            with self.subTest(count=invalid_count):
                with self.assertRaises(ValueError):
                    train.training_class_weights({0: 100, 1: invalid_count}, "balanced")


class CandidateSelectionTests(unittest.TestCase):
    @staticmethod
    def result(f1, recall, precision, baseline_f1=0.0):
        return {"selected": {"f1": f1, "recall": recall, "precision": precision},
                "baseline": {"f1": baseline_f1}}

    def test_selected_fraud_f1_takes_priority_over_baseline_and_recall(self):
        better_f1 = self.result(0.85, 0.8, 0.9, baseline_f1=0.3)
        higher_recall = self.result(0.80, 1.0, 0.67, baseline_f1=0.95)
        self.assertGreater(train.candidate_rank(better_f1), train.candidate_rank(higher_recall))

    def test_f1_ties_prefer_recall_then_precision_and_preserve_exact_ties(self):
        low_recall = self.result(0.8, 0.7, 0.95)
        high_recall = self.result(0.8, 0.9, 0.72)
        self.assertGreater(train.candidate_rank(high_recall), train.candidate_rank(low_recall))
        high_precision = self.result(0.8, 0.9, 0.73)
        self.assertGreater(train.candidate_rank(high_precision), train.candidate_rank(high_recall))
        tied = self.result(0.8, 0.9, 0.73)
        self.assertIs(max([high_precision, tied], key=train.candidate_rank), high_precision)


class TrainingLifecycleTests(unittest.TestCase):
    def test_main_stops_spark_when_training_fails(self):
        args = SimpleNamespace(data_path=Path("data.csv"), run_dir=Path("new-run"),
                               search=True, seed=42, master="local[2]")
        spark = Mock()
        with patch.object(train, "parse_args", return_value=args), \
                patch.object(train, "create_spark_session", return_value=spark), \
                patch.object(train, "run_training", side_effect=RuntimeError("fit failed")) as run:
            with self.assertRaisesRegex(RuntimeError, "fit failed"):
                train.main()
        run.assert_called_once_with(spark, args.data_path, args.run_dir, search=True, seed=42)
        spark.stop.assert_called_once_with()

    def test_existing_run_is_rejected_without_replacing_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "existing-run"
            run_dir.mkdir()
            manifest = run_dir / "manifest.json"
            original = '{"status":"completed","model_uid":"keep-this-model"}\n'
            manifest.write_text(original)
            with patch.object(train, "load_data") as load_data:
                with self.assertRaises(FileExistsError):
                    train.run_training(Mock(), Path(directory) / "data.csv", run_dir)
            load_data.assert_not_called()
            self.assertEqual(manifest.read_text(), original)
            self.assertEqual(list(run_dir.iterdir()), [manifest])


if __name__ == "__main__":
    unittest.main()
