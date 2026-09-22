"""Threshold math and artifact checks that do not start a Spark session."""

import json
import tempfile
import unittest
from pathlib import Path

from ml_model.tune_threshold import (
    compute_threshold_metrics,
    load_selected_threshold,
    select_threshold,
)


def brute_force_metrics(examples, threshold):
    """Compute a confusion matrix directly from individual labeled scores."""
    tp = sum(label == 1 and score >= threshold for score, label in examples)
    fp = sum(label == 0 and score >= threshold for score, label in examples)
    fn = sum(label == 1 and score < threshold for score, label in examples)
    tn = sum(label == 0 and score < threshold for score, label in examples)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn)
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return dict(tp=tp, fp=fp, fn=fn, tn=tn, precision=precision, recall=recall, f1=f1)


class ThresholdMetricsTests(unittest.TestCase):
    def test_sweep_matches_brute_force_with_tied_scores_and_endpoints(self):
        # Duplicate groups exercise merging; endpoint scores exercise inclusive >=.
        groups = [(0.5, 1, 2), (1.0, 1, 1), (0.0, 1, 2),
                  (0.75, 2, 1), (0.5, 2, 1), (0.25, 0, 3)]
        examples = [
            (score, label)
            for score, positives, negatives in groups
            for label, count in ((1, positives), (0, negatives))
            for _ in range(count)
        ]
        metrics = compute_threshold_metrics(iter(groups))
        self.assertEqual({row["threshold"] for row in metrics}, {0, 0.25, 0.5, 0.75, 1})
        for row in metrics:
            with self.subTest(threshold=row["threshold"]):
                expected = brute_force_metrics(examples, row["threshold"])
                for name, value in expected.items():
                    self.assertAlmostEqual(row[name], value)
                self.assertEqual(sum(row[name] for name in ("tp", "fp", "fn", "tn")), len(examples))

    def test_optimum_between_old_coarse_grid_cutoffs(self):
        groups = [(0.35, 2, 0), (0.34, 0, 10), (0.1, 0, 10)]
        metrics = compute_threshold_metrics(groups)
        selected = select_threshold(metrics)
        self.assertEqual(selected["threshold"], 0.35)
        self.assertEqual(selected["f1"], 1.0)
        examples = [(0.35, 1)] * 2 + [(0.34, 0)] * 10 + [(0.1, 0)] * 10
        coarse_best = max(brute_force_metrics(examples, i / 10)["f1"] for i in range(1, 10))
        self.assertGreater(selected["f1"], coarse_best)

    def test_no_predictions_has_zero_precision_recall_and_f1(self):
        metrics = compute_threshold_metrics([(0.8, 1, 0), (0.2, 0, 2)])
        at_one = next(row for row in metrics if row["threshold"] == 1.0)
        self.assertEqual((at_one["tp"], at_one["fp"], at_one["fn"], at_one["tn"]), (0, 0, 1, 2))
        self.assertEqual((at_one["precision"], at_one["recall"], at_one["f1"]), (0.0, 0.0, 0.0))

    def test_single_tied_score_includes_all_examples_at_equality(self):
        metrics = compute_threshold_metrics([(0.5, 2, 3)])
        at_half = next(row for row in metrics if row["threshold"] == 0.5)
        self.assertEqual((at_half["tp"], at_half["fp"], at_half["fn"], at_half["tn"]), (2, 3, 0, 0))
        self.assertEqual(select_threshold(metrics)["threshold"], 0.5)

    def test_f1_tie_prefers_higher_recall(self):
        metrics = compute_threshold_metrics([(0.9, 1, 0), (0.4, 2, 6), (0.1, 0, 10)])
        selected = select_threshold(metrics)
        self.assertEqual(selected["f1"], 0.5)
        self.assertEqual(selected["recall"], 1.0)
        self.assertEqual(selected["threshold"], 0.4)
        self.assertEqual(select_threshold(list(reversed(metrics))), selected)

    def test_identical_metrics_prefer_higher_threshold(self):
        metrics = compute_threshold_metrics([(0.8, 2, 0), (0.2, 0, 3)])
        selected = select_threshold(metrics)
        self.assertEqual(selected["threshold"], 0.8)
        self.assertEqual(select_threshold(list(reversed(metrics))), selected)

    def test_rejects_invalid_probabilities(self):
        for probability in (None, float("nan"), float("inf"), -float("inf"), -0.01, 1.01):
            with self.subTest(probability=probability):
                with self.assertRaises(ValueError):
                    compute_threshold_metrics([(probability, 1, 1)])

    def test_rejects_empty_and_single_class_validation(self):
        for groups in ([], [(0.2, 0, 0)], [(0.8, 2, 0)], [(0.2, 0, 2)]):
            with self.subTest(groups=groups):
                with self.assertRaisesRegex(ValueError, "both fraud"):
                    compute_threshold_metrics(groups)

    def test_rejects_invalid_counts(self):
        for positives, negatives in ((-1, 2), (1, -2), (0.5, 2), (1, 2.5)):
            with self.subTest(positives=positives, negatives=negatives):
                with self.assertRaisesRegex(ValueError, "nonnegative integers"):
                    compute_threshold_metrics([(0.5, positives, negatives)])

    def test_rejects_empty_candidate_list(self):
        with self.assertRaisesRegex(ValueError, "No candidate"):
            select_threshold([])


class ThresholdArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "threshold.json"
        self.feature_columns = ["Time", "V1", "Amount"]
        self.model_uid = "RandomForestClassifier_test"
        self.artifact = {
            "model_uid": self.model_uid,
            "feature_columns": self.feature_columns,
            "comparison": "fraud_probability >= threshold",
            "objective": "fraud_f1",
            "selected": select_threshold(compute_threshold_metrics([(0.35, 2, 0), (0.1, 0, 4)])),
        }

    def write_artifact(self):
        self.path.write_text(json.dumps(self.artifact))

    def load_artifact(self):
        return load_selected_threshold(self.path, self.model_uid, self.feature_columns)

    def test_selected_threshold_round_trip_preserves_precision(self):
        self.artifact["selected"]["threshold"] = 0.351234567890123
        self.write_artifact()
        self.assertEqual(self.load_artifact(), 0.351234567890123)
        self.assertEqual(load_selected_threshold(self.path, self.model_uid, tuple(self.feature_columns)), 0.351234567890123)

    def test_rejects_different_model(self):
        self.artifact["model_uid"] = "RandomForestClassifier_other"
        self.write_artifact()
        with self.assertRaisesRegex(ValueError, "different model"):
            self.load_artifact()

    def test_rejects_reordered_features(self):
        self.artifact["feature_columns"] = list(reversed(self.feature_columns))
        self.write_artifact()
        with self.assertRaisesRegex(ValueError, "different feature columns"):
            self.load_artifact()

    def test_rejects_nonfinite_or_out_of_range_saved_threshold(self):
        for threshold in (float("nan"), float("inf"), -float("inf"), -0.1, 1.1):
            with self.subTest(threshold=threshold):
                self.artifact["selected"]["threshold"] = threshold
                self.write_artifact()
                with self.assertRaisesRegex(ValueError, "finite and between"):
                    self.load_artifact()

    def test_accepts_endpoint_thresholds(self):
        for threshold in (0.0, 1.0):
            with self.subTest(threshold=threshold):
                self.artifact["selected"]["threshold"] = threshold
                self.write_artifact()
                self.assertEqual(self.load_artifact(), threshold)


if __name__ == "__main__":
    unittest.main()
