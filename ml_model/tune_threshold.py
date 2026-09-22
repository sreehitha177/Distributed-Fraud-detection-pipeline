"""Select and save a fraud threshold using only the saved validation split."""

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

if __package__:
    from .model_utils import (
        get_classifier, get_feature_columns, load_fraud_model, load_run_manifest,
        transform_model, validate_run_model,
    )
    from .train import BASE_DIR, FEATURE_COLUMNS, MODEL_PATH, VALIDATION_DATA_PATH, create_spark_session, validate_data, write_json
else:
    from model_utils import (
        get_classifier, get_feature_columns, load_fraud_model, load_run_manifest,
        transform_model, validate_run_model,
    )
    from train import BASE_DIR, FEATURE_COLUMNS, MODEL_PATH, VALIDATION_DATA_PATH, create_spark_session, validate_data, write_json


THRESHOLD_PATH = BASE_DIR / "threshold_v3.json"
METRICS_PATH = BASE_DIR / "threshold_metrics_v3.csv"


def compute_threshold_metrics(score_counts):
    """Sweep (probability, fraud_count, legitimate_count) groups using score >= threshold.

    Every distinct score is a candidate, with ties treated as a single group.
    Include 0, 0.5 and 1 to cover the endpoints and the default baseline.
    Only aggregated score counts are collected from Spark, never feature rows.
    """
    groups = {}
    for probability, positives, negatives in score_counts:
        if probability is None or not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("Fraud probabilities must be finite numbers between 0 and 1")
        if any(not isinstance(count, int) or count < 0 for count in (positives, negatives)):
            raise ValueError("Class counts must be nonnegative integers")
        previous = groups.get(probability, (0, 0))
        groups[probability] = (previous[0] + positives, previous[1] + negatives)

    total_positive = sum(counts[0] for counts in groups.values())
    total_negative = sum(counts[1] for counts in groups.values())
    if not total_positive or not total_negative:
        raise ValueError("Validation data must contain both fraud (1) and legitimate (0) rows")

    results = []
    tp = fp = 0
    for threshold in sorted(set(groups) | {0.0, 0.5, 1.0}, reverse=True):
        positives, negatives = groups.get(threshold, (0, 0))
        tp += positives
        fp += negatives
        fn, tn = total_positive - tp, total_negative - fp
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / total_positive
        f1 = 2 * tp / (2 * tp + fp + fn)
        results.append(dict(threshold=threshold, tp=tp, fp=fp, fn=fn, tn=tn,
                            precision=precision, recall=recall, f1=f1))
    return results


def select_threshold(metrics):
    """Maximize fraud F1; break ties by recall, precision, then higher threshold."""
    if not metrics:
        raise ValueError("No candidate thresholds to select")
    return max(metrics, key=lambda row: (row["f1"], row["recall"], row["precision"], row["threshold"]))


def load_selected_threshold(path, model_uid, feature_columns):
    """Reject a threshold saved for a different model or feature ordering."""
    with Path(path).open() as handle:
        result = json.load(handle)
    if result["model_uid"] != model_uid:
        raise ValueError("Saved threshold belongs to a different model; rerun threshold tuning")
    if result["feature_columns"] != list(feature_columns):
        raise ValueError("Saved threshold uses different feature columns; rerun threshold tuning")
    threshold = float(result["selected"]["threshold"])
    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("Saved threshold must be finite and between 0 and 1")
    return threshold


def score_validation_data(model, validation_data):
    """Validate inputs and score with exactly the same feature order as training."""
    feature_columns = get_feature_columns(model, FEATURE_COLUMNS)
    prepared = validate_data(validation_data, dataset_name="Validation data")
    predictions = transform_model(model, prepared.select(*feature_columns, "Class"), feature_columns)
    return predictions.select(
        "Class", vector_to_array(get_classifier(model).getProbabilityCol())[1].alias("fraud_probability")
    )


def tune_threshold(model, validation_data):
    scored = score_validation_data(model, validation_data)
    counts = scored.groupBy("fraud_probability").agg(
        F.sum(F.when(F.col("Class") == 1.0, 1).otherwise(0)).alias("positives"),
        F.sum(F.when(F.col("Class") == 0.0, 1).otherwise(0)).alias("negatives"),
    ).collect()
    metrics = compute_threshold_metrics(
        [(row["fraud_probability"], row["positives"], row["negatives"]) for row in counts]
    )
    return select_threshold(metrics), metrics


def save_threshold_results(model, selected, metrics, model_path, validation_data_path,
                           output, metrics_output):
    """Write the selected threshold and its complete validation sweep."""
    output, metrics_output = Path(output), Path(metrics_output)
    baseline = next(row for row in metrics if row["threshold"] == 0.5)
    result = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model_path": str(Path(model_path).resolve()),
        "model_uid": get_classifier(model).uid,
        "validation_data_path": str(Path(validation_data_path).resolve()),
        "feature_columns": get_feature_columns(model, FEATURE_COLUMNS),
        "comparison": "fraud_probability >= threshold",
        "objective": "fraud_f1",
        "tie_breaking": ["higher recall", "higher precision", "higher threshold"],
        "validation_rows": sum(selected[name] for name in ("tp", "fp", "fn", "tn")),
        "candidate_count": len(metrics),
        "baseline": baseline,
        "selected": selected,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    metrics_output.parent.mkdir(parents=True, exist_ok=True)
    temporary_metrics = metrics_output.with_name(metrics_output.name + ".tmp")
    with temporary_metrics.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(selected))
        writer.writeheader()
        writer.writerows(sorted(metrics, key=lambda row: row["threshold"]))
    temporary_metrics.replace(metrics_output)
    write_json(output, result)
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, help="Completed training run containing model and validation.parquet")
    parser.add_argument("--model-path", type=Path, help=f"Legacy model path (default: {MODEL_PATH})")
    parser.add_argument("--validation-data-path", type=Path, help=f"Legacy validation path (default: {VALIDATION_DATA_PATH})")
    parser.add_argument("--output", type=Path, help="Selected threshold JSON")
    parser.add_argument("--metrics-output", type=Path, help="All candidate metrics CSV")
    parser.add_argument("--master", default="local[*]", help="Spark master, for example local[2]")
    args = parser.parse_args(argv)
    paths = (args.model_path, args.validation_data_path, args.output, args.metrics_output)
    if args.run_dir is not None:
        if any(path is not None for path in paths):
            parser.error("--run-dir cannot be combined with individual input or output paths")
        args.model_path = args.run_dir / "model"
        args.validation_data_path = args.run_dir / "validation.parquet"
        args.output = args.run_dir / "threshold.json"
        args.metrics_output = args.run_dir / "threshold_metrics.csv"
    else:
        args.model_path = args.model_path if args.model_path is not None else MODEL_PATH
        args.validation_data_path = args.validation_data_path if args.validation_data_path is not None else VALIDATION_DATA_PATH
        args.output = args.output if args.output is not None else THRESHOLD_PATH
        args.metrics_output = args.metrics_output if args.metrics_output is not None else METRICS_PATH
    return args


def main():
    args = parse_args()
    manifest = load_run_manifest(args.run_dir) if args.run_dir is not None else None
    spark = create_spark_session(master=args.master)
    try:
        model = load_fraud_model(args.model_path)
        if manifest is not None:
            validate_run_model(manifest, model, FEATURE_COLUMNS)
        validation_data = spark.read.parquet(str(args.validation_data_path))
        selected, metrics = tune_threshold(model, validation_data)
        result = save_threshold_results(
            model, selected, metrics, args.model_path, args.validation_data_path,
            args.output, args.metrics_output,
        )
        baseline = result["baseline"]
        if manifest is not None:
            manifest["selected"] = selected
            manifest["threshold_updated_at"] = result["created_at"]
            write_json(args.run_dir / "manifest.json", manifest)

        print(f"\nTHRESHOLD TUNING — VALIDATION SET ({result['validation_rows']} rows)")
        print(f"Objective: fraud F1; candidates: {len(metrics)}")
        print(f"{'Choice':<10} {'Threshold':>12} {'TP':>6} {'FP':>6} {'FN':>6} {'TN':>7} "
              f"{'Precision':>10} {'Recall':>10} {'F1':>10}")
        for name, row in (("Baseline", baseline), ("Selected", selected)):
            print(f"{name:<10} {row['threshold']:>12.8f} {row['tp']:>6} {row['fp']:>6} "
                  f"{row['fn']:>6} {row['tn']:>7} {row['precision']:>10.4f} "
                  f"{row['recall']:>10.4f} {row['f1']:>10.4f}")
        print(f"\nSelected threshold saved to: {args.output}")
        print(f"All candidate metrics saved to: {args.metrics_output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
