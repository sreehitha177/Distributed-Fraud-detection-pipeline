import argparse
import math
from pathlib import Path

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

if __package__:
    from .model_utils import (
        get_classifier, get_feature_columns, load_fraud_model, load_run_manifest,
        transform_model, validate_run_model,
    )
    from .train import FEATURE_COLUMNS, create_spark_session, validate_data
    from .tune_threshold import THRESHOLD_PATH, load_selected_threshold
else:
    from model_utils import (
        get_classifier, get_feature_columns, load_fraud_model, load_run_manifest,
        transform_model, validate_run_model,
    )
    from train import FEATURE_COLUMNS, create_spark_session, validate_data
    from tune_threshold import THRESHOLD_PATH, load_selected_threshold


BASE_DIR = Path(__file__).resolve().parent

MODEL_PATH = BASE_DIR / "trained_model_v3"
TEST_DATA_PATH = BASE_DIR / "testdata_v3.parquet"

THRESHOLD = 0.50


def load_model(model_path):
    """Load a legacy forest or its saved preprocessing pipeline."""

    model = load_fraud_model(model_path)

    print(f"Model loaded from {model_path}")

    return model


def load_test_data(test_data_path, spark):
    """Load the saved test split without changing its numeric schema."""

    test_data = spark.read.parquet(str(test_data_path))

    print(f"Test data loaded from {test_data_path}")

    return test_data


def evaluate_model(
        model,
        test_data,
        input_cols,
        threshold=0.5
):
    """
    Evaluate the model using a custom fraud threshold.

    AUPRC is threshold-independent.

    Precision, recall, F1 and the confusion matrix are calculated
    using the provided threshold.
    """

    if not math.isfinite(threshold) or not 0 <= threshold <= 1:
        raise ValueError("Threshold must be finite and between 0 and 1")
    validated = validate_data(test_data, dataset_name="Test data")
    classifier = get_classifier(model)
    predictions = transform_model(model, validated.select(*input_cols, "Class"), input_cols)

    # Extract fraud probability:
    # probability[0] = legitimate
    # probability[1] = fraud
    predictions = predictions.withColumn(
        "fraud_probability",
        vector_to_array(classifier.getProbabilityCol())[1]
    )

    # Apply our custom threshold
    predictions = predictions.withColumn(
        "final_prediction",
        F.when(
            F.col("fraud_probability") >= threshold,
            1.0
        ).otherwise(0.0)
    )

    # -----------------------
    # Confusion matrix
    # -----------------------

    counts = (
        predictions
        .groupBy("Class", "final_prediction")
        .count()
        .collect()
    )

    confusion = {
        (row["Class"], row["final_prediction"]): row["count"]
        for row in counts
    }

    tn = confusion.get((0.0, 0.0), 0)
    fp = confusion.get((0.0, 1.0), 0)
    fn = confusion.get((1.0, 0.0), 0)
    tp = confusion.get((1.0, 1.0), 0)

    # -----------------------
    # Fraud metrics
    # -----------------------

    precision = (
        tp / (tp + fp)
        if (tp + fp) > 0
        else 0.0
    )

    recall = (
        tp / (tp + fn)
        if (tp + fn) > 0
        else 0.0
    )

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    # -----------------------
    # AUPRC
    # -----------------------

    auprc_evaluator = BinaryClassificationEvaluator(
        labelCol="Class",
        rawPredictionCol=classifier.getRawPredictionCol(),
        metricName="areaUnderPR"
    )

    auprc = auprc_evaluator.evaluate(predictions)

    # -----------------------
    # Display results
    # -----------------------

    print("\nFINAL TEST EVALUATION")
    print("---------------------")

    print(f"Threshold: {threshold:.12g}")

    print("\nConfusion Matrix")
    print("----------------")
    print(f"True Negatives  (TN): {tn}")
    print(f"False Positives (FP): {fp}")
    print(f"False Negatives (FN): {fn}")
    print(f"True Positives  (TP): {tp}")

    print("\nFraud Metrics")
    print("-------------")
    print(f"AUPRC:           {auprc:.4f}")
    print(f"Fraud Precision: {precision:.4f}")
    print(f"Fraud Recall:    {recall:.4f}")
    print(f"Fraud F1:        {f1:.4f}")

    return auprc, precision, recall, f1


def evaluate_incrementally(
        model,
        test_data,
        input_cols,
        size_list,
        threshold=0.5
):
    """
    Evaluate progressively larger subsets.

    This is retained from the original project, but should not be
    treated as the primary ML evaluation because the fraud class is
    extremely imbalanced.
    """

    for subset_size in size_list:

        subset = test_data.limit(subset_size)

        actual_size = subset.count()

        print(
            f"\nEvaluating subset with "
            f"{actual_size} records..."
        )

        auprc, precision, recall, f1 = evaluate_model(
            model,
            subset,
            input_cols,
            threshold
        )

        print(
            f"\nResults for subset size "
            f"{subset_size}:"
        )

        print(f"AUPRC:    {auprc:.4f}")
        print(f"Precision:{precision:.4f}")
        print(f"Recall:   {recall:.4f}")
        print(f"F1:       {f1:.4f}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Evaluate the saved model with its validation-selected threshold.")
    parser.add_argument("--run-dir", type=Path, help="Completed training run containing model, test.parquet and threshold.json")
    parser.add_argument("--threshold", type=float, help="Explicit override; otherwise use saved threshold (legacy fallback: 0.5)")
    parser.add_argument("--threshold-file", type=Path, help="Load a specific threshold JSON (default: threshold_v3.json)")
    parser.add_argument("--master", default="local[*]", help="Spark master, for example local[2]")
    args = parser.parse_args(argv)
    if args.threshold is not None and (not math.isfinite(args.threshold) or not 0 <= args.threshold <= 1):
        parser.error("--threshold must be finite and between 0 and 1")
    if args.threshold is not None and args.threshold_file is not None:
        parser.error("Use either --threshold or --threshold-file")
    args.model_path = args.run_dir / "model" if args.run_dir is not None else MODEL_PATH
    args.test_data_path = args.run_dir / "test.parquet" if args.run_dir is not None else TEST_DATA_PATH
    args.threshold_path = (
        args.threshold_file if args.threshold_file is not None
        else args.run_dir / "threshold.json" if args.run_dir is not None
        else THRESHOLD_PATH
    )
    return args


def main():
    args = parse_args()
    manifest = load_run_manifest(args.run_dir) if args.run_dir is not None else None
    if (args.threshold is None
            and (args.run_dir is not None or args.threshold_file is not None)
            and not args.threshold_path.is_file()):
        raise ValueError(f"Selected threshold file is missing: {args.threshold_path}")
    spark = create_spark_session(master=args.master)

    try:

        model = load_model(args.model_path)
        feature_columns = get_feature_columns(model, FEATURE_COLUMNS)
        if manifest is not None:
            feature_columns = validate_run_model(manifest, model, FEATURE_COLUMNS)

        threshold_path = args.threshold_path
        if args.threshold is not None:
            threshold = args.threshold
            print("Using explicit threshold override")
        elif args.threshold_file is not None or threshold_path.exists():
            threshold = load_selected_threshold(threshold_path, get_classifier(model).uid, feature_columns)
            print(f"Using validation-selected threshold from {threshold_path}")
        else:
            threshold = THRESHOLD
            print(f"No saved threshold found; using default {THRESHOLD}. Run tune_threshold.py to tune on validation data.")

        test_data = load_test_data(
            args.test_data_path,
            spark
        )

        evaluate_model(
            model=model,
            test_data=test_data,
            input_cols=feature_columns,
            threshold=threshold
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
