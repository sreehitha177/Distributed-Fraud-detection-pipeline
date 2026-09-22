"""Train reproducible Random Forest experiments using a fixed 64/16/20 split."""

import argparse
import hashlib
import json
import math
import platform
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NumericType

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "creditcard.csv"
# Legacy artifacts remain available to the standalone evaluation/tuning commands.
MODEL_PATH = BASE_DIR / "trained_model_v3"
VALIDATION_DATA_PATH = BASE_DIR / "validationdata_v3.parquet"
TEST_DATA_PATH = BASE_DIR / "testdata_v3.parquet"
RUNS_PATH = BASE_DIR / "runs"

FEATURE_COLUMNS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]
REQUIRED_COLUMNS = FEATURE_COLUMNS + ["Class"]
MODEL_SEED = 42
TEST_SPLIT_SEED = 42
VALIDATION_SPLIT_SEED = 43

# A bounded comparison, rather than a Cartesian grid of every combination.
CANDIDATES = [
    dict(name="baseline", num_trees=100, max_depth=5, min_instances_per_node=1, weighting="none"),
    dict(name="deeper", num_trees=100, max_depth=8, min_instances_per_node=1, weighting="none"),
    dict(name="larger_leaves", num_trees=100, max_depth=8, min_instances_per_node=5, weighting="none"),
    dict(name="more_trees", num_trees=200, max_depth=8, min_instances_per_node=5, weighting="none"),
    dict(name="balanced", num_trees=100, max_depth=8, min_instances_per_node=5, weighting="balanced"),
    dict(name="sqrt_balanced", num_trees=100, max_depth=8, min_instances_per_node=5, weighting="sqrt_balanced"),
]


def create_spark_session(master="local[*]"):
    return (SparkSession.builder.appName("CreditCardFraudDetection")
            .master(master).getOrCreate())


def check_schema(data, dataset_name="data"):
    missing = [name for name in REQUIRED_COLUMNS if name not in data.columns]
    if missing:
        raise ValueError(f"{dataset_name} is missing required columns: {missing}")
    nonnumeric = [name for name in REQUIRED_COLUMNS
                  if not isinstance(data.schema[name].dataType, NumericType)]
    if nonnumeric:
        raise ValueError(f"{dataset_name} has nonnumeric columns: {nonnumeric}")


def class_counts(data):
    return {int(row["Class"]): row["count"] for row in data.groupBy("Class").count().collect()}


def require_both_classes(counts, dataset_name):
    if set(counts) != {0, 1} or any(
            not isinstance(count, int) or isinstance(count, bool) or count <= 0
            for count in counts.values()):
        raise ValueError(f"{dataset_name} must contain both fraud (1) and legitimate (0) rows")


def validate_data(data, dataset_name="data", require_both_classes=True):
    """Reject bad values without changing row membership or numeric dtypes."""
    check_schema(data, dataset_name)
    invalid = ~F.col("Class").isin(0, 1)
    for name in REQUIRED_COLUMNS:
        value = F.col(name).cast("double")
        invalid = invalid | value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
    if data.filter(invalid).limit(1).count():
        raise ValueError(f"{dataset_name} contains missing/non-finite values or labels other than 0 and 1")
    if require_both_classes:
        counts = class_counts(data)
        if set(counts) != {0, 1} or any(count <= 0 for count in counts.values()):
            raise ValueError(f"{dataset_name} must contain both fraud (1) and legitimate (0) rows")
    return data


def load_data(spark, file_path):
    started = time.perf_counter()
    data = spark.read.option("header", True).option("inferSchema", True).csv(str(file_path))
    check_schema(data, "CSV")
    print(f"Loaded dataset from: {file_path} ({time.perf_counter() - started:.2f}s)", flush=True)
    return data


def clean_data(data):
    """Keep the existing missing-row policy; reject other malformed input."""
    check_schema(data)
    total_rows = data.count()
    cleaned = data.na.drop(subset=REQUIRED_COLUMNS)
    validate_data(cleaned, "Cleaned dataset")
    valid_rows = cleaned.count()
    print(f"Total rows: {total_rows}; valid: {valid_rows}; dropped missing: {total_rows - valid_rows}", flush=True)
    return cleaned


def prepare_features(data):
    return FEATURE_COLUMNS, data


def split_data(data):
    """Preserve the project's original nested split and its two fixed seeds."""
    development, test = data.randomSplit([0.8, 0.2], seed=TEST_SPLIT_SEED)
    training, validation = development.randomSplit([0.8, 0.2], seed=VALIDATION_SPLIT_SEED)
    return training, validation, test


def training_class_weights(counts, weighting):
    """Compute weights exclusively from the fitting split."""
    require_both_classes(counts, "Training data")
    if weighting not in {"none", "balanced", "sqrt_balanced"}:
        raise ValueError(f"Unknown weighting mode: {weighting}")
    if weighting == "none":
        return {0: 1.0, 1: 1.0}
    total = sum(counts.values())
    weights = {label: total / (2.0 * count) for label, count in counts.items()}
    if weighting == "sqrt_balanced":
        weights = {label: math.sqrt(weight) for label, weight in weights.items()}
    return weights


def train_random_forest(train_data, input_cols, *, seed=MODEL_SEED,
                        num_trees=100, max_depth=5, min_instances_per_node=1,
                        weighting="none", counts=None):
    """Fit a forest and retain its assembler in the returned PipelineModel."""
    assembler = VectorAssembler(inputCols=list(input_cols), outputCol="features")
    if counts is None:
        validate_data(train_data, "Training data")
        counts = class_counts(train_data)
    weights = training_class_weights(counts, weighting)
    if "features" in train_data.columns:
        processed = train_data.select("features", "Class")
    else:
        processed = assembler.transform(train_data).select("features", "Class")
    parameters = dict(labelCol="Class", featuresCol="features", numTrees=num_trees,
                      maxDepth=max_depth, minInstancesPerNode=min_instances_per_node, seed=seed)
    if weighting != "none":
        processed = processed.withColumn(
            "class_weight", F.when(F.col("Class") == 1, weights[1]).otherwise(weights[0]))
        parameters["weightCol"] = "class_weight"
    classifier = RandomForestClassifier(**parameters).fit(processed)
    return PipelineModel(stages=[assembler, classifier])


def save_model(model, model_path):
    """Write a new model; refuse to overwrite an existing experiment."""
    model.write().save(str(model_path))


def save_test_data(data, data_path):
    """Write a new split without overwriting existing artifacts."""
    data.write.mode("errorifexists").parquet(str(data_path))


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def dataset_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def candidate_rank(result):
    """Validation-only ranking; exact ties keep the first (simpler) candidate."""
    selected = result["selected"]
    return selected["f1"], selected["recall"], selected["precision"]


def run_training(spark, file_path, run_dir, *, search=False, seed=MODEL_SEED):
    """Persist one complete experiment without scoring the held-out test split."""
    # Local import avoids a cycle: the standalone tuner imports training constants.
    if __package__:
        from .tune_threshold import save_threshold_results, tune_threshold
    else:
        from tune_threshold import save_threshold_results, tune_threshold

    run_dir = Path(run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    configurations = [dict(candidate) for candidate in (CANDIDATES if search else CANDIDATES[:1])]
    manifest = {
        "status": "running",
        "run_id": run_dir.name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_path": str(Path(file_path).resolve()),
        "feature_columns": FEATURE_COLUMNS,
        "model_seed": seed,
        "split_seeds": {"test": TEST_SPLIT_SEED, "validation": VALIDATION_SPLIT_SEED},
        "split_fractions": {"train": 0.64, "validation": 0.16, "test": 0.20},
        "selection_metric": "validation_fraud_f1_at_validation_selected_threshold",
        "candidate_tie_breaking": ["higher recall", "higher precision", "earlier candidate"],
        "search": search,
        "candidate_configurations": configurations,
        "environment": {"python": platform.python_version(), "spark": spark.version,
                        "master": spark.sparkContext.master,
                        "input_max_partition_bytes": spark.conf.get("spark.sql.files.maxPartitionBytes")},
        "artifacts": {"model": "model", "training": "train.parquet", "validation": "validation.parquet",
                      "test": "test.parquet", "threshold": "threshold.json",
                      "threshold_metrics": "threshold_metrics.csv", "candidates": "candidates.json"},
    }
    cached = []
    try:
        write_json(run_dir / "manifest.json", manifest)
        manifest["dataset_sha256"] = dataset_sha256(file_path)
        source = load_data(spark, file_path).cache()
        cached.append(source)
        cleaned = clean_data(source).cache()
        cached.append(cleaned)
        training, validation, test = split_data(cleaned)
        splits = {"train": training.cache(), "validation": validation.cache(), "test": test.cache()}
        cached.extend(splits.values())
        manifest["splits"] = {}
        for name, frame in splits.items():
            counts = class_counts(frame)
            require_both_classes(counts, name)
            manifest["splits"][name] = {"rows": sum(counts.values()), "class_counts": counts}
            print(f"{name}: {manifest['splits'][name]}", flush=True)
            save_test_data(frame, run_dir / f"{name}.parquet")
        manifest["input_rows"] = source.count()
        manifest["dropped_missing_rows"] = manifest["input_rows"] - sum(
            split["rows"] for split in manifest["splits"].values())
        write_json(run_dir / "manifest.json", manifest)

        assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
        processed_training = assembler.transform(splits["train"]).select("features", "Class").cache()
        cached.append(processed_training)
        processed_training.count()
        counts = class_counts(splits["train"])
        best_model = best_result = best_metrics = None
        results = []
        for index, configuration in enumerate(configurations, start=1):
            print(f"\nTraining {index}/{len(configurations)}: {configuration['name']} {configuration}", flush=True)
            fit_started = time.perf_counter()
            parameters = {key: value for key, value in configuration.items() if key != "name"}
            model = train_random_forest(processed_training, FEATURE_COLUMNS, seed=seed, counts=counts, **parameters)
            fit_seconds = time.perf_counter() - fit_started
            selected, metrics = tune_threshold(model, splits["validation"])
            result = {
                "name": configuration["name"], "parameters": parameters, "seed": seed,
                "model_uid": model.stages[-1].uid, "class_weights": training_class_weights(counts, configuration["weighting"]),
                "fit_seconds": fit_seconds, "selected": selected,
                "baseline": next(row for row in metrics if row["threshold"] == 0.5),
                "threshold_candidate_count": len(metrics),
            }
            results.append(result)
            write_json(run_dir / "candidates.json", results)
            print(f"Validation: F1={selected['f1']:.4f}, precision={selected['precision']:.4f}, "
                  f"recall={selected['recall']:.4f}, threshold={selected['threshold']:.12g}", flush=True)
            if best_result is None or candidate_rank(result) > candidate_rank(best_result):
                best_model, best_result, best_metrics = model, result, metrics

        save_model(best_model, run_dir / "model")
        save_threshold_results(
            best_model, best_result["selected"], best_metrics, run_dir / "model",
            run_dir / "validation.parquet", run_dir / "threshold.json", run_dir / "threshold_metrics.csv")
        classifier = best_model.stages[-1]
        manifest.update(
            status="completed", selected_candidate=best_result["name"], model_uid=classifier.uid,
            pipeline_uid=best_model.uid, selected=best_result["selected"],
            effective_model_parameters={param.name: value for param, value in classifier.extractParamMap().items()},
            completed_at=datetime.now(timezone.utc).isoformat(),
            elapsed_seconds=time.perf_counter() - started,
        )
        # Publish completion only after every required artifact has been written.
        write_json(run_dir / "manifest.json", manifest)
        print(f"\nSelected: {best_result['name']} (validation F1={best_result['selected']['f1']:.4f})", flush=True)
        print(f"Experiment saved to: {run_dir}", flush=True)
        print(f"Final test evaluation: python3 ml_model/evaluate.py --run-dir {run_dir}", flush=True)
        return manifest
    except Exception as error:
        manifest.update(status="failed", error=f"{type(error).__name__}: {error}")
        write_json(run_dir / "manifest.json", manifest)
        raise
    finally:
        for frame in reversed(cached):
            frame.unpersist()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-path", type=Path, default=DATA_PATH)
    parser.add_argument("--run-dir", type=Path, help="New experiment directory; existing paths are refused")
    parser.add_argument("--search", action="store_true", help="Compare six configurations (default: one baseline)")
    parser.add_argument("--seed", type=int, default=MODEL_SEED, help="Random Forest seed; split seeds remain 42 and 43")
    parser.add_argument("--master", default="local[*]", help="Spark master, e.g. local[4]")
    args = parser.parse_args()
    if not args.data_path.is_file():
        parser.error(f"Dataset does not exist: {args.data_path}")
    if args.run_dir is not None and args.run_dir.exists():
        parser.error(f"Experiment directory already exists: {args.run_dir}")
    if not -(2 ** 63) <= args.seed < 2 ** 63:
        parser.error("--seed must fit in a signed 64-bit integer")
    return args


def main():
    args = parse_args()
    run_dir = args.run_dir or RUNS_PATH / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8])
    spark = create_spark_session(args.master)
    try:
        run_training(spark, args.data_path, run_dir, search=args.search, seed=args.seed)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
