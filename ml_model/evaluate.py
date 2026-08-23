from pathlib import Path

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

from train import create_spark_session


BASE_DIR = Path(__file__).resolve().parent

MODEL_PATH = BASE_DIR / "trained_model_v3"
TEST_DATA_PATH = BASE_DIR / "testdata_v3.parquet"

FEATURE_COLUMNS = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

THRESHOLD = 0.50


def load_model(model_path):
    """Load the trained Random Forest model."""

    model = RandomForestClassificationModel.load(str(model_path))

    print(f"Model loaded from {model_path}")

    return model


def load_test_data(test_data_path, spark):
    """Load and prepare test data."""

    test_data = spark.read.parquet(str(test_data_path))

    print(f"Test data loaded from {test_data_path}")

    columns_to_cast = (
            [f"V{i}" for i in range(1, 29)]
            + ["Time", "Amount", "Class"]
    )

    for column_name in columns_to_cast:
        test_data = test_data.withColumn(
            column_name,
            col(column_name).cast(DoubleType())
        )

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

    assembler = VectorAssembler(
        inputCols=input_cols,
        outputCol="features"
    )

    processed_test_data = (
        assembler
        .transform(test_data)
        .select("features", "Class")
    )

    predictions = model.transform(processed_test_data)

    # Extract fraud probability:
    # probability[0] = legitimate
    # probability[1] = fraud
    predictions = predictions.withColumn(
        "fraud_probability",
        vector_to_array("probability")[1]
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
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )

    auprc = auprc_evaluator.evaluate(predictions)

    # -----------------------
    # Display results
    # -----------------------

    print("\nFINAL TEST EVALUATION")
    print("---------------------")

    print(f"Threshold: {threshold:.2f}")

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


def main():

    spark = create_spark_session()

    try:

        model = load_model(MODEL_PATH)

        test_data = load_test_data(
            TEST_DATA_PATH,
            spark
        )

        evaluate_model(
            model=model,
            test_data=test_data,
            input_cols=FEATURE_COLUMNS,
            threshold=THRESHOLD
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()