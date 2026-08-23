from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array


BASE_DIR = Path(__file__).resolve().parent

MODEL_PATH = BASE_DIR / "trained_model_v3"
VALIDATION_DATA_PATH = BASE_DIR / "validationdata_v3.parquet"

FEATURE_COLUMNS = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("FraudThresholdTuning")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    try:
        model = RandomForestClassificationModel.load(str(MODEL_PATH))
        validation_data = spark.read.parquet(str(VALIDATION_DATA_PATH))

        assembler = VectorAssembler(
            inputCols=FEATURE_COLUMNS,
            outputCol="features"
        )

        prepared = assembler.transform(validation_data)

        predictions = model.transform(prepared)

        # probability = [P(legitimate), P(fraud)]
        scored = predictions.withColumn(
            "fraud_probability",
            vector_to_array("probability")[1]
        ).cache()

        thresholds = [
            0.10,
            0.20,
            0.30,
            0.40,
            0.50,
            0.60,
            0.70,
            0.80,
            0.90
        ]

        print("\nThreshold tuning on VALIDATION SET")
        print(
            f"{'Threshold':<12}"
            f"{'TP':<8}"
            f"{'FP':<8}"
            f"{'FN':<8}"
            f"{'TN':<8}"
            f"{'Precision':<12}"
            f"{'Recall':<12}"
            f"{'F1':<12}"
        )

        for threshold in thresholds:

            thresholded = scored.withColumn(
                "custom_prediction",
                F.when(
                    F.col("fraud_probability") >= threshold,
                    1.0
                ).otherwise(0.0)
            )

            counts = (
                thresholded
                .groupBy("Class", "custom_prediction")
                .count()
                .collect()
            )

            confusion = {
                (row["Class"], row["custom_prediction"]): row["count"]
                for row in counts
            }

            tn = confusion.get((0.0, 0.0), 0)
            fp = confusion.get((0.0, 1.0), 0)
            fn = confusion.get((1.0, 0.0), 0)
            tp = confusion.get((1.0, 1.0), 0)

            precision = tp / (tp + fp) if tp + fp else 0
            recall = tp / (tp + fn) if tp + fn else 0

            f1 = (
                2 * precision * recall / (precision + recall)
                if precision + recall
                else 0
            )

            print(
                f"{threshold:<12.2f}"
                f"{tp:<8}"
                f"{fp:<8}"
                f"{fn:<8}"
                f"{tn:<8}"
                f"{precision:<12.4f}"
                f"{recall:<12.4f}"
                f"{f1:<12.4f}"
            )

        scored.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()