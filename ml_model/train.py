# train.py
# This file contains functions to train a machine learning model for credit card fraud detection using 
# Apache Spark. It includes functions to load, clean, and prepare data for training, as well as to train 
# and save a Random Forest Classifier model. The script orchestrates the end-to-end process of model 
# training, from data loading to saving the trained model and test data. Additionally, it provides 
# time tracking for each step to help evaluate performance.

from pathlib import Path
import time

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

BASE_DIR = Path(__file__).resolve().parent

DATA_PATH = BASE_DIR / "creditcard.csv"
MODEL_PATH = BASE_DIR / "trained_model_v3"
VALIDATION_DATA_PATH = BASE_DIR / "validationdata_v3.parquet"
TEST_DATA_PATH = BASE_DIR / "testdata_v3.parquet"

FEATURE_COLUMNS = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]

REQUIRED_COLUMNS = (
        ["Time"]
        + [f"V{i}" for i in range(1, 29)]
        + ["Amount", "Class"]
)


# Function to create a Spark session
def create_spark_session():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
        .appName("CreditCardFraudDetection")
        .master("local[*]")
        .getOrCreate())

# Function to load dataset from a CSV file into a Spark DataFrame
def load_data(spark, file_path):
    """Load the credit-card dataset and validate its schema."""

    start_time = time.time()

    data = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(file_path))
    )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Dataset is missing required columns: {missing_columns}"
        )

    print(f"Loaded dataset from: {file_path}")
    print(f"Data loading time: {time.time() - start_time:.2f} seconds")

    return data

# Function to clean data by removing rows with missing values
def clean_data(data):
    """Reject rows containing missing required values."""

    start_time = time.time()

    total_rows = data.count()

    cleaned_data = data.na.drop(
        subset=REQUIRED_COLUMNS
    )

    valid_rows = cleaned_data.count()
    rejected_rows = total_rows - valid_rows

    print(f"Total rows: {total_rows}")
    print(f"Valid rows: {valid_rows}")
    print(f"Rejected rows: {rejected_rows}")
    print(
        f"Data cleaning time: "
        f"{time.time() - start_time:.2f} seconds"
    )

    return cleaned_data

# Function to prepare feature columns for model training
def prepare_features(data):
    """Prepare features for training."""
    return FEATURE_COLUMNS, data
    #start_time = time.time()  # Start timing
    # input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    # end_time = time.time()  # End timing
    #
    # print(f"Feature preparation time: {end_time - start_time} seconds")
    #
    # return input_cols, data

# Function to train a Random Forest model using the prepared training data
def train_random_forest(train_data, input_cols):
    """Train Random Forest Classifier."""
    start_time = time.time()  # Start timing
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    processed_train_data = assembler.transform(train_data).select("features", "Class")

    rf = RandomForestClassifier(labelCol="Class", featuresCol="features", numTrees=100)
    model = rf.fit(processed_train_data)
    
    end_time = time.time()  # End timing
    print(f"Model training time: {end_time - start_time} seconds")
    
    return model

# Function to orchestrate the training process: load data, clean it, prepare features, and train the model
def train(file_path):
    """Orchestrate the training and evaluation process."""
    # Create Spark session
    spark = create_spark_session()

    # Load and clean data
    data = load_data(spark, file_path)
    data = clean_data(data)

    # Prepare features for training
    input_cols, processed_data = prepare_features(data)

    # Split data into train and test sets (80% training, 20% testing)
    train_data, test_data = processed_data.randomSplit([0.8, 0.2], seed=42)
    # Split training data into train and validations sets for threshold training(64% training, 16% validation, 20% testing)
    model_train_data, validation_data = train_data.randomSplit([0.8, 0.2], seed=43)

    print("\nModel training distribution:")
    model_train_data.groupBy("Class").count().orderBy("Class").show()

    print("\nValidation distribution:")
    validation_data.groupBy("Class").count().orderBy("Class").show()

    print("\nTest distribution:")
    test_data.groupBy("Class").count().orderBy("Class").show()

    # Train the model
    model = train_random_forest(model_train_data, input_cols)

    return model, validation_data, test_data

# Function to save the trained model to the specified path
def save_model(model, model_path):
    """Save the trained model, replacing any previous model at this path."""
    start_time = time.time()

    model.write().overwrite().save(str(model_path))

    print(
        f"Model saved to: {model_path}"
    )
    print(
        f"Model saving time: "
        f"{time.time() - start_time:.2f} seconds"
    )

# Function to save the test data to a specified Parquet path
def save_test_data(test_data, test_data_path):
    """Save split data as Parquet, replacing the previous output at this path."""
    start_time = time.time()

    test_data.write.mode("overwrite").parquet(str(test_data_path))

    print(
        f"Test data saved to: {test_data_path}"
    )
    print(
        f"Test data saving time: "
        f"{time.time() - start_time:.2f} seconds"
    )

# Main function to train the model and save the trained model and test data
def main():
    """Main function to orchestrate the training, evaluation, and saving of the model and test data."""

    # Train the model 
    model, validation_data, test_data = train(DATA_PATH)
    
    # Save the model
    save_model(model, MODEL_PATH)

    # Save validation & test data
    save_test_data(validation_data, VALIDATION_DATA_PATH)
    save_test_data(test_data, TEST_DATA_PATH)

if __name__ == "__main__":
    main()
