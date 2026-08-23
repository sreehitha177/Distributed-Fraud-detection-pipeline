#!/usr/bin/env python3
"""
Test script to verify the ML model can load and make predictions
"""
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
import time

def test_model():
    print("=" * 60)
    print("TESTING ML MODEL")
    print("=" * 60)
    
    # Initialize Spark
    print("\n1. Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("ModelTest") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session created")
    
    # Load the trained model
    print("\n2. Loading trained model...")
    model_path = "ml_model/trained_model"
    try:
        model = RandomForestClassificationModel.load(model_path)
        print(f"✓ Model loaded from {model_path}")
        print(f"  - Number of trees: {model.getNumTrees}")
        print(f"  - Feature importance available: {model.featureImportances is not None}")
    except Exception as e:
        print(f"✗ Failed to load model: {e}")
        return
    
    # Load test data
    print("\n3. Loading test data...")
    test_data_path = "ml_model/testdata.csv"
    try:
        test_data = spark.read.option("header", "true").csv(test_data_path, inferSchema=True)
        count = test_data.count()
        print(f"✓ Test data loaded: {count} records")
        
        # Show sample
        print("\n  Sample records:")
        test_data.select("Time", "Amount", "Class").show(5, truncate=False)
    except Exception as e:
        print(f"✗ Failed to load test data: {e}")
        return
    
    # Prepare features
    print("\n4. Preparing features...")
    input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    test_with_features = assembler.transform(test_data).select("features", "Class")
    print("✓ Features assembled")
    
    # Make predictions
    print("\n5. Running predictions...")
    start_time = time.time()
    predictions = model.transform(test_with_features)
    predictions.cache()  # Cache to force evaluation
    
    pred_count = predictions.count()
    end_time = time.time()
    
    inference_time = end_time - start_time
    throughput = pred_count / inference_time
    
    print(f"✓ Predictions completed")
    print(f"  - Total records: {pred_count}")
    print(f"  - Time taken: {inference_time:.2f} seconds")
    print(f"  - Throughput: {throughput:.2f} records/second")
    
    # Show prediction distribution
    print("\n6. Prediction Results:")
    predictions.groupBy("prediction").count().show()
    
    # Show some fraud predictions
    print("\n  Sample fraud predictions (prediction=1):")
    fraud_predictions = predictions.filter(predictions.prediction == 1.0)
    fraud_count = fraud_predictions.count()
    print(f"  Total fraud predictions: {fraud_count}")
    
    if fraud_count > 0:
        fraud_predictions.select("Class", "prediction", "probability").show(5, truncate=False)
    
    # Calculate accuracy metrics
    print("\n7. Basic Accuracy Metrics:")
    correct_predictions = predictions.filter(predictions.Class == predictions.prediction).count()
    accuracy = correct_predictions / pred_count
    print(f"  - Accuracy: {accuracy * 100:.2f}%")
    
    # Show confusion matrix
    print("\n8. Confusion Matrix:")
    predictions.crosstab("Class", "prediction").show()
    
    spark.stop()
    
    print("\n" + "=" * 60)
    print("✓ MODEL TEST COMPLETED SUCCESSFULLY")
    print("=" * 60)

if __name__ == "__main__":
    test_model()
