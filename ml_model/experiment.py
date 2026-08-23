# from train import create_spark_session
# from evaluate import load_model, load_test_data, evaluate_model
# from pyspark.sql import functions as F
# import time


# def balance_test_data(test_data, output_path):
#     # Separate the fraudulent and non-fraudulent data
#     fraudulent_data = test_data.filter(test_data['Class'] == 1)
#     non_fraudulent_data = test_data.filter(test_data['Class'] == 0)

#     # Find the minimum size between the two classes
#     min_size = min(fraudulent_data.count(), non_fraudulent_data.count())

#     # Sample an equal number of fraudulent and non-fraudulent records
#     fraudulent_sample = fraudulent_data.sample(withReplacement=False, fraction=min_size / fraudulent_data.count())
#     non_fraudulent_sample = non_fraudulent_data.sample(withReplacement=False, fraction=min_size / non_fraudulent_data.count())

#     # Combine the two samples into a balanced dataset
#     balanced_data = fraudulent_sample.union(non_fraudulent_sample)

#     # Shuffle the dataset to randomize the order of records
#     balanced_data = balanced_data.orderBy(F.rand())

#     # Store the balanced dataset to a file (e.g., CSV or Parquet)
#     balanced_data.write.option("header", "true").csv(output_path)  # Or use .parquet() for Parquet format
#     balanced_data.write.mode("overwrite").parquet(output_path)
#     return balanced_data


# # def main():
# #     # Path to the saved model and test data
# #     model_path = 'trained_model'  # Path where the model was saved
# #     test_data_path = 'testdata.csv'  # Path where the test data was saved
# #     balanced_test_data_path = 'balanced_testdata.csv'  # Path to store the balanced test data

# #     # Create Spark session
# #     spark = create_spark_session()

# #     # Load the trained model
# #     model = load_model(model_path)

# #     # Load the test data
# #     test_data = load_test_data(test_data_path, spark)

# #     # Balance the test data to ensure an equal number of fraudulent and non-fraudulent records
# #     balanced_test_data = balance_test_data(test_data, balanced_test_data_path)

# #     # Prepare the input columns used for training the model
# #     input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

# #     # Evaluate the model using the balanced test data
# #     auprc, precision, recall, f1_score = evaluate_model(model, balanced_test_data, input_cols)
# def duplicate_data(data, num_times):
#     """Duplicate the dataset multiple times to increase its size."""
#     large_data = data
#     for _ in range(num_times - 1):
#         large_data = large_data.union(data)
#     return large_data

# def load_data(spark, file_path):
#     """Load dataset from CSV."""
#     start_time = time.time()  # Start timing
#     data = spark.read.csv(file_path, header=True, inferSchema=True)
#     end_time = time.time()  # End timing
    
#     print(f"Data loading time: {end_time - start_time} seconds")
    
#     return data



# def main():
#     model_path = 'trained_model'  # Path where the model was saved
#     # test_data_path = 'testdata.csv'  # Path where the test data was saved
#     # balanced_test_data_path = 'balanced_testdata.csv'  # Path to store the balanced test data

#     # Create Spark session
#     spark = create_spark_session()

#     # Load the trained model
#     model = load_model(model_path)
#     # Load and duplicate the data to scale it
#     test_data_path = 'creditcard.csv'
#     spark = create_spark_session()
#     data = load_data(spark, test_data_path)
    
#     # Duplicate the data to scale
#     large_data = duplicate_data(data, num_times=35)  # Example: scale to ~10 million rows
#     large_data.write.mode("overwrite").parquet('large_testdata.parquet')


#     # Load the new larger data for evaluation
#     test_data = load_test_data('large_testdata.parquet', spark)
#     balanced_test_data = balance_test_data(test_data, 'balanced_testdata.parquet')
#     input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]

#     # Evaluate the model on the large test data
#     auprc, precision, recall, f1_score = evaluate_model(model, balanced_test_data, input_cols)

# if __name__ == "__main__":
#     main()

from train import create_spark_session
from evaluate import load_model, load_test_data, evaluate_model
from pyspark.sql import functions as F
import time


import time

def balance_test_data(test_data, output_path, counter):
    """Balance the test dataset by equalizing fraudulent and non-fraudulent data."""
    # Separate the fraudulent and non-fraudulent data
    fraudulent_data = test_data.filter(test_data['Class'] == 1)
    non_fraudulent_data = test_data.filter(test_data['Class'] == 0)

    # Find the minimum size between the two classes
    min_size = min(fraudulent_data.count(), non_fraudulent_data.count())

    # Sample an equal number of fraudulent and non-fraudulent records
    fraudulent_sample = fraudulent_data.sample(withReplacement=False, fraction=min_size / fraudulent_data.count())
    non_fraudulent_sample = non_fraudulent_data.sample(withReplacement=False, fraction=min_size / non_fraudulent_data.count())

    # Combine the two samples into a balanced dataset
    balanced_data = fraudulent_sample.union(non_fraudulent_sample)

    # Shuffle the dataset to randomize the order of records
    balanced_data = balanced_data.orderBy(F.rand())

    # Generate a new file name with a counter (e.g., balanced_testdata_1.csv, balanced_testdata_2.csv)
    timestamp = time.strftime("%Y%m%d-%H%M%S")  # Optionally, add timestamp for uniqueness
    output_file = f"{output_path}_v{counter}_{timestamp}.csv"

    # Store the balanced dataset in a new CSV file
    balanced_data.write.option("header", "true").mode("overwrite").csv(output_file)
    
    return balanced_data



def duplicate_data(data, num_times):
    """Duplicate the dataset multiple times to increase its size."""
    large_data = data
    for _ in range(num_times - 1):
        large_data = large_data.union(data)
    return large_data


def load_data(spark, file_path):
    """Load dataset from CSV and measure time taken."""
    start_time = time.time()  # Start timing
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    end_time = time.time()  # End timing
    
    print(f"Data loading time: {end_time - start_time} seconds")
    
    return data

def main():
    model_path = 'trained_model'  # Path where the model was saved

    # Create Spark session
    spark = create_spark_session()

    # Load the trained model
    model = load_model(model_path)

    # Load the data
    test_data_path = 'creditcard.csv'
    data = load_data(spark, test_data_path)

    # Duplicate the data to scale
    large_data = duplicate_data(data, num_times=99)  # Example: scale to ~10 million rows
    large_data.write.mode("overwrite").parquet('large_testdata.parquet')

    # Load the new larger data for evaluation
    test_data = load_test_data('large_testdata.parquet', spark)

    # Track the size of the dataset
    data_size = test_data.count()  # Size of the dataset

    # Counter to track the version of output CSV
    counter = 1  # Starting counter value

    # Balance the data and save it to a new CSV each time
    balanced_test_data = balance_test_data(test_data, 'balanced_testdata', counter)

    # Increment the counter for the next run
    counter += 1

    # Log the size of the data being used
    print(f"Evaluating model on dataset of size: {data_size} records")

    # Evaluate the model on the balanced test data
    input_cols = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    auprc, precision, recall, f1_score = evaluate_model(model, balanced_test_data, input_cols)

    # Log the evaluation results along with the data size
    print(f"Model evaluation on {data_size} records:")
    print(f"AUPRC: {auprc}, Precision: {precision}, Recall: {recall}, F1-Score: {f1_score}")


if __name__ == "__main__":
    main()
