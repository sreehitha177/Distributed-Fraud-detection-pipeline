import time
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
import threading

# Kafka Producer configuration
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# def create_producer():
#     return KafkaProducer(**producer)


def create_producer():
    """Create a new KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def load_test_data(test_data_path, spark):
    """Load test data from CSV using Spark."""
    test_data = spark.read.option("header", "true").csv(test_data_path)
    print(f"Test data loaded from {test_data_path}")
    
    # Cast all required columns to DoubleType
    columns_to_cast = [f"V{i}" for i in range(1, 29)] + ["Time", "Amount"]
    for col_name in columns_to_cast:
        test_data = test_data.withColumn(col_name, col(col_name).cast(DoubleType()))
        
    # Cast the 'Class' column to DoubleType as well
    test_data = test_data.withColumn("Class", col("Class").cast(DoubleType()))
    
    # Optional: Print the first few rows to verify
    test_data.show(5)
    
    return test_data

# Generate transaction data from test data
def generate_transaction_data(test_data):
    """Randomly select a transaction from the test data."""
    # Randomly sample one row from the test data
    row = test_data.orderBy(F.rand()).limit(1).collect()[0]

    # Convert the row to a dictionary
    transaction = {col: row[col] for col in row.asDict().keys()}
    
    # Add timestamp to simulate real-time
    transaction["Time"] = time.time()
    
    return transaction


def producer_thread(producer, topic, test_data, rate, total_transactions):
    """Simulate a producer sending transactions to Kafka."""
    for _ in range(total_transactions):
        transaction = generate_transaction_data(test_data)
        producer.send(topic, value=transaction)
        time.sleep(1 / rate)
    print(f"Producer for topic {topic} completed.")


def main(rate,total_transactions):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .getOrCreate()

    # Load test data from CSV using Spark
    import os
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_dir, "ml_model", "testdata.csv")
    test_data = load_test_data(file_path, spark)

    topics = ["task-topic-1", "task-topic-1"]

    producers = [create_producer() for _ in topics]
    threads = [
        threading.Thread(target=producer_thread, args=(producers[i], topics[i], test_data, rate, total_transactions))
        for i in range(len(topics))
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
#     # Stream data to Kafka
    # topic = "task-topic"
    # print(f"Starting to stream data to Kafka topic: {topic}")

#     while True:
#         # Generate a single transaction
#         transaction = generate_transaction_data(test_data)

#         # Send transaction to Kafka
#         producer.send(topic, value=transaction)
#         print(f"Sent: {transaction}")

#         # Simulate a 1-second delay between transactions
#         time.sleep(1/rate)

# if __name__ == "__main__":
#     rate=1
#     main(rate)
    # start_time = time.time()

    # # for i in range(total_transactions):
    # #     transaction = generate_transaction_data(test_data)
    # #     # producers.send(topics, value=transaction)
    # #     for producer, topic in zip(producers, topics):
    # #         producer.send(topic, value=transaction)
    # #     # print(f"Sent: {transaction}")
    # #     time.sleep(1 / rate)

    # end_time = time.time()
    # total_time = end_time - start_time

    # throughput = total_transactions / total_time
    # print(f"Throughput: {throughput:.2f} transactions/second")
    # print(f"Response Time per transaction: {total_time / total_transactions:.4f} seconds")

if __name__ == "__main__":
    rate = 100  # Transactions per second
    total_transactions = 500
    main(rate, total_transactions)
