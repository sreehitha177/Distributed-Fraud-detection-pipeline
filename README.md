## Prerequisites

- Apache Kafka: Download and extract Apache Kafka on your local machine or a server. You can find the Kafka downloads and installation instructions on the [Apache Kafka website](https://kafka.apache.org/downloads).
- Python: Ensure Python is installed on your system.
- Install kafka-python and other required libraries using: ```pip install kafka-python pyspark```
- Ensure Java[OpenJDK 11] is installed and configured on your system 

## Setup and Usage -python virtual environment
- Clone the repository
1. Start ZooKeeper: ```bin/zookeeper-server-start.sh config/zookeeper.properties```
2. Start kafka brokers: ```bin/kafka-server-start.sh config/server.properties```
3. create kafka topics: ```bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1```
4. Run ```consumer.py``` and ```producer.py``` in separate terminals
- To run producer.py - ```python producer.py```
- To run spark-Consumer - ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 consumer.py```
5. Verify Output: The consumer will process the tasks produced by the producer and print the results to the console.

## Experimentation and Test Cases

1. Test Random Forest Model: Evaluate the model's accuracy and performance using different datasets. Experiment with hyperparameter tuning and feature importance to improve detection efficiency.

2. Failure Recovery: Test the system's ability to handle producer or consumer failures by manually stopping and restarting processes. Verify data consistency and recovery mechanisms.

3. Experiment with Different Kafka Topics: 
    - Create a single topic and multiple producers with varying configurations to simulate different data streams to evaluate system analyzing metrics such as throughput and response time while varying ingestion rates. Run ```distributed_producer.py``` and ```distributed_consumer.py``` to see these results.

    - Create multiple topics and multiple producers to simulate different data streams to evaluate system analyzing metrics such as throughput and response time while varying ingestion rates. Run ```different_producer.py``` and ```different_consumer.py``` to see these results.

## Troubleshooting

1. Kafka Errors: Ensure ZooKeeper and Kafka brokers are running before starting producer and consumer scripts. Check for port conflicts and verify logs for detailed error messages.

2. Python Errors: Verify all required Python libraries are installed. Use a virtual environment to manage dependencies to avoid version conflicts.

3. Java and Spark Errors: Ensure the correct version of Java and Spark is installed and matches the dependencies used in the spark-submit command. Check Spark’s logs for specific error details.

4. Ensure the path of all the files is given as absolute path wrt the unix system.
