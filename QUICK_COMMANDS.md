# Quick Commands Cheat Sheet

## 🚀 Start Everything (Copy-Paste Ready)

### Terminal 1: Kafka
```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

### Terminal 2: Create Topic (Run Once)
```bash
kafka-topics --create --topic task-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Terminal 3: Consumer
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
export PATH="/Users/sreehithanarayana/Library/Python/3.9/bin:$PATH"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 kafka_stream/consumer.py
```

### Terminal 4: Producer
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
python3 kafka_stream/producer.py
```

---

## 🛑 Stop Everything

```bash
# In each terminal: Ctrl+C

# Or force kill:
pkill -9 -f kafka
pkill -9 -f spark
```

---

## 🔍 Check Status

```bash
# Quick check
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
./check_kafka.sh

# Detailed checks
lsof -i :9092  # Is Kafka running?
kafka-topics --list --bootstrap-server localhost:9092  # List topics
ps aux | grep kafka  # Find Kafka processes
ps aux | grep spark  # Find Spark processes
```

---

## 📊 Monitor Kafka

```bash
# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics --describe --topic task-topic --bootstrap-server localhost:9092

# Peek at messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic task-topic --from-beginning --max-messages 5

# Consumer groups
kafka-consumer-groups --list --bootstrap-server localhost:9092

# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group-name>
```

---

## 🧪 Test Components

```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Test prerequisites
./quick_test.sh

# Test ML model standalone
python3 test_model.py

# Check Kafka status
./check_kafka.sh
```

---

## 🔄 Reset Everything

```bash
# Stop all
pkill -9 -f kafka
pkill -9 -f spark

# Clean Kafka data
rm -rf /opt/homebrew/var/lib/kraft-combined-logs/*

# Reinitialize
KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)"
kafka-storage format -t $KAFKA_CLUSTER_ID -c /opt/homebrew/etc/kafka/server.properties --standalone

# Restart from Terminal 1
```

---

## 🎛️ Change Producer Settings

```bash
# Edit producer
open -e kafka_stream/producer.py

# Find line ~175 and change:
ingestion_rates = [10]  # Try 10, 20, 50, 100
total_transactions = 100  # Try 100, 500, 1000
```

---

## 📁 View Results

```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Read test results
cat TEST_RESULTS.md | less

# See how to run
cat HOW_TO_RUN.md | less

# Understand flow
cat SYSTEM_FLOW.md | less

# Performance report
cat PERFORMANCE_REPORT.md | less
```

---

## 🐛 Troubleshooting One-Liners

```bash
# Kafka not starting?
lsof -i :9092 && echo "Already running" || echo "Not running"

# Clean restart
pkill -9 -f kafka && rm -rf /opt/homebrew/var/lib/kraft-combined-logs/* && KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)" && kafka-storage format -t $KAFKA_CLUSTER_ID -c /opt/homebrew/etc/kafka/server.properties --standalone

# Check PATH
echo $PATH | grep Python

# Add spark-submit to PATH
export PATH="/Users/sreehithanarayana/Library/Python/3.9/bin:$PATH"

# Verify installations
python3 -c "import pyspark; print('PySpark OK')"
python3 -c "import kafka; print('Kafka-python OK')"
which kafka-topics && echo "Kafka CLI OK"
```

---

## 📊 Expected Output Samples

### Consumer (Good):
```
Model loaded from: /Users/.../trained_model
-------------------------------------------
Batch: 5
-------------------------------------------
+-------------+-----------------+----------+
|IngestionRate|avg_response_time|throughput|
+-------------+-----------------+----------+
|         20.0|2.38             |2.1       |
+-------------+-----------------+----------+
```

### Producer (Good):
```
Testing with ingestion rate: 20 transactions/second
Test data loaded from /Users/.../testdata.csv
Starting to stream data to Kafka topic: task-topic
[Stage 10:===========================>  (5 + 2) / 7]
Throughput: 2.22 transactions/second
Response Time per transaction: 0.4509 seconds
```

### Kafka (Good):
```
[KafkaRaftServer nodeId=1] Kafka Server started
Awaiting socket connections on 0.0.0.0:9092.
```

---

## 💡 Pro Tips

```bash
# Run consumer and producer in same directory
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Keep Kafka terminal visible to spot errors
# Keep Consumer terminal visible to see metrics

# Test different rates without restarting consumer:
# 1. Ctrl+C in producer
# 2. Edit producer.py
# 3. Run producer again
# Consumer keeps running!

# Save metrics to file:
python3 kafka_stream/producer.py > producer_metrics.txt 2>&1
```

---

## 🎯 Quick Test Sequence

```bash
# 1. Status check
./quick_test.sh && ./check_kafka.sh

# 2. Start Kafka (Terminal 1)
kafka-server-start /opt/homebrew/etc/kafka/server.properties &

# 3. Wait 5 seconds, create topic
sleep 5 && kafka-topics --create --topic task-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 4. Start consumer (Terminal 2)
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection && export PATH="/Users/sreehithanarayana/Library/Python/3.9/bin:$PATH" && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 kafka_stream/consumer.py &

# 5. Wait 30 seconds, start producer (Terminal 3)
sleep 30 && python3 kafka_stream/producer.py
```

---

## 📞 Get Help

```bash
# Kafka help
kafka-topics --help
kafka-consumer-groups --help
kafka-console-consumer --help

# Spark help
spark-submit --help

# Python help
python3 kafka_stream/producer.py --help  # (not implemented yet)
```

---

## 🎓 Learning Commands

```bash
# See a message in Kafka
kafka-console-consumer --bootstrap-server localhost:9092 --topic task-topic --from-beginning --max-messages 1 | python3 -m json.tool

# Count messages in topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic task-topic --from-beginning --timeout-ms 5000 | wc -l

# See Spark UI (while consumer running)
open http://localhost:4040

# Monitor system resources
top -pid $(pgrep -f kafka) -pid $(pgrep -f spark)
```

---

That's your complete quick reference! Keep this handy while running experiments.
