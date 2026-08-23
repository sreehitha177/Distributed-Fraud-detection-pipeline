# Kafka Setup & Testing Guide

## Quick Start: Install Kafka on macOS

### Option 1: Using Homebrew (Recommended)
```bash
# Install Kafka (includes ZooKeeper)
brew install kafka

# Verify installation
kafka-topics --version
```

### Option 2: Manual Download
```bash
# Download Kafka
cd ~/Downloads
curl -O https://archive.apache.org/dist/kafka/3.4.4/kafka_2.12-3.4.4.tgz

# Extract
tar -xzf kafka_2.12-3.4.4.tgz
mv kafka_2.12-3.4.4 ~/kafka

# Add to PATH (add to ~/.zshrc)
export PATH="$HOME/kafka/bin:$PATH"
source ~/.zshrc
```

---

## Step-by-Step: Running the System

### Terminal 1: Start ZooKeeper
```bash
# If installed via Homebrew:
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

# If manual installation:
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Expected Output:**
```
[2026-08-08 18:00:00,000] INFO binding to port 0.0.0.0/0.0.0.0:2181
```

### Terminal 2: Start Kafka Broker
```bash
# If installed via Homebrew:
kafka-server-start /usr/local/etc/kafka/server.properties

# If manual installation:
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

**Expected Output:**
```
[2026-08-08 18:00:05,000] INFO [KafkaServer id=0] started
```

### Terminal 3: Create Kafka Topic
```bash
cd ~/kafka  # or wherever Kafka is installed

# Create the main topic
kafka-topics --create \
  --topic task-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topic creation
kafka-topics --list --bootstrap-server localhost:9092

# Check topic details
kafka-topics --describe \
  --topic task-topic \
  --bootstrap-server localhost:9092
```

**Expected Output:**
```
Created topic task-topic.
Topic: task-topic	TopicId: ... PartitionCount: 3	ReplicationFactor: 1
```

### Terminal 4: Start Consumer (Spark Streaming)
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Run with spark-submit
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/consumer.py
```

**What to Expect:**
- Spark will initialize (takes 5-10 seconds)
- Consumer will wait for messages from Kafka
- Will print metrics every second

### Terminal 5: Start Producer
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Run basic producer
python3 kafka_stream/producer.py
```

**What to Expect:**
- Loads test data
- Starts sending transactions to Kafka
- Prints throughput metrics

---

## Performance Tests to Run

### Test 1: Basic Functionality (Low Rate)
**Edit `kafka_stream/producer.py`:**
```python
if __name__ == "__main__":
    ingestion_rates = [10]  # 10 TPS
    total_transactions = 100
```

**Run and measure:**
- Consumer can keep up?
- Fraud alerts detected?
- No errors?

### Test 2: Medium Load
**Edit `kafka_stream/producer.py`:**
```python
if __name__ == "__main__":
    ingestion_rates = [50]  # 50 TPS
    total_transactions = 500
```

### Test 3: High Load Stress Test
**Edit `kafka_stream/producer.py`:**
```python
if __name__ == "__main__":
    ingestion_rates = [100, 150, 200]  # Test multiple rates
    total_transactions = 1000
```

### Test 4: Distributed Producers (Single Topic)
```bash
# Terminal 5:
python3 kafka_stream/distributed_producer.py

# Terminal 6 (new): Second consumer instance
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/distributed_consumer.py
```

### Test 5: Multi-Topic Architecture
```bash
# First, create additional topics:
kafka-topics --create \
  --topic task-topic-1 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

kafka-topics --create \
  --topic task-topic-2 \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Run multi-topic producer
python3 kafka_stream/different_producer.py

# Run multi-topic consumer
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/different_consumer.py
```

---

## Metrics to Collect

### From Producer:
- Throughput (transactions/second sent)
- Response time per transaction
- Total time to send N transactions

### From Consumer:
- Throughput (transactions/second processed)
- End-to-end latency (time from send to prediction)
- Average response time
- Fraud detection count
- Any errors or warnings

### From Kafka:
```bash
# Check consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <your-consumer-group>

# Monitor topic
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic task-topic \
  --from-beginning \
  --max-messages 10
```

---

## Troubleshooting

### Issue: "Connection refused to localhost:9092"
**Solution:** Kafka broker not started. Check Terminal 2.

### Issue: "Topic task-topic not found"
**Solution:** Create the topic (see Terminal 3 commands).

### Issue: Consumer shows no output
**Solution:** 
1. Check producer is running
2. Verify topic name matches in both producer and consumer
3. Check for errors in consumer terminal

### Issue: "Java heap space" error
**Solution:** Increase Spark memory:
```bash
export SPARK_SUBMIT_OPTS="-Xmx4g"
spark-submit ...
```

### Issue: Absolute path errors
**Solution:** Update hardcoded paths in files:
- `kafka_stream/distributed_producer.py` (line ~63)
- `kafka_stream/distributed_consumer.py` (line ~23)
- `kafka_stream/different_producer.py` (line ~43)

Replace with your actual path:
```python
file_path = "/Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection/ml_model/testdata.csv"
model_path = "/Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection/ml_model/trained_model"
```

---

## Success Criteria

✅ **System is working when:**
1. Producer sends N transactions without errors
2. Consumer receives and processes all transactions
3. Fraud predictions appear in console
4. Throughput metrics are reasonable (>100 TPS)
5. No consumer lag accumulation

---

## Quick Commands Reference

```bash
# List all topics
kafka-topics --list --bootstrap-server localhost:9092

# Delete a topic
kafka-topics --delete --topic task-topic --bootstrap-server localhost:9092

# Check consumer groups
kafka-consumer-groups --list --bootstrap-server localhost:9092

# Stop Kafka gracefully
kafka-server-stop
zookeeper-server-stop

# Kill if hung
pkill -9 -f kafka
pkill -9 -f zookeeper
```

---

## Next: After Successful Testing

Once you have working metrics, we'll:
1. Analyze bottlenecks
2. Optimize configurations
3. Fix identified flaws
4. Add missing features (monitoring, error handling, etc.)
