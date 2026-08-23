# How to Run the Complete System - Step by Step

This guide will help you run Kafka + Spark + ML Model and see all the metrics yourself.

---

## 🎯 What You'll See

When running the system, you will see:
- **Producer:** Transactions being sent to Kafka + throughput metrics
- **Consumer:** Real-time predictions + processing metrics
- **Kafka:** Topic status and message flow

---

## 📋 Prerequisites Check

Run this first to make sure everything is ready:

```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
./quick_test.sh
```

**Expected output:**
- ✓ Python, Java, PySpark, kafka-python all installed
- ✓ Model and data files exist
- ✓ Model test passes with 99.94% accuracy

---

## 🚀 Running the System (4 Terminals)

You need **4 separate terminal windows**. Open them all before starting.

---

### **TERMINAL 1: Start Kafka** ⚡

```bash
# Start Kafka Server
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

**What to look for:**
```
[KafkaRaftServer nodeId=1] Kafka Server started
```

**This means:** Kafka is running on `localhost:9092`

**Keep this terminal open!** Don't close it.

---

### **TERMINAL 2: Setup Kafka Topic** 📊

Once Kafka is running (Terminal 1 shows "started"), run:

```bash
# Create the topic (only needed once)
kafka-topics --create \
  --topic task-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

**Expected output:**
```
Created topic task-topic.
```

**Verify it worked:**
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

**Should show:**
```
task-topic
```

**Check status anytime:**
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
./check_kafka.sh
```

---

### **TERMINAL 3: Start Consumer (Spark Streaming)** 🔄

This is the ML inference engine that processes transactions.

```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Add spark-submit to PATH
export PATH="/Users/sreehithanarayana/Library/Python/3.9/bin:$PATH"

# Run the consumer
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/consumer.py
```

**What happens:**
1. Downloads Spark-Kafka packages (~10 seconds)
2. Initializes Spark (~5 seconds)
3. Loads ML model (~3 seconds)
4. Shows: "Model loaded from: /Users/..."
5. Displays empty batch table (waiting for data)

**You'll see this while waiting:**
```
-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+-----------------+----------+
|IngestionRate|avg_response_time|throughput|
+-------------+-----------------+----------+
+-------------+-----------------+----------+
```

**This means:** Consumer is ready and waiting for transactions!

**Keep this terminal visible!** You'll see metrics here.

---

### **TERMINAL 4: Start Producer (Send Transactions)** 📤

Now send transactions to Kafka for the consumer to process.

```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

# Run the producer
python3 kafka_stream/producer.py
```

**What you'll see:**

1. **Loading phase (~5 seconds):**
```
Testing with ingestion rate: 20 transactions/second
Test data loaded from /Users/.../testdata.csv
Starting to stream data to Kafka topic: task-topic
```

2. **Sending phase (~90 seconds):**
```
[Stage 10:==============================>  (5 + 2) / 7]
```
*Progress bars show transactions being sent*

3. **Final metrics:**
```
Throughput: 2.22 transactions/second
Response Time per transaction: 0.4509 seconds
```

---

## 📊 What to Watch

### In Terminal 3 (Consumer):

Once producer starts, you'll see batches processing:

```
-------------------------------------------
Batch: 1
-------------------------------------------
+-------------+-----------------+----------+
|IngestionRate|avg_response_time|throughput|
+-------------+-----------------+----------+
|         20.0|              0.0|      null|
+-------------+-----------------+----------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-------------+------------------+----------+
|IngestionRate| avg_response_time|throughput|
+-------------+------------------+----------+
|         20.0|2.1818181818181817|       2.2|
+-------------+------------------+----------+

... continues for each batch ...
```

**What these mean:**
- **IngestionRate:** Target rate from producer (20 TPS)
- **avg_response_time:** How long from send to prediction (seconds)
- **throughput:** Actual processing rate (TPS)

**Watch for:**
- Throughput should stabilize around **2.1-2.2 TPS**
- Response time should be around **2.5 seconds**
- Batches should appear every few seconds

### In Terminal 4 (Producer):

Final output shows:
```
Throughput: X.XX transactions/second     ← How fast producer sent
Response Time per transaction: X.XXXX seconds  ← Time per transaction
```

---

## 🎮 Testing Different Rates

Want to test with different ingestion rates?

**Edit producer.py:**
```bash
nano kafka_stream/producer.py
# or
open -e kafka_stream/producer.py
```

**Find this section (around line 175):**
```python
if __name__ == "__main__":
    ingestion_rates = [20]  # ← CHANGE THIS
    total_transactions = 200  # ← OR THIS
```

**Try different configurations:**

### Test 1: Very Low Rate (Easy)
```python
ingestion_rates = [5]    # 5 TPS
total_transactions = 50  # Quick test
```

### Test 2: Medium Rate
```python
ingestion_rates = [50]   # 50 TPS
total_transactions = 500
```

### Test 3: High Rate (Stress Test)
```python
ingestion_rates = [100]  # 100 TPS
total_transactions = 1000
```

### Test 4: Multiple Rates (Sequential)
```python
ingestion_rates = [10, 25, 50, 75, 100]  # Tests each rate
total_transactions = 200  # For each rate
```

**After editing, save and restart producer (Terminal 4):**
- Press `Ctrl+C` to stop producer
- Run `python3 kafka_stream/producer.py` again

**Consumer keeps running!** No need to restart it.

---

## 📈 Monitoring Commands

### Check Kafka Status
```bash
# Is Kafka running?
lsof -i :9092

# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Check topic details
kafka-topics --describe --topic task-topic --bootstrap-server localhost:9092
```

### Check Consumer Groups
```bash
# List consumer groups
kafka-consumer-groups --list --bootstrap-server localhost:9092

# Check consumer lag (after running consumer)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group <group-id>
```

### Peek at Messages in Kafka
```bash
# See messages in the topic
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic task-topic \
  --from-beginning \
  --max-messages 5
```

---

## 🛑 How to Stop Everything

### Stop in this order:

**1. Stop Producer (Terminal 4):**
```
Press Ctrl+C
```

**2. Stop Consumer (Terminal 3):**
```
Press Ctrl+C
(may take 5-10 seconds)
```

**3. Stop Kafka (Terminal 1):**
```
Press Ctrl+C
```

### Force Kill if Stuck:
```bash
# Kill Kafka processes
pkill -9 -f kafka

# Check nothing is running
lsof -i :9092  # Should return nothing
```

---

## 🔄 Starting Fresh

If you want to reset everything:

```bash
# 1. Stop all processes
pkill -9 -f kafka
pkill -9 -f spark

# 2. Clean Kafka data (CAUTION: deletes all messages)
rm -rf /opt/homebrew/var/lib/kraft-combined-logs/*

# 3. Re-initialize Kafka
KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)"
kafka-storage format -t $KAFKA_CLUSTER_ID \
  -c /opt/homebrew/etc/kafka/server.properties --standalone

# 4. Start from Terminal 1 again
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

---

## 🐛 Troubleshooting

### Issue: "Connection refused to localhost:9092"
**Solution:** Kafka not running. Check Terminal 1.

### Issue: "Topic task-topic does not exist"
**Solution:** Create topic in Terminal 2.

### Issue: Consumer shows errors about model path
**Solution:** Make sure you're running from the project directory:
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
```

### Issue: "spark-submit: command not found"
**Solution:** Set PATH:
```bash
export PATH="/Users/sreehithanarayana/Library/Python/3.9/bin:$PATH"
```

### Issue: Producer very slow
**Solution:** This is expected! That's what we'll optimize.

### Issue: Consumer not processing
**Solution:** 
1. Check Terminal 1 - Is Kafka running?
2. Check Terminal 2 - Does topic exist?
3. Check Terminal 4 - Is producer sending?

---

## 📊 Understanding the Metrics

### Producer Metrics

```
Throughput: 2.22 transactions/second
```
**Means:** Producer sent 200 transactions at 2.22 TPS (took ~90 seconds)

```
Response Time per transaction: 0.4509 seconds
```
**Means:** Each transaction took 0.45 seconds to generate and send

**Why so slow?** Producer uses Spark to read CSV and randomly sample data - very inefficient!

### Consumer Metrics

```
|IngestionRate|avg_response_time|throughput|
|         20.0|2.5              |2.1       |
```

**IngestionRate (20.0):** What producer *tried* to achieve  
**avg_response_time (2.5s):** Time from producer send to ML prediction complete  
**throughput (2.1 TPS):** Actual processing speed

**Why so slow?** Spark Streaming has batch coordination overhead, checkpointing, etc.

---

## 🎯 What Good Performance Looks Like

### Current Performance:
- Producer: **2.2 TPS** ❌
- Consumer: **2.1 TPS** ❌
- Latency: **2.5 seconds** ❌

### Target Performance (After Optimization):
- Producer: **100-500 TPS** ✅
- Consumer: **100-500 TPS** ✅
- Latency: **< 100ms** ✅

### Theoretical Maximum (ML Model Alone):
- **30,156 TPS** 🚀
- **0.033ms latency** ⚡

---

## 📁 Useful Files

### View Results
```bash
# Read test results
cat TEST_RESULTS.md | less

# Check system status
./quick_test.sh

# Check Kafka
./check_kafka.sh
```

### Test ML Model Standalone
```bash
# See how fast the model really is
python3 test_model.py
```

**This proves the model can do 30K TPS!**

---

## 🎓 Quick Reference

### 4-Step Startup:
```bash
# Terminal 1
kafka-server-start /opt/homebrew/etc/kafka/server.properties

# Terminal 2
kafka-topics --create --topic task-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Terminal 3
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
export PATH="/Users/sreehithanarayana/Library/Python/3.9/bin:$PATH"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 kafka_stream/consumer.py

# Terminal 4
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection
python3 kafka_stream/producer.py
```

### Check Status:
```bash
./check_kafka.sh
lsof -i :9092
```

### Stop All:
```bash
# Ctrl+C in each terminal, or:
pkill -9 -f kafka
pkill -9 -f spark
```

---

## ✅ Success Checklist

When running successfully, you should see:

- [ ] Terminal 1: "Kafka Server started"
- [ ] Terminal 2: "Created topic task-topic"
- [ ] Terminal 3: "Model loaded from: ..." and batch tables
- [ ] Terminal 4: "Starting to stream data" and progress bars
- [ ] Terminal 3: Metrics updating every few seconds
- [ ] Terminal 4: Final "Throughput: X.XX transactions/second"
- [ ] No errors in any terminal

---

Now you're ready to run the system and see the metrics yourself! Start with Terminal 1 and work your way through. Let me know if you hit any issues!
