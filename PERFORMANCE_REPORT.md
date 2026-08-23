# Performance Test Report
**Date:** August 8, 2026  
**System:** Credit Card Fraud Detection

---

## 1. ML Model Performance (Standalone Test)

### Model Configuration
- **Algorithm:** Random Forest Classifier
- **Number of Trees:** 100
- **Features:** 30 (V1-V28 + Time + Amount)
- **Framework:** Apache Spark MLlib 3.4.4

### Test Dataset
- **Total Records:** 34,820 transactions
- **Source:** testdata.csv (20% split from original dataset)

### Inference Performance
| Metric | Value |
|--------|-------|
| **Total Processing Time** | 1.15 seconds |
| **Throughput** | **30,156 records/second** |
| **Latency per record** | ~0.033 milliseconds |

### Model Accuracy
| Metric | Value |
|--------|-------|
| **Overall Accuracy** | **99.94%** |
| **Fraud Detected** | 62 transactions |
| **True Positives (Fraud caught)** | 55 |
| **False Positives** | 7 |
| **False Negatives (Fraud missed)** | 15 |
| **True Negatives** | 34,743 |

### Confusion Matrix
```
                Predicted
              Normal | Fraud
Actual Normal 34,743 |     7
Actual Fraud      15 |    55
```

### Key Insights
✅ **Strengths:**
- Very high accuracy (99.94%)
- Excellent throughput (30K+ TPS on single node)
- Low latency suitable for real-time detection
- Good fraud detection rate (78.6% recall on fraud cases)

⚠️ **Concerns:**
- 15 fraudulent transactions missed (21.4% false negative rate)
- Dataset may be imbalanced (only ~70 fraud cases in 34K records)
- Real-world performance may vary with streaming overhead

---

## 2. System Prerequisites Status

### ✅ Installed & Working
- [x] Python 3.9.6
- [x] Java OpenJDK 17.0.19
- [x] PySpark 3.4.4
- [x] kafka-python 2.0.2
- [x] All required Python dependencies

### ❌ Not Yet Tested
- [ ] Apache Kafka (not installed)
- [ ] ZooKeeper (not installed)
- [ ] Producer-Consumer pipeline
- [ ] Real-time streaming performance
- [ ] Multi-node scalability

---

## 3. Next Steps to Complete Testing

### Step 1: Install Kafka
```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.4.4/kafka_2.12-3.4.4.tgz
tar -xzf kafka_2.12-3.4.4.tgz
cd kafka_2.12-3.4.4
```

### Step 2: Start Kafka Infrastructure
```bash
# Terminal 1: Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

# Terminal 3: Create topic
bin/kafka-topics.sh --create --topic task-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

### Step 3: Run Streaming Tests
```bash
# Terminal 4: Start Consumer
cd /path/to/Credit-Card-Fraud-Detection
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/consumer.py

# Terminal 5: Start Producer
python3 kafka_stream/producer.py
```

### Step 4: Performance Testing Matrix

| Test Scenario | Ingestion Rate | Expected Throughput | Status |
|---------------|----------------|---------------------|--------|
| Basic Test | 10 TPS | ~10 TPS | Not Tested |
| Medium Load | 50 TPS | ~50 TPS | Not Tested |
| High Load | 100 TPS | ~100 TPS | Not Tested |
| Stress Test | 200 TPS | ? | Not Tested |
| Distributed (Single Topic) | Variable | ? | Not Tested |
| Multi-Topic | Variable | ? | Not Tested |

---

## 4. Expected Bottlenecks

Based on code analysis:

1. **Network Serialization** - JSON overhead in Kafka
2. **Single Broker** - No horizontal scaling configured
3. **Consumer Processing** - Sequential prediction (no batch inference)
4. **No Backpressure** - Fast producers can overwhelm consumers

---

## 5. Performance Baseline Established

✅ **ML Model Standalone:**
- Can process **30,156 transactions/second** without streaming overhead
- This sets the theoretical upper bound for the system

🎯 **Realistic Target with Kafka:**
- Expect **1,000-5,000 TPS** with single broker + streaming overhead
- Target **99%+ accuracy maintained** in real-time

---

## Conclusion

The ML model itself is **production-ready** from a performance standpoint. The next phase is to test the complete Kafka streaming pipeline to identify actual system bottlenecks.

**Recommendation:** Install Kafka and run the full pipeline tests to get end-to-end metrics.
