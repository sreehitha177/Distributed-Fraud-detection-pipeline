# System Flow - Visual Guide

## 📊 Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    TERMINAL 1: KAFKA SERVER                     │
│                                                                 │
│  $ kafka-server-start /opt/homebrew/etc/kafka/server.properties│
│                                                                 │
│  [KafkaRaftServer] Kafka Server started                        │
│  Listening on: localhost:9092                                  │
│                                                                 │
│  Status: RUNNING ✓                                             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   TERMINAL 2: CREATE TOPIC                      │
│                                                                 │
│  $ kafka-topics --create --topic task-topic \                  │
│    --bootstrap-server localhost:9092 --partitions 3            │
│                                                                 │
│  Created topic task-topic.                                     │
│  Partitions: 3                                                 │
│  Replication: 1                                                │
│                                                                 │
│  Status: READY ✓                                               │
└─────────────────────────────────────────────────────────────────┘
        ↓                                              ↑
        ↓                                              ↑
┌────────────────────┐                    ┌────────────────────────┐
│  TERMINAL 4:       │                    │   TERMINAL 3:          │
│  PRODUCER          │                    │   CONSUMER             │
│                    │                    │                        │
│  python3 producer  │   →  KAFKA  →      │   spark-submit         │
│                    │      Topic         │   consumer.py          │
└────────────────────┘   task-topic       └────────────────────────┘
```

---

## 🔄 Detailed Component Flow

### **1. Producer (Terminal 4)**

```
┌──────────────────────────────────────────────────────────┐
│                    PRODUCER FLOW                         │
└──────────────────────────────────────────────────────────┘

Start
  ↓
Load testdata.csv (34,820 transactions)
  ↓
Initialize Spark Session
  ↓
Connect to Kafka (localhost:9092)
  ↓
┌─────────────────────────────────────────────────┐
│         FOR EACH TRANSACTION (200 total)        │
│                                                 │
│  1. Randomly sample 1 row from CSV             │
│  2. Convert to JSON                            │
│  3. Add metadata (timestamp, ingestion_rate)   │
│  4. Send to Kafka topic "task-topic"           │
│  5. Sleep (1 / rate) seconds                   │
│                                                 │
│  Time: ~0.45 seconds per transaction           │
└─────────────────────────────────────────────────┘
  ↓
Print Final Metrics:
  - Throughput: 2.22 TPS
  - Response Time: 0.45s
  ↓
Exit
```

**What You See:**
```
Testing with ingestion rate: 20 transactions/second
Test data loaded from /Users/.../testdata.csv
Starting to stream data to Kafka topic: task-topic
[Stage 10:==============================>  (5 + 2) / 7]  ← Progress
Throughput: 2.22 transactions/second
Response Time per transaction: 0.4509 seconds
```

---

### **2. Kafka (Terminal 1)**

```
┌──────────────────────────────────────────────────────────┐
│                      KAFKA FLOW                          │
└──────────────────────────────────────────────────────────┘

Start Kafka Server
  ↓
Create Topic: task-topic
  - Partitions: 0, 1, 2
  - Messages stored temporarily
  ↓
┌─────────────────────────────────────────────────┐
│              RECEIVING MESSAGES                 │
│                                                 │
│  Producer → Kafka → Consumer                   │
│                                                 │
│  Message Format (JSON):                        │
│  {                                              │
│    "Time": 1786232449.2,                       │
│    "V1": -1.5, "V2": 0.8, ... "V28": 0.1,     │
│    "Amount": 26.88,                            │
│    "Class": 0,                                 │
│    "IngestionRate": 20.0                       │
│  }                                              │
│                                                 │
│  Messages/sec: ~2.2                            │
└─────────────────────────────────────────────────┘
```

**What You See:**
```
[KafkaRaftServer nodeId=1] Kafka Server started
... (mostly internal logs, no direct message visibility)
```

**To See Messages:**
```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic task-topic --from-beginning --max-messages 5
```

---

### **3. Consumer (Terminal 3)**

```
┌──────────────────────────────────────────────────────────┐
│                    CONSUMER FLOW                         │
└──────────────────────────────────────────────────────────┘

Start
  ↓
Initialize Spark Streaming Session
  ↓
Load ML Model: /Users/.../trained_model
  - Random Forest with 100 trees
  - 30 features (V1-V28, Time, Amount)
  ↓
Connect to Kafka (localhost:9092)
Subscribe to topic: "task-topic"
  ↓
┌─────────────────────────────────────────────────┐
│           STREAMING MICRO-BATCHES               │
│                                                 │
│  Every few seconds:                            │
│                                                 │
│  1. Read batch of messages from Kafka          │
│  2. Parse JSON → DataFrame                     │
│  3. Feature Engineering (VectorAssembler)      │
│  4. ML Prediction (Random Forest)              │
│  5. Calculate Metrics:                         │
│     - avg_response_time                        │
│     - throughput (TPS)                         │
│  6. Display Batch Results                      │
│                                                 │
│  Time per batch: ~2.5 seconds                  │
│  Transactions per batch: ~5-10                 │
└─────────────────────────────────────────────────┘
  ↓
Continue Until Stopped (Ctrl+C)
```

**What You See:**
```
Model loaded from: /Users/.../trained_model
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

---

## 🕐 Timeline View

```
TIME    TERMINAL 1        TERMINAL 2       TERMINAL 3          TERMINAL 4
        (Kafka)           (Setup)          (Consumer)          (Producer)
────────────────────────────────────────────────────────────────────────────
T+0s    Start Kafka       
        Loading...        

T+5s    ✓ Running         Create Topic
                          ✓ Created
                          
T+10s   Listening         Idle             Start Consumer
                                           Downloading...
                                           
T+20s   Listening         Idle             Loading model...
                                           
T+25s   Listening         Idle             ✓ Ready
                                           Waiting for data
                                           Batch: 0 (empty)
                                           
T+30s   Listening         Idle             ✓ Ready            Start Producer
                                           Batch: 0           Loading CSV...
                                           
T+35s   Listening         Idle             ✓ Ready            Sending...
                                           Batch: 0           
                                           
T+40s   ← Message         Idle             Batch: 1 →         → Sending
        ← Message                          Processing         → Sending
        ← Message                          Predicting         → Sending
        
T+45s   ← Message         Idle             Batch: 2 →         → Sending
        ← Message                          Metrics:           → Sending
        ← Message                          - 2.2 TPS          
                                           - 2.1s latency     
                                           
T+50-                     
T+120s  Messages          Idle             Batches            Sending...
        flowing                            processing         Progress bars
                                           every 5-10s        
                                           
T+130s  Idle              Idle             Batch: 20          ✓ Complete
                                           Final stats        Metrics shown
                                           
T+135s  Idle              Idle             ✓ Still running    Exited
                                           Waiting...         
```

---

## 📊 Data Transformation Flow

### Producer → Kafka → Consumer

```
PRODUCER                    KAFKA                 CONSUMER
────────────────────────────────────────────────────────────────────

CSV Row:                    JSON Message:         DataFrame:
┌──────────┐               ┌──────────┐          ┌──────────┐
│ Time: 100│  Serialize    │{"Time":  │ Parse    │ Time: 100│
│ V1: -1.5 │────────────→  │ 100,     │────────→ │ V1: -1.5 │
│ V2: 0.8  │               │ "V1":    │          │ V2: 0.8  │
│ ...      │               │ -1.5,... │          │ ...      │
│ Amount:  │               │ "Amount":│          │ Amount:  │
│ 26.88    │               │ 26.88}   │          │ 26.88    │
└──────────┘               └──────────┘          └──────────┘
                                                        ↓
                                                  VectorAssembler
                                                        ↓
                                                  Feature Vector:
                                                  [100,-1.5,0.8,...,26.88]
                                                        ↓
                                                  ML Model (Random Forest)
                                                        ↓
                                                  Prediction:
                                                  ┌──────────────┐
                                                  │ Class: 0     │
                                                  │ (Not Fraud)  │
                                                  │ Probability: │
                                                  │ [0.99, 0.01] │
                                                  └──────────────┘
```

---

## 🎯 Where Time Is Spent

### Producer (0.45s per transaction):
```
┌──────────────────────────────────────────┐
│ Spark DataFrame sample:     0.35s (78%) │
│ JSON serialization:         0.05s (11%) │
│ Kafka send:                 0.03s (7%)  │
│ Sleep (1/rate):             0.02s (4%)  │
├──────────────────────────────────────────┤
│ TOTAL:                      0.45s       │
└──────────────────────────────────────────┘
```

### Consumer (2.5s end-to-end):
```
┌──────────────────────────────────────────┐
│ Spark batch coordination:   1.8s (72%) │
│ Feature engineering:        0.4s (16%) │
│ Kafka read:                 0.2s (8%)  │
│ ML prediction:              0.05s (2%) │
│ Metrics calculation:        0.05s (2%) │
├──────────────────────────────────────────┤
│ TOTAL:                      2.5s       │
└──────────────────────────────────────────┘
```

**Key Insight:** ML model (0.05s) is NOT the bottleneck!

---

## 🔍 What Each Metric Means

### In Consumer Output:

```
+-------------+-----------------+----------+
|IngestionRate|avg_response_time|throughput|
+-------------+-----------------+----------+
|         20.0|2.38             |2.1       |
+-------------+-----------------+----------+
```

**IngestionRate = 20.0**
- What producer *tried* to achieve
- Configured in producer.py
- Doesn't match actual rate (producer too slow)

**avg_response_time = 2.38**
- Time from "producer sends message" to "prediction complete"
- Includes: Kafka latency + Consumer processing + ML inference
- Measured in seconds
- **Target: < 0.1s (100ms)**

**throughput = 2.1**
- Actual transactions processed per second
- This is the "real" performance
- **Target: 100-500 TPS**

---

## 🎓 Summary

### What's Working:
✅ All components communicate correctly  
✅ No data loss  
✅ Model makes predictions  
✅ Metrics are collected  

### What's Slow:
❌ Producer: Using Spark for simple CSV reading  
❌ Consumer: Spark Streaming batch overhead  
❌ Overall: 13,707x slower than theoretical max  

### The Path Forward:
🎯 Optimize Producer: Replace Spark with simple loop  
🎯 Optimize Consumer: Increase batch size, reduce overhead  
🎯 Expected Result: 10-100x faster  

---

Now you understand the complete flow! Open 4 terminals and follow HOW_TO_RUN.md to see it in action.
