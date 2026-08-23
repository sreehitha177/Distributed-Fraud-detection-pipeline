# Start Kafka 4.3.1 (KRaft Mode - No ZooKeeper Needed!)

Your Kafka version uses **KRaft mode** which doesn't need ZooKeeper. This is simpler!

## Step 1: Format Storage (First Time Only)

```bash
# Generate a cluster UUID
KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)"

# Format the storage
kafka-storage format -t $KAFKA_CLUSTER_ID -c /opt/homebrew/etc/kafka/server.properties
```

You only need to do this **once** (the first time you set up Kafka).

## Step 2: Start Kafka Server

**Terminal 1:**
```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

Wait for this message:
```
[KafkaServer id=0] started
```

## Step 3: Create Topic

**Terminal 2:**
```bash
# Create the main topic
kafka-topics --create \
  --topic task-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify it was created
kafka-topics --list --bootstrap-server localhost:9092

# See details
kafka-topics --describe --topic task-topic --bootstrap-server localhost:9092
```

Expected output:
```
Created topic task-topic.
task-topic
Topic: task-topic	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1
```

## Step 4: Test Kafka (Optional)

**Terminal 2 - Start a test consumer:**
```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic task-topic
```

**Terminal 3 - Send a test message:**
```bash
echo "Hello Kafka!" | kafka-console-producer --bootstrap-server localhost:9092 --topic task-topic
```

You should see "Hello Kafka!" appear in Terminal 2.

Press `Ctrl+C` in both terminals when done testing.

## Step 5: Start Your Application

**Terminal 2 (or new terminal 3):** Start Consumer
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 \
  kafka_stream/consumer.py
```

**Terminal 3 (or new terminal 4):** Start Producer
```bash
cd /Users/sreehithanarayana/Desktop/Credit-Card-Fraud-Detection

python3 kafka_stream/producer.py
```

## Quick Commands

### Check if Kafka is running
```bash
lsof -i :9092
```

### List all topics
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

### Delete a topic (if needed)
```bash
kafka-topics --delete --topic task-topic --bootstrap-server localhost:9092
```

### Stop Kafka
```bash
kafka-server-stop
# or press Ctrl+C in the terminal running kafka-server-start
```

### Check consumer groups
```bash
kafka-consumer-groups --list --bootstrap-server localhost:9092
```

## Troubleshooting

### "Command not found: kafka-topics"
Your PATH might not be set. Try:
```bash
export PATH="/opt/homebrew/bin:$PATH"
```

Or use full path:
```bash
/opt/homebrew/bin/kafka-topics --list --bootstrap-server localhost:9092
```

### "Connection refused"
Kafka isn't running. Check Terminal 1 for errors.

### "Topic already exists"
That's fine! Just use the existing topic.

### "Timeout" or "Unable to connect"
1. Make sure Kafka is running (Terminal 1)
2. Check port 9092: `lsof -i :9092`
3. Check server.properties has: `listeners=PLAINTEXT://localhost:9092`

## Clean Restart (If Things Go Wrong)

```bash
# Stop Kafka
kafka-server-stop
pkill -9 -f kafka

# Clean data directory (CAUTION: deletes all Kafka data)
rm -rf /opt/homebrew/var/lib/kafka-logs/*

# Re-format storage
KAFKA_CLUSTER_ID="$(kafka-storage random-uuid)"
kafka-storage format -t $KAFKA_CLUSTER_ID -c /opt/homebrew/etc/kafka/server.properties

# Start again
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

---

## Summary: 3 Simple Steps

1. **Format** (first time only): `kafka-storage format -t $(kafka-storage random-uuid) -c /opt/homebrew/etc/kafka/server.properties`
2. **Start Kafka**: `kafka-server-start /opt/homebrew/etc/kafka/server.properties`
3. **Create Topic**: `kafka-topics --create --topic task-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1`

Then run your producer and consumer!
