#!/bin/bash
# Check if Kafka and ZooKeeper are running

echo "Checking Kafka Infrastructure Status..."
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check ZooKeeper
echo -n "ZooKeeper (port 2181): "
if lsof -Pi :2181 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Running${NC}"
    ZK_RUNNING=true
else
    echo -e "${RED}✗ Not running${NC}"
    ZK_RUNNING=false
fi

# Check Kafka Broker
echo -n "Kafka Broker (port 9092): "
if lsof -Pi :9092 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Running${NC}"
    KAFKA_RUNNING=true
else
    echo -e "${RED}✗ Not running${NC}"
    KAFKA_RUNNING=false
fi

echo ""

# If Kafka is running, show topics
if [ "$KAFKA_RUNNING" = true ]; then
    echo "Kafka Topics:"
    if command -v kafka-topics &> /dev/null; then
        kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || echo "  (none or connection error)"
    elif command -v kafka-topics.sh &> /dev/null; then
        kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null || echo "  (none or connection error)"
    else
        echo "  (kafka-topics command not found)"
    fi
    echo ""
fi

# Summary and recommendations
echo "Status Summary:"
echo ""

if [ "$ZK_RUNNING" = true ] && [ "$KAFKA_RUNNING" = true ]; then
    echo -e "${GREEN}✓ Ready to run streaming tests${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Ensure topic 'task-topic' exists:"
    echo "   kafka-topics --create --topic task-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1"
    echo ""
    echo "2. Start consumer:"
    echo "   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4 kafka_stream/consumer.py"
    echo ""
    echo "3. Start producer:"
    echo "   python3 kafka_stream/producer.py"
    
elif [ "$ZK_RUNNING" = true ]; then
    echo -e "${YELLOW}⚠ ZooKeeper running but Kafka broker not started${NC}"
    echo ""
    echo "Start Kafka broker:"
    echo "  kafka-server-start /usr/local/etc/kafka/server.properties"
    echo "  # OR"
    echo "  bin/kafka-server-start.sh config/server.properties"
    
elif [ "$KAFKA_RUNNING" = true ]; then
    echo -e "${YELLOW}⚠ Kafka running but ZooKeeper not detected${NC}"
    echo "  (This might be OK if using KRaft mode)"
    
else
    echo -e "${RED}✗ Kafka infrastructure not running${NC}"
    echo ""
    echo "Start services in order:"
    echo ""
    echo "1. Start ZooKeeper (Terminal 1):"
    echo "   zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties"
    echo "   # OR"
    echo "   bin/zookeeper-server-start.sh config/zookeeper.properties"
    echo ""
    echo "2. Start Kafka Broker (Terminal 2):"
    echo "   kafka-server-start /usr/local/etc/kafka/server.properties"
    echo "   # OR"
    echo "   bin/kafka-server-start.sh config/server.properties"
fi

echo ""
