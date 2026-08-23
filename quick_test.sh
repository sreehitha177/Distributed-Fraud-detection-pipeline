#!/bin/bash
# Quick Test Script for Credit Card Fraud Detection System
# Run this to verify everything is working

set -e  # Exit on error

echo "=================================================="
echo "  Credit Card Fraud Detection - Quick Test"
echo "=================================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Python
echo -n "Checking Python... "
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    echo -e "${GREEN}✓ Python $PYTHON_VERSION${NC}"
else
    echo -e "${RED}✗ Python not found${NC}"
    exit 1
fi

# Check Java
echo -n "Checking Java... "
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2)
    echo -e "${GREEN}✓ Java $JAVA_VERSION${NC}"
else
    echo -e "${RED}✗ Java not found${NC}"
    exit 1
fi

# Check PySpark
echo -n "Checking PySpark... "
if python3 -c "import pyspark" 2>/dev/null; then
    PYSPARK_VERSION=$(python3 -c "import pyspark; print(pyspark.__version__)")
    echo -e "${GREEN}✓ PySpark $PYSPARK_VERSION${NC}"
else
    echo -e "${RED}✗ PySpark not installed${NC}"
    echo "Run: pip3 install --user -r requirements.txt"
    exit 1
fi

# Check Kafka Python
echo -n "Checking kafka-python... "
if python3 -c "import kafka" 2>/dev/null; then
    echo -e "${GREEN}✓ kafka-python installed${NC}"
else
    echo -e "${RED}✗ kafka-python not installed${NC}"
    echo "Run: pip3 install --user -r requirements.txt"
    exit 1
fi

# Check data files
echo -n "Checking training data... "
if [ -f "ml_model/creditcard.csv" ]; then
    DATA_SIZE=$(du -h ml_model/creditcard.csv | cut -f1)
    echo -e "${GREEN}✓ Found ($DATA_SIZE)${NC}"
else
    echo -e "${RED}✗ creditcard.csv not found${NC}"
    exit 1
fi

echo -n "Checking test data... "
if [ -d "ml_model/testdata.csv" ]; then
    echo -e "${GREEN}✓ Found${NC}"
else
    echo -e "${RED}✗ testdata.csv not found${NC}"
    exit 1
fi

# Check model
echo -n "Checking trained model... "
if [ -d "ml_model/trained_model" ]; then
    echo -e "${GREEN}✓ Found${NC}"
else
    echo -e "${RED}✗ Model not found${NC}"
    echo "Run: cd ml_model && python3 train.py"
    exit 1
fi

# Check Kafka (optional)
echo -n "Checking Kafka... "
if command -v kafka-topics &> /dev/null || command -v kafka-topics.sh &> /dev/null; then
    echo -e "${GREEN}✓ Kafka installed${NC}"
    KAFKA_INSTALLED=true
else
    echo -e "${YELLOW}⚠ Kafka not found (optional for now)${NC}"
    KAFKA_INSTALLED=false
fi

echo ""
echo "=================================================="
echo "  Running ML Model Test"
echo "=================================================="
echo ""

# Run model test
python3 test_model.py

echo ""
echo "=================================================="
echo "  Test Summary"
echo "=================================================="
echo ""

echo -e "${GREEN}✓ All prerequisites met${NC}"
echo -e "${GREEN}✓ ML model working correctly${NC}"
echo ""

if [ "$KAFKA_INSTALLED" = true ]; then
    echo -e "${GREEN}✓ Ready for full streaming tests${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Start ZooKeeper in Terminal 1"
    echo "2. Start Kafka in Terminal 2"
    echo "3. Create topic in Terminal 3"
    echo "4. Start consumer in Terminal 4"
    echo "5. Start producer in Terminal 5"
    echo ""
    echo "See KAFKA_SETUP_GUIDE.md for details"
else
    echo -e "${YELLOW}⚠ Install Kafka to run streaming tests${NC}"
    echo ""
    echo "Install with Homebrew:"
    echo "  brew install kafka"
    echo ""
    echo "Or see KAFKA_SETUP_GUIDE.md for manual installation"
fi

echo ""
echo "=================================================="
echo "  Documentation Files"
echo "=================================================="
echo "• PERFORMANCE_REPORT.md - Current metrics"
echo "• KAFKA_SETUP_GUIDE.md - Installation guide"
echo "• TESTING_STATUS.md - Overall status"
echo "=================================================="
