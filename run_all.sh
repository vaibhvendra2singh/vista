#!/bin/bash

# ==============================================================================
#   🛡️ BIG DATA FINANCIAL FRAUD DETECTION & TRANSACTION ANOMALY SYSTEM
#             Full-Project End-to-End Orchestrator
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR" || exit 1

export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
source venv/bin/activate 2>/dev/null || true

clear
echo "=============================================================================="
echo "          🛡️  VISTA: BIG DATA FINANCIAL FRAUD & ANOMALY SYSTEM"
echo "             Distributed Real-Time Streaming & ML Pipeline"
echo "=============================================================================="
echo " Authors: Aman Bhandhari & Vaibhvendra Singh"
echo " Stack: Apache Spark (PySpark), Apache Kafka, Flask Dashboard"
echo "=============================================================================="
echo ""

# 1. Check & Ensure Kafka Broker is Running
echo "[1/4] Verifying Apache Kafka Service..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo "  -> Starting Apache Kafka broker via Homebrew..."
    brew services start kafka
    sleep 4
    if ! nc -z localhost 9092 2>/dev/null; then
        echo "  [ERROR] Kafka failed to start on port 9092. Please run 'brew services start kafka' manually."
        exit 1
    fi
fi
echo "  -> Apache Kafka Broker is ONLINE on port 9092. ✅"

# 2. Check if Models are trained
echo ""
echo "[2/4] Checking Trained Model Artifacts..."
if [ ! -d "models/best_model" ] || [ ! -f "outputs/model_benchmarks.json" ]; then
    echo "  -> Training 5 PySpark ML models on 80% dataset (One-time)..."
    python 1_train_models.py
else
    echo "  -> Models are ready (Best Model: Gradient-Boosted Trees, ROC-AUC: 0.8407). ✅"
fi

# 3. Launch Web Dashboard & Spark Streaming Engine in Background
echo ""
echo "[3/4] Launching Web Dashboard & Spark Streaming Engine..."

# Kill previous background runners
pkill -f "app.py" 2>/dev/null
pkill -f "3_spark_streaming.py" 2>/dev/null

# Launch Flask Web Dashboard on port 5050
python app.py > outputs/dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo "  -> Web Dashboard active at: http://localhost:5050 (PID: $DASHBOARD_PID) ✅"

# Launch Spark Streaming Engine in Background
python 3_spark_streaming.py > outputs/spark_streaming.log 2>&1 &
SPARK_PID=$!
echo "  -> Spark Streaming Engine active in background (PID: $SPARK_PID) ✅"

# Automatically open browser on macOS
sleep 1
open "http://localhost:5050" 2>/dev/null

# Function to cleanly stop all background processes on exit
cleanup() {
    echo ""
    echo "Shutting down background services (Dashboard PID: $DASHBOARD_PID, Spark PID: $SPARK_PID)..."
    kill "$DASHBOARD_PID" "$SPARK_PID" 2>/dev/null
    echo "Project stopped cleanly."
    exit 0
}
trap cleanup SIGINT SIGTERM EXIT

# 4. Run Interactive Transaction Producer in Foreground
echo ""
echo "[4/4] Starting Real-Time Kafka Transaction Streamer..."
echo "=============================================================================="
echo " 🌐 DASHBOARD OPENED: Viewing live at http://localhost:5050"
echo " 🚀 Select a mode below to start streaming transactions directly through Kafka:"
echo "=============================================================================="

# Launch the interactive producer in the foreground
python 2_kafka_producer.py
