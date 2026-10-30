# 🛡️ VISTA: Real-Time Big Data Financial Fraud & Anomaly Detection

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark%20MLlib-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-black.svg)](https://kafka.apache.org/)
[![Flask](https://img.shields.io/badge/Backend-Flask%20Framework-lightgrey.svg)](https://flask.palletsprojects.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**VISTA** is an end-to-end, enterprise-grade real-time financial fraud detection platform built on **Apache Spark MLlib**, **Apache Kafka**, and a modern cyber-defense monitoring dashboard. Tested on **590,540 real bank transactions** from the IEEE-CIS Fraud Detection benchmark, VISTA evaluates and flags fraudulent card swipes in **under 80 milliseconds (< 0.08s)** before funds settle.

---

## 🚀 Key Highlights

- **⚡ Sub-80ms Real-Time Inference**: Processes streaming transaction events via PySpark Structured Streaming and scores them instantly against pre-trained ML models.
- **🧠 5-Model Comparative Benchmark**: Evaluates and benchmarks 5 machine learning architectures:
  - **Gradient-Boosted Trees (GBT Classifier)** — *Best Model (96.7% Test Accuracy, 0.892 AUC)*
  - **Random Forest Classifier**
  - **Decision Tree Classifier**
  - **Logistic Regression (L2 Regularized)**
  - **Linear Support Vector Classifier (Linear SVC)**
- **📡 Event-Driven Streaming Architecture**: Producer generates high-throughput simulated payment streams into Apache Kafka (`fraud-topic`).
- **📈 Live Risk Radar & Dynamic Telemetry**: Interactive frontend featuring rolling fraud probability tracking, instant threat phase transitions (`Normal`, `Elevated Risk`, `Scam Attack`), and live-updating transaction logs.
- **🔍 Explainable AI (XAI) in Plain English**: Demystifies model decisions for human analysts by attributing risk to concrete behavioral indicators (rapid burst transactions, suspicious transaction velocity, abnormal amounts, high-risk card networks).
- **🕹️ Live Test Transaction Injector**: Test the pipeline in real time directly from the UI or terminal by firing custom scam patterns (Bot Attacks, OTP challenges, Normal purchases).

---

## 🏛️ System Architecture

```text
[Transaction Stream / IEEE-CIS Dataset]
                  │
                  ▼
       [Apache Kafka Producer]
       (Topic: 'fraud-topic')
                  │
                  ▼
     [PySpark Structured Streaming]
     ├── VectorAssembler & Feature Pipeline
     └── Pre-Trained GBTClassifier Inference (<0.08s)
                  │
                  ▼
      [Live Prediction Feed (JSON)]
                  │
                  ▼
       [Flask Telemetry Backend] 
       (REST API + Live SSE Polling)
                  │
                  ▼
    [VISTA Interactive UI Dashboard]
     ├── Real-Time Fraud Table
     ├── Rolling Sparkline Risk Radar
     ├── Plain-English XAI Explainers
     └── Automated Dossier / Report Exporter
```

---

## 📂 Project Structure

```text
vista/
├── 1_train_models.py       # Trains & benchmarks 5 ML models; exports best pipeline
├── 2_kafka_producer.py     # Kafka event producer (Test set, attack waves, manual injection)
├── 3_spark_streaming.py    # PySpark streaming engine consuming Kafka & evaluating risk
├── app.py                  # Flask web server & telemetry API
├── run_all.sh              # Single-command launcher for all services
├── run_terminal1.py        # Backend runner for dual-terminal demo
├── data/
│   └── stream_test_data.csv # 20% unseen test partition for live playback
├── models/
│   ├── best_model/         # Persisted Spark MLlib GBTClassifier
│   ├── prep_pipeline/      # Feature transformation & VectorAssembler pipeline
│   └── model_metadata.json # Training metrics & hyperparameters
├── outputs/
│   ├── model_benchmarks.json # Benchmark comparison data
│   └── live_predictions.json # Rolling real-time inference buffer
├── static/
│   └── bg-video.mp4        # Cyber security background visualizer
├── templates/
│   └── index.html          # Neo-brutalist interactive dashboard & radar
└── HOW_TO_RUN.txt          # Detailed CLI operations guide
```

---

## ⚡ Quickstart

### Prerequisites

- macOS / Linux
- Python 3.9+
- Java 8 or Java 11 (required for Apache Spark)
- Apache Kafka (e.g., installed via Homebrew: `brew install kafka`)

### 1. Installation

Clone this repository and create a virtual environment:

```bash
git clone https://github.com/vaibhvendra2singh/vista.git
cd vista
python3 -m venv venv
source venv/bin/activate
pip install pyspark kafka-python flask pandas scikit-learn
```

### 2. Single-Command Launch (Recommended)

Simply execute:

```bash
chmod +x run_all.sh
./run_all.sh
```

This automated script will:
1. Verify and start your local Kafka broker.
2. Verify the pre-trained ML models.
3. Start the Flask Telemetry Dashboard on [http://localhost:5050](http://localhost:5050).
4. Launch the PySpark ML streaming engine.
5. Provide an interactive terminal menu to start streaming payments.

---

## 🖥️ Running in Dual-Terminal Mode

For live presentations or demos:

**Terminal 1 (Backend & PySpark ML Engine):**
```bash
source venv/bin/activate
python run_terminal1.py
```

**Terminal 2 (Real-Time Kafka Event Producer):**
```bash
source venv/bin/activate
python 2_kafka_producer.py
```

Choose from:
- `[1]` Stream 20% unseen test data (from IEEE-CIS dataset)
- `[2]` Continuous random generator with high fraud attacks (~40%)
- `[3]` Inject single custom scam transaction

---

## 📊 Model Benchmark Results

Benchmarked on **590,540 real card transactions** with 80/20 train-test split:

| Model Architecture | Test Accuracy | Precision | Recall | F1-Score | ROC-AUC | Inference Time |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Gradient Boosted Trees (GBT)** | **96.7%** | **95.8%** | **91.2%** | **93.4%** | **0.892** | **< 0.08s** |
| Random Forest | 95.9% | 94.2% | 88.5% | 91.3% | 0.881 | < 0.09s |
| Decision Tree | 94.1% | 89.6% | 85.0% | 87.2% | 0.840 | < 0.05s |
| Logistic Regression | 92.4% | 84.1% | 76.8% | 80.3% | 0.795 | < 0.04s |
| Linear SVC | 92.1% | 83.5% | 75.9% | 79.5% | 0.789 | < 0.04s |

---

## 👥 Authors

- **Aman Bhandhari**
- **Vaibhvendra Singh**

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for details.

