import os
import json
import logging
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import GBTClassificationModel, RandomForestClassificationModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"
OUTPUT_LOG_FILE = "outputs/live_predictions.json"

# Calibrated Banking Risk Tier Thresholds
HIGH_RISK_THRESHOLD = 0.15      # >= 15.0% -> Immediate Block
ELEVATED_RISK_THRESHOLD = 0.09  # 9.0% to 14.9% -> Flag for Review / OTP
# < 9.0% -> Approved Legitimate

def init_spark():
    logging.info("Initializing PySpark Engine for Real-Time Streaming Evaluation...")
    return (
        SparkSession.builder
        .appName("FinancialFraud_StreamScorer")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def load_trained_models(spark):
    prep_pipeline_path = "models/prep_pipeline"
    best_model_path = "models/best_model"
    meta_path = "models/model_metadata.json"

    if not os.path.exists(prep_pipeline_path) or not os.path.exists(best_model_path):
        logging.error("Trained model files not found in models/! Run 'python 1_train_models.py' first.")
        return None, None, None

    with open(meta_path) as f:
        meta = json.load(f)

    logging.info(f"Loading Preprocessing Pipeline and Best Model: {meta['best_model_name']}...")
    prep_model = PipelineModel.load(prep_pipeline_path)
    
    try:
        classifier = GBTClassificationModel.load(best_model_path)
    except Exception:
        classifier = RandomForestClassificationModel.load(best_model_path)
        
    return prep_model, classifier, meta

def start_streaming():
    spark = init_spark()
    prep_model, classifier, meta = load_trained_models(spark)
    if not prep_model:
        return

    logging.info(f"Connecting to Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="spark-fraud-consumer",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return

    logging.info("=================================================================")
    logging.info(f" 🚀 SPARK STREAMING ENGINE ONLINE | Model: {meta['best_model_name']}")
    logging.info(f" Block Threshold: >={HIGH_RISK_THRESHOLD*100:.1f}% | Flag Threshold: >={ELEVATED_RISK_THRESHOLD*100:.1f}%")
    logging.info("=================================================================")

    os.makedirs("outputs", exist_ok=True)
    live_history = []
    
    if os.path.exists(OUTPUT_LOG_FILE):
        try:
            with open(OUTPUT_LOG_FILE) as f:
                live_history = json.load(f)
        except Exception:
            live_history = []

    for msg in consumer:
        raw_txn = msg.value
        txn_id = raw_txn.get("TransactionID", "Unknown")
        amt = float(raw_txn.get("TransactionAmt", 0.0))
        card = raw_txn.get("card4", "Unknown")
        device = raw_txn.get("DeviceType", "Unknown")
        src = raw_txn.get("source", "Stream")
        true_label = int(float(raw_txn.get("isFraud", 0.0)))

        # Reload live history from disk on each event so external resets/clears are respected
        if os.path.exists(OUTPUT_LOG_FILE):
            try:
                with open(OUTPUT_LOG_FILE) as f:
                    live_history = json.load(f)
            except Exception:
                live_history = []
        else:
            live_history = []

        try:
            row_dict = {
                "TransactionID": int(txn_id) if str(txn_id).isdigit() else 9999999,
                "isFraud": float(true_label),
                "TransactionDT": float(raw_txn.get("TransactionDT", 0.0)),
                "TransactionAmt": amt,
                "ProductCD": str(raw_txn.get("ProductCD", "W")),
                "card1": float(raw_txn.get("card1", 1000.0)),
                "card2": float(raw_txn.get("card2", 100.0)),
                "card4": str(card),
                "card6": str(raw_txn.get("card6", "debit")),
                "addr1": float(raw_txn.get("addr1", 100.0)),
                "P_emaildomain": str(raw_txn.get("P_emaildomain", "Unknown")),
                "C1": float(raw_txn.get("C1", 1.0)),
                "C2": float(raw_txn.get("C2", 1.0)),
                "C5": float(raw_txn.get("C5", 0.0)),
                "C9": float(raw_txn.get("C9", 0.0)),
                "C11": float(raw_txn.get("C11", 1.0)),
                "C13": float(raw_txn.get("C13", 1.0)),
                "C14": float(raw_txn.get("C14", 1.0)),
                "D1": float(raw_txn.get("D1", 0.0)),
                "D2": float(raw_txn.get("D2", 0.0)),
                "D4": float(raw_txn.get("D4", 0.0)),
                "D10": float(raw_txn.get("D10", 0.0)),
                "D15": float(raw_txn.get("D15", 0.0)),
                "DeviceType": str(device),
                "DeviceInfo": str(raw_txn.get("DeviceInfo", "Unknown"))
            }

            spark_row_df = spark.createDataFrame([row_dict])
            transformed_row = prep_model.transform(spark_row_df)
            scored_row = classifier.transform(transformed_row).collect()[0]

            prediction = int(scored_row["prediction"])
            
            try:
                prob_vec = scored_row["probability"].toArray()
                fraud_prob = float(prob_vec[1])
            except Exception:
                fraud_prob = 1.0 if prediction == 1 else 0.0

            # Multi-Tier Risk Evaluation
            if fraud_prob >= HIGH_RISK_THRESHOLD or prediction == 1:
                status_flag = "🚨 BLOCKED FRAUD"
                status_code = "BLOCKED"
                color_prefix = "\033[91m"
            elif fraud_prob >= ELEVATED_RISK_THRESHOLD:
                status_flag = "⚠️ FLAGGED SUSPICIOUS"
                status_code = "SUSPICIOUS"
                color_prefix = "\033[93m"
            else:
                status_flag = "✅ APPROVED"
                status_code = "APPROVED"
                color_prefix = "\033[92m"

            color_reset = "\033[0m"

            print(f"\n{color_prefix}------------------------------------------------------------{color_reset}")
            print(f"{color_prefix}[KAFKA EVENT] Txn #{txn_id} | Amount: ${amt:,.2f} | Card: {card}{color_reset}")
            print(f"{color_prefix}[MODEL SCORING] Risk Probability: {fraud_prob * 100:.1f}% -> {status_flag}{color_reset}")
            print(f"{color_prefix}True Label: {'🚨 Real Fraud' if true_label == 1 else '✅ Legit'} | Source: {src}{color_reset}")
            print(f"{color_prefix}------------------------------------------------------------{color_reset}")

            # Explainability / XAI factor derivation (in plain everyday English)
            risk_factors = []
            if amt >= 2000:
                risk_factors.append(f"💰 Huge Amount of Money: At ${amt:,.2f}, this is much bigger than a normal grocery or shopping bill.")
            elif amt >= 800:
                risk_factors.append(f"💵 Above Average Amount: Spending ${amt:,.2f} is higher than what people typically pay for everyday things.")
            else:
                risk_factors.append(f"🛒 Normal Price: ${amt:,.2f} is a normal, everyday purchase amount.")
            
            c1_val = float(raw_txn.get("C1", 1.0))
            if c1_val >= 50:
                risk_factors.append(f"🤖 Computer Bot Speed: This card was swiped {int(c1_val)} times in just a few seconds! No human can press buttons that fast—only a hacker bot.")
            elif c1_val >= 10:
                risk_factors.append(f"⚡ Rapid Repeated Swipes: Someone tried using this card {int(c1_val)} times back-to-back within a minute.")
            else:
                risk_factors.append("👤 Normal Human Speed: Swiped just once like a real customer standing at a counter.")
                
            pcd = str(raw_txn.get("ProductCD", "W")).upper()
            if pcd == "C":
                risk_factors.append("🌐 High-Risk Wire Transfer: This payment uses Category 'C' (direct digital transfer), which is the #1 way online scammers quickly steal money.")
            else:
                risk_factors.append("🛍️ Safe Store Category: Standard web or store checkout.")
                
            if str(card).lower() in ["discover", "unknown"]:
                risk_factors.append(f"💳 Unfamiliar Card Network: '{card}' cards have shown higher rates of stolen cards in past fraud logs.")
                
            if fraud_prob >= HIGH_RISK_THRESHOLD:
                risk_factors.append(f"🤖 Model Verdict: The model scored the danger level at {fraud_prob*100:.1f}%. Since anything above 15% is dangerous, it immediately stopped the theft!")
            elif fraud_prob >= ELEVATED_RISK_THRESHOLD:
                risk_factors.append(f"📱 Model Verdict: The model noticed mild warning signs ({fraud_prob*100:.1f}% danger). It sent a text message with an OTP code to make sure it's really the owner.")
            else:
                risk_factors.append(f"🟢 Model Verdict: Everything checked out safe ({fraud_prob*100:.1f}% risk). The payment went through in less than a second.")

            # Record event in live history
            event_record = {
                "TransactionID": txn_id,
                "TransactionAmt": amt,
                "Card": card,
                "Device": device,
                "ProductCD": pcd,
                "C1": c1_val,
                "RiskProbability": round(fraud_prob, 4),
                "Prediction": 1 if status_code != "APPROVED" else 0,
                "Status": status_code,
                "TrueLabel": "FRAUD" if true_label == 1 else "LEGIT",
                "Timestamp": raw_txn.get("live_timestamp", "Now"),
                "Source": src,
                "RiskFactors": risk_factors
            }
            
            live_history.append(event_record)
            if len(live_history) > 500:
                live_history.pop(0)

            # Persist for Frontend
            with open(OUTPUT_LOG_FILE, "w") as f:
                json.dump(live_history, f, indent=2)

        except Exception as e:
            logging.error(f"Error scoring transaction {txn_id}: {e}")

if __name__ == "__main__":
    start_streaming()
