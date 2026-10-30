import os
import time
import json
import random
import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"\n[ERROR] Unable to connect to Kafka Broker on {KAFKA_BOOTSTRAP_SERVERS}.")
        print("Please make sure Kafka is running (e.g. run 'brew services start kafka').")
        return None

def stream_20_percent_test_data(producer):
    csv_path = "data/stream_test_data.csv"
    if not os.path.exists(csv_path):
        print(f"\n[ERROR] File '{csv_path}' not found! Please run 'python 1_train_models.py' first.")
        return

    print(f"\n[INFO] Loading 20% test slice from '{csv_path}'...")
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"[SUCCESS] Loaded {total_rows} test transactions. Starting real-time Kafka stream...\n")
    print("Press Ctrl+C anytime to pause/stop.\n")
    print("--------------------------------------------------------------------------------")
    print(f"{'Txn ID':<12} | {'Amount ($)':<12} | {'Product':<8} | {'Card':<10} | {'True Label':<10}")
    print("--------------------------------------------------------------------------------")

    count = 0
    try:
        for _, row in df.iterrows():
            payload = row.to_dict()
            payload["live_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            payload["source"] = "20% Held-Out Test Set"
            
            # Ensure proper typing
            payload["TransactionID"] = int(payload["TransactionID"])
            payload["TransactionAmt"] = float(payload.get("TransactionAmt", 0.0))
            payload["isFraud"] = float(payload.get("isFraud", 0.0))

            producer.send(KAFKA_TOPIC, payload)
            producer.flush()

            is_fraud_txt = "🚨 FRAUD" if int(payload["isFraud"]) == 1 else "✅ Legit"
            amt_txt = f"${payload['TransactionAmt']:,.2f}"
            print(f"{payload['TransactionID']:<12} | {amt_txt:<12} | {str(payload.get('ProductCD', 'W')):<8} | {str(payload.get('card4', 'Visa')):<10} | {is_fraud_txt:<10}")

            count += 1
            time.sleep(1.0)
    except KeyboardInterrupt:
        print(f"\n[STREAM PAUSED] Streamed {count} transactions to Kafka.")

def generate_random_transactions(producer):
    print("\n[INFO] Starting High-Fraud Synthetic Transaction Stream...")
    print("Simulating realistic live card swipes with ~40% High-Risk Fraud Attacks...")
    print("Press Ctrl+C anytime to pause/stop.\n")
    print("--------------------------------------------------------------------------------")
    print(f"{'Txn ID':<12} | {'Amount ($)':<12} | {'Product':<8} | {'Card':<10} | {'Simulated Type':<15}")
    print("--------------------------------------------------------------------------------")

    cards = ["visa", "mastercard", "discover"]
    devices = ["mobile", "desktop", "Unknown"]

    txn_id = 9000000
    count = 0
    try:
        while True:
            txn_id += 1
            count += 1
            # 40% probability of creating detectable fraud pattern
            is_fraud_attack = random.random() < 0.40

            if is_fraud_attack:
                amt = round(random.uniform(500.0, 4800.0), 2)
                product = "C" # High-risk digital cash/crypto product category in IEEE-CIS
                c1 = random.randint(50, 150) # High velocity velocity count triggers model
                c2 = random.randint(40, 120)
                d1 = 0.0
                addr1 = 0.0
                device = "Unknown"
                source_lbl = "🚨 FRAUD ATTACK"
            else:
                amt = round(random.uniform(12.0, 180.0), 2)
                product = "W"
                c1 = 1.0
                c2 = 1.0
                d1 = random.uniform(10.0, 100.0)
                addr1 = 325.0
                device = random.choice(["mobile", "desktop"])
                source_lbl = "✅ Normal Purchase"

            payload = {
                "TransactionID": txn_id,
                "isFraud": 1.0 if is_fraud_attack else 0.0,
                "TransactionDT": int(time.time()),
                "TransactionAmt": amt,
                "ProductCD": product,
                "card1": 15000.0,
                "card2": 250.0,
                "card4": random.choice(cards),
                "card6": "credit" if is_fraud_attack else "debit",
                "addr1": addr1,
                "P_emaildomain": "anonymous.com" if is_fraud_attack else "gmail.com",
                "C1": c1,
                "C2": c2,
                "C5": 0.0, "C9": 0.0, "C11": 5.0, "C13": 5.0, "C14": 5.0,
                "D1": d1, "D2": 0.0, "D4": 0.0, "D10": 0.0, "D15": 0.0,
                "DeviceType": device,
                "live_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "source": "Synthetic Stream"
            }

            producer.send(KAFKA_TOPIC, payload)
            producer.flush()

            amt_txt = f"${amt:,.2f}"
            print(f"{txn_id:<12} | {amt_txt:<12} | {product:<8} | {payload['card4']:<10} | {source_lbl:<15}")
            time.sleep(1.0)
    except KeyboardInterrupt:
        print(f"\n[STREAM PAUSED] Streamed {count} transactions to Kafka.")

def inject_manual_scam(producer):
    print("\n==============================================================")
    print("      LIVE VIVA DEMO: INJECT HIGH-RISK SCAM TRANSACTION")
    print("==============================================================")
    try:
        amt_str = input("Enter Transaction Amount in USD (e.g., 2500.00): ").strip()
        amt = float(amt_str) if amt_str else 2500.0
        
        card = input("Enter Card Network [visa / mastercard] (default: visa): ").strip() or "visa"
        device = input("Enter Device [mobile / desktop / Unknown] (default: Unknown): ").strip() or "Unknown"
        
        sim_id = random.randint(9900000, 9999999)
        payload = {
            "TransactionID": sim_id,
            "isFraud": 1.0,
            "TransactionDT": int(time.time()),
            "TransactionAmt": amt,
            "ProductCD": "C", # Triggers high-risk digital transfer in IEEE-CIS
            "card1": 15000.0,
            "card2": 250.0,
            "card4": card,
            "card6": "credit",
            "addr1": 0.0,
            "P_emaildomain": "anonymous.com",
            "C1": 95.0, # Rapid repeated attempts
            "C2": 85.0,
            "C5": 0.0, "C9": 0.0, "C11": 10.0, "C13": 10.0, "C14": 10.0,
            "D1": 0.0, "D2": 0.0, "D4": 0.0, "D10": 0.0, "D15": 0.0,
            "DeviceType": device,
            "live_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": "Manual Viva Injection"
        }

        producer.send(KAFKA_TOPIC, payload)
        producer.flush()
        
        print("\n--------------------------------------------------------------")
        print(f"✅ [SUCCESS] Injected High-Risk Txn #{sim_id} into Kafka topic '{KAFKA_TOPIC}'!")
        print(f"Amount: ${amt:,.2f} | Card: {card} | Device: {device}")
        print("Check your Web Dashboard to see it flagged in real time!")
        print("--------------------------------------------------------------\n")
    except Exception as e:
        print(f"[ERROR] Failed to inject custom transaction: {e}")

def main():
    producer = get_producer()
    if not producer:
        return

    while True:
        print("\n==============================================================")
        print("      TERMINAL 2: REAL-TIME KAFKA STREAM PRODUCER")
        print("==============================================================")
        print("Select streaming mode:")
        print(" [1] Stream 20% Unseen Test Data (Authentic IEEE-CIS Dataset)")
        print(" [2] Continuous Random Transaction Generator (High-Fraud Attacks: ~40%)")
        print(" [3] Inject a Single Custom Scam Payment (Instant Viva Test)")
        print(" [4] Exit")
        print("--------------------------------------------------------------")
        choice = input("Enter choice (1-4): ").strip()

        if choice == "1":
            stream_20_percent_test_data(producer)
        elif choice == "2":
            generate_random_transactions(producer)
        elif choice == "3":
            inject_manual_scam(producer)
        elif choice == "4":
            print("\nExiting Producer CLI. Goodbye!")
            break
        else:
            print("[WARN] Invalid option. Please select 1, 2, 3, or 4.")

if __name__ == "__main__":
    main()
