import os
import sys
import time
import subprocess

def print_banner():
    print("""
========================================================================
         🛡️  VISTA: BIG DATA FINANCIAL FRAUD & ANOMALY SYSTEM
========================================================================
    Authors: Aman Bhandhari & Vaibhvendra Singh
    Technologies: Apache Spark (PySpark), Apache Kafka, Flask Dashboard
========================================================================
""")

def check_kafka_status():
    print("[1/3] Checking Apache Kafka & Zookeeper status...")
    res = subprocess.run(["pgrep", "-f", "kafka.Kafka"], capture_output=True, text=True)
    if res.stdout.strip():
        print("  -> Kafka Broker is RUNNING.")
        return True
    else:
        print("  -> Kafka Broker is NOT running. Attempting to start via Homebrew...")
        subprocess.run(["brew", "services", "start", "kafka"], capture_output=True)
        time.sleep(3)
        res_after = subprocess.run(["pgrep", "-f", "kafka.Kafka"], capture_output=True, text=True)
        if res_after.stdout.strip():
            print("  -> Kafka Broker successfully started!")
            return True
        else:
            print("  [NOTE] Ensure Kafka is running on port 9092.")
            return False

def check_models_trained():
    print("[2/3] Checking Trained Model status...")
    if os.path.exists("models/best_model") and os.path.exists("outputs/model_benchmarks.json"):
        print("  -> Trained models & benchmarks are READY (Best: GBT, ROC-AUC: 0.8407).")
        return True
    else:
        print("  -> Models not yet trained. Need to run offline training step.")
        return False

def main():
    print_banner()
    check_kafka_status()
    models_ready = check_models_trained()

    print("\n------------------------------------------------------------------------")
    print("Main Control Menu:")
    print(" [1] Train 5 ML Models on 80% Dataset (PySpark Batch Training)")
    print(" [2] Start Web Dashboard + Real-Time Spark Streaming Consumer")
    print(" [3] Run Entire System (Dashboard + Streaming Engine)")
    print(" [4] Exit")
    print("------------------------------------------------------------------------")
    choice = input("Enter choice (1-4): ").strip()

    venv_python = "venv/bin/python" if os.path.exists("venv/bin/python") else sys.executable

    if choice == "1":
        print("\n[STARTING] Training 5 ML Models in PySpark...")
        subprocess.run([venv_python, "1_train_models.py"])
    elif choice in ["2", "3"]:
        if not models_ready:
            print("\n[WARNING] Please train models first (Option 1) before starting the stream consumer.")
            return
        
        # Kill previous runners
        subprocess.run(["pkill", "-f", "app.py"], capture_output=True)
        
        # Start Dashboard
        print("\n[STARTING] Web Dashboard at http://localhost:5050...")
        dash_proc = subprocess.Popen([venv_python, "app.py"])
        time.sleep(1)
        subprocess.run(["open", "http://localhost:5050"], capture_output=True)

        print("[STARTING] PySpark Real-Time Streaming Engine...")
        try:
            subprocess.run([venv_python, "3_spark_streaming.py"])
        finally:
            dash_proc.terminate()
    elif choice == "4":
        print("Exiting.")
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
