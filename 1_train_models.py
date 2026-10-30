import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
    LinearSVC
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# Silence verbose Spark logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

def init_spark():
    logging.info("Initializing Apache Spark Session (Local Distributed Engine)...")
    return (
        SparkSession.builder
        .appName("FinancialFraud_ModelTraining")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

def prepare_data(spark):
    logging.info("Loading IEEE-CIS Raw Dataset...")
    tx_path = "ieee-fraud-detection/train_transaction.csv"
    id_path = "ieee-fraud-detection/train_identity.csv"

    # Select critical informative features for scalable, high-speed training
    selected_cols = [
        "TransactionID", "isFraud", "TransactionDT", "TransactionAmt",
        "ProductCD", "card1", "card2", "card4", "card6", "addr1",
        "P_emaildomain", "C1", "C2", "C5", "C9", "C11", "C13", "C14",
        "D1", "D2", "D4", "D10", "D15"
    ]
    
    df_tx = spark.read.option("header", "true").option("inferSchema", "true").csv(tx_path).select(selected_cols)
    df_id = spark.read.option("header", "true").option("inferSchema", "true").csv(id_path).select("TransactionID", "DeviceType", "DeviceInfo")
    
    # Left join with identity telemetry
    df = df_tx.join(df_id, on="TransactionID", how="left")
    
    # Fill categorical nulls with 'Unknown'
    cat_cols = ["ProductCD", "card4", "card6", "P_emaildomain", "DeviceType"]
    for c in cat_cols:
        df = df.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("Unknown")))
        
    num_cols = ["TransactionAmt", "card1", "card2", "addr1", "C1", "C2", "C5", "C9", "C11", "C13", "C14", "D1", "D2", "D4", "D10", "D15"]
    for c in num_cols:
        df = df.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
        
    df = df.withColumn("isFraud", F.col("isFraud").cast("double"))
    
    # Stratified/Temporal ordering: 80% Train, 20% Held-out Test
    logging.info("Sorting chronologically by TransactionDT for 80% Train / 20% Test Split...")
    # Calculate cutoff for 80% time
    quantiles = df.approxQuantile("TransactionDT", [0.80], 0.01)
    split_dt = quantiles[0]
    
    train_df = df.filter(F.col("TransactionDT") <= split_dt).cache()
    test_df = df.filter(F.col("TransactionDT") > split_dt).cache()
    
    train_count = train_df.count()
    test_count = test_df.count()
    logging.info(f"Data Split Summary: Train rows={train_count:,} (80%), Test rows={test_count:,} (20%)")
    
    # Export the 20% test partition for Terminal 2 Kafka streaming
    os.makedirs("data", exist_ok=True)
    test_export_path = "data/stream_test_data.csv"
    logging.info(f"Saving 20% test slice to {test_export_path} for real-time Kafka streaming...")
    test_df.limit(10000).toPandas().to_csv(test_export_path, index=False)
    logging.info("20% test streaming slice exported successfully.")
    
    return train_df, test_df, cat_cols, num_cols

def build_pipeline(cat_cols, num_cols):
    stages = []
    indexed_cat_cols = []
    
    for c in cat_cols:
        indexer = StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        stages.append(indexer)
        indexed_cat_cols.append(f"{c}_idx")
        
    assembler_inputs = num_cols + indexed_cat_cols
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features", handleInvalid="keep")
    stages.append(assembler)
    
    return Pipeline(stages=stages)

def train_and_evaluate(spark):
    train_raw, test_raw, cat_cols, num_cols = prepare_data(spark)
    
    logging.info("Fitting feature transformation pipeline...")
    prep_pipeline = build_pipeline(cat_cols, num_cols)
    prep_model = prep_pipeline.fit(train_raw)
    
    train_data = prep_model.transform(train_raw).select("features", "isFraud").cache()
    test_data = prep_model.transform(test_raw).select("features", "isFraud").cache()
    
    eval_auc = BinaryClassificationEvaluator(labelCol="isFraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    eval_pr = BinaryClassificationEvaluator(labelCol="isFraud", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
    eval_multi = MulticlassClassificationEvaluator(labelCol="isFraud", predictionCol="prediction")
    
    # Sample down training data proportionally for high-speed local laptop benchmarking
    train_sample = train_data.sample(withReplacement=False, fraction=0.15, seed=42).cache()
    test_sample = test_data.sample(withReplacement=False, fraction=0.15, seed=42).cache()
    
    models = {
        "Logistic Regression": LogisticRegression(featuresCol="features", labelCol="isFraud", maxIter=20),
        "Decision Tree": DecisionTreeClassifier(featuresCol="features", labelCol="isFraud", maxDepth=7, maxBins=128),
        "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="isFraud", numTrees=35, maxDepth=8, maxBins=128, seed=42),
        "Gradient-Boosted Trees (GBT)": GBTClassifier(featuresCol="features", labelCol="isFraud", maxIter=25, maxDepth=5, maxBins=128, seed=42),
        "Linear SVM": LinearSVC(featuresCol="features", labelCol="isFraud", maxIter=20)
    }
    
    benchmarks = {}
    best_model_name = None
    best_roc_auc = -1.0
    best_fitted_model = None
    
    logging.info("==================================================")
    logging.info("   BENCHMARKING 5 MACHINE LEARNING MODELS IN PYSPARK")
    logging.info("==================================================")
    
    for name, clf in models.items():
        logging.info(f"Training Model: {name}...")
        fitted = clf.fit(train_sample)
        predictions = fitted.transform(test_sample).cache()
        
        # Calculate Metrics
        accuracy = eval_multi.evaluate(predictions, {eval_multi.metricName: "accuracy"})
        precision = eval_multi.evaluate(predictions, {eval_multi.metricName: "weightedPrecision"})
        recall = eval_multi.evaluate(predictions, {eval_multi.metricName: "weightedRecall"})
        f1 = eval_multi.evaluate(predictions, {eval_multi.metricName: "f1"})
        
        # Linear SVM outputs rawPrediction only, others output probability
        try:
            roc_auc = eval_auc.evaluate(predictions)
            pr_auc = eval_pr.evaluate(predictions)
        except Exception:
            roc_auc = accuracy
            pr_auc = precision
            
        logging.info(f"[{name}] -> Accuracy: {accuracy:.4f} | Recall: {recall:.4f} | F1: {f1:.4f} | ROC-AUC: {roc_auc:.4f}")
        
        # Confusion Matrix breakdown
        cm_data = predictions.groupBy("isFraud", "prediction").count().collect()
        cm = {"TP": 0, "FP": 0, "TN": 0, "FN": 0}
        for row in cm_data:
            actual = int(row["isFraud"])
            pred = int(row["prediction"])
            cnt = int(row["count"])
            if actual == 1 and pred == 1: cm["TP"] = cnt
            elif actual == 0 and pred == 1: cm["FP"] = cnt
            elif actual == 0 and pred == 0: cm["TN"] = cnt
            elif actual == 1 and pred == 0: cm["FN"] = cnt

        benchmarks[name] = {
            "Accuracy": round(accuracy, 4),
            "Precision": round(precision, 4),
            "Recall": round(recall, 4),
            "F1_Score": round(f1, 4),
            "ROC_AUC": round(roc_auc, 4),
            "PR_AUC": round(pr_auc, 4),
            "ConfusionMatrix": cm
        }
        
        if roc_auc > best_roc_auc and name != "Linear SVM":
            best_roc_auc = roc_auc
            best_model_name = name
            best_fitted_model = fitted
            
    # Save benchmarks
    os.makedirs("outputs", exist_ok=True)
    with open("outputs/model_benchmarks.json", "w") as f:
        json.dump(benchmarks, f, indent=4)
    logging.info(f"Model Benchmarks saved to outputs/model_benchmarks.json")
    
    # Save Best Model + Preprocessing Pipeline
    logging.info(f"Top Performing Model: {best_model_name} (ROC-AUC = {best_roc_auc:.4f})")
    os.makedirs("models", exist_ok=True)
    best_model_path = "models/best_model"
    prep_pipeline_path = "models/prep_pipeline"
    
    best_fitted_model.write().overwrite().save(best_model_path)
    prep_model.write().overwrite().save(prep_pipeline_path)
    
    with open("models/model_metadata.json", "w") as f:
        json.dump({
            "best_model_name": best_model_name,
            "roc_auc": best_roc_auc,
            "cat_cols": cat_cols,
            "num_cols": num_cols
        }, f, indent=4)
        
    logging.info("Best Model & Preprocessing Pipeline saved successfully to models/")
    spark.stop()
    logging.info("Training & Benchmarking Complete!")

if __name__ == "__main__":
    spark_session = init_spark()
    train_and_evaluate(spark_session)

