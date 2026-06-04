"""
save_model.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Week 6 — Sauvegarde + Tableau comparatif

Tâches couvertes : B10, B11, B12
- B10 : Sauvegarder modèle vers s3a://ml/models/demand_v1/
- B11 : Sauvegarder métriques (RMSE, MAE, comparaison) vers MinIO
- B12 : Créer tableau comparaison par zone (16 arrondissements)

Auteur : Personne B
Dépendance : evaluate_model.py (B6→B8) + feature_importance.py (B9)
"""

# ============================================================
# IMPORTS
# ============================================================
import json
import boto3
from io import BytesIO, StringIO
from botocore.client import Config

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml import PipelineModel

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"

FEATURES_PATH    = "s3a://machine-learning/features/feature_matrix/"
MODEL_PATH       = "s3a://machine-learning/models/demand_v1/"

# Chemins MinIO pour les sorties
BUCKET_ML        = "machine-learning"
METRICS_KEY      = "metrics/evaluation_metrics.json"
IMPORTANCE_KEY   = "metrics/feature_importance.json"
COMPARISON_KEY   = "metrics/zone_comparison.json"
COMPARISON_CSV   = "metrics/zone_comparison.csv"

# Fichiers locaux produits par les étapes précédentes
LOCAL_METRICS    = "/home/jovyan/work/ml/metrics.json"
LOCAL_IMPORTANCE = "/home/jovyan/work/ml/feature_importance.json"

LABEL_COL        = "demand"
SPLIT_DATE       = "2014-05-01"


# ============================================================
# INITIALISATION SPARK
# ============================================================
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaaSim-SaveModel")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# CLIENT MINIO (boto3) — pour upload JSON/CSV
# ============================================================
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_json(s3, data: dict, key: str):
    """Upload un dict Python comme JSON vers MinIO."""
    body = json.dumps(data, indent=2).encode("utf-8")
    s3.put_object(Bucket=BUCKET_ML, Key=key, Body=body,
                  ContentType="application/json")
    print(f"      → ✅ Uploadé : s3://{BUCKET_ML}/{key}")


def upload_text(s3, text: str, key: str):
    """Upload un texte (CSV) vers MinIO."""
    s3.put_object(Bucket=BUCKET_ML, Key=key,
                  Body=text.encode("utf-8"), ContentType="text/csv")
    print(f"      → ✅ Uploadé : s3://{BUCKET_ML}/{key}")


# ============================================================
# B10 — VÉRIFICATION MODÈLE DANS MINIO
# ============================================================
def verify_model(spark: SparkSession):
    """
    B10 : Vérifie que le modèle est bien sauvegardé dans MinIO
    (il a été sauvegardé à la fin de train_model.py).
    Si absent, le resauvegarde.
    """
    print("\n" + "=" * 55)
    print("[B10] Vérification modèle dans MinIO...")
    print("=" * 55)

    try:
        model = PipelineModel.load(MODEL_PATH)
        print(f"      → ✅ Modèle présent : {MODEL_PATH}")
        print(f"      → Stages : {[type(s).__name__ for s in model.stages]}")
        return model
    except Exception as e:
        print(f"      ⚠️  Modèle absent, resauvegarde nécessaire : {e}")
        print("      → Lance d'abord train_model.py !")
        raise


# ============================================================
# B11 — SAUVEGARDE MÉTRIQUES VERS MINIO
# ============================================================
def save_metrics_to_minio(s3):
    """
    B11 : Upload les métriques JSON (évaluation + feature importance)
    vers MinIO depuis les fichiers locaux produits par B6-B9.
    """
    print("\n" + "=" * 55)
    print("[B11] Sauvegarde métriques vers MinIO...")
    print("=" * 55)

    # ── Métriques d'évaluation (depuis evaluate_model.py) ────
    try:
        with open(LOCAL_METRICS, "r") as f:
            metrics = json.load(f)
        upload_json(s3, metrics, METRICS_KEY)
        print(f"\n  📊 Métriques uploadées :")
        print(f"     GBT  → RMSE={metrics['gbt_rmse']}, MAE={metrics['gbt_mae']}, R²={metrics['gbt_r2']}")
        print(f"     Base → RMSE={metrics['baseline_rmse']}, MAE={metrics['baseline_mae']}")
        print(f"     Amélioration RMSE : {metrics['rmse_improvement_pct']}%")
    except FileNotFoundError:
        print(f"      ⚠️  {LOCAL_METRICS} introuvable — lance evaluate_model.py d'abord")
        metrics = {}

    # ── Feature importance (depuis feature_importance.py) ────
    try:
        with open(LOCAL_IMPORTANCE, "r") as f:
            importance = json.load(f)
        upload_json(s3, importance, IMPORTANCE_KEY)
        print(f"\n  🏆 Top 3 features uploadées :")
        for item in importance.get("top3", []):
            print(f"     {item['rank']}. {item['feature']} → {item['importance']*100:.1f}%")
    except FileNotFoundError:
        print(f"      ⚠️  {LOCAL_IMPORTANCE} introuvable — lance feature_importance.py d'abord")
        importance = {}

    return metrics, importance


# ============================================================
# B12 — TABLEAU COMPARAISON PAR ZONE
# ============================================================
def build_zone_comparison(spark: SparkSession, s3, model: PipelineModel):
    """
    B12 : Calcule RMSE et MAE par zone (16 arrondissements)
    sur le test set et sauvegarde en JSON + CSV vers MinIO.
    """
    print("\n" + "=" * 55)
    print("[B12] Tableau comparaison par zone (16 arrondissements)...")
    print("=" * 55)

    # Relit le test set
    df = spark.read.parquet(FEATURES_PATH)
    df = (
        df
        .withColumn("hour_of_day",             F.col("hour_of_day").cast(IntegerType()))
        .withColumn("day_of_week",             F.col("day_of_week").cast(IntegerType()))
        .withColumn("is_weekend",              F.col("is_weekend").cast(IntegerType()))
        .withColumn("is_friday",               F.col("is_friday").cast(IntegerType()))
        .withColumn("zone_population_density", F.col("zone_population_density").cast(FloatType()))
        .withColumn("zone_type_urban",         F.col("zone_type_urban").cast(IntegerType()))
        .withColumn("zone_type_residential",   F.col("zone_type_residential").cast(IntegerType()))
        .withColumn("zone_type_commercial",    F.col("zone_type_commercial").cast(IntegerType()))
        .withColumn("zone_type_transit_hub",   F.col("zone_type_transit_hub").cast(IntegerType()))
        .withColumn("is_raining",              F.col("is_raining").cast(IntegerType()))
        .withColumn("demand_lag_1d",           F.col("demand_lag_1d").cast(FloatType()))
        .withColumn("demand_lag_7d",           F.col("demand_lag_7d").cast(FloatType()))
        .withColumn("rolling_7d_mean",         F.col("rolling_7d_mean").cast(FloatType()))
        .withColumn("demand",                  F.col("demand").cast(IntegerType()))
    )

    test_df = df.filter(
        F.col("time_slot_30min") >= F.lit(SPLIT_DATE).cast("timestamp")
    ).dropna(subset=[LABEL_COL])

    # Prédictions sur le test set complet
    predictions = model.transform(test_df)

    # Calcul RMSE + MAE + demande moyenne par zone
    zone_metrics = (
        predictions
        .withColumn("error",     F.col("prediction") - F.col(LABEL_COL))
        .withColumn("sq_error",  F.pow(F.col("error"), 2))
        .withColumn("abs_error", F.abs(F.col("error")))
        .groupBy("origin_zone_id")
        .agg(
            F.count("*").alias("n_slots"),
            F.avg(LABEL_COL).alias("avg_demand"),
            F.max(LABEL_COL).alias("max_demand"),
            F.sqrt(F.avg("sq_error")).alias("rmse"),
            F.avg("abs_error").alias("mae"),
        )
        .withColumn("rmse", F.round("rmse", 4))
        .withColumn("mae",  F.round("mae",  4))
        .withColumn("avg_demand", F.round("avg_demand", 2))
        .orderBy("origin_zone_id")
    )

    # Affichage
    print(f"\n  {'Zone':<8} {'Slots':>8} {'Demande moy':>12} {'Demande max':>12} {'RMSE':>8} {'MAE':>8}")
    print("  " + "-" * 65)

    rows = zone_metrics.collect()
    for row in rows:
        print(f"  {row['origin_zone_id']:<8} {row['n_slots']:>8} "
              f"{row['avg_demand']:>12.2f} {row['max_demand']:>12} "
              f"{row['rmse']:>8.4f} {row['mae']:>8.4f}")

    # Sauvegarde JSON
    comparison_data = {
        "description": "RMSE et MAE par zone sur le test set (Mai-Jun 2014)",
        "zones": [
            {
                "zone_id":    int(row["origin_zone_id"]),
                "n_slots":    int(row["n_slots"]),
                "avg_demand": float(row["avg_demand"]),
                "max_demand": int(row["max_demand"]),
                "rmse":       float(row["rmse"]),
                "mae":        float(row["mae"]),
            }
            for row in rows
        ]
    }
    upload_json(s3, comparison_data, COMPARISON_KEY)

    # Sauvegarde CSV
    csv_lines = ["zone_id,n_slots,avg_demand,max_demand,rmse,mae"]
    for row in rows:
        csv_lines.append(
            f"{row['origin_zone_id']},{row['n_slots']},"
            f"{row['avg_demand']},{row['max_demand']},"
            f"{row['rmse']},{row['mae']}"
        )
    upload_text(s3, "\n".join(csv_lines), COMPARISON_CSV)

    # Meilleure et pire zone
    best = min(rows, key=lambda r: r["rmse"])
    worst = max(rows, key=lambda r: r["rmse"])
    print(f"\n  🟢 Meilleure zone  : Zone {best['origin_zone_id']}  (RMSE={best['rmse']})")
    print(f"  🔴 Zone difficile  : Zone {worst['origin_zone_id']} (RMSE={worst['rmse']})")

    return comparison_data


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  TaaSim — Sauvegarde + Comparaison (B10, B11, B12)")
    print("=" * 60)

    spark = create_spark_session()
    s3    = get_minio_client()

    # B10 — Vérification modèle
    model = verify_model(spark)

    # B11 — Upload métriques
    metrics, importance = save_metrics_to_minio(s3)

    # B12 — Tableau par zone
    comparison = build_zone_comparison(spark, s3, model)

    print("\n" + "=" * 60)
    print("  ✅ B10 + B11 + B12 terminés !")
    print(f"  📁 Fichiers dans MinIO (bucket: {BUCKET_ML}) :")
    print(f"     → {METRICS_KEY}")
    print(f"     → {IMPORTANCE_KEY}")
    print(f"     → {COMPARISON_KEY}")
    print(f"     → {COMPARISON_CSV}")
    print("  ➡️  Prochaine étape : report.md (B13)")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()