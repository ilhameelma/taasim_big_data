"""
feature_importance.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Week 6 — Feature Importance

Tâche couverte : B9
- B9 : Générer feature importance chart (top 3 predictors)

Auteur : Personne B
Dépendance : train_model.py (B5) — modèle sauvegardé dans MinIO
"""

# ============================================================
# IMPORTS
# ============================================================
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"

MODEL_PATH       = "s3a://machine-learning/models/demand_v1/"
OUTPUT_JSON      = "/home/jovyan/work/ml/feature_importance.json"

# Noms des features dans l'ordre exact de VectorAssembler
# (NUMERIC_FEATURES + ["temp_bucket_idx"])
FEATURE_NAMES = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_friday",
    "zone_population_density",
    "zone_type_urban",
    "zone_type_residential",
    "zone_type_commercial",
    "zone_type_transit_hub",
    "is_raining",
    "demand_lag_1d",
    "demand_lag_7d",
    "rolling_7d_mean",
    "temp_bucket_idx",
]


# ============================================================
# INITIALISATION SPARK
# ============================================================
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaaSim-FeatureImportance")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# B9 — FEATURE IMPORTANCE
# ============================================================
def extract_feature_importance(spark: SparkSession):
    """
    B9 : Extrait les feature importances du GBTRegressor
    depuis le modèle sauvegardé dans MinIO.

    GBTRegressor calcule l'importance de chaque feature
    comme la réduction totale de l'impureté (Gini/variance)
    apportée par cette feature sur tous les arbres.
    """
    print("=" * 60)
    print("  TaaSim — Feature Importance (B9)")
    print("=" * 60)

    # ── Charge le modèle ─────────────────────────────────────
    print(f"\n[B9] Chargement du modèle depuis {MODEL_PATH}...")
    pipeline_model = PipelineModel.load(MODEL_PATH)

    # Le pipeline a 4 stages : StringIndexer, VectorAssembler, StandardScaler, GBT
    # GBT est le dernier stage (index -1)
    gbt_model = pipeline_model.stages[-1]

    # ── Extraction des importances ────────────────────────────
    importances = gbt_model.featureImportances.toArray()

    # ── Association nom → importance ─────────────────────────
    feature_scores = list(zip(FEATURE_NAMES, importances))

    # Tri décroissant par importance
    feature_scores.sort(key=lambda x: x[1], reverse=True)

    # ── Affichage complet ─────────────────────────────────────
    print("\n  📊 Feature Importances (toutes les features) :")
    print(f"  {'Rang':<5} {'Feature':<30} {'Importance':>10}  {'Barre'}")
    print("  " + "-" * 65)

    for i, (name, score) in enumerate(feature_scores):
        bar = "█" * int(score * 100)
        marker = " ← 🏆 TOP" if i < 3 else ""
        print(f"  {i+1:<5} {name:<30} {score:>10.4f}  {bar}{marker}")

    # ── TOP 3 ─────────────────────────────────────────────────
    top3 = feature_scores[:3]
    print(f"\n  🏆 TOP 3 PREDICTEURS :")
    for i, (name, score) in enumerate(top3):
        pct = score * 100
        print(f"     {i+1}. {name:<30} → {pct:.1f}% d'importance")

    # ── Sauvegarde JSON ───────────────────────────────────────
    result = {
        "all_features": [
            {"rank": i+1, "feature": name, "importance": round(float(score), 6)}
            for i, (name, score) in enumerate(feature_scores)
        ],
        "top3": [
            {"rank": i+1, "feature": name, "importance": round(float(score), 6)}
            for i, (name, score) in enumerate(top3)
        ]
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n  💾 Résultats sauvegardés : {OUTPUT_JSON}")
    print("\n" + "=" * 60)
    print("  ✅ B9 terminé !")
    print("=" * 60)

    return result


# ============================================================
# MAIN
# ============================================================
def main():
    spark = create_spark_session()
    result = extract_feature_importance(spark)
    spark.stop()
    return result


if __name__ == "__main__":
    main()