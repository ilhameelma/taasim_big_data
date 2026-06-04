"""
evaluate_model.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Week 6 — Évaluation + Baseline + Comparaison

Tâches couvertes : B6, B7, B8
- B6 : Évaluer modèle GBT sur test set → calculer RMSE, MAE
- B7 : Implémenter baseline naïve (demand_lag_7d)
- B8 : Comparer modèle vs baseline → vérifier RMSE_modèle < RMSE_baseline

Auteur : Personne B
Dépendance : train_model.py (B1→B5) doit être terminé
"""

# ============================================================
# IMPORTS
# ============================================================
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

# ============================================================
# CONFIGURATION MINIO / S3A
# ============================================================
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"

FEATURES_PATH    = "s3a://machine-learning/features/feature_matrix/"
MODEL_PATH       = "s3a://machine-learning/models/demand_v1/"
METRICS_PATH     = "s3a://machine-learning/metrics/evaluation_metrics.json"

LABEL_COL        = "demand"
SPLIT_DATE       = "2014-05-01"


# ============================================================
# INITIALISATION SPARK
# ============================================================
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TaaSim-EvaluateModel")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled",   "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# CHARGEMENT DONNÉES + SPLIT TEST
# ============================================================
def load_test_set(spark: SparkSession):
    """Relit la Feature Matrix et extrait uniquement le test set (2 derniers mois)."""
    print("[Load] Lecture Feature Matrix + extraction test set...")

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
    ).dropna(subset=[LABEL_COL, "demand_lag_7d"])

    n_test = test_df.count()
    print(f"      → Test set : {n_test:,} lignes (Mai–Jun 2014)")
    return test_df


# ============================================================
# B6 — ÉVALUATION DU MODÈLE GBT SUR LE TEST SET
# ============================================================
def evaluate_gbt_model(spark: SparkSession, test_df):
    """
    B6 : Charge le modèle entraîné depuis MinIO et calcule RMSE + MAE sur le test set.
    """
    print("\n" + "=" * 50)
    print("[B6] Évaluation GBTRegressor sur test set...")
    print("=" * 50)

    # Charge le modèle sauvegardé par save_model.py (B10)
    # Si pas encore sauvegardé, le modèle doit être passé en paramètre
    try:
        model = PipelineModel.load(MODEL_PATH)
        print(f"      → Modèle chargé depuis {MODEL_PATH}")
    except Exception as e:
        print(f"      ⚠️  Modèle non trouvé dans MinIO : {e}")
        print("      → Réentraînement rapide pour évaluation...")
        from train_model import (
            create_spark_session as _cs,
            load_feature_matrix, temporal_split,
            build_pipeline, cross_validate_model
        )
        df_full = load_feature_matrix(spark)
        train_df, _ = temporal_split(df_full)
        pipeline = build_pipeline()
        model, _ = cross_validate_model(pipeline, train_df)

    # Prédictions sur le test set
    predictions = model.transform(test_df)

    # Évaluateur RMSE
    evaluator_rmse = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="rmse"
    )

    # Évaluateur MAE
    evaluator_mae = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="mae"
    )

    # Évaluateur R²
    evaluator_r2 = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="r2"
    )

    rmse_gbt = evaluator_rmse.evaluate(predictions)
    mae_gbt  = evaluator_mae.evaluate(predictions)
    r2_gbt   = evaluator_r2.evaluate(predictions)

    print(f"\n  📊 Métriques GBTRegressor (test set) :")
    print(f"     RMSE = {rmse_gbt:.4f}  (erreur quadratique moyenne)")
    print(f"     MAE  = {mae_gbt:.4f}  (erreur absolue moyenne)")
    print(f"     R²   = {r2_gbt:.4f}  (variance expliquée)")

    # Aperçu des prédictions
    print("\n  Aperçu predictions vs réalité (5 exemples) :")
    predictions.select(
        "origin_zone_id", "time_slot_30min",
        F.col(LABEL_COL).alias("demande_réelle"),
        F.round("prediction", 2).alias("demande_prédite"),
        F.round(F.abs(F.col("prediction") - F.col(LABEL_COL)), 2).alias("erreur_abs")
    ).orderBy("time_slot_30min").show(5, truncate=False)

    return predictions, {
        "model_rmse": round(rmse_gbt, 4),
        "model_mae":  round(mae_gbt,  4),
        "model_r2":   round(r2_gbt,   4),
    }


# ============================================================
# B7 — BASELINE NAÏVE (demand_lag_7d)
# ============================================================
def evaluate_baseline(test_df):
    """
    B7 : Baseline naïve — prédit que la demande de ce slot =
         la demande du même slot il y a 7 jours (demand_lag_7d).

    C'est la stratégie la plus simple possible :
    "La semaine prochaine ressemblera à cette semaine."
    """
    print("\n" + "=" * 50)
    print("[B7] Évaluation Baseline naïve (demand_lag_7d)...")
    print("=" * 50)

    # La "prédiction" baseline = demand_lag_7d
    baseline_df = test_df.withColumn(
        "baseline_prediction",
        F.col("demand_lag_7d").cast(FloatType())
    ).filter(F.col("baseline_prediction").isNotNull())

    # Filtre les slots où lag_7d = 0 (début de série, pas fiable)
    baseline_df = baseline_df.filter(F.col("baseline_prediction") > 0)

    n_baseline = baseline_df.count()
    print(f"      → {n_baseline:,} lignes avec lag_7d valide")

    # RMSE baseline
    evaluator_rmse = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="baseline_prediction",
        metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="baseline_prediction",
        metricName="mae"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="baseline_prediction",
        metricName="r2"
    )

    rmse_baseline = evaluator_rmse.evaluate(baseline_df)
    mae_baseline  = evaluator_mae.evaluate(baseline_df)
    r2_baseline   = evaluator_r2.evaluate(baseline_df)

    print(f"\n  📊 Métriques Baseline naïve (demand_lag_7d) :")
    print(f"     RMSE = {rmse_baseline:.4f}")
    print(f"     MAE  = {mae_baseline:.4f}")
    print(f"     R²   = {r2_baseline:.4f}")

    return baseline_df, {
        "baseline_rmse": round(rmse_baseline, 4),
        "baseline_mae":  round(mae_baseline,  4),
        "baseline_r2":   round(r2_baseline,   4),
    }


# ============================================================
# B8 — COMPARAISON MODÈLE vs BASELINE
# ============================================================
def compare_model_vs_baseline(gbt_metrics: dict, baseline_metrics: dict):
    """
    B8 : Compare les métriques GBT vs baseline et vérifie que
         RMSE_modèle < RMSE_baseline (critère de réussite du capstone).
    """
    print("\n" + "=" * 50)
    print("[B8] Comparaison Modèle vs Baseline")
    print("=" * 50)

    rmse_gbt      = gbt_metrics["model_rmse"]
    rmse_baseline = baseline_metrics["baseline_rmse"]
    mae_gbt       = gbt_metrics["model_mae"]
    mae_baseline  = baseline_metrics["baseline_mae"]
    r2_gbt        = gbt_metrics["model_r2"]
    r2_baseline   = baseline_metrics["baseline_r2"]

    # Amélioration en %
    rmse_improvement = (rmse_baseline - rmse_gbt) / rmse_baseline * 100
    mae_improvement  = (mae_baseline  - mae_gbt)  / mae_baseline  * 100

    print(f"""
  ┌─────────────────────────────────────────────────────┐
  │           TABLEAU COMPARATIF FINAL                  │
  ├─────────────────┬──────────────┬────────────────────┤
  │ Métrique        │  GBT Modèle  │  Baseline (lag_7d) │
  ├─────────────────┼──────────────┼────────────────────┤
  │ RMSE            │  {rmse_gbt:>10.4f}  │  {rmse_baseline:>16.4f}  │
  │ MAE             │  {mae_gbt:>10.4f}  │  {mae_baseline:>16.4f}  │
  │ R²              │  {r2_gbt:>10.4f}  │  {r2_baseline:>16.4f}  │
  └─────────────────┴──────────────┴────────────────────┘
    """)

    print(f"  📉 Amélioration RMSE : {rmse_improvement:+.1f}%")
    print(f"  📉 Amélioration MAE  : {mae_improvement:+.1f}%")

    # Vérification du critère capstone
    if rmse_gbt < rmse_baseline:
        print(f"\n  ✅ CRITÈRE VALIDÉ : RMSE_modèle ({rmse_gbt:.4f}) "
              f"< RMSE_baseline ({rmse_baseline:.4f})")
        validation_ok = True
    else:
        print(f"\n  ❌ CRITÈRE NON VALIDÉ : RMSE_modèle ({rmse_gbt:.4f}) "
              f">= RMSE_baseline ({rmse_baseline:.4f})")
        print("     → Essaie d'augmenter maxIter ou d'ajuster les features.")
        validation_ok = False

    comparison = {
        "gbt_rmse":          rmse_gbt,
        "gbt_mae":           mae_gbt,
        "gbt_r2":            r2_gbt,
        "baseline_rmse":     rmse_baseline,
        "baseline_mae":      mae_baseline,
        "baseline_r2":       r2_baseline,
        "rmse_improvement_pct": round(rmse_improvement, 2),
        "mae_improvement_pct":  round(mae_improvement, 2),
        "validation_ok":        validation_ok,
    }

    return comparison


# ============================================================
# SAUVEGARDE MÉTRIQUES (JSON local — B11 les enverra vers MinIO)
# ============================================================
def save_metrics_local(comparison: dict, path: str = "/home/jovyan/work/ml/metrics.json"):
    """Sauvegarde les métriques en JSON local pour B11 (save_model.py)."""
    with open(path, "w") as f:
        json.dump(comparison, f, indent=2)
    print(f"\n  💾 Métriques sauvegardées localement : {path}")
    print(f"      → save_model.py (B11) les enverra vers MinIO")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  TaaSim — Évaluation + Baseline (B6, B7, B8)")
    print("=" * 60)

    spark = create_spark_session()

    # Chargement test set
    test_df = load_test_set(spark)
    test_df.cache()

    # B6 — Évaluation GBT
    predictions, gbt_metrics = evaluate_gbt_model(spark, test_df)

    # B7 — Baseline naïve
    baseline_df, baseline_metrics = evaluate_baseline(test_df)

    # B8 — Comparaison
    comparison = compare_model_vs_baseline(gbt_metrics, baseline_metrics)

    # Sauvegarde locale (pour B11)
    save_metrics_local(comparison)

    print("\n" + "=" * 60)
    print("  ✅ B6 + B7 + B8 terminés !")
    print("  ➡️  Lance maintenant :")
    print("       - feature_importance.py  (B9)")
    print("       - save_model.py          (B10, B11)")
    print("=" * 60)

    spark.stop()
    return comparison


if __name__ == "__main__":
    main()