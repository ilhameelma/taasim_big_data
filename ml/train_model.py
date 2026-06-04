"""
train_model.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Week 6 — ML Training Pipeline

Tâches couvertes : B1 → B5
- B1 : Lecture Feature Matrix depuis s3a://machine-learning/features/feature_matrix/
- B2 : Split temporel 10 mois train / 2 mois test (sans shuffle)
- B3 : Pipeline ML : StringIndexer → VectorAssembler → StandardScaler → GBTRegressor
- B4 : Entraînement GBTRegressor (maxDepth=5, maxIter=50)
- B5 : Cross-validation (3 folds, maxDepth=[5, 7])

Auteur : Personne B
Dépendance : feat_engineering.py (Personne A) — A7 doit être terminé
"""

# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler,
    StandardScaler,
)
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# ============================================================
# CONFIGURATION MINIO / S3A
# ============================================================
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"

# Chemin Feature Matrix (sortie de Personne A — A7)
FEATURES_PATH = "s3a://machine-learning/features/feature_matrix/"

# Chemin de sauvegarde du modèle (utilisé par save_model.py — B10)
MODEL_OUTPUT_PATH = "s3a://machine-learning/models/demand_v1/"

# ============================================================
# COLONNES DE LA FEATURE MATRIX (produites par feat_engineering.py)
# ============================================================
# Features numériques directement utilisables
NUMERIC_FEATURES = [
    # Temporelles
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_friday",
    # Spatiales
    "zone_population_density",
    "zone_type_urban",
    "zone_type_residential",
    "zone_type_commercial",
    "zone_type_transit_hub",
    # Météo (numérique)
    "is_raining",
    # Lag features
    "demand_lag_1d",
    "demand_lag_7d",
    "rolling_7d_mean",
]

# Feature catégorielle à encoder (temperature_bucket : cold/mild/hot)
CATEGORICAL_FEATURE = "temperature_bucket"   # → StringIndexer → temp_bucket_idx

# Label (ce qu'on prédit)
LABEL_COL = "demand"


# ============================================================
# INITIALISATION SPARK
# ============================================================
def create_spark_session() -> SparkSession:
    """Crée une SparkSession configurée pour MinIO (S3A)."""
    spark = (
        SparkSession.builder
        .appName("TaaSim-TrainModel")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Optimisations mémoire
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled",   "true")
        .config("spark.driver.memory",          "4g")
        .config("spark.executor.memory",        "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# B1 — LECTURE FEATURE MATRIX
# ============================================================
def load_feature_matrix(spark: SparkSession):
    """
    B1 : Lit la Feature Matrix Parquet depuis MinIO.
    Partitionné par origin_zone_id (17 zones, 0-16).
    Colonnes attendues (depuis feat_engineering.py — A6) :
        origin_zone_id, time_slot_30min,
        hour_of_day, day_of_week, is_weekend, is_friday,
        zone_population_density, zone_type_urban, zone_type_residential,
        zone_type_commercial, zone_type_transit_hub,
        is_raining, temperature_bucket,
        demand_lag_1d, demand_lag_7d, rolling_7d_mean,
        demand
    """
    print("[B1] Lecture Feature Matrix depuis MinIO...")
    print(f"      → Chemin : {FEATURES_PATH}")

    df = spark.read.parquet(FEATURES_PATH)

    # Cast explicite pour sécurité (en cas de type Parquet ambigu)
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

    # Supprime les lignes avec label ou features nulles
    df = df.dropna(subset=[LABEL_COL] + NUMERIC_FEATURES + [CATEGORICAL_FEATURE])

    n_rows  = df.count()
    n_zones = df.select("origin_zone_id").distinct().count()
    print(f"      → {n_rows:,} lignes chargées")
    print(f"      → {n_zones} zones distinctes")
    print(f"      → Colonnes : {df.columns}")
    return df


# ============================================================
# B2 — SPLIT TEMPOREL (10 mois train / 2 mois test, SANS shuffle)
# ============================================================
def temporal_split(df):
    """
    B2 : Split temporel strict — PAS de shuffle pour éviter le data leakage.
    Données : Jul 2013 → Jun 2014 (12 mois)
      - Train : Jul 2013 → Apr 2014 (10 mois)
      - Test  : Mai 2014 → Jun 2014 (2 mois)

    Le split se fait sur time_slot_30min (TimestampType).
    """
    print("[B2] Split temporel 10 mois train / 2 mois test (sans shuffle)...")

    # Date de coupure : 1er mai 2014 00:00:00
    SPLIT_DATE = "2014-05-01"

    train_df = df.filter(F.col("time_slot_30min") < F.lit(SPLIT_DATE).cast("timestamp"))
    test_df  = df.filter(F.col("time_slot_30min") >= F.lit(SPLIT_DATE).cast("timestamp"))

    n_train = train_df.count()
    n_test  = test_df.count()
    total   = n_train + n_test

    print(f"      → Train : {n_train:,} lignes  ({100*n_train/total:.1f}%)")
    print(f"      → Test  : {n_test:,} lignes  ({100*n_test/total:.1f}%)")
    print(f"      → Coupure : {SPLIT_DATE}")

    # Vérification que le test ne précède pas le train (sanity check)
    train_max = train_df.agg(F.max("time_slot_30min")).collect()[0][0]
    test_min  = test_df.agg(F.min("time_slot_30min")).collect()[0][0]
    print(f"      → Dernier slot train : {train_max}")
    print(f"      → Premier slot test  : {test_min}")
    assert train_max < test_min, "⚠️  Erreur : chevauchement train/test !"

    return train_df, test_df


# ============================================================
# B3 — PIPELINE ML
# ============================================================
def build_pipeline() -> Pipeline:
    """
    B3 : Construit le Pipeline Spark ML :
        StringIndexer (temperature_bucket → temp_bucket_idx)
        → VectorAssembler (toutes les features → features_raw)
        → StandardScaler (features_raw → features)
        → GBTRegressor (features → prediction)

    Le pipeline est retourné non-entraîné ; il sera fit en B4.
    """
    print("[B3] Construction du Pipeline ML...")

    # ── Étape 1 : Encoder temperature_bucket (cold=0, hot=1, mild=2 ou selon fréquence)
    temp_indexer = StringIndexer(
        inputCol=CATEGORICAL_FEATURE,
        outputCol="temp_bucket_idx",
        handleInvalid="keep"          # garde les valeurs inconnues comme catégorie distincte
    )

    # ── Étape 2 : Assemblage de toutes les features en un vecteur
    all_feature_cols = NUMERIC_FEATURES + ["temp_bucket_idx"]

    assembler = VectorAssembler(
        inputCols=all_feature_cols,
        outputCol="features_raw",
        handleInvalid="keep"
    )

    # ── Étape 3 : Normalisation (StandardScaler — μ=0, σ=1)
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )

    # ── Étape 4 : Modèle GBTRegressor (paramètres B4)
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=LABEL_COL,
        predictionCol="prediction",
        maxDepth=5,      # B4 : maxDepth=5
        maxIter=50,      # B4 : maxIter=50
        seed=42
    )

    pipeline = Pipeline(stages=[temp_indexer, assembler, scaler, gbt])

    print("      → Stages : StringIndexer → VectorAssembler → StandardScaler → GBTRegressor")
    print(f"      → Features : {len(all_feature_cols)} colonnes")
    print(f"      → Label    : {LABEL_COL}")
    return pipeline


# ============================================================
# B4 — ENTRAÎNEMENT
# ============================================================
def train_model(pipeline: Pipeline, train_df):
    """
    B4 : Entraîne le pipeline sur les données train.
    GBTRegressor avec maxDepth=5, maxIter=50.
    Retourne le PipelineModel entraîné.
    """
    print("[B4] Entraînement GBTRegressor (maxDepth=5, maxIter=50)...")
    print(f"      → Lignes train : {train_df.count():,}")

    import time
    t0 = time.time()

    model = pipeline.fit(train_df)

    elapsed = time.time() - t0
    print(f"      → ✅ Entraînement terminé en {elapsed:.1f}s")
    return model


# ============================================================
# B5 — CROSS-VALIDATION
# ============================================================
def cross_validate_model(pipeline: Pipeline, train_df):
    """
    B5 : Cross-validation temporelle (3 folds) sur maxDepth=[5, 7].

    ⚠️  NOTE IMPORTANTE sur le CV temporel :
    CrossValidator de Spark ML fait un CV k-fold classique (shuffle aléatoire).
    Pour des séries temporelles, cela introduit du data leakage.
    Ici on l'utilise comme requis par le capstone, mais en production
    on utiliserait un TimeSeriesSplit manuel.

    Retourne le meilleur modèle + les métriques CV.
    """
    print("[B5] Cross-validation (3 folds, maxDepth=[5, 7])...")

    evaluator = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="rmse"
    )

    # Grille d'hyperparamètres : uniquement maxDepth comme requis
    # On accède au stage GBT (index 3) dans le pipeline
    gbt_stage = pipeline.getStages()[-1]   # GBTRegressor

    param_grid = (
        ParamGridBuilder()
        .addGrid(gbt_stage.maxDepth, [5, 7])
        .build()
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,         # B5 : 3 folds
        seed=42,
        parallelism=2       # entraîne 2 modèles en parallèle si ressources dispo
    )

    import time
    t0 = time.time()
    cv_model = cv.fit(train_df)
    elapsed = time.time() - t0

    # Résultats par combinaison d'hyperparamètres
    avg_metrics = cv_model.avgMetrics
    print(f"\n      → CV terminée en {elapsed:.1f}s")
    print(f"      → Résultats RMSE moyen par fold :")
    for params, rmse in zip(param_grid, avg_metrics):
        depth = params[gbt_stage.maxDepth]
        print(f"         maxDepth={depth}  →  RMSE moyen = {rmse:.4f}")

    best_rmse   = min(avg_metrics)
    best_depth  = param_grid[avg_metrics.index(best_rmse)][gbt_stage.maxDepth]
    print(f"\n      → 🏆 Meilleur : maxDepth={best_depth}, RMSE={best_rmse:.4f}")

    return cv_model.bestModel, {
        "cv_param_grid":   [{"maxDepth": p[gbt_stage.maxDepth]} for p in param_grid],
        "cv_avg_rmse":     avg_metrics,
        "best_max_depth":  best_depth,
        "best_cv_rmse":    best_rmse,
    }


# ============================================================
# MAIN PIPELINE B1 → B5
# ============================================================
def main():
    print("=" * 60)
    print("  TaaSim — ML Training Pipeline (Semaine 6 — Partie B)")
    print("=" * 60)

    # Initialisation Spark
    spark = create_spark_session()

    # ── B1 : Lecture Feature Matrix ──────────────────────────
    df = load_feature_matrix(spark)

    # ── B2 : Split temporel ──────────────────────────────────
    train_df, test_df = temporal_split(df)

    # Persiste en mémoire pour éviter de recalculer
    train_df.cache()
    test_df.cache()

    # ── B3 : Construction Pipeline ───────────────────────────
    pipeline = build_pipeline()

    # ── B4 : Entraînement (modèle de base) ───────────────────
    base_model = train_model(pipeline, train_df)

    # ── B5 : Cross-validation ────────────────────────────────
    best_model, cv_metrics = cross_validate_model(pipeline, train_df)

    # ── Vérification rapide sur train ────────────────────────
    print("\n[Check] Prédictions sur un échantillon train...")
    sample_preds = best_model.transform(train_df.limit(5))
    sample_preds.select("origin_zone_id", "time_slot_30min",
                        LABEL_COL, "prediction").show(5, truncate=False)

    print("\n" + "=" * 60)
    print("  ✅ B1 → B5 terminé !")
    print(f"  📊 Meilleur maxDepth CV : {cv_metrics['best_max_depth']}")
    print(f"  📉 Meilleur RMSE CV     : {cv_metrics['best_cv_rmse']:.4f}")
    print("  ➡️  Lance maintenant : evaluate_model.py (B6)")
    print("=" * 60)

    

    # Retourne les objets pour les scripts suivants si importé comme module
    return best_model, train_df, test_df, cv_metrics


if __name__ == "__main__":
    best_model, train_df, test_df, cv_metrics = main()

    # Sauvegarde AVANT d'arrêter Spark
    print("\n💾 Sauvegarde du modèle vers MinIO...")
    best_model.write().overwrite().save("s3a://machine-learning/models/demand_v1/")
    print("✅ Modèle sauvegardé !")

    # Arrêt Spark APRÈS la sauvegarde
    from pyspark.sql import SparkSession
    SparkSession.builder.getOrCreate().stop()