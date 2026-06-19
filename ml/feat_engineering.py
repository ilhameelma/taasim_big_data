"""
feat_engineering.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Week 6 — Feature Engineering Pipeline

Tâches couvertes : A1 → A7
- A1 : Lecture curated/trips/ (Parquet partitionné par year_month)
- A2 : Features temporelles (hour_of_day, day_of_week, is_weekend, is_friday)
- A3 : Features spatiales (zone_population_density, zone_type) via join zone_mapping
- A4 : Features météo Open-Meteo (is_raining, temperature_bucket)
- A5 : Lag features (demand_lag_1d, demand_lag_7d, rolling_7d_mean)
- A6 : Agrégation 30-min → Feature Matrix
- A7 : Écriture s3a://machine-learning/features/ (Parquet)

Auteur : Personne A
"""

# ============================================================
# IMPORTS
# ============================================================
import requests
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, TimestampType, BooleanType
)

# ============================================================
# CONFIGURATION MINIO / S3A
# ============================================================
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"

# Chemins MinIO
CURATED_TRIPS_PATH   = "s3a://curated/trips/"
ZONE_MAPPING_PATH    = "s3a://raw/porto-trips/zone_mapping_geojson.csv"
FEATURES_OUTPUT_PATH = "s3a://machine-learning/features/feature_matrix/"

# Open-Meteo : coordonnées de Porto (source des données historiques)
PORTO_LAT = 41.15
PORTO_LON = -8.61

# Période des données Porto (12 mois)
WEATHER_START = "2013-07-01"
WEATHER_END   = "2014-06-30"

# ============================================================
# INITIALISATION SPARK
# ============================================================
def create_spark_session() -> SparkSession:
    """Crée une SparkSession configurée pour MinIO (S3A)."""
    spark = (
        SparkSession.builder
        .appName("TaaSim-FeatureEngineering")
        .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Optimisations
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled",   "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# A1 — LECTURE curated/trips/
# ============================================================
def load_curated_trips(spark: SparkSession):
    """
    A1 : Lit les données Porto nettoyées depuis MinIO.
    Partitionné par year_month (ex: year_month=2013-07).
    Colonnes attendues après ETL Semaine 5 :
        TRIP_ID, TAXI_ID, TIMESTAMP (epoch unix int),
        zone_id (int 0-16), CALL_TYPE, year_month
    """
    print("[A1] Lecture curated/trips/ depuis MinIO...")
    df = spark.read.parquet(CURATED_TRIPS_PATH)
    print(f"      → {df.count():,} lignes chargées")
    print(f"      → Colonnes : {df.columns}")
    return df


# ============================================================
# A2 — FEATURES TEMPORELLES
# ============================================================
def add_temporal_features(df):
    """
    A2 : Extrait les features temporelles depuis TIMESTAMP (epoch Unix).
        - hour_of_day   : 0-23
        - day_of_week   : 1=Dimanche … 7=Samedi (Spark convention)
        - is_weekend    : 1 si Samedi(7) ou Dimanche(1)
        - is_friday     : 1 si Vendredi(6)  ← important pour le Maroc

    On crée aussi time_slot_30min = clé d'agrégation (tronquée à 30 min)
    utilisée en A5 et A6.
    """
    print("[A2] Extraction features temporelles...")

    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.from_unixtime(F.col("timestamp")))
    )

    df = (
        df
        .withColumn("hour_of_day",     F.hour("event_time"))
        .withColumn("day_of_week",     F.dayofweek("event_time"))   # 1=Sun … 7=Sat
        .withColumn("is_weekend",
                    F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0))
        .withColumn("is_friday",
                    F.when(F.col("day_of_week") == 6, 1).otherwise(0))
        # Slot 30 min : tronque à la demi-heure la plus proche
        .withColumn("time_slot_30min",
                    F.date_trunc("hour", "event_time") + F.expr(
                        "INTERVAL 30 MINUTES * (minute(event_time) DIV 30)"
                    ))
    )

    print("      → Colonnes ajoutées : hour_of_day, day_of_week, is_weekend, "
          "is_friday, time_slot_30min")
    return df


# ============================================================
# A3 — FEATURES SPATIALES (join zone_mapping)
# ============================================================
def add_spatial_features(spark: SparkSession, df):
    """
    A3 : Joint la table de référence zone_mapping_geojson.csv pour ajouter :
        - zone_population_density (float)
        - zone_type               (string : urban / residential / commercial / transit_hub)

    Le fichier contient les colonnes :
        zone_id, zone_name, zone_type, population_density,
        centroid_lon, centroid_lat, bbox_*, adjacent_zones,
        adjacent_distances_km, base_fare_mad
    """
    print("[A3] Chargement zone_mapping et join spatial...")

    zone_ref = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ZONE_MAPPING_PATH)
        .select(
            F.col("zone_id").cast(IntegerType()),
            F.col("zone_type"),
            F.col("population_density").cast(FloatType()).alias("zone_population_density")
        )
    )

    # zone_id dans curated/trips doit être int
    df = df.withColumn("origin_zone_id", F.col("origin_zone_id").cast(IntegerType()))

    df = df.join(F.broadcast(zone_ref), df["origin_zone_id"] == zone_ref["zone_id"], how="left")

    # One-hot encode zone_type pour VectorAssembler du binôme
    zone_types = ["urban", "residential", "commercial", "transit_hub"]
    for zt in zone_types:
        df = df.withColumn(
            f"zone_type_{zt}",
            F.when(F.col("zone_type") == zt, 1).otherwise(0)
        )

    print("      → Colonnes ajoutées : zone_population_density, zone_type, "
          "zone_type_urban/residential/commercial/transit_hub")
    return df


# ============================================================
# A4 — FEATURES MÉTÉO (Open-Meteo API)
# ============================================================
def fetch_weather_data() -> dict:
    """
    Récupère les données météo horaires depuis l'API Open-Meteo
    pour Porto (Jul 2013 – Jun 2014).
    Retourne un dict : { "YYYY-MM-DD HH:00" : (is_raining, temperature_bucket) }
    """
    print("[A4] Appel Open-Meteo API (Porto, 2013-07 → 2014-06)...")

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={PORTO_LAT}&longitude={PORTO_LON}"
        f"&start_date={WEATHER_START}&end_date={WEATHER_END}"
        "&hourly=temperature_2m,precipitation"
        "&timezone=Europe%2FLisbon"
    )

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    data = response.json()

    hourly     = data["hourly"]
    times      = hourly["time"]           # ["2013-07-01T00:00", ...]
    temps      = hourly["temperature_2m"] # [°C]
    precips    = hourly["precipitation"]  # [mm]

    weather_map = {}
    for t, temp, precip in zip(times, temps, precips):
        # Clé : "YYYY-MM-DD HH" pour le join Spark
        hour_key = t[:13]  # "2013-07-01T00" → garde jusqu'à l'heure

        is_raining = 1 if (precip is not None and precip > 0.1) else 0

        if temp is None:
            temp_bucket = "mild"
        elif temp < 15:
            temp_bucket = "cold"
        elif temp > 28:
            temp_bucket = "hot"
        else:
            temp_bucket = "mild"

        weather_map[hour_key] = (is_raining, temp_bucket)

    print(f"      → {len(weather_map):,} entrées météo récupérées")
    return weather_map


def add_weather_features(spark: SparkSession, df, weather_map: dict):
    """
    A4 (suite) : Joint les données météo au DataFrame Spark.
        - is_raining         : 0/1
        - temperature_bucket : cold / mild / hot
    """
    # Crée un DataFrame Spark depuis le dict météo
    weather_rows = [
        (k, v[0], v[1])
        for k, v in weather_map.items()
    ]
    weather_schema = StructType([
        StructField("weather_hour_key",  StringType(),  False),
        StructField("is_raining",        IntegerType(), True),
        StructField("temperature_bucket",StringType(),  True),
    ])
    weather_df = spark.createDataFrame(weather_rows, schema=weather_schema)

    # Clé de join côté trips : "YYYY-MM-DDTHH" depuis event_time
    df = df.withColumn(
        "weather_hour_key",
        F.date_format("event_time", "yyyy-MM-dd'T'HH")
    )

    df = df.join(F.broadcast(weather_df), on="weather_hour_key", how="left")

    # Valeur par défaut si Open-Meteo ne couvre pas la date
    df = (
        df
        .withColumn("is_raining",
                    F.coalesce(F.col("is_raining").cast(IntegerType()), F.lit(0)))
        .withColumn("temperature_bucket",
                    F.coalesce(F.col("temperature_bucket"), F.lit("mild")))
    )

    print("      → Colonnes ajoutées : is_raining, temperature_bucket")
    return df


# ============================================================
# A5 — LAG FEATURES (Window functions)
# ============================================================
def compute_slot_demand(df):
    """
    Étape intermédiaire avant les lags :
    Agrège d'abord le nombre de trips par (zone_id, time_slot_30min)
    pour avoir la série temporelle de demande brute.
    """
    print("[A5-prep] Calcul demande brute par (zone_id, time_slot_30min)...")

    demand_df = (
        df
        .groupBy("origin_zone_id", "time_slot_30min",
                 "hour_of_day", "day_of_week", "is_weekend", "is_friday",
                 "zone_population_density", "zone_type",
                 "zone_type_urban", "zone_type_residential",
                 "zone_type_commercial", "zone_type_transit_hub",
                 "is_raining", "temperature_bucket")
        .agg(F.count("*").alias("demand"))
    )
    return demand_df


def add_lag_features(demand_df):
    """
    A5 : Calcule les lag features via Spark Window functions.
        - demand_lag_1d    : demande du même slot il y a 1 jour   (48 slots × 30 min)
        - demand_lag_7d    : demande du même slot il y a 7 jours  (336 slots)
        - rolling_7d_mean  : moyenne glissante sur les 7 derniers jours (336 slots)

    La fenêtre est partitionnée par zone_id et ordonnée par time_slot_30min.
    """
    print("[A5] Calcul des lag features (Window functions)...")

    # 1 jour  = 48 slots de 30 min
    # 7 jours = 336 slots de 30 min
    LAG_1D  = 48
    LAG_7D  = 336

    w = (
        Window
        .partitionBy("origin_zone_id")
        .orderBy(F.col("time_slot_30min").cast("long"))
    )

    demand_df = (
        demand_df
        .withColumn("demand_lag_1d",
                    F.lag("demand", LAG_1D).over(w))
        .withColumn("demand_lag_7d",
                    F.lag("demand", LAG_7D).over(w))
        .withColumn("rolling_7d_mean",
                    F.avg("demand").over(
                        w.rowsBetween(-LAG_7D, -1)
                    ))
    )

    # Remplace les NULLs des premières lignes par 0
    demand_df = (
        demand_df
        .withColumn("demand_lag_1d",
                    F.coalesce(F.col("demand_lag_1d"), F.lit(0.0)))
        .withColumn("demand_lag_7d",
                    F.coalesce(F.col("demand_lag_7d"), F.lit(0.0)))
        .withColumn("rolling_7d_mean",
                    F.coalesce(F.col("rolling_7d_mean"), F.lit(0.0)))
    )

    print("      → Colonnes ajoutées : demand_lag_1d, demand_lag_7d, rolling_7d_mean")
    return demand_df


# ============================================================
# A6 — FEATURE MATRIX FINALE
# ============================================================
def build_feature_matrix(demand_df):
    """
    A6 : Sélectionne et ordonne les colonnes finales de la Feature Matrix.
    C'est ce DataFrame que le binôme utilisera pour entraîner GBTRegressor.

    Colonnes finales (11 features + 1 label) :
        Features temporelles  : hour_of_day, day_of_week, is_weekend, is_friday
        Features spatiales    : zone_population_density, zone_type_*  (4 one-hot)
        Features météo        : is_raining, temperature_bucket
        Lag features          : demand_lag_1d, demand_lag_7d, rolling_7d_mean
        Label                 : demand
    """
    print("[A6] Construction de la Feature Matrix finale...")

    feature_matrix = demand_df.select(
        # Identifiants (non utilisés comme features machine-learning mais utiles pour debug)
        "origin_zone_id",
        "time_slot_30min",

        # === FEATURES TEMPORELLES (4) ===
        F.col("hour_of_day").cast(IntegerType()),
        F.col("day_of_week").cast(IntegerType()),
        F.col("is_weekend").cast(IntegerType()),
        F.col("is_friday").cast(IntegerType()),

        # === FEATURES SPATIALES (5 : 1 continu + 4 one-hot) ===
        F.col("zone_population_density").cast(FloatType()),
        F.col("zone_type_urban").cast(IntegerType()),
        F.col("zone_type_residential").cast(IntegerType()),
        F.col("zone_type_commercial").cast(IntegerType()),
        F.col("zone_type_transit_hub").cast(IntegerType()),

        # === FEATURES MÉTÉO (2) ===
        F.col("is_raining").cast(IntegerType()),
        F.col("temperature_bucket"),          # string → binôme utilisera StringIndexer

        # === LAG FEATURES (3) ===
        F.col("demand_lag_1d").cast(FloatType()),
        F.col("demand_lag_7d").cast(FloatType()),
        F.col("rolling_7d_mean").cast(FloatType()),

        # === LABEL (target) ===
        F.col("demand").cast(IntegerType()),
    )

    # Filtre les lignes sans demande ni lag valides (début de série)
    feature_matrix = feature_matrix.filter(F.col("demand").isNotNull())

    total = feature_matrix.count()
    print(f"      → Feature Matrix : {total:,} lignes × {len(feature_matrix.columns)} colonnes")
    print(f"      → Colonnes : {feature_matrix.columns}")
    return feature_matrix


# ============================================================
# A7 — ÉCRITURE vers s3a://machine-learning/features/
# ============================================================
def write_feature_matrix(feature_matrix):
    """
    A7 : Écrit la Feature Matrix en Parquet vers MinIO.
    Partitionné par zone_id pour faciliter la lecture sélective par le binôme.
    """
    print(f"[A7] Écriture Feature Matrix vers {FEATURES_OUTPUT_PATH}...")

    (
        feature_matrix
        .repartition("origin_zone_id")           # 1 fichier par zone
        .write
        .mode("overwrite")
        .partitionBy("origin_zone_id")
        .parquet(FEATURES_OUTPUT_PATH)
    )

    print(f"      → ✅ Feature Matrix écrite dans {FEATURES_OUTPUT_PATH}")
    print("      → Ton binôme peut maintenant lire s3a://machine-learning/features/feature_matrix/")


# ============================================================
# MAIN PIPELINE
# ============================================================
def main():
    print("=" * 60)
    print("  TaaSim — Feature Engineering Pipeline (Semaine 6)")
    print("=" * 60)

    # Initialisation Spark
    spark = create_spark_session()

    # ── A1 : Lecture données ──────────────────────────────────
    df = load_curated_trips(spark)

    # ── A2 : Features temporelles ────────────────────────────
    df = add_temporal_features(df)

    # ── A3 : Features spatiales ──────────────────────────────
    df = add_spatial_features(spark, df)

    # ── A4 : Features météo ──────────────────────────────────
    weather_map = fetch_weather_data()
    df = add_weather_features(spark, df, weather_map)

    # ── A5 : Lag features ────────────────────────────────────
    # D'abord agrégation brute par slot, ensuite window functions
    demand_df = compute_slot_demand(df)
    demand_df = add_lag_features(demand_df)

    # ── A6 : Feature Matrix ──────────────────────────────────
    feature_matrix = build_feature_matrix(demand_df)

    # ── A7 : Écriture MinIO ──────────────────────────────────
    write_feature_matrix(feature_matrix)

    print("\n" + "=" * 60)
    print("  ✅ Feature Engineering terminé avec succès !")
    print("  📋 Résumé des 11 features produites :")
    print("     Temporelles  (4) : hour_of_day, day_of_week, is_weekend, is_friday")
    print("     Spatiales    (5) : zone_population_density + 4× zone_type one-hot")
    print("     Météo        (2) : is_raining, temperature_bucket")
    print("     Lag          (3) : demand_lag_1d, demand_lag_7d, rolling_7d_mean")
    print("     Label        (1) : demand")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()