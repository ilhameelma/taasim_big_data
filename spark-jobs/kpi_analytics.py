"""
TaaSim — Spark SQL Analytics KPI
==================================
ENSA Al Hoceima — Capstone 2025-2026

Pipeline :
  1. Lecture Parquet depuis MinIO curated/trips/
  2. Calcul des 4 KPIs hebdomadaires via Spark SQL :
       - trips_count     : nombre de trajets par zone par semaine
       - avg_duration_sec: durée moyenne des trajets
       - peak_hour       : heure de pointe (plus forte demande)
       - coverage_gap    : True si demande > 0 mais < 2 taxis distincts
  3. Écriture dans Cassandra table kpi_metrics
"""

import argparse
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [KPI] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kpi_analytics")


# ─────────────────────────────────────────────────────────────────────────────
# Paramètres
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--minio-endpoint",     default="minio:9000")
    p.add_argument("--minio-access-key",   default="taasim")
    p.add_argument("--minio-secret-key",   default="taasim123")
    p.add_argument("--curated-bucket",     default="curated")
    p.add_argument("--curated-prefix",     default="trips")
    p.add_argument("--cassandra-host",     default="cassandra")
    p.add_argument("--cassandra-port",     default="9042")
    p.add_argument("--cassandra-keyspace", default="taasim")
    p.add_argument("--cassandra-table",    default="kpi_metrics")
    p.add_argument("--city",               default="Casablanca")
    p.add_argument("--shuffle-partitions", type=int, default=100)
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession — connectée au cluster spark-master + Cassandra connector
# ─────────────────────────────────────────────────────────────────────────────
def build_spark(args) -> SparkSession:
    connector_jar = "/opt/spark/jars/extras/spark-cassandra-connector_2.12-3.4.1.jar"
    s3_jar        = "/opt/spark/jars/extras/hadoop-aws-3.3.4.jar"
    sdk_jar       = "/opt/spark/jars/extras/aws-java-sdk-bundle-1.12.262.jar"

    spark = (
        SparkSession.builder
        .appName("TaaSim-KPI-Analytics")
        .master("spark://spark-master:7077")
        .config("spark.driver.memory",           "2g")
        .config("spark.executor.memory",         "2g")
        .config("spark.executor.cores",          "2")
        .config("spark.sql.shuffle.partitions",  str(args.shuffle_partitions))
        # ── Cassandra
        .config("spark.cassandra.connection.host", args.cassandra_host)
        .config("spark.cassandra.connection.port", args.cassandra_port)
        # ── MinIO S3A
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{args.minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",        args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",        args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"  ✓ Spark connecté : {spark.sparkContext.master}")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Étape 1 — Lecture Parquet
# ─────────────────────────────────────────────────────────────────────────────
def step1_read(spark, args):
    log.info("ÉTAPE 1 — Lecture Parquet depuis MinIO...")
    path = f"s3a://{args.curated_bucket}/{args.curated_prefix}"
    df = spark.read.parquet(path)

    # Colonnes nécessaires
    needed = ["trip_id", "taxi_id", "timestamp",
              "origin_zone_id", "origin_zone_name",
              "trip_duration_sec", "year_month"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans le Parquet : {missing}")

    count = df.count()
    log.info(f"  ✓ {count:,} trajets chargés")
    log.info(f"  Partitions : {df.rdd.getNumPartitions()}")
    return df, count


# ─────────────────────────────────────────────────────────────────────────────
# Étape 2 — Enrichissement : year_week + hour
# ─────────────────────────────────────────────────────────────────────────────
def step2_enrich(df):
    log.info("ÉTAPE 2 — Enrichissement (year_week, hour)...")
    df = (
        df
        .withColumn("event_ts",  F.from_unixtime(F.col("timestamp").cast("long")))
        .withColumn("year_week",
                    F.concat(F.year(F.col("event_ts")).cast("string"), F.lit("-W"), F.lpad(F.weekofyear(F.col("event_ts")).cast("string"), 2, "0")))
        .withColumn("hour",
                    F.hour(F.col("event_ts")))
    )
    log.info("  ✓ Colonnes year_week et hour ajoutées")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Étape 3 — KPI 1 + 2 : trips_count + avg_duration_sec
# ─────────────────────────────────────────────────────────────────────────────
def step3_trips_kpi(df):
    log.info("ÉTAPE 3 — KPI trips_count + avg_duration_sec...")
    df_trips = (
        df
        .groupBy("origin_zone_id", "origin_zone_name", "year_week")
        .agg(
            F.count("trip_id").cast("long").alias("trips_count"),
            F.avg("trip_duration_sec").alias("avg_duration_sec"),
        )
    )
    log.info(f"  ✓ {df_trips.count():,} lignes zone×semaine calculées")
    return df_trips


# ─────────────────────────────────────────────────────────────────────────────
# Étape 4 — KPI 3 : peak_hour (heure de pointe par zone par semaine)
# ─────────────────────────────────────────────────────────────────────────────
def step4_peak_hour(df):
    log.info("ÉTAPE 4 — KPI peak_hour...")

    # Compter les trajets par zone × semaine × heure
    df_hourly = (
        df
        .groupBy("origin_zone_id", "year_week", "hour")
        .agg(F.count("trip_id").alias("hourly_count"))
    )

    # Garder uniquement l'heure avec le plus de trajets par zone × semaine
    w = Window.partitionBy("origin_zone_id", "year_week") \
              .orderBy(F.col("hourly_count").desc())

    df_peak = (
        df_hourly
        .withColumn("rank", F.rank().over(w))
        .filter(F.col("rank") == 1)
        .select(
            F.col("origin_zone_id"),
            F.col("year_week"),
            F.col("hour").alias("peak_hour"),
        )
    )
    log.info("  ✓ peak_hour calculé")
    return df_peak


# ─────────────────────────────────────────────────────────────────────────────
# Étape 5 — KPI 4 : coverage_gap
# Zones avec demande > 0 mais < 2 taxis distincts actifs
# ─────────────────────────────────────────────────────────────────────────────
def step5_coverage_gap(df):
    log.info("ÉTAPE 5 — KPI coverage_gap...")
    df_vehicles = (
        df
        .groupBy("origin_zone_id", "year_week")
        .agg(
            F.countDistinct("taxi_id").alias("distinct_vehicles"),
            F.count("trip_id").alias("demand_count"),
        )
        .withColumn(
            "coverage_gap",
            (F.col("demand_count") > 0) & (F.col("distinct_vehicles") < 2)
        )
        .select("origin_zone_id", "year_week", "coverage_gap")
    )
    n_gaps = df_vehicles.filter(F.col("coverage_gap") == True).count()
    log.info(f"  ✓ coverage_gap calculé — {n_gaps} zones/semaines en gap")
    return df_vehicles


# ─────────────────────────────────────────────────────────────────────────────
# Étape 6 — Jointure de tous les KPIs
# ─────────────────────────────────────────────────────────────────────────────
def step6_join(df_trips, df_peak, df_coverage, city):
    log.info("ÉTAPE 6 — Jointure des KPIs...")
    df_kpi = (
        df_trips
        .join(df_peak,     on=["origin_zone_id", "year_week"], how="left")
        .join(df_coverage, on=["origin_zone_id", "year_week"], how="left")
        .withColumn("city",    F.lit(city))
        .withColumn("zone_id", F.col("origin_zone_id").cast("int"))
        .withColumnRenamed("origin_zone_name", "zone_name")
        .withColumn("trips_count",      F.col("trips_count").cast("long"))
        .withColumn("avg_duration_sec", F.col("avg_duration_sec").cast("double"))
        .withColumn("peak_hour",        F.col("peak_hour").cast("int"))
        .withColumn("coverage_gap",     F.col("coverage_gap").cast("boolean"))
        # Remplir les nulls
        .fillna({"peak_hour": 0, "coverage_gap": False})
        .select(
            "city",
            "zone_id",
            "year_week",
            "zone_name",
            "trips_count",
            "avg_duration_sec",
            "peak_hour",
            "coverage_gap",
        )
    )
    total = df_kpi.count()
    log.info(f"  ✓ DataFrame KPI final : {total:,} lignes")
    return df_kpi


# ─────────────────────────────────────────────────────────────────────────────
# Étape 7 — Écriture dans Cassandra
# ─────────────────────────────────────────────────────────────────────────────
def step7_write_cassandra(df_kpi, args):
    log.info(f"ÉTAPE 7 — Écriture dans Cassandra ({args.cassandra_keyspace}.{args.cassandra_table})...")
    (
        df_kpi.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace=args.cassandra_keyspace,
            table=args.cassandra_table,
        )
        .save()
    )
    log.info("  ✓ KPIs écrits dans Cassandra")


# ─────────────────────────────────────────────────────────────────────────────
# Rapport de validation
# ─────────────────────────────────────────────────────────────────────────────
def validate(df_kpi, elapsed, count_raw):
    log.info("")
    log.info("=" * 55)
    log.info("📊 RAPPORT DE VALIDATION KPI")
    log.info("=" * 55)
    log.info(f"  Trajets en entrée        : {count_raw:>10,}")
    log.info(f"  Lignes KPI produites     : {df_kpi.count():>10,}")
    log.info(f"  Temps total              : {elapsed:>10.1f} s")
    log.info(f"  Critère < 5 min          : {'✅ PASS' if elapsed < 300 else '⚠️  dépasse 5 min'}")
    log.info("")

    # Top 5 zones par trips_count
    try:
        top = (
            df_kpi
            .groupBy("zone_name")
            .agg(F.sum("trips_count").alias("total"))
            .orderBy(F.col("total").desc())
            .limit(5)
            .toPandas()
        )
        log.info("  Top 5 zones (tous trajets) :")
        for _, r in top.iterrows():
            log.info(f"    {str(r['zone_name'])[:28]:<30} {int(r['total']):>8,} trajets")
    except Exception as e:
        log.warning(f"  (impossible d'afficher le top zones : {e})")

    # Zones en coverage gap
    try:
        gaps = (
            df_kpi
            .filter(F.col("coverage_gap") == True)
            .select("zone_name", "year_week")
            .distinct()
            .count()
        )
        log.info(f"\n  Coverage gaps détectés   : {gaps:>10,} (zone × semaine)")
    except Exception as e:
        log.warning(f"  (impossible de compter les gaps : {e})")

    log.info("=" * 55)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    t0   = time.time()

    log.info("🚕 TaaSim — Spark SQL KPI Analytics")
    log.info(f"   Source     : s3a://{args.curated_bucket}/{args.curated_prefix}")
    log.info(f"   Cassandra  : {args.cassandra_host}:{args.cassandra_port}")
    log.info(f"   Table      : {args.cassandra_keyspace}.{args.cassandra_table}")
    log.info(f"   Ville      : {args.city}")
    log.info("")

    spark = build_spark(args)

    df, count_raw = step1_read(spark, args)
    df            = step2_enrich(df)
    df_trips      = step3_trips_kpi(df)
    df_peak       = step4_peak_hour(df)
    df_coverage   = step5_coverage_gap(df)
    df_kpi        = step6_join(df_trips, df_peak, df_coverage, args.city)
    step7_write_cassandra(df_kpi, args)

    validate(df_kpi, time.time() - t0, count_raw)

    spark.stop()
    log.info("✅ KPI Analytics terminé.")


if __name__ == "__main__":
    main()