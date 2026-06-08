"""
forecast_injector.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Semaine 6 — Panel 4 Grafana : ML Forecast Injector

But :
  - Lire le modèle ML depuis MinIO (s3a://machine-learning/models/demand_v1/)
  - Construire les features pour chaque zone à l'heure actuelle
  - Générer les prédictions (forecast_demand) via le modèle GBT
  - Mettre à jour demand_zones.forecast_demand dans Cassandra

Utilisation :
  python /home/jovyan/work/ml/forecast_injector.py

  Ou en mode continu (toutes les 30s) :
  python /home/jovyan/work/ml/forecast_injector.py --loop
"""

import argparse
import time
import json
from datetime import datetime, timezone

# ============================================================
# CONFIGURATION
# ============================================================
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"

MODEL_PATH       = "s3a://machine-learning/models/demand_v1/"

CASSANDRA_HOST   = "cassandra"
CASSANDRA_PORT   = 9042
KEYSPACE         = "taasim"

# 17 zones de Casablanca (zone_id 0 → 16)
ZONE_IDS = list(range(17))

# Métadonnées des zones (population density + type)
# Correspondance avec feat_engineering.py
ZONE_META = {
    0:  {"density": 0.3, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    1:  {"density": 0.8, "urban": 1, "residential": 0, "commercial": 0, "transit": 0},
    2:  {"density": 0.7, "urban": 0, "residential": 0, "commercial": 1, "transit": 0},
    3:  {"density": 0.9, "urban": 1, "residential": 0, "commercial": 0, "transit": 0},
    4:  {"density": 0.5, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    5:  {"density": 0.5, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    6:  {"density": 0.4, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    7:  {"density": 0.6, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    8:  {"density": 0.7, "urban": 0, "residential": 0, "commercial": 1, "transit": 0},
    9:  {"density": 0.6, "urban": 0, "residential": 0, "commercial": 1, "transit": 0},
    10: {"density": 0.5, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    11: {"density": 0.6, "urban": 1, "residential": 0, "commercial": 0, "transit": 0},
    12: {"density": 0.3, "urban": 0, "residential": 0, "commercial": 0, "transit": 1},
    13: {"density": 0.4, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    14: {"density": 0.4, "urban": 0, "residential": 1, "commercial": 0, "transit": 0},
    15: {"density": 0.5, "urban": 0, "residential": 0, "commercial": 1, "transit": 0},
    16: {"density": 0.7, "urban": 1, "residential": 0, "commercial": 0, "transit": 0},
}

# Demande historique moyenne par zone (pour lag features simulées)
# Issue du report.md — avg_demand par zone sur le test set
ZONE_AVG_DEMAND = {
    0: 1.37,  1: 10.63, 2: 10.49, 3: 25.00, 4: 6.00,
    5: 5.15,  6: 2.95,  7: 4.63,  8: 12.96, 9: 8.0,
    10: 7.5, 11: 9.0,  12: 1.05, 13: 1.95, 14: 1.92,
    15: 6.0, 16: 8.0,
}


# ============================================================
# CONNEXION SPARK
# ============================================================
def create_spark_session():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("TaaSim-ForecastInjector")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# CONSTRUCTION DES FEATURES POUR L'HEURE ACTUELLE
# ============================================================
def build_features_now():
    """
    Construit un DataFrame de features pour les 17 zones
    à l'heure actuelle, prêt pour model.transform().
    Les features correspondent exactement à celles de feat_engineering.py.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, FloatType, StringType
    )

    spark = SparkSession.getActiveSession()
    now = datetime.now(timezone.utc)

    hour       = now.hour
    dow        = now.isoweekday()          # 1=Lundi ... 7=Dimanche
    is_weekend = 1 if dow in (6, 7) else 0
    is_friday  = 1 if dow == 5 else 0

    # Température simulée (Casablanca juin = chaud)
    temp_bucket = "hot"   # cold / mild / hot

    # Pluie simulée (faible probabilité en juin à Casablanca)
    is_raining = 0

    rows = []
    for zone_id in ZONE_IDS:
        meta = ZONE_META.get(zone_id, ZONE_META[0])
        avg  = ZONE_AVG_DEMAND.get(zone_id, 5.0)

        rows.append((
            zone_id,
            hour,
            dow,
            is_weekend,
            is_friday,
            float(meta["density"]),
            meta["urban"],
            meta["residential"],
            meta["commercial"],
            meta["transit"],
            is_raining,
            temp_bucket,
            float(avg),          # demand_lag_1d  (approximé par la moyenne)
            float(avg),          # demand_lag_7d  (approximé par la moyenne)
            float(avg),          # rolling_7d_mean
        ))

    schema = StructType([
        StructField("origin_zone_id",           IntegerType(), False),
        StructField("hour_of_day",              IntegerType(), False),
        StructField("day_of_week",              IntegerType(), False),
        StructField("is_weekend",               IntegerType(), False),
        StructField("is_friday",                IntegerType(), False),
        StructField("zone_population_density",  FloatType(),   False),
        StructField("zone_type_urban",          IntegerType(), False),
        StructField("zone_type_residential",    IntegerType(), False),
        StructField("zone_type_commercial",     IntegerType(), False),
        StructField("zone_type_transit_hub",    IntegerType(), False),
        StructField("is_raining",               IntegerType(), False),
        StructField("temperature_bucket",       StringType(),  False),
        StructField("demand_lag_1d",            FloatType(),   False),
        StructField("demand_lag_7d",            FloatType(),   False),
        StructField("rolling_7d_mean",          FloatType(),   False),
    ])

    df = spark.createDataFrame(rows, schema)
    return df, now


# ============================================================
# GÉNÉRER LES PRÉDICTIONS ML
# ============================================================
def generate_predictions(spark):
    """
    Charge le modèle GBT depuis MinIO et génère les prédictions
    pour les 17 zones à l'heure actuelle.
    Retourne une liste de dicts {zone_id, forecast_demand}.
    """
    from pyspark.ml import PipelineModel

    print(f"[1/3] Chargement modèle depuis {MODEL_PATH}...")
    model = PipelineModel.load(MODEL_PATH)
    print(f"      ✅ Modèle chargé — stages: {[type(s).__name__ for s in model.stages]}")

    print("[2/3] Construction des features pour l'heure actuelle...")
    df_features, now = build_features_now()

    print("[3/3] Génération des prédictions...")
    predictions = model.transform(df_features)

    results = []
    for row in predictions.select("origin_zone_id", "prediction").collect():
        forecast = max(0.0, round(float(row["prediction"]), 2))
        results.append({
            "zone_id":         int(row["origin_zone_id"]),
            "forecast_demand": forecast,
        })
        print(f"      Zone {row['origin_zone_id']:>2} → forecast = {forecast:.2f} taxis")

    return results, now


# ============================================================
# ÉCRITURE DANS CASSANDRA
# ============================================================
def update_cassandra(predictions, window_time):
    """
    Met à jour forecast_demand dans demand_zones pour chaque zone.
    Crée une nouvelle fenêtre si elle n'existe pas encore,
    ou met à jour la dernière fenêtre existante.
    """
    from cassandra.cluster import Cluster

    print(f"\n[Cassandra] Connexion à {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    # Prépare le UPDATE — on met à jour la fenêtre la plus récente
    # Si aucune fenêtre n'existe pour cette zone, on en crée une
    upsert_stmt = session.prepare("""
        UPDATE demand_zones
        SET forecast_demand = ?
        WHERE city = ? AND zone_id = ? AND window_start = ?
    """)

    # INSERT si la ligne n'existe pas (window_start = maintenant arrondi à 30s)
    insert_stmt = session.prepare("""
        INSERT INTO demand_zones
            (city, zone_id, window_start,
             active_vehicles, pending_requests, completed_trips,
             supply_demand_ratio, forecast_demand, avg_wait_time_sec,
             lat, lon)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        IF NOT EXISTS
    """)

    # Coordonnées des centroides des zones
    ZONE_COORDS = {
        0:  (33.5799, -7.6052),  1:  (33.5835, -7.6813),
        2:  (33.5463, -7.6804),  3:  (33.5891, -7.4994),
        4:  (33.5661, -7.5405),  5:  (33.5568, -7.5607),
        6:  (33.5545, -7.5813),  7:  (33.5363, -7.5590),
        8:  (33.5268, -7.6217),  9:  (33.5652, -7.5949),
        10: (33.5765, -7.6021), 11:  (33.5856, -7.5831),
        12: (33.5894, -7.5654), 13:  (33.6040, -7.5446),
        14: (33.6200, -7.5038), 15:  (33.5704, -7.6325),
        16: (33.5982, -7.6159),
    }

    # Arrondir window_start à la fenêtre de 30s en cours
    ts = window_time.timestamp()
    window_start_unix = int(ts // 30) * 30
    window_start = datetime.fromtimestamp(window_start_unix, tz=timezone.utc)

    updated = 0
    inserted = 0

    for pred in predictions:
        zone_id  = pred["zone_id"]
        forecast = pred["forecast_demand"]
        lat, lon = ZONE_COORDS.get(zone_id, (33.5731, -7.5898))

        # 1. Essayer de mettre à jour la dernière fenêtre existante
        rows = list(session.execute(
            "SELECT window_start FROM demand_zones "
            "WHERE city=%s AND zone_id=%s "
            "ORDER BY window_start DESC LIMIT 1",
            ("Casablanca", zone_id)
        ))

        if rows:
            # Mettre à jour la fenêtre existante la plus récente
            session.execute(upsert_stmt, (
                forecast,
                "Casablanca",
                zone_id,
                rows[0].window_start,
            ))
            updated += 1
        else:
            # Créer une nouvelle ligne pour cette zone
            session.execute(insert_stmt, (
                "Casablanca",
                zone_id,
                window_start,
                0,       # active_vehicles
                0,       # pending_requests
                0,       # completed_trips
                0.0,     # supply_demand_ratio
                forecast,
                0.0,     # avg_wait_time_sec
                lat,
                lon,
            ))
            inserted += 1

    session.shutdown()
    cluster.shutdown()

    print(f"      ✅ {updated} zones mises à jour, {inserted} zones créées")
    return updated + inserted


# ============================================================
# RAPPORT
# ============================================================
def print_report(predictions, elapsed):
    print("\n" + "=" * 55)
    print("📊 RAPPORT FORECAST INJECTOR")
    print("=" * 55)
    print(f"  Zones traitées   : {len(predictions)}")
    print(f"  Temps total      : {elapsed:.1f}s")
    print(f"\n  {'Zone':<6} {'Forecast (taxis/30min)':>22}")
    print("  " + "-" * 30)
    for p in sorted(predictions, key=lambda x: x["forecast_demand"], reverse=True):
        bar = "█" * int(p["forecast_demand"] / 2)
        print(f"  Zone {p['zone_id']:<2}   {p['forecast_demand']:>8.2f}  {bar}")
    print("=" * 55)
    print("\n✅ forecast_demand mis à jour dans Cassandra.")
    print("   → Rafraîchis le Panel 4 dans Grafana !")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="TaaSim — Forecast Injector (ML → Cassandra)"
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="Mode continu : re-génère les prédictions toutes les 30s"
    )
    parser.add_argument(
        "--interval", type=int, default=30,
        help="Intervalle en secondes entre deux injections (défaut: 30)"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  TaaSim — Forecast Injector")
    print("  Modèle ML → Prédictions → Cassandra demand_zones")
    print("=" * 60)

    spark = create_spark_session()

    if args.loop:
        print(f"\n🔄 Mode continu — injection toutes les {args.interval}s")
        print("   Ctrl+C pour arrêter\n")
        try:
            while True:
                t0 = time.time()
                predictions, window_time = generate_predictions(spark)
                update_cassandra(predictions, window_time)
                elapsed = time.time() - t0
                print(f"   ⏱️  Injection terminée en {elapsed:.1f}s — prochaine dans {args.interval}s")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n🛑 Arrêt demandé.")
    else:
        t0 = time.time()
        predictions, window_time = generate_predictions(spark)
        update_cassandra(predictions, window_time)
        elapsed = time.time() - t0
        print_report(predictions, elapsed)

    spark.stop()


if __name__ == "__main__":
    main()
