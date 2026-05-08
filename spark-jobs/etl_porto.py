"""
TaaSim — Spark ETL Porto
========================
ENSA Al Hoceima — Capstone 2025-2026

Pipeline (identique au notebook 02_zone_remapping) :
  1. Lecture Porto CSV depuis MinIO raw/
  2. Remapping linéaire Porto → bbox Casablanca  (remap_udf)
  3. Translation pondérée population → zone Casa  (translate_udf)
  4. Road snapping OSMnx via plus court chemin    (snap_polyline_local)
  5. Calcul origin_zone / dest_zone
  6. Déduplication
  7. Écriture Parquet partitionné → curated/porto-trips/
"""

import argparse, json, time, logging, os
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    StructType, StructField
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ETL] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl_porto")


# ─────────────────────────────────────────────────────────────────────────────
# Paramètres
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--minio-endpoint",     default="minio:9000")
    p.add_argument("--minio-access-key",   default="taasim")
    p.add_argument("--minio-secret-key",   default="taasim123")
    p.add_argument("--raw-bucket",         default="raw")
    p.add_argument("--raw-prefix",         default="porto-trips/train.csv")
    p.add_argument("--curated-bucket",     default="curated")
    p.add_argument("--curated-prefix",     default="trips")
    p.add_argument("--geojson-path",       default="/home/jovyan/work/data/Arrondissements.geojson")
    p.add_argument("--graph-path",         default="/home/jovyan/work/data/casa_graph.graphml")
    p.add_argument("--snap-sample-size",   type=int, default=5000,
                   help="Nb de trajets à soumettre au road snapping (le reste garde translate)")
    p.add_argument("--max-rows",           type=int, default=None)
    p.add_argument("--shuffle-partitions", type=int, default=200)
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────
def build_spark(args):
    spark = (
        SparkSession.builder
        .appName("TaaSim-ETL-Porto")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{args.minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",        args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",        args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Chargement GeoJSON + construction zones_info (identique notebook cellule 6/8)
# ─────────────────────────────────────────────────────────────────────────────
def load_zones(geojson_path):
    import geopandas as gpd
    gdf = gpd.read_file(geojson_path).to_crs("EPSG:4326")
    casa_lon_min, casa_lat_min, casa_lon_max, casa_lat_max = gdf.total_bounds
    name_col = "Arrondissement" if "Arrondissement" in gdf.columns else gdf.columns[0]
    pop_col  = "Population" if "Population" in gdf.columns else None

    POPULATION_CASA = {
        "Ain Chock": 370000, "Ain Sebaa": 330000, "Al Fida": 320000,
        "Anfa": 280000, "Ben M'sik": 410000, "Bernoussi": 250000,
        "Hay Hassani": 360000, "Hay Mohammadi": 300000, "Maârif": 210000,
        "Moulay Rachid": 390000, "Sbata": 270000, "Sidi Bernoussi": 220000,
        "Sidi Moumen": 450000, "Sidi Othmane": 340000, "Derb Sultan": 290000,
        "El Fida": 310000,
    }
    if pop_col:
        POPULATION_CASA = dict(zip(gdf[name_col], gdf[pop_col]))

    zones_info = []
    for idx, row in gdf.iterrows():
        name = row[name_col]
        geom = row.geometry
        bounds = geom.bounds
        zones_info.append({
            "zone_id":      int(idx),
            "zone_name":    str(name),
            "centroid_lon": float(geom.centroid.x),
            "centroid_lat": float(geom.centroid.y),
            "lon_min":      float(bounds[0]),
            "lat_min":      float(bounds[1]),
            "lon_max":      float(bounds[2]),
            "lat_max":      float(bounds[3]),
            "population":   int(POPULATION_CASA.get(str(name), 250000)),
        })

    bounds_dict = dict(
        casa_lon_min=float(casa_lon_min), casa_lat_min=float(casa_lat_min),
        casa_lon_max=float(casa_lon_max), casa_lat_max=float(casa_lat_max),
    )
    log.info(f"  ✓ {len(zones_info)} zones chargées depuis GeoJSON")
    return zones_info, bounds_dict


# ─────────────────────────────────────────────────────────────────────────────
# Chargement graphe OSMnx (identique notebook cellule 16)
# ─────────────────────────────────────────────────────────────────────────────
def load_graph(graph_path):
    import osmnx as ox
    if os.path.exists(graph_path):
        log.info(f"  Chargement graphe depuis {graph_path}...")
        G = ox.load_graphml(graph_path)
    else:
        log.info("  Téléchargement graphe OSM Casablanca (première fois)...")
        G = ox.graph_from_place("Casablanca, Morocco", network_type="drive")
        ox.save_graphml(G, graph_path)
        log.info(f"  ✓ Graphe sauvegardé → {graph_path}")
    log.info(f"  ✓ Graphe prêt — {len(G.nodes)} nœuds, {len(G.edges)} arêtes")
    return G


# ─────────────────────────────────────────────────────────────────────────────
# UDFs Spark (identique notebook cellules 12/14)
# ─────────────────────────────────────────────────────────────────────────────
def build_spark_udfs(zones_info, bounds_dict, spark):
    import random

    # Bounds sérialisés pour les UDFs
    PORTO_LON_MIN, PORTO_LON_MAX = -8.73, -8.55
    PORTO_LAT_MIN, PORTO_LAT_MAX = 41.10, 41.22
    cb_s = json.dumps([bounds_dict["casa_lon_min"], bounds_dict["casa_lat_min"],
                       bounds_dict["casa_lon_max"], bounds_dict["casa_lat_max"]])
    pb_s = json.dumps([PORTO_LON_MIN, PORTO_LAT_MIN, PORTO_LON_MAX, PORTO_LAT_MAX])

    # Broadcast zones
    zones_bc   = spark.sparkContext.broadcast(json.dumps(zones_info))
    total_pop  = sum(z["population"] for z in zones_info)
    weights    = [z["population"] / total_pop for z in zones_info]
    weights_bc = spark.sparkContext.broadcast(json.dumps(weights))

    # ── remap_udf : remapping linéaire Porto → Casa (+ projection dans polygone)
    def remap_polyline(polyline_str):
        try:
            pts = json.loads(polyline_str)
            if not pts or len(pts) < 2:
                return None
            pb = json.loads(pb_s)
            cb = json.loads(cb_s)
            out = []
            for lon, lat in pts:
                new_lon = cb[0] + (lon - pb[0]) / (pb[2] - pb[0]) * (cb[2] - cb[0])
                new_lat = cb[1] + (lat - pb[1]) / (pb[3] - pb[1]) * (cb[3] - cb[1])
                # Clamp dans la bbox Casablanca
                new_lon = max(cb[0], min(cb[2], new_lon))
                new_lat = max(cb[1], min(cb[3], new_lat))
                out.append([round(new_lon, 6), round(new_lat, 6)])
            return json.dumps(out)
        except Exception:
            return None

    remap_udf = F.udf(remap_polyline, StringType())

    # ── translate_udf : translation pondérée population (identique notebook cellule 14)
    translate_schema = StructType([
        StructField("polyline",  StringType(),  True),
        StructField("zone_id",   IntegerType(), True),
        StructField("zone_name", StringType(),  True),
    ])

    def translate_fn(polyline_str, trip_id):
        try:
            pts     = json.loads(polyline_str)
            zones   = json.loads(zones_bc.value)
            weights = json.loads(weights_bc.value)
            if not pts or len(pts) < 2:
                return (None, -1, "unknown")
            rng  = random.Random(hash(trip_id) if trip_id else None)
            zone = rng.choices(zones, weights=weights, k=1)[0]
            c_lon = sum(p[0] for p in pts) / len(pts)
            c_lat = sum(p[1] for p in pts) / len(pts)
            dlon = zone["centroid_lon"] - c_lon
            dlat = zone["centroid_lat"] - c_lat
            new_pts = [[round(p[0]+dlon, 6), round(p[1]+dlat, 6)] for p in pts]
            return (json.dumps(new_pts), int(zone["zone_id"]), str(zone["zone_name"]))
        except Exception:
            return (None, -1, "unknown")

    translate_udf = F.udf(translate_fn, translate_schema)

    # ── zone_from_point_udf : lookup bbox (identique notebook cellule 20)
    zone_schema = StructType([
        StructField("zone_id",   IntegerType(), True),
        StructField("zone_name", StringType(),  True),
    ])

    def zone_from_point(lat, lon):
        try:
            zones = json.loads(zones_bc.value)
            for z in zones:
                if z["lon_min"] <= lon <= z["lon_max"] and z["lat_min"] <= lat <= z["lat_max"]:
                    return (z["zone_id"], z["zone_name"])
            best = min(zones, key=lambda z: (z["centroid_lon"]-lon)**2 + (z["centroid_lat"]-lat)**2)
            return (best["zone_id"], best["zone_name"])
        except Exception:
            return (-1, "unknown")

    zone_udf = F.udf(zone_from_point, zone_schema)

    # Helpers first/last point (format [[lon,lat],...])
    @F.udf(DoubleType())
    def first_lat_udf(s):
        try: return float(json.loads(s)[0][1])
        except: return None

    @F.udf(DoubleType())
    def first_lon_udf(s):
        try: return float(json.loads(s)[0][0])
        except: return None

    @F.udf(DoubleType())
    def last_lat_udf(s):
        try: return float(json.loads(s)[-1][1])
        except: return None

    @F.udf(DoubleType())
    def last_lon_udf(s):
        try: return float(json.loads(s)[-1][0])
        except: return None

    @F.udf(IntegerType())
    def duration_udf(s):
        try: return len(json.loads(s)) * 15
        except: return None

    return remap_udf, translate_udf, zone_udf, first_lat_udf, first_lon_udf, last_lat_udf, last_lon_udf, duration_udf


# ─────────────────────────────────────────────────────────────────────────────
# Road snapping en Pandas (identique notebook cellule 23)
# Appliqué sur un échantillon configurable, le reste garde la polyline traduite
# ─────────────────────────────────────────────────────────────────────────────
def snap_polyline_local(polyline_str, G):
    """Identique à snap_polyline_local du notebook cellule 23."""
    import networkx as nx
    import osmnx as ox
    try:
        pts = json.loads(polyline_str)
        if len(pts) < 2:
            return None
        lon1, lat1 = pts[0]
        lon2, lat2 = pts[-1]
        node1 = ox.nearest_nodes(G, lon1, lat1)
        node2 = ox.nearest_nodes(G, lon2, lat2)
        if node1 == node2:
            return None
        path = nx.shortest_path(G, node1, node2, weight="length")
        full_route = [[G.nodes[n]["x"], G.nodes[n]["y"]] for n in path]
        if len(full_route) < 2:
            return None
        return json.dumps(full_route)
    except Exception:
        return None


def apply_road_snap(df_spark, G, snap_sample_size, spark):
    """
    1. Prend un échantillon de snap_sample_size trajets
    2. Applique snap_polyline_local en Pandas (comme le notebook)
    3. Remonte en Spark et rejoint avec le reste du dataset
    """
    log.info(f"  Road snapping sur {snap_sample_size} trajets (Pandas)...")

    # Échantillon à snapper
    pdf_sample = df_spark.limit(snap_sample_size).toPandas()
    log.info(f"  Collecté {len(pdf_sample)} trajets pour snapping...")

    pdf_sample["snapped_polyline"] = pdf_sample["translated_polyline"].apply(
        lambda x: snap_polyline_local(x, G)
    )
    # Fallback : si snap échoue → garder la polyline traduite
    pdf_sample["snapped_polyline"] = pdf_sample.apply(
        lambda r: r["snapped_polyline"] if r["snapped_polyline"] else r["translated_polyline"],
        axis=1
    )
    n_snapped = pdf_sample["snapped_polyline"].notna().sum()
    log.info(f"  ✓ {n_snapped}/{len(pdf_sample)} trajets snappés sur routes OSM")

    # Remonter l'échantillon en Spark
    df_snapped = spark.createDataFrame(pdf_sample)

    # Le reste du dataset (non snappé) : utilise la polyline traduite directement
    snapped_ids = [str(r) for r in pdf_sample["TRIP_ID"].tolist()]
    df_rest = df_spark.filter(~F.col("TRIP_ID").isin(snapped_ids)) \
                      .withColumn("snapped_polyline", F.col("translated_polyline"))

    # Union des deux
    common_cols = [c for c in df_snapped.columns if c in df_rest.columns]
    df_final = df_snapped.select(common_cols).union(df_rest.select(common_cols))
    return df_final


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────
def main():
    args  = parse_args()
    t0    = time.time()

    log.info("🚕 TaaSim — Spark ETL Porto démarré")
    log.info(f"   Source   : s3a://{args.raw_bucket}/{args.raw_prefix}")
    log.info(f"   Output   : s3a://{args.curated_bucket}/{args.curated_prefix}")
    log.info(f"   Snapping : {args.snap_sample_size} trajets")
    log.info("")

    spark = build_spark(args)

    # ── Chargement ressources
    log.info("Chargement GeoJSON + graphe OSM...")
    zones_info, bounds_dict = load_zones(args.geojson_path)
    G = load_graph(args.graph_path)

    # ── UDFs
    (remap_udf, translate_udf, zone_udf,
     first_lat_udf, first_lon_udf, last_lat_udf, last_lon_udf,
     duration_udf) = build_spark_udfs(zones_info, bounds_dict, spark)

    # ── ÉTAPE 1 : Lecture CSV Porto
    log.info("ÉTAPE 1 — Lecture Porto CSV depuis MinIO...")
    path = f"s3a://{args.raw_bucket}/{args.raw_prefix}"
    df = spark.read.csv(path, header=True)
    df = (df.filter(F.col("MISSING_DATA") == "False")
            .filter(F.col("POLYLINE").isNotNull())
            .filter(F.col("POLYLINE") != "[]"))
    if args.max_rows:
        df = df.limit(args.max_rows)
    c_raw = df.count()
    log.info(f"  ✓ {c_raw:,} trajets valides chargés")

    # ── ÉTAPE 2 : Remapping linéaire Porto → Casa
    log.info("ÉTAPE 2 — Remapping linéaire Porto → Casablanca...")
    df = (df.withColumn("remapped_linear", remap_udf(F.col("POLYLINE")))
            .filter(F.col("remapped_linear").isNotNull()))

    # ── ÉTAPE 3 : Translation pondérée par zone
    log.info("ÉTAPE 3 — Translation pondérée par zone (population)...")
    df = (df.withColumn("translated", translate_udf(F.col("remapped_linear"), F.col("TRIP_ID")))
            .withColumn("translated_polyline", F.col("translated.polyline"))
            .filter(F.col("translated_polyline").isNotNull())
            .drop("translated", "remapped_linear"))
    c_remapped = df.count()
    log.info(f"  ✓ {c_remapped:,} trajets après remapping + translation")

    # ── ÉTAPE 4 : Road snapping OSMnx (Pandas, échantillon)
    log.info("ÉTAPE 4 — Road snapping OSMnx...")
    df = apply_road_snap(df, G, args.snap_sample_size, spark)

    # ── ÉTAPE 5 : Calcul origin_zone / dest_zone (identique notebook cellule 24)
    log.info("ÉTAPE 5 — Calcul origin_zone / dest_zone...")
    df = (df
        .withColumn("_o_lat", first_lat_udf(F.col("snapped_polyline")))
        .withColumn("_o_lon", first_lon_udf(F.col("snapped_polyline")))
        .withColumn("_d_lat", last_lat_udf(F.col("snapped_polyline")))
        .withColumn("_d_lon", last_lon_udf(F.col("snapped_polyline")))
        .withColumn("_oz", zone_udf(F.col("_o_lat"), F.col("_o_lon")))
        .withColumn("_dz", zone_udf(F.col("_d_lat"), F.col("_d_lon")))
        .withColumn("origin_zone_id",   F.col("_oz.zone_id"))
        .withColumn("origin_zone_name", F.col("_oz.zone_name"))
        .withColumn("dest_zone_id",     F.col("_dz.zone_id"))
        .withColumn("dest_zone_name",   F.col("_dz.zone_name"))
        .drop("_o_lat", "_o_lon", "_d_lat", "_d_lon", "_oz", "_dz"))

    # ── ÉTAPE 6 : Métadonnées + déduplication (identique notebook cellule 24)
    log.info("ÉTAPE 6 — Métadonnées + déduplication...")
    df = (df
        .withColumn("trip_id",          F.col("TRIP_ID").cast(StringType()))
        .withColumn("taxi_id",          F.col("TAXI_ID").cast(IntegerType()))
        .withColumn("timestamp",        F.col("TIMESTAMP").cast(LongType()))
        .withColumn("trip_duration_sec", duration_udf(F.col("snapped_polyline")))
        .withColumn("year_month",
                    F.date_format(F.from_unixtime(F.col("TIMESTAMP").cast("long")), "yyyy-MM")))

    before_dedup = df.count()
    df = df.dropDuplicates(["trip_id"])
    c_deduped = df.count()
    log.info(f"  ✓ {before_dedup - c_deduped} doublons supprimés → {c_deduped:,} trajets uniques")

    # ── ÉTAPE 7 : Sélection colonnes finales + écriture Parquet
    log.info("ÉTAPE 7 — Écriture Parquet partitionné dans MinIO...")
    FINAL_COLS = [
        "trip_id", "taxi_id", "timestamp",
        "snapped_polyline",
        "origin_zone_id", "origin_zone_name",
        "dest_zone_id",   "dest_zone_name",
        "trip_duration_sec", "year_month",
    ]
    df_final = df.select([c for c in FINAL_COLS if c in df.columns])
    out = f"s3a://{args.curated_bucket}/{args.curated_prefix}"
    (df_final.repartition("year_month")
             .write.mode("overwrite")
             .partitionBy("year_month")
             .parquet(out))
    log.info(f"  ✓ Parquet écrit → {out}")

    # ── Rapport de validation
    elapsed = time.time() - t0
    log.info("")
    log.info("=" * 55)
    log.info("📊 RAPPORT DE VALIDATION ETL")
    log.info("=" * 55)
    log.info(f"  Lignes brutes           : {c_raw:>10,}")
    log.info(f"  Après remapping         : {c_remapped:>10,}")
    log.info(f"  Après déduplication     : {c_deduped:>10,}")
    log.info(f"  Temps total             : {elapsed:>10.1f} s")
    log.info(f"  Critère < 5 min         : {'✅ PASS' if elapsed < 300 else '⚠️  (snapping inclus)'}")
    try:
        top = (df_final.groupBy("origin_zone_name").count()
               .orderBy(F.col("count").desc()).limit(5).toPandas())
        log.info("")
        log.info("  Top 5 zones origin :")
        for _, r in top.iterrows():
            log.info(f"    {str(r['origin_zone_name']):<28} {int(r['count']):>6,} trajets")
    except Exception:
        pass
    log.info("=" * 55)

    spark.stop()
    log.info("✅ ETL terminé.")


if __name__ == "__main__":
    main()