#!/usr/bin/env python3
"""
TaaSim — Spark ETL Porto
========================
ENSA Al Hoceima — Capstone 2025-2026

Pipeline complet 100% Spark avec optimisation par grille :
  1. Lecture Porto CSV depuis MinIO raw/
  2. Remapping linéaire Porto → bbox Casablanca
  3. Translation pondérée population → zone Casa
  4. Road snapping OSMnx via UDF Spark (recherche par grille optimisée)
  5. Calcul origin_zone / dest_zone
  6. Déduplication
  7. Écriture Parquet partitionné → curated/porto-trips/
"""

import argparse, json, time, logging, os, math
import heapq

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


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--minio-endpoint",     default="minio:9000")
    p.add_argument("--minio-access-key",   default="taasim")
    p.add_argument("--minio-secret-key",   default="taasim123")
    p.add_argument("--raw-bucket",         default="raw")
    p.add_argument("--raw-prefix",         default="porto-trips/train.csv")
    p.add_argument("--curated-bucket",     default="curated")
    p.add_argument("--curated-prefix",     default="trips")
    p.add_argument("--geojson-path",       default="/opt/spark/data/data/Arrondissements.geojson")
    p.add_argument("--graph-path",         default="/opt/spark/data/data/casa_graph.graphml")
    p.add_argument("--max-rows",           type=int, default=None)
    p.add_argument("--shuffle-partitions", type=int, default=200)
    p.add_argument("--grid-size",          type=int, default=100, help="Taille de la grille pour recherche spatiale")
    return p.parse_args()


def build_spark(args):
    spark = (
        SparkSession.builder
        .appName("TaaSim-ETL-Porto")
        .config("spark.master", "spark://spark-master:7077")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "4")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.default.parallelism", "200")
        .config("spark.broadcast.compress", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        .config("spark.sql.broadcastTimeout", "1200")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{args.minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    
    log.info(f"  ✓ Spark connecté au master: {spark.sparkContext.master}")
    log.info(f"  ✓ {spark.sparkContext.defaultParallelism} tasks parallèles")
    return spark


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
        name  = row[name_col]
        geom  = row.geometry
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


def build_grid_index(nodes_list, grid_size=100):
    """
    Construit un index spatial par grille pour recherche rapide des nœuds.
    
    Args:
        nodes_list: Liste de tuples (node_id, lon, lat)
        grid_size: Nombre de cellules par dimension (grid_size x grid_size)
    
    Returns:
        Dictionnaire contenant la grille et les métadonnées pour la recherche
    """
    if not nodes_list:
        return {"grid": {}, "nodes_list": [], "grid_size": grid_size}
    
    # Calculer les bornes
    lons = [n[1] for n in nodes_list]
    lats = [n[2] for n in nodes_list]
    lon_min, lon_max = min(lons), max(lons)
    lat_min, lat_max = min(lats), max(lats)
    
    # Éviter la division par zéro
    lon_step = (lon_max - lon_min) / grid_size if grid_size > 0 and lon_max > lon_min else 0.001
    lat_step = (lat_max - lat_min) / grid_size if grid_size > 0 and lat_max > lat_min else 0.001
    
    # Construire la grille
    grid = {}
    for nid, lon, lat in nodes_list:
        gx = int((lon - lon_min) / lon_step) if lon_step > 0 else 0
        gy = int((lat - lat_min) / lat_step) if lat_step > 0 else 0
        gx = max(0, min(grid_size - 1, gx))
        gy = max(0, min(grid_size - 1, gy))
        key = (gx, gy)
        if key not in grid:
            grid[key] = []
        grid[key].append((nid, lon, lat))
    
    log.info(f"  ✓ Grille construite: {len(grid)} cellules occupées sur {grid_size}x{grid_size}")
    
    return {
        "grid": grid,
        "lon_min": lon_min,
        "lon_max": lon_max,
        "lat_min": lat_min,
        "lat_max": lat_max,
        "lon_step": lon_step,
        "lat_step": lat_step,
        "grid_size": grid_size,
        "nodes_list": nodes_list  # fallback
    }


def nearest_node_grid(lon, lat, grid_data):
    """
    Trouve le nœud le plus proche en utilisant l'index par grille.
    Ne regarde que la cellule cible et ses 8 voisines.
    """
    # 1. Trouver la cellule cible
    gx = int((lon - grid_data["lon_min"]) / grid_data["lon_step"])
    gy = int((lat - grid_data["lat_min"]) / grid_data["lat_step"])
    gx = max(0, min(grid_data["grid_size"] - 1, gx))
    gy = max(0, min(grid_data["grid_size"] - 1, gy))
    
    # 2. Collecter les candidats des cellules voisines (9 cellules)
    candidates = []
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            nx, ny = gx + dx, gy + dy
            if 0 <= nx < grid_data["grid_size"] and 0 <= ny < grid_data["grid_size"]:
                key = (nx, ny)
                if key in grid_data["grid"]:
                    candidates.extend(grid_data["grid"][key])
    
    # 3. Fallback: tous les nœuds si aucune cellule trouvée
    if not candidates:
        candidates = grid_data["nodes_list"]
    
    # 4. Recherche linéaire sur les candidats seulement
    best_id = None
    best_dist = float("inf")
    for nid, nlon, nlat in candidates:
        d = (nlon - lon) ** 2 + (nlat - lat) ** 2
        if d < best_dist:
            best_dist = d
            best_id = nid
    
    return best_id


def load_graph_as_dict(graph_path, grid_size=100):
    """
    Charge le graphe OSMnx et le convertit en dictionnaires Python
    broadcastables dans Spark :
      - nodes_dict : {node_id: (lon, lat)}
      - adj_dict   : {node_id: {neighbor_id: length}}
      - nodes_list : list de (node_id, lon, lat)
      - grid_data  : index spatial pour recherche rapide
    """
    import osmnx as ox

    log.info(f"  Chargement graphe depuis {graph_path}...")
    if os.path.exists(graph_path):
        G = ox.load_graphml(graph_path)
    else:
        log.info("  Téléchargement graphe OSM Casablanca...")
        G = ox.graph_from_place("Casablanca, Morocco", network_type="drive")
        ox.save_graphml(G, graph_path)

    log.info(f"  ✓ Graphe chargé — {len(G.nodes)} nœuds, {len(G.edges)} arêtes")
    log.info("  Conversion en dictionnaires Python pour broadcast...")

    # Dictionnaire des noeuds : id → (lon, lat)
    nodes_dict = {}
    nodes_list = []
    for n in G.nodes:
        lon = float(G.nodes[n].get("x", 0))
        lat = float(G.nodes[n].get("y", 0))
        nodes_dict[int(n)] = (lon, lat)
        nodes_list.append((int(n), lon, lat))

    # Dictionnaire d'adjacence : id → {neighbor: length}
    adj_dict = {}
    for u, v, data in G.edges(data=True):
        u, v = int(u), int(v)
        length = float(data.get("length", 1.0))
        
        if u not in adj_dict:
            adj_dict[u] = {}
        if v not in adj_dict[u] or adj_dict[u][v] > length:
            adj_dict[u][v] = length
        
        # Graphe non-dirigé → les deux sens
        if v not in adj_dict:
            adj_dict[v] = {}
        if u not in adj_dict[v] or adj_dict[v][u] > length:
            adj_dict[v][u] = length

    log.info(f"  ✓ Dictionnaires prêts ({len(nodes_dict)} nœuds)")

    # Construire l'index spatial par grille
    log.info(f"  Construction de l'index spatial (grille {grid_size}x{grid_size})...")
    grid_data = build_grid_index(nodes_list, grid_size)
    
    return nodes_dict, adj_dict, nodes_list, grid_data


def build_udfs(zones_info, bounds_dict, nodes_dict, adj_dict, nodes_list, grid_data, spark):
    import random

    # Bornes Porto (fixes, issues du dataset Porto)
    PORTO_LON_MIN, PORTO_LON_MAX = -8.73, -8.55
    PORTO_LAT_MIN, PORTO_LAT_MAX = 41.10, 41.22

    cb_s = json.dumps([bounds_dict["casa_lon_min"], bounds_dict["casa_lat_min"],
                       bounds_dict["casa_lon_max"], bounds_dict["casa_lat_max"]])
    pb_s = json.dumps([PORTO_LON_MIN, PORTO_LAT_MIN, PORTO_LON_MAX, PORTO_LAT_MAX])

    # Broadcast toutes les données
    zones_bc    = spark.sparkContext.broadcast(json.dumps(zones_info))
    total_pop   = sum(z["population"] for z in zones_info)
    weights     = [z["population"] / total_pop for z in zones_info]
    weights_bc  = spark.sparkContext.broadcast(json.dumps(weights))
    nodes_bc    = spark.sparkContext.broadcast(nodes_dict)
    adj_bc      = spark.sparkContext.broadcast(adj_dict)
    grid_bc     = spark.sparkContext.broadcast(grid_data)  # ← Index spatial broadcasté

    # ── remap_udf
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
                new_lon = max(cb[0], min(cb[2], new_lon))
                new_lat = max(cb[1], min(cb[3], new_lat))
                out.append([round(new_lon, 6), round(new_lat, 6)])
            return json.dumps(out)
        except Exception:
            return None

    remap_udf = F.udf(remap_polyline, StringType())

    # ── translate_udf
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

    # ── snap_udf : road snapping OPTIMISÉ avec recherche par grille
    def snap_fn(polyline_str):
        """
        Snape une polyline sur le réseau routier OSM.
        Utilise l'index par grille pour trouver les nœuds rapidement.
        """
        try:
            pts = json.loads(polyline_str)
            if not pts or len(pts) < 2:
                return None

            nodes = nodes_bc.value
            adj = adj_bc.value
            grid_data_local = grid_bc.value

            # Fonction nearest_node avec recherche par grille (optimisée)
            def nearest_node(lon, lat):
                return nearest_node_grid(lon, lat, grid_data_local)

            # 1. Premier et dernier point de la polyline
            lon1, lat1 = pts[0]
            lon2, lat2 = pts[-1]

            # 2. Snap aux nœuds OSM les plus proches (optimisé!)
            start_node = nearest_node(lon1, lat1)
            end_node = nearest_node(lon2, lat2)

            if start_node == end_node or start_node is None or end_node is None:
                return None

            # 3. Dijkstra pour trouver le chemin le plus court
            dist = {start_node: 0.0}
            prev = {}
            heap = [(0.0, start_node)]
            visited = set()

            while heap:
                d, u = heapq.heappop(heap)
                if u in visited:
                    continue
                visited.add(u)
                if u == end_node:
                    break
                for v, length in adj.get(u, {}).items():
                    nd = d + length
                    if nd < dist.get(v, float("inf")):
                        dist[v] = nd
                        prev[v] = u
                        heapq.heappush(heap, (nd, v))

            # 4. Reconstruire le chemin
            if end_node not in prev and end_node != start_node:
                return None

            path = []
            cur = end_node
            while cur != start_node:
                path.append(cur)
                if cur not in prev:
                    return None
                cur = prev[cur]
            path.append(start_node)
            path.reverse()

            # 5. Convertir en coordonnées [[lon, lat], ...]
            route = []
            for nid in path:
                if nid in nodes:
                    route.append([nodes[nid][0], nodes[nid][1]])
                else:
                    return None

            if len(route) < 2:
                return None

            return json.dumps(route)

        except Exception:
            return None

    snap_udf = F.udf(snap_fn, StringType())

    # ── zone_udf
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

    # Helpers
    @F.udf(DoubleType())
    def first_lat_udf(s):
        try: 
            pts = json.loads(s)
            return float(pts[0][1]) if pts else None
        except: 
            return None

    @F.udf(DoubleType())
    def first_lon_udf(s):
        try: 
            pts = json.loads(s)
            return float(pts[0][0]) if pts else None
        except: 
            return None

    @F.udf(DoubleType())
    def last_lat_udf(s):
        try: 
            pts = json.loads(s)
            return float(pts[-1][1]) if pts else None
        except: 
            return None

    @F.udf(DoubleType())
    def last_lon_udf(s):
        try: 
            pts = json.loads(s)
            return float(pts[-1][0]) if pts else None
        except: 
            return None

    @F.udf(IntegerType())
    def duration_udf(s):
        try: 
            return len(json.loads(s)) * 15
        except: 
            return None

    return (remap_udf, translate_udf, snap_udf, zone_udf,
            first_lat_udf, first_lon_udf, last_lat_udf, last_lon_udf, duration_udf)


def main():
    args  = parse_args()
    t0    = time.time()

    log.info("🚕 TaaSim — Spark ETL Porto (snapping optimisé par grille)")
    log.info(f"   Source  : s3a://{args.raw_bucket}/{args.raw_prefix}")
    log.info(f"   Output  : s3a://{args.curated_bucket}/{args.curated_prefix}")
    log.info(f"   Grid size : {args.grid_size}x{args.grid_size}")
    log.info("")

    spark = build_spark(args)

    # Chargement ressources
    log.info("Chargement GeoJSON + graphe OSM...")
    zones_info, bounds_dict           = load_zones(args.geojson_path)
    nodes_dict, adj_dict, nodes_list, grid_data = load_graph_as_dict(args.graph_path, args.grid_size)

    # UDFs
    (remap_udf, translate_udf, snap_udf, zone_udf,
     first_lat_udf, first_lon_udf, last_lat_udf, last_lon_udf,
     duration_udf) = build_udfs(zones_info, bounds_dict, nodes_dict, adj_dict, nodes_list, grid_data, spark)

    # ── ÉTAPE 1 : Lecture
    log.info("ÉTAPE 1 — Lecture Porto CSV depuis MinIO...")
    df = spark.read.csv(f"s3a://{args.raw_bucket}/{args.raw_prefix}", header=True)
    df = (df.filter(F.col("MISSING_DATA") == "False")
            .filter(F.col("POLYLINE").isNotNull())
            .filter(F.col("POLYLINE") != "[]"))
    if args.max_rows:
        df = df.limit(args.max_rows)
    c_raw = df.count()
    log.info(f"  ✓ {c_raw:,} trajets valides chargés")

    # ── ÉTAPE 2 : Remapping linéaire
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

    # ── ÉTAPE 4 : Road snapping via UDF Spark (OPTIMISÉ)
    log.info("ÉTAPE 4 — Road snapping OSM (recherche par grille, UDF Spark)...")
    df = df.withColumn("snapped_polyline", snap_udf(F.col("translated_polyline"))) \
           .drop("translated_polyline")
    
    # Filtrer les trajets qui n'ont pas pu être snappés
    df = df.filter(F.col("snapped_polyline").isNotNull())
    c_snapped = df.count()
    log.info(f"  ✓ {c_snapped:,} trajets après snapping (taux: {c_snapped/c_remapped*100:.1f}%)")

    # ── ÉTAPE 5 : origin_zone / dest_zone
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

    # ── ÉTAPE 6 : Métadonnées + déduplication
    log.info("ÉTAPE 6 — Métadonnées + déduplication...")
    df = (df
        .withColumn("trip_id",           F.col("TRIP_ID").cast(StringType()))
        .withColumn("taxi_id",           F.col("TAXI_ID").cast(IntegerType()))
        .withColumn("timestamp",         F.col("TIMESTAMP").cast(LongType()))
        .withColumn("trip_duration_sec", duration_udf(F.col("snapped_polyline")))
        .withColumn("year_month",
                    F.date_format(F.from_unixtime(F.col("TIMESTAMP").cast("long")), "yyyy-MM")))
    before = df.count()
    df = df.dropDuplicates(["trip_id"])
    c_deduped = df.count()
    log.info(f"  ✓ {before - c_deduped} doublons supprimés → {c_deduped:,} trajets uniques")

    # ── ÉTAPE 7 : Écriture Parquet
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

    # Rapport
    elapsed = time.time() - t0
    log.info("")
    log.info("=" * 55)
    log.info("📊 RAPPORT DE VALIDATION ETL")
    log.info("=" * 55)
    log.info(f"  Lignes brutes           : {c_raw:>10,}")
    log.info(f"  Après remapping         : {c_remapped:>10,}")
    log.info(f"  Après snapping          : {c_snapped:>10,}")
    log.info(f"  Après déduplication     : {c_deduped:>10,}")
    log.info(f"  Temps total             : {elapsed:>10.1f} s")
    log.info(f"  Critère < 5 min         : {'✅ PASS' if elapsed < 300 else '⚠️  dépasse 5min'}")
    
    # Afficher statistiques de la grille
    log.info("")
    log.info("  Statistiques de l'index spatial:")
    log.info(f"    Grille: {args.grid_size}x{args.grid_size}")
    log.info(f"    Cellules occupées: {len(grid_data['grid'])}")
    log.info(f"    Nœuds par cellule (moyenne): {len(nodes_list) / max(1, len(grid_data['grid'])):.1f}")
    
    try:
        top = (df_final.groupBy("origin_zone_name").count()
               .orderBy(F.col("count").desc()).limit(5).toPandas())
        log.info("")
        log.info("  Top 5 zones origin :")
        for _, r in top.iterrows():
            log.info(f"    {str(r['origin_zone_name'])[:30]:<32} {int(r['count']):>6,} trajets")
    except Exception as e:
        log.warning(f"  Impossible d'afficher le top zones: {e}")
    log.info("=" * 55)

    spark.stop()
    log.info("✅ ETL terminé.")


if __name__ == "__main__":
    main()