# %% [markdown]
# # Notebook : 02_zone_remapping.ipynb
# # TaaSim — Semaine 1 : Remapping Porto → Casablanca

# %% [markdown]
# 
# **CELLULE 0 — Installation des dépendances**<br>
# 
# 
# Installation unique depuis requirements.txt

# %%
!pip install -r /home/jovyan/requirements.txt -q

print("✅ Toutes les dépendances installées")

# %% [markdown]
#  **CELLULE 0 — Vérification des dépendances**

# %%

import importlib

required_packages = ['geopandas', 'shapely', 'pyproj', 'fiona', 'h3', 'folium']
missing = []

for package in required_packages:
    try:
        importlib.import_module(package)
    except ImportError:
        missing.append(package)

if missing:
    print(f"📦 Installation des packages manquants : {missing}")
    !pip install {' '.join(missing)} -q
else:
    print("✅ Toutes les dépendances sont déjà installées")

# %% [markdown]
# CELLULE 1 — Bounding boxes des deux villes

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import folium
import json

spark = SparkSession.builder \
    .appName("TaaSim-Remapping") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "taasim") \
    .config("spark.hadoop.fs.s3a.secret.key", "taasim123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("✅ Spark prêt avec MinIO")
# ===============================
# Bounding boxes des villes
# ===============================

# Porto : inchangé, c'est la source de données
PORTO_LON_MIN = -8.7327
PORTO_LON_MAX = -8.5539
PORTO_LAT_MIN = 41.0527
PORTO_LAT_MAX = 41.2370

# Casa : NE PAS mettre de valeurs fixes ici
# Les vraies bornes seront lues depuis le GeoJSON en cellule 1.5
# Ces variables seront assignées APRÈS le chargement du GeoJSON
CASA_LON_MIN = None
CASA_LON_MAX = None
CASA_LAT_MIN = None
CASA_LAT_MAX = None

print("✅ Bbox Porto définie")
print("⏳ Bbox Casa sera chargée depuis le GeoJSON en cellule 1.5")

# ===============================
# Affichage des bounding boxes
# ===============================
print("Porto → lon:", PORTO_LON_MIN, "→", PORTO_LON_MAX, "| lat:", PORTO_LAT_MIN, "→", PORTO_LAT_MAX)


# %%
import geopandas as gpd
import os

GEOJSON_PATH = "/home/jovyan/work/data/Arrondissements.geojson"

if os.path.exists(GEOJSON_PATH):
    gdf_arrondissements = gpd.read_file(GEOJSON_PATH)
    
    # ✅ ICI : on écrase les None de la cellule 1 avec les vraies bornes
    bounds = gdf_arrondissements.total_bounds  # [lon_min, lat_min, lon_max, lat_max]
    CASA_LON_MIN = bounds[0]
    CASA_LON_MAX = bounds[2]
    CASA_LAT_MIN = bounds[1]
    CASA_LAT_MAX = bounds[3]
    
    print(f"✅ GeoJSON chargé : {len(gdf_arrondissements)} arrondissements")
    print(f"   Bbox réelle Casa depuis GeoJSON :")
    print(f"   Lon : [{CASA_LON_MIN:.4f}, {CASA_LON_MAX:.4f}]")
    print(f"   Lat : [{CASA_LAT_MIN:.4f}, {CASA_LAT_MAX:.4f}]")
    
    # Vérification : afficher si la bbox couvre bien la ville
    for _, row in gdf_arrondissements.iterrows():
        arr = row.get("Arrondissement", "?")
        b = row.geometry.bounds
        print(f"   • {arr} → lon [{b[0]:.4f}, {b[2]:.4f}] lat [{b[1]:.4f}, {b[3]:.4f}]")
else:
    print(f"❌ GeoJSON non trouvé : {GEOJSON_PATH}")
    # Fallback d'urgence seulement
    CASA_LON_MIN = -7.7500
    CASA_LON_MAX = -7.4500
    CASA_LAT_MIN = 33.4500
    CASA_LAT_MAX = 33.6500
    gdf_arrondissements = None

# %% [markdown]
# CELLULE 2 — Fonctions de transformation

# %%
# %% [markdown]
# CELLULE 2 — Fonctions de transformation avec projection sur polygone

# %%
import math
from shapely.geometry import Point as ShapelyPoint

# ===============================
# Création du polygone unifié de Casablanca
# ===============================

if gdf_arrondissements is not None:
    CASA_UNIFIED_POLYGON = gdf_arrondissements.union_all()  # union_all() → unary_union
    print(f"✅ Polygone unifié créé")
else:
    CASA_UNIFIED_POLYGON = None

# ===============================
# Transformation linéaire standard
# ===============================

def transform_lon_linear(lon_porto):
    ratio = (lon_porto - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)
    return CASA_LON_MIN + ratio * (CASA_LON_MAX - CASA_LON_MIN)

def transform_lat_linear(lat_porto):
    ratio = (lat_porto - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)
    return CASA_LAT_MIN + ratio * (CASA_LAT_MAX - CASA_LAT_MIN)

# ===============================
# Transformation avec étirement (recommandée)
# ===============================

def transform_point_with_stretch(lon_porto, lat_porto):
    """Transformation avec étirement est-ouest"""
    # 1. Transformation linéaire standard
    lon_casa = transform_lon_linear(lon_porto)
    lat_casa = transform_lat_linear(lat_porto)
    
    # 2. Centre de la bounding box
    lon_center = (CASA_LON_MIN + CASA_LON_MAX) / 2
    lat_center = (CASA_LAT_MIN + CASA_LAT_MAX) / 2
    
    # 3. Facteurs d'étirement
    stretch_east_west = 1.35
    stretch_north_south = 1.10
    
    # 4. Appliquer l'étirement
    lon_casa = lon_center + (lon_casa - lon_center) * stretch_east_west
    lat_casa = lat_center + (lat_casa - lat_center) * stretch_north_south
    
    # 5. Correction bounding box
    lon_casa = max(CASA_LON_MIN, min(CASA_LON_MAX, lon_casa))
    lat_casa = max(CASA_LAT_MIN, min(CASA_LAT_MAX, lat_casa))
    
    return lon_casa, lat_casa

# ===============================
# NOUVELLE : Projection sur polygone (GARANTIE)
# ===============================

def project_point_to_polygon(lon, lat):
    """Projette un point à l'intérieur du polygone de Casablanca"""
    if CASA_UNIFIED_POLYGON is None:
        return lon, lat
    
    point = ShapelyPoint(lon, lat)
    if CASA_UNIFIED_POLYGON.contains(point):
        return lon, lat
    
    # Point hors polygone → projeter sur la bordure
    nearest = CASA_UNIFIED_POLYGON.boundary.interpolate(
        CASA_UNIFIED_POLYGON.boundary.project(point)
    )
    return nearest.x, nearest.y

# ===============================
# Transformation COMPLÈTE (étirement + projection)
# ===============================

def transform_point_complete(lon_porto, lat_porto):
    """
    Transforme un point Porto → Casablanca :
    1. Étirement pour bien couvrir la zone urbaine
    2. Projection sur polygone pour garantir l'intérieur
    """
    # Étape 1 : Transformation avec étirement
    lon_casa, lat_casa = transform_point_with_stretch(lon_porto, lat_porto)
    
    # Étape 2 : Projection sur polygone
    lon_casa, lat_casa = project_point_to_polygon(lon_casa, lat_casa)
    
    return lon_casa, lat_casa

# ===============================
# UDF pour PySpark
# ===============================
def remap_polyline(polyline_str):
    """Transforme un trajet Porto → Casablanca par mapping direct de zone"""
    if polyline_str is None or polyline_str == "[]":
        return None
    try:
        points = json.loads(polyline_str)
        if len(points) < 2:
            return None
        
        # Identifier la zone de départ à Porto
        start_lon, start_lat = points[0][0], points[0][1]
        porto_zone = get_porto_zone(start_lon, start_lat)
        
        # Zone équivalente à Casablanca
        casa_zone = MAPPING_PORTO_TO_CASA.get(porto_zone, "Maarif")
        
        # Centroïde de la zone cible
        target_lon, target_lat = get_casa_centroid(casa_zone)
        
        # Générer le trajet
        remapped = []
        for i, pt in enumerate(points):
            ratio = i / len(points)
            # Variation progressive pour simuler un déplacement
            lon_casa = target_lon + (ratio - 0.5) * 0.015
            lat_casa = target_lat + (ratio - 0.5) * 0.012
            remapped.append([round(lon_casa, 6), round(lat_casa, 6)])
        
        return json.dumps(remapped)
    except Exception as e:
        return None

# ===============================
# Test des fonctions
# ===============================

test_points = [
    (-8.6110, 41.1496),  # Centre de Porto
    (-8.6500, 41.1600),  # Ouest de Porto (côté mer)
    (-8.5700, 41.1400),  # Est de Porto
    (-8.6100, 41.2000),  # Nord de Porto
    (-8.6100, 41.1000),  # Sud de Porto
]

print("=" * 60)
print("Test des transformations Porto → Casablanca")
print("=" * 60)

for lon_p, lat_p in test_points:
    lon_lin, lat_lin = transform_lon_linear(lon_p), transform_lat_linear(lat_p)
    lon_str, lat_str = transform_point_with_stretch(lon_p, lat_p)
    lon_complete, lat_complete = transform_point_complete(lon_p, lat_p)
    
    print(f"Porto ({lon_p:.4f}, {lat_p:.4f})")
    print(f"  → Linear    : ({lon_lin:.6f}, {lat_lin:.6f})")
    print(f"  → Stretch   : ({lon_str:.6f}, {lat_str:.6f})")
    print(f"  → Complete  : ({lon_complete:.6f}, {lat_complete:.6f})")
    print()

print("✅ Toutes les fonctions sont prêtes")
print("   transform_point_complete() = étirement + projection sur polygone")

# %%
# %% [markdown]
# CELLULE 2.5 — Création du mapping zone_id depuis le GeoJSON

# %%
BASE_PATH="/home/jovyan/work"
def create_zone_mapping_from_geojson(gdf):
    """
    Crée un mapping entre zone_id et les arrondissements du GeoJSON
    """
    zone_mapping = []
    
    for idx, row in gdf.iterrows():
        # Extraire les informations
        arr_name = row.get("Arrondissement", f"Arrondissement_{idx}")
        prefecture = row.get("Prefecture", "N/A")
        population = row.get("Population", 0)
        superficie = row.get("Superficie", 0)
        
        # Obtenir le bounding box de l'arrondissement
        bounds = row.geometry.bounds
        lon_min, lat_min, lon_max, lat_max = bounds
        
        # Déterminer le type de zone et le tarif
        zone_type = "residential"  # valeur par défaut
        commercial_keywords = ["Maarif", "Anfa", "Belyout", "Mers Sultan", "El Fida"]
        industrial_keywords = ["Ain Sebaa", "Hay Mohammadi", "Bernoussi"]
        
        for kw in commercial_keywords:
            if kw.lower() in arr_name.lower():
                zone_type = "commercial"
                break
        for kw in industrial_keywords:
            if kw.lower() in arr_name.lower():
                zone_type = "industrial"
                break
        
        # Tarif de base (MAD)
        expensive_zones = ["Anfa", "Maarif", "Belyout", "Mers Sultan", "Ain Diab"]
        base_fare = 8.0
        for zone in expensive_zones:
            if zone.lower() in arr_name.lower():
                base_fare = 10.0
                break
        if population > 300000:
            base_fare = 7.0
        
        # Centre de l'arrondissement
        centroid = row.geometry.centroid
        centroid_lon = centroid.x
        centroid_lat = centroid.y
        
        zone_mapping.append({
            "zone_id": idx + 1,
            "zone_name": arr_name,
            "prefecture": prefecture,
            "zone_type": zone_type,
            "population": population,
            "superficie_m2": superficie,
            "centroid_lon": centroid_lon,
            "centroid_lat": centroid_lat,
            "bbox_lon_min": lon_min,
            "bbox_lon_max": lon_max,
            "bbox_lat_min": lat_min,
            "bbox_lat_max": lat_max,
            "base_fare_mad": base_fare,
        })
    
    return zone_mapping

# Créer le mapping à partir du GeoJSON si disponible
if gdf_arrondissements is not None:
    zone_mapping_geo = create_zone_mapping_from_geojson(gdf_arrondissements)
    
    # Afficher le mapping créé
    print("✅ Mapping créé à partir du GeoJSON :")
    for zone in zone_mapping_geo[:5]:
        print(f"   {zone['zone_id']}: {zone['zone_name']} ({zone['zone_type']}) - {zone['base_fare_mad']} MAD")
    
    # Sauvegarder en CSV pour référence
    import pandas as pd
    pd.DataFrame(zone_mapping_geo).to_csv(f"{BASE_PATH}/data/zone_mapping_geojson.csv", index=False)
    print(f"✅ Sauvegardé dans {BASE_PATH}/data/zone_mapping_geojson.csv")
else:
    zone_mapping_geo = None
    print("⚠️ GeoJSON non disponible, utilisation du mapping manuel")

# %% [markdown]
# CELLULE 3 — UDF PySpark pour le POLYLINE

# %%
# %% [markdown]
# CELLULE 3 — UDF PySpark pour le POLYLINE (version GeoJSON)

# %%
from shapely.geometry import Point as ShapelyPoint

# Créer un index spatial à partir du GeoJSON pour la validation
if gdf_arrondissements is not None:
    polygon_list = []
    for idx, row in gdf_arrondissements.iterrows():
        polygon_list.append({
            "polygon": row.geometry,
            "zone_id": idx + 1,
            "zone_name": row.get("Arrondissement", f"Zone_{idx+1}"),
            "prefecture": row.get("Prefecture", ""),
            "base_fare": 8.0  # valeur par défaut, sera ajustée plus tard
        })
    print(f"✅ {len(polygon_list)} polygones chargés pour la validation")
else:
    polygon_list = []
    print("⚠️ Aucun polygone chargé")

def find_zone_for_point(lat, lon):
    """
    Trouve l'arrondissement contenant le point (lat, lon)
    Retourne (zone_id, zone_name, base_fare)
    """
    if not polygon_list:
        return (0, "inconnu", 7.0)
    
    point = ShapelyPoint(lon, lat)
    for zone in polygon_list:
        if zone["polygon"].contains(point):
            return (zone["zone_id"], zone["zone_name"], zone["base_fare"])
    return (0, "hors_zone", 7.0)

def remap_polyline(polyline_str):
    """Transforme tous les points GPS d'un trajet Porto → Casablanca"""
    if polyline_str is None or polyline_str == "[]":
        return None
    try:
        points = json.loads(polyline_str)
        if len(points) < 2:
            return None
        remapped = []
        for pt in points:
            lon_porto, lat_porto = pt[0], pt[1]
            lon_casa, lat_casa = transform_point_with_stretch(lon_porto, lat_porto)
            remapped.append([round(lon_casa, 6), round(lat_casa, 6)])
        return json.dumps(remapped)
    except Exception as e:
        return None

# Enregistrer comme UDF Spark
remap_udf = F.udf(remap_polyline, StringType())

# %% [markdown]
# CELLULE 4 — Table de mapping des zones

# %%
# %% [markdown]
# CELLULE 4 — Table de mapping des zones (avec fallback GeoJSON)

# %%
import os

BASE_PATH = "/home/jovyan/work" if os.path.exists("/home/jovyan/work") else "."

# ============================================
# ESSAYER D'ABORD D'UTILISER LE GEOJSON
# ============================================

if gdf_arrondissements is not None and zone_mapping_geo is not None:
    print("📊 Utilisation du mapping GeoJSON pour les zones")
    
    # Convertir le mapping GeoJSON en DataFrame Spark
    schema_mapping_geo = StructType([
        StructField("zone_id", IntegerType()),
        StructField("zone_name", StringType()),
        StructField("prefecture", StringType()),
        StructField("zone_type", StringType()),
        StructField("population", IntegerType()),
        StructField("superficie_m2", DoubleType()),
        StructField("centroid_lon", DoubleType()),
        StructField("centroid_lat", DoubleType()),
        StructField("bbox_lon_min", DoubleType()),
        StructField("bbox_lon_max", DoubleType()),
        StructField("bbox_lat_min", DoubleType()),
        StructField("bbox_lat_max", DoubleType()),
        StructField("base_fare_mad", DoubleType()),
    ])
    
    df_zones = spark.createDataFrame(zone_mapping_geo, schema=schema_mapping_geo)
    print("✅ Table de mapping créée depuis le GeoJSON")
    df_zones.show(20, truncate=False)
    
    # Sauvegarder CSV
    os.makedirs(f"{BASE_PATH}/data", exist_ok=True)
    df_zones.toPandas().to_csv(f"{BASE_PATH}/data/zone_mapping.csv", index=False)
    print(f"✅ zone_mapping.csv sauvegardé dans {BASE_PATH}/data/")

# ============================================
# SINON, UTILISER LE MAPPING MANUEL (fallback)
# ============================================

else:
    print("⚠️ GeoJSON non disponible, utilisation du mapping manuel (fallback)")
    
    zone_mapping_data = [
        # zones plus compactes, toutes dans la zone urbaine
        (1,  1,  "Ain Sebaa",           "industrial",   "medium", -7.5560, 33.6010, -7.5750, -7.5350, 33.5850, 33.6200, "2,5,12",    8.0),
        (2,  2,  "Al Fida",             "residential",  "high",   -7.6100, 33.5850, -7.6300, -7.5900, 33.5650, 33.6050, "1,3,7",     7.0),
        (3,  3,  "Anfa",                "commercial",   "medium", -7.6400, 33.5750, -7.6600, -7.6200, 33.5550, 33.5950, "2,8,10,16", 9.0),
        (4,  4,  "Ben M'Sick",          "residential",  "high",   -7.5850, 33.5550, -7.6050, -7.5650, 33.5350, 33.5750, "9,11,15",   7.0),
        (5,  5,  "Bernoussi",           "residential",  "high",   -7.5400, 33.6050, -7.5650, -7.5150, 33.5850, 33.6250, "1,12,14",   7.0),
        (6,  6,  "Bouskoura",           "residential",  "low",    -7.6350, 33.5300, -7.6600, -7.6100, 33.5200, 33.5500, "8,11",      10.0),
        (7,  7,  "El Fida-Mers Sultan", "commercial",   "high",   -7.5950, 33.5800, -7.6150, -7.5750, 33.5600, 33.6000, "2,9,13",    8.0),
        (8,  8,  "Hay Hassani",         "residential",  "high",   -7.6550, 33.5450, -7.6750, -7.6350, 33.5250, 33.5650, "3,6,10",    7.0),
        (9,  9,  "Hay Mohammadi",       "industrial",   "high",   -7.5650, 33.5700, -7.5850, -7.5450, 33.5500, 33.5900, "4,7,15",    7.0),
        (10, 10, "Maarif",              "commercial",   "medium", -7.6300, 33.5650, -7.6500, -7.6100, 33.5450, 33.5850, "3,8,16",    9.0),
        (11, 11, "Moulay Rachid",       "residential",  "high",   -7.5800, 33.5400, -7.6000, -7.5600, 33.5200, 33.5600, "4,6,15",    7.0),
        (12, 12, "Roches Noires",       "transit_hub",  "medium", -7.5700, 33.6000, -7.5900, -7.5500, 33.5800, 33.6200, "1,5,13",    8.0),
        (13, 13, "Sidi Belyout",        "commercial",   "high",   -7.6000, 33.5900, -7.6200, -7.5800, 33.5700, 33.6100, "7,12,16",   8.0),
        (14, 14, "Sidi Moumen",         "residential",  "high",   -7.5200, 33.5850, -7.5450, -7.4950, 33.5650, 33.6050, "5,15",      7.0),
        (15, 15, "Sidi Othmane",        "residential",  "high",   -7.5600, 33.5450, -7.5800, -7.5400, 33.5250, 33.5650, "4,9,11,14", 7.0),
        (16, 16, "Ain Diab",            "commercial",   "medium", -7.6650, 33.5900, -7.6850, -7.6450, 33.5700, 33.6100, "3,10,13",   10.0),
        # Porto 17-22
        (17, 1,  "Ain Sebaa",           "industrial",   "medium", -7.5560, 33.6010, -7.5750, -7.5350, 33.5850, 33.6200, "2,5,12",    8.0),
        (18, 5,  "Bernoussi",           "residential",  "high",   -7.5400, 33.6050, -7.5650, -7.5150, 33.5850, 33.6250, "1,12,14",   7.0),
        (19, 13, "Sidi Belyout",        "commercial",   "high",   -7.6000, 33.5900, -7.6200, -7.5800, 33.5700, 33.6100, "7,12,16",   8.0),
        (20, 3,  "Anfa",                "commercial",   "medium", -7.6400, 33.5750, -7.6600, -7.6200, 33.5550, 33.5950, "2,8,10,16", 9.0),
        (21, 10, "Maarif",              "commercial",   "medium", -7.6300, 33.5650, -7.6500, -7.6100, 33.5450, 33.5850, "3,8,16",    9.0),
        (22, 16, "Ain Diab",            "commercial",   "medium", -7.6650, 33.5900, -7.6850, -7.6450, 33.5700, 33.6100, "3,10,13",   10.0),
    ]

    schema_mapping = StructType([
        StructField("zone_porto",          IntegerType()),
        StructField("zone_id",             IntegerType()),
        StructField("zone_name",           StringType()),
        StructField("zone_type",           StringType()),
        StructField("population_density",  StringType()),
        StructField("centroid_lon",        DoubleType()),
        StructField("centroid_lat",        DoubleType()),
        StructField("bbox_lon_min",        DoubleType()),
        StructField("bbox_lon_max",        DoubleType()),
        StructField("bbox_lat_min",        DoubleType()),
        StructField("bbox_lat_max",        DoubleType()),
        StructField("adjacent_zones",      StringType()),
        StructField("base_fare_mad",       DoubleType()),
    ])

    df_zones = spark.createDataFrame(zone_mapping_data, schema=schema_mapping)
    print("✅ Table de mapping créée depuis le mapping manuel")
    df_zones.show(20, truncate=False)
    
    # Sauvegarder CSV
    os.makedirs(f"{BASE_PATH}/data", exist_ok=True)
    df_zones.toPandas().to_csv(f"{BASE_PATH}/data/zone_mapping.csv", index=False)
    print(f"✅ zone_mapping.csv sauvegardé dans {BASE_PATH}/data/")

print("\n📊 Résumé du zone_mapping généré :")
print(f"   Nombre de zones : {df_zones.count()}")

# %% [markdown]
# CELLULE 5 — Appliquer le remapping sur un échantillon

# %%
df = spark.read.csv("s3a://raw/porto-trips/train.csv", header=True, inferSchema=True)

# Filtrer les trajets valides
df_clean = df.filter(F.col("MISSING_DATA") == "False") \
             .filter(F.col("POLYLINE") != "[]") \
             .filter(F.col("POLYLINE").isNotNull())

# Sample
df_sample = df_clean.sample(0.02)

# Appliquer le remapping
df_remapped = df_sample.withColumn("POLYLINE_CASA", remap_udf(F.col("POLYLINE")))

# ── NOUVEAU : extraire TOUS les points du trajet ──────────────
def get_all_points(polyline_str):
    """Extrait tous les points GPS → liste de [lat, lon] pour folium"""
    if not polyline_str:
        return None
    try:
        pts = json.loads(polyline_str)
        if len(pts) < 2:
            return None
        return [[pt[1], pt[0]] for pt in pts]  # inverser lon,lat → lat,lon
    except:
        return None

get_all_points_udf = F.udf(get_all_points, ArrayType(ArrayType(DoubleType())))

# Construire le dataframe de visualisation
df_viz = df_remapped \
    .withColumn("route_casa", get_all_points_udf(F.col("POLYLINE_CASA"))) \
    .filter(F.col("route_casa").isNotNull()) \
    .select("TAXI_ID", "CALL_TYPE", "route_casa") \
    .limit(200)

points_pdf = df_viz.toPandas()
print(f"Trajets complets à visualiser : {len(points_pdf)}")
print(f"Exemple — nombre de points dans le 1er trajet : {len(points_pdf.iloc[0]['route_casa'])}")

# %% [markdown]
# CELLULE 6 — Carte Folium sur OpenStreetMap

# %%
# %% [markdown]
# CELLULE 6 — Carte Folium avec GeoJSON (polygones précis)

# %%
colors_call = {"A": "blue", "B": "green", "C": "red"}

m = folium.Map(
    location=[33.5731, -7.5898],
    zoom_start=12,
    tiles="OpenStreetMap"
)

# ============================================
# PARTIE 1 : Tracer les VRAIS arrondissements depuis GeoJSON
# ============================================
if gdf_arrondissements is not None:
    # Palette de couleurs par préfecture
    prefecture_colors = {
        "Anfa": "#FF6B6B",
        "Hay Hassani": "#4ECDC4", 
        "Sidi Bernoussi": "#45B7D1",
        "Moulay Rachid": "#96CEB4",
        "Ben M'sick": "#FFEAA7",
        "Ain Chock": "#DDA0DD",
        "Al Fida-Mers Sultan": "#98D8C8",
        "Ain Sebaa-Hay Mohammadi": "#F7DC6F",
        "Mechouar": "#BB8FCE"
    }
    
    for idx, row in gdf_arrondissements.iterrows():
        arr_name = row.get("Arrondissement", f"Arrondissement_{idx}")
        prefecture = row.get("Prefecture", "N/A")
        population = row.get("Population", 0)
        
        # Vérifier le type de géométrie
        if row.geometry.geom_type == "Polygon":
            coords = list(row.geometry.exterior.coords)
            folium_coords = [[lat, lon] for lon, lat in coords]
            
            color = prefecture_colors.get(prefecture, "#1D9E75")
            
            folium.Polygon(
                locations=folium_coords,
                color=color,
                weight=2,
                fill=True,
                fill_opacity=0.15,
                popup=f"""
                <b>{arr_name}</b><br>
                Préfecture: {prefecture}<br>
                Population: {population:,}<br>
                """,
                tooltip=f"{arr_name} - {prefecture}"
            ).add_to(m)
            
            # Ajouter un marqueur avec le nom
            centroid = row.geometry.centroid
            folium.Marker(
                location=[centroid.y, centroid.x],
                icon=folium.DivIcon(
                    html=f'<div style="font-size:10px;font-weight:bold;'
                         f'color:{color};background:white;padding:2px 4px;'
                         f'border-radius:3px;border:1px solid {color}">'
                         f'{arr_name[:18]}</div>'
                )
            ).add_to(m)
    
    print("✅ Polygones des arrondissements ajoutés à la carte")
    
else:
    # Fallback : utiliser les rectangles de l'ancienne méthode
    print("⚠️ GeoJSON non disponible, utilisation des rectangles")
    zones_pdf = df_zones.toPandas().drop_duplicates("zone_id")
    for _, row in zones_pdf.iterrows():
        folium.Rectangle(
            bounds=[[row["bbox_lat_min"], row["bbox_lon_min"]], 
                    [row["bbox_lat_max"], row["bbox_lon_max"]]],
            color="#1D9E75",
            fill=True,
            fill_opacity=0.08,
            weight=1,
            tooltip=f"Zone {row['zone_id']} — {row['zone_name']}"
        ).add_to(m)

# ============================================
# PARTIE 2 : Tracer les trajets (inchangé)
# ============================================
for _, row in points_pdf.iterrows():
    route = row["route_casa"]
    if route is not None and len(route) >= 2:
        call_type = str(row["CALL_TYPE"])
        color = colors_call.get(call_type, "gray")
        
        folium.PolyLine(
            locations=route,
            color=color,
            weight=1.5,
            opacity=0.6,
            tooltip=f"Taxi {row['TAXI_ID']} | Type {call_type}"
        ).add_to(m)
        
        folium.CircleMarker(
            location=route[0],
            radius=4,
            color=color,
            fill=True,
            fill_opacity=1.0
        ).add_to(m)
        
        folium.CircleMarker(
            location=route[-1],
            radius=4,
            color="black",
            fill=True,
            fill_opacity=0.8
        ).add_to(m)

# Légende mise à jour
legend_html = """
<div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
     background: white; padding: 12px; border: 1px solid #ccc;
     border-radius: 8px; font-size: 12px; min-width: 180px;">
  <b>TaaSim — Casablanca (GeoJSON)</b><br><br>
  <b>Type de course</b><br>
  <span style="color:blue">●</span> A — Dispatché<br>
  <span style="color:green">●</span> B — Station<br>
  <span style="color:red">●</span> C — Rue<br>
  <span style="color:black">●</span> Arrivée<br><br>
  <b>Arrondissements</b><br>
  <span style="color:#FF6B6B">■</span> Anfa<br>
  <span style="color:#4ECDC4">■</span> Hay Hassani<br>
  <span style="color:#45B7D1">■</span> Sidi Bernoussi<br>
  <span style="color:#96CEB4">■</span> Moulay Rachid
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

os.makedirs(f"{BASE_PATH}/notebooks", exist_ok=True)
m.save(f"{BASE_PATH}/notebooks/casablanca_geojson_trips.html")
print("✅ Carte sauvegardée avec les polygones des arrondissements")
m

# %% [markdown]
# CELLULE 7 — Validation du remapping

# %%
def validate_remapping(polyline_str):
    """
    Vérifie qu'au moins 70% des points sont DANS le polygone de Casablanca
    """
    if not polyline_str or polyline_str == "[]":
        return False
    try:
        pts = json.loads(polyline_str)
        if len(pts) < 2:
            return False
        
        if CASA_UNIFIED_POLYGON is None:
            return True
        
        n_valid = 0
        for pt in pts:
            lon, lat = pt[0], pt[1]
            point = ShapelyPoint(lon, lat)
            if CASA_UNIFIED_POLYGON.contains(point):
                n_valid += 1
        
        # Valide si au moins 70% des points sont dans le polygone
        return (n_valid / len(pts)) >= 0.70
    except:
        return False

# %% [markdown]
# CELLULE 8 — Validation finale 

# %%
print("=== ZONE_MAPPING.CSV — RÉSUMÉ ===")

if gdf_arrondissements is not None:
    zones_final = df_zones.toPandas()
    print(f"Source          : GeoJSON ({GEOJSON_PATH})")
    print(f"Nb arrondissements : {len(zones_final)}")
    print(f"Types de zones  : {zones_final['zone_type'].value_counts().to_dict()}")
    print(f"Tarifs min/max  : {zones_final['base_fare_mad'].min()} / {zones_final['base_fare_mad'].max()} MAD")
    print()
    
    # Vérifier que chaque arrondissement a reçu des trajets
    if 'zone_origin_casa' in points_pdf.columns:
        covered = points_pdf["zone_origin_casa"].nunique()
        print(f"Zones couvertes par les trajets : {covered} / {len(zones_final)}")
        uncovered = set(zones_final["zone_name"]) - set(points_pdf["zone_origin_casa"])
        if uncovered:
            print(f"⚠️ Zones sans trajet : {uncovered}")
        else:
            print("✅ Toutes les zones ont au moins un trajet")
else:
    print("⚠️ GeoJSON non chargé — utilisation du fallback manuel")

print()
print("Colonnes disponibles pour les jobs suivants :")
print("  ✓ POLYLINE_CASA      → vehicle_gps_producer.py")
print("  ✓ zone_origin_casa   → Flink Job 1 (assignment)")
print("  ✓ zone_dest_casa     → Flink Job 1 (assignment)")
print("  ✓ zone_mapping.csv   → Spark ETL Semaine 5")



