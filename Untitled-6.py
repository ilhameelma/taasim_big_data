# %%
!pip install -r /home/jovyan/requirements.txt -q

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

# %%
import json
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# %%
# Charge la vraie forme géographique de Casablanca
gdf_arr = gpd.read_file("/home/jovyan/work/data/Arrondissements.geojson")

# Polygone global (fusion de tous les arrondissements)
CASA_POLYGON = gdf_arr.unary_union

# Bounding box exacte de Casablanca
CASA_LON_MIN, CASA_LAT_MIN, CASA_LON_MAX, CASA_LAT_MAX = gdf_arr.total_bounds

print("BBOX Casablanca :", CASA_LON_MIN, CASA_LAT_MIN, CASA_LON_MAX, CASA_LAT_MAX)

# %%
PORTO_LON_MIN = -8.7350
PORTO_LON_MAX = -8.5300
PORTO_LAT_MIN = 41.0800
PORTO_LAT_MAX = 41.2200

# %%
def linear_map(value, src_min, src_max, dst_min, dst_max):
    ratio = (value - src_min) / (src_max - src_min)
    return dst_min + ratio * (dst_max - dst_min)

def porto_to_casa_linear(lon, lat):
    lon_c = linear_map(lon, PORTO_LON_MIN, PORTO_LON_MAX,
                       CASA_LON_MIN, CASA_LON_MAX)
    lat_c = linear_map(lat, PORTO_LAT_MIN, PORTO_LAT_MAX,
                       CASA_LAT_MIN, CASA_LAT_MAX)
    return lon_c, lat_c

# %%
def project_inside_casa_polygon(lon, lat):
    p = Point(lon, lat)
    
    # Si le point est déjà dans la ville → OK
    if CASA_POLYGON.contains(p):
        return lon, lat
    
    # Sinon → projection sur la frontière la plus proche
    boundary = CASA_POLYGON.boundary
    nearest_point = boundary.interpolate(boundary.project(p))
    return nearest_point.x, nearest_point.y

# %%
def porto_to_casa_complete(lon, lat):
    # 1) Porto → bbox Casablanca
    lon_c, lat_c = porto_to_casa_linear(lon, lat)

    # 2) Projection dans le polygone réel
    lon_c, lat_c = project_inside_casa_polygon(lon_c, lat_c)

    return lon_c, lat_c

# %%
def remap_polyline(polyline_str):
    try:
        pts = json.loads(polyline_str)
        remapped = []
        
        for lon, lat in pts:
            lon2, lat2 = porto_to_casa_complete(lon, lat)
            remapped.append([round(lon2, 6), round(lat2, 6)])
        
        return json.dumps(remapped)
    
    except Exception as e:
        return None

# %%
remap_udf = F.udf(remap_polyline, StringType())

# %%
df = spark.read.csv("s3a://raw/porto-trips/train.csv", header=True, inferSchema=True)
df_remapped = df.withColumn("remapped_polyline", remap_udf(F.col("polyline")))

# %%
df_remapped.select("polyline", "remapped_polyline").show(5, truncate=False)

# %%
import folium
import json

def plot_before_after(porto_polyline_str, casa_polyline_str,
                      porto_center=[41.1579, -8.6291],
                      casa_center=[33.5731, -7.5898]):
    """Affiche un trajet avant/après transformation sur une carte Folium."""
    
    # Convertir les polylines JSON → listes Python
    pts_porto = json.loads(porto_polyline_str)
    pts_casa  = json.loads(casa_polyline_str)

    # Folium Map avec double affichage
    m = folium.Map(location=[33.5731, -7.5898], zoom_start=12)

    # ----------- Trajet Porto (bleu) -----------
    folium.PolyLine(
        locations=[(lat, lon) for lon, lat in pts_porto],
        color="blue",
        weight=3,
        opacity=0.7,
        tooltip="Trajet original Porto"
    ).add_to(m)

    # ----------- Trajet Casablanca remappé (rouge) -----------
    folium.PolyLine(
        locations=[(lat, lon) for lon, lat in pts_casa],
        color="red",
        weight=4,
        opacity=0.9,
        tooltip="Trajet remappé Casablanca"
    ).add_to(m)

    # Ajouter les points de départ
    folium.Marker(
        location=[pts_porto[0][1], pts_porto[0][0]],
        icon=folium.Icon(color="blue"),
        popup="Départ Porto"
    ).add_to(m)

    folium.Marker(
        location=[pts_casa[0][1], pts_casa[0][0]],
        icon=folium.Icon(color="red"),
        popup="Départ Casablanca"
    ).add_to(m)

    return m

# %%
# On prend un trajet au hasard
row = df_remapped.select("polyline", "remapped_polyline").limit(5).collect()[0]

porto_poly = row["polyline"]
casa_poly  = row["remapped_polyline"]

m = plot_before_after(porto_poly, casa_poly)
m

# %%
# Sélection de 20 trajets au hasard
rows = df_remapped \
        .select("polyline", "remapped_polyline") \
        .sample(withReplacement=False, fraction=0.001) \
        .limit(5) \
        .collect()
maps = []
for i, row in enumerate(rows):
    porto_poly = row["polyline"]
    casa_poly  = row["remapped_polyline"]

    m = plot_before_after(porto_poly, casa_poly)
    maps.append(m)

# Afficher la première carte (les autres : maps[1], maps[2], etc.)
maps[0]

# %%
maps[1]

# %%
import folium

def plot_before_after_same_map(before_poly, after_poly):
    # Calcul du centre : on prend le centre du trajet avant
    lat_center = sum(p[0] for p in before_poly) / len(before_poly)
    lon_center = sum(p[1] for p in before_poly) / len(before_poly)

    m = folium.Map(location=[lat_center, lon_center], zoom_start=13)

    # Porto (avant)
    folium.PolyLine(
        before_poly,
        color="red",
        weight=4,
        opacity=0.7,
        tooltip="Porto (avant)"
    ).add_to(m)

    # Casablanca (après)
    folium.PolyLine(
        after_poly,
        color="blue",
        weight=4,
        opacity=0.7,
        tooltip="Casablanca (après)"
    ).add_to(m)

    # Points de départ et d'arrivée
    folium.Marker(before_poly[0], icon=folium.Icon(color="red"), tooltip="Start Porto").add_to(m)
    folium.Marker(after_poly[0], icon=folium.Icon(color="blue"), tooltip="Start Casa").add_to(m)

    return m

# %%
maps[2]

# %%
maps[3]

# %%
maps[4]

# %%


# %%
!pip install osmnx networkx

# %%
import osmnx as ox

# Télécharger le graphe routier réel
G = ox.graph_from_place("Casablanca, Morocco", network_type="drive")

print("Graph chargé")

# %%
def porto_to_casa_point(lon, lat):
    lon_c, lat_c = porto_to_casa_linear(lon, lat)
    return lat_c, lon_c  # attention ordre lat, lon

# %%
import networkx as nx

def compute_real_route(porto_polyline):
    pts = json.loads(porto_polyline)

    # début / fin du trajet Porto
    start_lon, start_lat = pts[0]
    end_lon, end_lat = pts[-1]

    # mapping vers Casablanca
    start_lat_c, start_lon_c = porto_to_casa_point(start_lon, start_lat)
    end_lat_c, end_lon_c = porto_to_casa_point(end_lon, end_lat)

    # trouver les noeuds les plus proches
    start_node = ox.distance.nearest_nodes(G, start_lon_c, start_lat_c)
    end_node = ox.distance.nearest_nodes(G, end_lon_c, end_lat_c)

    # calcul du chemin réel
    route = nx.shortest_path(G, start_node, end_node, weight="length")

    # convertir en coordonnées GPS
    route_coords = [(G.nodes[n]['y'], G.nodes[n]['x']) for n in route]

    return route_coords

# %%
def plot_real_route(route_coords):
    import folium

    m = folium.Map(location=route_coords[0], zoom_start=13)

    folium.PolyLine(route_coords, color="green", weight=5).add_to(m)

    return m

# %%
row = df.select("POLYLINE").limit(1).collect()[0]

route = compute_real_route(row["POLYLINE"])

m = plot_real_route(route)
m

# %%
pdf = df_remapped.select("remapped_polyline").toPandas()

# %%
def check_fast(polyline_str):
    pts = json.loads(polyline_str)
    if len(pts) < 3:
        return False

    test_pts = [pts[0], pts[len(pts)//2], pts[-1]]

    return all(CASA_POLYGON.contains(Point(lon, lat)) for lon, lat in test_pts)

pdf["inside"] = pdf["remapped_polyline"].apply(check_fast)
pdf["inside"].value_counts()

# %%
def check(polyline_str):
    pts = json.loads(polyline_str)
    return all(CASA_POLYGON.contains(Point(lon, lat)) for lon, lat in pts)

pdf["inside"] = pdf["remapped_polyline"].apply(check)

pdf["inside"].value_counts()


