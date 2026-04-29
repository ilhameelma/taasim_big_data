#!/usr/bin/env python3
"""
TaaSim - Vehicle GPS Producer
Lit les fichiers Parquet déjà remappés (Porto → Casablanca) et rejoue les polylines à travers Kafka
AVEC ROAD SNAPPING OSMnx (optionnel)
"""

import json
import time
import random
import argparse
import logging
import threading
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import io
import requests

import boto3
from botocore.client import Config
import pyarrow.parquet as pq
import pyarrow as pa

from shapely.geometry import Point, Polygon

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_to_native_types(obj):
    """Convertit les types numpy en types Python natifs pour JSON."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_native_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native_types(i) for i in obj]
    return obj


# ---------------------------------------------------------------------------
# Module Road Snapping (OSMnx)
# ---------------------------------------------------------------------------

class RoadSnapEngine:
    """
    Charge le graphe routier de Casablanca via OSMnx et snape chaque point
    GPS sur l'arête la plus proche du réseau drive.
    """

    def __init__(
        self,
        bbox: Tuple[float, float, float, float],
        cache_path: Optional[str] = None,
        network_type: str = "drive",
    ):
        self._ready = False
        self._graph = None
        self._nodes = None
        self._edges = None
        self.bbox = bbox
        self.cache_path = Path(cache_path) if cache_path else None
        self.network_type = network_type

        self.snap_stats = {
            "total": 0,
            "snapped": 0,
            "fallback": 0,
        }

        self._load_graph()

    def _load_graph(self):
        """Charge le graphe depuis le cache ou le télécharge via OSMnx."""
        try:
            import osmnx as ox
            import networkx as nx
        except ImportError:
            logger.error(
                "❌ OSMnx/NetworkX non installés. "
                "Installez-les: pip install osmnx networkx"
            )
            return

        lon_min, lon_max, lat_min, lat_max = self.bbox

        if self.cache_path and self.cache_path.exists():
            logger.info(f"📂 Chargement du graphe depuis le cache: {self.cache_path}")
            try:
                self._graph = ox.load_graphml(str(self.cache_path))
                logger.info("✅ Graphe chargé depuis le cache")
                self._post_load(ox)
                return
            except Exception as e:
                logger.warning(f"⚠️ Cache invalide ({e}), re-téléchargement…")

        logger.info(
            f"🌍 Téléchargement du réseau routier Casablanca "
            f"[{lat_min:.4f},{lat_max:.4f},{lon_min:.4f},{lon_max:.4f}]…"
        )
        try:
            self._graph = ox.graph_from_bbox(
                bbox=(lat_max, lat_min, lon_max, lon_min),
                network_type=self.network_type,
                simplify=True,
            )
            logger.info(
                f"✅ Graphe téléchargé: "
                f"{len(self._graph.nodes)} nœuds, {len(self._graph.edges)} arêtes"
            )

            if self.cache_path:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                ox.save_graphml(self._graph, str(self.cache_path))
                logger.info(f"💾 Graphe sauvegardé: {self.cache_path}")

            self._post_load(ox)

        except Exception as e:
            logger.error(f"❌ Impossible de charger le graphe OSMnx: {e}")

    def _post_load(self, ox):
        import geopandas as gpd
        self._nodes, self._edges = ox.graph_to_gdfs(self._graph)
        self._ready = True
        logger.info("✅ RoadSnapEngine prêt")

    def snap(self, lon: float, lat: float) -> Tuple[float, float, bool]:
        """
        Snape un point GPS sur le réseau routier.
        Returns (lon_snapped, lat_snapped, was_snapped)
        """
        self.snap_stats["total"] += 1

        if not self._ready:
            self.snap_stats["fallback"] += 1
            return lon, lat, False

        try:
            import osmnx as ox
            from shapely.geometry import LineString

            nearest_edge = ox.nearest_edges(self._graph, lon, lat)
            u, v, key = nearest_edge

            edge_data = self._graph.edges[u, v, key]

            if "geometry" in edge_data:
                line = edge_data["geometry"]
            else:
                u_data = self._graph.nodes[u]
                v_data = self._graph.nodes[v]
                line = LineString([
                    (u_data["x"], u_data["y"]),
                    (v_data["x"], v_data["y"]),
                ])

            point = Point(lon, lat)
            projected = line.interpolate(line.project(point))

            self.snap_stats["snapped"] += 1
            return round(projected.x, 6), round(projected.y, 6), True

        except Exception as e:
            logger.debug(f"⚠️ Snap échoué ({lon:.5f},{lat:.5f}): {e}")
            self.snap_stats["fallback"] += 1
            return lon, lat, False

    def print_stats(self):
        total = self.snap_stats["total"]
        if total == 0:
            logger.info("RoadSnapEngine: aucun point traité.")
            return
        snapped = self.snap_stats["snapped"]
        fallback = self.snap_stats["fallback"]
        logger.info("=" * 60)
        logger.info("🛣️  STATISTIQUES ROAD SNAPPING")
        logger.info("=" * 60)
        logger.info(f"   Total points             : {total}")
        logger.info(f"   Points snappés           : {snapped}  ({snapped/total*100:.1f}%)")
        logger.info(f"   Fallback (non snappés)   : {fallback} ({fallback/total*100:.1f}%)")
        logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Producer principal
# ---------------------------------------------------------------------------

class VehicleGPSProducer:
    """
    Producer GPS qui lit les fichiers Parquet déjà remappés
    (trajets Porto → Casablanca transformés)
    et simule des véhicules en mouvement.
    """

    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        speed_factor: float = 10.0,
        minio_endpoint: str = "minio:9000",
        minio_access_key: str = "taasim",
        minio_secret_key: str = "taasim123",
        curated_bucket: str = "curated",
        curated_prefix: str = "porto-trips",
        geojson_path: str = "/home/jovyan/work/data/Arrondissements.geojson",
        # --- Paramètres Road Snapping ---
        snap_sample_size: int = 500,
        snap_graph_cache: str = "/tmp/casablanca_drive.graphml",
        enable_road_snap: bool = True,
        # --- Limite mémoire ---
        max_trips: int = 5000,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.speed_factor = speed_factor
        self.max_trips = max_trips

        # --- Road Snapping ---
        self.snap_sample_size = snap_sample_size
        self.snap_graph_cache = snap_graph_cache
        self.enable_road_snap = enable_road_snap
        self.snapped_trip_ids: set = set()
        self.road_snap_engine: Optional[RoadSnapEngine] = None

        # Configuration MinIO
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.curated_bucket = curated_bucket
        self.curated_prefix = curated_prefix

        # Chargement des données (Parquet déjà remappé)
        self.trips_df = None
        self.zone_mapping = None
        self.zones_list = []
        
        # Chargement des trajets depuis Parquet
        self.load_trips_from_parquet()
        
        # Chargement du mapping des zones
        self.load_zone_mapping()

        # GeoJSON Casablanca
        self.casa_polygon = None
        self.load_geojson(geojson_path)
        
        # Bbox Casablanca (depuis les données chargées)
        self.casa_lon_min = None
        self.casa_lon_max = None
        self.casa_lat_min = None
        self.casa_lat_max = None
        self.compute_casa_bbox()

        # Road Snapping — sélectionner l'échantillon et charger le graphe
        if self.enable_road_snap:
            self._init_road_snap()

        # Kafka
        self.producer = None
        self.init_kafka()

        # Gestion des véhicules
        self.active_vehicles: Dict = {}
        self.vehicle_threads: Dict = {}

        # Stats globales
        self.total_events = 0
        self.blackout_events = 0
        
        # Stats validation
        self.validation_stats = {
            "total_points": 0,
            "invalid_points": 0,
            "projected_points": 0,
        }

    # -----------------------------------------------------------------------
    # Chargement des trajets depuis Parquet (DÉJÀ REMAPPÉS)
    # -----------------------------------------------------------------------

    def load_trips_from_parquet(self):
        """
        Lit les fichiers Parquet depuis S3/MinIO.
        Ces fichiers contiennent déjà les trajets remappés (Porto → Casablanca)
        avec les colonnes: trip_id, taxi_id, snapped_polyline, origin_zone_id, ...
        """
        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=f"http://{self.minio_endpoint}",
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                config=Config(signature_version="s3v4"),
                region_name="us-east-1",
            )
            
            # Lister les fichiers Parquet dans le bucket curated/porto-trips/
            paginator = s3_client.get_paginator('list_objects_v2')
            parquet_files = []
            
            for page in paginator.paginate(Bucket=self.curated_bucket, Prefix=self.curated_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.parquet'):
                            parquet_files.append(obj['Key'])
            
            if not parquet_files:
                raise Exception(f"Aucun fichier .parquet trouvé dans s3://{self.curated_bucket}/{self.curated_prefix}/")
            
            logger.info(f"📁 Trouvé {len(parquet_files)} fichiers Parquet")
            
            # Lire et concaténer les DataFrames
            dfs = []
            total_rows = 0
            
            for file_key in parquet_files[:10]:  # Limiter pour éviter OOM
                logger.info(f"   Lecture: {file_key}")
                response = s3_client.get_object(Bucket=self.curated_bucket, Key=file_key)
                
                # Lire le Parquet avec pyarrow
                table = pq.read_table(io.BytesIO(response['Body'].read()))
                df = table.to_pandas()
                dfs.append(df)
                total_rows += len(df)
                
                if total_rows >= self.max_trips:
                    break
            
            if not dfs:
                raise Exception("Aucune donnée lue depuis les fichiers Parquet")
            
            self.trips_df = pd.concat(dfs, ignore_index=True)
            
            # Limiter au nombre max de trajets
            if len(self.trips_df) > self.max_trips:
                self.trips_df = self.trips_df.head(self.max_trips)
            
            logger.info(f"✅ Trajets chargés: {len(self.trips_df)} trajets (limite: {self.max_trips})")
            logger.info(f"   Colonnes disponibles: {list(self.trips_df.columns)}")
            
            # Vérifier la colonne de polyline
            polyline_col = None
            for col in ['snapped_polyline', 'polyline', 'remapped_polyline']:
                if col in self.trips_df.columns:
                    polyline_col = col
                    break
            
            if polyline_col is None:
                raise Exception(f"Aucune colonne de polyline trouvée dans les colonnes: {list(self.trips_df.columns)}")
            
            logger.info(f"   Utilisation de la colonne: '{polyline_col}' pour les polylines")
            
            # Renommer pour uniformiser
            if polyline_col != 'POLYLINE':
                self.trips_df['POLYLINE'] = self.trips_df[polyline_col]
                
            # S'assurer que TAXI_ID existe
            if 'taxi_id' in self.trips_df.columns and 'TAXI_ID' not in self.trips_df.columns:
                self.trips_df['TAXI_ID'] = self.trips_df['taxi_id']
            
        except Exception as e:
            logger.error(f"❌ Erreur chargement Parquet: {e}")
            raise

    def compute_casa_bbox(self):
        """Calcule la bbox de Casablanca depuis les polylines chargées."""
        if self.trips_df is None or len(self.trips_df) == 0:
            # Valeurs par défaut
            self.casa_lon_min = -7.7000
            self.casa_lon_max = -7.4800
            self.casa_lat_min = 33.4800
            self.casa_lat_max = 33.6800
            logger.warning("⚠️ Utilisation des valeurs par défaut pour la bbox Casablanca")
            return
        
        lon_min, lon_max, lat_min, lat_max = float('inf'), float('-inf'), float('inf'), float('-inf')
        
        for _, row in self.trips_df.iterrows():
            try:
                polyline = json.loads(row['POLYLINE'])
                for lon, lat in polyline:
                    lon_min = min(lon_min, lon)
                    lon_max = max(lon_max, lon)
                    lat_min = min(lat_min, lat)
                    lat_max = max(lat_max, lat)
            except:
                continue
        
        if lon_min != float('inf'):
            self.casa_lon_min = lon_min
            self.casa_lon_max = lon_max
            self.casa_lat_min = lat_min
            self.casa_lat_max = lat_max
        
        logger.info(f"📍 Bbox Casablanca calculée: "
                   f"lon [{self.casa_lon_min:.4f}, {self.casa_lon_max:.4f}], "
                   f"lat [{self.casa_lat_min:.4f}, {self.casa_lat_max:.4f}]")

    def load_zone_mapping(self):
        """Charge le mapping des zones depuis MinIO ou fichier local."""
        try:
            url = f"http://{self.minio_endpoint}/curated/zone_mapping_geojson.csv"
            response = requests.get(
                url, auth=(self.minio_access_key, self.minio_secret_key)
            )
            if response.status_code == 200:
                self.zone_mapping = pd.read_csv(io.BytesIO(response.content))
                logger.info(f"✅ Zone mapping chargé depuis MinIO: {len(self.zone_mapping)} zones")
            else:
                raise Exception(f"HTTP {response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de charger depuis MinIO: {e}")
            zone_path = Path(__file__).parent.parent / "data" / "zone_mapping_geojson.csv"
            if zone_path.exists():
                self.zone_mapping = pd.read_csv(zone_path)
                logger.info(f"✅ Zone mapping chargé localement: {len(self.zone_mapping)} zones")
            else:
                logger.error("❌ Aucun fichier zone_mapping_geojson.csv trouvé")
                self.zone_mapping = None
                return

        required_cols = [
            "zone_id", "prefecture", "zone_type", "base_fare_mad",
            "bbox_lon_min", "bbox_lon_max", "bbox_lat_min", "bbox_lat_max",
        ]
        missing = [c for c in required_cols if c not in self.zone_mapping.columns]
        if missing:
            logger.error(f"❌ Colonnes manquantes: {missing}")
            self.zone_mapping = None
            return

        self.zones_list = []
        for _, row in self.zone_mapping.iterrows():
            try:
                self.zones_list.append({
                    "zone_id": int(row["zone_id"]),
                    "zone_name": str(row["prefecture"]),
                    "zone_type": str(row.get("zone_type", "mixed")),
                    "base_fare_mad": float(row.get("base_fare_mad", 8.0)),
                    "bbox": (
                        float(row["bbox_lon_min"]),
                        float(row["bbox_lon_max"]),
                        float(row["bbox_lat_min"]),
                        float(row["bbox_lat_max"]),
                    ),
                })
            except Exception as ex:
                logger.warning(f"⚠️ Erreur ligne {_}: {ex}")

        logger.info(f"✅ {len(self.zones_list)} zones préparées")

    def load_geojson(self, geojson_path: str):
        try:
            import geopandas as gpd
            gdf = gpd.read_file(geojson_path)
            self.casa_polygon = gdf.union_all()
            logger.info("✅ GeoJSON chargé pour validation des points")
        except Exception as e:
            logger.warning(f"⚠️ Impossible de charger le GeoJSON: {e}")
            self.casa_polygon = None

    # -----------------------------------------------------------------------
    # Road Snapping — initialisation
    # -----------------------------------------------------------------------

    def _init_road_snap(self):
        """Sélectionne un échantillon de trajets pour le road snapping."""
        if self.trips_df is None or len(self.trips_df) == 0:
            logger.warning("⚠️ Pas de données chargées — road snapping désactivé.")
            self.enable_road_snap = False
            return

        n_available = len(self.trips_df)
        n_snap = min(self.snap_sample_size, n_available)

        # Sélection aléatoire reproductible
        sampled_indices = self.trips_df.sample(n=n_snap, random_state=42).index
        self.snapped_trip_ids = set(
            self.trips_df.loc[sampled_indices, "TAXI_ID"].astype(int).tolist()
        )

        logger.info("=" * 60)
        logger.info(f"🛣️  ROAD SNAPPING activé sur {n_snap}/{n_available} trajets")
        logger.info(f"   Cache graphe : {self.snap_graph_cache}")
        logger.info("=" * 60)

        self.road_snap_engine = RoadSnapEngine(
            bbox=(
                self.casa_lon_min,
                self.casa_lon_max,
                self.casa_lat_min,
                self.casa_lat_max,
            ),
            cache_path=self.snap_graph_cache,
            network_type="drive",
        )

    def should_snap(self, taxi_id: int) -> bool:
        return (
            self.enable_road_snap
            and self.road_snap_engine is not None
            and self.road_snap_engine._ready
            and taxi_id in self.snapped_trip_ids
        )

    # -----------------------------------------------------------------------
    # Transformation (minimale car déjà remappé)
    # -----------------------------------------------------------------------

    def parse_polyline(self, polyline_str: str) -> List[Tuple[float, float]]:
        """Parse une polyline JSON en liste de points (lon, lat)."""
        try:
            points = json.loads(polyline_str)
            return [(float(p[0]), float(p[1])) for p in points]
        except Exception:
            return []

    def is_point_in_casablanca(self, lon: float, lat: float) -> bool:
        in_bbox = (
            self.casa_lon_min <= lon <= self.casa_lon_max
            and self.casa_lat_min <= lat <= self.casa_lat_max
        )
        if not in_bbox:
            return False
        if self.casa_polygon is not None:
            try:
                return self.casa_polygon.covers(Point(lon, lat))
            except Exception:
                return in_bbox
        return in_bbox

    def project_inside_casa_polygon(self, lon: float, lat: float) -> Tuple[float, float, bool]:
        if self.casa_polygon is None:
            return lon, lat, False
        p = Point(lon, lat)
        if self.casa_polygon.contains(p):
            return lon, lat, False
        boundary = self.casa_polygon.boundary
        nearest = boundary.interpolate(boundary.project(p))
        return nearest.x, nearest.y, True

    def add_gps_noise(self, lon: float, lat: float, sigma: float = 0.0002) -> Tuple[float, float]:
        return (
            round(lon + np.random.normal(0, sigma), 6),
            round(lat + np.random.normal(0, sigma), 6),
        )

    def apply_noise_and_project(self, lon: float, lat: float) -> Tuple[float, float]:
        lon_noisy, lat_noisy = self.add_gps_noise(lon, lat)
        lon_final, lat_final, was_projected = self.project_inside_casa_polygon(
            lon_noisy, lat_noisy
        )

        self.validation_stats["total_points"] += 1

        if was_projected:
            self.validation_stats["projected_points"] += 1
        elif not self.is_point_in_casablanca(lon_final, lat_final):
            self.validation_stats["invalid_points"] += 1

        return round(lon_final, 6), round(lat_final, 6)

    # -----------------------------------------------------------------------
    # Zones
    # -----------------------------------------------------------------------

    def get_zone_info(self, lon: float, lat: float) -> dict:
        if not self.zones_list:
            return {
                "zone_id": 1,
                "zone_name": "Unknown",
                "zone_type": "mixed",
                "base_fare_mad": 8.0,
            }

        for zone in self.zones_list:
            lon_min, lon_max, lat_min, lat_max = zone["bbox"]
            if lon_min <= lon <= lon_max and lat_min <= lat <= lat_max:
                return {
                    "zone_id": zone["zone_id"],
                    "zone_name": zone["zone_name"],
                    "zone_type": zone["zone_type"],
                    "base_fare_mad": zone["base_fare_mad"],
                }

        # Fallback: zone la plus proche
        best_zone = self.zones_list[0]
        min_dist = float("inf")
        for zone in self.zones_list:
            center_lon = (zone["bbox"][0] + zone["bbox"][1]) / 2
            center_lat = (zone["bbox"][2] + zone["bbox"][3]) / 2
            dist = ((lon - center_lon) ** 2 + (lat - center_lat) ** 2) ** 0.5
            if dist < min_dist:
                min_dist = dist
                best_zone = zone

        return {
            "zone_id": best_zone["zone_id"],
            "zone_name": best_zone["zone_name"],
            "zone_type": best_zone["zone_type"],
            "base_fare_mad": best_zone["base_fare_mad"],
        }

    # -----------------------------------------------------------------------
    # Kafka
    # -----------------------------------------------------------------------

    def init_kafka(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(
                    convert_to_native_types(v)
                ).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info(f"✅ Connecté à Kafka: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Erreur Kafka: {e}")
            self.producer = None

    # -----------------------------------------------------------------------
    # Simulation
    # -----------------------------------------------------------------------

    def should_blackout(self) -> bool:
        return random.random() < 0.05

    def get_blackout_duration(self) -> int:
        return random.randint(60, 180)

    def send_initial_available_vehicles(self, count: int):
        logger.info(f"🚕 Envoi de {count} taxis disponibles…")

        default_lat = 33.5731
        default_lon = -7.5898
        zone_info = self.get_zone_info(default_lon, default_lat)

        for i in range(min(count, len(self.trips_df))):
            trip = self.trips_df.iloc[i]
            taxi_id = int(trip["TAXI_ID"])

            event_time = datetime.now()
            event = {
                "taxi_id": taxi_id,
                "timestamp": event_time.isoformat(),
                "timestamp_unix": int(event_time.timestamp()),
                "lat": default_lat,
                "lon": default_lon,
                "zone_id": zone_info["zone_id"],
                "zone_name": zone_info["zone_name"],
                "zone_type": zone_info["zone_type"],
                "prefecture": zone_info["zone_name"],
                "base_fare_mad": zone_info["base_fare_mad"],
                "speed": 0.0,
                "status": "available",
                "trip_progress": 0.0,
                "road_snapped": False,
            }

            try:
                self.producer.send("raw.gps", key=taxi_id, value=event)
                self.total_events += 1
            except Exception as e:
                logger.error(f"Erreur: {e}")

            if (i + 1) % 50 == 0:
                logger.info(f"  {i+1} taxis disponibles envoyés")

            time.sleep(0.05)

        logger.info(f"✅ {count} taxis disponibles initialisés")

    def simulate_vehicle_trip(self, trip_row: pd.Series):
        """
        Simule un véhicule sur un trajet DÉJÀ REMAPPÉ.
        La polyline est déjà dans l'espace de Casablanca.
        """
        taxi_id = int(trip_row["TAXI_ID"])
        polyline = self.parse_polyline(trip_row["POLYLINE"])
        use_snap = self.should_snap(taxi_id)

        if not polyline:
            logger.warning(f"Polyline vide pour taxi {taxi_id}")
            return

        snap_label = "🛣️  [SNAPPED]" if use_snap else "🚖"
        logger.info(
            f"{snap_label} Véhicule {taxi_id} démarre trajet "
            f"({len(polyline)} points, snap={'oui' if use_snap else 'non'})"
        )

        for i, (lon, lat) in enumerate(polyline):
            if not self.active_vehicles.get(taxi_id, {}).get("active", True):
                break

            # Blackout GPS
            if self.should_blackout():
                blackout_duration = self.get_blackout_duration()
                self.blackout_events += 1
                logger.info(
                    f"⚠️ Taxi {taxi_id}: Blackout GPS de {blackout_duration}s simulés"
                )
                time.sleep(blackout_duration / self.speed_factor)
                continue

            # 1. Bruit + projection dans le polygone
            lon_noisy, lat_noisy = self.apply_noise_and_project(lon, lat)

            # 2. Road Snapping (optionnel)
            road_snapped = False
            if use_snap and self.road_snap_engine is not None:
                lon_final, lat_final, road_snapped = self.road_snap_engine.snap(
                    lon_noisy, lat_noisy
                )
            else:
                lon_final, lat_final = lon_noisy, lat_noisy

            # 3. Zone
            zone_info = self.get_zone_info(lon_final, lat_final)

            if i == 0:
                logger.info(
                    f"   📍 Départ: Zone {zone_info['zone_id']} - "
                    f"{zone_info['zone_name']} ({zone_info['zone_type']})"
                )
            elif i == len(polyline) - 1:
                logger.info(
                    f"   🏁 Arrivée: Zone {zone_info['zone_id']} - "
                    f"{zone_info['zone_name']} ({zone_info['zone_type']})"
                )

            event_time = datetime.now()
            event = {
                "taxi_id": taxi_id,
                "timestamp": event_time.isoformat(),
                "timestamp_unix": int(event_time.timestamp()),
                "lat": float(lat_final),
                "lon": float(lon_final),
                "zone_id": int(zone_info["zone_id"]),
                "zone_name": zone_info["zone_name"],
                "zone_type": zone_info["zone_type"],
                "prefecture": zone_info["zone_name"],
                "base_fare_mad": float(zone_info["base_fare_mad"]),
                "speed": float(random.uniform(20, 60)),
                "status": "moving",
                "trip_progress": float(i / max(len(polyline), 1)),
                "road_snapped": road_snapped,
                "snap_applied": use_snap,
            }

            try:
                self.producer.send("raw.gps", key=taxi_id, value=event)
                self.total_events += 1

                if self.total_events % 100 == 0:
                    logger.info(
                        f"📊 Événements GPS: {self.total_events} | "
                        f"Blackouts: {self.blackout_events}"
                    )
            except Exception as e:
                logger.error(f"Erreur envoi taxi {taxi_id}: {e}")

            time.sleep(15.0 / self.speed_factor)

        logger.info(f"🏁 Véhicule {taxi_id} termine son trajet")

        end_event = {
            "taxi_id": taxi_id,
            "timestamp": datetime.now().isoformat(),
            "timestamp_unix": int(datetime.now().timestamp()),
            "status": "available",
            "trip_progress": 1.0,
            "road_snapped": False,
            "snap_applied": use_snap,
        }

        try:
            self.producer.send("raw.gps", key=taxi_id, value=end_event)
        except Exception as e:
            logger.error(f"Erreur fin trajet taxi {taxi_id}: {e}")

    def run_vehicle(self, taxi_id: int, trip_row: pd.Series):
        self.active_vehicles[taxi_id] = {"active": True}
        thread = threading.Thread(
            target=self.simulate_vehicle_trip,
            args=(trip_row,),
            daemon=True,
            name=f"Vehicle-{taxi_id}",
        )
        self.vehicle_threads[taxi_id] = thread
        thread.start()
        logger.info(f"✅ Véhicule {taxi_id} démarré")

    def run(self, duration_seconds: int = None, max_vehicles: int = 50):
        if self.producer is None:
            logger.error("Producer Kafka non initialisé — abandon")
            return

        logger.info("=" * 60)
        logger.info("🚕 TaaSim - Vehicle GPS Producer")
        logger.info(f"📊 Speed factor       : {self.speed_factor}x")
        logger.info(f"🚗 Max vehicles        : {max_vehicles}")
        logger.info(f"📊 Trajets disponibles : {len(self.trips_df)}")
        logger.info(
            f"🛣️  Road snap activé   : {'oui' if self.enable_road_snap else 'non'}"
        )
        if self.enable_road_snap:
            logger.info(f"   Échantillon snap   : {len(self.snapped_trip_ids)} trajets")
        logger.info("=" * 60)

        self.send_initial_available_vehicles(max_vehicles)
        time.sleep(1)

        start_time = datetime.now()
        end_time = (
            start_time + timedelta(seconds=duration_seconds)
            if duration_seconds
            else None
        )

        vehicle_counter = 0
        active_vehicle_count = 0

        try:
            while True:
                if end_time and datetime.now() >= end_time:
                    break

                if (
                    vehicle_counter < len(self.trips_df)
                    and active_vehicle_count < max_vehicles
                ):
                    trip = self.trips_df.iloc[vehicle_counter]
                    taxi_id = int(trip["TAXI_ID"])

                    if taxi_id not in self.vehicle_threads:
                        self.run_vehicle(taxi_id, trip)
                        active_vehicle_count += 1

                    vehicle_counter += 1
                    time.sleep(2.0 / self.speed_factor)

                # Nettoyer les threads terminés
                completed = [
                    tid for tid, t in self.vehicle_threads.items()
                    if not t.is_alive()
                ]
                for tid in completed:
                    del self.vehicle_threads[tid]
                    self.active_vehicles.pop(tid, None)
                    active_vehicle_count = max(0, active_vehicle_count - 1)

                time.sleep(1.0)

        except KeyboardInterrupt:
            logger.info("\n🛑 Interruption utilisateur")
        finally:
            self.cleanup()

    # -----------------------------------------------------------------------
    # Stats & nettoyage
    # -----------------------------------------------------------------------

    def print_validation_stats(self):
        total = self.validation_stats["total_points"]
        if total == 0:
            logger.info("Aucun point GPS traité — pas de stats.")
            return

        invalid = self.validation_stats["invalid_points"]
        projected = self.validation_stats["projected_points"]

        logger.info("=" * 60)
        logger.info("📊 STATISTIQUES DE VALIDATION GPS")
        logger.info("=" * 60)
        logger.info(f"   Total points transformés   : {total}")
        logger.info(f"   Points invalides (hors bbox): {invalid}   ({invalid/total*100:.1f}%)")
        logger.info(f"   Points projetés (corrigés) : {projected}  ({projected/total*100:.1f}%)")

        if total > 0 and invalid / total > 0.05:
            logger.warning(
                f"⚠️ ATTENTION: {invalid/total*100:.1f}% des points sont hors de Casablanca!"
            )
        else:
            logger.info(f"✅ VALIDATION OK: {(total-invalid)/total*100:.1f}% des points sont dans Casablanca.")
        logger.info("=" * 60)

    def cleanup(self):
        logger.info("Nettoyage en cours…")

        self.print_validation_stats()

        if self.road_snap_engine:
            self.road_snap_engine.print_stats()

        for taxi_id in list(self.active_vehicles.keys()):
            self.active_vehicles[taxi_id]["active"] = False

        time.sleep(2)

        if self.producer:
            self.producer.flush()
            self.producer.close()

        logger.info(
            f"📊 Final: {self.total_events} events envoyés, "
            f"{self.blackout_events} blackouts"
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="TaaSim Vehicle GPS Producer (lit les trajets déjà remappés depuis Parquet)"
    )
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--speed-factor", type=float, default=10.0)
    parser.add_argument("--duration", type=int, default=None,
                        help="Durée de simulation en secondes (None = infini)")
    parser.add_argument("--max-vehicles", type=int, default=10)
    parser.add_argument("--minio-endpoint", default="minio:9000")
    parser.add_argument("--minio-access-key", default="taasim")
    parser.add_argument("--minio-secret-key", default="taasim123")
    parser.add_argument("--curated-bucket", default="curated",
                        help="Bucket contenant les trajets remappés")
    parser.add_argument("--curated-prefix", default="porto-trips",
                        help="Préfixe du dossier Parquet")
    parser.add_argument("--geojson-path",
                        default="/home/jovyan/work/data/Arrondissements.geojson")
    # Road Snapping
    parser.add_argument("--snap-sample-size", type=int, default=500,
                        help="Nombre de trajets à snapper sur le réseau routier")
    parser.add_argument("--snap-graph-cache", default="/tmp/casablanca_drive.graphml",
                        help="Chemin du cache GraphML OSMnx")
    parser.add_argument("--no-road-snap", action="store_true",
                        help="Désactive complètement le road snapping")
    parser.add_argument("--max-trips", type=int, default=5000,
                        help="Nombre max de trajets à charger depuis Parquet")

    args = parser.parse_args()

    producer = VehicleGPSProducer(
        bootstrap_servers=args.bootstrap_servers,
        speed_factor=args.speed_factor,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        curated_bucket=args.curated_bucket,
        curated_prefix=args.curated_prefix,
        geojson_path=args.geojson_path,
        snap_sample_size=args.snap_sample_size,
        snap_graph_cache=args.snap_graph_cache,
        enable_road_snap=not args.no_road_snap,
        max_trips=args.max_trips,
    )

    producer.run(duration_seconds=args.duration, max_vehicles=args.max_vehicles)


if __name__ == "__main__":
    main()