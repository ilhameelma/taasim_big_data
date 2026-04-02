#!/usr/bin/env python3
"""
TaaSim - Vehicle GPS Producer
Rejoue les polylines GPS du dataset Porto à travers Kafka
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_to_native_types(obj):
    """Convertit les types numpy en types Python natifs pour JSON"""
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


class VehicleGPSProducer:
    """Producer GPS qui simule des véhicules en mouvement"""
    
    def __init__(self, bootstrap_servers='kafka:9092', speed_factor=10.0,
                 minio_endpoint='minio:9000', minio_access_key='taasim', minio_secret_key='taasim123'):
        self.bootstrap_servers = bootstrap_servers
        self.speed_factor = speed_factor
        
        # Configuration MinIO
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = "raw"
        self.minio_object = "porto-trips/train.csv"
        
        # Charger les données depuis MinIO
        self.trips_df = None
        self.zone_mapping = None
        self.load_data_from_minio()
        
        # Coordonnées de transformation
        self.setup_coordinate_transform()
        
        # Producer Kafka
        self.producer = None
        self.init_kafka()
        
        # Gestion des véhicules
        self.active_vehicles = {}
        self.vehicle_threads = {}
        
        # Statistiques
        self.total_events = 0
        self.blackout_events = 0
    

    def load_data_from_minio(self):
        """Charge les données Porto depuis MinIO avec boto3"""
        try:
            # Configurer le client S3 pour MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url=f'http://{self.minio_endpoint}',
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
            )
            
            # Télécharger le fichier
            response = s3_client.get_object(
                Bucket=self.minio_bucket,
                Key=self.minio_object
            )
            
            # Lire le CSV
            self.trips_df = pd.read_csv(response['Body'])
            logger.info(f"✅ Données chargées: {len(self.trips_df)} trajets")
            
        except Exception as e:
            logger.error(f"❌ Erreur: {e}")
            raise
    
    def load_zone_mapping(self):
        """Charge le mapping des zones"""
        try:
            url = f"http://{self.minio_endpoint}/curated/zone_mapping.csv"
            response = requests.get(url, auth=(self.minio_access_key, self.minio_secret_key))
            
            if response.status_code == 200:
                self.zone_mapping = pd.read_csv(io.BytesIO(response.content))
                logger.info(f"✅ Zone mapping chargé: {len(self.zone_mapping)} zones")
                return
        except:
            pass
        
        # Fallback
        zone_path = Path(__file__).parent.parent / 'data' / 'zone_mapping.csv'
        if zone_path.exists():
            self.zone_mapping = pd.read_csv(zone_path)
            logger.info(f"✅ Zone mapping local: {len(self.zone_mapping)} zones")
        else:
            self.create_default_zone_mapping()
    
    def create_default_zone_mapping(self):
        """Crée un mapping par défaut"""
        zones = []
        for i in range(1, 17):
            zones.append({
                'zone_id': i,
                'zone_name': f'Zone_{i}',
                'zone_type': 'mixed',
                'centroid_lon': -7.59 + ((i % 8) * 0.015),
                'centroid_lat': 33.57 + ((i // 8) * 0.02)
            })
        self.zone_mapping = pd.DataFrame(zones)
        logger.info(f"Mapping par défaut: {len(self.zone_mapping)} zones")
    
    def send_initial_available_vehicles(self, count: int):
        """Envoie des événements 'available' avec une position par défaut"""
        logger.info(f"🚕 Envoi de {count} taxis disponibles...")
        
        for i in range(min(count, len(self.trips_df))):
            trip = self.trips_df.iloc[i]
            taxi_id = int(trip['TAXI_ID'])
            
            # Position par défaut (centre de Casablanca)
            default_lat = 33.5731
            default_lon = -7.5898
            default_zone = 8  # Zone centrale
            
            event = {
                "taxi_id": taxi_id,
                "timestamp": datetime.now().isoformat(),
                "timestamp_unix": int(datetime.now().timestamp()),
                "lat": default_lat,
                "lon": default_lon,
                "zone_id": default_zone,
                "status": "available",
                "trip_progress": 0.0
            }
            
            try:
                self.producer.send('raw.gps', key=taxi_id, value=event)
                self.total_events += 1
            except Exception as e:
                logger.error(f"Erreur: {e}")
            
            if (i + 1) % 50 == 0:
                logger.info(f"  {i+1} taxis disponibles envoyés")
            
            time.sleep(0.05)
        
        logger.info(f"✅ {count} taxis disponibles initialisés")
    
    def generate_synthetic_trips(self):
        """Génère des trajets synthétiques"""
        n_trips = 5000
        logger.info(f"Génération de {n_trips} trajets synthétiques...")
        
        trips = []
        for i in range(n_trips):
            n_points = random.randint(10, 50)
            polyline = []
            lon = -7.59 + random.uniform(-0.15, 0.15)
            lat = 33.57 + random.uniform(-0.1, 0.1)
            
            for j in range(n_points):
                lon += random.uniform(-0.001, 0.001)
                lat += random.uniform(-0.001, 0.001)
                polyline.append([round(lon, 6), round(lat, 6)])
            
            trips.append({
                'TRIP_ID': f'SYN_{i:08d}',
                'TAXI_ID': random.randint(20000001, 20000981),
                'POLYLINE': json.dumps(polyline),
                'TIMESTAMP': int(datetime(2014, 1, 1).timestamp() + i * 60)
            })
        
        self.trips_df = pd.DataFrame(trips)
        logger.info(f"✅ Données synthétiques: {len(self.trips_df)} trajets")
    
    def setup_coordinate_transform(self):
        """Configure la transformation Porto -> Casablanca"""
        self.porto_lon_min = -8.7327
        self.porto_lon_max = -8.5539
        self.porto_lat_min = 41.0527
        self.porto_lat_max = 41.2370
        
        self.casa_lon_min = -7.7000
        self.casa_lon_max = -7.4800
        self.casa_lat_min = 33.4800
        self.casa_lat_max = 33.6800
        
        logger.info("📍 Transformation Porto → Casablanca configurée")
    
    def transform_coordinates(self, lon_porto: float, lat_porto: float) -> Tuple[float, float]:
        """Transforme les coordonnées Porto vers Casablanca"""
        lon_ratio = (lon_porto - self.porto_lon_min) / (self.porto_lon_max - self.porto_lon_min)
        lat_ratio = (lat_porto - self.porto_lat_min) / (self.porto_lat_max - self.porto_lat_min)
        
        lon_casa = self.casa_lon_min + lon_ratio * (self.casa_lon_max - self.casa_lon_min)
        lat_casa = self.casa_lat_min + lat_ratio * (self.casa_lat_max - self.casa_lat_min)
        
        return round(lon_casa, 6), round(lat_casa, 6)
    
    def add_gps_noise(self, lon: float, lat: float, sigma: float = 0.0002) -> Tuple[float, float]:
        """Ajoute du bruit GPS (σ = 0.0002° ≈ 20m)"""
        return round(lon + np.random.normal(0, sigma), 6), round(lat + np.random.normal(0, sigma), 6)
    
    def get_zone_id(self, lon: float, lat: float) -> int:
        """Trouve la zone la plus proche"""
        if self.zone_mapping is None:
            return random.randint(1, 16)
        
        best_zone = 1
        min_dist = float('inf')
        
        for _, zone in self.zone_mapping.iterrows():
            dist = ((lon - zone['centroid_lon']) ** 2 + (lat - zone['centroid_lat']) ** 2) ** 0.5
            if dist < min_dist:
                min_dist = dist
                best_zone = int(zone['zone_id'])
        
        return best_zone
    
    def init_kafka(self):
        """Initialise la connexion Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(convert_to_native_types(v)).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"✅ Connecté à Kafka: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Erreur Kafka: {e}")
            self.producer = None
    
    def parse_polyline(self, polyline_str: str) -> List[Tuple[float, float]]:
        """Parse une polyline JSON"""
        try:
            points = json.loads(polyline_str)
            return [(float(p[0]), float(p[1])) for p in points]
        except:
            return []
    
    def should_blackout(self) -> bool:
        """5% de chance de blackout"""
        return random.random() < 0.05
    
    def get_blackout_duration(self) -> int:
        """Durée du blackout (60-180 secondes)"""
        return random.randint(60, 180)
    
    def simulate_vehicle_trip(self, trip_row: pd.Series):
        """Simule un véhicule sur un trajet"""
        taxi_id = int(trip_row['TAXI_ID'])
        polyline = self.parse_polyline(trip_row['POLYLINE'])
        
        if not polyline:
            logger.warning(f"Polyline vide pour taxi {taxi_id}")
            return
        
        logger.info(f"🚖 Véhicule {taxi_id} démarre trajet ({len(polyline)} points)")
        
        for i, (lon_porto, lat_porto) in enumerate(polyline):
            if not self.active_vehicles.get(taxi_id, {}).get('active', True):
                break
            
            # Transformation
            lon_casa, lat_casa = self.transform_coordinates(lon_porto, lat_porto)
            lon_noisy, lat_noisy = self.add_gps_noise(lon_casa, lat_casa)
            zone_id = self.get_zone_id(lon_noisy, lat_noisy)
            
            event_time = datetime.now()
            
            # Créer l'événement avec types Python natifs
            event = {
                "taxi_id": taxi_id,
                "timestamp": event_time.isoformat(),
                "timestamp_unix": int(event_time.timestamp()),
                "lat": float(lat_noisy),
                "lon": float(lon_noisy),
                "zone_id": int(zone_id),
                "speed": float(random.uniform(20, 60)),
                "status": "moving",
                "trip_progress": float(i / max(len(polyline), 1))
            }
            
            # Blackout
            if self.should_blackout():
                blackout_duration = self.get_blackout_duration()
                self.blackout_events += 1
                logger.info(f"⚠️ Taxi {taxi_id}: Blackout GPS de {blackout_duration}s")
                time.sleep(blackout_duration / self.speed_factor)
                continue
            
            try:
                self.producer.send('raw.gps', key=taxi_id, value=event)
                self.total_events += 1
                
                if self.total_events % 100 == 0:
                    logger.info(f"📊 Événements GPS: {self.total_events} | Blackouts: {self.blackout_events}")
            except Exception as e:
                logger.error(f"Erreur envoi taxi {taxi_id}: {e}")
            
            # Attendre 15 secondes réelles / speed_factor
            time.sleep(15.0 / self.speed_factor)
        
        # Fin du trajet
        logger.info(f"🏁 Véhicule {taxi_id} termine son trajet")
        
        end_event = {
            "taxi_id": taxi_id,
            "timestamp": datetime.now().isoformat(),
            "timestamp_unix": int(datetime.now().timestamp()),
            "status": "available",
            "trip_progress": 1.0
        }
        
        try:
            self.producer.send('raw.gps', key=taxi_id, value=end_event)
        except Exception as e:
            logger.error(f"Erreur fin trajet: {e}")
    
    def run_vehicle(self, taxi_id: int, trip_row: pd.Series):
        """Exécute un véhicule dans un thread"""
        self.active_vehicles[taxi_id] = {'active': True}
        
        thread = threading.Thread(
            target=self.simulate_vehicle_trip,
            args=(trip_row,),
            daemon=True,
            name=f"Vehicle-{taxi_id}"
        )
        self.vehicle_threads[taxi_id] = thread
        thread.start()
        logger.info(f"✅ Véhicule {taxi_id} démarré")
    
    def run(self, duration_seconds: int = None, max_vehicles: int = 50):
        """Lance la simulation"""
        if self.producer is None:
            logger.error("Producer non initialisé")
            return
        
        logger.info("=" * 60)
        logger.info("🚕 TaaSim - Vehicle GPS Producer")
        logger.info(f"📊 Speed factor: {self.speed_factor}x")
        logger.info(f"🚗 Max vehicles: {max_vehicles}")
        logger.info("=" * 60)
        
        # ÉTAPE 1: Envoyer les taxis disponibles au démarrage
        self.send_initial_available_vehicles(max_vehicles)
        
        # Petite pause pour que les messages "available" soient bien envoyés
        time.sleep(1)
        
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration_seconds) if duration_seconds else None
        
        vehicle_counter = 0
        active_vehicle_count = 0
        
        try:
            while True:
                if end_time and datetime.now() >= end_time:
                    break
                
                # Lancer un nouveau véhicule si possible
                if vehicle_counter < len(self.trips_df) and active_vehicle_count < max_vehicles:
                    trip = self.trips_df.iloc[vehicle_counter]
                    taxi_id = int(trip['TAXI_ID'])
                    
                    if taxi_id not in self.vehicle_threads:
                        self.run_vehicle(taxi_id, trip)
                        active_vehicle_count += 1
                    
                    vehicle_counter += 1
                    time.sleep(2.0 / self.speed_factor)
                
                # Nettoyer les threads terminés
                completed = []
                for tid, thread in self.vehicle_threads.items():
                    if not thread.is_alive():
                        completed.append(tid)
                
                for tid in completed:
                    del self.vehicle_threads[tid]
                    if tid in self.active_vehicles:
                        del self.active_vehicles[tid]
                    active_vehicle_count -= 1
                
                time.sleep(1.0)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Interruption utilisateur")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Nettoie les ressources"""
        logger.info("Nettoyage...")
        
        for taxi_id in list(self.active_vehicles.keys()):
            self.active_vehicles[taxi_id]['active'] = False
        
        time.sleep(2)
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        logger.info(f"📊 Final: {self.total_events} events, {self.blackout_events} blackouts")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', default='kafka:9092')
    parser.add_argument('--speed-factor', type=float, default=10.0)
    parser.add_argument('--duration', type=int, default=None)
    parser.add_argument('--max-vehicles', type=int, default=10)
    parser.add_argument('--minio-endpoint', default='minio:9000')
    parser.add_argument('--minio-access-key', default='taasim')
    parser.add_argument('--minio-secret-key', default='taasim123')
    
    args = parser.parse_args()
    
    producer = VehicleGPSProducer(
        bootstrap_servers=args.bootstrap_servers,
        speed_factor=args.speed_factor,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key
    )
    
    producer.run(duration_seconds=args.duration, max_vehicles=args.max_vehicles)


if __name__ == "__main__":
    main()