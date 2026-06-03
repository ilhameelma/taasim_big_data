#!/usr/bin/env python3
# flink_job3_trip_matcher.py
# Semaine 4 — Job 3: Trip Matcher
# 
# Fonctionnalités:
# - Lecture de raw.trips (demandes clients) ET processed.gps (positions taxis)
# - Maintien d'un état (keyed state) des véhicules disponibles par zone
# - Matching: trouve le véhicule disponible le plus proche dans la même zone
# - Fallback: si aucun véhicule dans la zone, cherche dans les zones adjacentes
# - Calcul d'ETA simple basé sur distance et vitesse moyenne
# - Écriture dans Cassandra (table trips) et Kafka (processed.matches)
#
# CORRECTION MAJEURE: Utilisation de KeyedCoProcessFunction au lieu de
# union + KeyedProcessFunction pour éviter la race condition où les trips
# arrivent en burst avant que les GPS aient peuplé le state.

import json
import os
import uuid
import math
from datetime import datetime, timezone
from collections import defaultdict

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.datastream.functions import MapFunction, FilterFunction, FlatMapFunction, KeyedCoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig, Time

# ============================================================
# CONFIGURATION
# ============================================================

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = "taasim"
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

# Zones adjacentes (fallback)
# Format: zone_id -> [liste des zones adjacentes]
ADJACENT_ZONES = {
    1: [2, 11, 12],           # Mechouar
    2: [1, 3, 12, 13],        # Anfa
    3: [2, 7, 8, 9],          # Hay Hassani
    4: [5, 6, 13, 14, 15],    # Sidi Moumen / Sidi Bernoussi
    5: [4, 6, 13],            # Moulay Rachid
    6: [4, 5, 13, 14],        # Sidi Othmane
    7: [3, 8, 10, 11],        # Ben M'Sick
    8: [3, 7, 9, 10],         # Sbata
    9: [3, 8, 10, 16],        # Ain Chock
    10: [7, 8, 9, 11, 16],    # Al Fida
    11: [1, 7, 10, 12, 16],   # Mers Sultan
    12: [1, 2, 11, 13, 16],   # Assoukhour Assawda
    13: [2, 4, 5, 6, 12, 14], # Hay Mohammadi
    14: [4, 6, 13, 15],       # Ain Sebaa
    15: [4, 14],              # Sidi Bernoussi
    16: [9, 10, 11, 12],      # El Maarif
    17: [2, 12, 13],          # Sidi Belyout
}

# Distances approximatives entre zones (en km)
# Pour le calcul ETA simplifié
ZONE_DISTANCES = {
    (1, 2): 5.2, (1, 3): 6.1, (1, 11): 3.5, (1, 12): 4.8,
    (2, 3): 3.2, (2, 12): 4.5, (2, 13): 5.0,
    (3, 7): 4.5, (3, 8): 5.0, (3, 9): 6.0,
    (4, 5): 3.0, (4, 6): 4.0, (4, 13): 5.5, (4, 14): 6.0, (4, 15): 4.0,
    (5, 6): 2.5, (5, 13): 4.5,
    (6, 13): 4.0, (6, 14): 5.0,
    (7, 8): 3.0, (7, 10): 4.0, (7, 11): 3.5,
    (8, 9): 4.0, (8, 10): 3.5,
    (9, 10): 4.5, (9, 16): 5.0,
    (10, 11): 3.0, (10, 16): 4.5,
    (11, 12): 3.5, (11, 16): 4.0,
    (12, 13): 4.0, (12, 16): 4.5,
    (13, 14): 5.0,
    (14, 15): 6.0,
}


# ============================================================
# 1. PARSERS
# ============================================================

class ParseTripRequest(MapFunction):
    """Parse les demandes de trajet depuis raw.trips"""
    def map(self, raw_str):
        try:
            e = json.loads(raw_str)
            return {
                "type": "trip",
                "trip_id": e.get("trip_id"),
                "rider_id": e.get("rider_id"),
                "origin_zone": int(e.get("origin_zone", 0)),
                "destination_zone": int(e.get("destination_zone", 0)),
                "requested_at": e.get("requested_at"),
                "call_type": e.get("call_type", "B"),
                "passenger_count": e.get("passenger_count", 1),
                "estimated_fare_mad": e.get("estimated_fare_mad", 0.0)
            }
        except Exception as e:
            print(f"❌ ParseTripRequest error: {e}")
            return None


class ParseGPSPosition(MapFunction):
    """Parse les positions GPS depuis processed.gps (déjà enrichies par Job 1)"""
    def map(self, raw_str):
        try:
            e = json.loads(raw_str)
            return {
                "type": "gps",
                "taxi_id": int(e.get("taxi_id", 0)),
                "zone_id": int(e.get("zone_id", 0)),
                "lat": float(e.get("lat", 0.0)),
                "lon": float(e.get("lon", 0.0)),
                "speed": float(e.get("speed", 0.0)),
                "status": e.get("status", "unknown"),
                "timestamp": e.get("timestamp"),
                "timestamp_unix": int(e.get("timestamp_unix", 0))
            }
        except Exception as e:
            print(f"❌ ParseGPSPosition error: {e}")
            return None


class NotNone(FilterFunction):
    def filter(self, v):
        return v is not None


# ============================================================
# 2. CORE TRIP MATCHER (KeyedCoProcessFunction)
# ============================================================

class TripMatcherCoProcess(KeyedCoProcessFunction):
    """
    CoProcessFunction qui gère deux streams distincts:
    - process_element1() pour les GPS events
    - process_element2() pour les TRIP events
    Partage le même state (available_vehicles) par clé (zone_id)
    
    Cette approche résout le problème de race condition où les trips arrivaient
    en burst avant que les GPS aient peuplé le state.
    """
    
    def open(self, ctx):
        # État: liste des véhicules disponibles dans la zone
        # TTL de 5 minutes pour nettoyer les véhicules qui n'ont pas envoyé de position
        ttl_config = StateTtlConfig.new_builder(Time.seconds(300)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()
        
        descriptor = ValueStateDescriptor("available_vehicles", Types.STRING())
        descriptor.enable_time_to_live(ttl_config)
        self.vehicle_state = ctx.get_state(descriptor)
        print("✅ TripMatcherCoProcess initialisé")
    
    def _get_vehicles(self):
        """Lit le state JSON et retourne une liste de dicts"""
        raw = self.vehicle_state.value()
        if not raw:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def _set_vehicles(self, vehicles):
        """Sérialise la liste en JSON et écrit dans le state"""
        self.vehicle_state.update(json.dumps(vehicles))

    def remove_vehicle(self, taxi_id):
        """Retire un véhicule de l'état"""
        vehicles = self._get_vehicles()
        new_vehicles = [v for v in vehicles if v["taxi_id"] != taxi_id]
        self._set_vehicles(new_vehicles)
    
    def add_or_update_vehicle(self, gps_event):
        """Ajoute ou met à jour un véhicule dans l'état"""
        vehicles = self._get_vehicles()
        found = False
        for i, v in enumerate(vehicles):
            if v["taxi_id"] == gps_event["taxi_id"]:
                vehicles[i] = gps_event
                found = True
                break
        if not found:
            vehicles.append(gps_event)
        self._set_vehicles(vehicles)
    
    # =========== STREAM 1: GPS EVENTS ===========
    def process_element1(self, event, ctx):
        if event["type"] != "gps":
            return []
        
        if event["status"] == "available":
            # ← Toujours ajouter/mettre à jour les taxis disponibles
            self.add_or_update_vehicle(event)
            vehicles = self._get_vehicles()
            print(f"✅ GPS available: Taxi {event['taxi_id']} zone {event['zone_id']} → {len(vehicles)} dispo")
        else:
            # Taxi en mouvement → retirer du pool
            self.remove_vehicle(event["taxi_id"])
        
        return []
        
        # Véhicule disponible → l'ajouter ou mettre à jour
        self.add_or_update_vehicle(event)
        
        # Log optionnel pour le debug
        vehicles = self._get_vehicles()
        if len(vehicles) % 5 == 0:  # Log périodique
            print(f"📍 GPS: zone {event['zone_id']} → {len(vehicles)} véhicules disponibles")
        
        return []
    
    # =========== STREAM 2: TRIP EVENTS ===========
    def process_element2(self, event, ctx):
        """
        Traite les events du deuxième stream connecté (TRIP)
        Utilise l'état GPS (déjà peuplé par process_element1) pour faire le matching
        """
        if event["type"] != "trip":
            return []
        
        origin_zone = event["origin_zone"]
        destination_zone = event["destination_zone"]
        
        # Log du state actuel
        vehicles = self._get_vehicles()
        print(f"🚕 TRIP: zone {origin_zone} → {len(vehicles)} véhicules dans le state")
        
        # 1. Chercher dans la zone demandée
        matched_vehicle = self.find_vehicle_in_zone(origin_zone)
        matched_zone = origin_zone
        is_fallback = False
        
        # 2. Fallback: zones adjacentes
        if not matched_vehicle:
            for adj_zone in ADJACENT_ZONES.get(origin_zone, []):
                matched_vehicle = self.find_vehicle_in_zone(adj_zone)
                if matched_vehicle:
                    matched_zone = adj_zone
                    is_fallback = True
                    print(f"🔄 FALLBACK: zone {origin_zone} → zone {adj_zone}")
                    break
        
        # 3. Créer l'événement de match
        if matched_vehicle:
            return self.create_match_event(
                event, matched_vehicle, matched_zone, is_fallback, destination_zone
            )
        else:
            return self.create_unmatch_event(event)
    
    def find_vehicle_in_zone(self, zone_id):
        """Trouve le meilleur véhicule dans une zone (le plus rapide)"""
        vehicles = self._get_vehicles()
        if not vehicles:
            return None
        
        # Filtrer les véhicules de la zone demandée
        zone_vehicles = [v for v in vehicles if v["zone_id"] == zone_id]
        if not zone_vehicles:
            return None
        
        # Choisir le meilleur véhicule (priorité: vitesse maximale)
        # Plus le véhicule est rapide, mieux c'est pour ETA
        best = max(zone_vehicles, key=lambda v: v.get("speed", 0))
        
        # Retirer le véhicule de l'état (il n'est plus disponible)
        self.remove_vehicle(best["taxi_id"])
        
        return best
    
    def compute_distance(self, origin_zone, dest_zone):
        """Calcule la distance approximative entre deux zones"""
        if origin_zone == dest_zone:
            return 3.0  # Distance intra-zone: ~3km
        
        key = (min(origin_zone, dest_zone), max(origin_zone, dest_zone))
        return ZONE_DISTANCES.get(key, 5.0)  # Valeur par défaut: 5km
    
    def compute_eta(self, distance_km, avg_speed_kmh):
        """Calcule l'ETA en secondes"""
        if avg_speed_kmh <= 0:
            avg_speed_kmh = 30  # Vitesse moyenne par défaut: 30 km/h
        eta_seconds = (distance_km / avg_speed_kmh) * 3600
        return int(min(eta_seconds, 1800))  # Max 30 minutes
    
    def create_match_event(self, trip, vehicle, matched_zone, is_fallback, dest_zone):
        """Crée un événement de match réussi"""
        distance_km = self.compute_distance(matched_zone, dest_zone)
        eta_seconds = self.compute_eta(distance_km, vehicle.get("speed", 30))
        
        match_event = {
            "status": "matched",
            "trip_id": trip["trip_id"],
            "rider_id": trip["rider_id"],
            "taxi_id": vehicle["taxi_id"],
            "origin_zone": trip["origin_zone"],
            "matched_zone": matched_zone,
            "destination_zone": dest_zone,
            "is_fallback": is_fallback,
            "distance_km": round(distance_km, 2),
            "eta_seconds": eta_seconds,
            "estimated_fare_mad": trip.get("estimated_fare_mad", 0.0),
            "requested_at": trip["requested_at"],
            "matched_at": datetime.now(timezone.utc).isoformat(),
            "match_latency_ms": self.compute_latency_ms(trip["requested_at"])
        }
        
        print(f"✅ MATCH: Trip {trip['trip_id']} → Taxi {vehicle['taxi_id']} (zone {matched_zone}) | ETA={eta_seconds}s fallback={is_fallback}")
        return [json.dumps(match_event)]
    
    def create_unmatch_event(self, trip):
        """Crée un événement de match échoué"""
        unmatch_event = {
            "status": "unmatched",
            "trip_id": trip["trip_id"],
            "rider_id": trip["rider_id"],
            "origin_zone": trip["origin_zone"],
            "destination_zone": trip["destination_zone"],
            "reason": "no_available_vehicle",
            "requested_at": trip["requested_at"],
            "unmatched_at": datetime.now(timezone.utc).isoformat()
        }
        print(f"❌ UNMATCH: Trip {trip['trip_id']} - Aucun véhicule disponible en zone {trip['origin_zone']}")
        return [json.dumps(unmatch_event)]
    
    def compute_latency_ms(self, requested_at_str):
        """Calcule la latence entre la demande et le match"""
        try:
            requested_at = datetime.fromisoformat(requested_at_str.replace('Z', '+00:00'))
            matched_at = datetime.now(timezone.utc)
            return int((matched_at - requested_at).total_seconds() * 1000)
        except:
            return 0


# ============================================================
# 3. SINKS
# ============================================================

class KafkaMatchSink(MapFunction):
    """Publie les matchs sur le topic Kafka processed.matches"""
    
    def open(self, ctx):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks=1,
            retries=3,
        )
        print("✅ KAFKA SINK READY → processed.matches")
    
    def map(self, s):
        try:
            rec = json.loads(s)
            if rec["status"] == "matched":
                self.producer.send("processed.matches", key=str(rec["trip_id"]), value=s)
        except Exception as e:
            print(f"KAFKA ERR: {e}")
        return s
    
    def close(self):
        if hasattr(self, "producer"):
            try:
                self.producer.flush()
                self.producer.close()
            except:
                pass


class CassandraTripSink(MapFunction):
    """Écrit les matchs dans la table trips de Cassandra"""
    
    def open(self, ctx):
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = cluster.connect(KEYSPACE)
        self.stmt = self.session.prepare("""
            INSERT INTO trips (
                city, date_bucket, created_at, trip_id, rider_id, taxi_id,
                origin_zone, dest_zone, call_type, status,
                estimated_fare, match_latency_ms,
                requested_at, matched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        print("✅ CASSANDRA SINK READY → trips")
    
    def map(self, s):
        try:
            r = json.loads(s)
            if r["status"] != "matched":
                return s
            
            # date_bucket = YYYY-MM-DD
            date_bucket = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            self.session.execute(self.stmt, (
                "Casablanca",
                date_bucket,
                datetime.now(timezone.utc),
                r["trip_id"],
                r["rider_id"],
                int(r["taxi_id"]),
                int(r["origin_zone"]),
                int(r["destination_zone"]),
                "B",  # call_type par défaut
                r["status"],
                float(r.get("estimated_fare_mad", 0.0)),
                int(r.get("match_latency_ms", 0)),
                datetime.fromisoformat(r["requested_at"].replace("Z", "+00:00")),
                datetime.fromisoformat(r["matched_at"].replace("Z", "+00:00"))
            ))
            print(f"💾 CASSANDRA: Trip {r['trip_id']} écrit")
        except Exception as e:
            print(f"CASSANDRA TRIP ERR: {e}")
        return s
    
    def close(self):
        if hasattr(self, "session") and self.session:
            try:
                self.session.shutdown()
            except:
                pass


<<<<<<< HEAD
class MinIOArchiveSink(MapFunction):
    """Archive les match events dans MinIO raw/kafka-archive/processed.matches/"""
    
    def open(self, ctx):
        from minio import Minio
        import io
        self.client = Minio(
            "minio:9000",
            access_key="taasim",
            secret_key="taasim123",
            secure=False
        )
        self.bucket = "raw"
        self.buffer = []
        self.buffer_size = 100  # flush tous les 100 messages
        self.batch_id = 0
        print("✅ MINIO SINK READY → raw/kafka-archive/processed.matches/")
    
    def map(self, s):
        try:
            self.buffer.append(s)
            if len(self.buffer) >= self.buffer_size:
                self._flush()
        except Exception as e:
            print(f"MINIO ERR: {e}")
        return s
    
    def _flush(self):
        if not self.buffer:
            return
        import io
        from datetime import datetime, timezone
        
        now = datetime.now(timezone.utc)
        # Chemin : raw/kafka-archive/processed.matches/2025/05/09/batch_001.jsonl
        path = f"kafka-archive/processed.matches/{now.strftime('%Y/%m/%d')}/batch_{self.batch_id:06d}.jsonl"
        
        content = "\n".join(self.buffer).encode("utf-8")
        self.client.put_object(
            self.bucket,
            path,
            io.BytesIO(content),
            length=len(content),
            content_type="application/jsonl"
        )
        print(f"💾 MINIO: {len(self.buffer)} matchs archivés → {path}")
        self.buffer = []
        self.batch_id += 1
    
    def close(self):
        # Flush final à la fermeture
        self._flush()

=======
>>>>>>> 90a31376a6591c75d804411da3621743e99542a1
# ============================================================
# 4. MAIN
# ============================================================

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Checkpointing pour sauvegarder l'état
    env.enable_checkpointing(60000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)
    
    # Configuration pour réduire le batching et améliorer la latence
    env.get_config().set_auto_watermark_interval(100)  # 100ms
    
    print("=" * 70)
    print("🚀 TaaSim — Flink Job 3 — Trip Matcher (CORRECTED VERSION)")
    print("   ✅ Sources: processed.gps + raw.trips")
    print("   ✅ KeyedCoProcessFunction pour éviter la race condition")
    print("   ✅ Stateful matching par zone_id")
    print("   ✅ Fallback vers zones adjacentes")
    print("   ✅ ETA calculé par distance/vitesse")
    print("   ✅ Sinks: Kafka (processed.matches) + Cassandra (trips)")
    print("=" * 70)
    
    # Source: raw.trips (demandes clients)
    trip_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.trips")
        .set_group_id("flink-job3-trips")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())
    
    # Source: processed.gps (positions taxis enrichies par Job 1)
    gps_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("processed.gps")
        .set_group_id("flink-job3-gps")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())
    
    # Streams parsés
    gps_stream = env.from_source(gps_source, WatermarkStrategy.no_watermarks(), "GPS") \
        .map(ParseGPSPosition()) \
        .filter(NotNone())
    
    trip_stream = env.from_source(trip_source, WatermarkStrategy.no_watermarks(), "Trips") \
        .map(ParseTripRequest()) \
        .filter(NotNone())
    
    # Key BY zone_id pour LES DEUX streams
    # Pour GPS: la clé est la zone_id du véhicule
    # Pour TRIP: la clé est l'origin_zone de la demande
    gps_keyed = gps_stream.key_by(lambda e: e["zone_id"])
    trip_keyed = trip_stream.key_by(lambda e: e["origin_zone"])
    
    # Connect + CoProcessFunction
    # C'est le cœur de la correction: les deux streams partagent le même state par clé
    connected = gps_keyed.connect(trip_keyed)
    match_stream = connected.process(TripMatcherCoProcess(), output_type=Types.STRING())
    
    # Sinks
    match_stream = match_stream.map(KafkaMatchSink(), output_type=Types.STRING())
    match_stream = match_stream.map(CassandraTripSink(), output_type=Types.STRING())
<<<<<<< HEAD
    match_stream = match_stream.map(MinIOArchiveSink(), output_type=Types.STRING()) 
=======
>>>>>>> 90a31376a6591c75d804411da3621743e99542a1
    match_stream.print()
    
    print("▶️  Démarrage du Job 3 (KeyedCoProcessFunction)...")
    env.execute("TaaSim — Job3 — Trip Matcher")


if __name__ == "__main__":
    main()