# flink_job1_gps_normalizer.py
# Semaine 3 — Tâche 5 : Sink Cassandra via MapFunction
# Semaine 4 — Ajout : publication sur Kafka processed.gps

import json
import csv
import os
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.datastream.functions import MapFunction, FilterFunction

CASA_LON_MIN = -7.78
CASA_LON_MAX = -7.40
CASA_LAT_MIN = 33.48
CASA_LAT_MAX = 33.70
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "taasim"
KAFKA_BROKERS = "kafka:9092"

def is_in_casablanca_bbox(lon, lat):
    return (CASA_LON_MIN <= lon <= CASA_LON_MAX and
            CASA_LAT_MIN <= lat <= CASA_LAT_MAX)

# ============================================================
# 1. Parse JSON
# ============================================================
class ParseGpsEvent(MapFunction):
    def map(self, raw_str):
        try:
            event = json.loads(raw_str)
            if 'timestamp_unix' in event:
                event['event_time_ms'] = int(event['timestamp_unix']) * 1000
            return event
        except Exception:
            return None

# ============================================================
# 2. Filtre bbox
# ============================================================
class ValidEvent(FilterFunction):
    def filter(self, event):
        if event is None:
            return False
        required = ["taxi_id", "timestamp_unix", "status"]
        if not all(k in event for k in required):
            return False
        
        # Si lat/lon présents, vérifier la bbox
        if "lat" in event and "lon" in event:
            return is_in_casablanca_bbox(event.get("lon"), event.get("lat"))
        
        # Event sans lat/lon (fin de trajet) → laisser passer si available
        return event.get("status") == "available"

# ============================================================
# 3. Zone mapping + Anonymisation
# ============================================================
class ZoneMappingFunction(MapFunction):
    def __init__(self):
        self.zones = None

    def open(self, runtime_context):
        self.zones = self.get_default_zones()
        for path in ["/opt/flink/data/zone_mapping_geojson.csv",
                     "/opt/flink/jobs/zone_mapping_geojson.csv"]:
            if os.path.exists(path):
                try:
                    zones = []
                    with open(path, 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            lon_min = float(row["bbox_lon_min"])
                            lon_max = float(row["bbox_lon_max"])
                            lat_min = float(row["bbox_lat_min"])
                            lat_max = float(row["bbox_lat_max"])
                            zones.append({
                                "zone_id": int(row["zone_id"]),
                                "zone_name": row.get("prefecture", row.get("zone_name", "Unknown")),
                                "zone_type": row.get("zone_type", "mixed"),
                                "base_fare_mad": float(row.get("base_fare_mad", 8.0)),
                                "bbox": (lon_min, lon_max, lat_min, lat_max),
                                "centroid_lon": (lon_min + lon_max) / 2,
                                "centroid_lat": (lat_min + lat_max) / 2,
                            })
                    if zones:
                        self.zones = zones
                        break
                except Exception:
                    pass

    def get_default_zones(self):
        return [
            {"zone_id": 1,  "zone_name": "Mechouar",      "zone_type": "residential",
             "base_fare_mad": 8.0,  "bbox": (-7.61, -7.59, 33.57, 33.59),
             "centroid_lon": -7.60,  "centroid_lat": 33.58},
            {"zone_id": 2,  "zone_name": "Anfa",           "zone_type": "commercial",
             "base_fare_mad": 10.0, "bbox": (-7.68, -7.64, 33.55, 33.58),
             "centroid_lon": -7.66,  "centroid_lat": 33.565},
            {"zone_id": 3,  "zone_name": "Hay Hassani",    "zone_type": "commercial",
             "base_fare_mad": 10.0, "bbox": (-7.67, -7.64, 33.54, 33.56),
             "centroid_lon": -7.655, "centroid_lat": 33.55},
            {"zone_id": 4,  "zone_name": "Sidi Bernoussi", "zone_type": "residential",
             "base_fare_mad": 8.0,  "bbox": (-7.55, -7.53, 33.58, 33.60),
             "centroid_lon": -7.54,  "centroid_lat": 33.59},
            {"zone_id": 5,  "zone_name": "Moulay Rachid",  "zone_type": "residential",
             "base_fare_mad": 8.0,  "bbox": (-7.60, -7.57, 33.54, 33.56),
             "centroid_lon": -7.585, "centroid_lat": 33.55},
        ]

    def get_zone(self, lon, lat):
        for zone in self.zones:
            lon_min, lon_max, lat_min, lat_max = zone["bbox"]
            if lon_min <= lon <= lon_max and lat_min <= lat <= lat_max:
                return zone
        return None

    def map(self, event):
        lon = event.get("lon")
        lat = event.get("lat")
        
        # Si pas de coordonnées, utiliser le centroïde de Casablanca par défaut
        if lon is None or lat is None:
            event["zone_id"]       = event.get("zone_id", 1)
            event["zone_name"]     = event.get("zone_name", "Unknown")
            event["zone_type"]     = event.get("zone_type", "mixed")
            event["base_fare_mad"] = event.get("base_fare_mad", 8.0)
            event["lat"]           = 33.5731
            event["lon"]           = -7.5898
            return event
        if zone:
            event["zone_id"]       = zone["zone_id"]
            event["zone_name"]     = zone["zone_name"]
            event["zone_type"]     = zone["zone_type"]
            event["base_fare_mad"] = zone["base_fare_mad"]
            event["lat"]           = zone["centroid_lat"]   # 🔒 anonymisé
            event["lon"]           = zone["centroid_lon"]   # 🔒 anonymisé
        else:
            event["zone_id"]       = 0
            event["zone_name"]     = "Inconnu"
            event["zone_type"]     = "unknown"
            event["base_fare_mad"] = 8.0
            event["lat"]           = 33.5731
            event["lon"]           = -7.5898
        return event

# ============================================================
# 4. Sink Cassandra → vehicle_positions
# ============================================================
class CassandraSinkMap(MapFunction):
    def __init__(self):
        self.session     = None
        self.insert_stmt = None

    def open(self, runtime_context):
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = cluster.connect(KEYSPACE)
        self.insert_stmt = self.session.prepare("""
            INSERT INTO vehicle_positions
                (city, zone_id, event_time, taxi_id, lat, lon, speed, status, trip_progress)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        print("✅ [Job1] Cassandra connecté — vehicle_positions prêt")

    def map(self, event):
        try:
            ts = datetime.utcfromtimestamp(event.get("timestamp_unix", 0))
            self.session.execute(self.insert_stmt, (
                "Casablanca",
                int(event.get("zone_id", 0)),
                ts,
                int(event.get("taxi_id", 0)),
                float(event.get("lat", 0.0)),
                float(event.get("lon", 0.0)),
                float(event.get("speed", 0.0)),
                str(event.get("status", "unknown")),
                float(event.get("trip_progress", 0.0))
            ))
            return event   # passe l'event au sink Kafka suivant
        except Exception as e:
            print(f"❌ [Job1] Cassandra error taxi {event.get('taxi_id')}: {e}")
            return event   # on continue quand même vers Kafka

    def close(self):
        if self.session:
            self.session.shutdown()

# ============================================================
# 5. ✨ NOUVEAU — Sink Kafka → processed.gps
#    Sérialise l'event enrichi (avec zone_id, zone_name, anonymisé)
#    et le publie sur le topic processed.gps
# ============================================================
class KafkaProcessedGpsSink(MapFunction):
    """
    Publie chaque event GPS validé + enrichi sur processed.gps.
    Utilise kafka-python (déjà installé dans le Dockerfile).
    Clé = taxi_id pour garantir l'ordre par véhicule.
    """

    def open(self, runtime_context):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            # Fiabilité : attendre acknowledgement du broker
            acks=1,
            retries=3,
        )
        print("✅ [Job1] KafkaProducer → processed.gps prêt")

    def map(self, event):
        try:
            # Message processed.gps = event enrichi complet
            # (zone_id, zone_name, lat/lon anonymisés déjà appliqués)
            processed_event = {
                "taxi_id":        event.get("taxi_id"),
                "timestamp":      event.get("timestamp"),
                "timestamp_unix": event.get("timestamp_unix"),
                "lat":            event.get("lat"),          # anonymisé (centroïde)
                "lon":            event.get("lon"),          # anonymisé (centroïde)
                "zone_id":        event.get("zone_id"),
                "zone_name":      event.get("zone_name"),
                "zone_type":      event.get("zone_type"),
                "base_fare_mad":  event.get("base_fare_mad"),
                "speed":          event.get("speed", 0.0),
                "status":         event.get("status"),
                "trip_progress":  event.get("trip_progress", 0.0),
                "road_snapped":   event.get("road_snapped", False),
                # Champ ajouté pour indiquer que c'est un event normalisé
                "processed":      True,
                "processed_at":   datetime.utcnow().isoformat(),
            }
            self.producer.send(
                "processed.gps",
                key=event.get("taxi_id"),
                value=json.dumps(processed_event),
            )
            return (
                f"✅ Taxi {event.get('taxi_id')} | Zone {event.get('zone_id')} "
                f"({event.get('zone_name')}) | {event.get('status')} "
                f"→ Cassandra + processed.gps OK"
            )
        except Exception as e:
            return f"❌ [Job1] Kafka processed.gps error: {e}"

    def close(self):
        if hasattr(self, "producer"):
            self.producer.flush()
            self.producer.close()

# ============================================================
# 6. Main
# ============================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.enable_checkpointing(60000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)
    env.get_checkpoint_config().set_checkpoint_timeout(120000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    print("=" * 60)
    print("🚀 TaaSim - Flink Job 1 — GPS Normalizer")
    print("   raw.gps → validation → zone mapping → anonymisation")
    print("   → Cassandra: vehicle_positions")
    print("   → Kafka:     processed.gps  ← NOUVEAU")
    print("=" * 60)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.gps")
        .set_group_id("flink-gps-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    def timestamp_assigner(event, timestamp):
        return int(event.get("timestamp_unix", 0)) * 1000

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_timestamp_assigner(timestamp_assigner)
    )

    gps_stream = (
        env
        .from_source(kafka_source, watermark_strategy, "KafkaGPSSource")
        .map(ParseGpsEvent(),      output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(ValidEvent())
        .map(ZoneMappingFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # Pipeline dual sink :
    #   1. CassandraSinkMap  → écrit vehicle_positions, passe l'event
    #   2. KafkaProcessedGpsSink → publie sur processed.gps, retourne log string
    (
        gps_stream
        .map(CassandraSinkMap(),         output_type=Types.PICKLED_BYTE_ARRAY())
        .map(KafkaProcessedGpsSink(),    output_type=Types.STRING())
        .print()
    )

    env.execute("TaaSim - Flink Job1 - GPS Normalizer")

if __name__ == "__main__":
    main()