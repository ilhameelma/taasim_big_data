# flink_job1_gps_normalizer.py
# Semaine 3 — Tâche 5 : Sink Cassandra via MapFunction

import json
import csv
import os
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode

CASA_LON_MIN = -7.78
CASA_LON_MAX = -7.40
CASA_LAT_MIN = 33.48
CASA_LAT_MAX = 33.70
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "taasim"

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
        required = ["taxi_id", "timestamp_unix", "lat", "lon", "status"]
        if not all(k in event for k in required):
            return False
        return is_in_casablanca_bbox(event.get("lon"), event.get("lat"))

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
            {"zone_id": 1, "zone_name": "Mechouar", "zone_type": "residential",
             "base_fare_mad": 8.0, "bbox": (-7.61, -7.59, 33.57, 33.59),
             "centroid_lon": -7.60, "centroid_lat": 33.58},
            {"zone_id": 2, "zone_name": "Anfa", "zone_type": "commercial",
             "base_fare_mad": 10.0, "bbox": (-7.68, -7.64, 33.55, 33.58),
             "centroid_lon": -7.66, "centroid_lat": 33.565},
            {"zone_id": 3, "zone_name": "Hay Hassani", "zone_type": "commercial",
             "base_fare_mad": 10.0, "bbox": (-7.67, -7.64, 33.54, 33.56),
             "centroid_lon": -7.655, "centroid_lat": 33.55},
            {"zone_id": 4, "zone_name": "Sidi Bernoussi", "zone_type": "residential",
             "base_fare_mad": 8.0, "bbox": (-7.55, -7.53, 33.58, 33.60),
             "centroid_lon": -7.54, "centroid_lat": 33.59},
            {"zone_id": 5, "zone_name": "Moulay Rachid", "zone_type": "residential",
             "base_fare_mad": 8.0, "bbox": (-7.60, -7.57, 33.54, 33.56),
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
        zone = self.get_zone(lon, lat)
        if zone:
            event["zone_id"] = zone["zone_id"]
            event["zone_name"] = zone["zone_name"]
            event["zone_type"] = zone["zone_type"]
            event["base_fare_mad"] = zone["base_fare_mad"]
            event["lat"] = zone["centroid_lat"]   # 🔒 anonymisé
            event["lon"] = zone["centroid_lon"]   # 🔒 anonymisé
        else:
            event["zone_id"] = 0
            event["zone_name"] = "Inconnu"
            event["zone_type"] = "unknown"
            event["base_fare_mad"] = 8.0
            event["lat"] = 33.5731
            event["lon"] = -7.5898
        return event

# ============================================================
# 4. Sink Cassandra — via MapFunction (correct en PyFlink)
# ============================================================
class CassandraSinkMap(MapFunction):
    """
    En PyFlink, le sink custom se fait avec MapFunction + open/close.
    add_sink(SinkFunction) ne supporte pas les classes Python custom.
    """
    def __init__(self):
        self.session = None
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
        print("✅ Cassandra connecté — vehicle_positions prêt")

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
            return (
                f"✅ Taxi {event['taxi_id']} | Zone {event.get('zone_id')} "
                f"({event.get('zone_name')}) | {event.get('status')} "
                f"→ Cassandra OK"
            )
        except Exception as e:
            return f"❌ Erreur Cassandra taxi {event.get('taxi_id')}: {e}"

    def close(self):
        if self.session:
            self.session.shutdown()

# ============================================================
# 5. Main
# ============================================================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
   

    # ── Tâche 6 : Checkpointing toutes les 60 secondes ──
    env.enable_checkpointing(60000)  # 60 000 ms = 60 secondes
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)   # 30s min entre deux checkpoints
    env.get_checkpoint_config().set_checkpoint_timeout(120000)             # timeout 2 minutes
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)          # un seul checkpoint à la fois         # un seul checkpoint à la fois

    print("=" * 60)
    print("🚀 TaaSim - Flink Job 1 — GPS → Cassandra")
    print("   Kafka raw.gps → validation → anonymisation → Cassandra")
    print("=" * 60)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
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
        .map(ParseGpsEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(ValidEvent())
        .map(ZoneMappingFunction())
    )

    # Sink Cassandra + log dans les logs Flink
    gps_stream.map(CassandraSinkMap(), output_type=Types.STRING()).print()

    env.execute("TaaSim - Flink Job1 - GPS → Cassandra")

if __name__ == "__main__":
    main()