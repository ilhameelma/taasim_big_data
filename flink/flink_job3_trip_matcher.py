#!/usr/bin/env python3
# =============================================================================
# flink_job3_trip_matcher.py
# TaaSim — Flink Job 3 — Trip Matcher
# =============================================================================

import csv
import io
import json
import math
import os
import uuid
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.functions import (
    MapFunction, FilterFunction, KeyedCoProcessFunction,
)
from pyflink.datastream.state import (
    ValueStateDescriptor,
    StateTtlConfig, Time,
)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

KAFKA_BROKERS    = os.getenv("KAFKA_BROKERS",    "kafka:9092")
CASSANDRA_HOST   = os.getenv("CASSANDRA_HOST",   "cassandra")
CASSANDRA_PORT   = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE         = "taasim"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "taasim")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "taasim123")
MINIO_BUCKET     = "raw"
ZONE_MAPPING_OBJECT = "porto-trips/zone_mapping_geojson.csv"

MATCH_SLA_MS = 5_000
DEFAULT_SPEED_KMH = 30.0


# ─────────────────────────────────────────────
# CHARGEMENT DU ZONE MAPPING DEPUIS MINIO
# ─────────────────────────────────────────────

def load_zone_mapping_from_minio():
    """Lit zone_mapping_geojson.csv depuis MinIO en utilisant boto3."""
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        use_ssl=False
    )

    response = s3.get_object(Bucket=MINIO_BUCKET, Key=ZONE_MAPPING_OBJECT)
    content = response['Body'].read().decode('utf-8')

    adjacent_zones = {}
    zone_centroids = {}
    zone_distances = {}

    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        zid = int(row["zone_id"])
        adj_raw = row.get("adjacent_zones", "[]")
        neighbors = json.loads(adj_raw)
        adjacent_zones[zid] = neighbors
        zone_centroids[zid] = (
            float(row["centroid_lon"]),
            float(row["centroid_lat"]),
        )

    for zid, neighbors in adjacent_zones.items():
        lon1, lat1 = zone_centroids[zid]
        for nid in neighbors:
            key = (min(zid, nid), max(zid, nid))
            if key not in zone_distances:
                lon2, lat2 = zone_centroids[nid]
                zone_distances[key] = _haversine(lon1, lat1, lon2, lat2)

    print(f"✅ Zone mapping chargé : {len(adjacent_zones)} zones, "
          f"{len(zone_distances)} paires de distances")
    return adjacent_zones, zone_centroids, zone_distances


def _haversine(lon1, lat1, lon2, lat2):
    R = 6371.0
    dlon = math.radians(lon2 - lon1)
    dlat = math.radians(lat2 - lat1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return round(R * 2 * math.asin(math.sqrt(a)), 2)


# ─────────────────────────────────────────────
# 1. PARSERS
# ─────────────────────────────────────────────

class ParseGPSPosition(MapFunction):
    def map(self, raw_str):
        try:
            e = json.loads(raw_str)
            return {
                "type":           "gps",
                "taxi_id":        int(e.get("taxi_id", 0)),
                "zone_id":        int(e.get("zone_id", 0)),
                "lat":            float(e.get("lat", 0.0)),
                "lon":            float(e.get("lon", 0.0)),
                "speed":          float(e.get("speed", 0.0)),
                "status":         e.get("status", "unknown"),
                "timestamp":      e.get("timestamp"),
                "timestamp_unix": int(e.get("timestamp_unix", 0)),
            }
        except Exception as ex:
            print(f"❌ ParseGPSPosition: {ex}")
            return None


class ParseTripRequest(MapFunction):
    def map(self, raw_str):
        try:
            e = json.loads(raw_str)
            return {
                "type":                "trip",
                "trip_id":             e.get("trip_id", str(uuid.uuid4())),
                "rider_id":            e.get("rider_id"),
                "origin_zone":         int(e.get("origin_zone", 0)),
                "destination_zone":    int(e.get("destination_zone", 0)),
                "requested_at":        e.get("requested_at"),
                "call_type":           e.get("call_type", "B"),
                "passenger_count":     int(e.get("passenger_count", 1)),
                "estimated_fare_mad":  float(e.get("estimated_fare_mad", 0.0)),
            }
        except Exception as ex:
            print(f"❌ ParseTripRequest: {ex}")
            return None


class NotNone(FilterFunction):
    def filter(self, v):
        return v is not None


# ─────────────────────────────────────────────
# 2. CORE — KeyedCoProcessFunction
# ─────────────────────────────────────────────

class TripMatcherCoProcess(KeyedCoProcessFunction):

    def open(self, ctx):
        ttl = (StateTtlConfig
               .new_builder(Time.seconds(300))
               .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
               .set_state_visibility(
                   StateTtlConfig.StateVisibility.NeverReturnExpired)
               .build())

        desc = ValueStateDescriptor("available_vehicles", Types.STRING())
        desc.enable_time_to_live(ttl)
        self.vehicle_state = ctx.get_state(desc)

        pending_desc = ValueStateDescriptor("pending_trips", Types.STRING())
        self.pending_state = ctx.get_state(pending_desc)

        self.adjacent_zones, self.zone_centroids, self.zone_distances = \
            load_zone_mapping_from_minio()
<<<<<<< HEAD
        
        print(f"🔍 Zones adjacentes chargées: {list(self.adjacent_zones.keys())[:5]}...")
        print(f"🔍 Zone 2 adjacente à: {self.adjacent_zones.get(2, [])}")
        print(f"🔍 Zone 1 adjacente à: {self.adjacent_zones.get(1, [])}")
=======
>>>>>>> 90a31376a6591c75d804411da3621743e99542a1

        print("✅ TripMatcherCoProcess initialisé")

    # ── Helpers ───────────────────────────────────────────────────

    def _get_vehicles(self):
        raw = self.vehicle_state.value()
        if not raw:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def _set_vehicles(self, vehicles):
        self.vehicle_state.update(json.dumps(vehicles))

    def _remove_vehicle(self, taxi_id):
        self._set_vehicles(
            [v for v in self._get_vehicles() if v["taxi_id"] != taxi_id]
        )

    def _upsert_vehicle(self, gps_event):
        vehicles = self._get_vehicles()
        for i, v in enumerate(vehicles):
            if v["taxi_id"] == gps_event["taxi_id"]:
                vehicles[i] = gps_event
                self._set_vehicles(vehicles)
                return
        vehicles.append(gps_event)
        self._set_vehicles(vehicles)

    def _get_pending(self):
        raw = self.pending_state.value()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def _set_pending(self, pending):
        self.pending_state.update(json.dumps(pending))

    # ── Stream 1 : GPS ────────────────────────────────────────────────
    
    def process_element1(self, value, ctx):
        if value.get("type") != "gps":
            return

        if value["status"] == "available":
            self._upsert_vehicle(value)
        else:
            self._remove_vehicle(value["taxi_id"])

        pending = self._get_pending()
        matched_trips = []

        for trip_id, trip_info in list(pending.items()):
            origin_zone = trip_info["origin_zone"]
            matched_vehicle = self._find_best_vehicle(origin_zone)
            matched_zone = origin_zone
            is_fallback = False

            if not matched_vehicle:
                for adj_zone in self.adjacent_zones.get(origin_zone, []):
                    matched_vehicle = self._find_best_vehicle(adj_zone)
                    if matched_vehicle:
                        matched_zone = adj_zone
                        is_fallback = True
                        break

            if matched_vehicle:
                yield self._build_match_event(
                    trip_info, matched_vehicle, matched_zone,
                    is_fallback, trip_info["destination_zone"]
                )
                matched_trips.append(trip_id)

        for tid in matched_trips:
            del pending[tid]
        self._set_pending(pending)

    def process_element2(self, value, ctx):
        if value.get("type") != "trip":
            return

        origin_zone = value["origin_zone"]
        destination_zone = value["destination_zone"]

        matched_vehicle = self._find_best_vehicle(origin_zone)
        matched_zone = origin_zone
        is_fallback = False

        if not matched_vehicle:
            for adj_zone in self.adjacent_zones.get(origin_zone, []):
                matched_vehicle = self._find_best_vehicle(adj_zone)
                if matched_vehicle:
                    matched_zone = adj_zone
                    is_fallback = True
                    break

        if matched_vehicle:
            yield self._build_match_event(
                value, matched_vehicle, matched_zone,
                is_fallback, destination_zone
            )
            return

        # No vehicle → pending + SLA timer
        pending = self._get_pending()
        pending[value["trip_id"]] = value
        self._set_pending(pending)

        fire_time = ctx.timer_service().current_processing_time() + MATCH_SLA_MS
        ctx.timer_service().register_processing_time_timer(fire_time)

    def on_timer(self, timestamp, ctx):
        pending = self._get_pending()
        if not pending:
            return

        for trip_id, trip_info in pending.items():
            yield self._build_unmatch_event(trip_info)

        self._set_pending({})

    # ── Sélection du meilleur véhicule ────────────────────────────

    def _find_best_vehicle(self, zone_id):
        vehicles = self._get_vehicles()
        zone_vehicles = [v for v in vehicles if v["zone_id"] == zone_id]
        if not zone_vehicles:
            return None

        best = min(zone_vehicles, key=lambda v: v.get("timestamp_unix", 0))
        self._remove_vehicle(best["taxi_id"])
        return best

    # ── Calcul ETA ─────────────────────────────────────────────────

    def _get_distance_km(self, zone_a, zone_b):
        if zone_a == zone_b:
            return 2.0
        key = (min(zone_a, zone_b), max(zone_a, zone_b))
        return self.zone_distances.get(key, 5.0)

    def _compute_eta(self, distance_km, speed_kmh):
        if speed_kmh <= 0:
            speed_kmh = DEFAULT_SPEED_KMH
        return int(min((distance_km / speed_kmh) * 3600, 1800))

    # ── Construction des événements ────────────────────────────────

    def _build_match_event(self, trip, vehicle, matched_zone,
                           is_fallback, dest_zone):
        now = datetime.now(timezone.utc)
        dist_km = self._get_distance_km(matched_zone, dest_zone)
        eta_seconds = self._compute_eta(dist_km, vehicle.get("speed", DEFAULT_SPEED_KMH))

        event = {
            "status":              "matched",
            "trip_id":             trip["trip_id"],
            "rider_id":            trip["rider_id"],
            "taxi_id":             vehicle["taxi_id"],
            "origin_zone":         trip["origin_zone"],
            "matched_zone":        matched_zone,
            "destination_zone":    dest_zone,
            "is_fallback":         is_fallback,
            "distance_km":         round(dist_km, 2),
            "eta_seconds":         eta_seconds,
            "estimated_fare_mad":  trip.get("estimated_fare_mad", 0.0),
            "requested_at":        trip["requested_at"],
            "matched_at":          now.isoformat(),
            "match_latency_ms":    self._latency_ms(trip["requested_at"]),
        }
        return json.dumps(event)

    def _build_unmatch_event(self, trip):
        now = datetime.now(timezone.utc)
        event = {
            "status":           "unmatched",
            "trip_id":          trip["trip_id"],
            "rider_id":         trip["rider_id"],
            "origin_zone":      trip["origin_zone"],
            "destination_zone": trip["destination_zone"],
            "reason":           "no_available_vehicle_within_sla",
            "sla_ms":           MATCH_SLA_MS,
            "requested_at":     trip["requested_at"],
            "unmatched_at":     now.isoformat(),
        }
        return json.dumps(event)

    def _latency_ms(self, requested_at_str):
        try:
<<<<<<< HEAD
            # Nettoyer la chaîne : remplacer 'Z' si présent, sinon ajouter '+00:00'
            clean_str = requested_at_str.replace('Z', '+00:00')
            if 'Z' not in requested_at_str and '+' not in requested_at_str:
               clean_str = requested_at_str + '+00:00'
            req = datetime.fromisoformat(clean_str)
            now = datetime.now(timezone.utc)
            latency_ms = int((now - req).total_seconds() * 1000)
            print(f"📊 Latence calculée: {latency_ms} ms")  # Pour debug
            return latency_ms
        except Exception as e:
            print(f"❌ Erreur calcul latence: {e}")  # Affiche l'erreur au lieu de silence
=======
            req = datetime.fromisoformat(requested_at_str.replace("Z", "+00:00"))
            return int((datetime.now(timezone.utc) - req).total_seconds() * 1000)
        except Exception:
>>>>>>> 90a31376a6591c75d804411da3621743e99542a1
            return 0


# ─────────────────────────────────────────────
# 3. SINKS
# ─────────────────────────────────────────────

class KafkaMatchSink(MapFunction):
    def open(self, ctx):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks=1,
            retries=3,
        )
        print("✅ KafkaMatchSink prêt")

    def map(self, s):
        try:
            rec = json.loads(s)
            status = rec.get("status")
            if status == "matched":
                self.producer.send("processed.matches", key=str(rec["trip_id"]), value=s)
            elif status == "unmatched":
                self.producer.send("unmatched.trips", key=str(rec["trip_id"]), value=s)
        except Exception as e:
            print(f"❌ KafkaMatchSink: {e}")
        return s

    def close(self):
        if hasattr(self, "producer"):
            try:
                self.producer.flush()
                self.producer.close()
            except Exception:
                pass


class CassandraTripSink(MapFunction):
    def open(self, ctx):
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = cluster.connect(KEYSPACE)
        self.stmt = self.session.prepare("""
            INSERT INTO trips (
                city, date_bucket, created_at,
                trip_id, rider_id, taxi_id,
                origin_zone, dest_zone,
                call_type, status,
                estimated_fare, match_latency_ms,
                requested_at, matched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            IF NOT EXISTS
        """)
        print("✅ CassandraTripSink prêt")

    def map(self, s):
        try:
            r = json.loads(s)
            if r.get("status") != "matched":
                return s
            date_bucket = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            self.session.execute(self.stmt, (
                "Casablanca", date_bucket, datetime.now(timezone.utc),
                r["trip_id"], r["rider_id"], int(r["taxi_id"]),
                int(r["origin_zone"]), int(r["destination_zone"]),
                r.get("call_type", "B"), r["status"],
                float(r.get("estimated_fare_mad", 0.0)),
                int(r.get("match_latency_ms", 0)),
                datetime.fromisoformat(r["requested_at"].replace("Z", "+00:00")),
                datetime.fromisoformat(r["matched_at"].replace("Z", "+00:00")),
            ))
            print(f"💾 Cassandra: Trip {r['trip_id']} écrit")
        except Exception as e:
            print(f"❌ CassandraTripSink: {e}")
        return s

    def close(self):
        if hasattr(self, "session") and self.session:
            try:
                self.session.shutdown()
            except Exception:
                pass


class MinIOArchiveSink(MapFunction):
    def open(self, ctx):
        import boto3
        from botocore.client import Config
        self.s3 = boto3.client(
            's3',
            endpoint_url=f'http://{MINIO_ENDPOINT}',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            use_ssl=False
        )
        self.bucket = MINIO_BUCKET
        self.buffer = []
        self.flush_at = 100
        self.batch_id = 0
        print("✅ MinIOArchiveSink prêt")

    def map(self, s):
        self.buffer.append(s)
        if len(self.buffer) >= self.flush_at:
            self._flush()
        return s

    def _flush(self):
        if not self.buffer:
            return
        import io as _io
        now = datetime.now(timezone.utc)
        path = (f"kafka-archive/processed.matches/"
                f"{now.strftime('%Y/%m/%d')}/batch_{self.batch_id:06d}.jsonl")
        data = "\n".join(self.buffer).encode("utf-8")
        self.s3.put_object(
            Bucket=self.bucket, Key=path,
            Body=_io.BytesIO(data), ContentType="application/jsonl"
        )
        print(f"💾 MinIO: {len(self.buffer)} events → {path}")
        self.buffer = []
        self.batch_id += 1

    def close(self):
        self._flush()


# ─────────────────────────────────────────────
# 4. MAIN
# ─────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.enable_checkpointing(60_000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_config().set_auto_watermark_interval(100)

    print("🚀 TaaSim — Flink Job 3 — Trip Matcher")
    print(f"   SLA: {MATCH_SLA_MS} ms")

    gps_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("processed.gps")
        .set_group_id("flink-job3-gps")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    trip_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.trips")
        .set_group_id("flink-job3-trips")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    gps_stream = (env.from_source(gps_source, WatermarkStrategy.no_watermarks(), "GPS-Source")
        .map(ParseGPSPosition()).filter(NotNone()))
    trip_stream = (env.from_source(trip_source, WatermarkStrategy.no_watermarks(), "Trip-Source")
        .map(ParseTripRequest()).filter(NotNone()))

    gps_keyed = gps_stream.key_by(lambda e: e["zone_id"])
    trip_keyed = trip_stream.key_by(lambda e: e["origin_zone"])

    connected = gps_keyed.connect(trip_keyed)
    match_stream = connected.process(TripMatcherCoProcess(), output_type=Types.STRING())

    match_stream = match_stream.map(KafkaMatchSink(), output_type=Types.STRING())
    match_stream = match_stream.map(CassandraTripSink(), output_type=Types.STRING())
    match_stream = match_stream.map(MinIOArchiveSink(), output_type=Types.STRING())
    match_stream.print()

    env.execute("TaaSim — Job3 — Trip Matcher")


if __name__ == "__main__":
    main()