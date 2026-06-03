#!/usr/bin/env python3
# =============================================================================
# flink_job3_trip_matcher.py
# TaaSim — Flink Job 3 — Trip Matcher
# Semaine 4 — CONFORME AU CAHIER DES CHARGES
#
# ✅ CDC §3.3  : Consume raw.trips + processed.gps
# ✅ CDC §3.3  : KeyedCoProcessFunction — state keyed par zone_id
# ✅ CDC §3.3  : Matching : véhicule avec oldest last_seen dans la zone
# ✅ CDC §3.3  : Fallback zones adjacentes (lu depuis zone_mapping_geojson.csv)
# ✅ CDC §3.3  : ETA = distance_km / avg_speed_kmh × 3600
# ✅ CDC §3.3  : Sink Cassandra table trips
# ✅ CDC §3.3  : Sink Kafka processed.matches  (matched uniquement)
# ✅ CDC §6.1  : SLA < 5 s  — ProcessingTimeTimer émet unmatched si dépassé
# ✅ CDC §6.1  : unmatched → topic Kafka séparé  unmatched.trips
# ✅ CDC §6.2  : Checkpointing 60 s → MinIO (RocksDB backend)
# ✅ CDC §6.2  : Idempotent Cassandra write (IF NOT EXISTS)
# ✅ CDC §6.2  : Named consumer groups pour reprise depuis checkpoint
# ✅ CDC §13   : adjacent_zones lu depuis zone_mapping_geojson.csv (MinIO)
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
    ValueStateDescriptor, MapStateDescriptor,
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
ZONE_MAPPING_OBJECT = "porto-trips/zone_mapping_geojson.csv"   # chemin dans MinIO

# SLA : si aucun véhicule trouvé dans ce délai → émettre unmatched
MATCH_SLA_MS = 5_000   # 5 secondes (CDC §6.1)

# Vitesse moyenne par défaut si le GPS ne rapporte rien
DEFAULT_SPEED_KMH = 30.0


# ─────────────────────────────────────────────
# CHARGEMENT DU ZONE MAPPING DEPUIS MINIO
# ─────────────────────────────────────────────

def load_zone_mapping_from_minio():
    """
    Lit zone_mapping_geojson.csv depuis MinIO.
    Retourne deux dicts :
      adjacent_zones  : {zone_id -> [zone_id, ...]}
      zone_centroids  : {zone_id -> (centroid_lon, centroid_lat)}
    """
    from minio import Minio

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    response = client.get_object(MINIO_BUCKET, ZONE_MAPPING_OBJECT)
    content  = response.read().decode("utf-8")
    response.close()

    adjacent_zones = {}
    zone_centroids = {}
    zone_distances = {}   # {(min_id, max_id) -> km}

    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        zid = int(row["zone_id"])

        # Zones adjacentes (colonne JSON)
        adj_raw = row.get("adjacent_zones", "[]")
        neighbors = json.loads(adj_raw)
        adjacent_zones[zid] = neighbors

        # Centroïde
        zone_centroids[zid] = (
            float(row["centroid_lon"]),
            float(row["centroid_lat"]),
        )

    # Précalcul des distances entre centroïdes adjacents
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
    """Désérialise un message JSON depuis processed.gps."""

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
    """Désérialise un message JSON depuis raw.trips."""

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
    """
    Gère deux streams keyed par zone_id :
      process_element1 ← GPS events  (stream GPS keyed par zone_id du véhicule)
      process_element2 ← Trip events (stream Trip keyed par origin_zone)

    État Flink (par zone_id) :
      vehicle_state : JSON list des véhicules disponibles dans cette zone

    Conformité CDC :
      • Sélection : oldest last_seen  (CDC §9.3)
      • Fallback : zones adjacentes   (CDC §9.3)
      • SLA 5 s  : ProcessingTimeTimer → unmatched si expiré (CDC §6.1)
      • Cassandra IF NOT EXISTS       (CDC §6.2)
    """

    # ── Initialisation ──────────────────────────────────────────────

    def open(self, ctx):
        # TTL 5 min : purge automatique des taxis sans ping récent
        ttl = (StateTtlConfig
               .new_builder(Time.seconds(300))
               .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
               .set_state_visibility(
                   StateTtlConfig.StateVisibility.NeverReturnExpired)
               .build())

        desc = ValueStateDescriptor("available_vehicles", Types.STRING())
        desc.enable_time_to_live(ttl)
        self.vehicle_state = ctx.get_state(desc)

        # State pour les trips en attente de match (SLA timer)
        pending_desc = ValueStateDescriptor("pending_trips", Types.STRING())
        self.pending_state = ctx.get_state(pending_desc)

        # Charger le zone mapping (adjacent_zones + distances)
        self.adjacent_zones, self.zone_centroids, self.zone_distances = \
            load_zone_mapping_from_minio()

        print("✅ TripMatcherCoProcess initialisé")

    # ── Helpers état véhicules ───────────────────────────────────────

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

    # ── Helpers état pending trips (SLA) ────────────────────────────

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

    # ── Stream 1 : GPS ───────────────────────────────────────────────

    def process_element1(self, event, ctx):
        """Met à jour le pool de véhicules disponibles dans la zone."""
        if event.get("type") != "gps":
            return []

        if event["status"] == "available":
            self._upsert_vehicle(event)
        else:
            # Taxi en course ou hors service → retirer du pool
            self._remove_vehicle(event["taxi_id"])

        # Vérifier si un trip en attente peut maintenant être matché
        pending = self._get_pending()
        results = []
        matched_trips = []

        for trip_id, trip_info in pending.items():
            origin_zone = trip_info["origin_zone"]

            matched_vehicle = self._find_best_vehicle(origin_zone)
            matched_zone    = origin_zone
            is_fallback     = False

            if not matched_vehicle:
                for adj_zone in self.adjacent_zones.get(origin_zone, []):
                    matched_vehicle = self._find_best_vehicle(adj_zone)
                    if matched_vehicle:
                        matched_zone = adj_zone
                        is_fallback  = True
                        break

            if matched_vehicle:
                match_json = self._build_match_event(
                    trip_info, matched_vehicle, matched_zone,
                    is_fallback, trip_info["destination_zone"]
                )
                results.append(match_json)
                matched_trips.append(trip_id)
                print(f"✅ MATCH (GPS trigger): Trip {trip_id} → "
                      f"Taxi {matched_vehicle['taxi_id']} zone {matched_zone}")

        # Retirer les trips matchés du pending
        for tid in matched_trips:
            del pending[tid]
        self._set_pending(pending)

        return results

    # ── Stream 2 : TRIP ──────────────────────────────────────────────

    def process_element2(self, event, ctx):
        """
        Tente de matcher le trip immédiatement.
        Si aucun véhicule disponible → mettre en pending + armer un timer SLA.
        """
        if event.get("type") != "trip":
            return []

        origin_zone      = event["origin_zone"]
        destination_zone = event["destination_zone"]

        # 1. Tentative immédiate dans la zone demandée
        matched_vehicle = self._find_best_vehicle(origin_zone)
        matched_zone    = origin_zone
        is_fallback     = False

        # 2. Fallback zones adjacentes (CDC §9.3)
        if not matched_vehicle:
            for adj_zone in self.adjacent_zones.get(origin_zone, []):
                matched_vehicle = self._find_best_vehicle(adj_zone)
                if matched_vehicle:
                    matched_zone = adj_zone
                    is_fallback  = True
                    print(f"🔄 FALLBACK immédiat: zone {origin_zone} → zone {adj_zone}")
                    break

        if matched_vehicle:
            print(f"✅ MATCH immédiat: Trip {event['trip_id']} → "
                  f"Taxi {matched_vehicle['taxi_id']} zone {matched_zone}")
            return [self._build_match_event(
                event, matched_vehicle, matched_zone,
                is_fallback, destination_zone
            )]

        # 3. Aucun véhicule disponible → placer en pending + timer SLA (CDC §6.1)
        pending = self._get_pending()
        pending[event["trip_id"]] = event
        self._set_pending(pending)

        # Timer processing-time dans MATCH_SLA_MS ms
        fire_time = ctx.timer_service().current_processing_time() + MATCH_SLA_MS
        ctx.timer_service().register_processing_time_timer(fire_time)

        print(f"⏳ PENDING: Trip {event['trip_id']} — SLA timer armé ({MATCH_SLA_MS} ms)")
        return []

    # ── Timer SLA (CDC §6.1) ─────────────────────────────────────────

    def on_timer(self, timestamp, ctx):
        """
        Déclenché après MATCH_SLA_MS.
        Émet un événement unmatched pour chaque trip encore en attente.
        """
        pending = self._get_pending()
        if not pending:
            return []

        results = []
        for trip_id, trip_info in pending.items():
            unmatch_json = self._build_unmatch_event(trip_info)
            results.append(unmatch_json)
            print(f"❌ UNMATCH SLA: Trip {trip_id} — aucun véhicule en {MATCH_SLA_MS} ms")

        # Vider le pending
        self._set_pending({})
        return results

    # ── Sélection du meilleur véhicule (CDC §9.3 : oldest last_seen) ─

    def _find_best_vehicle(self, zone_id):
        """
        Retourne le véhicule disponible dans zone_id ayant le
        timestamp_unix le plus ancien (le plus longtemps en attente).
        Le véhicule choisi est retiré du pool.
        """
        vehicles      = self._get_vehicles()
        zone_vehicles = [v for v in vehicles if v["zone_id"] == zone_id]
        if not zone_vehicles:
            return None

        # CDC §9.3 : oldest last_seen  →  min(timestamp_unix)
        best = min(zone_vehicles, key=lambda v: v.get("timestamp_unix", 0))
        self._remove_vehicle(best["taxi_id"])
        return best

    # ── Calcul ETA ───────────────────────────────────────────────────

    def _get_distance_km(self, zone_a, zone_b):
        """Distance entre deux zones (centroïdes Haversine)."""
        if zone_a == zone_b:
            return 2.0   # intra-zone ≈ 2 km
        key = (min(zone_a, zone_b), max(zone_a, zone_b))
        return self.zone_distances.get(key, 5.0)

    def _compute_eta(self, distance_km, speed_kmh):
        if speed_kmh <= 0:
            speed_kmh = DEFAULT_SPEED_KMH
        # ETA = distance / vitesse  (en heures) → secondes
        return int(min((distance_km / speed_kmh) * 3600, 1800))

    # ── Construction des événements ──────────────────────────────────

    def _build_match_event(self, trip, vehicle, matched_zone,
                           is_fallback, dest_zone):
        now = datetime.now(timezone.utc)
        dist_km     = self._get_distance_km(matched_zone, dest_zone)
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
            req = datetime.fromisoformat(
                requested_at_str.replace("Z", "+00:00"))
            return int((datetime.now(timezone.utc) - req).total_seconds() * 1000)
        except Exception:
            return 0


# ─────────────────────────────────────────────
# 3. SINKS
# ─────────────────────────────────────────────

class KafkaMatchSink(MapFunction):
    """
    Publie les événements matched → processed.matches
                        unmatched → unmatched.trips   (CDC §6.1)
    """

    def open(self, ctx):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks=1,
            retries=3,
        )
        print("✅ KafkaMatchSink prêt → processed.matches | unmatched.trips")

    def map(self, s):
        try:
            rec = json.loads(s)
            status = rec.get("status")

            if status == "matched":
                # CDC §3.3 : matched → processed.matches
                self.producer.send(
                    "processed.matches",
                    key=str(rec["trip_id"]),
                    value=s,
                )

            elif status == "unmatched":
                # CDC §6.1 : unmatched → topic séparé pour monitoring
                self.producer.send(
                    "unmatched.trips",
                    key=str(rec["trip_id"]),
                    value=s,
                )

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
    """
    Écrit les matchs dans la table Cassandra trips.
    Utilise IF NOT EXISTS pour l'idempotence (CDC §6.2).
    """

    def open(self, ctx):
        from cassandra.cluster import Cluster
        cluster      = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = cluster.connect(KEYSPACE)

        # IF NOT EXISTS → idempotent  (CDC §6.2)
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
        print("✅ CassandraTripSink prêt → trips (IF NOT EXISTS)")

    def map(self, s):
        try:
            r = json.loads(s)
            if r.get("status") != "matched":
                return s

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
                r.get("call_type", "B"),
                r["status"],
                float(r.get("estimated_fare_mad", 0.0)),
                int(r.get("match_latency_ms", 0)),
                datetime.fromisoformat(
                    r["requested_at"].replace("Z", "+00:00")),
                datetime.fromisoformat(
                    r["matched_at"].replace("Z", "+00:00")),
            ))
            print(f"💾 Cassandra: Trip {r['trip_id']} écrit (latency "
                  f"{r.get('match_latency_ms', '?')} ms)")
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
    """Archive les match/unmatch events dans MinIO (CDC §4.2)."""

    def open(self, ctx):
        from minio import Minio
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        self.bucket    = MINIO_BUCKET
        self.buffer    = []
        self.flush_at  = 100
        self.batch_id  = 0
        print("✅ MinIOArchiveSink prêt → raw/kafka-archive/processed.matches/")

    def map(self, s):
        self.buffer.append(s)
        if len(self.buffer) >= self.flush_at:
            self._flush()
        return s

    def _flush(self):
        if not self.buffer:
            return
        import io as _io
        now  = datetime.now(timezone.utc)
        path = (f"kafka-archive/processed.matches/"
                f"{now.strftime('%Y/%m/%d')}/batch_{self.batch_id:06d}.jsonl")
        data = "\n".join(self.buffer).encode("utf-8")
        self.client.put_object(
            self.bucket, path,
            _io.BytesIO(data), len(data),
            content_type="application/jsonl",
        )
        print(f"💾 MinIO: {len(self.buffer)} events → {path}")
        self.buffer   = []
        self.batch_id += 1

    def close(self):
        self._flush()


# ─────────────────────────────────────────────
# 4. MAIN
# ─────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # ── Checkpointing CDC §6.2 ──────────────────────────────────────
    env.enable_checkpointing(60_000)   # toutes les 60 secondes
    env.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)

    # RocksDB state backend (CDC §W4 deliverable)
    from pyflink.datastream.state_backend import RocksDBStateBackend
    env.set_state_backend(
        RocksDBStateBackend(
            f"s3a://{MINIO_BUCKET}/flink-checkpoints/job3",
            incremental_checkpointing=True,
        )
    )

    env.get_config().set_auto_watermark_interval(100)

    print("=" * 70)
    print("🚀 TaaSim — Flink Job 3 — Trip Matcher (CDC COMPLIANT)")
    print(f"   ✅ Sources     : raw.trips + processed.gps")
    print(f"   ✅ State       : KeyedCoProcessFunction par zone_id")
    print(f"   ✅ Matching    : oldest last_seen (CDC §9.3)")
    print(f"   ✅ Fallback    : zones adjacentes depuis MinIO CSV")
    print(f"   ✅ SLA         : {MATCH_SLA_MS} ms → unmatched.trips (CDC §6.1)")
    print(f"   ✅ Idempotence : Cassandra IF NOT EXISTS (CDC §6.2)")
    print(f"   ✅ Checkpoint  : 60 s → MinIO RocksDB (CDC §6.2)")
    print("=" * 70)

    # ── Sources Kafka ───────────────────────────────────────────────

    # CDC §6.2 : named consumer groups pour reprise depuis checkpoint
    gps_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("processed.gps")
        .set_group_id("flink-job3-gps")          # named group CDC §6.2
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    trip_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.trips")
        .set_group_id("flink-job3-trips")         # named group CDC §6.2
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Streams parsés & filtrés ────────────────────────────────────

    gps_stream = (
        env.from_source(gps_source, WatermarkStrategy.no_watermarks(), "GPS-Source")
        .map(ParseGPSPosition())
        .filter(NotNone())
    )

    trip_stream = (
        env.from_source(trip_source, WatermarkStrategy.no_watermarks(), "Trip-Source")
        .map(ParseTripRequest())
        .filter(NotNone())
    )

    # ── Keying par zone_id ──────────────────────────────────────────

    gps_keyed  = gps_stream.key_by(lambda e: e["zone_id"])
    trip_keyed = trip_stream.key_by(lambda e: e["origin_zone"])

    # ── CoProcess : matching stateful ──────────────────────────────

    connected    = gps_keyed.connect(trip_keyed)
    match_stream = connected.process(
        TripMatcherCoProcess(),
        output_type=Types.STRING(),
    )

    # ── Sinks ───────────────────────────────────────────────────────

    match_stream = match_stream.map(KafkaMatchSink(),     output_type=Types.STRING())
    match_stream = match_stream.map(CassandraTripSink(),  output_type=Types.STRING())
    match_stream = match_stream.map(MinIOArchiveSink(),   output_type=Types.STRING())
    match_stream.print()

    print("▶️  Démarrage Job 3...")
    env.execute("TaaSim — Job3 — Trip Matcher")


if __name__ == "__main__":
    main()
