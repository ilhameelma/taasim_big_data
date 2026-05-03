#!/usr/bin/env python3
# flink_job2_demand_aggregator_complete.py
# Semaine 4 — VERSION COMPLÈTE AVEC GPS + TRIP REQUESTS
# FIX: Correction des timestamps pour les trips

import json, os, time
from collections import defaultdict
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.datastream.functions import MapFunction, FilterFunction, FlatMapFunction

# Configuration
CASA_LON_MIN, CASA_LON_MAX = -7.78, -7.40
CASA_LAT_MIN, CASA_LAT_MAX = 33.48, 33.70
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE       = "taasim"
KAFKA_BROKERS  = os.getenv("KAFKA_BROKERS", "kafka:9092")
WINDOW_SECONDS = 30
WATERMARK_MAX_LATENESS_SECONDS = 180


# ═══════════════════════════════════════════════════════════════
#  1. PARSER GPS (raw.gps)
# ═══════════════════════════════════════════════════════════════
class ParseGPS(MapFunction):
    def open(self, ctx):
        self._in = 0
        self._ok = 0
        self._bad = 0

    def map(self, raw):
        self._in += 1
        try:
            e = json.loads(raw)
            if not all(k in e for k in ["zone_id", "timestamp_unix", "status"]):
                self._bad += 1
                return None
            
            self._ok += 1
            if self._in % 100 == 0:
                print(f"GPS: in={self._in} ok={self._ok} bad={self._bad}")
            
            return (
                "gps",                                    # 0: type
                int(e.get("zone_id", 0)),                # 1: zone_id
                int(e.get("timestamp_unix", 0)) * 1000,  # 2: event_time_ms
                1 if e.get("status") == "moving" else 0, # 3: moving
                1 if e.get("status") == "available" else 0, # 4: available
                0,                                       # 5: pending_request
                str(e.get("zone_name", "Inconnu")),      # 6: zone_name
                str(e.get("zone_type", "unknown")),      # 7: zone_type
            )
        except Exception:
            self._bad += 1
            return None


# ═══════════════════════════════════════════════════════════════
#  2. PARSER TRIP REQUEST (raw.trips) - AVEC CORRECTION TIMESTAMP
# ═══════════════════════════════════════════════════════════════
class ParseTrip(MapFunction):
    def open(self, ctx):
        self._in = 0
        self._ok = 0
        self._bad = 0

    def map(self, raw):
        self._in += 1
        try:
            e = json.loads(raw)
            if not all(k in e for k in ["trip_id", "origin_zone", "requested_at"]):
                self._bad += 1
                return None
            
            self._ok += 1
            if self._in % 100 == 0:
                print(f"TRIP: in={self._in} ok={self._ok} bad={self._bad}")
            
            # 🔧 CORRECTION : Utiliser le timestamp actuel pour les trips
            # pour qu'ils soient dans la même fenêtre que les GPS
            current_time_ms = int(time.time()) * 1000
            
            return (
                "trip",                                   # 0: type
                int(e.get("origin_zone", 0)),            # 1: zone_id
                current_time_ms,                         # 2: event_time_ms (FORCÉ à maintenant)
                0,                                       # 3: moving
                0,                                       # 4: available
                1,                                       # 5: pending_request
                str(e.get("origin_zone_name", "")),      # 6: zone_name
                str(e.get("origin_zone_type", "")),      # 7: zone_type
            )
        except Exception:
            self._bad += 1
            return None


class NotNone(FilterFunction):
    def filter(self, v):
        return v is not None


# ═══════════════════════════════════════════════════════════════
#  3. Extract timestamp pour watermark
# ═══════════════════════════════════════════════════════════════
def extract_timestamp(value, timestamp):
    return value[2]  # event_time_ms


# ═══════════════════════════════════════════════════════════════
#  4. Union + Fenêtre tumbling 30s par zone
# ═══════════════════════════════════════════════════════════════
class UnifiedWindowAggregator(FlatMapFunction):
    def open(self, ctx):
        self._zones = defaultdict(lambda: {
            "moving": 0,
            "available": 0,
            "pending": 0,
            "n_gps": 0,
            "n_trips": 0,
            "name": "Inconnu",
            "zone_type": "unknown",
            "min_ts": None,
            "max_ts": None
        })
        self._last_window = None

    def flat_map(self, value):
        try:
            event_type = value[0]
            zone_id = value[1]
            event_time_ms = value[2]
            moving = value[3]
            available = value[4]
            pending = value[5]
            zone_name = value[6]
            zone_type = value[7]
            
            z = self._zones[zone_id]
            
            if event_type == "gps":
                z["moving"] += moving
                z["available"] += available
                z["n_gps"] += 1
                if zone_name != "Inconnu" and z["name"] == "Inconnu":
                    z["name"] = zone_name
                if zone_type != "unknown" and z["zone_type"] == "unknown":
                    z["zone_type"] = zone_type
            else:  # trip
                z["pending"] += pending
                z["n_trips"] += 1
                print(f"🔔 TRIP reçu pour zone {zone_id} ! pending={z['pending']}")
            
            ts_sec = event_time_ms // 1000
            if z["min_ts"] is None or ts_sec < z["min_ts"]:
                z["min_ts"] = ts_sec
            if z["max_ts"] is None or ts_sec > z["max_ts"]:
                z["max_ts"] = ts_sec
                
        except Exception as e:
            print(f"AGG ERR: {e}")
            return

        current_window = (event_time_ms // 1000) // WINDOW_SECONDS
        
        if self._last_window is None:
            self._last_window = current_window
        
        if current_window != self._last_window:
            for zid, z in self._zones.items():
                if z["n_gps"] == 0 and z["n_trips"] == 0:
                    continue
                
                win_unix = (self._last_window * WINDOW_SECONDS)
                win_str = datetime.fromtimestamp(win_unix, tz=timezone.utc).isoformat()
                
                supply_demand_ratio = round(z["pending"] / max(z["moving"], 1), 3)
                score = round(z["moving"] + 0.5 * z["available"], 2)
                
                print(f"📊 WINDOW zone={zid} ({z['name']}) | "
                      f"moving={z['moving']} pending={z['pending']} | "
                      f"ratio={supply_demand_ratio} | trips={z['n_trips']}")
                
                yield json.dumps({
                    "city":                "Casablanca",
                    "zone_id":             zid,
                    "zone_name":           z["name"],
                    "zone_type":           z["zone_type"],
                    "window_start":        win_str,
                    "window_start_unix":   win_unix,
                    "active_vehicles":     z["moving"],
                    "pending_requests":    z["pending"],
                    "completed_trips":     0,
                    "supply_demand_ratio": supply_demand_ratio,
                    "forecast_demand":     0.0,
                    "avg_wait_time_sec":   0.0,
                    "demand_score":        score,
                    "total_gps_events":    z["n_gps"],
                    "total_trip_events":   z["n_trips"],
                })
            
            self._zones = defaultdict(lambda: {
                "moving": 0, "available": 0, "pending": 0,
                "n_gps": 0, "n_trips": 0,
                "name": "Inconnu", "zone_type": "unknown",
                "min_ts": None, "max_ts": None
            })
            self._last_window = current_window


# ═══════════════════════════════════════════════════════════════
#  5. Sink Kafka → processed.demand
# ═══════════════════════════════════════════════════════════════
class KafkaDemandSink(MapFunction):
    def open(self, ctx):
        from kafka import KafkaProducer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )
        print("✅ KAFKA SINK READY → processed.demand")

    def map(self, s):
        try:
            rec = json.loads(s)
            self.producer.send("processed.demand", key=str(rec["zone_id"]), value=s)
        except Exception as e:
            print(f"KAFKA ERR: {e}")
        return s

    def close(self):
        if hasattr(self, "producer"):
            try: 
                self.producer.flush()
                self.producer.close()
            except: pass


# ═══════════════════════════════════════════════════════════════
#  6. Sink Cassandra → demand_zones
# ═══════════════════════════════════════════════════════════════
class CassandraDemandSink(MapFunction):
    def open(self, ctx):
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = cluster.connect(KEYSPACE)
        self.stmt = self.session.prepare("""
            INSERT INTO demand_zones
                (city, zone_id, window_start,
                 active_vehicles, pending_requests, completed_trips,
                 supply_demand_ratio, forecast_demand, avg_wait_time_sec)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        print("✅ CASSANDRA SINK READY → demand_zones")

    def map(self, s):
        try:
            r = json.loads(s)
            ws = datetime.fromtimestamp(r["window_start_unix"], tz=timezone.utc)
            self.session.execute(self.stmt, (
                r["city"],
                int(r["zone_id"]),
                ws,
                int(r["active_vehicles"]),
                int(r["pending_requests"]),
                int(r["completed_trips"]),
                float(r["supply_demand_ratio"]),
                float(r["forecast_demand"]),
                float(r["avg_wait_time_sec"]),
            ))
            print(f"✅ CASSANDRA: zone {r['zone_id']} | pending={r['pending_requests']} ratio={r['supply_demand_ratio']:.3f}")
        except Exception as e:
            print(f"CASSANDRA ERR: {e}")
        return s

    def close(self):
        if hasattr(self, "session") and self.session:
            try: self.session.shutdown()
            except: pass


# ═══════════════════════════════════════════════════════════════
#  7. Main
# ═══════════════════════════════════════════════════════════════
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("=" * 70)
    print("🚀 TaaSim — Flink Job 2 — Demand Aggregator COMPLET")
    print(f"   ✅ Sources: raw.gps (GPS) + raw.trips (Trip Requests)")
    print(f"   ✅ Fenêtre tumbling: {WINDOW_SECONDS}s")
    print("=" * 70)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_MAX_LATENESS_SECONDS))
        .with_timestamp_assigner(extract_timestamp)
    )

    # Source GPS
    gps_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.gps")
        .set_group_id("flink-job2-gps")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    # Source Trip
    trip_source = (KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("raw.trips")
        .set_group_id("flink-job2-trips")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())

    gps_stream = env.from_source(gps_source, watermark_strategy, "raw.gps") \
        .map(ParseGPS()) \
        .filter(NotNone())

    trip_stream = env.from_source(trip_source, watermark_strategy, "raw.trips") \
        .map(ParseTrip()) \
        .filter(NotNone())

    union_stream = gps_stream.union(trip_stream)

    result_stream = union_stream \
        .flat_map(UnifiedWindowAggregator(), output_type=Types.STRING())

    result_stream = result_stream.map(KafkaDemandSink(), output_type=Types.STRING())
    result_stream = result_stream.map(CassandraDemandSink(), output_type=Types.STRING())
    result_stream.print()

    print("▶️  Démarrage...")
    env.execute("TaaSim — Job2 — GPS + Trip Requests")


if __name__ == "__main__":
    main()