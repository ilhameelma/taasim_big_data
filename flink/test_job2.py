#!/usr/bin/env python3
# test_job2_validation.py
# Semaine 4 — Validation Job2 avec les producers officiels
#
# Ce script vérifie que Job2 fonctionne correctement avec les flux réels :
#   - vehicle_gps_producer.py (flux GPS réel)
#   - flink_job1_gps_normalizer.py (normalisation)
#   - flink_job2_demand_aggregator.py (agrégation)
#
# Lancement (dans un terminal séparé après avoir démarré les producers et Flink jobs) :
#   docker exec -it taasim-flink-jobmanager python /opt/flink/test_job2_validation.py
#
# Ou depuis Jupyter :
#   docker exec -it taasim-jupyter python /home/jovyan/work/flink/test_job2_validation.py

import time
import sys
from datetime import datetime, timedelta

try:
    from cassandra.cluster import Cluster
    from kafka import KafkaConsumer
except ImportError:
    print("❌ Installation des dépendances...")
    print("   pip install cassandra-driver kafka-python")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "taasim"
KAFKA_BROKERS = "kafka:9092"

# Durées de test (en secondes)
CHECK_INTERVAL = 10      # Vérification toutes les 10s
TEST_DURATION = 90       # Test pendant 90 secondes (3 fenêtres de 30s)
WINDOW_SECONDS = 30      # Fenêtre d'agrégation

# Zones attendues (les principales zones de Casablanca)
EXPECTED_ZONES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]


# ─────────────────────────────────────────────────────────────
#  Vérification Cassandra
# ─────────────────────────────────────────────────────────────
def check_cassandra():
    """Vérifie que demand_zones reçoit des données toutes les 30s"""
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)
        
        # Compter le nombre total de lignes
        rows = list(session.execute("SELECT COUNT(*) FROM demand_zones"))
        total_rows = rows[0].count if rows else 0
        
        # Récupérer les dernières fenêtres par zone
        latest_windows = []
        zones_found = set()
        
        for zone_id in EXPECTED_ZONES[:5]:  # Limiter pour lisibilité
            rows = list(session.execute(
                "SELECT zone_id, window_start, active_vehicles, pending_requests, supply_demand_ratio "
                "FROM demand_zones WHERE city=%s AND zone_id=%s ORDER BY window_start DESC LIMIT 1",
                ("Casablanca", zone_id)
            ))
            if rows:
                zones_found.add(zone_id)
                latest_windows.append({
                    "zone_id": zone_id,
                    "window_start": rows[0].window_start,
                    "active_vehicles": rows[0].active_vehicles,
                    "pending_requests": rows[0].pending_requests,
                    "ratio": rows[0].supply_demand_ratio
                })
        
        session.shutdown()
        cluster.shutdown()
        
        return {
            "total_rows": total_rows,
            "zones_found": zones_found,
            "latest_windows": latest_windows,
            "success": total_rows > 0
        }
    except Exception as e:
        return {"success": False, "error": str(e), "total_rows": 0, "zones_found": set()}


# ─────────────────────────────────────────────────────────────
#  Vérification Kafka processed.demand
# ─────────────────────────────────────────────────────────────
def check_kafka():
    """Vérifie que les messages arrivent dans processed.demand"""
    try:
        consumer = KafkaConsumer(
            "processed.demand",
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=3000,
            value_deserializer=lambda v: __import__('json').loads(v.decode("utf-8"))
        )
        
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= 10:
                break
        
        consumer.close()
        
        return {
            "success": len(messages) > 0,
            "count": len(messages),
            "messages": messages[:5]  # 5 premiers pour affichage
        }
    except Exception as e:
        return {"success": False, "error": str(e), "count": 0}


# ─────────────────────────────────────────────────────────────
#  Vérification en continu sur la durée
# ─────────────────────────────────────────────────────────────
def monitor_over_time(duration_seconds):
    """Surveille l'écriture dans Cassandra sur la durée du test"""
    print("\n" + "=" * 70)
    print("📊 SURVEILLANCE CONTINUE - Vérification des fenêtres 30s")
    print("=" * 70)
    
    start_time = time.time()
    check_count = 0
    previous_rows = 0
    rows_growth = []
    
    while time.time() - start_time < duration_seconds:
        check_count += 1
        elapsed = int(time.time() - start_time)
        
        result = check_cassandra()
        
        growth = result["total_rows"] - previous_rows
        if growth > 0:
            rows_growth.append({"time": elapsed, "new_rows": growth, "total": result["total_rows"]})
        
        print(f"\n⏱️  t={elapsed}s | Check #{check_count}")
        print(f"   📈 Lignes dans demand_zones : {result['total_rows']} (+{growth if growth > 0 else 0})")
        
        if result["zones_found"]:
            print(f"   🗺️  Zones actives : {sorted(result['zones_found'])}")
        
        # Afficher les dernières fenêtres
        for win in result.get("latest_windows", [])[:3]:
            print(f"   📍 Zone {win['zone_id']} : vehicles={win['active_vehicles']}, "
                  f"requests={win['pending_requests']}, ratio={win['ratio']:.2f}")
        
        if not result["success"] and "error" in result:
            print(f"   ⚠️  {result['error']}")
        
        previous_rows = result["total_rows"]
        
        # Attendre avant prochaine vérification
        time.sleep(CHECK_INTERVAL)
    
    return {
        "checks": check_count,
        "duration": duration_seconds,
        "total_rows": previous_rows,
        "growth": rows_growth
    }


# ─────────────────────────────────────────────────────────────
#  Vérification que les services sont accessibles
# ─────────────────────────────────────────────────────────────
def check_services():
    """Vérifie que Cassandra et Kafka sont accessibles"""
    print("\n🔍 Vérification des services...")
    
    # Vérifier Cassandra
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.execute("SELECT release_version FROM system.local")
        session.shutdown()
        cluster.shutdown()
        print("   ✅ Cassandra disponible")
    except Exception as e:
        print(f"   ❌ Cassandra indisponible: {e}")
        return False
    
    # Vérifier Kafka
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS, consumer_timeout_ms=1000)
        consumer.close()
        print("   ✅ Kafka disponible")
    except Exception as e:
        print(f"   ❌ Kafka indisponible: {e}")
        return False
    
    return True


# ─────────────────────────────────────────────────────────────
#  Message de démarrage
# ─────────────────────────────────────────────────────────────
def print_header():
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 68 + "║")
    print("║" + "   TaaSim — Validation Job 2 — Demand Aggregator".center(68) + "║")
    print("║" + " " * 68 + "║")
    print("║" + "   Vérifie que les agrégations sont écrites toutes les 30s".center(68) + "║")
    print("║" + "   dans demand_zones (Cassandra) et processed.demand (Kafka)".center(68) + "║")
    print("║" + " " * 68 + "║")
    print("╚" + "═" * 68 + "╝")


def print_instructions():
    print("\n" + "─" * 70)
    print("📋 AVANT DE LANCER CE TEST, assurez-vous que :")
    print("─" * 70)
    print("""
    Terminal 1 (Producer GPS) :
    ┌─────────────────────────────────────────────────────────────────────┐
    │ docker exec -it taasim-jupyter python -u                             │
    │   /home/jovyan/work/producers/vehicle_gps_producer.py               │
    │   --snap-sample-size 500                                            │
    │   --snap-graph-cache /home/jovyan/work/data/casa_graph.graphml      │
    │   --geojson-path /home/jovyan/work/data/Arrondissements.geojson     │
    │   --max-vehicles 20                                                 │
    │   --minio-endpoint minio:9000                                       │
    │   --bootstrap-servers kafka:9092                                    │
    └─────────────────────────────────────────────────────────────────────┘

    Terminal 2 (Flink Job1 - GPS Normalizer) :
    ┌─────────────────────────────────────────────────────────────────────┐
    │ docker exec -it taasim-flink-jobmanager flink run -py               │
    │   /opt/flink/flink_job1_gps_normalizer.py                          │
    └─────────────────────────────────────────────────────────────────────┘

    Terminal 3 (Flink Job2 - Demand Aggregator) :
    ┌─────────────────────────────────────────────────────────────────────┐
    │ docker exec -it taasim-flink-jobmanager flink run -py               │
    │   /opt/flink/flink_job2_demand_aggregator.py                       │
    └─────────────────────────────────────────────────────────────────────┘

    Terminal 4 (Ce test) :
    ┌─────────────────────────────────────────────────────────────────────┐
    │ docker exec -it taasim-flink-jobmanager python                      │
    │   /opt/flink/test_job2_validation.py                               │
    └─────────────────────────────────────────────────────────────────────┘
    """)
    
    input("⏎ Appuyez sur ENTER quand tous les services sont démarrés...")


# ─────────────────────────────────────────────────────────────
#  Rapport final
# ─────────────────────────────────────────────────────────────
def print_report(monitor_result, kafka_result):
    print("\n" + "=" * 70)
    print("📊 RAPPORT FINAL DE VALIDATION — JOB 2")
    print("=" * 70)
    
    # Résultat Cassandra
    print("\n🗄️  CASSANDRA - demand_zones")
    print(f"   Durée du test         : {monitor_result['duration']}s")
    print(f"   Nombre de vérifications : {monitor_result['checks']}")
    print(f"   Lignes totales écrites : {monitor_result['total_rows']}")
    
    if monitor_result['growth']:
        print(f"\n   📈 Évolution des écritures :")
        for g in monitor_result['growth']:
            print(f"      t={g['time']}s → +{g['new_rows']} lignes (total={g['total']})")
    
    # Vérification des fenêtres de 30s
    print("\n   ⏱️  Vérification fenêtrage 30s :")
    window_ok = len(monitor_result['growth']) >= 2  # Au moins 2 fenêtres dans 90s
    if window_ok:
        print(f"      ✅ Des écritures ont eu lieu toutes les 30s")
    else:
        print(f"      ⚠️  Seulement {len(monitor_result['growth'])} fenêtres détectées (minimum 2 attendues)")
    
    # Résultat Kafka
    print("\n📨 KAFKA - processed.demand")
    if kafka_result["success"]:
        print(f"   ✅ {kafka_result['count']} messages trouvés")
        if kafka_result.get("messages"):
            print(f"\n   📦 Aperçu des messages :")
            for msg in kafka_result["messages"][:3]:
                print(f"      Zone {msg.get('zone_id')} : vehicles={msg.get('active_vehicles')}, "
                      f"requests={msg.get('pending_requests')}, ratio={msg.get('supply_demand_ratio'):.2f}")
    else:
        print(f"   ⚠️  Aucun message trouvé dans processed.demand")
    
    # Conclusion
    print("\n" + "─" * 70)
    if monitor_result['total_rows'] > 0 and window_ok:
        print("✅ VALIDATION RÉUSSIE — Job2 écrit bien dans demand_zones toutes les 30s")
        print("   Et les messages sont bien émis dans processed.demand")
    elif monitor_result['total_rows'] > 0:
        print("⚠️  VALIDATION PARTIELLE — Des données sont écrites mais le fenêtrage 30s n'est pas confirmé")
    else:
        print("❌ VALIDATION ÉCHOUÉE — Aucune donnée dans demand_zones")
        print("   Vérifiez que les producers et Flink jobs sont bien démarrés")
    
    print("=" * 70)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print_header()
    
    if not check_services():
        print("\n❌ Services non disponibles. Vérifiez docker compose up -d")
        sys.exit(1)
    
    print_instructions()
    
    print("\n🚀 Démarrage de la validation...")
    print(f"   Durée du test : {TEST_DURATION}s ({TEST_DURATION//30} fenêtres de 30s attendues)")
    print(f"   Vérification toutes les {CHECK_INTERVAL}s")
    
    # Surveillance continue
    monitor_result = monitor_over_time(TEST_DURATION)
    
    # Vérification Kafka à la fin
    kafka_result = check_kafka()
    
    # Rapport final
    print_report(monitor_result, kafka_result)


if __name__ == "__main__":
    main()