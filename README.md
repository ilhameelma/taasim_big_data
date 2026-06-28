# 🚕 TaaSim — Urban Mobility Simulation Platform

> **Projet de fin d'études — ENSA Al Hoceima | 2025-2026**
> Encadrant : Pr. Mohamed EL Marouani

Plateforme de simulation de mobilité urbaine en temps réel, modélisant le dispatch de taxis dans la ville de **Casablanca, Maroc**. TaaSim combine un pipeline de données massives, du streaming événementiel, du machine learning et une API REST pour simuler et prévoir la demande de courses.

---

## 📐 Architecture globale

TaaSim repose sur une **Architecture Kappa** (stream-first, sans couche batch/speed séparée) :

```
Porto Taxi Dataset (CSV)
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  PARTIE A — Streaming & Infrastructure                       │
│                                                             │
│  PySpark ETL ──► MinIO (curated/porto-trips/*.parquet)      │
│                         │                                   │
│  VehicleGPSProducer ────┤                                   │
│  TripRequestProducer    │                                   │
│                         ▼                                   │
│               Apache Kafka (raw.gps / trip.requests)        │
│                         │                                   │
│         ┌───────────────┼───────────────┐                   │
│         ▼               ▼               ▼                   │
│   GPS Normalizer  Demand Aggregator  Trip Matcher           │
│   (Flink Job)     (Flink Job)        (Flink Job)            │
│         │               │               │                   │
│         └───────────────┴───────────────┘                   │
│                         │                                   │
│                  Apache Cassandra                           │
│             (trips / vehicle_positions)                     │
│                         │                                   │
│               FastAPI (REST + JWT)                          │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  PARTIE B — Machine Learning                                 │
│                                                             │
│  Feature Engineering (PySpark) ──► GBTRegressor (MLlib)     │
│                                          │                  │
│  Évaluation & Baseline comparison        │                  │
│                                          ▼                  │
│              MinIO (machine-learning/models/demand_v1/)     │
│                                          │                  │
│              FastAPI /predict/demand ◄───┘                  │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
  Grafana Dashboards (observabilité temps réel)
```

---

## 📦 Stack technique

| Couche | Technologie |
|--------|-------------|
| Stockage data lake | MinIO (S3-compatible) |
| Traitement batch | Apache Spark (PySpark) |
| Streaming | Apache Kafka (KRaft), Apache Flink |
| Base opérationnelle | Apache Cassandra |
| Machine Learning | PySpark MLlib — GBTRegressor |
| API | FastAPI, JWT (`python-jose`, `passlib`) |
| Observabilité | Grafana |
| Conteneurisation | Docker, Docker Compose |
| Documentation | GitHub Pages (Swagger UI statique) |

---

## 🗂️ Structure du dépôt

```
taasim/
├── notebooks/
│   ├── 01_eda.ipynb                    # Exploration du dataset Porto
│   └── 02_zone_remapping_v3.ipynb      # ETL PySpark + remapping Casablanca
│
├── producers/
│   ├── vehicle_gps_producer.py         # Simule les GPS des taxis → Kafka
│   └── trip_request_producer.py        # Simule les demandes de courses → Kafka
│
├── flink-jobs/
│   ├── gps_normalizer/                 # Normalisation et filtrage GPS
│   ├── demand_aggregator/              # Agrégation de la demande par zone/fenêtre
│   └── trip_matcher/                   # Matching rider ↔ véhicule disponible
│
├── ml/
│   ├── train_demand_model.py           # Entraînement GBTRegressor (PySpark MLlib)
│   ├── evaluate_model.py               # RMSE, MAE, R², baseline comparison
│   └── model_persistence.py           # Sauvegarde/chargement depuis MinIO
│
├── api/
│   ├── main.py                         # FastAPI — point d'entrée
│   ├── auth.py                         # JWT (rider / admin roles)
│   ├── routers/
│   │   ├── trips.py                    # Création et suivi de courses
│   │   ├── vehicles.py                 # Position et zone des véhicules
│   │   └── forecast.py                 # Prévision de la demande
│   └── schemas.py                      # Modèles Pydantic
│
├── data/
│   ├── Arrondissements.geojson         # Polygones des arrondissements Casablanca
│   ├── casa_graph.graphml              # Réseau routier OSMnx (cache)
│   └── zone_mapping_geojson.csv        # Mapping zones Casablanca
│
├── docs/
│   ├── index.html                      # Swagger UI statique (GitHub Pages)
│   └── openapi.json                    # Spec OpenAPI exportée
│
├── docker-compose.yml                  # Orchestration de tous les services
└── README.md
```

---

## 🚀 Démarrage rapide

### Prérequis

- Docker & Docker Compose
- Python 3.10+
- 16 Go de RAM recommandés (Spark + Flink + Kafka)

### 1. Lancer tous les services

```bash
docker compose up -d
```

Services démarrés : MinIO, Kafka (KRaft), Flink (JobManager + TaskManager), Cassandra, Jupyter, Spark Master/Worker, FastAPI, Grafana.

### 2. Préparer les données

Exécuter les notebooks dans l'ordre depuis Jupyter (`http://localhost:8888`) :

```
01_eda.ipynb              # optionnel — exploration
02_zone_remapping_v3.ipynb  # obligatoire — génère les Parquet dans MinIO
```

### 3. Lancer le producer GPS

```bash
docker exec -it taasim-jupyter python -u \
  /home/jovyan/work/producers/vehicle_gps_producer.py \
  --curated-bucket curated \
  --curated-prefix porto-trips \
  --geojson-path /home/jovyan/work/data/Arrondissements.geojson \
  --snap-graph-cache /home/jovyan/work/data/casa_graph.graphml \
  --max-vehicles 50 \
  --max-trips 5000 \
  --minio-endpoint minio:9000 \
  --bootstrap-servers kafka:9092
```

### 4. Entraîner le modèle ML

```bash
docker exec -it taasim-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /home/jovyan/work/ml/train_demand_model.py
```

### 5. Accéder à l'API

```
http://localhost:8000/docs       # Swagger UI interactif
http://localhost:8000/redoc      # ReDoc
```

---

## 🤖 Modèle ML — Prévision de demande

**Algorithme** : Gradient Boosted Tree Regressor (PySpark MLlib)  
**Cible** : nombre de courses demandées par zone et par fenêtre temporelle  
**Split** : temporel (train avant / test après)

| Métrique | Valeur |
|----------|--------|
| RMSE (modèle GBT) | — |
| RMSE (baseline naïf lag) | — |
| **Amélioration vs baseline** | **~33,6 %** |

Le modèle entraîné est persisté dans MinIO :
```
s3a://machine-learning/models/demand_v1/
```

---

## 🔌 API — Endpoints principaux

| Méthode | Route | Description | Auth |
|---------|-------|-------------|------|
| `POST` | `/auth/token` | Obtenir un JWT | — |
| `POST` | `/trips/` | Créer une course | rider |
| `GET` | `/trips/{id}` | Statut d'une course | rider |
| `GET` | `/vehicles/zone/{zone_id}` | Véhicules disponibles dans une zone | admin |
| `GET` | `/predict/demand` | Prévision de demande (ML) | admin |

**Rôles** : `rider` (créer/suivre ses courses) — `admin` (accès complet)

---

## 📨 Format des messages Kafka

**Topic `raw.gps`** — émis par le VehicleGPSProducer :

```json
{
  "taxi_id": 20000589,
  "timestamp": "2026-04-24T10:49:03.123456",
  "lat": 33.574821,
  "lon": -7.591034,
  "zone_id": 2,
  "zone_name": "Anfa",
  "speed": 43.7,
  "status": "moving",
  "trip_progress": 0.42,
  "road_snapped": true
}
```

---

## 📊 Observabilité

Grafana est accessible sur `http://localhost:3000` (admin / admin).

Dashboards disponibles :
- **GPS Live** — positions des taxis en temps réel par zone
- **Demand Heatmap** — demande agrégée par zone et fenêtre temporelle
- **Trip Pipeline** — statuts des courses (pending → assigned → completed)

---

## 📚 Documentation en ligne

La documentation API statique est hébergée sur GitHub Pages :  
👉 **[https://\<username\>.github.io/taasim]()**

Elle inclut le Swagger UI complet avec la spec `openapi.json` embarquée.  
> ⚠️ Les appels live nécessitent le stack Docker en local ou une exposition via ngrok.

---

## 🐛 Dépannage fréquent

| Erreur | Cause | Solution |
|--------|-------|----------|
| `Killed` au démarrage du producer | OOM Spark | Réduire `--max-trips` à 2000 |
| `DNS failure kafka:9092` | Lancé hors Docker | Utiliser `docker exec` ou `--bootstrap-servers localhost:9092` |
| `No .parquet found` | Notebook non exécuté | Exécuter `02_zone_remapping_v3.ipynb` d'abord |
| `SyntaxError` dans un script Python | Marqueurs de merge Git non résolus | Supprimer les blocs `<<<<<<<` / `=======` / `>>>>>>>` |
| Cassandra vide (pas de `pending`) | Flink ne traite que les GPS simulés | Créer une course via Swagger pour déclencher le pipeline complet |
| Spark OOM (shuffle) | Trop de partitions / mémoire | Augmenter `spark.executor.memory` ou réduire le dataset |

---

## 👥 Équipe

| Rôle | Nom | Partie |
|------|-----|--------|
| Streaming & Infrastructure | Ilhame El Mettichi / Soumya Laaouina | Partie A |
| ML & Évaluation | Ily | Partie B |

**Encadrant** : Pr. Mohamed EL Marouani — ENSA Al Hoceima  
**Année académique** : 2025-2026

---

## 📄 Licence

Projet académique — ENSA Al Hoceima. Tous droits réservés.
