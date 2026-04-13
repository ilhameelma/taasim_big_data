# ADR-001: Architecture de Stockage TaaSim

## Statut
 - Semaine 2

## Contexte
TaaSim doit gérer trois types de données :
1. Données brutes (Porto CSV, NYC TLC) → batch processing
2. Messages Kafka (GPS, réservations) → streaming temps réel
3. Données de service (positions, trajets, demande) → accès faible latence

## Décision

### 1. MinIO comme Data Lake (S3-compatible)

**Structure des buckets :**
- `raw/` : Données brutes (Porto CSV, NYC Parquet)
- `curated/` : Données nettoyées (Parquet avec compression Snappy)
- `ml/` : Modèles ML entraînés
- `kafka-archive/` : Backup des messages Kafka (JSON)

### 2. Kafka comme System of Record

**Topics principaux :**
- `raw.trips` (3 partitions) : Réservations citoyens
- `raw.gps` (3 partitions) : Positions GPS des taxis
- `processed.gps` (3 partitions) : GPS normalisé
- `processed.demand` (3 partitions) : Agrégats demande
- `processed.matches` (3 partitions) : Matchs trip-taxi

### 3. Cassandra pour la couche Serving

**Tables :**

| Table | Partition Key | Clustering Key | Justification |
|-------|---------------|----------------|---------------|
| vehicle_positions | (city, zone_id) | event_time DESC | API requiert "véhicules dans zone X" |
| trips | (city, date_bucket) | created_at DESC | Évite partitions trop grandes |
| demand_zones | (city, zone_id) | window_start DESC | Heatmap temps réel par zone |

## Alternatives considérées

| Alternative | Raison du rejet |
|-------------|-----------------|
| PostgreSQL | Pas scalable pour millions de messages temps réel |
| MongoDB | Pas optimisé pour agrégations temps réel |
| Redis | Persistance limitée, pas d'historique |

## Conséquences

### Positives
- Séparation claire des responsabilités
- Scalabilité horizontale possible
- Replay des données pour ML

### Négatives
- Trois systèmes à maintenir
- Latence entre Kafka et Cassandra
- Complexité des relations

## Auteur
EL Mettichi Ilhame
Laaouina Soumya

## Date
2026-04-13