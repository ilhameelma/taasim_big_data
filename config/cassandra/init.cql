-- config/cassandra/init.cql

-- Créer le keyspace
CREATE KEYSPACE IF NOT EXISTS taasim
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE taasim;

-- ============================================================
-- TABLE 1: vehicle_positions
-- ============================================================
-- But: Stocker la dernière position de chaque véhicule par zone
-- Partition key: (city, zone_id) - Pour requêtes "tous les véhicules dans une zone"
-- Clustering key: event_time DESC - Pour avoir les positions les plus récentes en premier

CREATE TABLE IF NOT EXISTS vehicle_positions (
    city text,                    -- 'Casablanca'
    zone_id int,                  -- ID de la zone (1-16)
    event_time timestamp,         -- Heure de l'événement GPS
    taxi_id int,                  -- ID du taxi
    lat double,                   -- Latitude
    lon double,                   -- Longitude
    speed double,                 -- Vitesse (km/h)
    status text,                  -- 'available', 'moving', 'offline'
    trip_progress double,         -- Progression du trajet (0 à 1)
    PRIMARY KEY ((city, zone_id), event_time, taxi_id)
) WITH CLUSTERING ORDER BY (event_time DESC, taxi_id ASC)
  AND default_time_to_live = 604800  -- 7 jours de rétention
  AND comment = 'Positions GPS des taxis par zone';

-- Justification de la partition key (city, zone_id):
-- La requête principale est: "Tous les véhicules disponibles dans la zone X"
-- Cette partition permet de lire tous les véhicules d'une zone en une seule requête.

-- ============================================================
-- TABLE 2: trips
-- ============================================================
-- But: Stocker l'historique des trajets
-- Partition key: (city, date_bucket) - Pour éviter les partitions trop grandes
-- Clustering key: created_at DESC - Pour les trajets récents en premier

CREATE TABLE IF NOT EXISTS trips (
    city text,                    -- 'Casablanca'
    date_bucket text,             -- '2024-01-15' (partition par jour)
    created_at timestamp,         -- Date de création de la réservation
    trip_id text,                 -- ID unique du trajet
    rider_id text,                -- ID du passager
    taxi_id int,                  -- ID du taxi assigné
    origin_zone int,              -- Zone de départ
    dest_zone int,                -- Zone d'arrivée
    call_type text,               -- 'A', 'B', ou 'C'
    status text,                  -- 'pending', 'matched', 'completed', 'cancelled'
    estimated_fare decimal,       -- Prix estimé (MAD)
    actual_fare decimal,          -- Prix réel (MAD)
    distance_km double,           -- Distance parcourue
    duration_min double,          -- Durée du trajet
    match_latency_ms int,         -- Temps de matching (ms)
    requested_at timestamp,       -- Heure de la demande
    matched_at timestamp,         -- Heure du matching
    completed_at timestamp,       -- Heure de fin
    PRIMARY KEY ((city, date_bucket), created_at, trip_id)
) WITH CLUSTERING ORDER BY (created_at DESC, trip_id ASC)
  AND comment = 'Historique complet des trajets';

-- Justification de la partition key (city, date_bucket):
-- Évite l'accumulation de millions de lignes dans une seule partition
-- Facilite la suppression des données anciennes (TTL ou suppression par bucket)

-- ============================================================
-- TABLE 3: demand_zones
-- ============================================================
-- But: Stocker les agrégats de demande par zone pour le heatmap
-- Partition key: (city, zone_id) - Pour toutes les données d'une zone
-- Clustering key: window_start DESC - Fenêtres récentes en premier

CREATE TABLE IF NOT EXISTS demand_zones (
    city text,                    -- 'Casablanca'
    zone_id int,                  -- ID de la zone
    window_start timestamp,       -- Début de la fenêtre de 30s
    active_vehicles int,          -- Nombre de véhicules actifs
    pending_requests int,         -- Demandes en attente
    completed_trips int,          -- Trajets complétés dans la fenêtre
    lat double,                    -- ← AJOUTER
    lon double,  
    supply_demand_ratio double,   -- Ratio offre/demande (vehicles/requests)
    forecast_demand double,       -- Prédiction ML (optionnel)
    avg_wait_time_sec double,     -- Temps d'attente moyen
    PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 86400  -- 24h de rétention
  AND comment = 'Agrégats de demande par zone pour heatmap temps réel';

-- Justification de la partition key (city, zone_id):
-- La requête principale est: "Demande pour la zone X sur les dernières heures"
-- Toutes les fenêtres d'une zone sont dans la même partition

-- ============================================================
-- INDEX (optionnels, à utiliser avec précaution)
-- ============================================================

-- Pour rechercher les trajets d'un taxi spécifique
CREATE INDEX IF NOT EXISTS idx_trips_taxi_id ON trips (taxi_id);

-- Pour rechercher les trajets d'un passager
CREATE INDEX IF NOT EXISTS idx_trips_rider_id ON trips (rider_id);

-- Pour rechercher les trajets par statut
CREATE INDEX IF NOT EXISTS idx_trips_status ON trips (status);

-- ============================================================
-- Vérifications
-- ============================================================

-- Lister les tables
DESCRIBE TABLES;

-- Décrire une table
DESCRIBE TABLE vehicle_positions;
DESCRIBE TABLE trips;
DESCRIBE TABLE demand_zones;