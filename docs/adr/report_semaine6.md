# Rapport Feature Engineering — Semaine 6
## TaaSim · ENSA Al Hoceima · Capstone 2025-2026

**Auteur** : Personne A  
**Date** : Juin 2026  
**Fichier** : `feat_engineering.py`  
**Sortie** : `s3a://machine-learning/features/feature_matrix/`

---

## 1. Objectif

L'objectif de cette semaine est de transformer les données brutes de trajets Porto
(stockées dans `s3a://curated/trips/`) en une **Feature Matrix** exploitable
par le modèle de prédiction de demande (GBTRegressor) développé par le binôme.

Le pipeline produit **162 495 lignes × 17 colonnes** couvrant la période
juillet 2013 – mars 2014, agrégées par tranche de 30 minutes et par zone
géographique de Casablanca.

---

## 2. Source de données

| Source | Emplacement MinIO | Description |
|---|---|---|
| Trajets Porto remappés | `s3a://curated/trips/` | 1 081 137 trajets, partitionné par `year_month` |
| Zones Casablanca | `s3a://raw/porto-trips/zone_mapping_geojson.csv` | 17 arrondissements avec population et type |
| Météo historique | API Open-Meteo (archive) | Porto, Jul 2013 – Jun 2014, résolution horaire |

---

## 3. Pipeline de construction (A1 → A7)

### A1 — Lecture des données
Lecture du Parquet depuis MinIO, partitionné par `year_month` (2013-07 → 2014-03).
**1 081 137 trajets** valides chargés avec les colonnes :
`trip_id`, `taxi_id`, `timestamp`, `origin_zone_id`, `dest_zone_id`, `trip_duration_sec`.

---

### A2 — Features temporelles
Extraction depuis la colonne `timestamp` (epoch Unix) :

| Feature | Type | Description |
|---|---|---|
| `hour_of_day` | int (0–23) | Heure de la demande |
| `day_of_week` | int (1–7) | Jour de la semaine (1=Dim, 7=Sam) |
| `is_weekend` | int (0/1) | 1 si Samedi ou Dimanche |
| `is_friday` | int (0/1) | 1 si Vendredi — pic de demande culturel au Maroc |
| `time_slot_30min` | timestamp | Créneau 30 min (clé d'agrégation) |

**Justification** : La demande de taxi est fortement corrélée à l'heure
(pics à 8h et 18h) et au jour (vendredi et week-end au Maroc).

---

### A3 — Features spatiales
Join avec `zone_mapping_geojson.csv` sur `origin_zone_id` = `zone_id` :

| Feature | Type | Description |
|---|---|---|
| `zone_population_density` | float | Densité de population de la zone (hab/km²) |
| `zone_type` | string | Type de zone (urban pour toutes les zones) |
| `zone_type_urban` | int (0/1) | One-hot encoding |
| `zone_type_residential` | int (0/1) | One-hot encoding |
| `zone_type_commercial` | int (0/1) | One-hot encoding |
| `zone_type_transit_hub` | int (0/1) | One-hot encoding |

**Justification** : Les zones à forte densité (ex: Ben M'Sick 468 542 hab/km²)
génèrent structurellement plus de demande. Le one-hot encoding est nécessaire
pour le `VectorAssembler` de Spark MLlib.

Les 17 zones et leurs densités :

| Zone | Densité (hab/km²) |
|---|---|
| Ben M'Sick | 468 542 |
| Sidi Moumen | 454 779 |
| Ain Chock | 377 744 |
| Ain Sebaa | 171 452 |
| Sidi Bernoussi | 173 189 |
| … | … |
| Mechouar | 2 645 (zone administrative) |

---

### A4 — Features météo (Open-Meteo)
Appel à l'API historique Open-Meteo pour Porto, résolution horaire.
**8 760 entrées** récupérées (365 jours × 24 heures).

| Feature | Type | Description |
|---|---|---|
| `is_raining` | int (0/1) | 1 si précipitation > 0.1 mm/h |
| `temperature_bucket` | string | `cold` (<15°C) / `mild` (15–28°C) / `hot` (>28°C) |

**Justification** : La pluie augmente la demande de taxi de façon significative.
La température influence le comportement de mobilité (moins de marche par temps froid ou très chaud).

Join effectué sur la clé horaire `YYYY-MM-DDTHH` entre les données météo et les trajets.

---

### A5 — Lag features (Window functions)
Calcul des features de série temporelle par zone, ordonnées par `time_slot_30min`.
Chaque slot = 30 minutes → 1 jour = 48 slots, 7 jours = 336 slots.

| Feature | Décalage | Description |
|---|---|---|
| `demand_lag_1d` | 48 slots (1 jour) | Demande au même créneau hier |
| `demand_lag_7d` | 336 slots (7 jours) | Demande au même créneau il y a 7 jours |
| `rolling_7d_mean` | fenêtre [-336, -1] | Moyenne glissante sur les 7 derniers jours |

**Justification** : Ce sont les features les plus prédictives pour la demande de taxi.
Un lundi à 8h ressemble aux lundis précédents à 8h. La moyenne glissante lisse
les variations exceptionnelles (événements, jours fériés).

Les valeurs nulles des premières lignes (début de série, pas encore 7 jours d'historique)
sont remplacées par 0.

---

### A6 — Agrégation 30 min → Feature Matrix
Groupement par `(origin_zone_id, time_slot_30min)` avec toutes les features.
La colonne cible `demand` = nombre de trajets dans le créneau.

**Résultat** : 162 495 lignes correspondant à 17 zones × ~9 550 créneaux de 30 min
sur 8 mois de données.

---

### A7 — Écriture Feature Matrix
Écriture en Parquet partitionné par `origin_zone_id` vers :

```
s3a://machine-learning/features/feature_matrix/
├── origin_zone_id=0/
├── origin_zone_id=1/
├── ...
└── origin_zone_id=16/
```

---

## 4. Schéma final de la Feature Matrix

| Colonne | Type | Rôle |
|---|---|---|
| `origin_zone_id` | int | Identifiant (non feature ML) |
| `time_slot_30min` | timestamp | Identifiant (non feature ML) |
| `hour_of_day` | int | Feature temporelle |
| `day_of_week` | int | Feature temporelle |
| `is_weekend` | int | Feature temporelle |
| `is_friday` | int | Feature temporelle |
| `zone_population_density` | float | Feature spatiale |
| `zone_type_urban` | int | Feature spatiale |
| `zone_type_residential` | int | Feature spatiale |
| `zone_type_commercial` | int | Feature spatiale |
| `zone_type_transit_hub` | int | Feature spatiale |
| `is_raining` | int | Feature météo |
| `temperature_bucket` | string | Feature météo (StringIndexer requis) |
| `demand_lag_1d` | float | Lag feature |
| `demand_lag_7d` | float | Lag feature |
| `rolling_7d_mean` | float | Lag feature |
| `demand` | int | **Label / Target** |

**Total : 13 features numériques + 1 feature string + 1 label**

---

## 5. Instructions pour le binôme (ML Training)

```python
# Lecture de la Feature Matrix
df = spark.read.parquet("s3a://machine-learning/features/feature_matrix/")

# Colonnes numériques directement utilisables dans VectorAssembler
feature_cols = [
    "hour_of_day", "day_of_week", "is_weekend", "is_friday",
    "zone_population_density",
    "zone_type_urban", "zone_type_residential",
    "zone_type_commercial", "zone_type_transit_hub",
    "is_raining",
    "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean"
]

# temperature_bucket nécessite un StringIndexer avant VectorAssembler
# Label : "demand"
```

---

## 6. Statistiques de validation

| Métrique | Valeur |
|---|---|
| Lignes en entrée (trips) | 1 081 137 |
| Lignes Feature Matrix | 162 495 |
| Zones couvertes | 17 (0–16) |
| Période couverte | 2013-07 → 2014-03 |
| Entrées météo | 8 760 (365 jours × 24h) |
| Colonnes produites | 17 (13 features + 1 string + 1 label + 2 ids) |
