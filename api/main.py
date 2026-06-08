"""
TaaSim — Demand Forecasting API
================================
ENSA Al Hoceima — Capstone 2025-2026
Semaine 6 — POST /api/demand/forecast

Endpoint unique : prédit la demande de taxis par zone et datetime.
Le modèle GBT est chargé UNE SEULE FOIS au démarrage (Spark session persistante).
Les lag features sont lues depuis Cassandra demand_zones.
"""

import logging
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"
S3_MODEL_PATH    = "s3a://machine-learning/models/demand_v1/"

CASSANDRA_HOST   = "cassandra"
CASSANDRA_PORT   = 9042
KEYSPACE         = "taasim"

# Features spatiales fixes par zone (depuis zone_mapping_geojson.csv)
ZONE_FEATURES = {
    1:  {"population_density": 8500,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    2:  {"population_density": 12000, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 0},
    3:  {"population_density": 15000, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 1},
    4:  {"population_density": 7000,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    5:  {"population_density": 9000,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    6:  {"population_density": 8000,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    7:  {"population_density": 11000, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 0},
    8:  {"population_density": 13000, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 1},
    9:  {"population_density": 10000, "urban": 1, "residential": 0, "commercial": 0, "transit_hub": 0},
    10: {"population_density": 9500,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    11: {"population_density": 8000,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    12: {"population_density": 14000, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 1},
    13: {"population_density": 7500,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    14: {"population_density": 6500,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    15: {"population_density": 5000,  "urban": 0, "residential": 1, "commercial": 0, "transit_hub": 0},
    16: {"population_density": 11500, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 0},
    17: {"population_density": 13500, "urban": 1, "residential": 0, "commercial": 1, "transit_hub": 1},
}

# Valeurs par défaut si Cassandra ne retourne rien
DEFAULT_LAG_1D       = 40.0
DEFAULT_LAG_7D       = 38.0
DEFAULT_ROLLING_MEAN = 39.0


# ─────────────────────────────────────────────
# ÉTAT GLOBAL (chargé une seule fois au démarrage)
# ─────────────────────────────────────────────

state = {
    "spark":   None,
    "model":   None,
    "cassandra_session": None,
    "ready":   False,
}


# ─────────────────────────────────────────────
# INITIALISATION AU DÉMARRAGE
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Charge Spark + modèle + Cassandra au démarrage du container."""
    logger.info("🚀 Démarrage TaaSim Demand API...")

    # 1. Spark session
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName("TaaSim-DemandAPI")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        state["spark"] = spark
        logger.info("✅ Spark session démarrée")
    except Exception as e:
        logger.error(f"❌ Spark failed: {e}")
        raise

    # 2. Chargement du modèle depuis MinIO
    try:
        from pyspark.ml import PipelineModel
        model = PipelineModel.load(S3_MODEL_PATH)
        state["model"] = model
        logger.info(f"✅ Modèle chargé depuis {S3_MODEL_PATH}")
    except Exception as e:
        logger.error(f"❌ Modèle non trouvé: {e}")
        raise

    # 3. Connexion Cassandra
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)
        state["cassandra_session"] = session
        logger.info("✅ Cassandra connecté")
    except Exception as e:
        logger.warning(f"⚠️ Cassandra indisponible, valeurs par défaut utilisées: {e}")

    state["ready"] = True
    logger.info("✅ API prête à recevoir des requêtes")

    yield  # L'application tourne ici

    # Nettoyage à l'arrêt
    if state["spark"]:
        state["spark"].stop()
    if state["cassandra_session"]:
        state["cassandra_session"].shutdown()
    logger.info("🛑 API arrêtée proprement")


# ─────────────────────────────────────────────
# MODÈLES PYDANTIC
# ─────────────────────────────────────────────

class ForecastRequest(BaseModel):
    zone_id:  int      = Field(..., ge=1, le=17, description="ID de la zone (1-17)")
    datetime: str      = Field(..., description="Format ISO 8601: 2026-06-05T08:30:00")


class ForecastResponse(BaseModel):
    zone_id:          int
    datetime:         str
    predicted_demand: float
    zone_name:        Optional[str] = None
    confidence:       str = "model_v1"


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_lag_features(zone_id: int, dt: datetime) -> dict:
    """
    Lit les lag features depuis Cassandra demand_zones.
    Retourne les valeurs par défaut si Cassandra est indisponible.
    """
    session = state["cassandra_session"]
    if session is None:
        return {
            "lag_1d":       DEFAULT_LAG_1D,
            "lag_7d":       DEFAULT_LAG_7D,
            "rolling_mean": DEFAULT_ROLLING_MEAN,
        }

    try:
        # Fenêtre correspondant à il y a 1 jour (même heure)
        ts_1d = dt - timedelta(days=1)
        ts_7d = dt - timedelta(days=7)

        # Lire pending_requests pour lag_1d
        row_1d = session.execute(
            """SELECT pending_requests FROM demand_zones
               WHERE city=%s AND zone_id=%s AND window_start >= %s
               ORDER BY window_start DESC LIMIT 1""",
            ("Casablanca", zone_id, ts_1d - timedelta(hours=1))
        ).one()

        # Lire pending_requests pour lag_7d
        row_7d = session.execute(
            """SELECT pending_requests FROM demand_zones
               WHERE city=%s AND zone_id=%s AND window_start >= %s
               ORDER BY window_start DESC LIMIT 1""",
            ("Casablanca", zone_id, ts_7d - timedelta(hours=1))
        ).one()

        # Moyenne glissante 7 jours
        rows_7d = session.execute(
            """SELECT pending_requests FROM demand_zones
               WHERE city=%s AND zone_id=%s AND window_start >= %s
               LIMIT 336""",
            ("Casablanca", zone_id, dt - timedelta(days=7))
        ).all()

        lag_1d = float(row_1d.pending_requests) if row_1d else DEFAULT_LAG_1D
        lag_7d = float(row_7d.pending_requests) if row_7d else DEFAULT_LAG_7D
        rolling = (
            sum(r.pending_requests for r in rows_7d) / len(rows_7d)
            if rows_7d else DEFAULT_ROLLING_MEAN
        )

        return {"lag_1d": lag_1d, "lag_7d": lag_7d, "rolling_mean": rolling}

    except Exception as e:
        logger.warning(f"⚠️ Cassandra read failed pour zone {zone_id}: {e}")
        return {
            "lag_1d":       DEFAULT_LAG_1D,
            "lag_7d":       DEFAULT_LAG_7D,
            "rolling_mean": DEFAULT_ROLLING_MEAN,
        }


def build_feature_row(zone_id: int, dt: datetime, lags: dict) -> dict:
    """Construit le vecteur de features dans l'ordre exact du modèle."""
    zone = ZONE_FEATURES.get(zone_id, ZONE_FEATURES[1])
    dow  = dt.weekday()  # 0=Lundi ... 6=Dimanche

    # temperature_bucket : simulé selon le mois (Casablanca)
    month = dt.month
    if month in [12, 1, 2]:
        temp_bucket = "cold"
    elif month in [6, 7, 8, 9]:
        temp_bucket = "hot"
    else:
        temp_bucket = "mild"

    return {
        # Temporelles
        "hour_of_day":             dt.hour,
        "day_of_week":             dow,
        "is_weekend":              1 if dow >= 5 else 0,
        "is_friday":               1 if dow == 4 else 0,
        # Spatiales
        "zone_population_density": float(zone["population_density"]),
        "zone_type_urban":         zone["urban"],
        "zone_type_residential":   zone["residential"],
        "zone_type_commercial":    zone["commercial"],
        "zone_type_transit_hub":   zone["transit_hub"],
        # Météo
        "is_raining":              0,
        # Lag features
        "demand_lag_1d":           lags["lag_1d"],
        "demand_lag_7d":           lags["lag_7d"],
        "rolling_7d_mean":         lags["rolling_mean"],
        # Catégorielle (StringIndexer attend la string, pas l'index)
        "temperature_bucket":      temp_bucket,
    }


# ─────────────────────────────────────────────
# APPLICATION FASTAPI
# ─────────────────────────────────────────────

app = FastAPI(
    title="TaaSim Demand Forecast API",
    description="Prédit la demande de taxis par zone — Capstone ENSA Al Hoceima 2025-2026",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/api/health")
async def health():
    return {
        "status":       "healthy" if state["ready"] else "loading",
        "model_loaded": state["model"] is not None,
        "cassandra":    state["cassandra_session"] is not None,
    }


@app.post("/api/demand/forecast", response_model=ForecastResponse)
async def forecast(request: ForecastRequest):
    """
    Prédit la demande de taxis pour une zone et une datetime données.
    
    - **zone_id** : identifiant de zone Casablanca (1-17)
    - **datetime** : datetime ISO 8601 (ex: 2026-06-05T08:30:00)
    
    Retourne le nombre prédit de courses dans le slot de 30 min.
    """
    if not state["ready"] or state["model"] is None:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    # 1. Parser la datetime
    try:
        dt = datetime.fromisoformat(request.datetime)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Format datetime invalide. Utilisez ISO 8601: 2026-06-05T08:30:00"
        )

    # 2. Récupérer les lag features depuis Cassandra
    lags = get_lag_features(request.zone_id, dt)

    # 3. Construire le vecteur de features
    features = build_feature_row(request.zone_id, dt, lags)

    # 4. Créer le DataFrame Spark et prédire
    try:
        spark_df   = state["spark"].createDataFrame([features])
        pred_df    = state["model"].transform(spark_df)
        predicted  = pred_df.select("prediction").collect()[0][0]
        predicted  = max(0.0, round(float(predicted), 2))
    except Exception as e:
        logger.error(f"❌ Erreur prédiction zone {request.zone_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur de prédiction: {str(e)}")

    return ForecastResponse(
        zone_id=request.zone_id,
        datetime=request.datetime,
        predicted_demand=predicted,
        confidence="model_v1",
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)