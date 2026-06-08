"""
TaaSim — Full API
================================
ENSA Al Hoceima — Capstone 2025-2026
Semaine 7 — JWT Authentication (rider + admin roles)

Endpoints :
  - POST /auth/token                  → génère un JWT (public)
  - GET  /api/health                  → public
  - POST /api/v1/trips                → rider + admin
  - GET  /api/v1/trips/{trip_id}      → rider + admin
  - GET  /api/v1/vehicles/zone/{zone_id} → admin seulement
  - POST /api/demand/forecast         → rider + admin
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from jose import JWTError, jwt
from passlib.context import CryptContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIGURATION JWT
# ─────────────────────────────────────────────

SECRET_KEY       = "taasim-secret-key-change-in-production"
ALGORITHM        = "HS256"
TOKEN_EXPIRE_MIN = 60

pwd_context   = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

FAKE_USERS = {
    "rider1": {
        "username": "rider1",
        "hashed_password": pwd_context.hash("rider123"),
        "role": "rider",
    },
    "admin": {
        "username": "admin",
        "hashed_password": pwd_context.hash("admin123"),
        "role": "admin",
    },
}

# ─────────────────────────────────────────────
# CONFIGURATION APP
# ─────────────────────────────────────────────

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"
S3_MODEL_PATH    = "s3a://machine-learning/models/demand_v1/"

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE       = "taasim"

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

DEFAULT_LAG_1D       = 40.0
DEFAULT_LAG_7D       = 38.0
DEFAULT_ROLLING_MEAN = 39.0

state = {
    "spark":             None,
    "model":             None,
    "cassandra_session": None,
    "ready":             False,
}

# ─────────────────────────────────────────────
# FONCTIONS JWT
# ─────────────────────────────────────────────

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def authenticate_user(username: str, password: str) -> Optional[dict]:
    user = FAKE_USERS.get(username)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user

def create_access_token(data: dict) -> str:
    payload = data.copy()
    expire  = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRE_MIN)
    payload.update({"exp": expire})
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token invalide ou expiré",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload  = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        role     = payload.get("role")
        if username is None or role is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = FAKE_USERS.get(username)
    if user is None:
        raise credentials_exception
    return user

async def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """Autorise uniquement le rôle admin."""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Accès réservé aux administrateurs",
        )
    return current_user

async def require_rider_or_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """Autorise rider ET admin."""
    if current_user["role"] not in ("rider", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Accès non autorisé",
        )
    return current_user

# ─────────────────────────────────────────────
# DÉMARRAGE
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Démarrage TaaSim API...")

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

    try:
        from pyspark.ml import PipelineModel
        model = PipelineModel.load(S3_MODEL_PATH)
        state["model"] = model
        logger.info(f"✅ Modèle chargé depuis {S3_MODEL_PATH}")
    except Exception as e:
        logger.error(f"❌ Modèle non trouvé: {e}")
        raise

    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(KEYSPACE)
        state["cassandra_session"] = session
        logger.info("✅ Cassandra connecté")
    except Exception as e:
        logger.warning(f"⚠️ Cassandra indisponible, valeurs par défaut utilisées: {e}")

    state["ready"] = True
    logger.info("✅ API prête")
    yield

    if state["spark"]:
        state["spark"].stop()
    if state["cassandra_session"]:
        state["cassandra_session"].shutdown()
    logger.info("🛑 API arrêtée proprement")

# ─────────────────────────────────────────────
# MODÈLES PYDANTIC
# ─────────────────────────────────────────────

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    role:         str

# -- Trips --
class TripRequest(BaseModel):
    origin_zone:      int = Field(..., ge=1, le=17, description="Zone de départ (1-17)")
    destination_zone: int = Field(..., ge=1, le=17, description="Zone d'arrivée (1-17)")
    rider_id:         str = Field(..., description="Identifiant du passager")

class TripResponse(BaseModel):
    trip_id: str
    status:  str  # "pending"
    origin_zone:      int
    destination_zone: int
    rider_id:         str
    created_at:       str

class TripStatusResponse(BaseModel):
    trip_id:          str
    status:           str   # "pending" | "matched" | "completed"
    origin_zone:      int
    destination_zone: int
    eta_seconds:      Optional[int] = None
    taxi_id:          Optional[int] = None

# -- Vehicles --
class VehiclePosition(BaseModel):
    taxi_id:    int
    lat:        float
    lon:        float
    zone_id:    int
    zone_name:  str
    status:     str   # "available" | "moving"
    speed:      float
    event_time: str

# -- Forecast --
class ForecastRequest(BaseModel):
    zone_id:  int = Field(..., ge=1, le=17, description="ID de la zone (1-17)")
    datetime: str = Field(..., description="Format ISO 8601: 2026-06-05T08:30:00")

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
    session = state["cassandra_session"]
    if session is None:
        return {"lag_1d": DEFAULT_LAG_1D, "lag_7d": DEFAULT_LAG_7D, "rolling_mean": DEFAULT_ROLLING_MEAN}
    try:
        ts_1d = dt - timedelta(days=1)
        ts_7d = dt - timedelta(days=7)

        row_1d = session.execute(
            "SELECT pending_requests FROM demand_zones WHERE city=%s AND zone_id=%s AND window_start >= %s ORDER BY window_start DESC LIMIT 1",
            ("Casablanca", zone_id, ts_1d - timedelta(hours=1))
        ).one()
        row_7d = session.execute(
            "SELECT pending_requests FROM demand_zones WHERE city=%s AND zone_id=%s AND window_start >= %s ORDER BY window_start DESC LIMIT 1",
            ("Casablanca", zone_id, ts_7d - timedelta(hours=1))
        ).one()
        rows_7d = session.execute(
            "SELECT pending_requests FROM demand_zones WHERE city=%s AND zone_id=%s AND window_start >= %s LIMIT 336",
            ("Casablanca", zone_id, dt - timedelta(days=7))
        ).all()

        lag_1d  = float(row_1d.pending_requests) if row_1d else DEFAULT_LAG_1D
        lag_7d  = float(row_7d.pending_requests) if row_7d else DEFAULT_LAG_7D
        rolling = sum(r.pending_requests for r in rows_7d) / len(rows_7d) if rows_7d else DEFAULT_ROLLING_MEAN
        return {"lag_1d": lag_1d, "lag_7d": lag_7d, "rolling_mean": rolling}
    except Exception as e:
        logger.warning(f"⚠️ Cassandra read failed zone {zone_id}: {e}")
        return {"lag_1d": DEFAULT_LAG_1D, "lag_7d": DEFAULT_LAG_7D, "rolling_mean": DEFAULT_ROLLING_MEAN}

def build_feature_row(zone_id: int, dt: datetime, lags: dict) -> dict:
    zone  = ZONE_FEATURES.get(zone_id, ZONE_FEATURES[1])
    dow   = dt.weekday()
    month = dt.month
    if month in [12, 1, 2]:
        temp_bucket = "cold"
    elif month in [6, 7, 8, 9]:
        temp_bucket = "hot"
    else:
        temp_bucket = "mild"
    return {
        "hour_of_day":             dt.hour,
        "day_of_week":             dow,
        "is_weekend":              1 if dow >= 5 else 0,
        "is_friday":               1 if dow == 4 else 0,
        "zone_population_density": float(zone["population_density"]),
        "zone_type_urban":         zone["urban"],
        "zone_type_residential":   zone["residential"],
        "zone_type_commercial":    zone["commercial"],
        "zone_type_transit_hub":   zone["transit_hub"],
        "is_raining":              0,
        "demand_lag_1d":           lags["lag_1d"],
        "demand_lag_7d":           lags["lag_7d"],
        "rolling_7d_mean":         lags["rolling_mean"],
        "temperature_bucket":      temp_bucket,
    }

# ─────────────────────────────────────────────
# APPLICATION FASTAPI
# ─────────────────────────────────────────────

app = FastAPI(
    title="TaaSim API",
    description="Plateforme de mobilité urbaine Casablanca — Capstone ENSA Al Hoceima 2025-2026",
    version="1.0.0",
    lifespan=lifespan,
)

# ── AUTH ──────────────────────────────────────

@app.post("/auth/token", response_model=TokenResponse, tags=["Auth"])
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Génère un token JWT.

    Comptes de test :
    - rider1 / rider123  → rôle rider
    - admin  / admin123  → rôle admin
    """
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token({"sub": user["username"], "role": user["role"]})
    return TokenResponse(access_token=token, token_type="bearer", role=user["role"])

# ── HEALTH (public) ───────────────────────────

@app.get("/api/health", tags=["System"])
async def health():
    return {
        "status":       "healthy" if state["ready"] else "loading",
        "model_loaded": state["model"] is not None,
        "cassandra":    state["cassandra_session"] is not None,
    }

# ── TRIPS (rider + admin) ─────────────────────

@app.post("/api/v1/trips", response_model=TripResponse, tags=["Trips"])
async def create_trip(
    request: TripRequest,
    current_user: dict = Depends(require_rider_or_admin),
):
    """
    Crée une réservation de course.
    Publie l'événement dans Kafka topic raw.trips.
    Accessible aux rôles rider et admin.
    """
    trip_id = str(uuid.uuid4())

    # Publier dans Kafka
    try:
        from kafka import KafkaProducer
        import json
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        event = {
            "trip_id":          trip_id,
            "origin_zone":      request.origin_zone,
            "destination_zone": request.destination_zone,
            "rider_id":         request.rider_id,
            "timestamp":        datetime.now(timezone.utc).isoformat(),
            "status":           "pending",
        }
        producer.send("raw.trips", key=trip_id, value=event)
        producer.flush()
        logger.info(f"✅ Trip {trip_id} publié dans Kafka")
    except Exception as e:
        logger.warning(f"⚠️ Kafka indisponible: {e} — trip enregistré sans streaming")

    return TripResponse(
        trip_id=trip_id,
        status="pending",
        origin_zone=request.origin_zone,
        destination_zone=request.destination_zone,
        rider_id=request.rider_id,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/api/v1/trips/{trip_id}", response_model=TripStatusResponse, tags=["Trips"])
async def get_trip_status(
    trip_id: str,
    current_user: dict = Depends(require_rider_or_admin),
):
    """
    Retourne le statut d'une course (pending / matched / completed).
    Lit depuis Cassandra table trips.
    Accessible aux rôles rider et admin.
    """
    session = state["cassandra_session"]

    if session is not None:
        try:
            row = session.execute(
                "SELECT * FROM trips WHERE trip_id=%s LIMIT 1",
                (trip_id,)
            ).one()
            if row:
                return TripStatusResponse(
                    trip_id=trip_id,
                    status=row.status,
                    origin_zone=row.origin_zone,
                    destination_zone=row.destination_zone,
                    eta_seconds=getattr(row, "eta_seconds", None),
                    taxi_id=getattr(row, "taxi_id", None),
                )
        except Exception as e:
            logger.warning(f"⚠️ Cassandra read trips failed: {e}")

    # Fallback si Cassandra indisponible ou trip pas encore traité par Flink
    return TripStatusResponse(
        trip_id=trip_id,
        status="pending",
        origin_zone=0,
        destination_zone=0,
        eta_seconds=None,
        taxi_id=None,
    )

# ── VEHICLES (admin seulement) ────────────────

@app.get("/api/v1/vehicles/zone/{zone_id}", response_model=List[VehiclePosition], tags=["Vehicles"])
async def get_vehicles_in_zone(
    zone_id: int,
    current_user: dict = Depends(require_admin),   # ← admin SEULEMENT
):
    """
    Retourne les véhicules actifs dans une zone (dernières 30 secondes).
    Lit depuis Cassandra table vehicle_positions.
    Accessible uniquement au rôle admin.
    """
    session = state["cassandra_session"]

    if session is not None:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(seconds=30)
            rows = session.execute(
                "SELECT * FROM vehicle_positions WHERE city=%s AND zone_id=%s AND event_time > %s",
                ("Casablanca", zone_id, cutoff)
            ).all()
            return [
                VehiclePosition(
                    taxi_id=r.taxi_id,
                    lat=r.lat,
                    lon=r.lon,
                    zone_id=r.zone_id,
                    zone_name=getattr(r, "zone_name", f"Zone {zone_id}"),
                    status=getattr(r, "status", "available"),
                    speed=getattr(r, "speed", 0.0),
                    event_time=str(r.event_time),
                )
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"⚠️ Cassandra read vehicle_positions failed: {e}")

    # Fallback : liste vide si Cassandra indisponible
    return []

# ── FORECAST (rider + admin) ──────────────────

@app.post("/api/demand/forecast", response_model=ForecastResponse, tags=["Demand"])
async def forecast(
    request: ForecastRequest,
    current_user: dict = Depends(require_rider_or_admin),
):
    """
    Prédit la demande de taxis pour une zone et une datetime données.
    Accessible aux rôles rider et admin.
    """
    if not state["ready"] or state["model"] is None:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    try:
        dt = datetime.fromisoformat(request.datetime)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Format datetime invalide. Utilisez ISO 8601: 2026-06-05T08:30:00",
        )

    lags     = get_lag_features(request.zone_id, dt)
    features = build_feature_row(request.zone_id, dt, lags)

    try:
        spark_df  = state["spark"].createDataFrame([features])
        pred_df   = state["model"].transform(spark_df)
        predicted = pred_df.select("prediction").collect()[0][0]
        predicted = max(0.0, round(float(predicted), 2))
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