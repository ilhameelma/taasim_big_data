"""
TaaSim Demand Forecasting API
Adapté pour docker-compose + scripts Personne B
Modèle: s3a://machine-learning/models/demand_v1/
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List
import pandas as pd
import os
import logging
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

model = None
spark = None

# Configuration (alignée avec docker-compose et scripts Personne B)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "taasim"
MINIO_SECRET_KEY = "taasim123"
S3_MODEL_PATH = "s3a://machine-learning/models/demand_v1/"

class ForecastRequest(BaseModel):
    zone_id: int = Field(..., ge=1, le=16)
    datetime: str

class ForecastResponse(BaseModel):
    zone_id: int
    datetime: str
    predicted_demand: float

app = FastAPI(title="TaaSim Demand Forecast API")

def get_spark_session():
    spark = SparkSession.builder \
        .appName("TaaSim-Demand-API") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .master("local[2]") \
        .getOrCreate()
    return spark

def load_model():
    global spark, model
    logger.info(f"Chargement modèle depuis {S3_MODEL_PATH}...")
    if spark is None:
        spark = get_spark_session()
    try:
        model = PipelineModel.load(S3_MODEL_PATH)
        logger.info("✅ Modèle chargé")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        model = None
        return False

def create_feature_vector(zone_id: int, dt: datetime, 
                          lag_1d=42.0, lag_7d=38.0, rolling=40.0) -> pd.DataFrame:
    """
    Construit le vecteur de features dans l'ORDRE EXACT attendu par le modèle.
    Ordre = NUMERIC_FEATURES + ["temp_bucket_idx"]
    """
    # Temporelles
    hour = dt.hour
    dow = dt.weekday()
    is_weekend = 1 if dow >= 5 else 0
    is_friday = 1 if dow == 4 else 0
    
    # Spatiales (à adapter selon zone_mapping réel)
    zone_density = {i: 5000 + i*300 for i in range(1, 17)}.get(zone_id, 5000)
    zone_type_urban = 1 if zone_id in [1, 8, 12] else 0
    zone_type_residential = 1 if zone_id in [2,3,4,5,6,7] else 0
    zone_type_commercial = 1 if zone_id in [8,12] else 0
    zone_type_transit_hub = 1 if zone_id in [1,8,12] else 0
    
    # Météo (simulée - à remplacer par Open-Meteo)
    is_raining = 0
    # temperature_bucket → StringIndexer output: 0=cold, 1=mild, 2=hot
    temp_bucket_idx = 1  # mild par défaut
    
    data_dict = {
        'hour_of_day': [hour],
        'day_of_week': [dow],
        'is_weekend': [is_weekend],
        'is_friday': [is_friday],
        'zone_population_density': [zone_density],
        'zone_type_urban': [zone_type_urban],
        'zone_type_residential': [zone_type_residential],
        'zone_type_commercial': [zone_type_commercial],
        'zone_type_transit_hub': [zone_type_transit_hub],
        'is_raining': [is_raining],
        'demand_lag_1d': [lag_1d],
        'demand_lag_7d': [lag_7d],
        'rolling_7d_mean': [rolling],
        'temp_bucket_idx': [temp_bucket_idx],
    }
    return pd.DataFrame(data_dict)

@app.on_event("startup")
async def startup():
    load_model()

@app.post("/api/v1/demand/forecast", response_model=ForecastResponse)
async def forecast(request: ForecastRequest):
    if model is None:
        raise HTTPException(status_code=503, detail="Modèle non chargé")
    
    try:
        dt = datetime.fromisoformat(request.datetime)
        features = create_feature_vector(request.zone_id, dt)
        spark_df = spark.createDataFrame(features)
        pred_df = model.transform(spark_df)
        predicted = pred_df.select("prediction").collect()[0][0]
        
        return ForecastResponse(
            zone_id=request.zone_id,
            datetime=request.datetime,
            predicted_demand=float(predicted)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/health")
async def health():
    return {"status": "healthy", "model_loaded": model is not None}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)