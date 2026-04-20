#!/usr/bin/env python3
"""
TaaSim - Trip Request Producer
Selon specs du projet: page 19 - Simule des réservations citoyens

Utilise les courbes de demande générées par le notebook:
- demand_curve_hourly.json: demande par heure (normalisée entre 0 et 1)
- demand_curve_daily.json: demande par jour de semaine (normalisée entre 0 et 1)

Utilise zone_mapping_geojson.csv avec la colonne prefecture comme nom de zone
"""

import json
import time
import random
import uuid
import argparse
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TripRequestProducer:
    def __init__(self, bootstrap_servers='kafka:9092', data_path=None, speed_factor=1.0):
        self.bootstrap_servers = bootstrap_servers
        self.speed_factor = speed_factor
        self.data_path = Path(data_path) if data_path else Path(__file__).parent.parent / 'data'
        
        # Charger les courbes de demande
        self.demand_hourly = {}
        self.demand_daily = {}
        self.load_demand_curves()
        
        # Charger le mapping des zones
        self.load_zone_mapping()
        
        # Producer Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.sent_count = 0
        
    def load_demand_curves(self):
        """Charge les courbes de demande depuis les fichiers JSON"""
        # Charger la courbe horaire
        hourly_path = self.data_path / 'demand_curve_hourly.json'
        if hourly_path.exists():
            with open(hourly_path, 'r') as f:
                self.demand_hourly = json.load(f)
            logger.info(f"Courbe horaire chargée: {len(self.demand_hourly)} heures")
            self.demand_hourly = {int(k): v for k, v in self.demand_hourly.items()}
        else:
            logger.warning(f"demand_curve_hourly.json non trouvé, utilisation des valeurs par défaut")
            self.demand_hourly = {
                0: 0.3, 1: 0.2, 2: 0.1, 3: 0.1, 4: 0.2, 5: 0.5,
                6: 0.8, 7: 3.5, 8: 4.0, 9: 3.0, 10: 1.5, 11: 1.8,
                12: 1.5, 13: 1.2, 14: 1.2, 15: 1.5, 16: 2.0,
                17: 3.5, 18: 4.0, 19: 3.0, 20: 1.8, 21: 1.5,
                22: 1.0, 23: 0.6
            }
        
        # Charger la courbe journalière
        daily_path = self.data_path / 'demand_curve_daily.json'
        if daily_path.exists():
            with open(daily_path, 'r') as f:
                self.demand_daily = json.load(f)
            logger.info(f"Courbe journalière chargée: {len(self.demand_daily)} jours")
            self.demand_daily = {int(k): v for k, v in self.demand_daily.items()}
        else:
            logger.warning(f"demand_curve_daily.json non trouvé, utilisation des valeurs par défaut")
            self.demand_daily = {
                0: 0.83, 1: 0.85, 2: 0.84, 3: 0.89, 4: 1.0, 5: 0.93, 6: 0.83
            }
        
        # Normaliser les valeurs
        max_hourly = max(self.demand_hourly.values())
        self.demand_hourly = {h: v / max_hourly for h, v in self.demand_hourly.items()}
        
        max_daily = max(self.demand_daily.values())
        self.demand_daily = {d: v / max_daily for d, v in self.demand_daily.items()}
        
        logger.info("Courbes de demande normalisées (valeurs entre 0 et 1)")
        
    def load_zone_mapping(self):
        """Charge la table des zones depuis zone_mapping_geojson.csv"""
        # Essayer d'abord le nouveau fichier
        zone_path_geojson = self.data_path / 'zone_mapping_geojson.csv'
        zone_path_old = self.data_path / 'zone_mapping.csv'
        
        if zone_path_geojson.exists():
            self.zones_df = pd.read_csv(zone_path_geojson)
            logger.info(f"✅ Zones chargées depuis zone_mapping_geojson.csv: {len(self.zones_df)} zones")
        elif zone_path_old.exists():
            self.zones_df = pd.read_csv(zone_path_old)
            logger.info(f"⚠️ Zones chargées depuis zone_mapping.csv (ancien): {len(self.zones_df)} zones")
        else:
            logger.error(f"❌ Aucun fichier zone_mapping trouvé")
            self.zones = [{'zone_id': i, 'zone_type': 'mixed', 'zone_name': f'Zone_{i}'} for i in range(1, 17)]
            return

        # Vérifier les colonnes disponibles
        logger.info(f"Colonnes disponibles: {list(self.zones_df.columns)}")
        
        # Préparer les zones pour la sélection
        self.zones = []
        
        # Si le fichier a la colonne 'prefecture', l'utiliser comme zone_name
        if 'prefecture' in self.zones_df.columns:
            for _, row in self.zones_df.iterrows():
                zone = {
                    'zone_id': int(row['zone_id']),
                    'zone_name': str(row['prefecture']),  # ← Utilise prefecture
                    'zone_type': str(row.get('zone_type', 'mixed')),
                    'base_fare_mad': float(row.get('base_fare_mad', 8.0)),
                    'population': row.get('population', 0),
                    'superficie_m2': row.get('superficie_m2', 0)
                }
                self.zones.append(zone)
            logger.info(f"✅ {len(self.zones)} zones chargées (avec prefecture comme nom)")
        else:
            # Fallback: utiliser les colonnes existantes
            for _, row in self.zones_df.iterrows():
                zone = {
                    'zone_id': int(row['zone_id']),
                    'zone_name': str(row.get('zone_name', f"Zone_{row['zone_id']}")),
                    'zone_type': str(row.get('zone_type', 'mixed')),
                    'base_fare_mad': float(row.get('base_fare_mad', 8.0))
                }
                self.zones.append(zone)
            logger.info(f"⚠️ {len(self.zones)} zones chargées (sans prefecture)")
        
        # Afficher les zones pour vérification
        logger.info("📋 ZONES DISPONIBLES:")
        for zone in self.zones[:10]:
            logger.info(f"   Zone {zone['zone_id']}: {zone['zone_name']} ({zone['zone_type']}) - {zone['base_fare_mad']} MAD")
        if len(self.zones) > 10:
            logger.info(f"   ... et {len(self.zones)-10} autres zones")
        
        # Statistiques sur les tarifs
        if self.zones:
            fares = [z['base_fare_mad'] for z in self.zones]
            logger.info(f"Tarifs: min={min(fares)} MAD, max={max(fares)} MAD, moyenne={sum(fares)/len(fares):.1f} MAD")
    
    def get_demand_multiplier(self, current_time):
        """Calcule le multiplicateur de demande"""
        hour = current_time.hour
        day_of_week = current_time.weekday()
        
        hourly_factor = self.demand_hourly.get(hour, 0.5)
        daily_factor = self.demand_daily.get(day_of_week, 0.8)
        
        combined_factor = (hourly_factor * 0.7 + daily_factor * 0.3)
        
        return combined_factor
    
    def get_zone_weight(self, zone):
        """Calcule le poids d'une zone pour la sélection"""
        weight = 1.0
        
        # Ajuster selon le type de zone
        zone_type = str(zone.get('zone_type', 'mixed')).lower()
        if zone_type == 'commercial':
            weight *= 1.3
        elif zone_type == 'residential':
            weight *= 0.8
        elif zone_type == 'industrial':
            weight *= 0.6
        elif zone_type == 'transit_hub':
            weight *= 1.5
        
        # Ajuster selon la population (si disponible)
        if 'population' in zone and zone['population']:
            try:
                pop = float(zone['population'])
                # Normaliser la population (max ~500000)
                pop_factor = min(1.5, max(0.5, pop / 250000))
                weight *= pop_factor
            except:
                pass
        
        return max(0.3, min(weight, 3.0))
    
    def select_weighted_zone(self, exclude_zone=None):
        """Sélectionne une zone aléatoire avec un poids"""
        eligible_zones = []
        weights = []
        
        for zone in self.zones:
            if exclude_zone is not None and zone['zone_id'] == exclude_zone:
                continue
            eligible_zones.append(zone)
            weights.append(self.get_zone_weight(zone))
        
        if not eligible_zones:
            return self.zones[0]
        
        selected = random.choices(eligible_zones, weights=weights, k=1)[0]
        return selected
    
    def get_call_type(self, hour):
        """Distribution CALL_TYPE basée sur l'heure"""
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            weights = [0.4, 0.3, 0.3]  # A, B, C
        elif 20 <= hour <= 23:
            weights = [0.2, 0.3, 0.5]
        elif 0 <= hour <= 5:
            weights = [0.2, 0.5, 0.3]
        else:
            weights = [0.3, 0.3, 0.4]
        
        return random.choices(['A', 'B', 'C'], weights=weights)[0]
    
    def create_trip_request(self, current_time):
        """Crée un message de réservation"""
        demand_multiplier = self.get_demand_multiplier(current_time)
        base_rate = demand_multiplier * self.speed_factor
        
        origin_zone = self.select_weighted_zone()
        destination_zone = self.select_weighted_zone(exclude_zone=origin_zone['zone_id'])
        
        # Calculer le prix estimé
        estimated_fare = None
        if 'base_fare_mad' in origin_zone and 'base_fare_mad' in destination_zone:
            try:
                base_fare = (float(origin_zone.get('base_fare_mad', 5)) + 
                            float(destination_zone.get('base_fare_mad', 5))) / 2
                estimated_fare = round(base_fare + random.uniform(-2, 5), 2)
                estimated_fare = max(5, estimated_fare)
            except (ValueError, TypeError):
                estimated_fare = 8.0
        
        # Créer le message avec prefecture comme zone_name
        request = {
            "trip_id": str(uuid.uuid4()),
            "rider_id": f"rider_{random.randint(1, 10000)}",
            "origin_zone": origin_zone['zone_id'],
            "origin_zone_name": origin_zone.get('zone_name', 'Unknown'),  # ← prefecture
            "origin_zone_type": origin_zone.get('zone_type', 'mixed'),
            "origin_prefecture": origin_zone.get('zone_name', 'Unknown'),  # ← champ explicite
            "destination_zone": destination_zone['zone_id'],
            "destination_zone_name": destination_zone.get('zone_name', 'Unknown'),  # ← prefecture
            "destination_prefecture": destination_zone.get('zone_name', 'Unknown'),  # ← champ explicite
            "destination_zone_type": destination_zone.get('zone_type', 'mixed'),
            "requested_at": current_time.isoformat(),
            "call_type": self.get_call_type(current_time.hour),
            "passenger_count": random.randint(1, 4),
            "demand_multiplier": round(demand_multiplier, 3)
        }
        
        if estimated_fare:
            request["estimated_fare_mad"] = estimated_fare
        
        return request, base_rate
    
    def run(self, duration_seconds=None):
        """Lance la simulation"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration_seconds) if duration_seconds else None
        
        logger.info("=" * 60)
        logger.info("🚕 Démarrage du producer de réservations TaaSim")
        logger.info(f"📊 Speed factor: {self.speed_factor}x")
        logger.info(f"📍 Courbe de demande: horaire + journalière")
        logger.info(f"🗺️ Zones disponibles: {len(self.zones)}")
        logger.info("=" * 60)
        
        try:
            while True:
                current_time = datetime.now()
                
                if end_time and current_time >= end_time:
                    break
                
                request, base_rate = self.create_trip_request(current_time)
                delay = 1.0 / max(base_rate, 0.1)
                
                self.producer.send('raw.trips', request)
                self.sent_count += 1
                
                if self.sent_count % 100 == 0:
                    logger.info(f"📊 Envoyé: {self.sent_count} réservations | Taux: {base_rate:.2f} req/s")
                
                time.sleep(delay / self.speed_factor)
                
        except KeyboardInterrupt:
            logger.info("🛑 Arrêt demandé par l'utilisateur")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"✅ Total envoyé: {self.sent_count} réservations")
            logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Trip Request Producer pour TaaSim',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Simulation normale (1x)
  python trip_request_producer.py --speed-factor 1.0
  
  # Simulation accélérée (5x)
  python trip_request_producer.py --speed-factor 5.0
  
  # Simulation pendant 60 secondes
  python trip_request_producer.py --duration 60 --speed-factor 2.0
  
  # Avec données personnalisées
  python trip_request_producer.py --data-path /home/jovyan/work/data --speed-factor 3.0
        """
    )
    
    parser.add_argument('--bootstrap-servers', default='kafka:9092',
                        help='Adresse du broker Kafka')
    parser.add_argument('--data-path', default=None,
                        help='Chemin vers les données')
    parser.add_argument('--speed-factor', type=float, default=1.0,
                        help='Facteur d\'accélération')
    parser.add_argument('--duration', type=int, default=None,
                        help='Durée de simulation en secondes')
    
    args = parser.parse_args()
    
    producer = TripRequestProducer(
        bootstrap_servers=args.bootstrap_servers,
        data_path=args.data_path,
        speed_factor=args.speed_factor
    )
    
    producer.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()