#!/usr/bin/env python3
"""
TaaSim - Trip Request Producer
Selon specs du projet: page 19 - Simule des réservations citoyens
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
        
        # Charger les données Porto pour la courbe de demande
        self.load_demand_profile()
        self.load_zone_mapping()
        
        # Producer Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.sent_count = 0
        
    def load_demand_profile(self):
        """Charge la courbe de demande du dataset Porto (page 19)"""
        # À partir de l'analyse du notebook 01_porto_profiling.ipynb
        # Pics: 7-9h et 17-19h
        self.demand_profile = {
            0: 0.3, 1: 0.2, 2: 0.1, 3: 0.1, 4: 0.2, 5: 0.5,
            6: 0.8, 7: 3.5, 8: 4.0, 9: 3.0, 10: 1.5, 11: 1.8,
            12: 1.5, 13: 1.2, 14: 1.2, 15: 1.5, 16: 2.0,
            17: 3.5, 18: 4.0, 19: 3.0, 20: 1.8, 21: 1.5,
            22: 1.0, 23: 0.6
        }
        
    def load_zone_mapping(self):
        """Charge la table des zones Casablanca"""
        zone_path = self.data_path / 'zone_mapping.csv'
        if zone_path.exists():
            self.zones_df = pd.read_csv(zone_path)
            self.zones = self.zones_df[['zone_id', 'zone_type']].drop_duplicates().to_dict('records')
            logger.info(f"Zones chargées: {len(self.zones)}")
        else:
            logger.warning(f"zone_mapping.csv non trouvé dans {zone_path}")
            self.zones = [{'zone_id': i, 'zone_type': 'mixed'} for i in range(1, 17)]
    
    def get_call_type(self):
        """Distribution CALL_TYPE du dataset Porto (A:30%, B:30%, C:40%)"""
        return random.choices(['A', 'B', 'C'], weights=[0.3, 0.3, 0.4])[0]
    
    def create_trip_request(self, current_time):
        """Crée un message de réservation selon specs page 19"""
        hour = current_time.hour
        
        # Appliquer le multiplicateur de demande
        base_rate = self.demand_profile.get(hour, 0.5)
        
        # Facteur Vendredi midi (page 19)
        if current_time.weekday() == 4 and 12 <= hour <= 14:
            base_rate *= 0.5
        
        # Facteur d'accélération
        rate = base_rate * self.speed_factor
        
        # Zones aléatoires (origin != destination)
        zones = random.sample(self.zones, 2)
        
        return {
            "trip_id": str(uuid.uuid4()),
            "rider_id": f"rider_{random.randint(1, 10000)}",
            "origin_zone": zones[0]['zone_id'],
            "destination_zone": zones[1]['zone_id'],
            "requested_at": current_time.isoformat(),
            "call_type": self.get_call_type(),
            "passenger_count": random.randint(1, 4)
        }, rate
    
    def run(self, duration_seconds=None):
        """Lance la simulation (page 19)"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration_seconds) if duration_seconds else None
        
        logger.info("Démarrage du producer de réservations...")
        
        try:
            while True:
                current_time = datetime.now()
                
                if end_time and current_time >= end_time:
                    break
                
                # Créer et envoyer un message
                request, rate = self.create_trip_request(current_time)
                
                # Ajuster le délai selon la demande
                delay = 1.0 / max(rate, 0.1)  # Inverse du taux
                
                self.producer.send('raw.trips', request)
                self.sent_count += 1
                
                if self.sent_count % 100 == 0:
                    logger.info(f"Envoyé: {self.sent_count} réservations")
                
                time.sleep(delay / self.speed_factor)
                
        except KeyboardInterrupt:
            logger.info("Arrêt demandé")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Total envoyé: {self.sent_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', default='kafka:9092')
    parser.add_argument('--data-path', default=None)
    parser.add_argument('--speed-factor', type=float, default=1.0)
    parser.add_argument('--duration', type=int, default=None)
    
    args = parser.parse_args()
    
    producer = TripRequestProducer(
        bootstrap_servers=args.bootstrap_servers,
        data_path=args.data_path,
        speed_factor=args.speed_factor
    )
    
    producer.run(duration_seconds=args.duration)